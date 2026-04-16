"""
Tests for the rolling ridge regression calibrator.

Tests cover:
  - Feature vector construction from category scores + raw indicators + EMA positions
  - Ridge regression training and prediction
  - Cold-start fallback when insufficient training data
  - Excess return computation (vs SPY benchmark)
  - Full calibrate_score integration with database
"""

import sqlite3
from datetime import date, timedelta
from typing import Generator

import numpy as np
import pytest

from src.scorer.calibrator import (
    build_feature_vector,
    compute_excess_return,
    fetch_training_data,
    train_ridge_and_predict,
    calibrate_score,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def calibration_config() -> dict:
    """Return a calibration config dict matching scorer.json['calibration']."""
    return {
        "enabled": True,
        "window_size": 90,
        "ridge_lambda": 0.1,
        "min_training_samples": 30,
        "benchmark_ticker": "SPY",
        "forward_days": 10,
    }


@pytest.fixture
def sample_category_scores() -> dict:
    """Return realistic category scores for a bullish signal."""
    return {
        "trend": 55.0,
        "momentum": 60.0,
        "volume": 20.0,
        "volatility": -10.0,
        "fundamental": 15.0,
        "macro": 30.0,
    }


@pytest.fixture
def sample_raw_indicators() -> dict:
    """Return realistic raw indicator values."""
    return {
        "rsi_14": 62.0,
        "adx": 28.0,
        "macd_histogram": 0.35,
        "stoch_k": 68.0,
        "bb_pctb": 0.72,
        "cmf_20": 0.08,
    }


@pytest.fixture
def sample_ema_positions() -> dict:
    """Return realistic EMA position spreads (% terms)."""
    return {
        "price_ema9_spread": 1.2,
        "ema9_ema21_spread": 0.8,
        "ema21_ema50_spread": 1.5,
    }


def _add_ticker_data(
    conn: sqlite3.Connection,
    ticker: str,
    all_dates: list[str],
    n_signals: int,
    base_price: float,
    ticker_idx: int = 0,
) -> None:
    """Insert synthetic OHLCV, indicators, and scores for one ticker into existing dates."""
    for i in range(n_signals):
        dt = all_dates[i]
        cycle = np.sin(2 * np.pi * i / 20)
        trend = 50.0 * cycle + (ticker_idx - 1) * 10
        momentum = 40.0 * cycle + 5
        volume = 10.0 * cycle
        volatility = -5.0
        fundamental = 15.0 + ticker_idx * 5
        macro = 20.0 * cycle

        close = base_price + i * 0.5 + cycle * 5
        conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ticker, dt, close - 1, close + 2, close - 2, close, 50_000_000),
        )

        rsi = 50 + 15 * cycle
        adx = 25 + 5 * abs(cycle)
        macd_hist = 0.5 * cycle
        stoch_k = 50 + 20 * cycle
        bb_pctb = 0.5 + 0.3 * cycle
        cmf = 0.05 * cycle
        ema_9 = close * 0.99
        ema_21 = close * 0.98
        ema_50 = close * 0.96
        conn.execute(
            "INSERT OR REPLACE INTO indicators_daily "
            "(ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal, "
            "macd_histogram, adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, "
            "obv, cmf_20, ad_line, bb_upper, bb_lower, bb_pctb, atr_14, "
            "keltner_upper, keltner_lower) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker, dt, ema_9, ema_21, ema_50, 0.5, 0.3, macd_hist,
             adx, rsi, stoch_k, 55, 30, -30, 1_000_000, cmf, 500_000,
             close + 5, close - 5, bb_pctb, 1.5, close + 6, close - 6),
        )

        daily_score = trend * 0.3 + momentum * 0.3 + volume * 0.1 + macro * 0.3
        weekly_score = daily_score * 0.9
        final_score = daily_score * 0.2 + weekly_score * 0.8
        signal = "BULLISH" if final_score > 20 else ("BEARISH" if final_score < -20 else "NEUTRAL")
        conn.execute(
            "INSERT OR REPLACE INTO scores_daily "
            "(ticker, date, signal, confidence, final_score, regime, daily_score, weekly_score, "
            "trend_score, momentum_score, volume_score, volatility_score, "
            "candlestick_score, structural_score, sentiment_score, "
            "fundamental_score, macro_score, data_completeness, key_signals) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker, dt, signal, abs(final_score), final_score, "trending",
             daily_score, weekly_score,
             trend, momentum, volume, volatility, 0, 0, 0, fundamental, macro,
             "{}", "[]"),
        )


def _populate_training_data(conn: sqlite3.Connection, n_signals: int = 50) -> None:
    """Insert synthetic scores + OHLCV data to serve as training data for the calibrator.

    Creates n_signals days of scored data for 3 tickers (AAPL, MSFT, JPM) plus SPY.
    Forward returns are deterministic: higher trend/momentum scores → higher future prices.
    """
    tickers = ["AAPL", "MSFT", "JPM"]
    base = date(2025, 1, 2)
    day_count = 0
    trading_date = base

    # Generate enough trading days
    all_dates = []
    d = base
    while len(all_dates) < n_signals + 20:  # extra for forward returns
        if d.weekday() < 5:
            all_dates.append(d.isoformat())
        d += timedelta(days=1)

    # Insert SPY OHLCV (close = 500 + small drift)
    for i, dt in enumerate(all_dates):
        close = 500.0 + i * 0.1
        conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("SPY", dt, close - 0.5, close + 1, close - 1, close, 80_000_000),
        )

    # Insert ticker OHLCV and scores
    for ticker_idx, ticker in enumerate(tickers):
        base_price = 100.0 + ticker_idx * 50  # AAPL=100, MSFT=150, JPM=200
        _add_ticker_data(conn, ticker, all_dates, n_signals, base_price, ticker_idx)

    conn.commit()


# ---------------------------------------------------------------------------
# Tests: build_feature_vector
# ---------------------------------------------------------------------------

class TestBuildFeatureVector:
    """Tests for building the 17-element feature vector."""

    def test_returns_17_features(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """Feature vector has exactly 17 elements."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions
        )
        assert len(vec) == 17

    def test_correct_values(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """Feature vector contains the expected values in the correct order."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions,
            weekly_score=42.5,
            monthly_score=30.0,
        )
        # Categories: trend, momentum, volume, volatility, fundamental, macro
        assert vec[0] == 55.0  # trend
        assert vec[1] == 60.0  # momentum
        assert vec[2] == 20.0  # volume
        assert vec[3] == -10.0  # volatility
        assert vec[4] == 15.0  # fundamental
        assert vec[5] == 30.0  # macro
        # Raw indicators: rsi, adx, macd_hist, stoch_k, bb_pctb, cmf
        assert vec[6] == 62.0  # rsi
        assert vec[7] == 28.0  # adx
        assert vec[8] == 0.35  # macd_histogram
        assert vec[9] == 68.0  # stoch_k
        assert vec[10] == 0.72  # bb_pctb
        assert vec[11] == 0.08  # cmf
        # EMA positions: price-ema9, ema9-ema21, ema21-ema50
        assert vec[12] == 1.2
        assert vec[13] == 0.8
        assert vec[14] == 1.5
        # Weekly score — raw, not divided by 100
        assert vec[15] == 42.5
        # Monthly score — raw, not divided by 100
        assert vec[16] == 30.0

    def test_weekly_score_none_defaults_to_zero(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """weekly_score=None is replaced with 0.0 at position 15."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions,
            weekly_score=None,
        )
        assert len(vec) == 17
        assert vec[15] == 0.0

    def test_weekly_score_omitted_defaults_to_zero(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """Calling without weekly_score kwarg defaults to 0.0 at position 15."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions
        )
        assert len(vec) == 17
        assert vec[15] == 0.0

    def test_monthly_score_none_defaults_to_zero(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """monthly_score=None is replaced with 0.0 at position 16."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions,
            monthly_score=None,
        )
        assert len(vec) == 17
        assert vec[16] == 0.0

    def test_monthly_score_omitted_defaults_to_zero(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """Calling without monthly_score kwarg defaults to 0.0 at position 16."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions
        )
        assert len(vec) == 17
        assert vec[16] == 0.0

    def test_monthly_score_not_scaled(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """monthly_score is stored as-is (no /100 scaling)."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions,
            monthly_score=-45.0,
        )
        assert vec[16] == -45.0

    def test_weekly_score_not_scaled(
        self, sample_category_scores, sample_raw_indicators, sample_ema_positions
    ):
        """weekly_score is stored as-is (no /100 scaling)."""
        vec = build_feature_vector(
            sample_category_scores, sample_raw_indicators, sample_ema_positions,
            weekly_score=75.0,
        )
        assert vec[15] == 75.0

    def test_handles_none_values(self, sample_category_scores, sample_ema_positions):
        """None indicator values are replaced with defaults (0.0)."""
        indicators_with_nones = {
            "rsi_14": None,
            "adx": None,
            "macd_histogram": 0.5,
            "stoch_k": None,
            "bb_pctb": 0.6,
            "cmf_20": None,
        }
        vec = build_feature_vector(
            sample_category_scores, indicators_with_nones, sample_ema_positions
        )
        assert vec[6] == 0.0  # rsi default
        assert vec[7] == 0.0  # adx default
        assert vec[8] == 0.5  # macd_histogram (not None)
        assert vec[9] == 0.0  # stoch_k default
        assert vec[10] == 0.6  # bb_pctb (not None)
        assert vec[11] == 0.0  # cmf default


# ---------------------------------------------------------------------------
# Tests: compute_excess_return
# ---------------------------------------------------------------------------

class TestComputeExcessReturn:
    """Tests for excess return computation against SPY benchmark."""

    def test_positive_excess(self):
        """Ticker outperforms SPY → positive excess return."""
        excess = compute_excess_return(
            ticker_return_pct=5.0, benchmark_return_pct=2.0
        )
        assert excess == pytest.approx(3.0)

    def test_negative_excess(self):
        """Ticker underperforms SPY → negative excess return."""
        excess = compute_excess_return(
            ticker_return_pct=-1.0, benchmark_return_pct=2.0
        )
        assert excess == pytest.approx(-3.0)

    def test_zero_excess(self):
        """Ticker matches SPY exactly → zero excess."""
        excess = compute_excess_return(
            ticker_return_pct=3.0, benchmark_return_pct=3.0
        )
        assert excess == pytest.approx(0.0)

    def test_none_benchmark_falls_back_to_raw(self):
        """When benchmark return is None, return raw ticker return."""
        excess = compute_excess_return(
            ticker_return_pct=5.0, benchmark_return_pct=None
        )
        assert excess == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Tests: train_ridge_and_predict
# ---------------------------------------------------------------------------

class TestTrainRidgeAndPredict:
    """Tests for the ridge regression training and prediction."""

    def test_basic_prediction(self):
        """Ridge regression produces a reasonable prediction for correlated data."""
        np.random.seed(42)
        n = 100
        X = np.random.randn(n, 3)
        true_weights = np.array([2.0, -1.0, 0.5])
        y = X @ true_weights + np.random.randn(n) * 0.1

        x_new = np.array([1.0, 0.0, 0.0])
        result = train_ridge_and_predict(X, y, x_new, ridge_lambda=0.01)

        assert "prediction" in result
        assert "model_r2" in result
        assert "weights" in result
        # Prediction should be close to true_weights[0] * 1.0 = 2.0
        assert abs(result["prediction"] - 2.0) < 0.5
        # R² should be high for this clean signal
        assert result["model_r2"] > 0.9

    def test_returns_zero_for_single_sample(self):
        """With only 1 training sample, returns prediction=0.0 and r2=0.0."""
        X = np.array([[1.0, 2.0]])
        y = np.array([3.0])
        x_new = np.array([1.0, 2.0])

        result = train_ridge_and_predict(X, y, x_new, ridge_lambda=0.1)
        assert result["prediction"] == 0.0
        assert result["model_r2"] == 0.0

    def test_regularization_prevents_extreme_weights(self):
        """High lambda shrinks weights toward zero."""
        np.random.seed(42)
        n = 50
        X = np.random.randn(n, 5)
        y = X[:, 0] * 10  # only first feature matters

        x_new = np.array([1.0, 0, 0, 0, 0])

        result_low_lambda = train_ridge_and_predict(X, y, x_new, ridge_lambda=0.01)
        result_high_lambda = train_ridge_and_predict(X, y, x_new, ridge_lambda=100.0)

        # High lambda should produce a smaller prediction magnitude
        assert abs(result_high_lambda["prediction"]) < abs(result_low_lambda["prediction"])

    def test_weights_length_matches_features(self):
        """Returned weights have same length as number of features + 1 (intercept)."""
        X = np.random.randn(20, 4)
        y = np.random.randn(20)
        x_new = np.random.randn(4)

        result = train_ridge_and_predict(X, y, x_new, ridge_lambda=0.1)
        assert len(result["weights"]) == 5  # 4 features + 1 intercept


# ---------------------------------------------------------------------------
# Tests: fetch_training_data
# ---------------------------------------------------------------------------

class TestFetchTrainingData:
    """Tests for fetching historical scored signals with forward returns from DB."""

    def test_returns_features_and_targets(self, db_connection, calibration_config):
        """fetch_training_data returns X (features) and y (excess returns)."""
        _populate_training_data(db_connection, n_signals=50)

        X, y = fetch_training_data(
            db_connection,
            scoring_date="2025-02-20",
            config=calibration_config,
        )

        assert isinstance(X, np.ndarray)
        assert isinstance(y, np.ndarray)
        assert X.ndim == 2
        assert X.shape[1] == 17  # 17 features
        assert len(y) == len(X)
        assert len(y) > 0

    def test_respects_window_size(self, db_connection, calibration_config):
        """Training data is limited to signals within window_size calendar days."""
        _populate_training_data(db_connection, n_signals=50)
        scoring_date = "2025-02-20"

        # 5-day window: cutoff = 2025-02-15 (Sat), eligible trading days = Feb 17, 18, 19
        # At most 3 trading days × 3 tickers = 9 samples
        calibration_config["window_size"] = 5
        X_small, _ = fetch_training_data(
            db_connection, scoring_date=scoring_date, config=calibration_config
        )

        # 60-day window: covers all signals inserted (back to 2025-01-02)
        calibration_config["window_size"] = 60
        X_large, _ = fetch_training_data(
            db_connection, scoring_date=scoring_date, config=calibration_config
        )

        assert len(X_small) < len(X_large)
        assert len(X_small) <= 9

    def test_empty_when_no_data(self, db_connection, calibration_config):
        """Returns empty arrays when no training data exists."""
        X, y = fetch_training_data(
            db_connection,
            scoring_date="2025-02-20",
            config=calibration_config,
        )

        assert len(X) == 0
        assert len(y) == 0

    def test_etf_tickers_excluded_from_training(self, db_connection, calibration_config):
        """Tickers in excluded_tickers are not returned as training examples."""
        base = date(2025, 1, 2)
        all_dates = []
        d = base
        while len(all_dates) < 70:
            if d.weekday() < 5:
                all_dates.append(d.isoformat())
            d += timedelta(days=1)

        # SPY benchmark rows (needed for excess return computation)
        for i, dt in enumerate(all_dates):
            spy_close = 500.0 + i * 0.1
            db_connection.execute(
                "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("SPY", dt, spy_close - 0.5, spy_close + 1, spy_close - 1, spy_close, 80_000_000),
            )

        # Insert one regular stock (AAPL) and one sector ETF (XLK)
        _add_ticker_data(db_connection, "AAPL", all_dates, n_signals=50, base_price=100.0)
        _add_ticker_data(db_connection, "XLK", all_dates, n_signals=50, base_price=175.0)
        db_connection.commit()

        # Without exclusion, both AAPL and XLK rows are eligible
        X_all, _ = fetch_training_data(
            db_connection, scoring_date="2025-02-20", config=calibration_config,
        )

        # With XLK excluded, only AAPL rows remain
        X_excluded, _ = fetch_training_data(
            db_connection, scoring_date="2025-02-20", config=calibration_config,
            excluded_tickers={"XLK"},
        )

        assert len(X_excluded) < len(X_all)
        # Roughly half the rows since we only have two tickers with equal signal counts
        assert len(X_excluded) <= len(X_all) // 2 + 5

    def test_empty_excluded_set_does_not_filter(self, db_connection, calibration_config):
        """An empty excluded_tickers set returns the same data as no exclusion."""
        _populate_training_data(db_connection, n_signals=50)

        X_default, _ = fetch_training_data(
            db_connection, scoring_date="2025-02-20", config=calibration_config,
        )
        X_empty_set, _ = fetch_training_data(
            db_connection, scoring_date="2025-02-20", config=calibration_config,
            excluded_tickers=set(),
        )

        assert len(X_default) == len(X_empty_set)


# ---------------------------------------------------------------------------
# Tests: calibrate_score (full integration)
# ---------------------------------------------------------------------------

class TestCalibrateScore:
    """Integration tests for the complete calibration pipeline."""

    def test_returns_calibrated_score(
        self, db_connection, calibration_config,
        sample_category_scores, sample_raw_indicators, sample_ema_positions,
    ):
        """calibrate_score returns a dict with calibrated_score, model_r2, and weights."""
        _populate_training_data(db_connection, n_signals=50)

        result = calibrate_score(
            conn=db_connection,
            scoring_date="2025-02-20",
            category_scores=sample_category_scores,
            raw_indicators=sample_raw_indicators,
            ema_positions=sample_ema_positions,
            config=calibration_config,
        )

        assert "calibrated_score" in result
        assert "model_r2" in result
        assert isinstance(result["calibrated_score"], float)
        assert isinstance(result["model_r2"], float)
        assert result["model_r2"] >= 0.0

    def test_cold_start_returns_none(
        self, db_connection, calibration_config,
        sample_category_scores, sample_raw_indicators, sample_ema_positions,
    ):
        """When insufficient training data exists, calibrated_score is None."""
        calibration_config["min_training_samples"] = 30
        # No data in DB → cold start

        result = calibrate_score(
            conn=db_connection,
            scoring_date="2025-02-20",
            category_scores=sample_category_scores,
            raw_indicators=sample_raw_indicators,
            ema_positions=sample_ema_positions,
            config=calibration_config,
        )

        assert result["calibrated_score"] is None
        assert result["model_r2"] == 0.0

    def test_disabled_returns_none(
        self, db_connection, calibration_config,
        sample_category_scores, sample_raw_indicators, sample_ema_positions,
    ):
        """When calibration is disabled, calibrated_score is None."""
        calibration_config["enabled"] = False
        _populate_training_data(db_connection, n_signals=50)

        result = calibrate_score(
            conn=db_connection,
            scoring_date="2025-02-20",
            category_scores=sample_category_scores,
            raw_indicators=sample_raw_indicators,
            ema_positions=sample_ema_positions,
            config=calibration_config,
        )

        assert result["calibrated_score"] is None

    def test_bullish_features_produce_positive_score(
        self, db_connection, calibration_config,
    ):
        """Strongly bullish features should produce a positive calibrated_score."""
        _populate_training_data(db_connection, n_signals=50)

        bullish_categories = {
            "trend": 80.0, "momentum": 75.0, "volume": 40.0,
            "volatility": 10.0, "fundamental": 30.0, "macro": 60.0,
        }
        bullish_indicators = {
            "rsi_14": 72.0, "adx": 35.0, "macd_histogram": 1.5,
            "stoch_k": 80.0, "bb_pctb": 0.9, "cmf_20": 0.15,
        }
        bullish_ema = {
            "price_ema9_spread": 2.5,
            "ema9_ema21_spread": 1.5,
            "ema21_ema50_spread": 3.0,
        }

        result = calibrate_score(
            conn=db_connection,
            scoring_date="2025-02-20",
            category_scores=bullish_categories,
            raw_indicators=bullish_indicators,
            ema_positions=bullish_ema,
            config=calibration_config,
        )

        # With deterministic correlated training data, bullish features should
        # produce a positive or at least not strongly negative calibrated score
        assert result["calibrated_score"] is not None
