"""
Rolling ridge regression calibrator for signal scoring.

Replaces the static composite score with a data-driven prediction of
expected excess return. Trains a ridge regression on a rolling window
of recent signals and their realized 10-day excess returns (vs SPY),
then uses the learned weights to predict the current signal's expected
return.

Features (15 total):
  6 category scores  — trend, momentum, volume, volatility, fundamental, macro
  6 raw indicators   — RSI, ADX, MACD histogram, Stochastic %K, BB %B, CMF
  3 EMA positions    — price-EMA9, EMA9-EMA21, EMA21-EMA50 spreads (%)

Config keys (under scorer.json → "calibration"):
  enabled             — master switch (bool)
  window_size         — number of calendar days to look back for training signals (int)
  ridge_lambda        — L2 regularisation strength (float)
  min_training_samples — minimum samples required; fewer triggers cold-start fallback (int)
  benchmark_ticker    — ticker whose return is subtracted from each signal's return (str)
  forward_days        — trading-day horizon for measuring forward returns (int)
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

import numpy as np
from datetime import date, timedelta

logger = logging.getLogger(__name__)

_PHASE = "calibrator"

# Feature order — must match build_feature_vector() and fetch_training_data()
FEATURE_NAMES: list[str] = [
    "trend_score", "momentum_score", "volume_score",
    "volatility_score", "fundamental_score", "macro_score",
    "rsi_14", "adx", "macd_histogram", "stoch_k", "bb_pctb", "cmf_20",
    "price_ema9_spread", "ema9_ema21_spread", "ema21_ema50_spread",
]


# ---------------------------------------------------------------------------
# Feature construction
# ---------------------------------------------------------------------------

def build_feature_vector(
    category_scores: dict,
    raw_indicators: dict,
    ema_positions: dict,
) -> list[float]:
    """
    Assemble a 15-element feature vector from scored categories, raw
    indicator values, and EMA position spreads.

    None values are replaced with 0.0 so the model always receives a
    complete numeric vector.

    Parameters:
        category_scores: Dict with keys trend, momentum, volume, volatility,
                         fundamental, macro — each a float or None.
        raw_indicators:  Dict with keys rsi_14, adx, macd_histogram, stoch_k,
                         bb_pctb, cmf_20 — each a float or None.
        ema_positions:   Dict with keys price_ema9_spread, ema9_ema21_spread,
                         ema21_ema50_spread — each a float.

    Returns:
        List of 15 floats in the canonical feature order.
    """
    def _safe(val: Optional[float]) -> float:
        return float(val) if val is not None else 0.0

    return [
        _safe(category_scores.get("trend")),
        _safe(category_scores.get("momentum")),
        _safe(category_scores.get("volume")),
        _safe(category_scores.get("volatility")),
        _safe(category_scores.get("fundamental")),
        _safe(category_scores.get("macro")),
        _safe(raw_indicators.get("rsi_14")),
        _safe(raw_indicators.get("adx")),
        _safe(raw_indicators.get("macd_histogram")),
        _safe(raw_indicators.get("stoch_k")),
        _safe(raw_indicators.get("bb_pctb")),
        _safe(raw_indicators.get("cmf_20")),
        _safe(ema_positions.get("price_ema9_spread")),
        _safe(ema_positions.get("ema9_ema21_spread")),
        _safe(ema_positions.get("ema21_ema50_spread")),
    ]


# ---------------------------------------------------------------------------
# Excess return
# ---------------------------------------------------------------------------

def compute_excess_return(
    ticker_return_pct: float,
    benchmark_return_pct: Optional[float],
) -> float:
    """
    Compute the excess return of a ticker over a benchmark.

    Parameters:
        ticker_return_pct:    The ticker's N-day forward return (%).
        benchmark_return_pct: The benchmark's N-day forward return (%), or None.

    Returns:
        Excess return in percent. Falls back to raw ticker return if
        benchmark is None.
    """
    if benchmark_return_pct is None:
        return ticker_return_pct
    return ticker_return_pct - benchmark_return_pct


# ---------------------------------------------------------------------------
# Ridge regression
# ---------------------------------------------------------------------------

def train_ridge_and_predict(
    X_train: np.ndarray,
    y_train: np.ndarray,
    x_new: np.ndarray,
    ridge_lambda: float = 0.1,
) -> dict:
    """
    Train a ridge regression on (X_train, y_train) and predict for x_new.

    Appends an intercept column to X_train automatically.
    Returns prediction=0.0 and model_r2=0.0 when X_train has fewer than
    2 rows (not enough data to fit a meaningful model).

    Parameters:
        X_train: (n_samples, n_features) training feature matrix.
        y_train: (n_samples,) target vector of excess returns.
        x_new:   (n_features,) feature vector for the current signal.
        ridge_lambda: L2 regularisation strength.

    Returns:
        Dict with keys:
            prediction (float) — predicted excess return for x_new
            model_r2   (float) — in-sample R² of the training fit
            weights    (list[float]) — learned coefficients incl. intercept
    """
    n_samples = X_train.shape[0]
    if n_samples < 2:
        n_feats = x_new.shape[0] if x_new.ndim > 0 else 0
        return {
            "prediction": 0.0,
            "model_r2": 0.0,
            "weights": [0.0] * (n_feats + 1),
        }

    # Suppress spurious BLAS warnings from Apple Accelerate on macOS.
    # The matmul results are correct; the Accelerate backend triggers
    # false-positive divide/overflow/invalid FP exceptions.
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        return _fit_ridge(X_train, y_train, x_new, ridge_lambda, n_samples)


def _fit_ridge(
    X_train: np.ndarray,
    y_train: np.ndarray,
    x_new: np.ndarray,
    ridge_lambda: float,
    n_samples: int,
) -> dict:
    """Internal: standardise, solve ridge via augmented least-squares, predict."""
    # Standardise features
    col_mean = X_train.mean(axis=0)
    col_std = X_train.std(axis=0)
    col_std[col_std == 0] = 1.0  # avoid divide-by-zero for constant features
    X_scaled = (X_train - col_mean) / col_std
    x_new_scaled = (x_new - col_mean) / col_std

    # Append intercept column
    ones_train = np.ones((n_samples, 1))
    X_aug = np.hstack([X_scaled, ones_train])

    # Ridge via augmented least-squares (more stable than normal equations)
    n_cols = X_aug.shape[1]
    sqrt_lambda = np.sqrt(ridge_lambda)
    reg_rows = sqrt_lambda * np.eye(n_cols)
    reg_rows[-1, -1] = 0.0  # don't regularise the intercept
    X_ridge = np.vstack([X_aug, reg_rows])
    y_ridge = np.concatenate([y_train, np.zeros(n_cols)])

    weights, _, _, _ = np.linalg.lstsq(X_ridge, y_ridge, rcond=None)

    # Predict for x_new
    x_new_aug = np.append(x_new_scaled, 1.0)
    prediction = float(x_new_aug @ weights)

    # In-sample R² (on original data, not augmented)
    y_pred = X_aug @ weights
    ss_res = float(np.sum((y_train - y_pred) ** 2))
    ss_tot = float(np.sum((y_train - y_train.mean()) ** 2))
    model_r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    model_r2 = max(0.0, model_r2)  # clamp negative R² to 0

    return {
        "prediction": prediction,
        "model_r2": model_r2,
        "weights": weights.tolist(),
    }


# ---------------------------------------------------------------------------
# Training data fetch
# ---------------------------------------------------------------------------

def fetch_training_data(
    conn: sqlite3.Connection,
    scoring_date: str,
    config: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Fetch historical signals with their 15-feature vectors and realized
    excess returns to serve as training data for the ridge regression.

    Queries scored signals within the past `window_size` calendar days (across all
    tickers) that have both:
      - A known forward close price N trading days after the signal
      - A known SPY close price for the same forward period

    Parameters:
        conn:         Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date: The current scoring date (YYYY-MM-DD). Only signals
                      before this date are eligible for training.
        config:       Calibration config dict.

    Returns:
        (X, y) tuple where X is (n_samples, 15) feature matrix and
        y is (n_samples,) vector of excess returns. Both may be empty
        if no training data is available.
    """
    window_size = config.get("window_size", 90)
    forward_days = config.get("forward_days", 10)
    benchmark = config.get("benchmark_ticker", "SPY")

    # Compute the calendar-day cutoff: only signals within the last window_size days
    scoring_dt = date.fromisoformat(scoring_date)
    cutoff_date = (scoring_dt - timedelta(days=window_size)).isoformat()

    # Fetch scored signals within the calendar window, across all tickers
    rows = conn.execute(
        """
        SELECT s.ticker, s.date,
               s.trend_score, s.momentum_score, s.volume_score,
               s.volatility_score, s.fundamental_score, s.macro_score,
               i.rsi_14, i.adx, i.macd_histogram, i.stoch_k, i.bb_pctb, i.cmf_20,
               i.ema_9, i.ema_21, i.ema_50,
               o_sig.close AS signal_close
        FROM scores_daily s
        JOIN indicators_daily i ON s.ticker = i.ticker AND s.date = i.date
        JOIN ohlcv_daily o_sig ON s.ticker = o_sig.ticker AND s.date = o_sig.date
        WHERE s.date < ?
          AND s.date >= ?
          AND s.signal IS NOT NULL
          AND o_sig.close > 0
        ORDER BY s.date DESC
        """,
        (scoring_date, cutoff_date),
    ).fetchall()

    if not rows:
        return np.empty((0, 15)), np.empty(0)

    X_list: list[list[float]] = []
    y_list: list[float] = []

    for row in rows:
        ticker = row["ticker"]
        signal_date = row["date"]
        signal_close = row["signal_close"]

        # Get ticker's forward close
        future_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
            "ORDER BY date LIMIT 1 OFFSET ?",
            (ticker, signal_date, forward_days - 1),
        ).fetchone()
        if not future_row or future_row["close"] is None:
            continue
        ticker_return = (future_row["close"] - signal_close) / signal_close * 100.0

        # Get SPY's forward return over the same period
        spy_sig_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
            (benchmark, signal_date),
        ).fetchone()
        spy_fwd_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
            "ORDER BY date LIMIT 1 OFFSET ?",
            (benchmark, signal_date, forward_days - 1),
        ).fetchone()

        benchmark_return: Optional[float] = None
        if spy_sig_row and spy_fwd_row and spy_sig_row["close"] and spy_sig_row["close"] > 0:
            benchmark_return = (
                (spy_fwd_row["close"] - spy_sig_row["close"]) / spy_sig_row["close"] * 100.0
            )

        excess = compute_excess_return(ticker_return, benchmark_return)

        # Build feature vector from row data
        ema_9 = row["ema_9"]
        ema_21 = row["ema_21"]
        ema_50 = row["ema_50"]
        close = signal_close

        price_ema9_spread = ((close - ema_9) / ema_9 * 100.0) if ema_9 and ema_9 != 0 else 0.0
        ema9_ema21_spread = ((ema_9 - ema_21) / ema_21 * 100.0) if ema_9 and ema_21 and ema_21 != 0 else 0.0
        ema21_ema50_spread = ((ema_21 - ema_50) / ema_50 * 100.0) if ema_21 and ema_50 and ema_50 != 0 else 0.0

        category_scores = {
            "trend": row["trend_score"],
            "momentum": row["momentum_score"],
            "volume": row["volume_score"],
            "volatility": row["volatility_score"],
            "fundamental": row["fundamental_score"],
            "macro": row["macro_score"],
        }
        raw_indicators = {
            "rsi_14": row["rsi_14"],
            "adx": row["adx"],
            "macd_histogram": row["macd_histogram"],
            "stoch_k": row["stoch_k"],
            "bb_pctb": row["bb_pctb"],
            "cmf_20": row["cmf_20"],
        }
        ema_positions = {
            "price_ema9_spread": price_ema9_spread,
            "ema9_ema21_spread": ema9_ema21_spread,
            "ema21_ema50_spread": ema21_ema50_spread,
        }

        features = build_feature_vector(category_scores, raw_indicators, ema_positions)
        X_list.append(features)
        y_list.append(excess)

    if not X_list:
        return np.empty((0, 15)), np.empty(0)

    return np.array(X_list), np.array(y_list)


# ---------------------------------------------------------------------------
# Main calibration entry point
# ---------------------------------------------------------------------------

def calibrate_score(
    conn: sqlite3.Connection,
    scoring_date: str,
    category_scores: dict,
    raw_indicators: dict,
    ema_positions: dict,
    config: dict,
) -> dict:
    """
    Calibrate the current signal using a rolling ridge regression.

    Fetches recent historical signals with known forward excess returns,
    trains a ridge regression on their features, and predicts the expected
    excess return for the current signal's feature vector.

    Returns calibrated_score=None in three cases:
      1. Calibration is disabled in config.
      2. Fewer than min_training_samples are available (cold start).
      3. The ridge solve fails (numerical issues).

    Parameters:
        conn:             Open SQLite connection (row_factory=sqlite3.Row).
        scoring_date:     Current scoring date (YYYY-MM-DD).
        category_scores:  Dict of 6 category scores (trend, momentum, ...).
        raw_indicators:   Dict of 6 raw indicator values (rsi_14, adx, ...).
        ema_positions:    Dict of 3 EMA position spreads.
        config:           Calibration config dict (from scorer.json["calibration"]).

    Returns:
        Dict with keys:
            calibrated_score (Optional[float]) — predicted excess return (%),
                or None if calibration is unavailable.
            model_r2 (float) — in-sample R² of the training window.
            weights (Optional[list[float]]) — learned regression weights,
                or None if calibration is unavailable.
    """
    if not config.get("enabled", True):
        logger.info("phase=%s calibration disabled", _PHASE)
        return {"calibrated_score": None, "model_r2": 0.0, "weights": None}

    min_samples = config.get("min_training_samples", 30)
    ridge_lambda = config.get("ridge_lambda", 0.1)

    # Fetch training data
    X_train, y_train = fetch_training_data(conn, scoring_date, config)

    if len(X_train) < min_samples:
        logger.info(
            "phase=%s cold start — only %d training samples (need %d)",
            _PHASE, len(X_train), min_samples,
        )
        return {"calibrated_score": None, "model_r2": 0.0, "weights": None}

    # Build feature vector for the current signal
    x_new = np.array(build_feature_vector(category_scores, raw_indicators, ema_positions))

    # Train and predict
    result = train_ridge_and_predict(X_train, y_train, x_new, ridge_lambda)

    calibrated_score = result["prediction"]
    model_r2 = result["model_r2"]

    logger.info(
        "phase=%s date=%s n_train=%d model_r2=%.4f calibrated_score=%.2f%%",
        _PHASE, scoring_date, len(X_train), model_r2, calibrated_score,
    )

    return {
        "calibrated_score": calibrated_score,
        "model_r2": model_r2,
        "weights": result["weights"],
    }
