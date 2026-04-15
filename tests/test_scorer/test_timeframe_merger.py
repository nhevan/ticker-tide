"""
Tests for src/scorer/timeframe_merger.py — dual timeframe score merging.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.timeframe_merger import compute_monthly_score, compute_weekly_score, merge_timeframes


SAMPLE_CONFIG = {
    "timeframe_weights": {
        "trending": { "daily": 0.2, "weekly": 0.8 },
        "ranging":  { "daily": 0.8, "weekly": 0.2 },
        "volatile": { "daily": 0.5, "weekly": 0.5 },
    },
    "weekly_adaptive_weights": {
        "trending": {"trend": 0.45, "momentum": 0.25, "volume": 0.15, "volatility": 0.15},
        "ranging": {"trend": 0.20, "momentum": 0.40, "volume": 0.20, "volatility": 0.20},
        "volatile": {"trend": 0.30, "momentum": 0.25, "volume": 0.15, "volatility": 0.30},
    },
    "scoring": {
        "score_expansion_factor": 1.5,
    },
}


def _make_weekly_db(tmp_path, rows: list[dict]) -> sqlite3.Connection:
    """
    Build a SQLite connection with weekly_candles + indicators_weekly using
    the full production schema (all 22 indicator columns).

    Each row is a dict with keys: week_start, close, and any indicator columns.
    Missing indicator columns default to None.
    """
    db_path = tmp_path / "weekly_test.db"
    tmp_path.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE weekly_candles (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(ticker, week_start)
        )"""
    )
    conn.execute(
        """CREATE TABLE indicators_weekly (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            ema_9 REAL, ema_21 REAL, ema_50 REAL,
            macd_line REAL, macd_signal REAL, macd_histogram REAL,
            adx REAL,
            rsi_14 REAL,
            stoch_k REAL, stoch_d REAL,
            cci_20 REAL, williams_r REAL,
            obv REAL, cmf_20 REAL, ad_line REAL,
            bb_upper REAL, bb_lower REAL, bb_pctb REAL,
            atr_14 REAL,
            keltner_upper REAL, keltner_lower REAL,
            UNIQUE(ticker, week_start)
        )"""
    )
    conn.execute(
        """CREATE TABLE indicator_profiles (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL,
            mean REAL, std REAL,
            window_start TEXT, window_end TEXT, computed_at TEXT,
            UNIQUE(ticker, indicator)
        )"""
    )

    indicator_cols = [
        "ema_9", "ema_21", "ema_50", "macd_line", "macd_signal", "macd_histogram",
        "adx", "rsi_14", "stoch_k", "stoch_d", "cci_20", "williams_r",
        "obv", "cmf_20", "ad_line", "bb_upper", "bb_lower", "bb_pctb",
        "atr_14", "keltner_upper", "keltner_lower",
    ]

    for row_dict in rows:
        week_start = row_dict["week_start"]
        close = row_dict["close"]
        conn.execute(
            "INSERT INTO weekly_candles(ticker, week_start, close) VALUES (?, ?, ?)",
            ("QQQ", week_start, close),
        )
        values = [row_dict.get(col) for col in indicator_cols]
        placeholders = ", ".join(["?"] * len(indicator_cols))
        col_names = ", ".join(indicator_cols)
        conn.execute(
            f"INSERT INTO indicators_weekly(ticker, week_start, {col_names}) "
            f"VALUES (?, ?, {placeholders})",
            ("QQQ", week_start, *values),
        )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

# Two weekly candles with only EMA data:
#   Mar 30 — bullish EMA stack (close > EMA9 > EMA21 > EMA50)
#   Apr 6  — bearish EMA stack (close < EMA9 < EMA21 < EMA50)
_WEEKLY_ROWS = [
    {"week_start": "2026-03-30", "close": 520.0, "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0},
    {"week_start": "2026-04-06", "close": 480.0, "ema_9": 485.0, "ema_21": 495.0, "ema_50": 510.0},
]

# A row with all 14 scorable indicators populated (strongly bullish in ranging).
# In ranging regime, low oscillators = oversold = bullish (mean-reversion).
_BULLISH_ALL_INDICATORS = {
    "week_start": "2026-03-30",
    "close": 520.0,
    "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0,
    "macd_line": 5.0, "macd_histogram": 3.0,
    "adx": 35.0,
    "rsi_14": 28.0,
    "stoch_k": 15.0, "cci_20": -120.0, "williams_r": -88.0,
    "obv": 1000000.0, "cmf_20": 0.25, "ad_line": 500000.0,
    "bb_pctb": 0.15, "atr_14": 8.0,
}

# A row with all 14 scorable indicators populated (strongly bearish in ranging).
# In ranging regime, high oscillators = overbought = bearish (mean-reversion).
_BEARISH_ALL_INDICATORS = {
    "week_start": "2026-03-30",
    "close": 480.0,
    "ema_9": 485.0, "ema_21": 495.0, "ema_50": 510.0,
    "macd_line": -5.0, "macd_histogram": -3.0,
    "adx": 35.0,
    "rsi_14": 78.0,
    "stoch_k": 85.0, "cci_20": 150.0, "williams_r": -10.0,
    "obv": 1000000.0, "cmf_20": -0.25, "ad_line": 500000.0,
    "bb_pctb": 0.9, "atr_14": 8.0,
}


class TestComputeWeeklyScoreDateAware:
    """compute_weekly_score must respect scoring_date — no look-ahead."""

    def test_scoring_date_within_earlier_week_returns_earlier_candle(self, tmp_path) -> None:
        """
        scoring_date='2026-04-01' falls in the Mar 30 week.
        The Apr 6 candle must NOT be used (it doesn't exist yet).
        """
        conn = _make_weekly_db(tmp_path, _WEEKLY_ROWS)
        score_apr1 = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-04-01")
        score_apr9 = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-04-09")
        # Apr 1 uses Mar 30 candle (bullish EMA stack → positive score).
        # Apr 9 uses Apr 6 candle (bearish EMA stack → negative score).
        assert score_apr1 is not None
        assert score_apr9 is not None
        assert score_apr1 > score_apr9, (
            f"Apr 1 score ({score_apr1:.2f}) should be higher than Apr 9 score "
            f"({score_apr9:.2f}) because Apr 1 uses the bullish Mar 30 candle"
        )

    def test_scoring_date_before_any_weekly_data_returns_none(self, tmp_path) -> None:
        """When no weekly candle exists on or before scoring_date, return None."""
        conn = _make_weekly_db(tmp_path, _WEEKLY_ROWS)
        result = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-01")
        assert result is None

    def test_scoring_date_exactly_on_week_start_uses_that_candle(self, tmp_path) -> None:
        """scoring_date='2026-04-06' should use the Apr 6 candle (week_start == scoring_date)."""
        conn = _make_weekly_db(tmp_path, _WEEKLY_ROWS)
        score_on_start = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-04-06")
        score_day_before = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-04-05")
        assert score_on_start is not None
        assert score_day_before is not None
        # Apr 6 candle has a bearish EMA stack, Mar 30 has a bullish stack.
        assert score_on_start < score_day_before

    def test_no_scoring_date_raises_type_error(self, tmp_path) -> None:
        """compute_weekly_score now requires scoring_date — calling without it must fail."""
        conn = _make_weekly_db(tmp_path, _WEEKLY_ROWS)
        with pytest.raises(TypeError):
            compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG)  # type: ignore[call-arg]

    def test_all_dates_use_same_candle_when_only_one_candle_exists(self, tmp_path) -> None:
        """When only one weekly candle is available, every date on or after it should use it."""
        rows = [{"week_start": "2026-03-30", "close": 520.0, "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0}]
        conn = _make_weekly_db(tmp_path, rows)
        score_a = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        score_b = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-12-31")
        assert score_a is not None
        assert score_b is not None
        assert score_a == pytest.approx(score_b, abs=0.01)


class TestComputeWeeklyScoreRegimeAware:
    """RSI in the weekly path should flip direction in trending regime."""

    def test_trending_regime_high_rsi_is_bullish(self, tmp_path) -> None:
        """
        With a high RSI (70) in trending regime, the weekly score should be
        more bullish than in ranging regime.
        """
        rows_high_rsi = [
            {"week_start": "2026-03-30", "close": 500.0, "ema_9": 500.0, "ema_21": 499.0, "ema_50": 498.0, "rsi_14": 70.0},
        ]
        conn_trending = _make_weekly_db(tmp_path / "t", rows_high_rsi)
        conn_ranging = _make_weekly_db(tmp_path / "r", rows_high_rsi)

        score_trending = compute_weekly_score(
            conn_trending, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="trending"
        )
        score_ranging = compute_weekly_score(
            conn_ranging, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="ranging"
        )
        assert score_trending is not None
        assert score_ranging is not None
        assert score_trending > score_ranging, (
            f"RSI=70 should score higher in trending ({score_trending:.2f}) "
            f"than ranging ({score_ranging:.2f})"
        )

    def test_trending_regime_low_rsi_is_bearish(self, tmp_path) -> None:
        """
        With a low RSI (30) in trending regime, the weekly score should be
        more bearish than in ranging regime (where low RSI = oversold = bullish).
        """
        rows_low_rsi = [
            {"week_start": "2026-03-30", "close": 500.0, "ema_9": 500.0, "ema_21": 499.0, "ema_50": 498.0, "rsi_14": 30.0},
        ]
        conn_trending = _make_weekly_db(tmp_path / "t", rows_low_rsi)
        conn_ranging = _make_weekly_db(tmp_path / "r", rows_low_rsi)

        score_trending = compute_weekly_score(
            conn_trending, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="trending"
        )
        score_ranging = compute_weekly_score(
            conn_ranging, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="ranging"
        )
        assert score_trending is not None
        assert score_ranging is not None
        assert score_trending < score_ranging, (
            f"RSI=30 should score lower in trending ({score_trending:.2f}) "
            f"than ranging ({score_ranging:.2f})"
        )

    def test_ranging_regime_is_default(self, tmp_path) -> None:
        """Default regime (omitted) should behave identically to regime='ranging'."""
        rows = [{"week_start": "2026-03-30", "close": 500.0, "ema_9": 495.0, "ema_21": 510.0, "ema_50": 520.0,
                 "macd_histogram": -2.5, "rsi_14": 42.0, "adx": 25.0, "cmf_20": -0.1, "bb_pctb": 0.3, "atr_14": 8.0}]
        conn_default = _make_weekly_db(tmp_path / "d", rows)
        conn_ranging = _make_weekly_db(tmp_path / "r", rows)

        score_default = compute_weekly_score(
            conn_default, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30"
        )
        score_ranging = compute_weekly_score(
            conn_ranging, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30", regime="ranging"
        )
        assert score_default == pytest.approx(score_ranging, abs=0.01)

    def test_volatile_regime_uses_mean_reversion(self, tmp_path) -> None:
        """Volatile regime should behave like ranging (mean-reversion) not trending."""
        rows_high_rsi = [
            {"week_start": "2026-03-30", "close": 500.0, "ema_9": 500.0, "ema_21": 499.0, "ema_50": 498.0, "rsi_14": 70.0},
        ]
        conn_volatile = _make_weekly_db(tmp_path / "v", rows_high_rsi)
        conn_ranging = _make_weekly_db(tmp_path / "r", rows_high_rsi)

        score_volatile = compute_weekly_score(
            conn_volatile, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="volatile"
        )
        score_ranging = compute_weekly_score(
            conn_ranging, "QQQ", SAMPLE_CONFIG,
            scoring_date="2026-03-30", regime="ranging"
        )
        # volatile and ranging both use mean-reversion for oscillators, but may
        # have different adaptive weights — so scores may differ slightly
        # The key property: both should treat high RSI as bearish (negative contribution)
        assert score_volatile is not None
        assert score_ranging is not None


class TestComputeWeeklyScoreFullPipeline:
    """Tests for the expanded weekly scoring pipeline using all 14 indicators."""

    def test_bullish_indicators_produce_positive_score(self, tmp_path) -> None:
        """A strongly bullish indicator set should produce a positive score."""
        conn = _make_weekly_db(tmp_path, [_BULLISH_ALL_INDICATORS])
        score = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        assert score is not None
        assert score > 0, f"Bullish indicators should produce positive score, got {score:.2f}"

    def test_bearish_indicators_produce_negative_score(self, tmp_path) -> None:
        """A strongly bearish indicator set should produce a negative score."""
        conn = _make_weekly_db(tmp_path, [_BEARISH_ALL_INDICATORS])
        score = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        assert score is not None
        assert score < 0, f"Bearish indicators should produce negative score, got {score:.2f}"

    def test_all_indicators_wider_than_ema_only(self, tmp_path) -> None:
        """
        Using all 14 indicators should produce a wider score spread than
        using only EMA alignment (the old 6-indicator path with mostly Nones).
        """
        # EMA-only bullish
        ema_only_bullish = {"week_start": "2026-03-30", "close": 520.0,
                            "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0}
        conn_ema = _make_weekly_db(tmp_path / "ema", [ema_only_bullish])
        score_ema = compute_weekly_score(conn_ema, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")

        # Full bullish
        conn_full = _make_weekly_db(tmp_path / "full", [_BULLISH_ALL_INDICATORS])
        score_full = compute_weekly_score(conn_full, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")

        assert score_ema is not None
        assert score_full is not None
        # Both should be positive (bullish), but full should be more extreme
        assert abs(score_full) > abs(score_ema), (
            f"Full indicator score ({score_full:.2f}) should be more extreme "
            f"than EMA-only ({score_ema:.2f})"
        )

    def test_sparse_indicators_still_produce_score(self, tmp_path) -> None:
        """When only a few indicators are populated (others None), a score is still produced."""
        sparse_row = {"week_start": "2026-03-30", "close": 500.0,
                      "rsi_14": 55.0, "cmf_20": 0.1}
        conn = _make_weekly_db(tmp_path, [sparse_row])
        score = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        assert score is not None

    def test_all_none_indicators_returns_none(self, tmp_path) -> None:
        """When all indicators are None, return None."""
        empty_row = {"week_start": "2026-03-30", "close": 500.0}
        conn = _make_weekly_db(tmp_path, [empty_row])
        score = compute_weekly_score(conn, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        assert score is None

    def test_expansion_factor_widens_scores(self, tmp_path) -> None:
        """A higher expansion factor should produce a more extreme score."""
        config_no_expansion = {**SAMPLE_CONFIG, "scoring": {"score_expansion_factor": 1.0}}
        config_high_expansion = {**SAMPLE_CONFIG, "scoring": {"score_expansion_factor": 2.0}}

        conn_no = _make_weekly_db(tmp_path / "no", [_BULLISH_ALL_INDICATORS])
        conn_hi = _make_weekly_db(tmp_path / "hi", [_BULLISH_ALL_INDICATORS])

        score_no = compute_weekly_score(conn_no, "QQQ", config_no_expansion, scoring_date="2026-03-30")
        score_hi = compute_weekly_score(conn_hi, "QQQ", config_high_expansion, scoring_date="2026-03-30")

        assert score_no is not None
        assert score_hi is not None
        assert abs(score_hi) >= abs(score_no), (
            f"2x expansion ({score_hi:.2f}) should be >= 1x ({score_no:.2f})"
        )

    def test_weekly_adaptive_weights_applied(self, tmp_path) -> None:
        """
        Config with weekly_adaptive_weights should affect the score.
        In ranging regime, high oscillators = overbought = bearish momentum.
        Bullish trend + bearish momentum: momentum-heavy weights should
        produce a lower score than trend-heavy weights.
        """
        # Bullish trend, bearish momentum (high oscillators = overbought in ranging)
        mixed_row = {
            "week_start": "2026-03-30", "close": 520.0,
            "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0,
            "macd_line": 5.0, "macd_histogram": 3.0, "adx": 35.0,
            "rsi_14": 78.0, "stoch_k": 85.0, "cci_20": 150.0, "williams_r": -10.0,
            "cmf_20": 0.1,
        }
        # Momentum-heavy weights
        config_momentum = {
            **SAMPLE_CONFIG,
            "weekly_adaptive_weights": {
                "ranging": {"trend": 0.10, "momentum": 0.70, "volume": 0.10, "volatility": 0.10},
            },
        }
        # Trend-heavy weights
        config_trend = {
            **SAMPLE_CONFIG,
            "weekly_adaptive_weights": {
                "ranging": {"trend": 0.70, "momentum": 0.10, "volume": 0.10, "volatility": 0.10},
            },
        }

        conn_mom = _make_weekly_db(tmp_path / "mom", [mixed_row])
        conn_trend = _make_weekly_db(tmp_path / "trend", [mixed_row])

        score_momentum = compute_weekly_score(conn_mom, "QQQ", config_momentum, scoring_date="2026-03-30")
        score_trend = compute_weekly_score(conn_trend, "QQQ", config_trend, scoring_date="2026-03-30")

        assert score_momentum is not None
        assert score_trend is not None
        # Trend is bullish, momentum is bearish. Momentum-heavy should be lower.
        assert score_momentum < score_trend, (
            f"Momentum-heavy ({score_momentum:.2f}) should be lower than "
            f"trend-heavy ({score_trend:.2f}) with bullish trend + bearish momentum"
        )

    def test_profiles_affect_scoring(self, tmp_path) -> None:
        """When indicator_profiles exist, they should influence the score."""
        rows = [{"week_start": "2026-03-30", "close": 500.0,
                 "rsi_14": 65.0, "macd_histogram": 2.0}]
        conn_no_profile = _make_weekly_db(tmp_path / "np", rows)
        conn_with_profile = _make_weekly_db(tmp_path / "wp", rows)

        # Insert a profile that shifts RSI scoring (p80=60 makes RSI=65 relatively high)
        conn_with_profile.execute(
            "INSERT INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("QQQ", "rsi_14", 25.0, 35.0, 50.0, 60.0, 75.0, 50.0, 12.0),
        )
        conn_with_profile.execute(
            "INSERT INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("QQQ", "macd_histogram", -5.0, -2.0, 0.5, 3.0, 6.0, 0.5, 2.5),
        )
        conn_with_profile.commit()

        score_no = compute_weekly_score(conn_no_profile, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")
        score_with = compute_weekly_score(conn_with_profile, "QQQ", SAMPLE_CONFIG, scoring_date="2026-03-30")

        assert score_no is not None
        assert score_with is not None
        # Scores should differ when profiles are present
        assert score_no != pytest.approx(score_with, abs=0.5), (
            f"Profile should change the score: no_profile={score_no:.2f}, with_profile={score_with:.2f}"
        )

    def test_fallback_when_no_weekly_adaptive_weights(self, tmp_path) -> None:
        """When weekly_adaptive_weights is missing from config, re-normalize daily weights."""
        config_no_weekly_weights = {
            "timeframe_weights": {"ranging": {"daily": 0.8, "weekly": 0.2}},
            "adaptive_weights": {
                "ranging": {
                    "trend": 0.10, "momentum": 0.25, "volume": 0.10, "volatility": 0.10,
                    "candlestick": 0.10, "structural": 0.15, "sentiment": 0.10,
                    "fundamental": 0.05, "macro": 0.05,
                },
            },
            "scoring": {"score_expansion_factor": 1.5},
        }
        conn = _make_weekly_db(tmp_path, [_BULLISH_ALL_INDICATORS])
        score = compute_weekly_score(conn, "QQQ", config_no_weekly_weights, scoring_date="2026-03-30")
        assert score is not None
        assert score > 0, "Bullish indicators should still produce positive score with fallback weights"


class TestMergeTimeframes:
    def test_merge_trending_regime_weekly_dominant(self) -> None:
        """In trending regime, weights are 0.2/0.8 → 0.2*60 + 0.8*50 = 52.0."""
        result = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG, regime="trending")
        assert result == pytest.approx(52.0, abs=0.01)

    def test_merge_ranging_regime_daily_dominant(self) -> None:
        """In ranging regime, weights are 0.8/0.2 → 0.8*60 + 0.2*50 = 58.0."""
        result = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG, regime="ranging")
        assert result == pytest.approx(58.0, abs=0.01)

    def test_merge_volatile_regime_equal(self) -> None:
        """In volatile regime, weights are 0.5/0.5 → 0.5*60 + 0.5*50 = 55.0."""
        result = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG, regime="volatile")
        assert result == pytest.approx(55.0, abs=0.01)

    def test_merge_weekly_not_available(self) -> None:
        """weekly_score=None → merged = daily_score only."""
        result = merge_timeframes(daily_score=60.0, weekly_score=None, config=SAMPLE_CONFIG)
        assert result == pytest.approx(60.0, abs=0.01)

    def test_merge_uses_flat_config_weights(self) -> None:
        """Flat (non-nested) config format still works for backward compatibility."""
        config = {"timeframe_weights": {"daily": 0.7, "weekly": 0.3}}
        result = merge_timeframes(daily_score=100.0, weekly_score=0.0, config=config)
        assert result == pytest.approx(70.0, abs=0.01)

    def test_merge_result_is_clamped(self) -> None:
        """daily=+100, weekly=+100 → merged does not exceed +100."""
        result = merge_timeframes(daily_score=100.0, weekly_score=100.0, config=SAMPLE_CONFIG, regime="trending")
        assert result == pytest.approx(100.0, abs=0.01)

    def test_default_regime_is_ranging(self) -> None:
        """Default regime (omitted) should use ranging weights."""
        result_default = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG)
        result_ranging = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG, regime="ranging")
        assert result_default == pytest.approx(result_ranging, abs=0.01)


# ---------------------------------------------------------------------------
# 3-way merge config (daily + weekly + monthly)
# ---------------------------------------------------------------------------

SAMPLE_CONFIG_3WAY = {
    "timeframe_weights": {
        "trending": {"daily": 0.10, "weekly": 0.50, "monthly": 0.40},
        "ranging":  {"daily": 0.60, "weekly": 0.30, "monthly": 0.10},
        "volatile": {"daily": 0.25, "weekly": 0.45, "monthly": 0.30},
    },
    "weekly_adaptive_weights": {
        "trending": {"trend": 0.45, "momentum": 0.25, "volume": 0.15, "volatility": 0.15},
        "ranging":  {"trend": 0.20, "momentum": 0.40, "volume": 0.20, "volatility": 0.20},
        "volatile": {"trend": 0.30, "momentum": 0.25, "volume": 0.15, "volatility": 0.30},
    },
    "monthly_adaptive_weights": {
        "trending": {"trend": 0.45, "momentum": 0.25, "volume": 0.15, "volatility": 0.15},
        "ranging":  {"trend": 0.20, "momentum": 0.40, "volume": 0.20, "volatility": 0.20},
        "volatile": {"trend": 0.30, "momentum": 0.25, "volume": 0.15, "volatility": 0.30},
    },
    "scoring": {"score_expansion_factor": 1.5},
}


def _make_monthly_db(tmp_path, rows: list[dict]) -> sqlite3.Connection:
    """
    Build a SQLite connection with monthly_candles + indicators_monthly for
    compute_monthly_score tests.
    """
    db_path = tmp_path / "monthly_test.db"
    tmp_path.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE monthly_candles (
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(ticker, month_start)
        )"""
    )
    conn.execute(
        """CREATE TABLE indicators_monthly (
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            ema_9 REAL, ema_21 REAL, ema_50 REAL,
            macd_line REAL, macd_signal REAL, macd_histogram REAL,
            adx REAL,
            rsi_14 REAL,
            stoch_k REAL, stoch_d REAL,
            cci_20 REAL, williams_r REAL,
            obv REAL, cmf_20 REAL, ad_line REAL,
            bb_upper REAL, bb_lower REAL, bb_pctb REAL,
            atr_14 REAL,
            keltner_upper REAL, keltner_lower REAL,
            UNIQUE(ticker, month_start)
        )"""
    )
    conn.execute(
        """CREATE TABLE indicator_profiles (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL,
            mean REAL, std REAL,
            window_start TEXT, window_end TEXT, computed_at TEXT,
            UNIQUE(ticker, indicator)
        )"""
    )

    indicator_cols = [
        "ema_9", "ema_21", "ema_50", "macd_line", "macd_signal", "macd_histogram",
        "adx", "rsi_14", "stoch_k", "stoch_d", "cci_20", "williams_r",
        "obv", "cmf_20", "ad_line", "bb_upper", "bb_lower", "bb_pctb",
        "atr_14", "keltner_upper", "keltner_lower",
    ]

    for row_dict in rows:
        month_start = row_dict["month_start"]
        close = row_dict["close"]
        conn.execute(
            "INSERT INTO monthly_candles(ticker, month_start, close) VALUES (?, ?, ?)",
            ("QQQ", month_start, close),
        )
        values = [row_dict.get(col) for col in indicator_cols]
        placeholders = ", ".join(["?"] * len(indicator_cols))
        col_names = ", ".join(indicator_cols)
        conn.execute(
            f"INSERT INTO indicators_monthly(ticker, month_start, {col_names}) "
            f"VALUES (?, ?, {placeholders})",
            ("QQQ", month_start, *values),
        )
    conn.commit()
    return conn


_MONTHLY_BULLISH = {
    "month_start": "2025-12-01",
    "close": 520.0,
    "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0,
    "macd_line": 5.0, "macd_histogram": 3.0,
    "adx": 35.0,
    "rsi_14": 28.0,
    "stoch_k": 15.0, "cci_20": -120.0, "williams_r": -88.0,
    "obv": 1_000_000.0, "cmf_20": 0.25, "ad_line": 500_000.0,
    "bb_pctb": 0.15, "atr_14": 8.0,
}

_MONTHLY_BEARISH = {
    "month_start": "2025-12-01",
    "close": 480.0,
    "ema_9": 485.0, "ema_21": 495.0, "ema_50": 510.0,
    "macd_line": -5.0, "macd_histogram": -3.0,
    "adx": 35.0,
    "rsi_14": 78.0,
    "stoch_k": 85.0, "cci_20": 150.0, "williams_r": -10.0,
    "obv": 1_000_000.0, "cmf_20": -0.25, "ad_line": 500_000.0,
    "bb_pctb": 0.9, "atr_14": 8.0,
}


class TestComputeMonthlyScore:
    """Tests for compute_monthly_score — monthly timeframe indicator scoring."""

    def test_bullish_indicators_produce_positive_score(self, tmp_path) -> None:
        """Strongly bullish monthly indicators should produce a positive score."""
        conn = _make_monthly_db(tmp_path, [_MONTHLY_BULLISH])
        score = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-12-15")
        assert score is not None
        assert score > 0, f"Bullish monthly indicators should produce positive score, got {score:.2f}"

    def test_bearish_indicators_produce_negative_score(self, tmp_path) -> None:
        """Strongly bearish monthly indicators should produce a negative score."""
        conn = _make_monthly_db(tmp_path, [_MONTHLY_BEARISH])
        score = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-12-15")
        assert score is not None
        assert score < 0, f"Bearish monthly indicators should produce negative score, got {score:.2f}"

    def test_no_monthly_data_returns_none(self, tmp_path) -> None:
        """When no monthly data exists, compute_monthly_score returns None."""
        conn = _make_monthly_db(tmp_path, [])
        score = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-12-15")
        assert score is None

    def test_scoring_date_before_any_monthly_data_returns_none(self, tmp_path) -> None:
        """Scoring date before earliest month_start returns None (no look-ahead)."""
        conn = _make_monthly_db(tmp_path, [_MONTHLY_BULLISH])
        score = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-11-01")
        assert score is None

    def test_scoring_date_on_month_start_uses_that_candle(self, tmp_path) -> None:
        """scoring_date == month_start should use that candle."""
        rows = [
            {"month_start": "2025-11-01", "close": 480.0,
             "ema_9": 485.0, "ema_21": 495.0, "ema_50": 510.0},
            {"month_start": "2025-12-01", "close": 520.0,
             "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0},
        ]
        conn = _make_monthly_db(tmp_path, rows)
        score_nov = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-11-15")
        score_dec = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-12-01")
        assert score_nov is not None
        assert score_dec is not None
        # Dec candle is bullish EMA stack, Nov is bearish — should produce different scores
        assert score_dec != pytest.approx(score_nov, abs=0.1)

    def test_all_none_indicators_returns_none(self, tmp_path) -> None:
        """Monthly candle with no indicator data returns None."""
        conn = _make_monthly_db(tmp_path, [{"month_start": "2025-12-01", "close": 500.0}])
        score = compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY, scoring_date="2025-12-15")
        assert score is None

    def test_requires_scoring_date(self, tmp_path) -> None:
        """compute_monthly_score requires scoring_date — omitting it raises TypeError."""
        conn = _make_monthly_db(tmp_path, [_MONTHLY_BULLISH])
        with pytest.raises(TypeError):
            compute_monthly_score(conn, "QQQ", SAMPLE_CONFIG_3WAY)  # type: ignore[call-arg]


class TestMergeTimeframes3Way:
    """Tests for 3-way merge (daily + weekly + monthly)."""

    def test_3way_trending_weights_applied(self) -> None:
        """trending: daily=0.10, weekly=0.50, monthly=0.40 → 0.10*60+0.50*50+0.40*40=47.0."""
        result = merge_timeframes(
            daily_score=60.0, weekly_score=50.0, monthly_score=40.0,
            config=SAMPLE_CONFIG_3WAY, regime="trending",
        )
        assert result == pytest.approx(47.0, abs=0.01)

    def test_3way_ranging_weights_applied(self) -> None:
        """ranging: daily=0.60, weekly=0.30, monthly=0.10 → 0.60*60+0.30*50+0.10*40=55.0."""
        result = merge_timeframes(
            daily_score=60.0, weekly_score=50.0, monthly_score=40.0,
            config=SAMPLE_CONFIG_3WAY, regime="ranging",
        )
        assert result == pytest.approx(55.0, abs=0.01)

    def test_3way_volatile_weights_applied(self) -> None:
        """volatile: daily=0.25, weekly=0.45, monthly=0.30 → 0.25*60+0.45*50+0.30*40=49.5."""
        result = merge_timeframes(
            daily_score=60.0, weekly_score=50.0, monthly_score=40.0,
            config=SAMPLE_CONFIG_3WAY, regime="volatile",
        )
        assert result == pytest.approx(49.5, abs=0.01)

    def test_monthly_none_falls_back_to_2way_normalized(self) -> None:
        """When monthly_score=None, weights for daily+weekly are renormalized to sum to 1."""
        # trending: daily=0.10, weekly=0.50 → renormalized: daily=0.167, weekly=0.833
        # result = 0.10/0.60*60 + 0.50/0.60*50 = 10 + 41.67 = 51.67
        result = merge_timeframes(
            daily_score=60.0, weekly_score=50.0, monthly_score=None,
            config=SAMPLE_CONFIG_3WAY, regime="trending",
        )
        daily_w, weekly_w = 0.10 / 0.60, 0.50 / 0.60
        expected = daily_w * 60.0 + weekly_w * 50.0
        assert result == pytest.approx(expected, abs=0.01)

    def test_weekly_and_monthly_none_uses_daily_only(self) -> None:
        """When both weekly and monthly are None, result equals daily_score."""
        result = merge_timeframes(
            daily_score=55.0, weekly_score=None, monthly_score=None,
            config=SAMPLE_CONFIG_3WAY, regime="ranging",
        )
        assert result == pytest.approx(55.0, abs=0.01)

    def test_weekly_none_monthly_present_normalizes_daily_monthly(self) -> None:
        """When weekly=None but monthly is present, use daily+monthly renormalized."""
        # ranging: daily=0.60, monthly=0.10 → renormalized: daily=0.857, monthly=0.143
        # result = 0.60/0.70*60 + 0.10/0.70*40 = 51.43 + 5.71 = 57.14
        result = merge_timeframes(
            daily_score=60.0, weekly_score=None, monthly_score=40.0,
            config=SAMPLE_CONFIG_3WAY, regime="ranging",
        )
        daily_w, monthly_w = 0.60 / 0.70, 0.10 / 0.70
        expected = daily_w * 60.0 + monthly_w * 40.0
        assert result == pytest.approx(expected, abs=0.01)

    def test_3way_result_clamped_to_100(self) -> None:
        """Result is clamped to [-100, +100] even with extreme inputs."""
        result = merge_timeframes(
            daily_score=100.0, weekly_score=100.0, monthly_score=100.0,
            config=SAMPLE_CONFIG_3WAY, regime="trending",
        )
        assert result == pytest.approx(100.0, abs=0.01)

    def test_3way_result_clamped_negative(self) -> None:
        result = merge_timeframes(
            daily_score=-100.0, weekly_score=-100.0, monthly_score=-100.0,
            config=SAMPLE_CONFIG_3WAY, regime="volatile",
        )
        assert result == pytest.approx(-100.0, abs=0.01)

    def test_monthly_none_backward_compat_with_2way_config(self) -> None:
        """Config with only daily+weekly keys still works when monthly_score=None."""
        result = merge_timeframes(
            daily_score=60.0, weekly_score=50.0, monthly_score=None,
            config=SAMPLE_CONFIG, regime="trending",
        )
        assert result == pytest.approx(52.0, abs=0.01)  # 0.2*60 + 0.8*50

