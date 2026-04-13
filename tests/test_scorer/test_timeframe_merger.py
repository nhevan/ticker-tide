"""
Tests for src/scorer/timeframe_merger.py — dual timeframe score merging.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.timeframe_merger import compute_weekly_score, merge_timeframes


SAMPLE_CONFIG = {
    "timeframe_weights": {
        "daily": 0.6,
        "weekly": 0.4,
    }
}


def _make_weekly_db(tmp_path, rows: list[tuple]) -> sqlite3.Connection:
    """
    Build an in-memory-ish SQLite connection with weekly_candles + indicators_weekly.

    rows: list of (week_start, close, ema_9, ema_21, ema_50, macd_histogram, rsi_14,
                   adx, cmf_20, bb_pctb, atr_14)
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
            macd_histogram REAL, rsi_14 REAL,
            adx REAL, cmf_20 REAL, bb_pctb REAL, atr_14 REAL,
            UNIQUE(ticker, week_start)
        )"""
    )
    for (week_start, close, ema_9, ema_21, ema_50, macd_hist, rsi, adx, cmf, bb_pctb, atr) in rows:
        conn.execute(
            "INSERT INTO weekly_candles(ticker, week_start, close) VALUES (?, ?, ?)",
            ("QQQ", week_start, close),
        )
        conn.execute(
            "INSERT INTO indicators_weekly(ticker, week_start, ema_9, ema_21, ema_50, "
            "macd_histogram, rsi_14, adx, cmf_20, bb_pctb, atr_14) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("QQQ", week_start, ema_9, ema_21, ema_50, macd_hist, rsi, adx, cmf, bb_pctb, atr),
        )
    conn.commit()
    return conn


# Two weekly candles:
#   Mar 30 — bullish EMA stack (close > EMA9 > EMA21 > EMA50)
#   Apr 6  — bearish EMA stack (close < EMA9 < EMA21 < EMA50)
# The ordering is unambiguous: score(Mar 30) > 0 > score(Apr 6)
_WEEKLY_ROWS = [
    # (week_start,   close, ema_9, ema_21, ema_50, macd_hist, rsi,  adx,  cmf,   bb_pctb, atr)
    ("2026-03-30", 520.0, 515.0, 505.0, 490.0,  None,      None, None, None,  None,    None),
    ("2026-04-06", 480.0, 485.0, 495.0, 510.0,  None,      None, None, None,  None,    None),
]


class TestComputeWeeklyScoreDateAware:
    """compute_weekly_score must respect scoring_date — no look-ahead."""

    def test_scoring_date_within_earlier_week_returns_earlier_candle(self, tmp_path) -> None:
        """
        scoring_date='2026-04-01' falls in the Mar 30 week.
        The Apr 6 candle must NOT be used (it doesn't exist yet).
        """
        conn = _make_weekly_db(tmp_path, _WEEKLY_ROWS)
        # Apr 6 candle has rsi=38 (more bearish); Mar 30 has rsi=42.
        # If the date filter works, we get the Mar 30 candle.
        # Verify by inserting a sentinel with a wildly different RSI into Mar 30 row only.
        # Simplest: just assert the function returns *some* result without error
        # and does not use the Apr 6 candle (which would produce a lower score).
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
        rows = [("2026-03-30", 520.0, 515.0, 505.0, 490.0, None, None, None, None, None, None)]
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
        # Neutral/flat EMA alignment, high RSI, no MACD/ADX/CMF/BB to avoid noise.
        rows_high_rsi = [
            ("2026-03-30", 500.0, 500.0, 499.0, 498.0, None, 70.0, None, None, None, None),
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
            ("2026-03-30", 500.0, 500.0, 499.0, 498.0, None, 30.0, None, None, None, None),
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
        rows = [("2026-03-30", 500.0, 495.0, 510.0, 520.0, -2.5, 42.0, 25.0, -0.1, 0.3, 8.0)]
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
            ("2026-03-30", 500.0, 500.0, 499.0, 498.0, None, 70.0, None, None, None, None),
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
        assert score_volatile == pytest.approx(score_ranging, abs=0.01)


class TestMergeTimeframes:
    def test_merge_daily_weekly_same_direction(self) -> None:
        """daily=+60, weekly=+50, weights 0.6/0.4 → 0.6*60 + 0.4*50 = 56.0."""
        result = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(56.0, abs=0.01)

    def test_merge_daily_weekly_opposite_direction(self) -> None:
        """daily=+60, weekly=-40 → 0.6*60 + 0.4*(-40) = 20.0 (conflict → closer to neutral)."""
        result = merge_timeframes(daily_score=60.0, weekly_score=-40.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(20.0, abs=0.01)

    def test_merge_weekly_not_available(self) -> None:
        """weekly_score=None → merged = daily_score only."""
        result = merge_timeframes(daily_score=60.0, weekly_score=None, config=SAMPLE_CONFIG)
        assert result == pytest.approx(60.0, abs=0.01)

    def test_merge_uses_config_weights(self) -> None:
        """daily=+100, weekly=0, custom weights daily=0.7/weekly=0.3 → 70.0."""
        config = {"timeframe_weights": {"daily": 0.7, "weekly": 0.3}}
        result = merge_timeframes(daily_score=100.0, weekly_score=0.0, config=config)
        assert result == pytest.approx(70.0, abs=0.01)

    def test_merge_result_is_clamped(self) -> None:
        """daily=+100, weekly=+100 → merged does not exceed +100."""
        result = merge_timeframes(daily_score=100.0, weekly_score=100.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(100.0, abs=0.01)
