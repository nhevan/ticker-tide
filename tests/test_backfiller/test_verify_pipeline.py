"""Tests for src/backfiller/verify_pipeline.py.

TDD: tests written before implementation.
All external API calls and config loading are mocked.
Covers indicator, score, pattern, divergence, crossover, profile,
weekly candle, news summary, cross-table, signal flip, and aggregate
health checks.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from src.backfiller.verify_pipeline import (
    INDICATOR_RANGES,
    check_category_score_ranges,
    check_confidence_distribution,
    check_confidence_range,
    check_crossover_validity,
    check_divergence_consistency,
    check_divergence_counts,
    check_indicator_coverage,
    check_indicator_date_alignment,
    check_indicator_null_percentage,
    check_indicator_ranges,
    check_indicators_have_ohlcv,
    check_json_fields,
    check_news_summary_consistency,
    check_pattern_counts,
    check_pattern_duplicates,
    check_pattern_field_validity,
    check_profile_coverage,
    check_profile_freshness,
    check_profile_percentile_order,
    check_regime_values,
    check_score_ranges,
    check_scores_have_indicators,
    check_signal_distribution,
    check_signal_flip_validity,
    check_signal_score_consistency,
    check_sr_levels_within_range,
    check_weighted_score_math,
    check_weekly_candle_validity,
    check_weekly_indicator_coverage,
    format_pipeline_verification_report,
    run_full_pipeline_verification,
)
from src.backfiller.verify import CheckResult, VerificationReport


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def _insert_ticker(conn: sqlite3.Connection, symbol: str, active: int = 1) -> None:
    """Insert a ticker row."""
    conn.execute(
        "INSERT OR REPLACE INTO tickers (symbol, active, sector, sector_etf, added_date) "
        "VALUES (?, ?, 'Technology', 'XLK', '2020-01-01')",
        (symbol, active),
    )
    conn.commit()


def _insert_ohlcv(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    close: float = 150.0,
) -> None:
    """Insert a single OHLCV row."""
    conn.execute(
        "INSERT OR REPLACE INTO ohlcv_daily "
        "(ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, date_str, close * 0.99, close * 1.01, close * 0.985, close, 1_000_000),
    )
    conn.commit()


def _insert_indicator_row(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    rsi: float = 50.0,
    adx: float = 25.0,
    stoch_k: float = 50.0,
    stoch_d: float = 50.0,
    williams_r: float = -50.0,
    cmf: float = 0.0,
    bb_pctb: float = 0.5,
    atr: float = 2.0,
    ema_9: float = 150.0,
    ema_21: float = 148.0,
    ema_50: float = 145.0,
) -> None:
    """Insert a single indicators_daily row."""
    conn.execute(
        "INSERT OR REPLACE INTO indicators_daily "
        "(ticker, date, rsi_14, adx, stoch_k, stoch_d, williams_r, "
        "cmf_20, bb_pctb, atr_14, ema_9, ema_21, ema_50) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, date_str, rsi, adx, stoch_k, stoch_d, williams_r,
         cmf, bb_pctb, atr, ema_9, ema_21, ema_50),
    )
    conn.commit()


def _insert_warmup_rows(
    conn: sqlite3.Connection,
    ticker: str,
    count: int = 50,
) -> None:
    """Insert `count` valid indicator rows on consecutive dates starting 2020-01-02.

    Used to push violation rows past the warm-up window so that range checks
    are applied. Dates start far in the past so they sort before any 2026-*
    test dates.
    """
    base = date(2020, 1, 2)
    for i in range(count):
        d = (base + timedelta(days=i)).isoformat()
        _insert_indicator_row(conn, ticker, d)


def _insert_score(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    signal: str = "NEUTRAL",
    confidence: float = 50.0,
    final_score: float = 0.0,
    daily_score: float = 0.0,
    weekly_score: float = 0.0,
    trend_score: float = 0.0,
    momentum_score: float = 0.0,
    volume_score: float = 0.0,
    volatility_score: float = 0.0,
    candlestick_score: float = 0.0,
    structural_score: float = 0.0,
    sentiment_score: float = 0.0,
    fundamental_score: float = 0.0,
    macro_score: float = 0.0,
    regime: str = "trending",
    data_completeness: str = None,
    key_signals: str = None,
) -> None:
    """Insert a single scores_daily row."""
    if data_completeness is None:
        data_completeness = json.dumps({"ohlcv": True, "indicators": True})
    if key_signals is None:
        key_signals = json.dumps(["RSI above midline"])
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily "
        "(ticker, date, signal, confidence, final_score, regime, "
        "daily_score, weekly_score, "
        "trend_score, momentum_score, volume_score, volatility_score, "
        "candlestick_score, structural_score, sentiment_score, "
        "fundamental_score, macro_score, data_completeness, key_signals) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            ticker, date_str, signal, confidence, final_score, regime,
            daily_score, weekly_score,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, data_completeness, key_signals,
        ),
    )
    conn.commit()


def _insert_score_with_calibrated(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    signal: str = "NEUTRAL",
    confidence: float = 50.0,
    final_score: float = 0.0,
    calibrated_score: float | None = None,
) -> None:
    """Insert a scores_daily row including calibrated_score for consistency-check tests."""
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily "
        "(ticker, date, signal, confidence, final_score, calibrated_score, regime, "
        "data_completeness, key_signals) "
        "VALUES (?, ?, ?, ?, ?, ?, 'trending', '{}', '[]')",
        (ticker, date_str, signal, confidence, final_score, calibrated_score),
    )
    conn.commit()


def _insert_pattern(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    pattern_name: str = "hammer",
    pattern_category: str = "candlestick",
    direction: str = "bullish",
    strength: int = 3,
) -> None:
    """Insert a single patterns_daily row."""
    conn.execute(
        "INSERT INTO patterns_daily "
        "(ticker, date, pattern_name, pattern_category, direction, strength) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (ticker, date_str, pattern_name, pattern_category, direction, strength),
    )
    conn.commit()


def _insert_divergence(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    divergence_type: str = "regular_bullish",
    indicator: str = "rsi",
    price_swing_1: float = 100.0,
    price_swing_2: float = 95.0,
    ind_swing_1: float = 30.0,
    ind_swing_2: float = 35.0,
) -> None:
    """Insert a single divergences_daily row."""
    conn.execute(
        "INSERT INTO divergences_daily "
        "(ticker, date, indicator, divergence_type, "
        "price_swing_1_value, price_swing_2_value, "
        "indicator_swing_1_value, indicator_swing_2_value) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, date_str, indicator, divergence_type,
         price_swing_1, price_swing_2, ind_swing_1, ind_swing_2),
    )
    conn.commit()


def _insert_crossover(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    crossover_type: str = "ema_9_21",
    direction: str = "bullish",
    days_ago: int = 3,
) -> None:
    """Insert a single crossovers_daily row."""
    conn.execute(
        "INSERT INTO crossovers_daily (ticker, date, crossover_type, direction, days_ago) "
        "VALUES (?, ?, ?, ?, ?)",
        (ticker, date_str, crossover_type, direction, days_ago),
    )
    conn.commit()


def _insert_profile(
    conn: sqlite3.Connection,
    ticker: str,
    indicator: str,
    p5: float = 10.0,
    p20: float = 25.0,
    p50: float = 50.0,
    p80: float = 75.0,
    p95: float = 90.0,
    std: float = 15.0,
    window_end: str = None,
) -> None:
    """Insert a single indicator_profiles row."""
    if window_end is None:
        window_end = date.today().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO indicator_profiles "
        "(ticker, indicator, p5, p20, p50, p80, p95, mean, std, "
        "window_start, window_end, computed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '2024-01-01', ?, '2026-03-19')",
        (ticker, indicator, p5, p20, p50, p80, p95, (p5 + p95) / 2, std, window_end),
    )
    conn.commit()


def _insert_weekly_candle(
    conn: sqlite3.Connection,
    ticker: str,
    week_start: str,
    open_: float = 150.0,
    high: float = 155.0,
    low: float = 148.0,
    close: float = 152.0,
    volume: float = 5_000_000.0,
) -> None:
    """Insert a single weekly_candles row."""
    conn.execute(
        "INSERT OR REPLACE INTO weekly_candles "
        "(ticker, week_start, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, week_start, open_, high, low, close, volume),
    )
    conn.commit()


def _insert_weekly_indicator(
    conn: sqlite3.Connection,
    ticker: str,
    week_start: str,
    rsi: float = 50.0,
) -> None:
    """Insert a single indicators_weekly row."""
    conn.execute(
        "INSERT OR REPLACE INTO indicators_weekly "
        "(ticker, week_start, rsi_14) VALUES (?, ?, ?)",
        (ticker, week_start, rsi),
    )
    conn.commit()


def _insert_news_article(
    conn: sqlite3.Connection,
    article_id: str,
    ticker: str,
    date_str: str,
    sentiment: str = "positive",
) -> None:
    """Insert a single news_articles row."""
    conn.execute(
        "INSERT OR REPLACE INTO news_articles "
        "(id, ticker, date, source, headline, sentiment) "
        "VALUES (?, ?, ?, 'polygon', 'test headline', ?)",
        (article_id, ticker, date_str, sentiment),
    )
    conn.commit()


def _insert_news_summary(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    avg_sentiment: float = 0.5,
    article_count: int = 3,
    positive_count: int = 2,
    negative_count: int = 0,
    neutral_count: int = 1,
) -> None:
    """Insert a single news_daily_summary row."""
    conn.execute(
        "INSERT OR REPLACE INTO news_daily_summary "
        "(ticker, date, avg_sentiment_score, article_count, "
        "positive_count, negative_count, neutral_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, date_str, avg_sentiment, article_count,
         positive_count, negative_count, neutral_count),
    )
    conn.commit()


def _insert_signal_flip(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    previous_signal: str = "BEARISH",
    new_signal: str = "BULLISH",
) -> None:
    """Insert a single signal_flips row."""
    conn.execute(
        "INSERT INTO signal_flips (ticker, date, previous_signal, new_signal) "
        "VALUES (?, ?, ?, ?)",
        (ticker, date_str, previous_signal, new_signal),
    )
    conn.commit()


def _insert_sr_level(
    conn: sqlite3.Connection,
    ticker: str,
    level_price: float,
    date_computed: str = "2026-01-01",
) -> None:
    """Insert a single support_resistance row."""
    conn.execute(
        "INSERT INTO support_resistance "
        "(ticker, date_computed, level_price, level_type, touch_count) "
        "VALUES (?, ?, ?, 'support', 3)",
        (ticker, date_computed, level_price),
    )
    conn.commit()


def _generate_trading_days(start_date: date, count: int) -> list[str]:
    """Return a list of ISO date strings for the next `count` weekdays starting at start_date."""
    result: list[str] = []
    current = start_date
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# ═══ INDICATOR CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckIndicatorRanges:
    """Tests for check_indicator_ranges()."""

    def test_check_indicator_ranges_rsi_valid(self, db_connection: sqlite3.Connection) -> None:
        """RSI values between 0-100 should pass."""
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", rsi=45.0)
        _insert_indicator_row(db_connection, "AAPL", "2026-01-05", rsi=72.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "pass"

    def test_check_indicator_ranges_rsi(self, db_connection: sqlite3.Connection) -> None:
        """RSI outside 0-100 should be flagged as fail (critical)."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", rsi=105.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("rsi_14" in d for d in result.details)

    def test_check_indicator_ranges_adx(self, db_connection: sqlite3.Connection) -> None:
        """ADX outside 0-100 should be flagged as fail."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", adx=110.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("adx" in d for d in result.details)

    def test_check_indicator_ranges_stochastic(self, db_connection: sqlite3.Connection) -> None:
        """stoch_k outside 0-100 should be flagged."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", stoch_k=115.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("stoch_k" in d for d in result.details)

    def test_check_indicator_ranges_williams_r(self, db_connection: sqlite3.Connection) -> None:
        """Williams %R outside -100 to 0 should be flagged."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", williams_r=10.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("williams_r" in d for d in result.details)

    def test_check_indicator_ranges_cmf(self, db_connection: sqlite3.Connection) -> None:
        """CMF outside -1 to 1 should be flagged (non-critical → warn)."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", cmf=1.5)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("cmf_20" in d for d in result.details)

    def test_check_indicator_ranges_bb_pctb(self, db_connection: sqlite3.Connection) -> None:
        """BB %B outside -0.5 to 1.5 should be flagged."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", bb_pctb=3.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("bb_pctb" in d for d in result.details)

    def test_check_indicator_ranges_atr_positive(self, db_connection: sqlite3.Connection) -> None:
        """ATR < 0 should be flagged as fail (critical) even with tolerance."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", atr=-1.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("atr_14" in d for d in result.details)

    def test_check_indicator_ranges_ema_negative_critical(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Negative EMA is a computation error — should be flagged as critical fail."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", ema_9=-50.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("ema_9" in d for d in result.details)

    def test_check_indicator_ranges_bb_pctb_moderate(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """BB %B in the 'moderate' zone (1.8) no longer flagged with widened bounds [-1.0, 2.0]."""
        _insert_warmup_rows(db_connection, "AAPL")
        # 1.8 is between old max (1.5) and new max (2.0) — should now pass
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", bb_pctb=1.8)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "pass"

    def test_check_indicator_ranges_ema_large_distance_not_flagged(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Extreme EMA distance (COIN 2022: 176%) is NOT flagged — only logged at INFO."""
        _insert_warmup_rows(db_connection, "AAPL")
        # ema_50 = 300 on close=100 is 200% divergence — legitimate crash behavior
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02",
                               ema_9=102.0, ema_21=105.0, ema_50=300.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "pass"

    def test_check_indicator_ranges_ema_stuck_warning(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """EMA stuck at same value for 10+ consecutive days should trigger a warning."""
        _insert_warmup_rows(db_connection, "AAPL")
        # 10 post-warmup rows with identical ema_9 — computation error
        for i in range(10):
            d = (date(2026, 1, 2) + timedelta(days=i)).isoformat()
            _insert_indicator_row(db_connection, "AAPL", d, ema_9=200.0,
                                   ema_21=198.0, ema_50=195.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("ema_9" in d for d in result.details)

    def test_check_indicator_ranges_float_tolerance(self, db_connection: sqlite3.Connection) -> None:
        """Values that exceed bounds only by floating-point noise should not be flagged."""
        _insert_warmup_rows(db_connection, "AAPL")
        # RSI = 100 + 1e-12 (pure float noise, well within 1e-9 tolerance)
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", rsi=100.0 + 1e-12)
        # Williams %R = -100 - 1e-12
        _insert_indicator_row(db_connection, "AAPL", "2026-01-03", williams_r=-100.0 - 1e-12)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "pass"

    def test_check_indicator_ranges_atr_zero_in_warmup(self, db_connection: sqlite3.Connection) -> None:
        """ATR=0 within the first 50 rows (warm-up) should NOT be flagged."""
        # Only 1 row inserted — it will be at rn=1, inside warm-up
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", atr=0.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "pass"

    def test_check_indicator_ranges_atr_zero_after_warmup(self, db_connection: sqlite3.Connection) -> None:
        """ATR=0 AFTER the first 50 rows should be flagged (warm-up is over)."""
        _insert_warmup_rows(db_connection, "AAPL")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02", atr=0.0)
        result = check_indicator_ranges(db_connection, ["AAPL"])
        assert result.status == "fail"
        assert result.details is not None
        assert any("atr_14" in d for d in result.details)


class TestCheckIndicatorCoverage:
    """Tests for check_indicator_coverage()."""

    def test_check_indicator_coverage_all_present(self, db_connection: sqlite3.Connection) -> None:
        """All 55 tickers having indicator data should pass."""
        tickers = [f"T{i:02d}" for i in range(55)]
        for ticker in tickers:
            _insert_indicator_row(db_connection, ticker, "2026-01-02")
        result = check_indicator_coverage(db_connection, tickers)
        assert result.status == "pass"

    def test_check_indicator_coverage(self, db_connection: sqlite3.Connection) -> None:
        """Ticker missing from indicators_daily should be flagged."""
        tickers = ["AAPL", "MSFT", "GOOGL"]
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02")
        _insert_indicator_row(db_connection, "MSFT", "2026-01-02")
        # GOOGL has no indicators
        result = check_indicator_coverage(db_connection, tickers)
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("GOOGL" in d for d in result.details)


class TestCheckIndicatorDateAlignment:
    """Tests for check_indicator_date_alignment()."""

    def test_check_indicator_date_alignment(self, db_connection: sqlite3.Connection) -> None:
        """OHLCV for 100 dates but indicators only for 95 should flag 5 missing dates."""
        ticker = "AAPL"
        all_dates = _generate_trading_days(date(2025, 1, 2), 100)
        indicator_dates = all_dates[:95]

        for day in all_dates:
            _insert_ohlcv(db_connection, ticker, day)
        for day in indicator_dates:
            _insert_indicator_row(db_connection, ticker, day)

        result = check_indicator_date_alignment(db_connection, [ticker])
        assert result.status in ("warn", "fail")
        assert result.data is not None
        assert result.data.get("missing_dates_count", 0) >= 5

    def test_check_indicator_date_alignment_pass(self, db_connection: sqlite3.Connection) -> None:
        """OHLCV and indicators fully aligned should pass."""
        ticker = "AAPL"
        for day in ["2026-01-02", "2026-01-05", "2026-01-06"]:
            _insert_ohlcv(db_connection, ticker, day)
            _insert_indicator_row(db_connection, ticker, day)
        result = check_indicator_date_alignment(db_connection, [ticker])
        assert result.status == "pass"


class TestCheckIndicatorNullPercentage:
    """Tests for check_indicator_null_percentage()."""

    def test_check_indicator_no_all_null_rows(self, db_connection: sqlite3.Connection) -> None:
        """A row where every indicator column is NULL should be flagged."""
        db_connection.execute(
            "INSERT OR REPLACE INTO indicators_daily (ticker, date) "
            "VALUES ('AAPL', '2026-01-02')"
        )
        db_connection.commit()
        result = check_indicator_null_percentage(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_indicator_nan_percentage(self, db_connection: sqlite3.Connection) -> None:
        """10% NULL RSI (below 20% threshold) should pass but report the rate."""
        ticker = "AAPL"
        all_dates = _generate_trading_days(date(2025, 1, 2), 100)
        for idx, day in enumerate(all_dates):
            # Keep all key indicators populated; only rsi_14 is NULL for first 10 rows.
            rsi_val = None if idx < 10 else 50.0
            db_connection.execute(
                "INSERT OR REPLACE INTO indicators_daily "
                "(ticker, date, rsi_14, adx, macd_line, ema_9, ema_21, ema_50, atr_14) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ticker, day, rsi_val, 25.0, 0.5, 150.0, 148.0, 145.0, 2.0),
            )
        db_connection.commit()
        result = check_indicator_null_percentage(db_connection, [ticker])
        assert result.status == "pass"
        assert result.data is not None

    def test_check_indicator_nan_percentage_warn(self, db_connection: sqlite3.Connection) -> None:
        """30% NULL RSI (exceeds 20% threshold) should produce a warning."""
        ticker = "AAPL"
        all_dates = _generate_trading_days(date(2025, 1, 2), 100)
        for idx, day in enumerate(all_dates):
            # Keep other indicators populated so the all-NULL check does not trigger.
            rsi_val = None if idx < 30 else 50.0
            db_connection.execute(
                "INSERT OR REPLACE INTO indicators_daily "
                "(ticker, date, rsi_14, adx, ema_9, ema_21, ema_50, atr_14) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ticker, day, rsi_val, 25.0, 150.0, 148.0, 145.0, 2.0),
            )
        db_connection.commit()
        result = check_indicator_null_percentage(db_connection, [ticker], max_null_pct=20.0)
        assert result.status == "warn"


# ---------------------------------------------------------------------------
# ═══ SCORE CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckScoreRanges:
    """Tests for check_score_ranges()."""

    def test_check_score_range(self, db_connection: sqlite3.Connection) -> None:
        """final_score within -100 to +100 should pass."""
        _insert_score(db_connection, "AAPL", "2026-01-02", final_score=45.0)
        _insert_score(db_connection, "MSFT", "2026-01-02", final_score=-30.0)
        result = check_score_ranges(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_score_range_violation(self, db_connection: sqlite3.Connection) -> None:
        """final_score=150 should be flagged as fail."""
        _insert_score(db_connection, "AAPL", "2026-01-02", final_score=150.0)
        result = check_score_ranges(db_connection, "2026-01-02")
        assert result.status == "fail"
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)


class TestCheckCategoryScoreRanges:
    """Tests for check_category_score_ranges()."""

    def test_check_category_score_range(self, db_connection: sqlite3.Connection) -> None:
        """All 9 category scores within -100 to +100 should pass."""
        _insert_score(
            db_connection, "AAPL", "2026-01-02",
            trend_score=20.0, momentum_score=15.0, volume_score=-10.0,
            volatility_score=5.0, candlestick_score=30.0, structural_score=0.0,
            sentiment_score=10.0, fundamental_score=-5.0, macro_score=8.0,
        )
        result = check_category_score_ranges(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_category_score_range_violation(self, db_connection: sqlite3.Connection) -> None:
        """trend_score=120 should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02", trend_score=120.0)
        result = check_category_score_ranges(db_connection, "2026-01-02")
        assert result.status == "fail"
        assert result.details is not None


class TestCheckConfidenceRange:
    """Tests for check_confidence_range()."""

    def test_check_confidence_range(self, db_connection: sqlite3.Connection) -> None:
        """Confidence between 0 and 100 should pass."""
        _insert_score(db_connection, "AAPL", "2026-01-02", confidence=75.0)
        result = check_confidence_range(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_confidence_range_violation(self, db_connection: sqlite3.Connection) -> None:
        """Confidence > 100 should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02", confidence=110.0)
        result = check_confidence_range(db_connection, "2026-01-02")
        assert result.status == "fail"
        assert result.details is not None


class TestCheckSignalScoreConsistency:
    """Tests for check_signal_score_consistency()."""

    def test_check_signal_matches_score(self, db_connection: sqlite3.Connection) -> None:
        """BULLISH signal with positive score should pass."""
        _insert_score(db_connection, "AAPL", "2026-01-02",
                      signal="BULLISH", final_score=35.0)
        result = check_signal_score_consistency(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_signal_inconsistent_with_score(self, db_connection: sqlite3.Connection) -> None:
        """BEARISH signal with positive final_score=+35 should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02",
                      signal="BEARISH", final_score=35.0)
        result = check_signal_score_consistency(db_connection, "2026-01-02")
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)

    def test_bullish_via_calibrated_score_not_flagged(self, db_connection: sqlite3.Connection) -> None:
        """BULLISH signal driven by calibrated_score must NOT be flagged even if final_score <= 0.

        When calibration is warm, the signal is classified from calibrated_score
        (the ridge regression prediction). final_score is the raw ±100 composite
        which can legitimately disagree in sign. The consistency check must use
        calibrated_score as the arbiter when it is non-NULL.
        """
        _insert_score_with_calibrated(
            db_connection, "AAPL", "2026-01-02",
            signal="BULLISH",
            final_score=-12.0,     # raw composite is slightly negative
            calibrated_score=4.5,  # calibration says positive excess return → BULLISH
        )
        result = check_signal_score_consistency(db_connection, "2026-01-02")
        assert result.status == "pass", (
            f"Expected pass when calibrated_score drives the BULLISH signal, got: {result.status}. "
            f"Details: {result.details}"
        )

    def test_no_calibrated_score_falls_back_to_final_score(self, db_connection: sqlite3.Connection) -> None:
        """When calibrated_score is NULL, final_score is used for consistency check."""
        _insert_score_with_calibrated(
            db_connection, "AAPL", "2026-01-02",
            signal="BULLISH",
            final_score=35.0,
            calibrated_score=None,  # cold start — no calibration
        )
        result = check_signal_score_consistency(db_connection, "2026-01-02")
        assert result.status == "pass"


class TestCheckSignalDistribution:
    """Tests for check_signal_distribution()."""

    def test_check_signal_distribution_all_neutral(self, db_connection: sqlite3.Connection) -> None:
        """100% NEUTRAL signals across all tickers should trigger a warning."""
        tickers = [f"T{i:02d}" for i in range(59)]
        for ticker in tickers:
            _insert_score(db_connection, ticker, "2026-01-02", signal="NEUTRAL")
        result = check_signal_distribution(db_connection, "2026-01-02")
        assert result.status == "warn"
        assert result.message is not None
        assert any(
            keyword in result.message.lower()
            for keyword in ("100%", "same signal", "all")
        )

    def test_check_signal_distribution_healthy(self, db_connection: sqlite3.Connection) -> None:
        """15 bullish / 5 bearish / 39 neutral should pass."""
        for idx in range(15):
            _insert_score(db_connection, f"B{idx:02d}", "2026-01-02", signal="BULLISH")
        for idx in range(5):
            _insert_score(db_connection, f"D{idx:02d}", "2026-01-02", signal="BEARISH")
        for idx in range(39):
            _insert_score(db_connection, f"N{idx:02d}", "2026-01-02", signal="NEUTRAL")
        result = check_signal_distribution(db_connection, "2026-01-02")
        assert result.status == "pass"


class TestCheckConfidenceDistribution:
    """Tests for check_confidence_distribution()."""

    def test_check_confidence_distribution_all_zero(self, db_connection: sqlite3.Connection) -> None:
        """All tickers at 0% confidence should trigger a warning."""
        tickers = [f"T{i:02d}" for i in range(59)]
        for ticker in tickers:
            _insert_score(db_connection, ticker, "2026-01-02", confidence=0.0)
        result = check_confidence_distribution(db_connection, "2026-01-02")
        assert result.status == "warn"
        assert result.message is not None
        assert any(
            keyword in result.message.lower()
            for keyword in ("0%", "zero", "all")
        )

    def test_check_confidence_distribution_healthy(self, db_connection: sqlite3.Connection) -> None:
        """Mix of confidence values 0–79% should pass."""
        for idx in range(59):
            _insert_score(db_connection, f"T{idx:02d}", "2026-01-02",
                          confidence=float(idx % 80))
        result = check_confidence_distribution(db_connection, "2026-01-02")
        assert result.status == "pass"


class TestCheckWeightedScoreMath:
    """Tests for check_weighted_score_math()."""

    def test_check_weighted_score_math(self, db_connection: sqlite3.Connection) -> None:
        """final_score exactly equal to 0.2*daily + 0.8*weekly should pass."""
        daily_score = 50.0
        weekly_score = 30.0
        expected_final = 0.2 * daily_score + 0.8 * weekly_score  # 34.0
        _insert_score(db_connection, "AAPL", "2026-01-02",
                      final_score=expected_final,
                      daily_score=daily_score,
                      weekly_score=weekly_score)
        result = check_weighted_score_math(db_connection, "2026-01-02",
                                           daily_weight=0.2, weekly_weight=0.8)
        assert result.status == "pass"

    def test_check_weighted_score_math_violation(self, db_connection: sqlite3.Connection) -> None:
        """final_score far from 0.2*daily + 0.8*weekly should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02",
                      final_score=99.0,
                      daily_score=10.0,
                      weekly_score=10.0)  # expected ≈ 10.0, got 99.0
        result = check_weighted_score_math(db_connection, "2026-01-02", tolerance=2.0,
                                           daily_weight=0.2, weekly_weight=0.8)
        assert result.status in ("warn", "fail")
        assert result.details is not None


class TestCheckRegimeValues:
    """Tests for check_regime_values()."""

    def test_check_regime_valid(self, db_connection: sqlite3.Connection) -> None:
        """trending, ranging, volatile are all valid regime values."""
        for idx, regime in enumerate(["trending", "ranging", "volatile"]):
            _insert_score(db_connection, f"T{idx}", "2026-01-02", regime=regime)
        result = check_regime_values(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_regime_invalid(self, db_connection: sqlite3.Connection) -> None:
        """Unknown regime value should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02", regime="sideways")
        result = check_regime_values(db_connection, "2026-01-02")
        assert result.status == "fail"
        assert result.details is not None
        assert any("sideways" in d for d in result.details)


class TestCheckJsonFields:
    """Tests for check_json_fields()."""

    def test_check_data_completeness_json(self, db_connection: sqlite3.Connection) -> None:
        """Valid JSON in both data_completeness and key_signals should pass."""
        _insert_score(db_connection, "AAPL", "2026-01-02",
                      data_completeness=json.dumps({"ohlcv": True, "indicators": True}),
                      key_signals=json.dumps(["RSI above midline"]))
        result = check_json_fields(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_key_signals_json(self, db_connection: sqlite3.Connection) -> None:
        """Invalid JSON in key_signals should be flagged."""
        db_connection.execute(
            "INSERT OR REPLACE INTO scores_daily "
            "(ticker, date, signal, confidence, final_score, regime, "
            "data_completeness, key_signals) "
            "VALUES ('AAPL', '2026-01-02', 'NEUTRAL', 50.0, 0.0, 'trending', "
            "'{\"ohlcv\":true}', 'not valid json[')"
        )
        db_connection.commit()
        result = check_json_fields(db_connection, "2026-01-02")
        assert result.status in ("warn", "fail")
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ PATTERN CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckPatternCounts:
    """Tests for check_pattern_counts()."""

    def test_check_pattern_count_reasonable(self, db_connection: sqlite3.Connection) -> None:
        """300 candlestick + 30 structural patterns over 5 years should pass."""
        ticker = "AAPL"
        base = date(2021, 1, 4)
        for idx in range(300):
            day = (base + timedelta(days=idx)).isoformat()
            _insert_pattern(db_connection, ticker, day, pattern_category="candlestick")
        for idx in range(30):
            day = (base + timedelta(days=300 + idx)).isoformat()
            _insert_pattern(db_connection, ticker, day, pattern_category="structural",
                            pattern_name=f"double_top_{idx}")
        result = check_pattern_counts(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_pattern_count_too_high(self, db_connection: sqlite3.Connection) -> None:
        """5000 structural patterns for a ticker should trigger a warning."""
        ticker = "AAPL"
        base = date(2021, 1, 4)
        for idx in range(5000):
            day = (base + timedelta(days=idx % 1260)).isoformat()
            _insert_pattern(db_connection, ticker, day,
                            pattern_category="structural",
                            pattern_name=f"struct_{idx}")
        result = check_pattern_counts(db_connection, [ticker])
        assert result.status == "warn"
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)

    def test_check_pattern_count_zero(self, db_connection: sqlite3.Connection) -> None:
        """Ticker with 0 patterns should trigger a warning."""
        result = check_pattern_counts(db_connection, ["AAPL"])
        assert result.status == "warn"
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)


class TestCheckPatternDuplicates:
    """Tests for check_pattern_duplicates()."""

    def test_check_pattern_no_duplicates(self, db_connection: sqlite3.Connection) -> None:
        """Two identical rows (same ticker, date, name, direction) should be flagged."""
        _insert_pattern(db_connection, "AAPL", "2026-01-02",
                        pattern_name="hammer", direction="bullish")
        _insert_pattern(db_connection, "AAPL", "2026-01-02",
                        pattern_name="hammer", direction="bullish")
        result = check_pattern_duplicates(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_pattern_no_duplicates_pass(self, db_connection: sqlite3.Connection) -> None:
        """Different patterns on the same date should pass."""
        _insert_pattern(db_connection, "AAPL", "2026-01-02",
                        pattern_name="hammer", direction="bullish")
        _insert_pattern(db_connection, "AAPL", "2026-01-02",
                        pattern_name="doji", direction="neutral")
        result = check_pattern_duplicates(db_connection, ["AAPL"])
        assert result.status == "pass"


class TestCheckPatternFieldValidity:
    """Tests for check_pattern_field_validity()."""

    def test_check_pattern_direction_valid(self, db_connection: sqlite3.Connection) -> None:
        """Direction not in {bullish, bearish, neutral} should be flagged."""
        db_connection.execute(
            "INSERT INTO patterns_daily "
            "(ticker, date, pattern_name, direction, pattern_category, strength) "
            "VALUES ('AAPL', '2026-01-02', 'hammer', 'up', 'candlestick', 3)"
        )
        db_connection.commit()
        result = check_pattern_field_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_pattern_strength_range(self, db_connection: sqlite3.Connection) -> None:
        """strength=10 (outside 1-5) should be flagged."""
        db_connection.execute(
            "INSERT INTO patterns_daily "
            "(ticker, date, pattern_name, direction, pattern_category, strength) "
            "VALUES ('AAPL', '2026-01-02', 'hammer', 'bullish', 'candlestick', 10)"
        )
        db_connection.commit()
        result = check_pattern_field_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_pattern_category_valid(self, db_connection: sqlite3.Connection) -> None:
        """pattern_category not in {candlestick, structural} should be flagged."""
        db_connection.execute(
            "INSERT INTO patterns_daily "
            "(ticker, date, pattern_name, direction, pattern_category, strength) "
            "VALUES ('AAPL', '2026-01-02', 'hammer', 'bullish', 'unknown_cat', 3)"
        )
        db_connection.commit()
        result = check_pattern_field_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_pattern_field_validity_pass(self, db_connection: sqlite3.Connection) -> None:
        """Valid direction, strength, and category should all pass."""
        _insert_pattern(db_connection, "AAPL", "2026-01-02",
                        direction="bullish", strength=3, pattern_category="candlestick")
        _insert_pattern(db_connection, "AAPL", "2026-01-05",
                        direction="bearish", strength=2, pattern_category="structural")
        _insert_pattern(db_connection, "AAPL", "2026-01-06",
                        direction="neutral", strength=1, pattern_category="candlestick")
        result = check_pattern_field_validity(db_connection)
        assert result.status == "pass"


# ---------------------------------------------------------------------------
# ═══ DIVERGENCE CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckDivergenceCounts:
    """Tests for check_divergence_counts()."""

    def test_check_divergence_count_reasonable(self, db_connection: sqlite3.Connection) -> None:
        """150 divergences over 5 years should pass."""
        ticker = "AAPL"
        base = date(2021, 1, 4)
        for idx in range(150):
            day = (base + timedelta(days=idx * 8)).isoformat()
            _insert_divergence(db_connection, ticker, day)
        result = check_divergence_counts(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_divergence_count_zero(self, db_connection: sqlite3.Connection) -> None:
        """0 divergences for a ticker should trigger a warning."""
        result = check_divergence_counts(db_connection, ["AAPL"])
        assert result.status == "warn"
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)


class TestCheckDivergenceConsistency:
    """Tests for check_divergence_consistency()."""

    def test_check_divergence_type_valid(self, db_connection: sqlite3.Connection) -> None:
        """All four valid divergence_type values with correct swing values should pass."""
        # regular_bullish: price lower low, indicator higher low
        _insert_divergence(db_connection, "AAPL", "2026-01-02",
                           divergence_type="regular_bullish", indicator="rsi",
                           price_swing_1=100.0, price_swing_2=95.0,
                           ind_swing_1=30.0, ind_swing_2=35.0)
        # regular_bearish: price higher high, indicator lower high
        _insert_divergence(db_connection, "AAPL", "2026-01-05",
                           divergence_type="regular_bearish", indicator="rsi",
                           price_swing_1=100.0, price_swing_2=108.0,
                           ind_swing_1=70.0, ind_swing_2=65.0)
        # hidden_bullish: price higher low, indicator lower low
        _insert_divergence(db_connection, "AAPL", "2026-01-06",
                           divergence_type="hidden_bullish", indicator="macd_histogram",
                           price_swing_1=90.0, price_swing_2=95.0,
                           ind_swing_1=35.0, ind_swing_2=28.0)
        # hidden_bearish: price lower high, indicator higher high
        _insert_divergence(db_connection, "AAPL", "2026-01-07",
                           divergence_type="hidden_bearish", indicator="obv",
                           price_swing_1=105.0, price_swing_2=100.0,
                           ind_swing_1=65.0, ind_swing_2=72.0)
        result = check_divergence_consistency(db_connection)
        assert result.status == "pass"

    def test_check_divergence_type_invalid(self, db_connection: sqlite3.Connection) -> None:
        """Unknown divergence_type should be flagged."""
        db_connection.execute(
            "INSERT INTO divergences_daily "
            "(ticker, date, indicator, divergence_type) "
            "VALUES ('AAPL', '2026-01-02', 'rsi', 'mystery_type')"
        )
        db_connection.commit()
        result = check_divergence_consistency(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_divergence_indicator_valid(self, db_connection: sqlite3.Connection) -> None:
        """Unknown indicator name should be flagged."""
        db_connection.execute(
            "INSERT INTO divergences_daily "
            "(ticker, date, indicator, divergence_type) "
            "VALUES ('AAPL', '2026-01-02', 'unknown_indicator', 'regular_bullish')"
        )
        db_connection.commit()
        result = check_divergence_consistency(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_divergence_swing_values_consistent(self, db_connection: sqlite3.Connection) -> None:
        """regular_bullish with price lower low and indicator higher low should pass."""
        _insert_divergence(
            db_connection, "AAPL", "2026-01-02",
            divergence_type="regular_bullish",
            price_swing_1=100.0, price_swing_2=95.0,   # lower low ✓
            ind_swing_1=30.0, ind_swing_2=35.0,         # higher low ✓
        )
        result = check_divergence_consistency(db_connection)
        assert result.status == "pass"

    def test_check_divergence_swing_values_inconsistent(self, db_connection: sqlite3.Connection) -> None:
        """regular_bullish where price_swing_2 > price_swing_1 contradicts the type."""
        _insert_divergence(
            db_connection, "AAPL", "2026-01-02",
            divergence_type="regular_bullish",
            price_swing_1=90.0, price_swing_2=100.0,   # higher high — wrong for regular_bullish
            ind_swing_1=30.0, ind_swing_2=35.0,
        )
        result = check_divergence_consistency(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ CROSSOVER CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckCrossoverValidity:
    """Tests for check_crossover_validity()."""

    def test_check_crossover_type_valid(self, db_connection: sqlite3.Connection) -> None:
        """All three valid crossover_type values should pass."""
        for idx, ctype in enumerate(["ema_9_21", "ema_21_50", "macd_signal"]):
            day = (date(2026, 1, 2) + timedelta(days=idx)).isoformat()
            _insert_crossover(db_connection, "AAPL", day, crossover_type=ctype)
        result = check_crossover_validity(db_connection)
        assert result.status == "pass"

    def test_check_crossover_type_invalid(self, db_connection: sqlite3.Connection) -> None:
        """Unknown crossover_type should be flagged."""
        db_connection.execute(
            "INSERT INTO crossovers_daily (ticker, date, crossover_type, direction, days_ago) "
            "VALUES ('AAPL', '2026-01-02', 'unknown_cross', 'bullish', 1)"
        )
        db_connection.commit()
        result = check_crossover_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_crossover_direction_valid(self, db_connection: sqlite3.Connection) -> None:
        """direction not in {bullish, bearish} should be flagged."""
        db_connection.execute(
            "INSERT INTO crossovers_daily (ticker, date, crossover_type, direction, days_ago) "
            "VALUES ('AAPL', '2026-01-02', 'ema_9_21', 'sideways', 1)"
        )
        db_connection.commit()
        result = check_crossover_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_crossover_days_ago_valid(self, db_connection: sqlite3.Connection) -> None:
        """Negative days_ago should be flagged."""
        db_connection.execute(
            "INSERT INTO crossovers_daily (ticker, date, crossover_type, direction, days_ago) "
            "VALUES ('AAPL', '2026-01-02', 'ema_9_21', 'bullish', -5)"
        )
        db_connection.commit()
        result = check_crossover_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ PROFILE CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckProfileCoverage:
    """Tests for check_profile_coverage()."""

    def test_check_profiles_exist_for_all_tickers(self, db_connection: sqlite3.Connection) -> None:
        """55 tickers each with 15+ indicator profiles should pass."""
        tickers = [f"T{i:02d}" for i in range(55)]
        indicators = [
            "rsi_14", "stoch_k", "stoch_d", "cci_20", "williams_r",
            "cmf_20", "bb_pctb", "adx", "macd_histogram", "atr_14",
            "obv", "ad_line", "macd_line", "macd_signal",
            "ema_9", "ema_21", "ema_50",
        ]
        for ticker in tickers:
            for ind in indicators:
                _insert_profile(db_connection, ticker, ind)
        result = check_profile_coverage(db_connection, tickers)
        assert result.status == "pass"

    def test_check_profiles_exist_for_all_tickers_missing(self, db_connection: sqlite3.Connection) -> None:
        """Ticker with 0 profiles should be flagged."""
        tickers = ["AAPL", "MSFT"]
        _insert_profile(db_connection, "AAPL", "rsi_14")
        # MSFT has no profiles
        result = check_profile_coverage(db_connection, tickers)
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("MSFT" in d for d in result.details)


class TestCheckProfilePercentileOrder:
    """Tests for check_profile_percentile_order()."""

    def test_check_profile_percentiles_ordered(self, db_connection: sqlite3.Connection) -> None:
        """p5 < p20 < p50 < p80 < p95 should pass."""
        _insert_profile(db_connection, "AAPL", "rsi_14",
                        p5=10.0, p20=25.0, p50=50.0, p80=75.0, p95=90.0, std=15.0)
        result = check_profile_percentile_order(db_connection)
        assert result.status == "pass"

    def test_check_profile_percentiles_out_of_order(self, db_connection: sqlite3.Connection) -> None:
        """p20 > p50 should be flagged."""
        _insert_profile(db_connection, "AAPL", "rsi_14",
                        p5=10.0, p20=70.0, p50=50.0, p80=75.0, p95=90.0, std=15.0)
        result = check_profile_percentile_order(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_profile_std_positive(self, db_connection: sqlite3.Connection) -> None:
        """std=0 means no variance and should be flagged."""
        _insert_profile(db_connection, "AAPL", "rsi_14",
                        p5=10.0, p20=25.0, p50=50.0, p80=75.0, p95=90.0, std=0.0)
        result = check_profile_percentile_order(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None


class TestCheckProfileFreshness:
    """Tests for check_profile_freshness()."""

    def test_check_profile_window_recent(self, db_connection: sqlite3.Connection) -> None:
        """window_end = today should pass."""
        _insert_profile(db_connection, "AAPL", "rsi_14",
                        window_end=date.today().isoformat())
        result = check_profile_freshness(db_connection, max_age_days=30)
        assert result.status == "pass"

    def test_check_profile_window_stale(self, db_connection: sqlite3.Connection) -> None:
        """window_end 60 days ago should trigger a warning (stale profiles)."""
        stale_date = (date.today() - timedelta(days=60)).isoformat()
        _insert_profile(db_connection, "AAPL", "rsi_14", window_end=stale_date)
        result = check_profile_freshness(db_connection, max_age_days=30)
        assert result.status == "warn"
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ WEEKLY CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckWeeklyCandleValidity:
    """Tests for check_weekly_candle_validity()."""

    def test_check_weekly_candle_count(self, db_connection: sqlite3.Connection) -> None:
        """~260 weekly candles with no OHLCV data falls back to 5yr assumption → pass."""
        ticker = "AAPL"
        base = date(2021, 1, 4)
        for idx in range(260):
            week_start = (base + timedelta(weeks=idx)).isoformat()
            _insert_weekly_candle(db_connection, ticker, week_start)
        result = check_weekly_candle_validity(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_weekly_candle_count_dynamic_start_date(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Ticker with ~2.5yr of OHLCV data and 100 weekly candles should pass.

        With a fixed 5yr assumption this would fail (100 < 156), but dynamic
        calculation yields ~130 weeks elapsed → min_expected ≈ 78.
        """
        ticker = "AAPL"
        # Insert one OHLCV row ~2.5 years ago to set the data start date
        start = (date.today() - timedelta(weeks=130)).isoformat()
        _insert_ohlcv(db_connection, ticker, start)
        # 100 weekly candles > 130 * 0.6 = 78 → should pass
        base = date.fromisoformat(start)
        for idx in range(100):
            week_start = (base + timedelta(weeks=idx)).isoformat()
            _insert_weekly_candle(db_connection, ticker, week_start)
        result = check_weekly_candle_validity(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_weekly_ohlc_valid(self, db_connection: sqlite3.Connection) -> None:
        """high < low (invalid candle) should be flagged."""
        db_connection.execute(
            "INSERT OR REPLACE INTO weekly_candles "
            "(ticker, week_start, open, high, low, close, volume) "
            "VALUES ('AAPL', '2026-01-05', 150.0, 145.0, 148.0, 152.0, 1000000)"
        )
        db_connection.commit()
        result = check_weekly_candle_validity(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_weekly_volume_summed(self, db_connection: sqlite3.Connection) -> None:
        """No false positive when weekly candles and OHLCV cover non-overlapping date ranges."""
        ticker = "AAPL"
        # Weekly candles from 2021 through ~2025 (no OHLCV for those weeks)
        base = date(2021, 1, 4)
        for idx in range(260):
            week_start = (base + timedelta(weeks=idx)).isoformat()
            _insert_weekly_candle(db_connection, ticker, week_start, volume=5_000_000.0)
        # OHLCV only for a week that has no weekly candle — no matchable reference
        for day_idx in range(5):
            day = (date(2026, 1, 5) + timedelta(days=day_idx)).isoformat()
            _insert_ohlcv(db_connection, ticker, day, close=150.0)
        result = check_weekly_candle_validity(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_weekly_volume_holiday_week(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """3-day holiday week with proportionally lower volume should NOT be flagged."""
        ticker = "AAPL"
        today = date.today()
        # this_monday is the current week's start → will be the most recent candle (skipped)
        this_monday = today - timedelta(days=today.weekday())
        # holiday week is 2 weeks ago — not the most recent, so volume IS checked
        holiday_monday = this_monday - timedelta(weeks=2)

        _insert_weekly_candle(
            db_connection, ticker, holiday_monday.isoformat(), volume=2_000_000.0
        )
        _insert_weekly_candle(
            db_connection, ticker, this_monday.isoformat(), volume=5_000_000.0
        )
        # 3 trading days (Mon, Tue, Wed) in the holiday week; avg_daily ≈ 1M
        for i in range(3):
            _insert_ohlcv(db_connection, ticker, (holiday_monday + timedelta(days=i)).isoformat())
        # threshold = 3 × 0.3 × local_avg(1M) = 900_000; volume=2M >= threshold → pass
        result = check_weekly_candle_validity(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_weekly_volume_holiday_week_fail(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """3-day holiday week with volume well below threshold is logged as INFO (not a warning)."""
        ticker = "AAPL"
        today = date.today()
        this_monday = today - timedelta(days=today.weekday())
        holiday_monday = this_monday - timedelta(weeks=2)

        _insert_weekly_candle(
            db_connection, ticker, holiday_monday.isoformat(), volume=200_000.0
        )
        _insert_weekly_candle(
            db_connection, ticker, this_monday.isoformat(), volume=5_000_000.0
        )
        # 3 trading days, local avg ≈ 1M → threshold = 0.9M; volume=200K < threshold
        for i in range(3):
            _insert_ohlcv(db_connection, ticker, (holiday_monday + timedelta(days=i)).isoformat())
        result = check_weekly_candle_validity(db_connection, [ticker])
        # Volume issues are INFO-only — overall status stays "pass"
        assert result.status == "pass"
        assert result.details is not None
        assert any(holiday_monday.isoformat() in d for d in result.details)

    def test_check_weekly_volume_current_week_skipped(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Most recent week is always skipped regardless of volume."""
        ticker = "AAPL"
        today = date.today()
        this_monday = today - timedelta(days=today.weekday())
        # Only one candle — it IS the most recent → volume check is skipped entirely
        _insert_weekly_candle(db_connection, ticker, this_monday.isoformat(), volume=100.0)
        _insert_ohlcv(db_connection, ticker, this_monday.isoformat())
        result = check_weekly_candle_validity(db_connection, [ticker])
        assert result.status == "pass"


class TestCheckWeeklyIndicatorCoverage:
    """Tests for check_weekly_indicator_coverage()."""

    def test_check_weekly_indicators_exist(self, db_connection: sqlite3.Connection) -> None:
        """Ticker with weekly candles but no weekly indicators should be flagged."""
        _insert_weekly_candle(db_connection, "AAPL", "2026-01-05")
        # No weekly indicator
        result = check_weekly_indicator_coverage(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_weekly_indicators_exist_pass(self, db_connection: sqlite3.Connection) -> None:
        """Weekly candle + weekly indicator should pass."""
        _insert_weekly_candle(db_connection, "AAPL", "2026-01-05")
        _insert_weekly_indicator(db_connection, "AAPL", "2026-01-05")
        result = check_weekly_indicator_coverage(db_connection, ["AAPL"])
        assert result.status == "pass"


# ---------------------------------------------------------------------------
# ═══ NEWS SUMMARY CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckNewsSummaryConsistency:
    """Tests for check_news_summary_consistency()."""

    def test_check_news_summary_counts_match(self, db_connection: sqlite3.Connection) -> None:
        """Summary article_count matching actual article count should pass."""
        ticker = "AAPL"
        date_str = "2026-01-02"
        for idx in range(3):
            _insert_news_article(db_connection, f"art_{idx}", ticker, date_str)
        _insert_news_summary(db_connection, ticker, date_str, article_count=3)
        result = check_news_summary_consistency(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_news_summary_counts_mismatch(self, db_connection: sqlite3.Connection) -> None:
        """Summary says 5 articles but only 3 exist — should be flagged."""
        ticker = "AAPL"
        date_str = "2026-01-02"
        for idx in range(3):
            _insert_news_article(db_connection, f"art_{idx}", ticker, date_str)
        _insert_news_summary(db_connection, ticker, date_str, article_count=5)
        result = check_news_summary_consistency(db_connection, [ticker])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_news_summary_sentiment_range(self, db_connection: sqlite3.Connection) -> None:
        """avg_sentiment_score=2.5 (outside -1 to 1) should be flagged."""
        db_connection.execute(
            "INSERT OR REPLACE INTO news_daily_summary "
            "(ticker, date, avg_sentiment_score, article_count, "
            "positive_count, negative_count, neutral_count) "
            "VALUES ('AAPL', '2026-01-02', 2.5, 3, 3, 0, 0)"
        )
        db_connection.commit()
        result = check_news_summary_consistency(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_news_summary_counts_add_up(self, db_connection: sqlite3.Connection) -> None:
        """positive+negative+neutral != article_count should be flagged."""
        # 2 + 0 + 1 = 3, but article_count = 4
        _insert_news_summary(
            db_connection, "AAPL", "2026-01-02",
            article_count=4,
            positive_count=2, negative_count=0, neutral_count=1,
        )
        result = check_news_summary_consistency(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ CROSS-TABLE CONSISTENCY ═══
# ---------------------------------------------------------------------------

class TestCheckScoresHaveIndicators:
    """Tests for check_scores_have_indicators()."""

    def test_check_scores_have_indicators(self, db_connection: sqlite3.Connection) -> None:
        """Score with matching indicator row should pass."""
        _insert_score(db_connection, "AAPL", "2026-01-02")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02")
        result = check_scores_have_indicators(db_connection, "2026-01-02")
        assert result.status == "pass"

    def test_check_scores_have_indicators_missing(self, db_connection: sqlite3.Connection) -> None:
        """Score without matching indicator row should be flagged."""
        _insert_score(db_connection, "AAPL", "2026-01-02")
        # No indicator row for AAPL 2026-01-02
        result = check_scores_have_indicators(db_connection, "2026-01-02")
        assert result.status in ("warn", "fail")
        assert result.details is not None
        assert any("AAPL" in d for d in result.details)


class TestCheckIndicatorsHaveOhlcv:
    """Tests for check_indicators_have_ohlcv()."""

    def test_check_indicators_have_ohlcv(self, db_connection: sqlite3.Connection) -> None:
        """Indicator row without corresponding OHLCV row should be flagged."""
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02")
        # No OHLCV for AAPL on that date
        result = check_indicators_have_ohlcv(db_connection, ["AAPL"])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_indicators_have_ohlcv_pass(self, db_connection: sqlite3.Connection) -> None:
        """Indicator row with matching OHLCV row should pass."""
        _insert_ohlcv(db_connection, "AAPL", "2026-01-02")
        _insert_indicator_row(db_connection, "AAPL", "2026-01-02")
        result = check_indicators_have_ohlcv(db_connection, ["AAPL"])
        assert result.status == "pass"


class TestCheckSrLevelsWithinRange:
    """Tests for check_sr_levels_within_range()."""

    def test_check_sr_levels_within_price_range(self, db_connection: sqlite3.Connection) -> None:
        """S/R level within historical price range should pass."""
        ticker = "AAPL"
        for idx in range(10):
            day = (date(2025, 1, 2) + timedelta(days=idx)).isoformat()
            _insert_ohlcv(db_connection, ticker, day, close=150.0)
        _insert_sr_level(db_connection, ticker, level_price=148.0)
        result = check_sr_levels_within_range(db_connection, [ticker])
        assert result.status == "pass"

    def test_check_sr_levels_outside_price_range(self, db_connection: sqlite3.Connection) -> None:
        """S/R level at 5000 when stock trades around 150 should be flagged."""
        ticker = "AAPL"
        for idx in range(10):
            day = (date(2025, 1, 2) + timedelta(days=idx)).isoformat()
            _insert_ohlcv(db_connection, ticker, day, close=150.0)
        _insert_sr_level(db_connection, ticker, level_price=5000.0)
        result = check_sr_levels_within_range(db_connection, [ticker])
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_fibonacci_within_price_range(self, db_connection: sqlite3.Connection) -> None:
        """Swing points within historical OHLCV range should compute valid fibonacci levels."""
        ticker = "AAPL"
        # Insert OHLCV spanning 120–180
        for idx in range(10):
            day = (date(2025, 1, 2) + timedelta(days=idx)).isoformat()
            _insert_ohlcv(db_connection, ticker, day, close=150.0)
        # Insert valid swing points within the price range
        db_connection.execute(
            "INSERT INTO swing_points (ticker, date, type, price, strength) "
            "VALUES ('AAPL', '2025-01-02', 'low', 120.0, 3)"
        )
        db_connection.execute(
            "INSERT INTO swing_points (ticker, date, type, price, strength) "
            "VALUES ('AAPL', '2025-01-09', 'high', 180.0, 3)"
        )
        db_connection.commit()
        result = check_sr_levels_within_range(db_connection, [ticker])
        assert result.status == "pass"


# ---------------------------------------------------------------------------
# ═══ SIGNAL FLIP CHECKS ═══
# ---------------------------------------------------------------------------

class TestCheckSignalFlipValidity:
    """Tests for check_signal_flip_validity()."""

    def test_check_flip_signals_actually_differ(self, db_connection: sqlite3.Connection) -> None:
        """Flip where previous_signal == new_signal should be flagged."""
        db_connection.execute(
            "INSERT INTO signal_flips (ticker, date, previous_signal, new_signal) "
            "VALUES ('AAPL', '2026-01-02', 'BULLISH', 'BULLISH')"
        )
        db_connection.commit()
        result = check_signal_flip_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None

    def test_check_flip_signals_differ_pass(self, db_connection: sqlite3.Connection) -> None:
        """Valid flip with corresponding score row should pass."""
        _insert_signal_flip(db_connection, "AAPL", "2026-01-02",
                            previous_signal="BEARISH", new_signal="BULLISH")
        _insert_score(db_connection, "AAPL", "2026-01-02")
        result = check_signal_flip_validity(db_connection)
        assert result.status == "pass"

    def test_check_flip_date_has_score(self, db_connection: sqlite3.Connection) -> None:
        """Flip without corresponding score row should be flagged."""
        _insert_signal_flip(db_connection, "AAPL", "2026-01-02",
                            previous_signal="BEARISH", new_signal="BULLISH")
        # No score inserted
        result = check_signal_flip_validity(db_connection)
        assert result.status in ("warn", "fail")
        assert result.details is not None


# ---------------------------------------------------------------------------
# ═══ AGGREGATE HEALTH ═══
# ---------------------------------------------------------------------------

class TestRunFullPipelineVerification:
    """Tests for run_full_pipeline_verification() aggregate health."""

    def _setup_db(self, db_path: str) -> sqlite3.Connection:
        """Create a fresh database with full schema at the given path."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        from src.common.db import create_all_tables
        create_all_tables(conn)
        return conn

    def test_check_overall_health_pass(self, tmp_path) -> None:
        """All checks pass → overall_status = PASS."""
        db_path = str(tmp_path / "signals_pass.db")
        conn = self._setup_db(db_path)

        ticker = "AAPL"
        date_str = "2026-01-02"
        _insert_ticker(conn, ticker)
        _insert_ohlcv(conn, ticker, date_str)
        _insert_indicator_row(conn, ticker, date_str)
        daily_score = 50.0
        weekly_score = 30.0
        final_score = round(0.2 * daily_score + 0.8 * weekly_score, 6)
        _insert_score(conn, ticker, date_str,
                      signal="BULLISH", confidence=60.0, final_score=final_score,
                      daily_score=daily_score, weekly_score=weekly_score)
        _insert_profile(conn, ticker, "rsi_14", window_end=date.today().isoformat())
        conn.close()

        with patch("src.backfiller.verify_pipeline.get_active_tickers",
                   return_value=[{"symbol": ticker}]):
            report = run_full_pipeline_verification(db_path=db_path, scoring_date=date_str)

        assert isinstance(report, VerificationReport)
        assert report.overall_status == "PASS"

    def test_check_overall_health_warn(self, tmp_path) -> None:
        """Non-critical warnings present → overall_status = PASS with warn_count > 0."""
        db_path = str(tmp_path / "signals_warn.db")
        conn = self._setup_db(db_path)

        ticker = "AAPL"
        date_str = "2026-01-02"
        _insert_ticker(conn, ticker)
        _insert_ohlcv(conn, ticker, date_str)
        _insert_indicator_row(conn, ticker, date_str)
        _insert_score(conn, ticker, date_str,
                      signal="NEUTRAL", confidence=50.0, final_score=0.0,
                      daily_score=0.0, weekly_score=0.0)
        # Stale profile → should produce a warning, not a failure
        stale = (date.today() - timedelta(days=60)).isoformat()
        _insert_profile(conn, ticker, "rsi_14", window_end=stale)
        conn.close()

        with patch("src.backfiller.verify_pipeline.get_active_tickers",
                   return_value=[{"symbol": ticker}]):
            report = run_full_pipeline_verification(db_path=db_path, scoring_date=date_str)

        assert report.overall_status == "PASS"
        assert report.warn_count > 0

    def test_check_overall_health_fail(self, tmp_path) -> None:
        """Critical check fails → overall_status = FAIL."""
        db_path = str(tmp_path / "signals_fail.db")
        conn = self._setup_db(db_path)

        ticker = "AAPL"
        date_str = "2026-01-02"
        _insert_ticker(conn, ticker)
        _insert_ohlcv(conn, ticker, date_str)
        # RSI out of range → critical failure
        _insert_indicator_row(conn, ticker, date_str, rsi=999.0)
        # final_score out of range → critical failure
        _insert_score(conn, ticker, date_str, final_score=999.0)
        conn.close()

        with patch("src.backfiller.verify_pipeline.get_active_tickers",
                   return_value=[{"symbol": ticker}]):
            report = run_full_pipeline_verification(db_path=db_path, scoring_date=date_str)

        assert report.overall_status == "FAIL"
        assert report.fail_count > 0


# ---------------------------------------------------------------------------
# ═══ FORMAT REPORT ═══
# ---------------------------------------------------------------------------

class TestFormatPipelineVerificationReport:
    """Tests for format_pipeline_verification_report()."""

    def test_format_report_contains_header(self) -> None:
        """Formatted report should contain a 'Pipeline Verification' header."""
        report = VerificationReport(
            checks=[CheckResult(name="test_check", status="pass", message="all good")],
            overall_status="PASS",
            pass_count=1,
            warn_count=0,
            fail_count=0,
            timestamp="2026-03-19T08:00:00+00:00",
        )
        formatted = format_pipeline_verification_report(report)
        assert "Pipeline Verification" in formatted
        assert "PASS" in formatted

    def test_format_report_under_4096_chars(self) -> None:
        """Formatted report must be ≤ 4096 characters for Telegram."""
        checks = [
            CheckResult(
                name=f"check_{idx}",
                status="warn",
                message=f"Warning {idx}",
                details=[f"detail line {j}" for j in range(25)],
            )
            for idx in range(20)
        ]
        report = VerificationReport(
            checks=checks,
            overall_status="PASS",
            pass_count=0,
            warn_count=20,
            fail_count=0,
            timestamp="2026-03-19T08:00:00+00:00",
        )
        formatted = format_pipeline_verification_report(report)
        assert len(formatted) <= 4096

    def test_format_report_fail_shows_emoji(self) -> None:
        """FAIL status should produce a report containing the ❌ emoji."""
        report = VerificationReport(
            checks=[CheckResult(name="score_ranges", status="fail",
                                message="out of range")],
            overall_status="FAIL",
            pass_count=0,
            warn_count=0,
            fail_count=1,
            timestamp="2026-03-19T08:00:00+00:00",
        )
        formatted = format_pipeline_verification_report(report)
        assert "❌" in formatted


# ---------------------------------------------------------------------------
# ═══ INDICATOR_RANGES CONSTANT ═══
# ---------------------------------------------------------------------------

class TestIndicatorRangesConstant:
    """Tests for the INDICATOR_RANGES module constant."""

    def test_indicator_ranges_has_rsi(self) -> None:
        """INDICATOR_RANGES must include rsi_14."""
        assert "rsi_14" in INDICATOR_RANGES

    def test_indicator_ranges_rsi_critical(self) -> None:
        """rsi_14 must be marked critical=True."""
        assert INDICATOR_RANGES["rsi_14"]["critical"] is True

    def test_indicator_ranges_rsi_bounds(self) -> None:
        """rsi_14 must have min=0 and max=100."""
        assert INDICATOR_RANGES["rsi_14"]["min"] == 0
        assert INDICATOR_RANGES["rsi_14"]["max"] == 100

    def test_indicator_ranges_williams_r_bounds(self) -> None:
        """williams_r must have min=-100 and max=0."""
        assert INDICATOR_RANGES["williams_r"]["min"] == -100
        assert INDICATOR_RANGES["williams_r"]["max"] == 0

    def test_indicator_ranges_macd_no_bounds(self) -> None:
        """macd_line must have min=None and max=None (unbounded)."""
        assert INDICATOR_RANGES["macd_line"]["min"] is None
        assert INDICATOR_RANGES["macd_line"]["max"] is None
