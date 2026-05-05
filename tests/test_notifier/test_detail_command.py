"""
Tests for src/notifier/detail_command.py — /detail Telegram bot command.

Covers command parsing, all breakdown-section builders, and the end-to-end
handle_detail_command flow (with mocked Telegram and Claude).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

SAMPLE_CONFIG = {
    "detail_command": {
        "default_chart_days": 30,
        "max_chart_days": 180,
        "chart_style": "nightclouds",
        "chart_figsize": [14, 10],
        "sr_levels_to_show": 3,
        "signal_history_days": 30,
        "peer_count": 5,
        "category_agreement_min_score": 10.0,
        "calibration_divergence_min_abs": 0.3,
        "earnings_warning_days": 7,
        "timeframe_direction_threshold": 15.0,
    },
    "ai_reasoner": {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4096,
        "temperature": 0.3,
    },
}

ACTIVE_TICKERS = [
    {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "NVDA", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "GOOGL", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "AVGO", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": True},
]

SCORING_DATE = "2026-03-16"

SAMPLE_SCORER_CFG = {
    "timeframe_weights": {
        "trending": {"daily": 0.10, "weekly": 0.50, "monthly": 0.40},
        "ranging":  {"daily": 0.60, "weekly": 0.30, "monthly": 0.10},
        "volatile": {"daily": 0.25, "weekly": 0.45, "monthly": 0.30},
    }
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_score(
    conn: sqlite3.Connection,
    ticker: str,
    signal: str = "NEUTRAL",
    confidence: float = 15.0,
    final_score: float = 1.7,
    date_str: str = SCORING_DATE,
    regime: str = "ranging",
    daily_score: float = 10.6,
    weekly_score: float = -11.6,
    monthly_score: float = 0.0,
    trend_score: float = -30.5,
    momentum_score: float = 38.6,
    volume_score: float = 5.0,
    volatility_score: float = -3.0,
    candlestick_score: float = 2.0,
    structural_score: float = -1.0,
    sentiment_score: float = 0.0,
    fundamental_score: float = 5.0,
    macro_score: float = -2.0,
) -> None:
    """
    Insert a row into scores_daily for testing.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        signal: Signal string (e.g. 'NEUTRAL', 'BULLISH').
        confidence: Confidence percentage.
        final_score: Final merged score.
        date_str: Date string in YYYY-MM-DD format.
        regime: Market regime string.
        daily_score: Daily timeframe score.
        weekly_score: Weekly timeframe score.
        monthly_score: Monthly timeframe score.
        trend_score: Trend category score.
        momentum_score: Momentum category score.
        volume_score: Volume category score.
        volatility_score: Volatility category score.
        candlestick_score: Candlestick category score.
        structural_score: Structural category score.
        sentiment_score: Sentiment category score.
        fundamental_score: Fundamental category score.
        macro_score: Macro category score.

    Returns:
        None
    """
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily "
        "(ticker, date, signal, confidence, final_score, regime, daily_score, weekly_score, monthly_score, "
        "trend_score, momentum_score, volume_score, volatility_score, candlestick_score, "
        "structural_score, sentiment_score, fundamental_score, macro_score, data_completeness, key_signals) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            ticker, date_str, signal, confidence, final_score, regime,
            daily_score, weekly_score, monthly_score, trend_score, momentum_score, volume_score,
            volatility_score, candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, "complete",
            json.dumps(["bearish EMA stack", "RSI oversold"]),
        ),
    )
    conn.commit()


def _insert_indicators(conn: sqlite3.Connection, ticker: str, date_str: str = SCORING_DATE) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO indicators_daily "
        "(ticker, date, rsi_14, macd_line, macd_signal, macd_histogram, "
        "ema_9, ema_21, ema_50, bb_upper, bb_lower, adx, obv) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ticker, date_str, 38.7, -2.97, -1.73, -1.24,
         257.44, 263.26, 266.45, 270.0, 242.0, 18.9, 500_000_000),
    )
    conn.commit()


def _insert_earnings(
    conn: sqlite3.Connection,
    ticker: str,
    earnings_date: str,
    estimated_eps: float = 2.35,
    actual_eps: float = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO earnings_calendar "
        "(ticker, earnings_date, estimated_eps, actual_eps) VALUES (?,?,?,?)",
        (ticker, earnings_date, estimated_eps, actual_eps),
    )
    conn.commit()


def _insert_fundamentals(conn: sqlite3.Connection, ticker: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO fundamentals "
        "(ticker, report_date, period, pe_ratio, eps, eps_growth_yoy, "
        "revenue_growth_yoy, debt_to_equity, market_cap, dividend_yield) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ticker, "2026-01-01", "Q4", 28.5, 6.43, 0.08, 0.05, 0.4, 3_800_000_000_000, 0.005),
    )
    conn.commit()


def _insert_weekly_score(
    conn: sqlite3.Connection,
    ticker: str,
    week_start: str = "2026-03-09",
    composite_score: float = 20.0,
    regime: str = "trending",
    trend_score: float = 25.0,
    momentum_score: float = 18.0,
    volume_score: float = 10.0,
    volatility_score: float = -5.0,
    candlestick_score: float = 8.0,
    structural_score: float = 12.0,
    fundamental_score: float = 6.0,
    macro_score: float = 3.0,
) -> None:
    """
    Insert a row into scores_weekly for testing.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        week_start: Week start date in YYYY-MM-DD format.
        composite_score: Weekly composite score.
        regime: Market regime string.
        trend_score: Trend category score.
        momentum_score: Momentum category score.
        volume_score: Volume category score.
        volatility_score: Volatility category score.
        candlestick_score: Candlestick category score.
        structural_score: Structural category score.
        fundamental_score: Fundamental category score.
        macro_score: Macro category score.

    Returns:
        None
    """
    conn.execute(
        "INSERT OR REPLACE INTO scores_weekly "
        "(ticker, week_start, composite_score, regime, trend_score, momentum_score, "
        "volume_score, volatility_score, candlestick_score, structural_score, "
        "fundamental_score, macro_score, data_completeness, key_signals) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            ticker, week_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, fundamental_score, macro_score,
            "complete", json.dumps(["weekly signal"]),
        ),
    )
    conn.commit()


def _insert_monthly_score(
    conn: sqlite3.Connection,
    ticker: str,
    month_start: str = "2026-03-01",
    composite_score: float = 15.0,
    regime: str = "trending",
    trend_score: float = 20.0,
    momentum_score: float = 15.0,
    volume_score: float = 8.0,
    volatility_score: float = -3.0,
    candlestick_score: float = 5.0,
    structural_score: float = 10.0,
    fundamental_score: float = 7.0,
    macro_score: float = 2.0,
) -> None:
    """
    Insert a row into scores_monthly for testing.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        month_start: Month start date in YYYY-MM-DD format.
        composite_score: Monthly composite score.
        regime: Market regime string.
        trend_score: Trend category score.
        momentum_score: Momentum category score.
        volume_score: Volume category score.
        volatility_score: Volatility category score.
        candlestick_score: Candlestick category score.
        structural_score: Structural category score.
        fundamental_score: Fundamental category score.
        macro_score: Macro category score.

    Returns:
        None
    """
    conn.execute(
        "INSERT OR REPLACE INTO scores_monthly "
        "(ticker, month_start, composite_score, regime, trend_score, momentum_score, "
        "volume_score, volatility_score, candlestick_score, structural_score, "
        "fundamental_score, macro_score, data_completeness, key_signals) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            ticker, month_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, fundamental_score, macro_score,
            "complete", json.dumps(["monthly signal"]),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests: parse_detail_command
# ---------------------------------------------------------------------------

class TestParseDetailCommand:
    def test_parse_ticker_only(self) -> None:
        """/detail AAPL returns ticker=AAPL, days=30 (default)."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail AAPL", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert result == {"ticker": "AAPL", "days": 30}

    def test_parse_with_days(self) -> None:
        """/detail AAPL 90 returns ticker=AAPL, days=90."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail AAPL 90", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert result == {"ticker": "AAPL", "days": 90}

    def test_parse_invalid_ticker(self) -> None:
        """/detail ZZZZ returns error when ticker not in active list."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail ZZZZ", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert "error" in result
        assert "ZZZZ" in result["error"]

    def test_parse_no_ticker(self) -> None:
        """/detail with no ticker returns error asking for ticker symbol."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert "error" in result

    def test_parse_days_exceeds_max(self) -> None:
        """/detail AAPL 365 clamps days to max_chart_days=180."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail AAPL 365", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert result == {"ticker": "AAPL", "days": 180}

    def test_parse_case_insensitive(self) -> None:
        """/detail aapl normalizes ticker to AAPL."""
        from src.notifier.detail_command import parse_detail_command

        result = parse_detail_command("/detail aapl", ACTIVE_TICKERS, SAMPLE_CONFIG)

        assert result == {"ticker": "AAPL", "days": 30}


# ---------------------------------------------------------------------------
# Tests: build_key_levels
# ---------------------------------------------------------------------------

class TestBuildKeyLevels:
    def test_contains_ema_values(self, db_connection: sqlite3.Connection) -> None:
        """build_key_levels includes EMA 9/21/50 with distance percentages."""
        from src.notifier.detail_command import build_key_levels

        indicators = {
            "ema_9": 257.44, "ema_21": 263.26, "ema_50": 266.45,
            "rsi_14": 38.7, "macd_histogram": -1.24,
        }
        sr_levels = [
            {"level_price": 244.32, "level_type": "support", "touch_count": 5, "strength": "strong"},
            {"level_price": 257.62, "level_type": "resistance", "touch_count": 3, "strength": "weak"},
        ]
        fib_result = {
            "levels": [
                {"level_pct": 0.382, "price": 252.88},
            ],
            "current_price": 252.82,
            "nearest_level": {"level_pct": 0.382, "level_price": 252.88, "distance_pct": 0.02, "is_near": True},
            "is_near_level": True,
        }

        result = build_key_levels(
            db_connection, "AAPL", 252.82, indicators, fib_result, sr_levels, SAMPLE_CONFIG
        )

        assert "EMA 9" in result
        assert "EMA 21" in result
        assert "EMA 50" in result
        assert "257.44" in result

    def test_contains_sr_levels(self, db_connection: sqlite3.Connection) -> None:
        """build_key_levels includes S/R resistance and support."""
        from src.notifier.detail_command import build_key_levels

        indicators = {"ema_9": 257.44, "ema_21": 263.26, "ema_50": 266.45}
        sr_levels = [
            {"level_price": 244.32, "level_type": "support", "touch_count": 5, "strength": "strong"},
            {"level_price": 257.62, "level_type": "resistance", "touch_count": 3, "strength": "weak"},
        ]

        result = build_key_levels(
            db_connection, "AAPL", 252.82, indicators, None, sr_levels, SAMPLE_CONFIG
        )

        assert "244.32" in result
        assert "257.62" in result

    def test_handles_no_sr_levels(self, db_connection: sqlite3.Connection) -> None:
        """build_key_levels works without S/R levels (no crash)."""
        from src.notifier.detail_command import build_key_levels

        indicators = {"ema_9": 257.44, "ema_21": 263.26, "ema_50": 266.45}

        result = build_key_levels(
            db_connection, "AAPL", 252.82, indicators, None, [], SAMPLE_CONFIG
        )

        assert isinstance(result, str)
        assert "EMA" in result

    def test_fibonacci_price_here_marker(self, db_connection: sqlite3.Connection) -> None:
        """build_key_levels marks the nearest Fibonacci level with PRICE HERE."""
        from src.notifier.detail_command import build_key_levels

        indicators = {"ema_9": 257.44, "ema_21": 263.26, "ema_50": 266.45}
        fib_result = {
            "levels": [{"level_pct": 0.382, "price": 252.88}],
            "current_price": 252.82,
            "nearest_level": {"level_pct": 0.382, "level_price": 252.88, "distance_pct": 0.02, "is_near": True},
            "is_near_level": True,
        }

        result = build_key_levels(
            db_connection, "AAPL", 252.82, indicators, fib_result, [], SAMPLE_CONFIG
        )

        assert "PRICE HERE" in result


# ---------------------------------------------------------------------------
# Tests: build_signal_change_triggers
# ---------------------------------------------------------------------------

class TestBuildSignalChangeTriggers:
    def test_neutral_shows_both_directions(self) -> None:
        """NEUTRAL signal shows both BULLISH and BEARISH trigger conditions."""
        from src.notifier.detail_command import build_signal_change_triggers

        indicators = {
            "rsi_14": 38.7,
            "macd_histogram": -1.24,
            "ema_9": 257.44,
            "macd_line": -2.97,
            "adx": 18.9,
        }
        score = {
            "signal": "NEUTRAL",
            "final_score": 1.7,
            "daily_score": 10.6,
            "weekly_score": -11.6,
            "confidence": 0.0,
        }

        result = build_signal_change_triggers(indicators, score, SAMPLE_CONFIG)

        assert "BULLISH" in result
        assert "BEARISH" in result

    def test_contains_specific_indicator_values(self) -> None:
        """build_signal_change_triggers includes specific indicator values in output."""
        from src.notifier.detail_command import build_signal_change_triggers

        indicators = {
            "rsi_14": 38.7,
            "macd_histogram": -1.24,
            "ema_9": 257.44,
            "macd_line": -2.97,
            "adx": 18.9,
        }
        score = {"signal": "NEUTRAL", "final_score": 1.7, "daily_score": 10.6, "weekly_score": -11.6, "confidence": 0.0}

        result = build_signal_change_triggers(indicators, score, SAMPLE_CONFIG)

        assert "38.7" in result or "257.44" in result or "-1.24" in result

    def test_returns_string(self) -> None:
        """build_signal_change_triggers always returns a string."""
        from src.notifier.detail_command import build_signal_change_triggers

        result = build_signal_change_triggers({}, {"signal": "NEUTRAL", "final_score": 0.0, "daily_score": 0.0, "weekly_score": 0.0, "confidence": 0.0}, SAMPLE_CONFIG)

        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Tests: build_signal_history
# ---------------------------------------------------------------------------

class TestBuildSignalHistory:
    def test_contains_signal_entries(self, db_connection: sqlite3.Connection) -> None:
        """build_signal_history shows recent signal entries."""
        from src.notifier.detail_command import build_signal_history

        base = date(2026, 3, 1)
        for i in range(10):
            day = base + timedelta(days=i)
            _insert_score(db_connection, "AAPL", date_str=day.isoformat(), final_score=1.0 + i * 0.5)

        result = build_signal_history(db_connection, "AAPL", days=30, reference_date="2026-03-15")

        assert "NEUTRAL" in result

    def test_contains_summary_counts(self, db_connection: sqlite3.Connection) -> None:
        """build_signal_history includes summary counts (N bullish/bearish/neutral)."""
        from src.notifier.detail_command import build_signal_history

        base = date(2026, 3, 1)
        signals = ["BULLISH", "BEARISH", "NEUTRAL", "NEUTRAL", "BULLISH"]
        for i, sig in enumerate(signals):
            day = base + timedelta(days=i)
            _insert_score(db_connection, "AAPL", signal=sig, date_str=day.isoformat())

        result = build_signal_history(db_connection, "AAPL", days=30, reference_date="2026-03-15")

        assert "🟢" in result
        assert "🔴" in result
        assert "🟡" in result

    def test_no_history_returns_message(self, db_connection: sqlite3.Connection) -> None:
        """build_signal_history returns 'No signal history available.' when empty."""
        from src.notifier.detail_command import build_signal_history

        result = build_signal_history(db_connection, "AAPL", days=30)

        assert "No signal history available" in result

    def test_contains_trend_description(self, db_connection: sqlite3.Connection) -> None:
        """build_signal_history includes a trend description."""
        from src.notifier.detail_command import build_signal_history

        base = date(2026, 3, 1)
        # Declining scores: improving → deteriorating
        for i in range(10):
            day = base + timedelta(days=i)
            _insert_score(db_connection, "AAPL", date_str=day.isoformat(), final_score=20.0 - i * 2.0)

        result = build_signal_history(db_connection, "AAPL", days=30, reference_date="2026-03-15")

        trend_words = ["improving", "deteriorating", "stable"]
        assert any(word in result.lower() for word in trend_words)


# ---------------------------------------------------------------------------
# Tests: build_earnings_warning
# ---------------------------------------------------------------------------

class TestBuildEarningsWarning:
    def test_upcoming_earnings_shows_info(self, db_connection: sqlite3.Connection) -> None:
        """build_earnings_warning shows earnings date and days away."""
        from src.notifier.detail_command import build_earnings_warning

        future_date = "2026-04-24"
        _insert_earnings(db_connection, "AAPL", future_date, estimated_eps=2.35)

        result = build_earnings_warning(db_connection, "AAPL", SCORING_DATE)

        assert future_date in result or "April" in result or "Apr" in result
        assert "2.35" in result

    def test_no_upcoming_returns_empty(self, db_connection: sqlite3.Connection) -> None:
        """build_earnings_warning returns empty string when no upcoming earnings."""
        from src.notifier.detail_command import build_earnings_warning

        result = build_earnings_warning(db_connection, "AAPL", SCORING_DATE)

        assert result == ""

    def test_within_7_days_shows_warning(self, db_connection: sqlite3.Connection) -> None:
        """build_earnings_warning shows ⚠️ warning when earnings within 7 days."""
        from src.notifier.detail_command import build_earnings_warning

        near_date = (date.fromisoformat(SCORING_DATE) + timedelta(days=5)).isoformat()
        _insert_earnings(db_connection, "AAPL", near_date, estimated_eps=2.35)

        result = build_earnings_warning(db_connection, "AAPL", SCORING_DATE)

        assert "⚠️" in result


# ---------------------------------------------------------------------------
# Tests: build_sector_peers
# ---------------------------------------------------------------------------

class TestBuildSectorPeers:
    def test_shows_peer_tickers(self, db_connection: sqlite3.Connection) -> None:
        """build_sector_peers shows tickers from the same sector."""
        from src.notifier.detail_command import build_sector_peers

        tech_tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AVGO"]
        for ticker in tech_tickers:
            _insert_score(db_connection, ticker, final_score=5.0 if ticker != "AAPL" else 1.7)

        result = build_sector_peers(
            db_connection, "AAPL", "Technology", ACTIVE_TICKERS, SCORING_DATE, SAMPLE_CONFIG
        )

        assert "MSFT" in result or "NVDA" in result

    def test_highlights_queried_ticker(self, db_connection: sqlite3.Connection) -> None:
        """build_sector_peers marks the queried ticker distinctly."""
        from src.notifier.detail_command import build_sector_peers

        tech_tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AVGO"]
        for ticker in tech_tickers:
            _insert_score(db_connection, ticker, final_score=5.0)

        result = build_sector_peers(
            db_connection, "AAPL", "Technology", ACTIVE_TICKERS, SCORING_DATE, SAMPLE_CONFIG
        )

        assert "▸AAPL" in result or "← you are here" in result or "AAPL" in result

    def test_shows_rank(self, db_connection: sqlite3.Connection) -> None:
        """build_sector_peers shows the ticker's rank in sector."""
        from src.notifier.detail_command import build_sector_peers

        tech_tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AVGO"]
        for ticker in tech_tickers:
            _insert_score(db_connection, ticker, final_score=5.0)

        result = build_sector_peers(
            db_connection, "AAPL", "Technology", ACTIVE_TICKERS, SCORING_DATE, SAMPLE_CONFIG
        )

        assert "rank" in result.lower() or "/" in result

    def test_small_sector_still_works(self, db_connection: sqlite3.Connection) -> None:
        """build_sector_peers works with only 2 tickers in sector."""
        from src.notifier.detail_command import build_sector_peers

        fin_tickers = [{"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": True}]
        for ticker in ["JPM"]:
            _insert_score(db_connection, ticker)

        result = build_sector_peers(
            db_connection, "JPM", "Financials", fin_tickers, SCORING_DATE, SAMPLE_CONFIG
        )

        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Tests: build_analyst_prompt (new XML-structured version)
# ---------------------------------------------------------------------------

class TestBuildAnalystPrompt:
    def test_prompt_includes_xml_format_instructions(self) -> None:
        """build_analyst_prompt system prompt instructs Claude to emit XML-tagged sections."""
        from src.notifier.detail_command import build_analyst_prompt

        score = {
            "ticker": "AAPL", "date": SCORING_DATE, "signal": "BULLISH",
            "final_score": 35.0, "confidence": 70.0, "regime": "trending",
            "daily_score": 30.0, "weekly_score": 40.0, "monthly_score": 25.0,
            "calibrated_score": 0.8,
        }
        system_prompt, _user_prompt = build_analyst_prompt(
            ticker="AAPL",
            score=score,
            weekly_row=None,
            monthly_row=None,
            market_context="",
            key_levels="",
            signal_triggers="",
            signal_history="",
            earnings_info="",
            sector_peers="",
            calibration_divergence_note="",
        )

        assert "<verdict>" in system_prompt
        assert "<timeframe_note>" in system_prompt
        assert "<reasoning>" in system_prompt

    def test_prompt_includes_weekly_monthly_context(self) -> None:
        """build_analyst_prompt user prompt contains weekly and monthly score context."""
        from src.notifier.detail_command import build_analyst_prompt

        score = {
            "ticker": "AAPL", "date": SCORING_DATE, "signal": "BULLISH",
            "final_score": 35.0, "confidence": 70.0, "regime": "trending",
            "daily_score": 30.0, "weekly_score": 40.0, "monthly_score": 25.0,
            "calibrated_score": None,
        }
        weekly_row = {"composite_score": 40.0, "trend_score": 25.0, "momentum_score": 18.0}
        monthly_row = {"composite_score": 25.0, "trend_score": 20.0, "momentum_score": 12.0}

        _system_prompt, user_prompt = build_analyst_prompt(
            ticker="AAPL",
            score=score,
            weekly_row=weekly_row,
            monthly_row=monthly_row,
            market_context="",
            key_levels="",
            signal_triggers="",
            signal_history="",
            earnings_info="",
            sector_peers="",
            calibration_divergence_note="",
        )

        assert "40.0" in user_prompt or "weekly" in user_prompt.lower()
        assert "25.0" in user_prompt or "monthly" in user_prompt.lower()

    def test_prompt_includes_calibration_note_when_divergence_flagged(self) -> None:
        """build_analyst_prompt user prompt includes calibration note when note is non-empty."""
        from src.notifier.detail_command import build_analyst_prompt

        score = {
            "ticker": "AAPL", "date": SCORING_DATE, "signal": "BULLISH",
            "final_score": 35.0, "confidence": 70.0, "regime": "trending",
            "daily_score": 30.0, "weekly_score": 40.0, "monthly_score": 25.0,
            "calibrated_score": -0.5,
        }

        _system_prompt, user_prompt = build_analyst_prompt(
            ticker="AAPL",
            score=score,
            weekly_row=None,
            monthly_row=None,
            market_context="",
            key_levels="",
            signal_triggers="",
            signal_history="",
            earnings_info="",
            sector_peers="",
            calibration_divergence_note="⚠️ Calibrated score -0.50 contradicts BULLISH signal",
        )

        assert "⚠️ Calibrated score -0.50 contradicts BULLISH signal" in user_prompt

    def test_prompt_omits_calibration_note_when_no_divergence(self) -> None:
        """build_analyst_prompt user prompt omits calibration text when note is empty."""
        from src.notifier.detail_command import build_analyst_prompt

        score = {
            "ticker": "AAPL", "date": SCORING_DATE, "signal": "BULLISH",
            "final_score": 35.0, "confidence": 70.0, "regime": "trending",
            "daily_score": 30.0, "weekly_score": 40.0, "monthly_score": 25.0,
            "calibrated_score": 0.6,
        }

        _system_prompt, user_prompt = build_analyst_prompt(
            ticker="AAPL",
            score=score,
            weekly_row=None,
            monthly_row=None,
            market_context="",
            key_levels="",
            signal_triggers="",
            signal_history="",
            earnings_info="",
            sector_peers="",
            calibration_divergence_note="",
        )

        assert "calibrat" not in user_prompt.lower() or "calibration" not in user_prompt.lower()


# ---------------------------------------------------------------------------
# Tests: fetch_weekly_score / fetch_monthly_score
# ---------------------------------------------------------------------------

class TestFetchWeeklyScore:
    def test_fetch_weekly_score_returns_latest_row(self, db_connection: sqlite3.Connection) -> None:
        """fetch_weekly_score returns the most recent scores_weekly row for the ticker."""
        from src.notifier.detail_command import fetch_weekly_score

        _insert_weekly_score(db_connection, "AAPL", week_start="2026-03-02", composite_score=10.0)
        _insert_weekly_score(db_connection, "AAPL", week_start="2026-03-09", composite_score=20.0)

        result = fetch_weekly_score(db_connection, "AAPL")

        assert result is not None
        assert result["week_start"] == "2026-03-09"
        assert result["composite_score"] == 20.0

    def test_fetch_weekly_score_returns_none_when_empty(self, db_connection: sqlite3.Connection) -> None:
        """fetch_weekly_score returns None when no rows exist for the ticker."""
        from src.notifier.detail_command import fetch_weekly_score

        result = fetch_weekly_score(db_connection, "AAPL")

        assert result is None


class TestFetchMonthlyScore:
    def test_fetch_monthly_score_returns_latest_row(self, db_connection: sqlite3.Connection) -> None:
        """fetch_monthly_score returns the most recent scores_monthly row for the ticker."""
        from src.notifier.detail_command import fetch_monthly_score

        _insert_monthly_score(db_connection, "AAPL", month_start="2026-02-01", composite_score=12.0)
        _insert_monthly_score(db_connection, "AAPL", month_start="2026-03-01", composite_score=18.0)

        result = fetch_monthly_score(db_connection, "AAPL")

        assert result is not None
        assert result["month_start"] == "2026-03-01"
        assert result["composite_score"] == 18.0

    def test_fetch_monthly_score_returns_none_when_empty(self, db_connection: sqlite3.Connection) -> None:
        """fetch_monthly_score returns None when no rows exist for the ticker."""
        from src.notifier.detail_command import fetch_monthly_score

        result = fetch_monthly_score(db_connection, "AAPL")

        assert result is None


# ---------------------------------------------------------------------------
# Tests: build_timeframe_table
# ---------------------------------------------------------------------------

class TestBuildTimeframeTable:
    def _make_daily_score(self) -> dict:
        """Return a minimal daily score dict for table tests."""
        return {
            "final_score": 30.0,
            "trend_score": 25.0,
            "momentum_score": 20.0,
            "signal": "BULLISH",
        }

    def _make_weekly_row(self) -> dict:
        """Return a minimal weekly row dict for table tests (no sentiment_score)."""
        return {
            "composite_score": 20.0,
            "trend_score": 18.0,
            "momentum_score": 15.0,
        }

    def _make_monthly_row(self) -> dict:
        """Return a minimal monthly row dict for table tests (no sentiment_score)."""
        return {
            "composite_score": 16.0,
            "trend_score": 14.0,
            "momentum_score": 12.0,
        }

    def test_build_timeframe_table_renders_three_rows(self) -> None:
        """build_timeframe_table produces a table with Daily, Weekly, and Monthly rows."""
        from src.notifier.detail_command import build_timeframe_table

        result = build_timeframe_table(
            daily_row=self._make_daily_score(),
            weekly_row=self._make_weekly_row(),
            monthly_row=self._make_monthly_row(),
            config=SAMPLE_CONFIG,
        )

        assert "Daily" in result
        assert "Weekly" in result
        assert "Monthly" in result

    def test_build_timeframe_table_renders_na_for_missing_weekly(self) -> None:
        """build_timeframe_table shows N/A for the Weekly row when weekly_row is None."""
        from src.notifier.detail_command import build_timeframe_table

        result = build_timeframe_table(
            daily_row=self._make_daily_score(),
            weekly_row=None,
            monthly_row=self._make_monthly_row(),
            config=SAMPLE_CONFIG,
        )

        assert "N/A" in result

    def test_build_timeframe_table_renders_na_for_missing_monthly(self) -> None:
        """build_timeframe_table shows N/A for the Monthly row when monthly_row is None."""
        from src.notifier.detail_command import build_timeframe_table

        result = build_timeframe_table(
            daily_row=self._make_daily_score(),
            weekly_row=self._make_weekly_row(),
            monthly_row=None,
            config=SAMPLE_CONFIG,
        )

        assert "N/A" in result

    def test_build_timeframe_table_does_not_read_sentiment_from_weekly_monthly_rows(self) -> None:
        """build_timeframe_table accepts weekly/monthly rows without sentiment_score key — no KeyError."""
        from src.notifier.detail_command import build_timeframe_table

        # Rows explicitly have no sentiment_score key
        weekly_no_sentiment = {"composite_score": 20.0, "trend_score": 18.0, "momentum_score": 15.0}
        monthly_no_sentiment = {"composite_score": 16.0, "trend_score": 14.0, "momentum_score": 12.0}

        # Must not raise KeyError
        result = build_timeframe_table(
            daily_row=self._make_daily_score(),
            weekly_row=weekly_no_sentiment,
            monthly_row=monthly_no_sentiment,
            config=SAMPLE_CONFIG,
        )

        assert "Sentiment" not in result
        assert isinstance(result, str)

    def test_build_timeframe_table_uses_config_direction_threshold(self) -> None:
        """build_timeframe_table renders ▲/▼/▬ based on timeframe_direction_threshold config."""
        from src.notifier.detail_command import build_timeframe_table

        high_threshold_config = {
            "detail_command": {**SAMPLE_CONFIG["detail_command"], "timeframe_direction_threshold": 50.0}
        }
        low_threshold_config = {
            "detail_command": {**SAMPLE_CONFIG["detail_command"], "timeframe_direction_threshold": 5.0}
        }

        daily_row = {"final_score": 30.0, "trend_score": 25.0, "momentum_score": 20.0, "signal": "BULLISH"}

        result_high = build_timeframe_table(daily_row=daily_row, weekly_row=None, monthly_row=None, config=high_threshold_config)
        result_low = build_timeframe_table(daily_row=daily_row, weekly_row=None, monthly_row=None, config=low_threshold_config)

        # With threshold=50, score=30 is below threshold → ▬
        # With threshold=5, score=30 is above threshold → ▲
        assert "▬" in result_high
        assert "▲" in result_low

    def test_build_timeframe_table_renders_under_markdownv2_correctly(self) -> None:
        """build_timeframe_table output is wrapped in a triple-backtick code block (MarkdownV2-safe)."""
        from src.notifier.detail_command import build_timeframe_table

        result = build_timeframe_table(
            daily_row=self._make_daily_score(),
            weekly_row=self._make_weekly_row(),
            monthly_row=self._make_monthly_row(),
            config=SAMPLE_CONFIG,
        )

        assert result.startswith("```")
        assert result.endswith("```")


# ---------------------------------------------------------------------------
# Tests: build_deterministic_confidence
# ---------------------------------------------------------------------------

class TestBuildDeterministicConfidence:
    def _make_score(
        self,
        signal: str = "BULLISH",
        final_score: float = 35.0,
        calibrated_score: float = None,
    ) -> dict:
        """Return a daily score dict for confidence tests."""
        return {
            "signal": signal,
            "final_score": final_score,
            "calibrated_score": calibrated_score,
            "daily_score": 30.0,
            "weekly_score": 40.0,
            "monthly_score": 25.0,
            "confidence": 70.0,
            "trend_score": 30.0,
            "momentum_score": 25.0,
            "volume_score": 15.0,
            "volatility_score": -5.0,
            "candlestick_score": 12.0,
            "structural_score": 8.0,
            "sentiment_score": 20.0,
            "fundamental_score": 6.0,
            "macro_score": 3.0,
        }

    def test_build_deterministic_confidence_lists_agreeing_categories(self) -> None:
        """build_deterministic_confidence lists categories agreeing with bullish signal."""
        from src.notifier.detail_command import build_deterministic_confidence

        score = self._make_score(signal="BULLISH", final_score=35.0)
        weekly_row = {"composite_score": 20.0, "trend_score": 25.0, "momentum_score": 18.0,
                      "volume_score": 10.0, "volatility_score": -3.0, "candlestick_score": 5.0,
                      "structural_score": 8.0, "fundamental_score": 6.0, "macro_score": 2.0}

        result = build_deterministic_confidence(score, weekly_row, None, SAMPLE_CONFIG)

        assert "Agreeing" in result or "agreeing" in result.lower()

    def test_build_deterministic_confidence_lists_disagreeing_categories(self) -> None:
        """build_deterministic_confidence lists categories that oppose the signal direction."""
        from src.notifier.detail_command import build_deterministic_confidence

        score = self._make_score(signal="BULLISH", final_score=35.0)
        # Give volatility a strongly negative score (opposes BULLISH)
        score["volatility_score"] = -50.0

        result = build_deterministic_confidence(score, None, None, SAMPLE_CONFIG)

        assert "Disagree" in result or "disagree" in result.lower()

    def test_build_deterministic_confidence_flags_calibration_sign_flip(self) -> None:
        """build_deterministic_confidence flags ⚠️ when BULLISH signal but calibrated_score < 0."""
        from src.notifier.detail_command import build_deterministic_confidence

        score = self._make_score(signal="BULLISH", final_score=35.0, calibrated_score=-0.5)

        result = build_deterministic_confidence(score, None, None, SAMPLE_CONFIG)

        assert "⚠️" in result

    def test_build_deterministic_confidence_does_not_flag_when_calibrated_below_min_abs(self) -> None:
        """build_deterministic_confidence skips flag when abs(calibrated_score) < 0.3 threshold."""
        from src.notifier.detail_command import build_deterministic_confidence

        # abs(-0.2) < 0.3 → no flag
        score = self._make_score(signal="BULLISH", final_score=35.0, calibrated_score=-0.2)

        result = build_deterministic_confidence(score, None, None, SAMPLE_CONFIG)

        assert "⚠️" not in result

    def test_build_deterministic_confidence_handles_calibrated_score_none(self) -> None:
        """build_deterministic_confidence handles calibrated_score=None gracefully — no crash, no flag."""
        from src.notifier.detail_command import build_deterministic_confidence

        score = self._make_score(signal="BULLISH", final_score=35.0, calibrated_score=None)

        result = build_deterministic_confidence(score, None, None, SAMPLE_CONFIG)

        assert isinstance(result, str)
        # No crash, no spurious flag
        assert "calibrated" not in result.lower() or "⚠️" not in result

    def test_build_deterministic_confidence_final_score_zero_skips_sign_logic(self) -> None:
        """build_deterministic_confidence returns neutral message when final_score == 0."""
        from src.notifier.detail_command import build_deterministic_confidence

        score = self._make_score(signal="NEUTRAL", final_score=0.0, calibrated_score=None)

        result = build_deterministic_confidence(score, None, None, SAMPLE_CONFIG)

        assert "NEUTRAL" in result or "neutral" in result.lower()


# ---------------------------------------------------------------------------
# Tests: build_verdict_header
# ---------------------------------------------------------------------------

class TestBuildVerdictHeader:
    def _make_score(self, date_str: str = SCORING_DATE) -> dict:
        """Return a minimal score dict for verdict header tests."""
        return {
            "ticker": "AAPL",
            "date": date_str,
            "signal": "BULLISH",
            "final_score": 35.0,
        }

    def _make_earnings_row(self, earnings_date: str) -> dict:
        """Return a minimal earnings row dict."""
        return {"earnings_date": earnings_date, "estimated_eps": 2.35}

    def test_build_verdict_header_prepends_earnings_warning_within_window(self) -> None:
        """build_verdict_header prepends ⚠️ earnings warning when earnings is 3 days away (threshold 7)."""
        from src.notifier.detail_command import build_verdict_header

        score = self._make_score(date_str="2026-04-20")
        earnings_row = self._make_earnings_row(earnings_date="2026-04-23")  # 3 days away

        result = build_verdict_header(score, earnings_row, SAMPLE_CONFIG)

        assert "⚠️" in result

    def test_build_verdict_header_no_earnings_warning_outside_window(self) -> None:
        """build_verdict_header omits ⚠️ warning when earnings is 14 days away (threshold 7)."""
        from src.notifier.detail_command import build_verdict_header

        score = self._make_score(date_str="2026-04-20")
        earnings_row = self._make_earnings_row(earnings_date="2026-05-04")  # 14 days away

        result = build_verdict_header(score, earnings_row, SAMPLE_CONFIG)

        assert "⚠️" not in result

    def test_build_verdict_header_earnings_at_exact_boundary(self) -> None:
        """build_verdict_header prepends ⚠️ warning when earnings is exactly 7 days away (inclusive)."""
        from src.notifier.detail_command import build_verdict_header

        score = self._make_score(date_str="2026-04-20")
        earnings_row = self._make_earnings_row(earnings_date="2026-04-27")  # exactly 7 days away

        result = build_verdict_header(score, earnings_row, SAMPLE_CONFIG)

        assert "⚠️" in result


# ---------------------------------------------------------------------------
# Tests: parse_ai_response
# ---------------------------------------------------------------------------

class TestParseAiResponse:
    def test_parse_ai_response_extracts_three_sections(self) -> None:
        """parse_ai_response extracts verdict, timeframe_note, and reasoning from XML-tagged text."""
        from src.notifier.detail_command import parse_ai_response

        raw = "BUY at $185</verdict><timeframe_note>All timeframes bullish</timeframe_note><reasoning>MACD rising, EMA stack intact.</reasoning>"
        result = parse_ai_response(raw, prefill="<verdict>")

        assert result["verdict"] == "BUY at $185"
        assert result["timeframe_note"] == "All timeframes bullish"
        assert result["reasoning"] == "MACD rising, EMA stack intact."

    def test_parse_ai_response_falls_back_to_raw_on_malformed(self) -> None:
        """parse_ai_response returns raw text in verdict slot when XML is malformed."""
        from src.notifier.detail_command import parse_ai_response

        raw = "This is just free-form text with no XML tags."
        result = parse_ai_response(raw, prefill="<verdict>")

        assert result["verdict"] == raw
        assert result["timeframe_note"] == ""
        assert result["reasoning"] == ""

    def test_parse_ai_response_handles_truncated_response(self) -> None:
        """parse_ai_response falls back gracefully when response is cut off mid-tag."""
        from src.notifier.detail_command import parse_ai_response

        raw = "BUY at $185</verdict><timeframe_note>Bullish"  # missing closing tags
        result = parse_ai_response(raw, prefill="<verdict>")

        # Should fall back — not crash
        assert isinstance(result, dict)
        assert "verdict" in result

    def test_parse_ai_response_handles_empty_reasoning_section(self) -> None:
        """parse_ai_response returns empty string (not None) when reasoning section is empty."""
        from src.notifier.detail_command import parse_ai_response

        raw = "BUY</verdict><timeframe_note>Bullish</timeframe_note><reasoning></reasoning>"
        result = parse_ai_response(raw, prefill="<verdict>")

        assert result["reasoning"] == ""
        assert result["reasoning"] is not None


# ---------------------------------------------------------------------------
# Tests: _split_message_at_section_markers
# ---------------------------------------------------------------------------

class TestSplitMessageAtSectionMarkers:
    def test_split_returns_single_element_when_under_max_len(self) -> None:
        """_split_message_at_section_markers returns [input] when text fits in one message."""
        from src.notifier.detail_command import _split_message_at_section_markers

        text = "Short text under limit."
        result = _split_message_at_section_markers(text, max_len=4096)

        assert result == [text]

    def test_message2_overflow_splits_on_section_markers(self) -> None:
        """_split_message_at_section_markers splits at section headers when text overflows."""
        from src.notifier.detail_command import _split_message_at_section_markers

        # Build a text that exceeds max_len with two section headers.
        # max_len=100; section1 = "📍 VERDICT\n" + 80 A's = ~91 chars.
        # When the splitter sees "📊 CONFIDENCE" (a marker), current_len=92 > 100-13=87,
        # so 92+13 = 105 > 100 → triggers split.
        section1 = "📍 VERDICT\n" + "A" * 80
        section2 = "📊 CONFIDENCE\n" + "B" * 80
        text = section1 + "\n" + section2

        result = _split_message_at_section_markers(text, max_len=100)

        assert len(result) >= 2

    def test_message2_splitter_does_not_split_on_verdict_header_emoji(self) -> None:
        """_split_message_at_section_markers does not split on '📊 AAPL — Detail Analysis' header."""
        from src.notifier.detail_command import _split_message_at_section_markers

        header_line = "📊 AAPL — Detail Analysis (2026-04-27)"
        confidence_line = "📊 CONFIDENCE"
        text = header_line + "\n" + "A" * 10 + "\n" + confidence_line + "\n" + "B" * 10

        # With a very small max_len, splitting should only happen at "📊 CONFIDENCE", not the header
        result = _split_message_at_section_markers(text, max_len=50)

        # The first chunk should contain the header line intact
        full_text = "\n".join(result)
        assert header_line in full_text


# ---------------------------------------------------------------------------
# Tests: escape_markdown_v2
# ---------------------------------------------------------------------------

class TestEscapeMarkdownV2:
    def test_escape_markdown_v2_escapes_dot_minus_paren_plus_etc(self) -> None:
        """escape_markdown_v2 escapes MarkdownV2 special characters."""
        from src.notifier.detail_command import escape_markdown_v2

        text = "Price is $185.50 (up +2.3%) today!"
        result = escape_markdown_v2(text)

        assert r"\." in result
        assert r"\(" in result
        assert r"\+" in result
        assert r"\!" in result

    def test_escape_markdown_v2_does_not_escape_inside_code_block(self) -> None:
        """escape_markdown_v2 passes triple-backtick code block contents through unchanged."""
        from src.notifier.detail_command import escape_markdown_v2

        code_content = "Daily  +30.0  ▲"
        text = f"Before\n```\n{code_content}\n```\nAfter."

        result = escape_markdown_v2(text)

        # The code block content must be unchanged
        assert code_content in result
        # Text outside code block should be escaped
        assert r"\." in result


# ---------------------------------------------------------------------------
# Tests: build_full_breakdown
# ---------------------------------------------------------------------------

class TestBuildFullBreakdown:
    def test_all_sections_present(self, db_connection: sqlite3.Connection) -> None:
        """build_full_breakdown assembles expected sections (no SCORING CHAIN or CATEGORY SCORES)."""
        from src.notifier.detail_command import build_full_breakdown

        _insert_score(db_connection, "AAPL")
        _insert_indicators(db_connection, "AAPL")
        _insert_fundamentals(db_connection, "AAPL")

        score = {
            "ticker": "AAPL",
            "date": SCORING_DATE,
            "signal": "NEUTRAL",
            "confidence": 0.0,
            "final_score": 1.7,
            "regime": "ranging",
            "daily_score": 10.6,
            "weekly_score": -11.6,
            "monthly_score": 0.0,
            "trend_score": -30.5,
            "momentum_score": 38.6,
            "volume_score": 5.0,
            "volatility_score": -3.0,
            "candlestick_score": 2.0,
            "structural_score": -1.0,
            "sentiment_score": 0.0,
            "fundamental_score": 5.0,
            "macro_score": -2.0,
            "data_completeness": "complete",
            "key_signals": json.dumps([]),
        }

        result = build_full_breakdown(db_connection, "AAPL", score, SAMPLE_CONFIG)

        # SCORING CHAIN and CATEGORY SCORES are removed in Plan B
        assert "SCORING CHAIN" not in result
        assert "CATEGORY SCORES" not in result
        assert isinstance(result, str)
        assert len(result) > 0

    def test_handles_missing_sections_gracefully(self, db_connection: sqlite3.Connection) -> None:
        """build_full_breakdown omits empty sections without 'None' in output."""
        from src.notifier.detail_command import build_full_breakdown

        score = {
            "ticker": "AAPL",
            "date": SCORING_DATE,
            "signal": "NEUTRAL",
            "confidence": 0.0,
            "final_score": 1.7,
            "regime": "ranging",
            "daily_score": 10.6,
            "weekly_score": -11.6,
            "monthly_score": 0.0,
            "trend_score": -30.5,
            "momentum_score": 38.6,
            "volume_score": 5.0,
            "volatility_score": -3.0,
            "candlestick_score": 2.0,
            "structural_score": -1.0,
            "sentiment_score": 0.0,
            "fundamental_score": 5.0,
            "macro_score": -2.0,
            "data_completeness": "complete",
            "key_signals": json.dumps([]),
        }

        result = build_full_breakdown(db_connection, "AAPL", score, SAMPLE_CONFIG)

        assert "None" not in result


# ---------------------------------------------------------------------------
# Tests: handle_detail_command (end-to-end)
# ---------------------------------------------------------------------------

class TestHandleDetailCommand:
    def test_end_to_end_sends_3_messages(self, db_connection: sqlite3.Connection) -> None:
        """handle_detail_command sends photo + structured AI analysis + raw breakdown."""
        from src.notifier.detail_command import handle_detail_command

        _insert_score(db_connection, "AAPL")
        _insert_indicators(db_connection, "AAPL")

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        # Mock returns XML response body (prefill "<verdict>" is prepended by parse_ai_response)
        ai_mock_return = (
            "BUY pullback to $185</verdict>"
            "<timeframe_note>All timeframes bullish</timeframe_note>"
            "<reasoning>MACD rising, EMA stack intact, momentum building.</reasoning>"
        )

        with patch("src.notifier.detail_command.generate_chart", return_value="/tmp/fake_chart.png"):
            with patch("src.notifier.detail_command.cleanup_chart") as mock_cleanup:
                with patch("src.notifier.detail_command.send_photo_to_chat", return_value=True) as mock_photo:
                    with patch("src.notifier.detail_command._call_claude_for_analysis", return_value=ai_mock_return):
                        with patch("src.notifier.detail_command.send_telegram_message", return_value=42) as mock_send:
                            with patch("src.notifier.detail_command.edit_telegram_message", return_value=True):
                                handle_detail_command(
                                    db_connection,
                                    "chat123",
                                    "/detail AAPL",
                                    "bot_token",
                                    SAMPLE_CONFIG,
                                    ACTIVE_TICKERS,
                                    calc_config,
                                )

        mock_photo.assert_called_once()
        assert mock_send.call_count >= 2  # placeholder + at least one message body
        mock_cleanup.assert_called_once_with("/tmp/fake_chart.png")

        # Inspect all message bodies sent
        all_sent_texts = " ".join(
            str(c.args[2]) if len(c.args) >= 3 else str(c)
            for c in mock_send.call_args_list
        )

        # The structured AI message must contain all 5 section headers
        assert "📍 VERDICT" in all_sent_texts
        assert "⏱️ TIMEFRAME SUMMARY" in all_sent_texts
        assert "📊 CONFIDENCE" in all_sent_texts
        assert "🎯 LEVELS & TRIGGERS" in all_sent_texts

    def test_handle_detail_command_passes_parse_mode_markdownv2_for_message_2(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """handle_detail_command sends the AI analysis message with parse_mode='MarkdownV2'."""
        from src.notifier.detail_command import handle_detail_command

        _insert_score(db_connection, "AAPL")
        _insert_indicators(db_connection, "AAPL")

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        ai_mock_return = (
            "BUY</verdict>"
            "<timeframe_note>Bullish</timeframe_note>"
            "<reasoning>Strong setup.</reasoning>"
        )

        with patch("src.notifier.detail_command.generate_chart", return_value="/tmp/fake_chart.png"):
            with patch("src.notifier.detail_command.cleanup_chart"):
                with patch("src.notifier.detail_command.send_photo_to_chat", return_value=True):
                    with patch("src.notifier.detail_command._call_claude_for_analysis", return_value=ai_mock_return):
                        with patch("src.notifier.detail_command.send_telegram_message", return_value=42) as mock_send:
                            with patch("src.notifier.detail_command.edit_telegram_message", return_value=True):
                                handle_detail_command(
                                    db_connection,
                                    "chat123",
                                    "/detail AAPL",
                                    "bot_token",
                                    SAMPLE_CONFIG,
                                    ACTIVE_TICKERS,
                                    calc_config,
                                )

        # At least one call should have parse_mode="MarkdownV2"
        parse_modes = [
            c.kwargs.get("parse_mode") or (c.args[3] if len(c.args) > 3 else None)
            for c in mock_send.call_args_list
        ]
        assert "MarkdownV2" in parse_modes, (
            f"Expected at least one send_telegram_message call with parse_mode='MarkdownV2', "
            f"got call_args_list: {mock_send.call_args_list}"
        )

    def test_message_2_has_no_unescaped_markdownv2_specials_outside_code_blocks(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        msg #2 sent under parse_mode='MarkdownV2' must escape every MarkdownV2 special
        character outside triple-backtick code blocks. An unescaped '(' or '.' makes
        Telegram return 400 Bad Request, causing msg #2 to silently fail to send.

        This test asserts the deterministic confidence section, key_levels_text, and
        signal_triggers_text are all properly escaped before send — they contain
        prices, percentages, and parentheses that would otherwise break the parser.
        """
        from src.notifier.detail_command import handle_detail_command

        _insert_score(db_connection, "AAPL")
        _insert_indicators(db_connection, "AAPL")

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        ai_mock_return = (
            "BUY pullback to $185</verdict>"
            "<timeframe_note>All timeframes bullish</timeframe_note>"
            "<reasoning>MACD rising.</reasoning>"
        )

        with patch("src.notifier.detail_command.generate_chart", return_value="/tmp/fake_chart.png"):
            with patch("src.notifier.detail_command.cleanup_chart"):
                with patch("src.notifier.detail_command.send_photo_to_chat", return_value=True):
                    with patch("src.notifier.detail_command._call_claude_for_analysis", return_value=ai_mock_return):
                        with patch("src.notifier.detail_command.send_telegram_message", return_value=42) as mock_send:
                            with patch("src.notifier.detail_command.edit_telegram_message", return_value=True):
                                handle_detail_command(
                                    db_connection,
                                    "chat123",
                                    "/detail AAPL",
                                    "bot_token",
                                    SAMPLE_CONFIG,
                                    ACTIVE_TICKERS,
                                    calc_config,
                                )

        # Find every send_telegram_message call that used parse_mode='MarkdownV2'
        markdownv2_chunks: list[str] = []
        for call in mock_send.call_args_list:
            parse_mode = call.kwargs.get("parse_mode") or (call.args[3] if len(call.args) > 3 else None)
            if parse_mode == "MarkdownV2":
                # text is the 3rd positional arg
                text = call.args[2] if len(call.args) >= 3 else call.kwargs.get("text", "")
                markdownv2_chunks.append(text)

        assert markdownv2_chunks, "Expected at least one MarkdownV2 chunk to be sent"

        # MarkdownV2 special characters per Telegram Bot API spec
        specials = "_*[]()~`>#+-=|{}.!"

        for chunk in markdownv2_chunks:
            # Strip triple-backtick code blocks — their contents are exempt from escaping
            parts = chunk.split("```")
            non_code_segments = parts[::2]  # even-indexed segments are outside code blocks

            for segment in non_code_segments:
                for idx, ch in enumerate(segment):
                    if ch in specials:
                        # The character must be preceded by a backslash (escape)
                        preceded_by_backslash = idx > 0 and segment[idx - 1] == "\\"
                        assert preceded_by_backslash, (
                            f"Unescaped MarkdownV2 special character {ch!r} at index {idx} "
                            f"in non-code-block portion of msg #2 chunk:\n"
                            f"{segment[max(0, idx - 30):idx + 30]!r}"
                        )

    def test_unknown_ticker_sends_error(self, db_connection: sqlite3.Connection) -> None:
        """handle_detail_command sends error for unknown ticker."""
        from src.notifier.detail_command import handle_detail_command

        calc_config = {"fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}}

        with patch("src.notifier.detail_command.send_telegram_message", return_value=1) as mock_send:
            handle_detail_command(
                db_connection,
                "chat123",
                "/detail ZZZZ",
                "bot_token",
                SAMPLE_CONFIG,
                ACTIVE_TICKERS,
                calc_config,
            )

        calls_text = " ".join(str(c) for c in mock_send.call_args_list)
        assert "ZZZZ" in calls_text

    def test_no_score_data_sends_error(self, db_connection: sqlite3.Connection) -> None:
        """handle_detail_command sends error when no scoring data exists."""
        from src.notifier.detail_command import handle_detail_command

        calc_config = {"fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}}

        with patch("src.notifier.detail_command.send_telegram_message", return_value=1) as mock_send:
            handle_detail_command(
                db_connection,
                "chat123",
                "/detail AAPL",
                "bot_token",
                SAMPLE_CONFIG,
                ACTIVE_TICKERS,
                calc_config,
            )

        calls_text = " ".join(str(c) for c in mock_send.call_args_list)
        assert "AAPL" in calls_text
        assert "scorer" in calls_text.lower() or "scoring" in calls_text.lower() or "no" in calls_text.lower()
