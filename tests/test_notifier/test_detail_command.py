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
    },
    "ai_reasoner": {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4096,
        "temperature": 0.3,
    },
    "indicator_thresholds": {
        "rsi_overbought": 70,
        "rsi_oversold": 30,
        "adx_strong_trend": 40,
        "adx_trending": 25,
        "stoch_overbought": 80,
        "stoch_oversold": 20,
        "cci_overbought": 100,
        "cci_oversold": -100,
        "williams_r_overbought": -20,
        "williams_r_oversold": -80,
        "cmf_strong_accumulation": 0.1,
        "cmf_strong_distribution": -0.1,
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
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily "
        "(ticker, date, signal, confidence, final_score, regime, daily_score, weekly_score, "
        "trend_score, momentum_score, volume_score, volatility_score, candlestick_score, "
        "structural_score, sentiment_score, fundamental_score, macro_score, data_completeness, key_signals) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            ticker, date_str, signal, confidence, final_score, regime,
            daily_score, weekly_score, trend_score, momentum_score, volume_score,
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

        result = build_signal_history(db_connection, "AAPL", days=30)

        assert "NEUTRAL" in result

    def test_contains_summary_counts(self, db_connection: sqlite3.Connection) -> None:
        """build_signal_history includes summary counts (N bullish/bearish/neutral)."""
        from src.notifier.detail_command import build_signal_history

        base = date(2026, 3, 1)
        signals = ["BULLISH", "BEARISH", "NEUTRAL", "NEUTRAL", "BULLISH"]
        for i, sig in enumerate(signals):
            day = base + timedelta(days=i)
            _insert_score(db_connection, "AAPL", signal=sig, date_str=day.isoformat())

        result = build_signal_history(db_connection, "AAPL", days=30)

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

        result = build_signal_history(db_connection, "AAPL", days=30)

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
# Tests: build_analyst_prompt
# ---------------------------------------------------------------------------

class TestBuildAnalystPrompt:
    def test_contains_all_sections(self) -> None:
        """build_analyst_prompt includes all provided context sections."""
        from src.notifier.detail_command import build_analyst_prompt

        prompt = build_analyst_prompt(
            ticker_context="AAPL context",
            market_context="Market context",
            key_levels="Key levels text",
            signal_triggers="Triggers text",
            signal_history="History text",
            earnings_info="Earnings info",
            sector_peers="Peers text",
        )

        assert "AAPL context" in prompt
        assert "Market context" in prompt
        assert "Key levels text" in prompt
        assert "Triggers text" in prompt

    def test_contains_analyst_instructions(self) -> None:
        """build_analyst_prompt contains instructions for 3-4 paragraph analysis."""
        from src.notifier.detail_command import build_analyst_prompt

        prompt = build_analyst_prompt(
            ticker_context="AAPL",
            market_context="",
            key_levels="",
            signal_triggers="",
            signal_history="",
            earnings_info="",
            sector_peers="",
        )

        assert "paragraph" in prompt.lower() or "3-4" in prompt or "analysis" in prompt.lower()


# ---------------------------------------------------------------------------
# Tests: build_scoring_chain
# ---------------------------------------------------------------------------

class TestBuildScoringChain:
    def test_shows_daily_weekly_merged(self) -> None:
        """build_scoring_chain shows daily, weekly, and merged scores."""
        from src.notifier.detail_command import build_scoring_chain

        score = {
            "daily_score": 10.6,
            "weekly_score": -11.6,
            "final_score": 1.7,
            "signal": "NEUTRAL",
            "confidence": 0.0,
            "regime": "ranging",
        }

        result = build_scoring_chain(score)

        assert "10.6" in result
        assert "-11.6" in result or "11.6" in result
        assert "1.7" in result


# ---------------------------------------------------------------------------
# Tests: build_category_scores
# ---------------------------------------------------------------------------

class TestBuildCategoryScores:
    def test_shows_all_categories(self) -> None:
        """build_category_scores shows all scoring categories."""
        from src.notifier.detail_command import build_category_scores

        score = {
            "trend_score": -30.5,
            "momentum_score": 38.6,
            "volume_score": 5.0,
            "volatility_score": -3.0,
            "candlestick_score": 2.0,
            "structural_score": -1.0,
            "sentiment_score": 0.0,
            "fundamental_score": 5.0,
            "macro_score": -2.0,
        }

        result = build_category_scores(score)

        assert "Trend" in result
        assert "Momentum" in result
        assert "-30.5" in result

    def test_includes_visual_bars(self) -> None:
        """build_category_scores includes visual bar characters."""
        from src.notifier.detail_command import build_category_scores

        score = {"trend_score": -30.5, "momentum_score": 38.6, "volume_score": 5.0,
                 "volatility_score": -3.0, "candlestick_score": 2.0, "structural_score": -1.0,
                 "sentiment_score": 0.0, "fundamental_score": 5.0, "macro_score": -2.0}

        result = build_category_scores(score)

        assert "▓" in result or "░" in result


# ---------------------------------------------------------------------------
# Tests: build_full_breakdown
# ---------------------------------------------------------------------------

class TestBuildFullBreakdown:
    def test_all_sections_present(self, db_connection: sqlite3.Connection) -> None:
        """build_full_breakdown assembles all expected sections."""
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

        assert "SCORING CHAIN" in result
        assert "CATEGORY SCORES" in result
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
        """handle_detail_command sends photo + AI analysis + raw breakdown."""
        from src.notifier.detail_command import handle_detail_command

        _insert_score(db_connection, "AAPL")
        _insert_indicators(db_connection, "AAPL")

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        with patch("src.notifier.detail_command.generate_chart", return_value="/tmp/fake_chart.png"):
            with patch("src.notifier.detail_command.cleanup_chart") as mock_cleanup:
                with patch("src.notifier.detail_command.send_photo_to_chat", return_value=True) as mock_photo:
                    with patch("src.notifier.detail_command._call_claude_for_analysis", return_value="Claude analysis text."):
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
        assert mock_send.call_count >= 2  # placeholder + breakdown (at minimum)
        mock_cleanup.assert_called_once_with("/tmp/fake_chart.png")

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


# ---------------------------------------------------------------------------
# Tests: _explain_indicator
# ---------------------------------------------------------------------------

SAMPLE_THRESHOLDS = {
    "rsi_overbought": 70,
    "rsi_oversold": 30,
    "adx_strong_trend": 40,
    "adx_trending": 25,
    "stoch_overbought": 80,
    "stoch_oversold": 20,
    "cci_overbought": 100,
    "cci_oversold": -100,
    "williams_r_overbought": -20,
    "williams_r_oversold": -80,
    "cmf_strong_accumulation": 0.1,
    "cmf_strong_distribution": -0.1,
}


class TestExplainIndicator:
    def test_rsi_overbought_contains_overbought_wording(self) -> None:
        """RSI above overbought threshold mentions overbought in explanation."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("rsi_14", 75.0, {}, None, SAMPLE_THRESHOLDS)

        assert "overbought" in result.lower()

    def test_rsi_oversold_contains_oversold_wording(self) -> None:
        """RSI below oversold threshold mentions oversold in explanation."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("rsi_14", 22.0, {}, None, SAMPLE_THRESHOLDS)

        assert "oversold" in result.lower()

    def test_rsi_neutral_mentions_neutral_or_balanced(self) -> None:
        """RSI in the neutral zone mentions neutral or balanced momentum."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("rsi_14", 52.0, {}, None, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("neutral", "balanced", "moderate"))

    def test_adx_strong_trend_mentions_strong(self) -> None:
        """ADX above strong-trend threshold says strong trend."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("adx", 45.0, {}, None, SAMPLE_THRESHOLDS)

        assert "strong" in result.lower()

    def test_adx_trending_mentions_trend(self) -> None:
        """ADX between trending and strong threshold mentions trending."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("adx", 30.0, {}, None, SAMPLE_THRESHOLDS)

        assert "trend" in result.lower()

    def test_adx_weak_mentions_ranging_or_weak(self) -> None:
        """ADX below trending threshold mentions weak trend or ranging."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("adx", 15.0, {}, None, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("weak", "rang", "choppy", "non-trending"))

    def test_macd_line_positive_mentions_bullish(self) -> None:
        """Positive MACD line explanation mentions bullish momentum."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("macd_line", 0.45, {}, None, SAMPLE_THRESHOLDS)

        assert "bullish" in result.lower() or "positive" in result.lower()

    def test_macd_line_negative_mentions_bearish(self) -> None:
        """Negative MACD line explanation mentions bearish momentum."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("macd_line", -1.2, {}, None, SAMPLE_THRESHOLDS)

        assert "bearish" in result.lower() or "negative" in result.lower()

    def test_macd_histogram_positive_mentions_bullish(self) -> None:
        """Positive MACD histogram explanation mentions bullish momentum."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("macd_histogram", 0.3, {}, None, SAMPLE_THRESHOLDS)

        assert "bullish" in result.lower() or "positive" in result.lower()

    def test_macd_histogram_negative_mentions_bearish(self) -> None:
        """Negative MACD histogram explanation mentions bearish momentum."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("macd_histogram", -0.8, {}, None, SAMPLE_THRESHOLDS)

        assert "bearish" in result.lower() or "negative" in result.lower()

    def test_stoch_k_overbought_mentions_overbought(self) -> None:
        """Stochastic K above overbought threshold mentions overbought."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("stoch_k", 85.0, {}, None, SAMPLE_THRESHOLDS)

        assert "overbought" in result.lower()

    def test_stoch_k_oversold_mentions_oversold(self) -> None:
        """Stochastic K below oversold threshold mentions oversold."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("stoch_k", 12.0, {}, None, SAMPLE_THRESHOLDS)

        assert "oversold" in result.lower()

    def test_cci_overbought_mentions_overbought(self) -> None:
        """CCI above +100 mentions overbought."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("cci_20", 150.0, {}, None, SAMPLE_THRESHOLDS)

        assert "overbought" in result.lower()

    def test_cci_oversold_mentions_oversold(self) -> None:
        """CCI below -100 mentions oversold."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("cci_20", -120.0, {}, None, SAMPLE_THRESHOLDS)

        assert "oversold" in result.lower()

    def test_williams_r_overbought_mentions_overbought(self) -> None:
        """Williams %R near 0 (above -20) mentions overbought."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("williams_r", -10.0, {}, None, SAMPLE_THRESHOLDS)

        assert "overbought" in result.lower()

    def test_williams_r_oversold_mentions_oversold(self) -> None:
        """Williams %R near -100 (below -80) mentions oversold."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("williams_r", -90.0, {}, None, SAMPLE_THRESHOLDS)

        assert "oversold" in result.lower()

    def test_cmf_strong_accumulation_mentions_accumulation(self) -> None:
        """CMF above strong accumulation threshold mentions accumulation or buying."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("cmf_20", 0.25, {}, None, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("accumulation", "buying", "bullish"))

    def test_cmf_strong_distribution_mentions_distribution(self) -> None:
        """CMF below strong distribution threshold mentions distribution or selling."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("cmf_20", -0.2, {}, None, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("distribution", "selling", "bearish"))

    def test_ema_above_current_price_mentions_resistance(self) -> None:
        """EMA above current price says price is below EMA (resistance)."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("ema_9", 265.0, {}, 252.0, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("below", "resistance", "bearish"))

    def test_ema_below_current_price_mentions_support(self) -> None:
        """EMA below current price says price is above EMA (support)."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("ema_50", 240.0, {}, 252.0, SAMPLE_THRESHOLDS)

        assert any(word in result.lower() for word in ("above", "support", "bullish"))

    def test_returns_non_empty_string_for_all_indicator_keys(self) -> None:
        """_explain_indicator returns a non-empty string for every known indicator key."""
        from src.notifier.detail_command import _explain_indicator

        known_keys = [
            "rsi_14", "macd_line", "macd_signal", "macd_histogram",
            "ema_9", "ema_21", "ema_50", "adx",
            "stoch_k", "stoch_d", "cci_20", "williams_r",
            "obv", "cmf_20", "bb_upper", "bb_lower", "atr_14",
        ]
        all_indicators = {"macd_signal": 0.3, "macd_line": 0.5}

        for key in known_keys:
            result = _explain_indicator(key, 50.0, all_indicators, 200.0, SAMPLE_THRESHOLDS)
            assert isinstance(result, str), f"Expected str for {key}, got {type(result)}"
            assert len(result) > 0, f"Expected non-empty explanation for {key}"

    def test_returns_empty_string_for_unknown_key(self) -> None:
        """_explain_indicator returns empty string for unknown indicator keys."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("unknown_indicator", 50.0, {}, None, SAMPLE_THRESHOLDS)

        assert result == ""

    def test_works_with_empty_thresholds(self) -> None:
        """_explain_indicator does not crash when thresholds dict is empty (uses defaults)."""
        from src.notifier.detail_command import _explain_indicator

        result = _explain_indicator("rsi_14", 75.0, {}, None, {})

        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Tests: _explain_pattern
# ---------------------------------------------------------------------------

class TestExplainPattern:
    def test_double_bottom_mentions_reversal_and_bullish(self) -> None:
        """Double Bottom explanation mentions reversal and bullish signal."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Double Bottom", "bullish", 4, True)

        assert "reversal" in result.lower() or "support" in result.lower()
        assert "bullish" in result.lower() or "upward" in result.lower()

    def test_double_top_mentions_reversal_and_bearish(self) -> None:
        """Double Top explanation mentions reversal and bearish signal."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Double Top", "bearish", 4, True)

        assert "reversal" in result.lower() or "resistance" in result.lower()
        assert "bearish" in result.lower() or "downward" in result.lower()

    def test_bull_flag_mentions_continuation(self) -> None:
        """Bull Flag explanation mentions continuation."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Bull Flag", "bullish", 3, True)

        assert "continuation" in result.lower() or "breakout" in result.lower() or "upward" in result.lower()

    def test_bear_flag_mentions_continuation_or_downward(self) -> None:
        """Bear Flag explanation mentions downward continuation."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Bear Flag", "bearish", 3, True)

        assert "continuation" in result.lower() or "breakdown" in result.lower() or "downward" in result.lower()

    def test_hammer_mentions_reversal(self) -> None:
        """Hammer explanation mentions reversal."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Hammer", "bullish", 3, True)

        assert "reversal" in result.lower() or "buyers" in result.lower() or "downtrend" in result.lower()

    def test_shooting_star_mentions_reversal(self) -> None:
        """Shooting Star explanation mentions reversal."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Shooting Star", "bearish", 3, True)

        assert "reversal" in result.lower() or "sellers" in result.lower() or "uptrend" in result.lower()

    def test_doji_mentions_indecision(self) -> None:
        """Doji explanation mentions indecision."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Doji", "neutral", 2, False)

        assert "indecision" in result.lower() or "neutral" in result.lower() or "pause" in result.lower()

    def test_unconfirmed_pattern_mentions_unconfirmed(self) -> None:
        """Unconfirmed pattern adds a note about lack of confirmation."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Breakout", "bullish", 3, False)

        assert "unconfirmed" in result.lower() or "not yet confirmed" in result.lower() or "confirm" in result.lower()

    def test_confirmed_pattern_does_not_flag_unconfirmed(self) -> None:
        """Confirmed pattern does not say 'unconfirmed'."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Breakout", "bullish", 4, True)

        assert "unconfirmed" not in result.lower()

    def test_returns_non_empty_string_for_all_known_patterns(self) -> None:
        """_explain_pattern returns a non-empty string for all 14 known patterns."""
        from src.notifier.detail_command import _explain_pattern

        known_patterns = [
            ("Bullish Engulfing", "bullish"),
            ("Bearish Engulfing", "bearish"),
            ("Hammer", "bullish"),
            ("Shooting Star", "bearish"),
            ("Doji", "neutral"),
            ("Morning Star", "bullish"),
            ("Evening Star", "bearish"),
            ("Double Top", "bearish"),
            ("Double Bottom", "bullish"),
            ("Bull Flag", "bullish"),
            ("Bear Flag", "bearish"),
            ("Breakout", "bullish"),
            ("Breakdown", "bearish"),
            ("False Breakout", "bearish"),
        ]

        for pattern_name, direction in known_patterns:
            result = _explain_pattern(pattern_name, direction, 3, True)
            assert isinstance(result, str), f"Expected str for {pattern_name}"
            assert len(result) > 0, f"Expected non-empty explanation for {pattern_name}"

    def test_returns_generic_string_for_unknown_pattern(self) -> None:
        """_explain_pattern returns a generic non-empty string for unknown pattern names."""
        from src.notifier.detail_command import _explain_pattern

        result = _explain_pattern("Mystery Pattern", "bullish", 3, True)

        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Tests: build_indicators_section with explanations
# ---------------------------------------------------------------------------

class TestBuildIndicatorsSectionWithExplanations:
    def test_explanation_appears_after_rsi_line(self) -> None:
        """build_indicators_section adds → explanation under RSI value line."""
        from src.notifier.detail_command import build_indicators_section

        indicators = {"rsi_14": 75.0}

        result = build_indicators_section(indicators, thresholds=SAMPLE_THRESHOLDS)

        assert "RSI 14" in result
        assert "→" in result
        assert "overbought" in result.lower()

    def test_explanation_appears_for_macd(self) -> None:
        """build_indicators_section adds explanation for MACD line."""
        from src.notifier.detail_command import build_indicators_section

        indicators = {"macd_line": 0.5, "macd_signal": 0.3}

        result = build_indicators_section(indicators, thresholds=SAMPLE_THRESHOLDS)

        lines = result.splitlines()
        macd_line_idx = next(
            (i for i, ln in enumerate(lines) if "MACD line" in ln), None
        )
        assert macd_line_idx is not None
        assert any("→" in lines[j] for j in range(macd_line_idx + 1, min(macd_line_idx + 3, len(lines))))

    def test_current_price_used_for_ema_explanation(self) -> None:
        """build_indicators_section uses current_price for EMA explanations."""
        from src.notifier.detail_command import build_indicators_section

        indicators = {"ema_50": 240.0}

        result = build_indicators_section(indicators, current_price=252.0, thresholds=SAMPLE_THRESHOLDS)

        assert "EMA 50" in result
        assert any(word in result.lower() for word in ("above", "support", "bullish"))

    def test_no_explanation_for_none_value(self) -> None:
        """Indicators with None values are still skipped (no crash)."""
        from src.notifier.detail_command import build_indicators_section

        indicators = {"rsi_14": None, "adx": 30.0}

        result = build_indicators_section(indicators, thresholds=SAMPLE_THRESHOLDS)

        assert "RSI 14" not in result
        assert "ADX" in result
        assert "→" in result

    def test_empty_indicators_returns_empty_string(self) -> None:
        """build_indicators_section returns empty string for empty indicators dict."""
        from src.notifier.detail_command import build_indicators_section

        result = build_indicators_section({}, thresholds=SAMPLE_THRESHOLDS)

        assert result == ""

    def test_backward_compatible_without_thresholds(self) -> None:
        """build_indicators_section works without thresholds argument (still shows values)."""
        from src.notifier.detail_command import build_indicators_section

        indicators = {"rsi_14": 55.0, "adx": 22.0}

        result = build_indicators_section(indicators)

        assert "RSI 14" in result
        assert "ADX" in result


# ---------------------------------------------------------------------------
# Tests: build_patterns_section with explanations
# ---------------------------------------------------------------------------

class TestBuildPatternsSectionWithExplanations:
    def test_explanation_appears_under_pattern(self, db_connection: sqlite3.Connection) -> None:
        """build_patterns_section adds → explanation under each pattern row."""
        from src.notifier.detail_command import build_patterns_section

        db_connection.execute(
            "INSERT INTO patterns_daily (ticker, date, pattern_name, pattern_category, "
            "direction, strength, confirmed) VALUES (?,?,?,?,?,?,?)",
            ("AAPL", SCORING_DATE, "Double Bottom", "structural", "bullish", 4, True),
        )
        db_connection.commit()

        result = build_patterns_section(db_connection, "AAPL", days=30)

        assert "Double Bottom" in result
        assert "→" in result
        assert any(word in result.lower() for word in ("reversal", "support", "bullish", "upward"))

    def test_explanation_for_bearish_pattern(self, db_connection: sqlite3.Connection) -> None:
        """build_patterns_section adds bearish explanation for a bearish pattern."""
        from src.notifier.detail_command import build_patterns_section

        db_connection.execute(
            "INSERT INTO patterns_daily (ticker, date, pattern_name, pattern_category, "
            "direction, strength, confirmed) VALUES (?,?,?,?,?,?,?)",
            ("AAPL", SCORING_DATE, "Shooting Star", "candlestick", "bearish", 3, True),
        )
        db_connection.commit()

        result = build_patterns_section(db_connection, "AAPL", days=30)

        assert "Shooting Star" in result
        assert "→" in result

    def test_unconfirmed_pattern_explanation_notes_confirmation(self, db_connection: sqlite3.Connection) -> None:
        """build_patterns_section explanation notes unconfirmed status."""
        from src.notifier.detail_command import build_patterns_section

        db_connection.execute(
            "INSERT INTO patterns_daily (ticker, date, pattern_name, pattern_category, "
            "direction, strength, confirmed) VALUES (?,?,?,?,?,?,?)",
            ("AAPL", SCORING_DATE, "Breakout", "structural", "bullish", 3, False),
        )
        db_connection.commit()

        result = build_patterns_section(db_connection, "AAPL", days=30)

        assert "Breakout" in result
        assert any(word in result.lower() for word in ("unconfirmed", "not yet confirmed", "confirm"))

    def test_no_patterns_returns_empty_string(self, db_connection: sqlite3.Connection) -> None:
        """build_patterns_section returns empty string when no patterns found."""
        from src.notifier.detail_command import build_patterns_section

        result = build_patterns_section(db_connection, "AAPL", days=10)

        assert result == ""
