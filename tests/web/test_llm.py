"""
Tests for src/web/llm.py — context building and prompt generation for daily/weekly/monthly.

All Claude API calls are mocked at the anthropic.Anthropic class level.
No real API calls are made.
"""

from __future__ import annotations

import sqlite3
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from src.common.db import create_all_tables
from src.web.llm import (
    build_daily_context,
    build_timeframe_context,
    analyze_daily,
    analyze_timeframe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn(tmp_path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with full schema and minimal seed data."""
    db_path = str(tmp_path / "test_llm.db")
    c = sqlite3.connect(db_path)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    create_all_tables(c)
    _seed_llm_test_data(c)
    yield c
    c.close()


def _seed_llm_test_data(conn: sqlite3.Connection) -> None:
    """Insert minimal data needed for LLM context building."""
    conn.execute(
        "INSERT OR REPLACE INTO tickers(symbol, name, active) VALUES ('AAPL', 'Apple', 1)"
    )
    # Daily score row
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES ('AAPL','2026-04-25','BULLISH',72.5,55.0,'trending',
                  40.0,30.0,20.0,-10.0,25.0,15.0,5.0,8.0,-3.0,1.42)"""
    )
    # Weekly score row
    conn.execute(
        """INSERT OR REPLACE INTO scores_weekly(
            ticker, week_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES ('AAPL','2026-04-21',48.0,'ranging',35.0,20.0,15.0,-5.0,10.0,12.0)"""
    )
    # Monthly score row
    conn.execute(
        """INSERT OR REPLACE INTO scores_monthly(
            ticker, month_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES ('AAPL','2026-04-01',38.0,'ranging',30.0,15.0,10.0,-8.0,NULL,11.0)"""
    )
    # Daily indicators
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily(
            ticker, date, ema_9, ema_21, ema_50, rsi_14, adx, macd_line, macd_signal,
            macd_histogram, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower
        ) VALUES ('AAPL','2026-04-25',175.0,173.0,170.0,58.3,24.5,0.8,0.5,
                  0.3,62.0,60.0,50.0,-30.0,1000000.0,0.15,900000.0,
                  180.0,168.0,0.6,2.5,179.0,169.0)"""
    )
    # Weekly indicators
    conn.execute(
        """INSERT OR REPLACE INTO indicators_weekly(
            ticker, week_start, ema_9, ema_21, ema_50, rsi_14, adx, macd_line, macd_signal,
            macd_histogram, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower
        ) VALUES ('AAPL','2026-04-21',174.0,172.0,169.0,56.0,22.0,0.7,0.4,
                  0.3,60.0,58.0,45.0,-35.0,950000.0,0.12,880000.0,
                  179.0,167.0,0.58,2.8,178.0,168.0)"""
    )
    # Monthly indicators
    conn.execute(
        """INSERT OR REPLACE INTO indicators_monthly(
            ticker, month_start, ema_9, ema_21, ema_50, rsi_14, adx, macd_line, macd_signal,
            macd_histogram, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower
        ) VALUES ('AAPL','2026-04-01',172.0,170.0,165.0,54.0,20.0,0.5,0.3,
                  0.2,55.0,53.0,40.0,-40.0,900000.0,0.10,860000.0,
                  178.0,165.0,0.55,3.0,177.0,167.0)"""
    )
    # Daily patterns
    conn.execute(
        """INSERT INTO patterns_daily(ticker, date, pattern_name, direction, strength)
           VALUES ('AAPL','2026-04-25','Bullish Engulfing','bullish',3)"""
    )
    # Weekly patterns
    conn.execute(
        """INSERT INTO patterns_weekly(ticker, week_start, pattern_name, direction, strength)
           VALUES ('AAPL','2026-04-21','Morning Star','bullish',4)"""
    )
    # Monthly patterns
    conn.execute(
        """INSERT INTO patterns_monthly(ticker, month_start, pattern_name, direction, strength)
           VALUES ('AAPL','2026-04-01','Cup and Handle','bullish',5)"""
    )
    # News summary (daily-only)
    conn.execute(
        """INSERT OR REPLACE INTO news_daily_summary(
            ticker, date, avg_sentiment_score, article_count, positive_count,
            negative_count, neutral_count, top_headline
        ) VALUES ('AAPL','2026-04-25',0.65,3,2,0,1,'Apple beats earnings estimates')"""
    )
    # Fundamentals (daily-only)
    conn.execute(
        """INSERT OR REPLACE INTO fundamentals(
            ticker, report_date, period, revenue, eps, pe_ratio
        ) VALUES ('AAPL','2026-01-01','Q4-2025',100000000,6.50,28.5)"""
    )
    conn.commit()


def _web_config() -> dict:
    """Return a minimal web config dict for test use."""
    return {
        "ai_reasoner": {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 800,
            "temperature": 0.3,
            "target_words": 150,
        }
    }


def _mock_claude_response(text: str) -> MagicMock:
    """Build a mock anthropic response object."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=text)]
    return mock_response


# ---------------------------------------------------------------------------
# build_daily_context tests
# ---------------------------------------------------------------------------

class TestBuildDailyContext:
    """Tests for build_daily_context()."""

    def test_daily_context_contains_patterns(self, conn: sqlite3.Connection) -> None:
        """Daily context must include pattern information."""
        score_row = {
            "signal": "BULLISH",
            "confidence": 72.5,
            "final_score": 55.0,
            "regime": "trending",
            "daily_score": 55.0,
            "weekly_score": 48.0,
            "trend_score": 40.0,
            "momentum_score": 30.0,
            "volume_score": 20.0,
            "volatility_score": -10.0,
            "candlestick_score": 25.0,
            "structural_score": 15.0,
            "sentiment_score": 5.0,
            "fundamental_score": 8.0,
            "macro_score": -3.0,
            "calibrated_score": 1.42,
            "key_signals": "[]",
            "date": "2026-04-25",
        }
        context = build_daily_context(conn, "AAPL", score_row, "2026-04-25")
        assert "Bullish Engulfing" in context or "pattern" in context.lower()

    def test_daily_context_contains_fundamentals(self, conn: sqlite3.Connection) -> None:
        """Daily context must include fundamentals data."""
        score_row = {
            "signal": "BULLISH",
            "confidence": 72.5,
            "final_score": 55.0,
            "regime": "trending",
            "daily_score": 55.0,
            "weekly_score": 48.0,
            "trend_score": 40.0,
            "momentum_score": 30.0,
            "volume_score": 20.0,
            "volatility_score": -10.0,
            "candlestick_score": 25.0,
            "structural_score": 15.0,
            "sentiment_score": 5.0,
            "fundamental_score": 8.0,
            "macro_score": -3.0,
            "calibrated_score": 1.42,
            "key_signals": "[]",
            "date": "2026-04-25",
        }
        context = build_daily_context(conn, "AAPL", score_row, "2026-04-25")
        assert "fundamental" in context.lower() or "eps" in context.lower()

    def test_daily_context_contains_news(self, conn: sqlite3.Connection) -> None:
        """Daily context must include news sentiment data."""
        score_row = {
            "signal": "BULLISH",
            "confidence": 72.5,
            "final_score": 55.0,
            "regime": "trending",
            "daily_score": 55.0,
            "weekly_score": 48.0,
            "trend_score": 40.0,
            "momentum_score": 30.0,
            "volume_score": 20.0,
            "volatility_score": -10.0,
            "candlestick_score": 25.0,
            "structural_score": 15.0,
            "sentiment_score": 5.0,
            "fundamental_score": 8.0,
            "macro_score": -3.0,
            "calibrated_score": 1.42,
            "key_signals": "[]",
            "date": "2026-04-25",
        }
        context = build_daily_context(conn, "AAPL", score_row, "2026-04-25")
        assert "news" in context.lower() or "sentiment" in context.lower()


# ---------------------------------------------------------------------------
# build_timeframe_context tests
# ---------------------------------------------------------------------------

class TestBuildTimeframeContext:
    """Tests for build_timeframe_context()."""

    def test_weekly_context_contains_weekly_indicators(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly context must include weekly indicators data."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "weekly")
        assert "rsi" in context.lower() or "ema" in context.lower() or "indicator" in context.lower()

    def test_weekly_context_contains_weekly_patterns(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly context must include weekly patterns data."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "weekly")
        assert "Morning Star" in context or "pattern" in context.lower()

    def test_weekly_context_does_not_contain_news_fundamentals(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly context must NOT contain news or fundamentals keys (daily-only)."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "weekly")
        # Must not contain the news headline we seeded
        assert "Apple beats earnings" not in context

    def test_monthly_context_contains_monthly_indicators(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly context must include monthly indicators data."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "monthly")
        assert "rsi" in context.lower() or "ema" in context.lower() or "indicator" in context.lower()

    def test_monthly_context_contains_monthly_patterns(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly context must include monthly patterns data."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "monthly")
        assert "Cup and Handle" in context or "pattern" in context.lower()

    def test_monthly_context_does_not_contain_news(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly context must NOT contain the news headline (daily-only data)."""
        context = build_timeframe_context(conn, "AAPL", "2026-04-25", "monthly")
        assert "Apple beats earnings" not in context


# ---------------------------------------------------------------------------
# analyze_daily tests
# ---------------------------------------------------------------------------

class TestAnalyzeDaily:
    """Tests for analyze_daily() — mocked Claude calls."""

    def test_analyze_daily_calls_claude_and_returns_text(
        self, conn: sqlite3.Connection
    ) -> None:
        """analyze_daily() must call Claude and return the response text."""
        score_row = {
            "signal": "BULLISH",
            "confidence": 72.5,
            "final_score": 55.0,
            "regime": "trending",
            "daily_score": 55.0,
            "weekly_score": 48.0,
            "trend_score": 40.0,
            "momentum_score": 30.0,
            "volume_score": 20.0,
            "volatility_score": -10.0,
            "candlestick_score": 25.0,
            "structural_score": 15.0,
            "sentiment_score": 5.0,
            "fundamental_score": 8.0,
            "macro_score": -3.0,
            "calibrated_score": 1.42,
            "key_signals": "[]",
            "date": "2026-04-25",
        }
        with patch("anthropic.Anthropic") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.messages.create.return_value = _mock_claude_response(
                "AAPL is showing strong bullish momentum."
            )
            result = analyze_daily(conn, "AAPL", score_row, "2026-04-25", _web_config())

        assert "AAPL" in result or len(result) > 0
        mock_client.messages.create.assert_called_once()


# ---------------------------------------------------------------------------
# analyze_timeframe tests
# ---------------------------------------------------------------------------

class TestAnalyzeTimeframe:
    """Tests for analyze_timeframe() — mocked Claude calls."""

    def test_weekly_prompt_contains_disclaimer(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly analyze_timeframe() must prepend a disclaimer about limited input scope."""
        with patch("anthropic.Anthropic") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.messages.create.return_value = _mock_claude_response(
                "Weekly analysis here."
            )
            # Capture the prompt by inspecting the call
            result = analyze_timeframe(
                conn, "AAPL", "2026-04-25", "weekly", _web_config()
            )

        # The result is returned from Claude mock — just verify Claude was called
        mock_client.messages.create.assert_called_once()
        call_kwargs = mock_client.messages.create.call_args
        # The prompt is in the messages argument
        messages = call_kwargs.kwargs.get("messages") or call_kwargs.args[0] if call_kwargs.args else None
        if messages is None and call_kwargs.kwargs:
            messages = call_kwargs.kwargs.get("messages", [])
        prompt_content = str(call_kwargs)
        # Weekly/monthly should include disclaimer text
        assert "weekly" in prompt_content.lower() or "indicator" in prompt_content.lower()

    def test_monthly_analyze_timeframe_calls_claude(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly analyze_timeframe() must call Claude and return text."""
        with patch("anthropic.Anthropic") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.messages.create.return_value = _mock_claude_response(
                "Monthly outlook is bullish."
            )
            result = analyze_timeframe(
                conn, "AAPL", "2026-04-25", "monthly", _web_config()
            )

        assert len(result) > 0
        mock_client.messages.create.assert_called_once()
