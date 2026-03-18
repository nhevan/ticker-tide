"""
Tests for src/notifier/ai_reasoner.py — AI reasoning layer using Claude API.

Covers context building, prompt construction, Claude API calls (mocked),
and the full reasoning pipeline for qualifying tickers.
"""

from __future__ import annotations

import json
import re
import sqlite3
from unittest.mock import MagicMock, call, patch

import pytest

SAMPLE_CONFIG = {
    "ai_reasoner": {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4096,
        "temperature": 0.3,
    },
    "telegram": {
        "confidence_threshold": 70,
        "always_include_flips": True,
        "max_tickers_per_section": 10,
    },
}

SCORING_DATE = "2025-01-15"

_SAMPLE_CLAUDE_RESPONSE = (
    "AAPL shows a NEUTRAL signal with low confidence of 15%. "
    "RSI at 38.7 is oversold but the bearish EMA stack confirms the downtrend is intact. "
    "Price is sitting on Fibonacci support; watch for a bounce in this ranging market (ADX 18.9)."
)

_FALLBACK = "AI analysis unavailable — see raw scores above."


# ---------------------------------------------------------------------------
# Helpers for inserting test data
# ---------------------------------------------------------------------------

def _make_score_dict(
    ticker: str = "AAPL",
    signal: str = "NEUTRAL",
    confidence: float = 15.0,
    final_score: float = 1.7,
    regime: str = "ranging",
) -> dict:
    """Create a complete score dict matching scores_daily schema."""
    return {
        "ticker": ticker,
        "date": SCORING_DATE,
        "signal": signal,
        "confidence": confidence,
        "final_score": final_score,
        "regime": regime,
        "daily_score": 2.0,
        "weekly_score": 1.2,
        "trend_score": -10.0,
        "momentum_score": 5.0,
        "volume_score": 3.0,
        "volatility_score": -2.0,
        "candlestick_score": 8.0,
        "structural_score": -5.0,
        "sentiment_score": 12.0,
        "fundamental_score": 7.0,
        "macro_score": -4.0,
        "data_completeness": json.dumps({
            "news": True,
            "fundamentals": True,
            "weekly": True,
            "filings": False,
            "short_interest": True,
            "earnings": True,
        }),
        "key_signals": json.dumps([
            "RSI oversold — bullish reversal signal",
            "Bearish EMA stack — price below all EMAs",
            "On-balance volume accumulating (bullish)",
        ]),
    }


def _insert_score(conn: sqlite3.Connection, score: dict) -> None:
    """Insert a score dict into scores_daily."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily
           (ticker, date, signal, confidence, final_score, regime, daily_score, weekly_score,
            trend_score, momentum_score, volume_score, volatility_score, candlestick_score,
            structural_score, sentiment_score, fundamental_score, macro_score,
            data_completeness, key_signals)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            score["ticker"], score["date"], score["signal"], score["confidence"],
            score["final_score"], score["regime"], score["daily_score"], score["weekly_score"],
            score["trend_score"], score["momentum_score"], score["volume_score"],
            score["volatility_score"], score["candlestick_score"], score["structural_score"],
            score["sentiment_score"], score["fundamental_score"], score["macro_score"],
            score["data_completeness"], score["key_signals"],
        ),
    )
    conn.commit()


def _insert_ohlcv(conn: sqlite3.Connection, ticker: str, dt: str, close: float = 155.0) -> None:
    """Insert a minimal ohlcv_daily row."""
    conn.execute(
        "INSERT OR REPLACE INTO ohlcv_daily "
        "(ticker, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, dt, close * 0.99, close * 1.01, close * 0.98, close, 50_000_000),
    )
    conn.commit()


def _insert_indicators(conn: sqlite3.Connection, ticker: str, dt: str) -> None:
    """Insert a full indicators_daily row with realistic AAPL-like values."""
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily
           (ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal, macd_histogram,
            adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower)
           VALUES (?, ?, 152.0, 150.0, 148.0, -0.5, -0.3, -0.2, 18.9, 38.7,
                   22.0, 25.0, -80.0, -78.0, 9500000.0, -0.12, 4200000.0,
                   160.0, 145.0, 0.22, 2.1, 161.0, 144.0)""",
        (ticker, dt),
    )
    conn.commit()


def _insert_fundamentals(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert a fundamentals row."""
    conn.execute(
        """INSERT OR REPLACE INTO fundamentals
           (ticker, report_date, period, pe_ratio, eps, eps_growth_yoy,
            revenue, revenue_growth_yoy, debt_to_equity, market_cap)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, "2024-12-31", "Q4", 28.5, 6.72, 0.08, 124_300_000_000, 0.05, 1.87, 3_500_000_000_000),
    )
    conn.commit()


def _insert_news_summary(conn: sqlite3.Connection, ticker: str, dt: str) -> None:
    """Insert a news_daily_summary row."""
    conn.execute(
        """INSERT OR REPLACE INTO news_daily_summary
           (ticker, date, avg_sentiment_score, article_count, positive_count,
            negative_count, neutral_count, top_headline, filing_flag)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, dt, 0.35, 7, 4, 1, 2, "Apple reports record services revenue", 0),
    )
    conn.commit()


def _insert_short_interest(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert a short_interest row."""
    conn.execute(
        """INSERT OR REPLACE INTO short_interest
           (ticker, settlement_date, short_interest, avg_daily_volume, days_to_cover)
           VALUES (?, ?, ?, ?, ?)""",
        (ticker, "2025-01-10", 50_000_000, 55_000_000, 0.91),
    )
    conn.commit()


def _insert_ticker(conn: sqlite3.Connection, ticker: str, sector_etf: str = "XLK") -> None:
    """Insert a tickers table row."""
    conn.execute(
        "INSERT OR IGNORE INTO tickers (symbol, sector, sector_etf, active) VALUES (?, ?, ?, ?)",
        (ticker, "Technology", sector_etf, 1),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# TestBuildTickerContext
# ---------------------------------------------------------------------------

class TestBuildTickerContext:
    def test_build_ticker_context(self, db_connection: sqlite3.Connection) -> None:
        """Returns a formatted string containing all required sections."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE, close=155.0)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)
        _insert_fundamentals(db_connection, "AAPL")
        _insert_news_summary(db_connection, "AAPL", SCORING_DATE)
        _insert_short_interest(db_connection, "AAPL")
        _insert_ticker(db_connection, "AAPL", "XLK")

        score = _make_score_dict()

        from src.notifier.ai_reasoner import build_ticker_context
        context = build_ticker_context(db_connection, "AAPL", score, SCORING_DATE)

        # Ticker symbol and current price
        assert "AAPL" in context
        assert "155" in context

        # Signal and confidence
        assert "NEUTRAL" in context
        assert "15" in context

        # Regime
        assert "ranging" in context

        # All 9 category scores with labels
        assert "Trend:" in context
        assert "Momentum:" in context
        assert "Volume:" in context
        assert "Volatility:" in context
        assert "Candlestick:" in context
        assert "Structural:" in context
        assert "Sentiment:" in context
        assert "Fundamental:" in context
        assert "Macro:" in context

        # Key signals list
        assert "RSI oversold" in context
        assert "Bearish EMA stack" in context

        # Indicator values
        assert "RSI:" in context
        assert "MACD:" in context
        assert "ADX:" in context
        assert "38.7" in context  # rsi_14 value we inserted

        # Section headers
        assert "Fibonacci" in context
        assert "Relative Strength" in context

        # Fundamentals snapshot
        assert "P/E:" in context
        assert "28.5" in context

        # News sentiment
        assert "News Sentiment" in context
        assert "Apple reports record services revenue" in context

        # Short interest
        assert "Days to Cover" in context

    def test_build_ticker_context_handles_missing_data(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Gracefully omits missing sections instead of crashing."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)
        # No fundamentals, no news, no short interest

        score = _make_score_dict()
        score["data_completeness"] = json.dumps({"news": False, "fundamentals": False})

        from src.notifier.ai_reasoner import build_ticker_context
        context = build_ticker_context(db_connection, "AAPL", score, SCORING_DATE)

        # Must not crash; basic identifiers still present
        assert "AAPL" in context
        assert "NEUTRAL" in context
        assert "ranging" in context
        # Missing sections should have graceful placeholders
        assert "Fundamentals:" in context
        assert "News Sentiment" in context

    def test_build_ticker_context_includes_recent_patterns(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Patterns from the last 10 days appear with name and direction."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)

        # 3 days before scoring date
        db_connection.execute(
            "INSERT INTO patterns_daily "
            "(ticker, date, pattern_name, direction, strength) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2025-01-12", "bullish_engulfing", "bullish", 2),
        )
        # 1 day before scoring date
        db_connection.execute(
            "INSERT INTO patterns_daily "
            "(ticker, date, pattern_name, direction, strength) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2025-01-14", "evening_star", "bearish", 3),
        )
        db_connection.commit()

        score = _make_score_dict()

        from src.notifier.ai_reasoner import build_ticker_context
        context = build_ticker_context(db_connection, "AAPL", score, SCORING_DATE)

        assert "bullish_engulfing" in context
        assert "evening_star" in context

    def test_build_ticker_context_includes_divergences(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Divergences from last 30 days appear in context."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)

        db_connection.execute(
            "INSERT INTO divergences_daily "
            "(ticker, date, indicator, divergence_type, strength) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2025-01-10", "rsi_14", "bullish_regular", 2),
        )
        db_connection.commit()

        score = _make_score_dict()

        from src.notifier.ai_reasoner import build_ticker_context
        context = build_ticker_context(db_connection, "AAPL", score, SCORING_DATE)

        assert "rsi_14" in context
        assert "bullish_regular" in context


# ---------------------------------------------------------------------------
# TestBuildMarketContext
# ---------------------------------------------------------------------------

class TestBuildMarketContext:
    def test_build_market_context(self, db_connection: sqlite3.Connection) -> None:
        """Returns string with VIX level/interpretation, SPY trend, treasury yield."""
        _insert_ohlcv(db_connection, "^VIX", SCORING_DATE, close=18.5)
        _insert_ohlcv(db_connection, "SPY", SCORING_DATE, close=475.0)
        _insert_indicators(db_connection, "SPY", SCORING_DATE)
        _insert_ohlcv(db_connection, "QQQ", SCORING_DATE, close=390.0)
        db_connection.execute(
            "INSERT OR REPLACE INTO treasury_yields (date, yield_10_year) VALUES (?, ?)",
            (SCORING_DATE, 4.25),
        )
        db_connection.commit()

        from src.notifier.ai_reasoner import build_market_context
        context = build_market_context(db_connection, SCORING_DATE)

        assert "VIX" in context
        assert "18.5" in context
        assert "normal" in context  # VIX 18.5 is in the "normal" (15-20) range
        assert "SPY" in context
        assert "QQQ" in context
        assert "10Y" in context
        assert "4.25" in context

    def test_build_market_context_handles_missing_data(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns a context string gracefully even when market data is absent."""
        from src.notifier.ai_reasoner import build_market_context
        context = build_market_context(db_connection, SCORING_DATE)

        assert "Market Context" in context
        assert "VIX" in context
        assert "SPY" in context

    def test_build_market_context_null_treasury_yield(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Treasury row with NULL yield_10_year does not crash; shows N/A instead."""
        # Insert a row where the 10Y yield column is explicitly NULL
        db_connection.execute(
            "INSERT OR REPLACE INTO treasury_yields (date, yield_10_year) VALUES (?, ?)",
            (SCORING_DATE, None),
        )
        db_connection.commit()

        from src.notifier.ai_reasoner import build_market_context
        context = build_market_context(db_connection, SCORING_DATE)  # must not raise

        assert "Market Context" in context
        assert "N/A" in context  # yield shown as N/A


# ---------------------------------------------------------------------------
# TestBuildPromptForTicker
# ---------------------------------------------------------------------------

class TestBuildPromptForTicker:
    def test_build_prompt_single_ticker(self) -> None:
        """Prompt includes system instruction, contexts, and format requirement."""
        from src.notifier.ai_reasoner import build_prompt_for_ticker

        ticker_context = "Ticker: AAPL | Price: $155.00 | Signal: NEUTRAL"
        market_context = "Market Context — 2025-01-15\n  VIX: 18.5 (normal)"

        prompt = build_prompt_for_ticker(ticker_context, market_context)

        # Role / system instruction
        assert "analyst" in prompt.lower()
        # Ticker context is included
        assert ticker_context in prompt
        # Market context is included
        assert market_context in prompt
        # Asks for reasoning, not summarizing
        assert "interpret" in prompt.lower() or "reason" in prompt.lower() or "interpret" in prompt.lower()
        # Output format guidance
        assert "2-4 sentences" in prompt or "2–4" in prompt

    def test_build_prompt_single_ticker_no_flip_flag(self) -> None:
        """Without is_flip, prompt does NOT contain the flip instruction."""
        from src.notifier.ai_reasoner import build_prompt_for_ticker

        prompt = build_prompt_for_ticker("AAPL context", "market context", is_flip=False)
        assert "changed direction" not in prompt.lower()
        assert "signal change" not in prompt.lower()

    def test_build_prompt_single_ticker_with_flip_flag(self) -> None:
        """With is_flip=True, prompt contains the flip instruction."""
        from src.notifier.ai_reasoner import build_prompt_for_ticker

        prompt = build_prompt_for_ticker("AAPL context", "market context", is_flip=True)
        assert "changed direction" in prompt.lower() or "just changed" in prompt.lower()

    def test_build_prompt_daily_summary(self) -> None:
        """Daily summary prompt asks Claude for a cohesive market summary."""
        from src.notifier.ai_reasoner import build_prompt_for_daily_summary

        bullish = [
            {"ticker": "AAPL", "score": _make_score_dict("AAPL", "BULLISH", 80.0, 45.0)},
            {"ticker": "MSFT", "score": _make_score_dict("MSFT", "BULLISH", 75.0, 40.0)},
        ]
        bearish = [
            {"ticker": "BA", "score": _make_score_dict("BA", "BEARISH", 72.0, -38.0)},
        ]
        flips = [
            {
                "ticker": "GOOG",
                "flip": {"previous_signal": "BULLISH", "new_signal": "BEARISH",
                         "previous_confidence": 75.0, "new_confidence": 62.0},
                "score": _make_score_dict("GOOG", "BEARISH", 62.0, -32.0),
            }
        ]
        market_context = "Market Context — 2025-01-15"

        prompt = build_prompt_for_daily_summary(bullish, bearish, flips, market_context)

        assert "AAPL" in prompt
        assert "MSFT" in prompt
        assert "BA" in prompt
        assert "GOOG" in prompt
        assert "BULLISH" in prompt
        assert "BEARISH" in prompt
        assert "market" in prompt.lower()
        assert "summary" in prompt.lower() or "summarize" in prompt.lower()
        assert market_context in prompt


# ---------------------------------------------------------------------------
# TestCallClaude
# ---------------------------------------------------------------------------

class TestCallClaude:
    def test_call_claude_api(self) -> None:
        """Uses correct model, max_tokens, temperature from config; returns response text."""
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="AAPL analysis here.")]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_message

        with patch("anthropic.Anthropic", return_value=mock_client):
            from src.notifier.ai_reasoner import call_claude
            result = call_claude("test prompt", SAMPLE_CONFIG)

        assert result == "AAPL analysis here."
        call_kwargs = mock_client.messages.create.call_args
        assert call_kwargs.kwargs["model"] == "claude-sonnet-4-20250514"
        assert call_kwargs.kwargs["max_tokens"] == 4096
        assert call_kwargs.kwargs["temperature"] == 0.3
        assert call_kwargs.kwargs["messages"] == [{"role": "user", "content": "test prompt"}]

    def test_call_claude_api_error_handling(self) -> None:
        """API exception returns fallback message, does not crash."""
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API connection failed")

        with patch("anthropic.Anthropic", return_value=mock_client):
            from src.notifier.ai_reasoner import call_claude
            result = call_claude("test prompt", SAMPLE_CONFIG)

        assert result == _FALLBACK
        assert "unavailable" in result.lower()

    def test_call_claude_api_timeout(self) -> None:
        """Timeout exception returns fallback message, does not crash."""
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = TimeoutError("Request timed out")

        with patch("anthropic.Anthropic", return_value=mock_client):
            from src.notifier.ai_reasoner import call_claude
            result = call_claude("test prompt", SAMPLE_CONFIG)

        assert result == _FALLBACK

    def test_call_claude_rate_limit(self) -> None:
        """Rate limit error results in graceful fallback after retries exhausted."""
        mock_client = MagicMock()
        # Simulate a persistent error that exhausts retries
        mock_client.messages.create.side_effect = Exception("429 Too Many Requests — rate limited")

        with patch("anthropic.Anthropic", return_value=mock_client):
            from src.notifier.ai_reasoner import call_claude
            result = call_claude("test prompt", SAMPLE_CONFIG)

        assert result == _FALLBACK


# ---------------------------------------------------------------------------
# TestGenerateTickerReasoning
# ---------------------------------------------------------------------------

class TestGenerateTickerReasoning:
    def test_generate_ticker_reasoning(self, db_connection: sqlite3.Connection) -> None:
        """Returns Claude's analysis string (not empty, not raw prompt)."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)

        score = _make_score_dict()
        market_context = "Market Context — 2025-01-15\n  VIX: 18.5 (normal)"

        with patch("src.notifier.ai_reasoner.call_claude", return_value=_SAMPLE_CLAUDE_RESPONSE):
            from src.notifier.ai_reasoner import generate_ticker_reasoning
            result = generate_ticker_reasoning(
                db_connection, "AAPL", score, market_context, SAMPLE_CONFIG
            )

        assert result == _SAMPLE_CLAUDE_RESPONSE
        assert len(result) > 0
        # Must not return the raw prompt
        assert "You are an expert" not in result

    def test_generate_ticker_reasoning_respects_length(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Claude's response is returned as-is; 2-5 sentence mock passes the check."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)

        score = _make_score_dict()
        market_context = "Market Context"

        with patch("src.notifier.ai_reasoner.call_claude", return_value=_SAMPLE_CLAUDE_RESPONSE):
            from src.notifier.ai_reasoner import generate_ticker_reasoning
            result = generate_ticker_reasoning(
                db_connection, "AAPL", score, market_context, SAMPLE_CONFIG
            )

        # Count sentences in the response (split on sentence-ending punctuation)
        sentences = [s.strip() for s in re.split(r"[.!?]+", result) if s.strip()]
        assert 2 <= len(sentences) <= 5


# ---------------------------------------------------------------------------
# TestGenerateDailySummary
# ---------------------------------------------------------------------------

class TestGenerateDailySummary:
    def test_generate_daily_summary(self, db_connection: sqlite3.Connection) -> None:
        """Calls Claude with a summary prompt covering bullish, bearish, flips."""
        bullish = [
            {"ticker": "AAPL", "score": _make_score_dict("AAPL", "BULLISH", 80.0, 45.0)},
            {"ticker": "MSFT", "score": _make_score_dict("MSFT", "BULLISH", 75.0, 40.0)},
            {"ticker": "NVDA", "score": _make_score_dict("NVDA", "BULLISH", 72.0, 38.0)},
        ]
        bearish = [
            {"ticker": "BA", "score": _make_score_dict("BA", "BEARISH", 71.0, -36.0)},
            {"ticker": "F", "score": _make_score_dict("F", "BEARISH", 70.0, -33.0)},
        ]
        flips = [
            {
                "ticker": "GOOG",
                "flip": {
                    "previous_signal": "BULLISH", "new_signal": "BEARISH",
                    "previous_confidence": 75.0, "new_confidence": 62.0,
                },
                "score": _make_score_dict("GOOG", "BEARISH", 62.0, -32.0),
            }
        ]
        market_context = "Market Context — 2025-01-15\n  VIX: 18.5 (normal)"
        expected_summary = "Technology led the session with AAPL and MSFT both confirming bullish signals."

        with patch("src.notifier.ai_reasoner.call_claude", return_value=expected_summary) as mock_claude:
            from src.notifier.ai_reasoner import generate_daily_summary
            result = generate_daily_summary(
                db_connection, bullish, bearish, flips, market_context, SAMPLE_CONFIG
            )

        assert result == expected_summary
        # Claude was called once for the daily summary
        assert mock_claude.call_count == 1
        # The prompt passed to Claude includes the ticker symbols
        prompt_arg = mock_claude.call_args[0][0]
        assert "AAPL" in prompt_arg
        assert "GOOG" in prompt_arg

    def test_generate_daily_summary_empty_results(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """No qualifying tickers returns the 'no signals' message without calling Claude."""
        market_context = "Market Context"

        with patch("src.notifier.ai_reasoner.call_claude") as mock_claude:
            from src.notifier.ai_reasoner import generate_daily_summary
            result = generate_daily_summary(
                db_connection, [], [], [], market_context, SAMPLE_CONFIG
            )

        assert "No significant signals today" in result
        mock_claude.assert_not_called()


# ---------------------------------------------------------------------------
# TestReasonAllQualifyingTickers
# ---------------------------------------------------------------------------

class TestReasonAllQualifyingTickers:
    def _setup_ticker(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        signal: str = "BULLISH",
        confidence: float = 80.0,
        final_score: float = 45.0,
    ) -> None:
        """Insert score, OHLCV, and indicators for a ticker."""
        _insert_score(conn, _make_score_dict(ticker, signal, confidence, final_score))
        _insert_ohlcv(conn, ticker, SCORING_DATE)
        _insert_indicators(conn, ticker, SCORING_DATE)

    def test_reason_all_qualifying_tickers(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Claude called once per qualifying ticker plus once for the daily summary."""
        # 2 BULLISH above threshold (80, 75), 3 below threshold / wrong direction
        self._setup_ticker(db_connection, "AAPL", "BULLISH", 80.0, 45.0)
        self._setup_ticker(db_connection, "MSFT", "BULLISH", 75.0, 40.0)
        self._setup_ticker(db_connection, "NVDA", "BEARISH", 30.0, -15.0)  # below threshold
        self._setup_ticker(db_connection, "AMZN", "NEUTRAL", 25.0, 5.0)    # neutral, below
        self._setup_ticker(db_connection, "GOOG", "BEARISH", 20.0, -10.0)  # below threshold

        # GOOG has a flip (always included regardless of confidence)
        db_connection.execute(
            "INSERT INTO signal_flips "
            "(ticker, date, previous_signal, new_signal, previous_confidence, new_confidence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("GOOG", SCORING_DATE, "BULLISH", "BEARISH", 75.0, 20.0),
        )
        db_connection.commit()

        _insert_ohlcv(db_connection, "^VIX", SCORING_DATE, close=18.0)

        with patch("src.notifier.ai_reasoner.call_claude", return_value="AI analysis.") as mock_claude:
            from src.notifier.ai_reasoner import reason_all_qualifying_tickers
            result = reason_all_qualifying_tickers(db_connection, SCORING_DATE, SAMPLE_CONFIG)

        # 2 bullish + 1 flip-only = 3 individual calls + 1 daily summary = 4 total
        assert mock_claude.call_count == 4

        assert len(result["bullish"]) == 2
        assert len(result["flips"]) == 1
        assert result["flips"][0]["ticker"] == "GOOG"
        assert "daily_summary" in result
        assert "market_context_summary" in result

        # Each bullish result has ticker, score, reasoning
        aapl_result = next(r for r in result["bullish"] if r["ticker"] == "AAPL")
        assert aapl_result["reasoning"] == "AI analysis."

    def test_reason_all_limits_api_calls(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """With 20 qualifying tickers, only the top 10 (by confidence) get individual reasoning."""
        # Create 20 BULLISH tickers with descending confidence 89, 88, ..., 70
        for idx in range(20):
            ticker = f"T{idx:02d}"
            confidence = 89.0 - idx  # T00=89, T01=88, ..., T09=80, T10=79, ..., T19=70
            self._setup_ticker(db_connection, ticker, "BULLISH", confidence, 40.0)

        _insert_ohlcv(db_connection, "^VIX", SCORING_DATE, close=18.0)

        config = {
            **SAMPLE_CONFIG,
            "telegram": {**SAMPLE_CONFIG["telegram"], "max_tickers_per_section": 10},
        }

        with patch("src.notifier.ai_reasoner.call_claude", return_value="AI analysis.") as mock_claude:
            from src.notifier.ai_reasoner import reason_all_qualifying_tickers
            result = reason_all_qualifying_tickers(db_connection, SCORING_DATE, config)

        # 10 individual calls (capped) + 1 daily summary = 11 total
        assert mock_claude.call_count == 11
        assert len(result["bullish"]) == 10

    def test_reasoning_includes_signal_flip_context(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Prompt includes flip information when ticker changed signal direction."""
        _insert_ohlcv(db_connection, "AAPL", SCORING_DATE)
        _insert_indicators(db_connection, "AAPL", SCORING_DATE)

        db_connection.execute(
            "INSERT INTO signal_flips "
            "(ticker, date, previous_signal, new_signal, previous_confidence, new_confidence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("AAPL", SCORING_DATE, "BULLISH", "BEARISH", 75.0, 60.0),
        )
        db_connection.commit()

        score = _make_score_dict("AAPL", "BEARISH", 60.0, -35.0)
        market_context = "Market Context — test"

        with patch("src.notifier.ai_reasoner.call_claude", return_value="AAPL flipped bearish.") as mock_claude:
            from src.notifier.ai_reasoner import generate_ticker_reasoning
            generate_ticker_reasoning(
                db_connection, "AAPL", score, market_context, SAMPLE_CONFIG, is_flip=True
            )

        # The prompt passed to call_claude must include flip context
        prompt_arg = mock_claude.call_args[0][0]
        assert (
            "changed direction" in prompt_arg.lower()
            or "signal change" in prompt_arg.upper()
            or "just changed" in prompt_arg.lower()
        )
