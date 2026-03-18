"""
Tests for src/notifier/formatter.py — Telegram message formatter.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.notifier.formatter import (
    DIVIDER,
    MAX_TELEGRAM_LENGTH,
    format_bearish_section,
    format_bullish_section,
    format_daily_summary_section,
    format_duration,
    format_flips_section,
    format_full_report,
    format_heartbeat,
    format_header,
    format_market_closed_message,
    format_market_context_section,
    format_no_signals_report,
    format_pipeline_error_message,
    format_signal_distribution,
)

SCORING_DATE = "2026-03-16"

SAMPLE_CONFIG = {
    "telegram": {
        "confidence_threshold": 70,
        "always_include_flips": True,
        "max_tickers_per_section": 10,
        "include_heartbeat": True,
        "display_timezone": "Europe/Amsterdam",
    }
}


def _make_bullish(ticker: str, confidence: float, final_score: float, reasoning: str = "Bullish reasoning.") -> dict:
    return {
        "ticker": ticker,
        "score": {"signal": "BULLISH", "confidence": confidence, "final_score": final_score},
        "reasoning": reasoning,
    }


def _make_bearish(ticker: str, confidence: float, final_score: float, reasoning: str = "Bearish reasoning.") -> dict:
    return {
        "ticker": ticker,
        "score": {"signal": "BEARISH", "confidence": confidence, "final_score": final_score},
        "reasoning": reasoning,
    }


def _make_flip(
    ticker: str,
    prev: str,
    new: str,
    prev_conf: float,
    new_conf: float,
    reasoning: str = "Flip reasoning.",
) -> dict:
    return {
        "ticker": ticker,
        "flip": {
            "previous_signal": prev,
            "new_signal": new,
            "previous_confidence": prev_conf,
            "new_confidence": new_conf,
        },
        "score": {"signal": new, "confidence": new_conf, "final_score": 35.0},
        "reasoning": reasoning,
    }


def _make_pipeline_stats(
    bullish_count: int = 3,
    bearish_count: int = 2,
    neutral_count: int = 54,
    tickers_processed: int = 59,
    tickers_total: int = 59,
    tickers_failed: int = 0,
    failed_tickers: list | None = None,
    fetcher_duration: float = 135.0,
    calculator_duration: float = 724.0,
    scorer_duration: float = 134.0,
    notifier_duration: float = 45.0,
    scoring_date: str = SCORING_DATE,
    display_timezone: str = "Europe/Amsterdam",
) -> dict:
    return {
        "scoring_date": scoring_date,
        "fetcher_duration": fetcher_duration,
        "calculator_duration": calculator_duration,
        "scorer_duration": scorer_duration,
        "notifier_duration": notifier_duration,
        "tickers_processed": tickers_processed,
        "tickers_total": tickers_total,
        "tickers_failed": tickers_failed,
        "failed_tickers": failed_tickers or [],
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "neutral_count": neutral_count,
        "display_timezone": display_timezone,
    }


def _make_results(
    bullish: list | None = None,
    bearish: list | None = None,
    flips: list | None = None,
    daily_summary: str = "Markets showed mixed signals today.",
    market_context_summary: str = "VIX: 18.5 | SPY: trending up\nSector leaders: Energy",
) -> dict:
    return {
        "bullish": bullish or [],
        "bearish": bearish or [],
        "flips": flips or [],
        "daily_summary": daily_summary,
        "market_context_summary": market_context_summary,
    }


# ---------------------------------------------------------------------------
# format_duration
# ---------------------------------------------------------------------------


def test_format_duration_seconds_only():
    assert format_duration(45.0) == "45s"


def test_format_duration_minutes_and_seconds():
    assert format_duration(135.0) == "2m 15s"


def test_format_duration_hours():
    assert format_duration(3725.0) == "1h 2m 5s"


def test_format_duration_exactly_60():
    assert format_duration(60.0) == "1m 0s"


# ---------------------------------------------------------------------------
# format_signal_distribution
# ---------------------------------------------------------------------------


def test_format_signal_distribution():
    result = format_signal_distribution(11, 5, 43)
    assert "🟢 11" in result
    assert "🔴 5" in result
    assert "🟡 43" in result


# ---------------------------------------------------------------------------
# format_bullish_section
# ---------------------------------------------------------------------------


def test_format_bullish_section():
    tickers = [
        _make_bullish("WMT", 67.0, 41.8, "WMT reasoning."),
        _make_bullish("CVX", 82.0, 52.3, "CVX reasoning."),
        _make_bullish("AAPL", 72.0, 45.2, "AAPL reasoning."),
    ]
    result = format_bullish_section(tickers)
    assert "🟢 BULLISH" in result
    assert "WMT" in result
    assert "67%" in result
    assert "WMT reasoning." in result
    assert "CVX" in result
    assert "82%" in result
    assert "AAPL" in result
    # CVX (82%) should appear before AAPL (72%) which should appear before WMT (67%)
    assert result.index("CVX") < result.index("AAPL") < result.index("WMT")


def test_format_bullish_section_empty():
    assert format_bullish_section([]) == ""


# ---------------------------------------------------------------------------
# format_bearish_section
# ---------------------------------------------------------------------------


def test_format_bearish_section():
    tickers = [
        _make_bearish("PYPL", 46.0, -36.1, "PYPL reasoning."),
        _make_bearish("META", 75.0, -52.0, "META reasoning."),
    ]
    result = format_bearish_section(tickers)
    assert "🔴 BEARISH" in result
    assert "PYPL" in result
    assert "META" in result
    # META (75%) before PYPL (46%)
    assert result.index("META") < result.index("PYPL")


def test_format_bearish_section_empty():
    assert format_bearish_section([]) == ""


# ---------------------------------------------------------------------------
# format_flips_section
# ---------------------------------------------------------------------------


def test_format_flips_section():
    flips = [
        _make_flip("AAPL", "NEUTRAL", "BULLISH", 15.0, 72.0, "AAPL flip reasoning."),
        _make_flip("TSLA", "BULLISH", "BEARISH", 71.0, 68.0, "TSLA flip reasoning."),
    ]
    result = format_flips_section(flips)
    assert "🔄 SIGNAL FLIPS" in result
    assert "AAPL" in result
    assert "→" in result
    assert "NEUTRAL" in result
    assert "BULLISH" in result
    assert "72%" in result
    assert "AAPL flip reasoning." in result
    assert "TSLA" in result


def test_format_flips_section_empty():
    assert format_flips_section([]) == ""


# ---------------------------------------------------------------------------
# format_market_context_section
# ---------------------------------------------------------------------------


def test_format_market_context_section():
    context = "VIX: 23.5 (elevated) | SPY: ranging\nSector leaders: Energy, Staples"
    result = format_market_context_section(context)
    assert "📉 Market Context" in result
    assert "VIX: 23.5" in result
    assert "Energy, Staples" in result


# ---------------------------------------------------------------------------
# format_daily_summary_section
# ---------------------------------------------------------------------------


def test_format_daily_summary_section():
    summary = "Markets showed mixed signals today with energy leading."
    result = format_daily_summary_section(summary)
    assert "📋 Daily Summary" in result
    assert summary in result


def test_format_daily_summary_section_no_signals():
    result = format_daily_summary_section("No significant signals today.")
    assert result == ""


def test_format_daily_summary_section_empty():
    assert format_daily_summary_section("") == ""


# ---------------------------------------------------------------------------
# format_heartbeat
# ---------------------------------------------------------------------------


def test_format_heartbeat():
    stats = _make_pipeline_stats(tickers_failed=0)
    result = format_heartbeat(stats)
    assert "Pipeline completed" in result
    assert "Fetcher:" in result
    assert "Calculator:" in result
    assert "Scorer:" in result
    assert "Tickers:" in result
    assert "✅" in result


def test_format_heartbeat_with_failures():
    stats = _make_pipeline_stats(
        tickers_failed=3,
        failed_tickers=["AAPL", "MSFT", "JPM"],
    )
    result = format_heartbeat(stats)
    assert "⚠️" in result
    assert "AAPL" in result
    assert "MSFT" in result
    assert "JPM" in result


# ---------------------------------------------------------------------------
# format_ticker_line_concise
# ---------------------------------------------------------------------------


def test_format_ticker_line_concise():
    """Each ticker entry uses format: '{ticker} — {conf}% 📊 {score:+.1f}'"""
    tickers = [_make_bullish("AAPL", 72.0, 45.2, "Some reasoning.")]
    result = format_bullish_section(tickers)
    assert "AAPL — 72% 📊 +45.2" in result


# ---------------------------------------------------------------------------
# format_full_report
# ---------------------------------------------------------------------------


def test_format_full_report():
    results = _make_results(
        bullish=[_make_bullish("WMT", 75.0, 41.8)],
        bearish=[_make_bearish("PYPL", 71.0, -36.1)],
        flips=[_make_flip("TSLA", "NEUTRAL", "BULLISH", 10.0, 72.0)],
    )
    stats = _make_pipeline_stats()
    messages = format_full_report(results, stats, SAMPLE_CONFIG)
    full = "\n".join(messages)
    assert "📊 Signal Report" in full
    assert "🟢 BULLISH" in full
    assert "🔴 BEARISH" in full
    assert "🔄 SIGNAL FLIPS" in full
    assert "📉 Market Context" in full
    assert "Pipeline completed" in full


def test_format_full_report_respects_timezone():
    """Header time should be in CET/CEST, not UTC."""
    results = _make_results()
    stats = _make_pipeline_stats(display_timezone="Europe/Amsterdam")
    # Mock datetime.now to return a known UTC time (midnight UTC = 1am CET)
    fixed_utc = datetime(2026, 3, 16, 0, 0, 0, tzinfo=timezone.utc)
    with patch("src.notifier.formatter.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_utc
        mock_dt.strptime = datetime.strptime
        messages = format_full_report(results, stats, SAMPLE_CONFIG)
    full = "\n".join(messages)
    # 00:00 UTC = 01:00 CET in March (UTC+1)
    assert "01:00" in full
    assert "CET" in full


def test_format_full_report_no_bullish():
    results = _make_results(
        bullish=[],
        bearish=[_make_bearish("PYPL", 71.0, -36.1)],
    )
    stats = _make_pipeline_stats(bullish_count=0)
    messages = format_full_report(results, stats, SAMPLE_CONFIG)
    full = "\n".join(messages)
    assert "🟢 BULLISH" not in full
    assert "🔴 BEARISH" in full


def test_format_full_report_only_flips():
    results = _make_results(
        bullish=[],
        bearish=[],
        flips=[
            _make_flip("AAPL", "NEUTRAL", "BULLISH", 10.0, 72.0),
            _make_flip("TSLA", "BULLISH", "BEARISH", 71.0, 68.0),
        ],
    )
    stats = _make_pipeline_stats(bullish_count=0, bearish_count=0, neutral_count=59)
    messages = format_full_report(results, stats, SAMPLE_CONFIG)
    full = "\n".join(messages)
    assert "🔄 SIGNAL FLIPS" in full
    assert "🟢 BULLISH" not in full
    assert "🔴 BEARISH" not in full


def test_format_full_report_under_4096_chars():
    results = _make_results(
        bullish=[_make_bullish(f"TK{i}", 70.0 + i, 30.0 + i) for i in range(5)],
        bearish=[_make_bearish(f"BK{i}", 70.0 + i, -30.0 - i) for i in range(3)],
    )
    stats = _make_pipeline_stats()
    messages = format_full_report(results, stats, SAMPLE_CONFIG)
    assert isinstance(messages, list)
    assert len(messages) >= 1
    for msg in messages:
        assert len(msg) <= MAX_TELEGRAM_LENGTH


def test_format_full_report_splits_if_too_long():
    long_reasoning = "X" * 250
    results = _make_results(
        bullish=[_make_bullish(f"TK{i}", 70.0 + i, 30.0 + i, long_reasoning) for i in range(10)],
        bearish=[_make_bearish(f"BK{i}", 70.0 + i, -30.0 - i, long_reasoning) for i in range(10)],
        flips=[_make_flip(f"FL{i}", "NEUTRAL", "BULLISH", 10.0, 72.0 + i, long_reasoning) for i in range(5)],
    )
    stats = _make_pipeline_stats()
    messages = format_full_report(results, stats, SAMPLE_CONFIG)
    assert len(messages) > 1
    for msg in messages:
        assert len(msg) <= MAX_TELEGRAM_LENGTH


def test_format_no_signals_report():
    stats = _make_pipeline_stats(bullish_count=0, bearish_count=0, neutral_count=59)
    messages = format_no_signals_report(
        "VIX: 15.0 | SPY: flat", stats, SAMPLE_CONFIG
    )
    full = "\n".join(messages)
    assert "No significant signals" in full
    assert isinstance(messages, list)
    assert all(len(m) <= MAX_TELEGRAM_LENGTH for m in messages)
