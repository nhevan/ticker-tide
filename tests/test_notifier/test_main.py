"""
Tests for src/notifier/main.py — Notifier orchestrator (Phase 4).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

SCORING_DATE = "2025-01-15"

SAMPLE_CONFIG = {
    "ai_reasoner": {"model": "claude-sonnet-4-20250514", "max_tokens": 4096, "temperature": 0.3},
    "telegram": {
        "admin_chat_id": "admin-chat",
        "subscriber_chat_ids": ["subscriber-chat"],
        "confidence_threshold": 40,
        "always_include_flips": True,
        "max_tickers_per_section": 10,
        "include_heartbeat": True,
        "display_timezone": "Europe/Amsterdam",
    },
}

SAMPLE_TELEGRAM_CONFIG = {
    "bot_token": "test-token",
    "admin_chat_id": "admin-chat",
    "subscriber_chat_ids": ["subscriber-chat"],
    "subscriber_ticker_filters": {},
}

SAMPLE_RESULTS = {
    "bullish": [
        {"ticker": "WMT", "score": {"signal": "BULLISH", "confidence": 75.0, "final_score": 41.8}, "reasoning": "WMT is strong."},
    ],
    "bearish": [],
    "flips": [],
    "daily_summary": "Markets were mixed today.",
    "market_context_summary": "VIX: 18.0 | SPY: ranging",
}

EMPTY_RESULTS = {
    "bullish": [],
    "bearish": [],
    "flips": [],
    "daily_summary": "No significant signals today.",
    "market_context_summary": "VIX: 18.0 | SPY: flat",
}


def _insert_scorer_done(conn: sqlite3.Connection, scoring_date: str = SCORING_DATE) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_events (event, date, status, timestamp) VALUES (?, ?, ?, ?)",
        ("scorer_done", scoring_date, "completed", "2025-01-15T23:00:00+00:00"),
    )
    conn.commit()


def _insert_scores_daily(conn: sqlite3.Connection, scoring_date: str = SCORING_DATE) -> None:
    rows = [
        ("WMT", scoring_date, "BULLISH", 75.0, 41.8),
        ("PYPL", scoring_date, "BEARISH", 71.0, -36.1),
        ("AAPL", scoring_date, "NEUTRAL", 15.0, 5.0),
    ]
    for ticker, dt, signal, conf, score in rows:
        conn.execute(
            """INSERT OR REPLACE INTO scores_daily
               (ticker, date, signal, confidence, final_score)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, dt, signal, conf, score),
        )
    conn.commit()


def _insert_indicators(conn: sqlite3.Connection, scoring_date: str = SCORING_DATE) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily
           (ticker, date, ema_9, ema_50, rsi_14)
           VALUES (?, ?, ?, ?, ?)""",
        ("WMT", scoring_date, 100.0, 95.0, 55.0),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_notifier_happy_path(db_connection, tmp_path, mocker):
    """Full happy path: scorer_done exists, qualifying tickers present."""
    _insert_scorer_done(db_connection)
    _insert_scores_daily(db_connection)
    _insert_indicators(db_connection)
    db_path = str(tmp_path / "test_signals.db")

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mock_reasoner = mocker.patch(
        "src.notifier.main.reason_all_qualifying_tickers",
        return_value=SAMPLE_RESULTS,
    )
    mocker.patch("src.notifier.main.format_full_report", return_value=["Report text"])
    mocker.patch("src.notifier.main.format_heartbeat", return_value="Heartbeat text")
    mock_send = mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)
    # Capture pipeline event writes so we can verify without the closed connection
    mock_write_event = mocker.patch("src.notifier.main.write_pipeline_event")

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=db_path)

    assert mock_reasoner.called
    assert mock_send.called
    assert result.get("telegram_sent") is True
    assert result.get("subscribers_notified") == 1

    # Verify notifier_done completed event was written
    completed_calls = [
        c for c in mock_write_event.call_args_list
        if c[0][1] == "notifier_done" and c[0][3] == "completed"
    ]
    assert completed_calls, "Expected write_pipeline_event('notifier_done', ..., 'completed') to be called"


# ---------------------------------------------------------------------------
# No qualifying tickers
# ---------------------------------------------------------------------------


def test_run_notifier_no_qualifying_tickers(db_connection, tmp_path, mocker):
    """No qualifying tickers: format_no_signals_report is used instead."""
    _insert_scorer_done(db_connection)
    _insert_scores_daily(db_connection)
    _insert_indicators(db_connection)

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.main.reason_all_qualifying_tickers",
        return_value=EMPTY_RESULTS,
    )
    mock_no_signals = mocker.patch(
        "src.notifier.main.format_no_signals_report",
        return_value=["No signals today."],
    )
    mock_full = mocker.patch("src.notifier.main.format_full_report")
    mocker.patch("src.notifier.main.format_heartbeat", return_value="Heartbeat")
    mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))

    assert mock_no_signals.called
    assert not mock_full.called


# ---------------------------------------------------------------------------
# AI failure
# ---------------------------------------------------------------------------


def test_run_notifier_ai_failure(db_connection, tmp_path, mocker):
    """Claude API failure: report still sent with fallback, no crash."""
    _insert_scorer_done(db_connection)
    _insert_scores_daily(db_connection)
    _insert_indicators(db_connection)

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.main.reason_all_qualifying_tickers",
        side_effect=Exception("Claude API error"),
    )
    mocker.patch("src.notifier.main.format_no_signals_report", return_value=["Fallback."])
    mocker.patch("src.notifier.main.format_full_report", return_value=["Fallback."])
    mocker.patch("src.notifier.main.format_heartbeat", return_value="Heartbeat")
    mock_send = mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))

    # Should not crash; report should still be sent
    assert mock_send.called
    assert "duration_seconds" in result


# ---------------------------------------------------------------------------
# Telegram failure
# ---------------------------------------------------------------------------


def test_run_notifier_telegram_failure(db_connection, tmp_path, mocker):
    """Telegram send failure: no crash, pipeline event still written."""
    _insert_scorer_done(db_connection)
    _insert_scores_daily(db_connection)
    _insert_indicators(db_connection)

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.main.reason_all_qualifying_tickers",
        return_value=SAMPLE_RESULTS,
    )
    mocker.patch("src.notifier.main.format_full_report", return_value=["Report"])
    mocker.patch("src.notifier.main.format_heartbeat", return_value="Heartbeat")
    mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 0, "failed": 1, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=False)
    mock_write_event = mocker.patch("src.notifier.main.write_pipeline_event")

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))

    assert result.get("telegram_sent") is False
    completed_calls = [
        c for c in mock_write_event.call_args_list
        if c[0][1] == "notifier_done" and c[0][3] == "completed"
    ]
    assert completed_calls, "Expected notifier_done completed event even on Telegram failure"


# ---------------------------------------------------------------------------
# Scorer not done
# ---------------------------------------------------------------------------


def test_run_notifier_waits_for_scorer(db_connection, tmp_path, mocker):
    """No scorer_done event: returns skipped, logs warning."""
    _insert_indicators(db_connection)  # no scorer_done event

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))

    assert result.get("skipped") is True


# ---------------------------------------------------------------------------
# Already done (idempotency)
# ---------------------------------------------------------------------------


def test_run_notifier_skips_if_already_done(db_connection, tmp_path, mocker):
    """notifier_done already completed: skips run."""
    _insert_scorer_done(db_connection)
    _insert_indicators(db_connection)
    db_connection.execute(
        "INSERT OR REPLACE INTO pipeline_events (event, date, status, timestamp) VALUES (?, ?, ?, ?)",
        ("notifier_done", SCORING_DATE, "completed", "2025-01-15T23:30:00+00:00"),
    )
    db_connection.commit()

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))

    assert result.get("skipped") is True


# ---------------------------------------------------------------------------
# Pipeline run logging
# ---------------------------------------------------------------------------


def test_run_notifier_logs_pipeline_run(db_connection, tmp_path, mocker):
    """Verifies log_pipeline_run is called with phase='notifier'."""
    _insert_scorer_done(db_connection)
    _insert_scores_daily(db_connection)
    _insert_indicators(db_connection)

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.main.reason_all_qualifying_tickers",
        return_value=SAMPLE_RESULTS,
    )
    mocker.patch("src.notifier.main.format_full_report", return_value=["Report"])
    mocker.patch("src.notifier.main.format_heartbeat", return_value="Heartbeat")
    mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)
    mock_log = mocker.patch("src.notifier.main.log_pipeline_run")

    from src.notifier.main import run_notifier

    run_notifier(db_path=str(tmp_path / "test.db"))

    assert mock_log.called
    call_kwargs = mock_log.call_args
    assert call_kwargs[1].get("phase") == "notifier" or (
        len(call_kwargs[0]) > 2 and call_kwargs[0][2] == "notifier"
    )
