"""
Tests for src/notifier/main.py — Notifier orchestrator (Phase 4).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.notifier.ai_reasoner import _FALLBACK_RESPONSE

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


# ---------------------------------------------------------------------------
# include_ai_reasoning=False skips the Claude API call
# ---------------------------------------------------------------------------


def test_run_notifier_skips_ai_reasoner_when_flag_false(db_connection, tmp_path, mocker):
    """When include_ai_reasoning=False, body still lists qualifying tickers (DB-only path)."""
    _insert_scorer_done(db_connection)
    _insert_indicators(db_connection)

    # Inline-seed scores_daily — three rows: WMT@BULLISH 75, AMZN@BULLISH 80, PYPL@BEARISH 71
    score_rows = [
        ("WMT", SCORING_DATE, "BULLISH", 75.0, 41.8),
        ("AMZN", SCORING_DATE, "BULLISH", 80.0, 45.0),
        ("PYPL", SCORING_DATE, "BEARISH", 71.0, -36.1),
    ]
    for ticker, dt, signal, conf, final_score in score_rows:
        db_connection.execute(
            """INSERT OR REPLACE INTO scores_daily
               (ticker, date, signal, confidence, final_score)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, dt, signal, conf, final_score),
        )
    # Inline-seed tickers rows
    for ticker in ("WMT", "AMZN", "PYPL"):
        db_connection.execute(
            "INSERT OR IGNORE INTO tickers (symbol, sector, sector_etf, active) VALUES (?, ?, ?, ?)",
            (ticker, "Consumer", "XLY", 1),
        )
    db_connection.commit()

    config_no_ai = {
        **SAMPLE_CONFIG,
        "telegram": {**SAMPLE_CONFIG["telegram"], "include_ai_reasoning": False, "confidence_threshold": 70},
    }

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=config_no_ai)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    # Test isolation — avoids dep on treasury_yields/ETF tables
    mocker.patch(
        "src.notifier.ai_reasoner.build_market_context",
        return_value="Market context: stable.",
    )
    mock_call_claude = mocker.patch(
        "src.notifier.ai_reasoner.call_claude",
        side_effect=AssertionError("Claude must not be called when include_ai_reasoning=False"),
    )
    # _build_pipeline_stats reads config/tickers.json from disk — accepted real-file dependency
    mock_send = mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)
    mocker.patch("src.notifier.main.log_pipeline_run")
    mock_write_event = mocker.patch("src.notifier.main.write_pipeline_event")

    from src.notifier.main import run_notifier

    run_notifier(db_path=str(tmp_path / "test.db"))

    assert mock_send.called
    captured_messages = mock_send.call_args[0][0] if mock_send.call_args[0] else mock_send.call_args[1]["messages"]
    joined = "\n".join(captured_messages) if isinstance(captured_messages, list) else str(captured_messages)
    assert "WMT" in joined
    assert "AMZN" in joined
    assert "PYPL" in joined
    assert "No significant signals today." not in joined
    assert mock_call_claude.call_count == 0

    completed_calls = [
        c for c in mock_write_event.call_args_list
        if len(c[0]) >= 4 and c[0][1] == "notifier_done" and c[0][2] == SCORING_DATE and c[0][3] == "completed"
    ]
    assert completed_calls, "Expected notifier_done completed event"


def test_run_notifier_claude_fallback_propagates(db_connection, tmp_path, mocker):
    """When Claude returns the fallback response, it propagates through to the message."""
    _insert_scorer_done(db_connection)
    _insert_indicators(db_connection)

    db_connection.execute(
        """INSERT OR REPLACE INTO scores_daily
           (ticker, date, signal, confidence, final_score, regime,
            daily_score, weekly_score, trend_score, momentum_score, volume_score,
            volatility_score, candlestick_score, structural_score,
            sentiment_score, fundamental_score, macro_score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("WMT", SCORING_DATE, "BULLISH", 85.0, 50.0, "trending",
         5.0, 3.0, 10.0, 8.0, 4.0, -2.0, 6.0, 3.0, 5.0, 4.0, 2.0),
    )
    db_connection.execute(
        "INSERT OR IGNORE INTO tickers (symbol, sector, sector_etf, active) VALUES (?, ?, ?, ?)",
        ("WMT", "Consumer", "XLY", 1),
    )
    # Seed indicators + OHLCV so build_ticker_context succeeds
    db_connection.execute(
        """INSERT OR REPLACE INTO indicators_daily
           (ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal, macd_histogram,
            adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower)
           VALUES (?, ?, 152.0, 150.0, 148.0, -0.5, -0.3, -0.2, 18.9, 38.7,
                   22.0, 25.0, -80.0, -78.0, 9500000.0, -0.12, 4200000.0,
                   160.0, 145.0, 0.22, 2.1, 161.0, 144.0)""",
        ("WMT", SCORING_DATE),
    )
    db_connection.execute(
        "INSERT OR REPLACE INTO ohlcv_daily "
        "(ticker, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("WMT", SCORING_DATE, 153.0, 156.0, 152.0, 155.0, 50_000_000),
    )
    db_connection.commit()

    config_with_ai = {
        **SAMPLE_CONFIG,
        "telegram": {**SAMPLE_CONFIG["telegram"], "include_ai_reasoning": True, "confidence_threshold": 70},
    }

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=config_with_ai)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.ai_reasoner.build_market_context",
        return_value="Market context: stable.",
    )
    mocker.patch(
        "src.notifier.ai_reasoner.call_claude",
        return_value=_FALLBACK_RESPONSE,
    )
    mock_send = mocker.patch(
        "src.notifier.main.send_daily_report",
        return_value={"sent": 1, "failed": 0, "total_subscribers": 1},
    )
    mocker.patch("src.notifier.main.send_heartbeat", return_value=True)
    mocker.patch("src.notifier.main.log_pipeline_run")
    mock_write_event = mocker.patch("src.notifier.main.write_pipeline_event")

    from src.notifier.main import run_notifier

    result = run_notifier(db_path=str(tmp_path / "test.db"))
    assert result is not None

    captured_messages = mock_send.call_args[0][0] if mock_send.call_args[0] else mock_send.call_args[1]["messages"]
    joined = "\n".join(captured_messages) if isinstance(captured_messages, list) else str(captured_messages)
    assert _FALLBACK_RESPONSE in joined

    completed_calls = [
        c for c in mock_write_event.call_args_list
        if len(c[0]) >= 4 and c[0][1] == "notifier_done" and c[0][2] == SCORING_DATE and c[0][3] == "completed"
    ]
    assert completed_calls, "Expected notifier_done completed event"


def test_run_notifier_db_error_propagates(db_connection, tmp_path, mocker):
    """DB errors from get_qualifying_tickers must propagate (no try/except masking).

    Also asserts that a `failed` pipeline event is written and no Telegram
    message is sent, so a future contributor restoring a swallowing try/except
    would break this test loudly.
    """
    _insert_scorer_done(db_connection)
    _insert_indicators(db_connection)

    mocker.patch("src.notifier.main.load_env")
    mocker.patch("src.notifier.main.load_config", return_value=SAMPLE_CONFIG)
    mocker.patch("src.notifier.main.get_connection", return_value=db_connection)
    mocker.patch("src.notifier.main.get_telegram_config", return_value=SAMPLE_TELEGRAM_CONFIG)
    mocker.patch(
        "src.notifier.ai_reasoner.get_qualifying_tickers",
        side_effect=sqlite3.OperationalError("simulated DB lock"),
    )
    mock_write_event = mocker.patch("src.notifier.main.write_pipeline_event")
    mock_send = mocker.patch("src.notifier.main.send_daily_report")
    mocker.patch("src.notifier.main.log_pipeline_run")

    from src.notifier.main import run_notifier

    with pytest.raises(sqlite3.OperationalError):
        run_notifier(db_path=str(tmp_path / "test.db"))

    assert not mock_send.called, "Telegram send must not happen when DB errors out"

    failed_calls = [
        c for c in mock_write_event.call_args_list
        if len(c[0]) >= 4 and c[0][1] == "notifier_done" and c[0][2] == SCORING_DATE and c[0][3] == "failed"
    ]
    assert failed_calls, "Expected notifier_done failed event on DB error"

    completed_calls = [
        c for c in mock_write_event.call_args_list
        if len(c[0]) >= 4 and c[0][1] == "notifier_done" and c[0][2] == SCORING_DATE and c[0][3] == "completed"
    ]
    assert not completed_calls, "Must not write completed event when DB errors out"
