"""
Tests for src/common/events.py — pipeline event system, alerts, pipeline runs.
"""

import sqlite3
import time

import pytest

from src.common.events import (
    check_pipeline_event,
    get_alerts_for_date,
    get_latest_pipeline_run,
    get_pipeline_event_status,
    is_trading_day,
    log_alert,
    log_pipeline_run,
    update_pipeline_event,
    write_pipeline_event,
)


# ---------------------------------------------------------------------------
# write_pipeline_event / check_pipeline_event / get_pipeline_event_status
# ---------------------------------------------------------------------------


def test_write_pipeline_event(db_connection: sqlite3.Connection):
    """Written event should appear in pipeline_events with correct fields."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    row = db_connection.execute(
        "SELECT event, date, status, timestamp FROM pipeline_events "
        "WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()
    assert row is not None
    assert row["event"] == "fetcher_done"
    assert row["date"] == "2026-03-16"
    assert row["status"] == "completed"
    assert row["timestamp"] != ""


def test_write_pipeline_event_sets_timestamp(db_connection: sqlite3.Connection):
    """Timestamp stored should be a valid ISO format UTC string."""
    from datetime import datetime

    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    row = db_connection.execute(
        "SELECT timestamp FROM pipeline_events WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()
    ts = row["timestamp"]
    # Should parse as ISO 8601 (datetime.fromisoformat strips trailing Z on py<3.11)
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert parsed is not None


def test_check_pipeline_event_exists(db_connection: sqlite3.Connection):
    """check_pipeline_event returns True when the event exists."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    assert check_pipeline_event(db_connection, "fetcher_done", "2026-03-16") is True


def test_check_pipeline_event_not_exists(db_connection: sqlite3.Connection):
    """check_pipeline_event returns False when the event does not exist."""
    assert check_pipeline_event(db_connection, "fetcher_done", "2026-03-16") is False


def test_check_pipeline_event_wrong_date(db_connection: sqlite3.Connection):
    """check_pipeline_event returns False when the date does not match."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    assert check_pipeline_event(db_connection, "fetcher_done", "2026-03-17") is False


def test_update_pipeline_event_status(db_connection: sqlite3.Connection):
    """update_pipeline_event should change the status field."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "ready")
    update_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    row = db_connection.execute(
        "SELECT status FROM pipeline_events WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()
    assert row["status"] == "completed"


def test_update_pipeline_event_updates_timestamp(db_connection: sqlite3.Connection):
    """update_pipeline_event should refresh the timestamp."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "ready")
    first_ts = db_connection.execute(
        "SELECT timestamp FROM pipeline_events WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()["timestamp"]

    time.sleep(0.01)  # ensure clock advances
    update_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")
    second_ts = db_connection.execute(
        "SELECT timestamp FROM pipeline_events WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()["timestamp"]

    assert second_ts != first_ts


def test_write_pipeline_event_is_idempotent(db_connection: sqlite3.Connection):
    """Writing the same event+date twice keeps exactly 1 row with the second status."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "ready")
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "completed")

    rows = db_connection.execute(
        "SELECT status FROM pipeline_events WHERE event=? AND date=?",
        ("fetcher_done", "2026-03-16"),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"


def test_get_pipeline_event_status(db_connection: sqlite3.Connection):
    """get_pipeline_event_status returns the stored status string."""
    write_pipeline_event(db_connection, "fetcher_done", "2026-03-16", "processing")
    status = get_pipeline_event_status(db_connection, "fetcher_done", "2026-03-16")
    assert status == "processing"


def test_get_pipeline_event_status_not_found(db_connection: sqlite3.Connection):
    """get_pipeline_event_status returns None for a non-existent event."""
    status = get_pipeline_event_status(db_connection, "fetcher_done", "2026-03-16")
    assert status is None


# ---------------------------------------------------------------------------
# is_trading_day
# ---------------------------------------------------------------------------


def test_is_trading_day_monday():
    """2026-03-16 is a Monday — should return True."""
    assert is_trading_day("2026-03-16", []) is True


def test_is_trading_day_saturday():
    """2026-03-14 is a Saturday — should return False."""
    assert is_trading_day("2026-03-14", []) is False


def test_is_trading_day_sunday():
    """2026-03-15 is a Sunday — should return False."""
    assert is_trading_day("2026-03-15", []) is False


def test_is_trading_day_holiday():
    """A date in the holidays list should return False."""
    assert is_trading_day("2026-12-25", ["2026-12-25"]) is False


def test_is_trading_day_not_holiday():
    """A weekday not in the holidays list should return True."""
    assert is_trading_day("2026-03-16", ["2026-12-25"]) is True


# ---------------------------------------------------------------------------
# log_alert / get_alerts_for_date
# ---------------------------------------------------------------------------


def test_log_alert(db_connection: sqlite3.Connection):
    """log_alert should insert a row with all correct fields."""
    log_alert(
        db_connection,
        ticker="AAPL",
        date="2026-03-16",
        phase="fetcher",
        severity="error",
        message="API timeout",
    )
    row = db_connection.execute(
        "SELECT * FROM alerts_log WHERE ticker=? AND date=?",
        ("AAPL", "2026-03-16"),
    ).fetchone()
    assert row is not None
    assert row["ticker"] == "AAPL"
    assert row["date"] == "2026-03-16"
    assert row["phase"] == "fetcher"
    assert row["severity"] == "error"
    assert row["message"] == "API timeout"


def test_log_alert_sets_created_at(db_connection: sqlite3.Connection):
    """log_alert should store a valid UTC timestamp in created_at."""
    from datetime import datetime

    log_alert(
        db_connection,
        ticker="AAPL",
        date="2026-03-16",
        phase="fetcher",
        severity="error",
        message="API timeout",
    )
    row = db_connection.execute(
        "SELECT created_at FROM alerts_log WHERE ticker=?", ("AAPL",)
    ).fetchone()
    parsed = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
    assert parsed is not None


def test_log_alert_default_not_notified(db_connection: sqlite3.Connection):
    """Newly inserted alerts should have notified=0."""
    log_alert(
        db_connection,
        ticker="AAPL",
        date="2026-03-16",
        phase="fetcher",
        severity="warning",
        message="slow response",
    )
    row = db_connection.execute(
        "SELECT notified FROM alerts_log WHERE ticker=?", ("AAPL",)
    ).fetchone()
    assert row["notified"] == 0


def test_log_alert_without_ticker(db_connection: sqlite3.Connection):
    """log_alert with ticker=None should work and store NULL ticker."""
    log_alert(
        db_connection,
        ticker=None,
        date="2026-03-16",
        phase="system",
        severity="info",
        message="pipeline started",
    )
    row = db_connection.execute(
        "SELECT ticker FROM alerts_log WHERE phase=?", ("system",)
    ).fetchone()
    assert row is not None
    assert row["ticker"] is None


def test_get_alerts_for_date(db_connection: sqlite3.Connection):
    """get_alerts_for_date should return only alerts matching the given date."""
    log_alert(db_connection, ticker="AAPL", date="2026-03-16", phase="fetcher", severity="error", message="msg1")
    log_alert(db_connection, ticker="MSFT", date="2026-03-16", phase="fetcher", severity="info", message="msg2")
    log_alert(db_connection, ticker="NVDA", date="2026-03-17", phase="fetcher", severity="error", message="msg3")

    alerts = get_alerts_for_date(db_connection, "2026-03-16")
    assert len(alerts) == 2
    dates = {a["date"] for a in alerts}
    assert dates == {"2026-03-16"}


# ---------------------------------------------------------------------------
# log_pipeline_run / get_latest_pipeline_run
# ---------------------------------------------------------------------------


def test_log_pipeline_run(db_connection: sqlite3.Connection):
    """log_pipeline_run should insert a row with all correct fields."""
    log_pipeline_run(
        db_connection,
        date="2026-03-16",
        phase="fetcher",
        started_at="2026-03-16T00:00:00Z",
        completed_at="2026-03-16T00:03:12Z",
        duration_seconds=192.0,
        tickers_processed=48,
        tickers_skipped=2,
        tickers_failed=0,
        api_calls_made=150,
        status="success",
        error_summary=None,
    )
    row = db_connection.execute(
        "SELECT * FROM pipeline_runs WHERE date=? AND phase=?",
        ("2026-03-16", "fetcher"),
    ).fetchone()
    assert row is not None
    assert row["date"] == "2026-03-16"
    assert row["phase"] == "fetcher"
    assert row["started_at"] == "2026-03-16T00:00:00Z"
    assert row["completed_at"] == "2026-03-16T00:03:12Z"
    assert row["duration_seconds"] == 192.0
    assert row["tickers_processed"] == 48
    assert row["tickers_skipped"] == 2
    assert row["tickers_failed"] == 0
    assert row["api_calls_made"] == 150
    assert row["status"] == "success"
    assert row["error_summary"] is None


def test_log_pipeline_run_with_errors(db_connection: sqlite3.Connection):
    """error_summary should be stored correctly when provided."""
    error_text = "INTC: API timeout, GE: no data, F: invalid response"
    log_pipeline_run(
        db_connection,
        date="2026-03-16",
        phase="fetcher",
        started_at="2026-03-16T00:00:00Z",
        completed_at="2026-03-16T00:05:00Z",
        duration_seconds=300.0,
        tickers_processed=45,
        tickers_skipped=2,
        tickers_failed=3,
        api_calls_made=140,
        status="partial",
        error_summary=error_text,
    )
    row = db_connection.execute(
        "SELECT error_summary, status FROM pipeline_runs WHERE date=? AND phase=?",
        ("2026-03-16", "fetcher"),
    ).fetchone()
    assert row["status"] == "partial"
    assert row["error_summary"] == error_text


def test_get_latest_pipeline_run(db_connection: sqlite3.Connection):
    """get_latest_pipeline_run returns the most recent run for a phase."""
    log_pipeline_run(
        db_connection,
        date="2026-03-15",
        phase="fetcher",
        started_at="2026-03-15T00:00:00Z",
        completed_at="2026-03-15T00:03:00Z",
        duration_seconds=180.0,
        tickers_processed=48,
        tickers_skipped=0,
        tickers_failed=0,
        api_calls_made=148,
        status="success",
    )
    log_pipeline_run(
        db_connection,
        date="2026-03-16",
        phase="fetcher",
        started_at="2026-03-16T00:00:00Z",
        completed_at="2026-03-16T00:03:12Z",
        duration_seconds=192.0,
        tickers_processed=48,
        tickers_skipped=2,
        tickers_failed=0,
        api_calls_made=150,
        status="success",
    )
    result = get_latest_pipeline_run(db_connection, "fetcher")
    assert result is not None
    assert result["date"] == "2026-03-16"


def test_get_latest_pipeline_run_no_runs(db_connection: sqlite3.Connection):
    """get_latest_pipeline_run returns None when no runs exist for the phase."""
    result = get_latest_pipeline_run(db_connection, "fetcher")
    assert result is None
