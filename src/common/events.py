"""
Pipeline event system for the Stock Signal Engine.

Uses SQLite pipeline_events table (Option B) for event-driven triggering
between pipeline phases. Also handles alerts logging, pipeline run tracking,
and trading day detection.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def write_pipeline_event(
    db_conn: sqlite3.Connection,
    event: str,
    date: str,
    status: str,
    details: str = None,
) -> None:
    """
    Insert or replace a pipeline event record.

    Uses INSERT OR REPLACE so duplicate (event, date) pairs update the existing row.
    Automatically sets timestamp to the current UTC time.

    Args:
        db_conn: Open SQLite connection with the pipeline_events table.
        event: Event name (e.g. "fetcher_done").
        date: Trading date in YYYY-MM-DD format.
        status: Event status (e.g. "ready", "processing", "completed", "failed").
        details: Optional free-text details string.
    """
    timestamp = _utc_now_iso()
    db_conn.execute(
        """
        INSERT OR REPLACE INTO pipeline_events (event, date, status, timestamp, details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (event, date, status, timestamp, details),
    )
    db_conn.commit()


def check_pipeline_event(
    db_conn: sqlite3.Connection, event: str, date: str
) -> bool:
    """
    Return True if a pipeline event with the given name and date exists.

    Args:
        db_conn: Open SQLite connection with the pipeline_events table.
        event: Event name to look up.
        date: Trading date in YYYY-MM-DD format.

    Returns:
        True if the event row exists, False otherwise.
    """
    row = db_conn.execute(
        "SELECT 1 FROM pipeline_events WHERE event=? AND date=?",
        (event, date),
    ).fetchone()
    return row is not None


def get_pipeline_event_status(
    db_conn: sqlite3.Connection, event: str, date: str
) -> str | None:
    """
    Return the status of a pipeline event, or None if it does not exist.

    Args:
        db_conn: Open SQLite connection with the pipeline_events table.
        event: Event name to look up.
        date: Trading date in YYYY-MM-DD format.

    Returns:
        The status string if found, None otherwise.
    """
    row = db_conn.execute(
        "SELECT status FROM pipeline_events WHERE event=? AND date=?",
        (event, date),
    ).fetchone()
    return row["status"] if row else None


def update_pipeline_event(
    db_conn: sqlite3.Connection,
    event: str,
    date: str,
    new_status: str,
    details: str = None,
) -> None:
    """
    Update the status and timestamp of an existing pipeline event.

    Args:
        db_conn: Open SQLite connection with the pipeline_events table.
        event: Event name to update.
        date: Trading date in YYYY-MM-DD format.
        new_status: New status value to store.
        details: Optional updated details string.
    """
    timestamp = _utc_now_iso()
    db_conn.execute(
        """
        UPDATE pipeline_events
        SET status=?, timestamp=?, details=?
        WHERE event=? AND date=?
        """,
        (new_status, timestamp, details, event, date),
    )
    db_conn.commit()


def is_trading_day(date_str: str, holidays: list[str]) -> bool:
    """
    Return True if the given date is a trading day (weekday, not a holiday).

    Args:
        date_str: Date in YYYY-MM-DD format.
        holidays: List of holiday date strings in YYYY-MM-DD format.

    Returns:
        False if date_str falls on Saturday (5) or Sunday (6), or is in holidays.
        True otherwise.
    """
    parsed = datetime.strptime(date_str, "%Y-%m-%d")
    if parsed.weekday() >= 5:
        return False
    if date_str in holidays:
        return False
    return True


def log_alert(
    db_conn: sqlite3.Connection,
    ticker: str | None,
    date: str,
    phase: str,
    severity: str,
    message: str,
) -> None:
    """
    Insert an alert record into the alerts_log table.

    Automatically sets created_at to the current UTC time.
    notified defaults to 0 (False).

    Args:
        db_conn: Open SQLite connection with the alerts_log table.
        ticker: Ticker symbol, or None for system-level alerts.
        date: Trading date in YYYY-MM-DD format.
        phase: Pipeline phase that raised the alert (e.g. "fetcher").
        severity: One of "info", "warning", "error".
        message: Human-readable description of the alert.
    """
    created_at = _utc_now_iso()
    db_conn.execute(
        """
        INSERT INTO alerts_log (ticker, date, phase, severity, message, notified, created_at)
        VALUES (?, ?, ?, ?, ?, 0, ?)
        """,
        (ticker, date, phase, severity, message, created_at),
    )
    db_conn.commit()


def get_alerts_for_date(
    db_conn: sqlite3.Connection, date: str
) -> list[dict]:
    """
    Return all alerts for the given date, ordered by created_at ascending.

    Args:
        db_conn: Open SQLite connection with the alerts_log table.
        date: Trading date in YYYY-MM-DD format.

    Returns:
        A list of dicts, each containing all columns from alerts_log.
    """
    rows = db_conn.execute(
        "SELECT * FROM alerts_log WHERE date=? ORDER BY created_at ASC",
        (date,),
    ).fetchall()
    return [dict(row) for row in rows]


def log_pipeline_run(
    db_conn: sqlite3.Connection,
    date: str,
    phase: str,
    started_at: str,
    completed_at: str,
    duration_seconds: float,
    tickers_processed: int,
    tickers_skipped: int,
    tickers_failed: int,
    api_calls_made: int,
    status: str,
    error_summary: str | None = None,
) -> None:
    """
    Insert a pipeline run record into the pipeline_runs table.

    Args:
        db_conn: Open SQLite connection with the pipeline_runs table.
        date: Trading date in YYYY-MM-DD format.
        phase: Pipeline phase name (e.g. "fetcher", "calculator").
        started_at: ISO 8601 UTC timestamp when the run began.
        completed_at: ISO 8601 UTC timestamp when the run ended.
        duration_seconds: Elapsed time in seconds.
        tickers_processed: Number of tickers successfully processed.
        tickers_skipped: Number of tickers skipped.
        tickers_failed: Number of tickers that failed.
        api_calls_made: Total API calls made during the run.
        status: Overall run status ("success", "partial", "failed").
        error_summary: Optional string summarising errors encountered.
    """
    db_conn.execute(
        """
        INSERT INTO pipeline_runs
            (date, phase, started_at, completed_at, duration_seconds,
             tickers_processed, tickers_skipped, tickers_failed,
             api_calls_made, status, error_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date,
            phase,
            started_at,
            completed_at,
            duration_seconds,
            tickers_processed,
            tickers_skipped,
            tickers_failed,
            api_calls_made,
            status,
            error_summary,
        ),
    )
    db_conn.commit()


def get_latest_pipeline_run(
    db_conn: sqlite3.Connection, phase: str
) -> dict | None:
    """
    Return the most recent pipeline run for the given phase, or None if none exist.

    Orders by date DESC, then started_at DESC, and returns the first row.

    Args:
        db_conn: Open SQLite connection with the pipeline_runs table.
        phase: Pipeline phase to query (e.g. "fetcher").

    Returns:
        A dict of all pipeline_run columns, or None if no runs exist for the phase.
    """
    row = db_conn.execute(
        """
        SELECT * FROM pipeline_runs
        WHERE phase=?
        ORDER BY date DESC, started_at DESC
        LIMIT 1
        """,
        (phase,),
    ).fetchone()
    return dict(row) if row else None
