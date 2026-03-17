"""
Shared utility functions for the backfiller module.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _is_table_data_fresh(
    db_conn: sqlite3.Connection,
    table: str,
    ticker: str,
    threshold_days: int,
) -> bool:
    """
    Return True if data in table for ticker was fetched within threshold_days.

    Queries MAX(fetched_at) FROM {table} WHERE ticker = ? to determine when
    data was last written. Returns False (stale) if no rows exist for the
    ticker, if the most-recent fetched_at is >= threshold_days old, or if
    threshold_days is 0 (which forces re-fetch unconditionally).

    Args:
        db_conn: Open SQLite connection with the target table.
        table: Name of the database table to check, e.g. 'earnings_calendar'.
        ticker: Ticker symbol to check freshness for, e.g. 'AAPL'.
        threshold_days: Maximum age in days before data is considered stale.
            A value of 0 always returns False.

    Returns:
        True if data exists and age < threshold_days, False otherwise.
    """
    if threshold_days == 0:
        return False

    row = db_conn.execute(
        f"SELECT MAX(fetched_at) AS last_fetched FROM {table} WHERE ticker = ?",  # noqa: S608
        (ticker,),
    ).fetchone()

    if row is None or row["last_fetched"] is None:
        return False

    last_fetched_str = row["last_fetched"]
    try:
        last_fetched = datetime.fromisoformat(last_fetched_str)
    except ValueError:
        return False

    if last_fetched.tzinfo is None:
        last_fetched = last_fetched.replace(tzinfo=timezone.utc)

    now = datetime.now(tz=timezone.utc)
    age_days = (now - last_fetched).total_seconds() / 86400

    if age_days < threshold_days:
        logger.info(
            f"Skipping {ticker} {table}: data is fresh (last fetched {age_days:.1f} days ago)"
        )
        return True

    return False
