"""Tests for src/backfiller/utils.py.

All tests are written first (TDD). No external API calls.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.backfiller.utils import _is_table_data_fresh


# ---------------------------------------------------------------------------
# Tests for _is_table_data_fresh
# ---------------------------------------------------------------------------

def test_is_table_data_fresh_returns_true_when_fresh(db_connection) -> None:
    """
    When fetched_at is less than threshold_days ago, returns True.
    """
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(hours=2)).isoformat()
    db_connection.execute(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        ("AAPL", "2025-01-30", fetched_at),
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is True


def test_is_table_data_fresh_returns_false_when_stale(db_connection) -> None:
    """
    When fetched_at is >= threshold_days ago, returns False.
    """
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat()
    db_connection.execute(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        ("AAPL", "2025-01-30", fetched_at),
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is False


def test_is_table_data_fresh_returns_false_when_no_data(db_connection) -> None:
    """
    When no rows exist for the ticker, returns False.
    """
    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is False


def test_is_table_data_fresh_returns_false_when_threshold_zero(db_connection) -> None:
    """
    When threshold_days is 0, always returns False (force re-fetch).
    """
    fetched_at = datetime.now(tz=timezone.utc).isoformat()
    db_connection.execute(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        ("AAPL", "2025-01-30", fetched_at),
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=0)

    assert result is False


def test_is_table_data_fresh_uses_max_fetched_at(db_connection) -> None:
    """
    When multiple rows exist, uses MAX(fetched_at) for the freshness check.
    An old row does not make fresh data appear stale.
    """
    old_fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=20)).isoformat()
    recent_fetched_at = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    db_connection.executemany(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        [
            ("AAPL", "2024-07-31", old_fetched_at),
            ("AAPL", "2025-01-30", recent_fetched_at),
        ],
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is True


def test_is_table_data_fresh_is_ticker_scoped(db_connection) -> None:
    """
    Fresh data for MSFT does not affect the freshness check for AAPL.
    """
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    db_connection.execute(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        ("MSFT", "2025-01-30", fetched_at),
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is False


def test_is_table_data_fresh_exactly_at_threshold_is_stale(db_connection) -> None:
    """
    Data fetched exactly threshold_days ago is considered stale (age >= threshold).
    """
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=7)).isoformat()
    db_connection.execute(
        "INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at) VALUES (?, ?, ?)",
        ("AAPL", "2025-01-30", fetched_at),
    )
    db_connection.commit()

    result = _is_table_data_fresh(db_connection, "earnings_calendar", "AAPL", threshold_days=7)

    assert result is False
