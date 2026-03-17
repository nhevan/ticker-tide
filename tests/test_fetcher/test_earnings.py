"""Tests for src/fetcher/earnings.py.

All tests are written first (TDD). All external API calls are mocked.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.fetcher.earnings import (
    _is_earnings_stale,
    refresh_earnings_for_ticker,
    run_periodic_earnings,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_yfinance_records() -> list[dict]:
    """
    Return 3 yfinance-style earnings records for AAPL.
    Matches the format returned by fetch_earnings_dates().
    """
    base = {
        "ticker": "AAPL",
        "estimated_eps": 2.35,
        "actual_eps": 2.40,
        "eps_surprise": 0.05,
        "fiscal_quarter": None,
        "fiscal_year": None,
        "revenue_estimated": None,
        "revenue_actual": None,
    }
    return [
        {**base, "earnings_date": "2025-01-30"},
        {**base, "earnings_date": "2024-10-31", "actual_eps": 2.30, "estimated_eps": 2.25},
        {**base, "earnings_date": "2024-07-31", "actual_eps": 1.45, "estimated_eps": 1.40},
    ]


@pytest.fixture
def fetcher_config() -> dict:
    """Return a minimal fetcher config dict with polling intervals."""
    return {
        "polling_intervals": {
            "earnings_calendar_days": 7,
        }
    }


# ---------------------------------------------------------------------------
# Tests for _is_earnings_stale
# ---------------------------------------------------------------------------

def test_is_earnings_stale_returns_true_when_no_data(db_connection) -> None:
    """
    A ticker with no rows in earnings_calendar is considered stale.
    """
    assert _is_earnings_stale(db_connection, "AAPL", refresh_days=7) is True


def test_is_earnings_stale_returns_false_when_recently_fetched(
    db_connection,
) -> None:
    """
    A ticker fetched 1 day ago with a 7-day threshold is not stale.
    """
    recent_ts = (datetime.now(tz=timezone.utc) - timedelta(days=1)).isoformat()
    db_connection.execute(
        """
        INSERT INTO earnings_calendar
            (ticker, earnings_date, fetched_at)
        VALUES (?, ?, ?)
        """,
        ("AAPL", "2025-01-30", recent_ts),
    )
    db_connection.commit()

    assert _is_earnings_stale(db_connection, "AAPL", refresh_days=7) is False


def test_is_earnings_stale_returns_true_when_old_data(db_connection) -> None:
    """
    A ticker fetched 10 days ago with a 7-day threshold is stale.
    """
    old_ts = (datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat()
    db_connection.execute(
        """
        INSERT INTO earnings_calendar
            (ticker, earnings_date, fetched_at)
        VALUES (?, ?, ?)
        """,
        ("AAPL", "2025-01-30", old_ts),
    )
    db_connection.commit()

    assert _is_earnings_stale(db_connection, "AAPL", refresh_days=7) is True


def test_is_earnings_stale_at_exact_boundary(db_connection) -> None:
    """
    A ticker fetched exactly refresh_days ago is considered stale (>= comparison).
    """
    boundary_ts = (datetime.now(tz=timezone.utc) - timedelta(days=7)).isoformat()
    db_connection.execute(
        """
        INSERT INTO earnings_calendar
            (ticker, earnings_date, fetched_at)
        VALUES (?, ?, ?)
        """,
        ("AAPL", "2025-01-30", boundary_ts),
    )
    db_connection.commit()

    assert _is_earnings_stale(db_connection, "AAPL", refresh_days=7) is True


# ---------------------------------------------------------------------------
# Tests for refresh_earnings_for_ticker
# ---------------------------------------------------------------------------

def test_refresh_earnings_stores_records(
    db_connection, sample_yfinance_records
) -> None:
    """
    Mock fetch_earnings_dates returning 3 records. Verify 3 rows inserted.
    """
    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        count = refresh_earnings_for_ticker(db_connection, "AAPL")

    assert count == 3
    stored = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert stored == 3


def test_refresh_earnings_is_idempotent(
    db_connection, sample_yfinance_records
) -> None:
    """
    Running refresh twice with same data yields no duplicate rows.
    """
    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        refresh_earnings_for_ticker(db_connection, "AAPL")
        refresh_earnings_for_ticker(db_connection, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_refresh_earnings_handles_fetch_error(db_connection) -> None:
    """
    When fetch_earnings_dates raises, the exception propagates so that
    run_periodic_earnings can count it as a failure.
    """
    import pytest as _pytest
    with patch("src.fetcher.earnings.fetch_earnings_dates",
               side_effect=RuntimeError("yfinance error")):
        with _pytest.raises(RuntimeError):
            refresh_earnings_for_ticker(db_connection, "AAPL")


def test_refresh_earnings_sets_fetched_at(
    db_connection, sample_yfinance_records
) -> None:
    """
    Every inserted row has a non-null fetched_at timestamp.
    """
    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        refresh_earnings_for_ticker(db_connection, "AAPL")

    rows = db_connection.execute(
        "SELECT fetched_at FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchall()
    assert all(row["fetched_at"] is not None for row in rows)


# ---------------------------------------------------------------------------
# Tests for run_periodic_earnings
# ---------------------------------------------------------------------------

def test_run_periodic_earnings_refreshes_stale_tickers(
    db_connection, sample_tickers_list, fetcher_config, sample_yfinance_records
) -> None:
    """
    Tickers with no prior data are refreshed. fetch_earnings_dates is called
    once per ticker.
    """
    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records) as mock_fetch:
        result = run_periodic_earnings(db_connection, sample_tickers_list, fetcher_config)

    assert mock_fetch.call_count == 3
    assert result["refreshed"] == 3
    assert result["skipped"] == 0
    assert result["failed"] == 0


def test_run_periodic_earnings_skips_fresh_tickers(
    db_connection, sample_tickers_list, fetcher_config
) -> None:
    """
    Tickers refreshed within the threshold window are skipped.
    """
    recent_ts = (datetime.now(tz=timezone.utc) - timedelta(days=1)).isoformat()
    for ticker_cfg in sample_tickers_list:
        db_connection.execute(
            """
            INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at)
            VALUES (?, ?, ?)
            """,
            (ticker_cfg["symbol"], "2025-01-30", recent_ts),
        )
    db_connection.commit()

    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=[]) as mock_fetch:
        result = run_periodic_earnings(db_connection, sample_tickers_list, fetcher_config)

    assert mock_fetch.call_count == 0
    assert result["skipped"] == 3
    assert result["refreshed"] == 0


def test_run_periodic_earnings_continues_after_error(
    db_connection, sample_tickers_list, fetcher_config
) -> None:
    """
    A failure on one ticker does not stop the run. Other tickers are still processed.
    """
    call_count = 0

    def fetch_side_effect(ticker: str) -> list:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("network error")
        return []

    with patch("src.fetcher.earnings.fetch_earnings_dates",
               side_effect=fetch_side_effect):
        result = run_periodic_earnings(db_connection, sample_tickers_list, fetcher_config)

    assert result["failed"] == 1
    assert result["refreshed"] == 2


def test_run_periodic_earnings_uses_config_refresh_days(
    db_connection, sample_tickers_list
) -> None:
    """
    The refresh_days threshold is read from config. With a 30-day threshold,
    tickers fetched 15 days ago are still considered fresh.
    """
    fifteen_days_ago = (datetime.now(tz=timezone.utc) - timedelta(days=15)).isoformat()
    for ticker_cfg in sample_tickers_list:
        db_connection.execute(
            """
            INSERT INTO earnings_calendar (ticker, earnings_date, fetched_at)
            VALUES (?, ?, ?)
            """,
            (ticker_cfg["symbol"], "2025-01-30", fifteen_days_ago),
        )
    db_connection.commit()

    config = {"polling_intervals": {"earnings_calendar_days": 30}}

    with patch("src.fetcher.earnings.fetch_earnings_dates",
               return_value=[]) as mock_fetch:
        result = run_periodic_earnings(db_connection, sample_tickers_list, config)

    assert mock_fetch.call_count == 0
    assert result["skipped"] == 3


def test_run_periodic_earnings_returns_summary_keys(
    db_connection, sample_tickers_list, fetcher_config
) -> None:
    """
    Return dict always contains refreshed, skipped, failed, and total_rows.
    """
    with patch("src.fetcher.earnings.fetch_earnings_dates", return_value=[]):
        result = run_periodic_earnings(db_connection, sample_tickers_list, fetcher_config)

    assert "refreshed" in result
    assert "skipped" in result
    assert "failed" in result
    assert "total_rows" in result
