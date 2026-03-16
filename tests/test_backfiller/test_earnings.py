"""Tests for src/backfiller/earnings.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.backfiller.earnings import (
    backfill_all_earnings,
    backfill_earnings_for_ticker,
    convert_finnhub_to_earnings_row,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_finnhub_record() -> dict:
    """
    Return a single Finnhub earnings record for AAPL Q1 2025.
    """
    return {
        "symbol": "AAPL",
        "date": "2025-01-30",
        "epsActual": 2.40,
        "epsEstimate": 2.35,
        "revenueActual": 124300000000,
        "revenueEstimate": 118900000000,
        "quarter": 1,
        "year": 2025,
    }


@pytest.fixture
def mock_finnhub_client(sample_finnhub_record) -> MagicMock:
    """
    Return a MagicMock FinnhubClient returning 3 AAPL earnings records.
    """
    client = MagicMock()
    client.fetch_earnings_calendar.return_value = [
        sample_finnhub_record,
        {**sample_finnhub_record, "date": "2024-10-31", "quarter": 4, "year": 2024,
         "epsActual": 2.30, "epsEstimate": 2.25},
        {**sample_finnhub_record, "date": "2024-07-31", "quarter": 3, "year": 2024,
         "epsActual": 1.45, "epsEstimate": 1.40},
    ]
    return client


# ---------------------------------------------------------------------------
# Tests for convert_finnhub_to_earnings_row
# ---------------------------------------------------------------------------

def test_convert_finnhub_maps_all_fields(sample_finnhub_record) -> None:
    """
    Finnhub fields map correctly to the DB schema column names.
    """
    row = convert_finnhub_to_earnings_row(sample_finnhub_record)

    assert row["ticker"] == "AAPL"
    assert row["earnings_date"] == "2025-01-30"
    assert row["actual_eps"] == pytest.approx(2.40)
    assert row["estimated_eps"] == pytest.approx(2.35)
    assert row["revenue_actual"] == pytest.approx(124300000000)
    assert row["revenue_estimated"] == pytest.approx(118900000000)
    assert row["fiscal_quarter"] == "Q1"
    assert row["fiscal_year"] == 2025


def test_convert_finnhub_computes_eps_surprise(sample_finnhub_record) -> None:
    """
    eps_surprise is computed as actual_eps - estimated_eps = 2.40 - 2.35 = 0.05.
    """
    row = convert_finnhub_to_earnings_row(sample_finnhub_record)
    assert row["eps_surprise"] == pytest.approx(0.05)


def test_convert_finnhub_formats_fiscal_quarter() -> None:
    """
    Finnhub quarter integer is formatted as 'Q{n}' string (e.g., 3 → 'Q3').
    """
    record = {
        "symbol": "MSFT", "date": "2024-07-25",
        "epsActual": 3.00, "epsEstimate": 2.90,
        "revenueActual": 64700000000, "revenueEstimate": 64300000000,
        "quarter": 3, "year": 2024,
    }
    row = convert_finnhub_to_earnings_row(record)
    assert row["fiscal_quarter"] == "Q3"


def test_convert_finnhub_handles_no_actuals() -> None:
    """
    Future earnings record with None actuals stores NULL for actual_eps and revenue_actual.
    eps_surprise should also be None.
    """
    record = {
        "symbol": "AAPL", "date": "2025-07-31",
        "epsActual": None, "epsEstimate": 1.60,
        "revenueActual": None, "revenueEstimate": 130000000000,
        "quarter": 3, "year": 2025,
    }
    row = convert_finnhub_to_earnings_row(record)

    assert row["actual_eps"] is None
    assert row["revenue_actual"] is None
    assert row["eps_surprise"] is None


def test_convert_finnhub_sets_fetched_at(sample_finnhub_record) -> None:
    """
    fetched_at is set to a non-null UTC timestamp string.
    """
    row = convert_finnhub_to_earnings_row(sample_finnhub_record)
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for backfill_earnings_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_earnings_stores_records(
    db_connection, mock_finnhub_client
) -> None:
    """
    Mock FinnhubClient returning 3 AAPL earnings records.
    Verify 3 rows exist in earnings_calendar table.
    """
    backfill_earnings_for_ticker(
        db_connection, mock_finnhub_client, "AAPL",
        from_date="2024-01-01", to_date="2025-12-31"
    )

    count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_earnings_maps_finnhub_fields(
    db_connection, sample_finnhub_record
) -> None:
    """
    Finnhub record fields are correctly mapped to DB column names.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.return_value = [sample_finnhub_record]

    backfill_earnings_for_ticker(
        db_connection, mock_client, "AAPL",
        from_date="2025-01-01", to_date="2025-03-31"
    )

    row = db_connection.execute(
        "SELECT * FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()

    assert row["ticker"] == "AAPL"
    assert row["earnings_date"] == "2025-01-30"
    assert row["actual_eps"] == pytest.approx(2.40)
    assert row["estimated_eps"] == pytest.approx(2.35)
    assert row["revenue_actual"] == pytest.approx(124300000000)
    assert row["revenue_estimated"] == pytest.approx(118900000000)
    assert row["fiscal_quarter"] == "Q1"
    assert row["fiscal_year"] == 2025


def test_backfill_earnings_computes_eps_surprise(db_connection) -> None:
    """
    eps_surprise is stored as actual_eps - estimated_eps = 2.40 - 2.35 = 0.05.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.return_value = [
        {
            "symbol": "AAPL", "date": "2025-01-30",
            "epsActual": 2.40, "epsEstimate": 2.35,
            "revenueActual": 124300000000, "revenueEstimate": 118900000000,
            "quarter": 1, "year": 2025,
        }
    ]

    backfill_earnings_for_ticker(
        db_connection, mock_client, "AAPL",
        from_date="2025-01-01", to_date="2025-03-31"
    )

    row = db_connection.execute(
        "SELECT eps_surprise FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()
    assert row["eps_surprise"] == pytest.approx(0.05)


def test_backfill_earnings_handles_no_actuals(db_connection) -> None:
    """
    Future earnings record with None actuals stores NULL without crashing.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.return_value = [
        {
            "symbol": "AAPL", "date": "2025-07-31",
            "epsActual": None, "epsEstimate": 1.60,
            "revenueActual": None, "revenueEstimate": 130000000000,
            "quarter": 3, "year": 2025,
        }
    ]

    count = backfill_earnings_for_ticker(
        db_connection, mock_client, "AAPL",
        from_date="2025-01-01", to_date="2025-12-31"
    )

    assert count == 1
    row = db_connection.execute(
        "SELECT actual_eps, revenue_actual FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()
    assert row["actual_eps"] is None
    assert row["revenue_actual"] is None


def test_backfill_earnings_handles_finnhub_error(db_connection) -> None:
    """
    When FinnhubClient raises an exception, no crash occurs and an alert is logged.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.side_effect = RuntimeError("Finnhub timeout")

    result = backfill_earnings_for_ticker(
        db_connection, mock_client, "AAPL",
        from_date="2025-01-01", to_date="2025-03-31"
    )

    assert result == 0

    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert alert_count >= 1


def test_backfill_earnings_is_idempotent(
    db_connection, mock_finnhub_client
) -> None:
    """
    Running backfill_earnings_for_ticker twice with same data yields no duplicate rows.
    """
    backfill_earnings_for_ticker(
        db_connection, mock_finnhub_client, "AAPL",
        from_date="2024-01-01", to_date="2025-12-31"
    )
    backfill_earnings_for_ticker(
        db_connection, mock_finnhub_client, "AAPL",
        from_date="2024-01-01", to_date="2025-12-31"
    )

    count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_earnings_sets_fetched_at(
    db_connection, mock_finnhub_client
) -> None:
    """
    Every inserted row has a non-null fetched_at timestamp.
    """
    backfill_earnings_for_ticker(
        db_connection, mock_finnhub_client, "AAPL",
        from_date="2024-01-01", to_date="2025-12-31"
    )

    rows = db_connection.execute(
        "SELECT fetched_at FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchall()
    assert all(row["fetched_at"] is not None for row in rows)


def test_backfill_earnings_filters_by_ticker(db_connection) -> None:
    """
    If Finnhub returns records for multiple tickers, only the requested ticker is stored.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.return_value = [
        {
            "symbol": "AAPL", "date": "2025-01-30",
            "epsActual": 2.40, "epsEstimate": 2.35,
            "revenueActual": 124300000000, "revenueEstimate": 118900000000,
            "quarter": 1, "year": 2025,
        },
        {
            "symbol": "MSFT", "date": "2025-01-29",
            "epsActual": 3.23, "epsEstimate": 3.10,
            "revenueActual": 69600000000, "revenueEstimate": 68900000000,
            "quarter": 2, "year": 2025,
        },
    ]

    backfill_earnings_for_ticker(
        db_connection, mock_client, "AAPL",
        from_date="2025-01-01", to_date="2025-03-31"
    )

    aapl_count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    msft_count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='MSFT'"
    ).fetchone()[0]

    assert aapl_count == 1
    assert msft_count == 0


# ---------------------------------------------------------------------------
# Tests for backfill_all_earnings
# ---------------------------------------------------------------------------

def test_backfill_all_earnings_processes_each_ticker(
    db_connection, sample_tickers_list
) -> None:
    """
    With 3 tickers, fetch_earnings_calendar is called exactly 3 times.
    """
    mock_client = MagicMock()
    mock_client.fetch_earnings_calendar.return_value = []
    config = {"earnings": {"lookback_years": 2}}

    backfill_all_earnings(db_connection, mock_client, sample_tickers_list, config)

    assert mock_client.fetch_earnings_calendar.call_count == 3


def test_backfill_all_earnings_respects_rate_limit(db_connection) -> None:
    """
    The FinnhubClient's internal rate limiting is applied between calls.
    time.sleep is called at least once for rapid consecutive calls.
    """
    from src.common.api_client import FinnhubClient

    finnhub_client = FinnhubClient(api_key="test_key", delay_seconds=1.0)
    mock_fh_inner = MagicMock()
    mock_fh_inner.earnings_calendar.return_value = {"earningsCalendar": []}
    finnhub_client.fh_client = mock_fh_inner

    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]
    config = {"earnings": {"lookback_years": 1}}

    with patch("time.sleep") as mock_sleep:
        backfill_all_earnings(db_connection, finnhub_client, tickers, config)

    # At least 2 rate-limit sleeps for 3 rapid consecutive calls (2nd and 3rd trigger sleep)
    assert mock_sleep.call_count >= 2
