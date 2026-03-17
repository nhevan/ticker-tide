"""Tests for src/backfiller/earnings.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from unittest.mock import patch

import pytest

from src.backfiller.earnings import (
    backfill_all_earnings,
    backfill_earnings_for_ticker,
    convert_yfinance_to_earnings_row,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_yfinance_record() -> dict:
    """
    Return a single yfinance-style earnings record for AAPL Q1 2025.
    Matches the format returned by fetch_earnings_dates().
    """
    return {
        "ticker": "AAPL",
        "earnings_date": "2025-01-30",
        "estimated_eps": 2.35,
        "actual_eps": 2.40,
        "eps_surprise": 0.05,
        "fiscal_quarter": None,
        "fiscal_year": None,
        "revenue_estimated": None,
        "revenue_actual": None,
    }


@pytest.fixture
def sample_yfinance_records(sample_yfinance_record) -> list[dict]:
    """
    Return 3 yfinance-style earnings records for AAPL.
    """
    return [
        sample_yfinance_record,
        {**sample_yfinance_record, "earnings_date": "2024-10-31",
         "actual_eps": 2.30, "estimated_eps": 2.25, "eps_surprise": 0.05},
        {**sample_yfinance_record, "earnings_date": "2024-07-31",
         "actual_eps": 1.45, "estimated_eps": 1.40, "eps_surprise": 0.05},
    ]


# ---------------------------------------------------------------------------
# Tests for convert_yfinance_to_earnings_row
# ---------------------------------------------------------------------------

def test_convert_yfinance_maps_all_fields(sample_yfinance_record) -> None:
    """
    yfinance fields map correctly to the DB schema column names.
    """
    row = convert_yfinance_to_earnings_row(sample_yfinance_record)

    assert row["ticker"] == "AAPL"
    assert row["earnings_date"] == "2025-01-30"
    assert row["actual_eps"] == pytest.approx(2.40)
    assert row["estimated_eps"] == pytest.approx(2.35)
    assert row["eps_surprise"] == pytest.approx(0.05)
    assert row["fiscal_quarter"] is None
    assert row["fiscal_year"] is None
    assert row["revenue_estimated"] is None
    assert row["revenue_actual"] is None


def test_convert_yfinance_sets_fetched_at(sample_yfinance_record) -> None:
    """
    fetched_at is set to a non-null UTC timestamp string.
    """
    row = convert_yfinance_to_earnings_row(sample_yfinance_record)
    assert row["fetched_at"] is not None
    assert "T" in row["fetched_at"]


def test_convert_yfinance_handles_no_actuals() -> None:
    """
    Future earnings record with None actuals stores NULL for actual_eps.
    eps_surprise should also be None.
    """
    record = {
        "ticker": "AAPL",
        "earnings_date": "2025-07-31",
        "estimated_eps": 1.60,
        "actual_eps": None,
        "eps_surprise": None,
        "fiscal_quarter": None,
        "fiscal_year": None,
        "revenue_estimated": None,
        "revenue_actual": None,
    }
    row = convert_yfinance_to_earnings_row(record)

    assert row["actual_eps"] is None
    assert row["eps_surprise"] is None
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for backfill_earnings_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_earnings_stores_records(
    db_connection, sample_yfinance_records
) -> None:
    """
    Mock fetch_earnings_dates returning 3 AAPL records.
    Verify 3 rows exist in earnings_calendar table.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        backfill_earnings_for_ticker(db_connection, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_earnings_maps_yfinance_fields(
    db_connection, sample_yfinance_record
) -> None:
    """
    yfinance record fields are correctly mapped to DB column names.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=[sample_yfinance_record]):
        backfill_earnings_for_ticker(db_connection, "AAPL")

    row = db_connection.execute(
        "SELECT * FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()

    assert row["ticker"] == "AAPL"
    assert row["earnings_date"] == "2025-01-30"
    assert row["actual_eps"] == pytest.approx(2.40)
    assert row["estimated_eps"] == pytest.approx(2.35)
    assert row["eps_surprise"] == pytest.approx(0.05)
    assert row["fiscal_quarter"] is None
    assert row["fiscal_year"] is None


def test_backfill_earnings_handles_no_actuals(db_connection) -> None:
    """
    Future earnings record with None actuals stores NULL without crashing.
    """
    future_record = {
        "ticker": "AAPL",
        "earnings_date": "2025-07-31",
        "estimated_eps": 1.60,
        "actual_eps": None,
        "eps_surprise": None,
        "fiscal_quarter": None,
        "fiscal_year": None,
        "revenue_estimated": None,
        "revenue_actual": None,
    }

    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=[future_record]):
        count = backfill_earnings_for_ticker(db_connection, "AAPL")

    assert count == 1
    row = db_connection.execute(
        "SELECT actual_eps, revenue_actual FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()
    assert row["actual_eps"] is None
    assert row["revenue_actual"] is None


def test_backfill_earnings_handles_fetch_error(db_connection) -> None:
    """
    When fetch_earnings_dates raises an exception, the exception propagates
    so that backfill_all_earnings can count it as a failure.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               side_effect=RuntimeError("yfinance timeout")):
        with pytest.raises(RuntimeError):
            backfill_earnings_for_ticker(db_connection, "AAPL")


def test_backfill_earnings_is_idempotent(
    db_connection, sample_yfinance_records
) -> None:
    """
    Running backfill_earnings_for_ticker twice with same data yields no duplicate rows.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        backfill_earnings_for_ticker(db_connection, "AAPL")
        backfill_earnings_for_ticker(db_connection, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_earnings_sets_fetched_at(
    db_connection, sample_yfinance_records
) -> None:
    """
    Every inserted row has a non-null fetched_at timestamp.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        backfill_earnings_for_ticker(db_connection, "AAPL")

    rows = db_connection.execute(
        "SELECT fetched_at FROM earnings_calendar WHERE ticker='AAPL'"
    ).fetchall()
    assert all(row["fetched_at"] is not None for row in rows)


def test_backfill_earnings_returns_zero_for_empty_result(db_connection) -> None:
    """
    When fetch_earnings_dates returns an empty list, backfill returns 0.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates", return_value=[]):
        result = backfill_earnings_for_ticker(db_connection, "AAPL")

    assert result == 0


# ---------------------------------------------------------------------------
# Tests for backfill_all_earnings
# ---------------------------------------------------------------------------

def test_backfill_all_earnings_processes_each_ticker(
    db_connection, sample_tickers_list
) -> None:
    """
    With 3 tickers, fetch_earnings_dates is called exactly 3 times.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=[]) as mock_fetch:
        backfill_all_earnings(db_connection, sample_tickers_list)

    assert mock_fetch.call_count == 3


def test_backfill_all_earnings_returns_summary(
    db_connection, sample_tickers_list, sample_yfinance_records
) -> None:
    """
    Return dict contains processed, failed, and total_rows keys.
    """
    with patch("src.backfiller.earnings.fetch_earnings_dates",
               return_value=sample_yfinance_records):
        result = backfill_all_earnings(db_connection, sample_tickers_list)

    assert "processed" in result
    assert "failed" in result
    assert "total_rows" in result
    assert result["processed"] == 3
    assert result["failed"] == 0
    assert result["total_rows"] == 9  # 3 tickers × 3 records each


def test_backfill_all_earnings_continues_after_ticker_error(
    db_connection, sample_tickers_list
) -> None:
    """
    A failure on one ticker does not abort the run; other tickers are still processed.
    """
    call_count = 0

    def fetch_side_effect(ticker: str) -> list:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("network error")
        return []

    with patch("src.backfiller.earnings.fetch_earnings_dates",
               side_effect=fetch_side_effect):
        result = backfill_all_earnings(db_connection, sample_tickers_list)

    assert result["failed"] == 1
    assert result["processed"] == 2

