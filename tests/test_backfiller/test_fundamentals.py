"""Tests for src/backfiller/fundamentals.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.backfiller.fundamentals import (
    backfill_all_fundamentals,
    backfill_fundamentals_for_ticker,
    compute_yoy_growth,
    convert_yfinance_to_fundamentals_row,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def four_quarterly_records() -> list[dict]:
    """
    Return 4 quarterly fundamental records for AAPL spanning Q1 2024–Q4 2024.

    Each record contains the full set of fields returned by a mocked
    fetch_fundamentals_history call.
    """
    return [
        {
            "report_date": "2024-12-31",
            "period": "Q4",
            "revenue": 124300000000,
            "net_income": 36330000000,
            "eps": 2.40,
            "pe_ratio": 30.5,
            "pb_ratio": 55.0,
            "ps_ratio": 8.0,
            "debt_to_equity": 1.50,
            "return_on_assets": 0.25,
            "return_on_equity": 1.60,
            "free_cash_flow": 29000000000,
            "market_cap": 3200000000000,
            "dividend_yield": 0.005,
        },
        {
            "report_date": "2024-09-30",
            "period": "Q3",
            "revenue": 94930000000,
            "net_income": 14736000000,
            "eps": 0.97,
            "pe_ratio": 29.0,
            "pb_ratio": 54.0,
            "ps_ratio": 7.8,
            "debt_to_equity": 1.52,
            "return_on_assets": 0.22,
            "return_on_equity": 1.55,
            "free_cash_flow": 26000000000,
            "market_cap": 3100000000000,
            "dividend_yield": 0.005,
        },
        {
            "report_date": "2024-06-30",
            "period": "Q2",
            "revenue": 85777000000,
            "net_income": 21448000000,
            "eps": 1.40,
            "pe_ratio": 28.5,
            "pb_ratio": 52.16,
            "ps_ratio": 7.5,
            "debt_to_equity": 1.52,
            "return_on_assets": 0.23,
            "return_on_equity": 1.53,
            "free_cash_flow": 27000000000,
            "market_cap": 3000000000000,
            "dividend_yield": 0.005,
        },
        {
            "report_date": "2024-03-31",
            "period": "Q1",
            "revenue": 94036000000,
            "net_income": 23636000000,
            "eps": 1.57,
            "pe_ratio": 28.5,
            "pb_ratio": 52.16,
            "ps_ratio": 7.5,
            "debt_to_equity": 1.52,
            "return_on_assets": 0.23,
            "return_on_equity": 1.53,
            "free_cash_flow": 27000000000,
            "market_cap": 3000000000000,
            "dividend_yield": 0.005,
        },
    ]


# ---------------------------------------------------------------------------
# Tests for compute_yoy_growth
# ---------------------------------------------------------------------------

def test_compute_yoy_growth_positive() -> None:
    """Growth is positive when current > prior: (110 - 100) / 100 = 0.10."""
    result = compute_yoy_growth(110.0, 100.0)
    assert result == pytest.approx(0.10)


def test_compute_yoy_growth_negative() -> None:
    """Growth is negative when current < prior: (90 - 100) / 100 = -0.10."""
    result = compute_yoy_growth(90.0, 100.0)
    assert result == pytest.approx(-0.10)


def test_compute_yoy_growth_returns_none_when_prior_is_none() -> None:
    """Returns None when prior_value is None."""
    assert compute_yoy_growth(100.0, None) is None


def test_compute_yoy_growth_returns_none_when_current_is_none() -> None:
    """Returns None when current_value is None."""
    assert compute_yoy_growth(None, 100.0) is None


def test_compute_yoy_growth_returns_none_when_prior_is_zero() -> None:
    """Returns None when prior_value is 0 to avoid division by zero."""
    assert compute_yoy_growth(100.0, 0.0) is None


# ---------------------------------------------------------------------------
# Tests for convert_yfinance_to_fundamentals_row
# ---------------------------------------------------------------------------

def test_convert_yfinance_maps_all_fields() -> None:
    """All financial fields are mapped correctly from the source record."""
    record = {
        "report_date": "2024-03-31",
        "period": "Q1",
        "revenue": 94036000000,
        "net_income": 23636000000,
        "eps": 1.57,
        "pe_ratio": 28.5,
        "pb_ratio": 52.16,
        "ps_ratio": 7.5,
        "debt_to_equity": 1.52,
        "return_on_assets": 0.23,
        "return_on_equity": 1.53,
        "free_cash_flow": 27000000000,
        "market_cap": 3000000000000,
        "dividend_yield": 0.005,
    }
    row = convert_yfinance_to_fundamentals_row("AAPL", record)

    assert row["ticker"] == "AAPL"
    assert row["report_date"] == "2024-03-31"
    assert row["period"] == "Q1"
    assert row["revenue"] == pytest.approx(94036000000)
    assert row["net_income"] == pytest.approx(23636000000)
    assert row["eps"] == pytest.approx(1.57)
    assert row["pe_ratio"] == pytest.approx(28.5)
    assert row["pb_ratio"] == pytest.approx(52.16)
    assert row["ps_ratio"] == pytest.approx(7.5)
    assert row["debt_to_equity"] == pytest.approx(1.52)
    assert row["return_on_assets"] == pytest.approx(0.23)
    assert row["return_on_equity"] == pytest.approx(1.53)
    assert row["free_cash_flow"] == pytest.approx(27000000000)
    assert row["market_cap"] == pytest.approx(3000000000000)
    assert row["dividend_yield"] == pytest.approx(0.005)
    assert row["fetched_at"] is not None


def test_convert_yfinance_computes_revenue_growth_with_prior() -> None:
    """revenue_growth_yoy is computed correctly when a prior record is supplied."""
    record = {"report_date": "2025-03-31", "period": "Q1", "revenue": 94036000000, "eps": None, "net_income": None}
    prior = {"report_date": "2024-03-31", "period": "Q1", "revenue": 80000000000, "eps": None, "net_income": None}

    row = convert_yfinance_to_fundamentals_row("AAPL", record, prior_record=prior)
    expected_growth = (94036000000 - 80000000000) / 80000000000
    assert row["revenue_growth_yoy"] == pytest.approx(expected_growth)


def test_convert_yfinance_computes_eps_growth_with_prior() -> None:
    """eps_growth_yoy is computed correctly when a prior record is supplied."""
    record = {"report_date": "2025-03-31", "period": "Q1", "revenue": None, "eps": 1.57, "net_income": None}
    prior = {"report_date": "2024-03-31", "period": "Q1", "revenue": None, "eps": 1.40, "net_income": None}

    row = convert_yfinance_to_fundamentals_row("AAPL", record, prior_record=prior)
    expected_growth = (1.57 - 1.40) / 1.40
    assert row["eps_growth_yoy"] == pytest.approx(expected_growth)


def test_convert_yfinance_growth_is_none_without_prior() -> None:
    """revenue_growth_yoy and eps_growth_yoy are None when no prior record is given."""
    record = {"report_date": "2025-03-31", "period": "Q1", "revenue": 100.0, "eps": 1.0, "net_income": None}
    row = convert_yfinance_to_fundamentals_row("AAPL", record)

    assert row["revenue_growth_yoy"] is None
    assert row["eps_growth_yoy"] is None


# ---------------------------------------------------------------------------
# Tests for backfill_fundamentals_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_fundamentals_stores_quarterly_data(
    db_connection, four_quarterly_records
) -> None:
    """
    Mock fetch_fundamentals_history returning 4 quarterly records.
    Verify 4 rows in fundamentals table with period values like Q1, Q2, etc.
    """
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = four_quarterly_records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    rows = db_connection.execute(
        "SELECT * FROM fundamentals WHERE ticker='AAPL'"
    ).fetchall()
    assert len(rows) == 4

    periods = {row["period"] for row in rows}
    assert "Q1" in periods
    assert "Q2" in periods
    assert "Q3" in periods
    assert "Q4" in periods


def test_backfill_fundamentals_stores_revenue(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning a record with revenue=94036000000.
    Verify the row in DB has revenue=94036000000.
    """
    records = [
        {
            "report_date": "2024-03-31",
            "period": "Q1",
            "revenue": 94036000000,
            "net_income": None,
            "eps": None,
        }
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    row = db_connection.execute(
        "SELECT revenue FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()
    assert row["revenue"] == pytest.approx(94036000000)


def test_backfill_fundamentals_stores_eps(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning a record with eps=1.57.
    Verify eps=1.57 stored in DB.
    """
    records = [
        {"report_date": "2024-03-31", "period": "Q1", "revenue": None, "net_income": None, "eps": 1.57}
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    row = db_connection.execute(
        "SELECT eps FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()
    assert row["eps"] == pytest.approx(1.57)


def test_backfill_fundamentals_stores_ratios(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning ratio fields.
    Verify pe_ratio, pb_ratio, debt_to_equity, return_on_equity are all stored.
    """
    records = [
        {
            "report_date": "2024-03-31",
            "period": "Q1",
            "revenue": None,
            "net_income": None,
            "eps": None,
            "pe_ratio": 28.5,
            "pb_ratio": 52.16,
            "debt_to_equity": 1.52,
            "return_on_equity": 1.53,
        }
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    row = db_connection.execute(
        "SELECT pe_ratio, pb_ratio, debt_to_equity, return_on_equity "
        "FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()
    assert row["pe_ratio"] == pytest.approx(28.5)
    assert row["pb_ratio"] == pytest.approx(52.16)
    assert row["debt_to_equity"] == pytest.approx(1.52)
    assert row["return_on_equity"] == pytest.approx(1.53)


def test_backfill_fundamentals_computes_revenue_growth_yoy(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning 4 quarters across 2 years.
    Verify revenue_growth_yoy for Q1 2025 vs Q1 2024 is computed correctly.
    """
    records = [
        {"report_date": "2025-03-31", "period": "Q1", "revenue": 94036000000, "net_income": None, "eps": None},
        {"report_date": "2024-12-31", "period": "Q4", "revenue": 90000000000, "net_income": None, "eps": None},
        {"report_date": "2024-06-30", "period": "Q2", "revenue": 85000000000, "net_income": None, "eps": None},
        {"report_date": "2024-03-31", "period": "Q1", "revenue": 80000000000, "net_income": None, "eps": None},
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=2)

    row = db_connection.execute(
        "SELECT revenue_growth_yoy FROM fundamentals "
        "WHERE ticker='AAPL' AND report_date='2025-03-31'"
    ).fetchone()

    expected = (94036000000 - 80000000000) / 80000000000
    assert row["revenue_growth_yoy"] == pytest.approx(expected)


def test_backfill_fundamentals_computes_eps_growth_yoy(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning 4 quarters across 2 years.
    Verify eps_growth_yoy for Q1 2025 vs Q1 2024 is computed correctly.
    """
    records = [
        {"report_date": "2025-03-31", "period": "Q1", "revenue": None, "net_income": None, "eps": 1.57},
        {"report_date": "2024-12-31", "period": "Q4", "revenue": None, "net_income": None, "eps": 2.40},
        {"report_date": "2024-06-30", "period": "Q2", "revenue": None, "net_income": None, "eps": 1.40},
        {"report_date": "2024-03-31", "period": "Q1", "revenue": None, "net_income": None, "eps": 1.40},
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=2)

    row = db_connection.execute(
        "SELECT eps_growth_yoy FROM fundamentals "
        "WHERE ticker='AAPL' AND report_date='2025-03-31'"
    ).fetchone()

    expected = (1.57 - 1.40) / 1.40
    assert row["eps_growth_yoy"] == pytest.approx(expected)


def test_backfill_fundamentals_handles_missing_fields(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning a record where debt_to_equity is None.
    Verify debt_to_equity is stored as NULL in DB without crash.
    """
    records = [
        {
            "report_date": "2024-03-31",
            "period": "Q1",
            "revenue": 94036000000,
            "net_income": None,
            "eps": 1.57,
            "pe_ratio": None,
            "pb_ratio": None,
            "ps_ratio": None,
            "debt_to_equity": None,
            "return_on_assets": None,
            "return_on_equity": None,
            "free_cash_flow": None,
            "market_cap": None,
            "dividend_yield": None,
        }
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        count = backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    assert count == 1
    row = db_connection.execute(
        "SELECT debt_to_equity FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()
    assert row["debt_to_equity"] is None


def test_backfill_fundamentals_handles_yfinance_error(db_connection) -> None:
    """
    Mock fetch_fundamentals_history to return empty list.
    Verify no crash, warning alert logged, function returns 0.
    """
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = []
        result = backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    assert result == 0

    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert row_count == 0

    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='AAPL' AND severity='warning'"
    ).fetchone()[0]
    assert alert_count >= 1


def test_backfill_fundamentals_handles_exception(db_connection) -> None:
    """
    Mock fetch_fundamentals_history to raise an exception.
    Verify no crash and function returns 0.
    """
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.side_effect = RuntimeError("yfinance connection error")
        result = backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    assert result == 0


def test_backfill_fundamentals_is_idempotent(db_connection) -> None:
    """
    Running backfill_fundamentals_for_ticker twice with same data yields no duplicates.
    UNIQUE constraint on (ticker, report_date, period) is enforced.
    """
    records = [
        {"report_date": "2024-03-31", "period": "Q1", "revenue": 94036000000, "net_income": None, "eps": 1.57}
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    count = db_connection.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 1


def test_backfill_fundamentals_sets_fetched_at(db_connection) -> None:
    """
    Every inserted row has a non-null fetched_at UTC timestamp.
    """
    records = [
        {"report_date": "2024-03-31", "period": "Q1", "revenue": None, "net_income": None, "eps": None}
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=1)

    row = db_connection.execute(
        "SELECT fetched_at FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()
    assert row["fetched_at"] is not None
    # Should be a valid ISO 8601 timestamp
    assert "T" in row["fetched_at"] or len(row["fetched_at"]) >= 10


def test_backfill_fundamentals_returns_row_count(db_connection) -> None:
    """
    Mock fetch_fundamentals_history returning 8 records. Function returns 8.
    """
    records = [
        {"report_date": f"202{4 - i // 4}-{3 * (i % 4 + 1):02d}-31", "period": f"Q{i % 4 + 1}",
         "revenue": None, "net_income": None, "eps": None}
        for i in range(8)
    ]
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = records
        result = backfill_fundamentals_for_ticker(db_connection, "AAPL", lookback_years=2)

    assert result == 8


# ---------------------------------------------------------------------------
# Tests for backfill_all_fundamentals
# ---------------------------------------------------------------------------

def test_backfill_all_fundamentals_processes_each_ticker(
    db_connection, sample_tickers_list
) -> None:
    """
    With 3 tickers, fetch_fundamentals_history is called exactly 3 times.
    """
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.return_value = []
        backfill_all_fundamentals(db_connection, sample_tickers_list, config={})

    assert mock_fetch.call_count == 3


def test_backfill_all_fundamentals_continues_on_error(db_connection) -> None:
    """
    When ticker 2 raises an exception, tickers 1 and 3 are still processed.
    Ticker 2 is skipped with an alert logged.
    """
    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]
    aapl_records = [
        {"report_date": "2024-03-31", "period": "Q1", "revenue": 94036000000, "net_income": None, "eps": None}
    ]
    jpm_records = [
        {"report_date": "2024-03-31", "period": "Q1", "revenue": 50000000000, "net_income": None, "eps": None}
    ]

    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        mock_fetch.side_effect = [
            aapl_records,
            RuntimeError("MSFT yfinance unavailable"),
            jpm_records,
        ]
        backfill_all_fundamentals(db_connection, tickers, config={})

    aapl_count = db_connection.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE ticker='AAPL'"
    ).fetchone()[0]
    jpm_count = db_connection.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE ticker='JPM'"
    ).fetchone()[0]
    msft_count = db_connection.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE ticker='MSFT'"
    ).fetchone()[0]

    assert aapl_count == 1
    assert jpm_count == 1
    assert msft_count == 0

    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='MSFT'"
    ).fetchone()[0]
    assert alert_count >= 1


def test_backfill_all_fundamentals_uses_progress_tracker(
    db_connection, sample_tickers_list
) -> None:
    """
    When bot_token and chat_id are provided, Telegram progress messages are sent.
    """
    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch, \
         patch("src.backfiller.fundamentals.send_telegram_message") as mock_send, \
         patch("src.backfiller.fundamentals.edit_telegram_message") as mock_edit:
        mock_fetch.return_value = []
        mock_send.return_value = 42

        backfill_all_fundamentals(
            db_connection,
            sample_tickers_list,
            config={},
            bot_token="test_token",
            chat_id="test_chat_id",
        )

    assert mock_send.call_count >= 1
    assert mock_edit.call_count >= 3


# ---------------------------------------------------------------------------
# Staleness / skip-if-fresh tests
# ---------------------------------------------------------------------------

def _insert_fresh_fundamentals(db_connection, ticker: str) -> None:
    """Insert a recent fundamentals row to simulate fresh data."""
    from datetime import datetime, timedelta, timezone
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=5)).isoformat()
    db_connection.execute(
        """
        INSERT INTO fundamentals (ticker, report_date, period, fetched_at)
        VALUES (?, ?, ?, ?)
        """,
        (ticker, "2024-12-31", "Q4", fetched_at),
    )
    db_connection.commit()


def test_backfill_fundamentals_skips_when_fresh(
    db_connection,
) -> None:
    """
    When data was fetched less than 30 days ago, fetch_fundamentals_history is NOT called.
    """
    _insert_fresh_fundamentals(db_connection, "AAPL")
    config = {"skip_if_fresh_days": {"fundamentals": 30}}

    with patch("src.backfiller.fundamentals.fetch_fundamentals_history") as mock_fetch:
        result = backfill_fundamentals_for_ticker(
            db_connection, "AAPL", 5, config=config
        )

    mock_fetch.assert_not_called()
    assert result == 0


def test_backfill_fundamentals_fetches_when_stale(
    db_connection, four_quarterly_records
) -> None:
    """
    When data is older than the threshold, fetch_fundamentals_history IS called.
    """
    from datetime import datetime, timedelta, timezone
    stale_fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=45)).isoformat()
    db_connection.execute(
        """
        INSERT INTO fundamentals (ticker, report_date, period, fetched_at)
        VALUES (?, ?, ?, ?)
        """,
        ("AAPL", "2024-12-31", "Q4", stale_fetched_at),
    )
    db_connection.commit()
    config = {"skip_if_fresh_days": {"fundamentals": 30}}

    with patch("src.backfiller.fundamentals.fetch_fundamentals_history",
               return_value=four_quarterly_records):
        result = backfill_fundamentals_for_ticker(
            db_connection, "AAPL", 5, config=config
        )

    assert result == len(four_quarterly_records)


def test_backfill_fundamentals_fetches_when_no_data(
    db_connection, four_quarterly_records
) -> None:
    """
    When no data exists, fetch_fundamentals_history IS called.
    """
    config = {"skip_if_fresh_days": {"fundamentals": 30}}

    with patch("src.backfiller.fundamentals.fetch_fundamentals_history",
               return_value=four_quarterly_records):
        result = backfill_fundamentals_for_ticker(
            db_connection, "AAPL", 5, config=config
        )

    assert result == len(four_quarterly_records)


def test_backfill_fundamentals_force_bypasses_staleness(
    db_connection, four_quarterly_records
) -> None:
    """
    When force=True, fetch_fundamentals_history is called even when data is fresh.
    """
    _insert_fresh_fundamentals(db_connection, "AAPL")
    config = {"skip_if_fresh_days": {"fundamentals": 30}}

    with patch("src.backfiller.fundamentals.fetch_fundamentals_history",
               return_value=four_quarterly_records):
        result = backfill_fundamentals_for_ticker(
            db_connection, "AAPL", 5, config=config, force=True
        )

    assert result == len(four_quarterly_records)
