"""Tests for src/backfiller/corporate_actions.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from unittest.mock import MagicMock, call, patch

import pytest

from src.backfiller.corporate_actions import (
    backfill_all_corporate_actions,
    backfill_dividends_for_ticker,
    backfill_short_interest_for_ticker,
    backfill_splits_for_ticker,
    convert_polygon_dividend_to_row,
    convert_polygon_short_interest_to_row,
    convert_polygon_split_to_row,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_dividend_records() -> list[dict]:
    """Return 3 Polygon dividend records for AAPL."""
    return [
        {"id": "Eabc001", "ticker": "AAPL", "ex_dividend_date": "2025-08-11",
         "pay_date": "2025-08-14", "cash_amount": 0.26, "frequency": 4},
        {"id": "Eabc002", "ticker": "AAPL", "ex_dividend_date": "2025-05-12",
         "pay_date": "2025-05-15", "cash_amount": 0.26, "frequency": 4},
        {"id": "Eabc003", "ticker": "AAPL", "ex_dividend_date": "2025-02-10",
         "pay_date": "2025-02-13", "cash_amount": 0.25, "frequency": 4},
    ]


@pytest.fixture
def sample_split_records() -> list[dict]:
    """Return 2 Polygon stock split records for AAPL."""
    return [
        {"id": "Exyz789", "ticker": "AAPL", "execution_date": "2020-08-31",
         "split_from": 1, "split_to": 4},
        {"id": "Exyz456", "ticker": "AAPL", "execution_date": "2014-06-09",
         "split_from": 1, "split_to": 7},
    ]


@pytest.fixture
def sample_short_interest_records() -> list[dict]:
    """Return 5 Polygon short interest records for AAPL."""
    return [
        {"ticker": "AAPL", "settlement_date": "2025-03-14",
         "short_interest": 3906231, "avg_daily_volume": 2340158, "days_to_cover": 1.67},
        {"ticker": "AAPL", "settlement_date": "2025-02-28",
         "short_interest": 4000000, "avg_daily_volume": 2200000, "days_to_cover": 1.82},
        {"ticker": "AAPL", "settlement_date": "2025-02-14",
         "short_interest": 3800000, "avg_daily_volume": 2300000, "days_to_cover": 1.65},
        {"ticker": "AAPL", "settlement_date": "2025-01-31",
         "short_interest": 4100000, "avg_daily_volume": 2400000, "days_to_cover": 1.71},
        {"ticker": "AAPL", "settlement_date": "2025-01-15",
         "short_interest": 3700000, "avg_daily_volume": 2100000, "days_to_cover": 1.76},
    ]


@pytest.fixture
def mock_polygon_client(
    sample_dividend_records, sample_split_records, sample_short_interest_records
) -> MagicMock:
    """Return a MagicMock PolygonClient pre-configured for all 3 data types."""
    client = MagicMock()
    client.fetch_dividends.return_value = sample_dividend_records
    client.fetch_splits.return_value = sample_split_records
    client.fetch_short_interest.return_value = sample_short_interest_records
    return client


# ---------------------------------------------------------------------------
# Tests for convert_polygon_dividend_to_row
# ---------------------------------------------------------------------------

def test_convert_polygon_dividend_maps_fields() -> None:
    """All Polygon dividend fields are mapped correctly to the DB schema."""
    record = {
        "id": "Eabc123", "ticker": "AAPL", "ex_dividend_date": "2025-08-11",
        "pay_date": "2025-08-14", "cash_amount": 0.26, "frequency": 4,
    }
    row = convert_polygon_dividend_to_row(record)

    assert row["id"] == "Eabc123"
    assert row["ticker"] == "AAPL"
    assert row["ex_dividend_date"] == "2025-08-11"
    assert row["pay_date"] == "2025-08-14"
    assert row["cash_amount"] == pytest.approx(0.26)
    assert row["frequency"] == 4
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for convert_polygon_split_to_row
# ---------------------------------------------------------------------------

def test_convert_polygon_split_maps_fields() -> None:
    """All Polygon split fields are mapped correctly to the DB schema."""
    record = {
        "id": "Exyz789", "ticker": "AAPL", "execution_date": "2020-08-31",
        "split_from": 1, "split_to": 4,
    }
    row = convert_polygon_split_to_row(record)

    assert row["id"] == "Exyz789"
    assert row["ticker"] == "AAPL"
    assert row["execution_date"] == "2020-08-31"
    assert row["split_from"] == 1
    assert row["split_to"] == 4
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for convert_polygon_short_interest_to_row
# ---------------------------------------------------------------------------

def test_convert_polygon_short_interest_maps_fields() -> None:
    """All Polygon short interest fields are mapped correctly to the DB schema."""
    record = {
        "ticker": "AAPL", "settlement_date": "2025-03-14",
        "short_interest": 3906231, "avg_daily_volume": 2340158, "days_to_cover": 1.67,
    }
    row = convert_polygon_short_interest_to_row(record)

    assert row["ticker"] == "AAPL"
    assert row["settlement_date"] == "2025-03-14"
    assert row["short_interest"] == 3906231
    assert row["avg_daily_volume"] == 2340158
    assert row["days_to_cover"] == pytest.approx(1.67)
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for backfill_dividends_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_dividends_stores_records(
    db_connection, mock_polygon_client
) -> None:
    """Mock PolygonClient returning 3 dividend records. Verify 3 rows in dividends table."""
    count = backfill_dividends_for_ticker(db_connection, mock_polygon_client, "AAPL")

    assert count == 3
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM dividends WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert row_count == 3


def test_backfill_dividends_maps_polygon_fields(db_connection) -> None:
    """Polygon dividend fields are correctly stored in the dividends table."""
    mock_client = MagicMock()
    mock_client.fetch_dividends.return_value = [
        {"id": "Eabc123", "ticker": "AAPL", "ex_dividend_date": "2025-08-11",
         "pay_date": "2025-08-14", "cash_amount": 0.26, "frequency": 4}
    ]

    backfill_dividends_for_ticker(db_connection, mock_client, "AAPL")

    row = db_connection.execute(
        "SELECT * FROM dividends WHERE id='Eabc123'"
    ).fetchone()
    assert row["ticker"] == "AAPL"
    assert row["ex_dividend_date"] == "2025-08-11"
    assert row["pay_date"] == "2025-08-14"
    assert row["cash_amount"] == pytest.approx(0.26)
    assert row["frequency"] == 4


def test_backfill_dividends_is_idempotent(
    db_connection, mock_polygon_client
) -> None:
    """Running twice with same data yields no duplicate rows (id is PRIMARY KEY)."""
    backfill_dividends_for_ticker(db_connection, mock_polygon_client, "AAPL")
    backfill_dividends_for_ticker(db_connection, mock_polygon_client, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM dividends WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_dividends_handles_api_error(db_connection) -> None:
    """When fetch_dividends returns empty list, no crash occurs and alert is logged."""
    mock_client = MagicMock()
    mock_client.fetch_dividends.return_value = []

    result = backfill_dividends_for_ticker(db_connection, mock_client, "AAPL")

    assert result == 0


def test_backfill_dividends_sets_fetched_at(
    db_connection, mock_polygon_client
) -> None:
    """Every inserted dividend row has a non-null fetched_at timestamp."""
    backfill_dividends_for_ticker(db_connection, mock_polygon_client, "AAPL")

    rows = db_connection.execute(
        "SELECT fetched_at FROM dividends WHERE ticker='AAPL'"
    ).fetchall()
    assert all(row["fetched_at"] is not None for row in rows)


# ---------------------------------------------------------------------------
# Tests for backfill_splits_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_splits_stores_records(
    db_connection, mock_polygon_client
) -> None:
    """Mock PolygonClient returning 2 split records. Verify 2 rows in splits table."""
    count = backfill_splits_for_ticker(db_connection, mock_polygon_client, "AAPL")

    assert count == 2
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM splits WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert row_count == 2


def test_backfill_splits_maps_polygon_fields(db_connection) -> None:
    """Polygon split fields are correctly stored in the splits table."""
    mock_client = MagicMock()
    mock_client.fetch_splits.return_value = [
        {"id": "Exyz789", "ticker": "AAPL", "execution_date": "2020-08-31",
         "split_from": 1, "split_to": 4}
    ]

    backfill_splits_for_ticker(db_connection, mock_client, "AAPL")

    row = db_connection.execute(
        "SELECT * FROM splits WHERE id='Exyz789'"
    ).fetchone()
    assert row["ticker"] == "AAPL"
    assert row["execution_date"] == "2020-08-31"
    assert row["split_from"] == 1
    assert row["split_to"] == 4


def test_backfill_splits_is_idempotent(
    db_connection, mock_polygon_client
) -> None:
    """Running splits backfill twice with same data yields no duplicate rows."""
    backfill_splits_for_ticker(db_connection, mock_polygon_client, "AAPL")
    backfill_splits_for_ticker(db_connection, mock_polygon_client, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM splits WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 2


# ---------------------------------------------------------------------------
# Tests for backfill_short_interest_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_short_interest_stores_records(
    db_connection, mock_polygon_client
) -> None:
    """Mock PolygonClient returning 5 short interest records. Verify 5 rows."""
    count = backfill_short_interest_for_ticker(db_connection, mock_polygon_client, "AAPL")

    assert count == 5
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM short_interest WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert row_count == 5


def test_backfill_short_interest_maps_polygon_fields(db_connection) -> None:
    """Polygon short interest fields are correctly stored including days_to_cover."""
    mock_client = MagicMock()
    mock_client.fetch_short_interest.return_value = [
        {"ticker": "AAPL", "settlement_date": "2025-03-14",
         "short_interest": 3906231, "avg_daily_volume": 2340158, "days_to_cover": 1.67}
    ]

    backfill_short_interest_for_ticker(db_connection, mock_client, "AAPL")

    row = db_connection.execute(
        "SELECT * FROM short_interest WHERE ticker='AAPL' AND settlement_date='2025-03-14'"
    ).fetchone()
    assert row["short_interest"] == 3906231
    assert row["avg_daily_volume"] == 2340158
    assert row["days_to_cover"] == pytest.approx(1.67)


def test_backfill_short_interest_is_idempotent(
    db_connection, mock_polygon_client
) -> None:
    """Running short interest backfill twice yields no duplicates (UNIQUE on ticker, settlement_date)."""
    backfill_short_interest_for_ticker(db_connection, mock_polygon_client, "AAPL")
    backfill_short_interest_for_ticker(db_connection, mock_polygon_client, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM short_interest WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 5


def test_backfill_short_interest_handles_api_error(db_connection) -> None:
    """When fetch_short_interest returns empty list, no crash occurs and returns 0."""
    mock_client = MagicMock()
    mock_client.fetch_short_interest.return_value = []

    result = backfill_short_interest_for_ticker(db_connection, mock_client, "AAPL")

    assert result == 0


# ---------------------------------------------------------------------------
# Tests for backfill_all_corporate_actions
# ---------------------------------------------------------------------------

def test_backfill_all_corporate_actions_processes_each_ticker(
    db_connection, sample_tickers_list, mock_polygon_client
) -> None:
    """
    With 3 tickers, dividends, splits, and short interest are all fetched for each.
    Each fetch method is called exactly 3 times (once per ticker).
    """
    backfill_all_corporate_actions(
        db_connection, mock_polygon_client, sample_tickers_list
    )

    assert mock_polygon_client.fetch_dividends.call_count == 3
    assert mock_polygon_client.fetch_splits.call_count == 3
    assert mock_polygon_client.fetch_short_interest.call_count == 3


def test_backfill_all_corporate_actions_continues_on_error(
    db_connection, sample_split_records, sample_short_interest_records
) -> None:
    """
    If dividends fetch fails for ticker 2, splits and short interest still run for it.
    All 3 data types still run for the other tickers.
    """
    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]

    # Dividends: fail for MSFT, succeed for AAPL and JPM
    aapl_dividends = [{"id": "d001", "ticker": "AAPL", "ex_dividend_date": "2025-08-11",
                       "pay_date": "2025-08-14", "cash_amount": 0.26, "frequency": 4}]
    jpm_dividends = [{"id": "d002", "ticker": "JPM", "ex_dividend_date": "2025-07-01",
                      "pay_date": "2025-07-10", "cash_amount": 1.25, "frequency": 4}]

    # Splits: unique IDs per ticker to avoid conflict
    aapl_splits = [{"id": "s001", "ticker": "AAPL", "execution_date": "2020-08-31",
                    "split_from": 1, "split_to": 4}]
    msft_splits = [{"id": "s002", "ticker": "MSFT", "execution_date": "2003-02-18",
                    "split_from": 2, "split_to": 1}]
    jpm_splits = []

    # Short interest: vary by ticker
    aapl_si = [{"ticker": "AAPL", "settlement_date": "2025-03-14",
                "short_interest": 3906231, "avg_daily_volume": 2340158, "days_to_cover": 1.67}]
    msft_si = [{"ticker": "MSFT", "settlement_date": "2025-03-14",
                "short_interest": 5000000, "avg_daily_volume": 3000000, "days_to_cover": 1.67}]
    jpm_si = [{"ticker": "JPM", "settlement_date": "2025-03-14",
               "short_interest": 2000000, "avg_daily_volume": 1500000, "days_to_cover": 1.33}]

    mock_client = MagicMock()
    mock_client.fetch_dividends.side_effect = [
        aapl_dividends,
        RuntimeError("Dividends API error for MSFT"),
        jpm_dividends,
    ]
    mock_client.fetch_splits.side_effect = [aapl_splits, msft_splits, jpm_splits]
    mock_client.fetch_short_interest.side_effect = [aapl_si, msft_si, jpm_si]

    backfill_all_corporate_actions(db_connection, mock_client, tickers)

    # Dividends: AAPL and JPM should be stored, MSFT should not
    aapl_div_count = db_connection.execute(
        "SELECT COUNT(*) FROM dividends WHERE ticker='AAPL'"
    ).fetchone()[0]
    jpm_div_count = db_connection.execute(
        "SELECT COUNT(*) FROM dividends WHERE ticker='JPM'"
    ).fetchone()[0]
    msft_div_count = db_connection.execute(
        "SELECT COUNT(*) FROM dividends WHERE ticker='MSFT'"
    ).fetchone()[0]
    assert aapl_div_count == 1
    assert jpm_div_count == 1
    assert msft_div_count == 0

    # Splits and short interest: all 3 tickers should have data
    msft_split_count = db_connection.execute(
        "SELECT COUNT(*) FROM splits WHERE ticker='MSFT'"
    ).fetchone()[0]
    msft_si_count = db_connection.execute(
        "SELECT COUNT(*) FROM short_interest WHERE ticker='MSFT'"
    ).fetchone()[0]
    assert msft_split_count == 1
    assert msft_si_count == 1


def test_backfill_all_corporate_actions_uses_progress_tracker(
    db_connection, sample_tickers_list, mock_polygon_client
) -> None:
    """
    When bot_token and chat_id are provided, Telegram progress messages are sent.
    """
    with patch("src.backfiller.corporate_actions.send_telegram_message") as mock_send, \
         patch("src.backfiller.corporate_actions.edit_telegram_message") as mock_edit:
        mock_send.return_value = 42

        backfill_all_corporate_actions(
            db_connection,
            mock_polygon_client,
            sample_tickers_list,
            bot_token="test_token",
            chat_id="test_chat_id",
        )

    assert mock_send.call_count >= 1
    assert mock_edit.call_count >= 3


def test_backfill_all_corporate_actions_returns_summary(
    db_connection, sample_tickers_list, mock_polygon_client
) -> None:
    """
    Return dict includes dividends_total, splits_total, short_interest_total,
    tickers_processed, and tickers_failed.
    """
    result = backfill_all_corporate_actions(
        db_connection, mock_polygon_client, sample_tickers_list
    )

    assert "dividends_total" in result
    assert "splits_total" in result
    assert "short_interest_total" in result
    assert "tickers_processed" in result
    assert "tickers_failed" in result
    assert result["tickers_processed"] == 3
    assert result["tickers_failed"] == 0
    assert result["dividends_total"] == 9   # 3 records × 3 tickers
    assert result["splits_total"] == 6      # 2 records × 3 tickers
    assert result["short_interest_total"] == 15  # 5 records × 3 tickers


# ---------------------------------------------------------------------------
# Staleness / skip-if-fresh tests
# ---------------------------------------------------------------------------

def _insert_fresh_row(db_connection, table: str, ticker: str, pk_col: str = None, pk_val: str = None, date_col: str = None, date_val: str = None) -> None:
    """Helper to insert a recent row into a corporate-actions table."""
    from datetime import datetime, timedelta, timezone
    fetched_at = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    if table == "dividends":
        db_connection.execute(
            "INSERT INTO dividends (id, ticker, ex_dividend_date, fetched_at) VALUES (?, ?, ?, ?)",
            ("test-id-fresh", ticker, "2025-01-01", fetched_at),
        )
    elif table == "splits":
        db_connection.execute(
            "INSERT INTO splits (id, ticker, execution_date, fetched_at) VALUES (?, ?, ?, ?)",
            ("test-id-fresh", ticker, "2025-01-01", fetched_at),
        )
    elif table == "short_interest":
        db_connection.execute(
            "INSERT INTO short_interest (ticker, settlement_date, fetched_at) VALUES (?, ?, ?)",
            (ticker, "2025-01-01", fetched_at),
        )
    db_connection.commit()


def test_backfill_dividends_skips_when_fresh(
    db_connection, sample_dividend_records
) -> None:
    """When dividends data is fresh, fetch_dividends is NOT called."""
    _insert_fresh_row(db_connection, "dividends", "AAPL")
    mock_client = MagicMock()
    config = {"skip_if_fresh_days": {"dividends": 7}}

    result = backfill_dividends_for_ticker(db_connection, mock_client, "AAPL", config=config)

    mock_client.fetch_dividends.assert_not_called()
    assert result == 0


def test_backfill_dividends_fetches_when_stale(
    db_connection, sample_dividend_records
) -> None:
    """When dividends data is stale, fetch_dividends IS called."""
    from datetime import datetime, timedelta, timezone
    stale_fetched_at = (datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat()
    db_connection.execute(
        "INSERT INTO dividends (id, ticker, ex_dividend_date, fetched_at) VALUES (?, ?, ?, ?)",
        ("test-stale", "AAPL", "2025-01-01", stale_fetched_at),
    )
    db_connection.commit()
    mock_client = MagicMock()
    mock_client.fetch_dividends.return_value = sample_dividend_records
    config = {"skip_if_fresh_days": {"dividends": 7}}

    result = backfill_dividends_for_ticker(db_connection, mock_client, "AAPL", config=config)

    mock_client.fetch_dividends.assert_called_once()
    assert result == len(sample_dividend_records)


def test_backfill_dividends_force_bypasses_staleness(
    db_connection, sample_dividend_records
) -> None:
    """When force=True, fetch_dividends is called even when data is fresh."""
    _insert_fresh_row(db_connection, "dividends", "AAPL")
    mock_client = MagicMock()
    mock_client.fetch_dividends.return_value = sample_dividend_records
    config = {"skip_if_fresh_days": {"dividends": 7}}

    result = backfill_dividends_for_ticker(
        db_connection, mock_client, "AAPL", config=config, force=True
    )

    mock_client.fetch_dividends.assert_called_once()
    assert result == len(sample_dividend_records)


def test_backfill_splits_skips_when_fresh(
    db_connection, sample_split_records
) -> None:
    """When splits data is fresh, fetch_splits is NOT called."""
    _insert_fresh_row(db_connection, "splits", "AAPL")
    mock_client = MagicMock()
    config = {"skip_if_fresh_days": {"splits": 30}}

    result = backfill_splits_for_ticker(db_connection, mock_client, "AAPL", config=config)

    mock_client.fetch_splits.assert_not_called()
    assert result == 0


def test_backfill_splits_force_bypasses_staleness(
    db_connection, sample_split_records
) -> None:
    """When force=True, fetch_splits is called even when data is fresh."""
    _insert_fresh_row(db_connection, "splits", "AAPL")
    mock_client = MagicMock()
    mock_client.fetch_splits.return_value = sample_split_records
    config = {"skip_if_fresh_days": {"splits": 30}}

    result = backfill_splits_for_ticker(
        db_connection, mock_client, "AAPL", config=config, force=True
    )

    mock_client.fetch_splits.assert_called_once()
    assert result == len(sample_split_records)


def test_backfill_short_interest_skips_when_fresh(
    db_connection, sample_short_interest_records
) -> None:
    """When short_interest data is fresh, fetch_short_interest is NOT called."""
    _insert_fresh_row(db_connection, "short_interest", "AAPL")
    mock_client = MagicMock()
    config = {"skip_if_fresh_days": {"short_interest": 7}}

    result = backfill_short_interest_for_ticker(
        db_connection, mock_client, "AAPL", config=config
    )

    mock_client.fetch_short_interest.assert_not_called()
    assert result == 0


def test_backfill_short_interest_force_bypasses_staleness(
    db_connection, sample_short_interest_records
) -> None:
    """When force=True, fetch_short_interest is called even when data is fresh."""
    _insert_fresh_row(db_connection, "short_interest", "AAPL")
    mock_client = MagicMock()
    mock_client.fetch_short_interest.return_value = sample_short_interest_records
    config = {"skip_if_fresh_days": {"short_interest": 7}}

    result = backfill_short_interest_for_ticker(
        db_connection, mock_client, "AAPL", config=config, force=True
    )

    mock_client.fetch_short_interest.assert_called_once()
    assert result == len(sample_short_interest_records)
