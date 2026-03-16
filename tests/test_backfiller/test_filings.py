"""Tests for src/backfiller/filings.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.backfiller.filings import (
    backfill_8k_for_ticker,
    backfill_all_filings,
    convert_polygon_filing_to_row,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_filings() -> list[dict]:
    """Return 2 Polygon 8-K filing records for AAPL."""
    return [
        {
            "accession_number": "0000320193-24-000001",
            "ticker": "AAPL",
            "filing_date": "2024-06-01",
            "form_type": "8-K",
            "items_text": "Item 5.02: Departure of Directors or Certain Officers",
            "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001.txt",
        },
        {
            "accession_number": "0000320193-24-000002",
            "ticker": "AAPL",
            "filing_date": "2024-05-15",
            "form_type": "8-K",
            "items_text": "Item 2.02: Results of Operations and Financial Condition",
            "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000002.txt",
        },
    ]


@pytest.fixture
def sample_config() -> dict:
    """Return a minimal backfiller config dict for filings."""
    return {
        "filings": {
            "lookback_months": 6,
        }
    }


# ---------------------------------------------------------------------------
# Tests for convert_polygon_filing_to_row
# ---------------------------------------------------------------------------

def test_convert_polygon_filing_to_row_maps_fields() -> None:
    """All Polygon filing fields are mapped correctly to the DB schema."""
    filing = {
        "accession_number": "0000320193-24-000001",
        "ticker": "AAPL",
        "filing_date": "2024-06-01",
        "form_type": "8-K",
        "items_text": "Item 5.02: Departure of Directors",
        "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001.txt",
    }
    row = convert_polygon_filing_to_row(filing)

    assert row["accession_number"] == "0000320193-24-000001"
    assert row["ticker"] == "AAPL"
    assert row["filing_date"] == "2024-06-01"
    assert row["form_type"] == "8-K"
    assert row["items_text"] == "Item 5.02: Departure of Directors"
    assert row["filing_url"] == "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001.txt"
    assert row["fetched_at"] is not None


# ---------------------------------------------------------------------------
# Tests for backfill_8k_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_8k_stores_filings(db_connection, sample_filings) -> None:
    """Mock fetch_8k_filings returning 2 filings → 2 rows in filings_8k."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = sample_filings

    count = backfill_8k_for_ticker(
        db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01"
    )

    assert count == 2
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM filings_8k WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert row_count == 2


def test_backfill_8k_maps_polygon_fields(db_connection) -> None:
    """All fields are correctly stored in filings_8k table."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = [
        {
            "accession_number": "0000320193-24-000001",
            "ticker": "AAPL",
            "filing_date": "2024-06-01",
            "form_type": "8-K",
            "items_text": "Item 5.02: Departure of Directors",
            "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001.txt",
        }
    ]

    backfill_8k_for_ticker(db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01")

    row = db_connection.execute(
        "SELECT * FROM filings_8k WHERE accession_number='0000320193-24-000001'"
    ).fetchone()
    assert row["ticker"] == "AAPL"
    assert row["filing_date"] == "2024-06-01"
    assert row["form_type"] == "8-K"
    assert row["items_text"] == "Item 5.02: Departure of Directors"
    assert row["filing_url"] == "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001.txt"


def test_backfill_8k_is_idempotent(db_connection, sample_filings) -> None:
    """Running backfill twice with the same data yields no duplicates."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = sample_filings

    backfill_8k_for_ticker(db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01")
    backfill_8k_for_ticker(db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM filings_8k WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 2


def test_backfill_8k_handles_api_error(db_connection) -> None:
    """When fetch_8k_filings returns [], no crash occurs and 0 is returned."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = []

    count = backfill_8k_for_ticker(
        db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01"
    )

    assert count == 0


def test_backfill_8k_sets_fetched_at(db_connection, sample_filings) -> None:
    """Every inserted filing row has a non-null fetched_at timestamp."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = sample_filings

    backfill_8k_for_ticker(db_connection, mock_client, "AAPL", "2023-12-01", "2024-06-01")

    rows = db_connection.execute(
        "SELECT fetched_at FROM filings_8k WHERE ticker='AAPL'"
    ).fetchall()
    assert all(row["fetched_at"] is not None for row in rows)


# ---------------------------------------------------------------------------
# Tests for backfill_all_filings
# ---------------------------------------------------------------------------

def test_backfill_all_filings_processes_each_ticker(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """With 3 tickers, fetch_8k_filings is called exactly 3 times."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = []

    backfill_all_filings(db_connection, mock_client, sample_tickers_list, sample_config)

    assert mock_client.fetch_8k_filings.call_count == 3


def test_backfill_all_filings_uses_progress_tracker(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """When bot_token and chat_id are provided, Telegram messages are sent."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = []

    with patch("src.backfiller.filings.send_telegram_message") as mock_send, \
         patch("src.backfiller.filings.edit_telegram_message") as mock_edit:
        mock_send.return_value = 42

        backfill_all_filings(
            db_connection,
            mock_client,
            sample_tickers_list,
            sample_config,
            bot_token="test_token",
            chat_id="test_chat_id",
        )

    assert mock_send.call_count >= 1
    assert mock_edit.call_count >= 3


def test_backfill_all_filings_returns_summary(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """Return dict includes filings_total, tickers_processed, tickers_failed."""
    mock_client = MagicMock()
    mock_client.fetch_8k_filings.return_value = []

    result = backfill_all_filings(
        db_connection, mock_client, sample_tickers_list, sample_config
    )

    assert "filings_total" in result
    assert "tickers_processed" in result
    assert "tickers_failed" in result
    assert result["tickers_processed"] == 3
    assert result["tickers_failed"] == 0
