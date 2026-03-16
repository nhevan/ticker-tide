"""Tests for src/backfiller/ohlcv.py.

Tests are written first (TDD). All external API calls are mocked.
"""

import sqlite3
from datetime import date, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from src.backfiller.ohlcv import (
    backfill_all_tickers,
    backfill_ohlcv_for_ticker,
    convert_polygon_bar_to_ohlcv_row,
    convert_polygon_timestamp_to_date,
)


# ---------------------------------------------------------------------------
# Local fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_polygon_bars() -> list[dict]:
    """
    Return 5 valid Polygon OHLCV bars with known timestamps.

    Timestamps correspond to 2024-01-02 through 2024-01-06.
    """
    return [
        {"o": 150.0, "h": 155.0, "l": 148.0, "c": 153.0, "v": 5_000_000, "vw": 152.0, "t": 1704171600000, "n": 400_000},
        {"o": 153.0, "h": 157.0, "l": 151.0, "c": 156.0, "v": 4_800_000, "vw": 154.5, "t": 1704258000000, "n": 380_000},
        {"o": 156.0, "h": 160.0, "l": 154.0, "c": 158.0, "v": 5_200_000, "vw": 157.0, "t": 1704344400000, "n": 420_000},
        {"o": 158.0, "h": 162.0, "l": 156.0, "c": 160.0, "v": 4_900_000, "vw": 159.0, "t": 1704430800000, "n": 390_000},
        {"o": 160.0, "h": 164.0, "l": 158.0, "c": 162.0, "v": 5_100_000, "vw": 161.0, "t": 1704517200000, "n": 410_000},
    ]


@pytest.fixture
def single_bar_fixture() -> dict:
    """
    Return one Polygon bar with exact values for field mapping tests.

    Timestamp 1704171600000 corresponds to 2024-01-02.
    """
    return {
        "o": 187.15,
        "h": 188.44,
        "l": 183.88,
        "c": 185.64,
        "v": 81_964_874,
        "vw": 185.95,
        "t": 1704171600000,
        "n": 1_008_871,
    }


# ---------------------------------------------------------------------------
# Tests for convert_polygon_timestamp_to_date
# ---------------------------------------------------------------------------

def test_backfill_ohlcv_converts_timestamp_to_date(db_connection, sample_polygon_bars) -> None:
    """
    Two bars at known timestamps map to correct YYYY-MM-DD date strings.

    1704171600000 → 2024-01-02
    1704258000000 → 2024-01-03
    """
    bars = sample_polygon_bars[:2]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = bars

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    rows = db_connection.execute(
        "SELECT date FROM ohlcv_daily WHERE ticker='AAPL' ORDER BY date"
    ).fetchall()
    dates = [row["date"] for row in rows]
    assert "2024-01-02" in dates
    assert "2024-01-03" in dates


# ---------------------------------------------------------------------------
# Tests for convert_polygon_bar_to_ohlcv_row
# ---------------------------------------------------------------------------

def test_backfill_ohlcv_maps_polygon_fields_correctly(
    db_connection, single_bar_fixture
) -> None:
    """
    Bar dict with exact Polygon field names maps to correct DB column values.

    Tests that o→open, h→high, l→low, c→close, v→volume, vw→vwap,
    n→num_transactions, and t converts to date string "2024-01-02".
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = [single_bar_fixture]

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    row = db_connection.execute(
        "SELECT * FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()

    assert row["ticker"] == "AAPL"
    assert row["date"] == "2024-01-02"
    assert row["open"] == pytest.approx(187.15)
    assert row["high"] == pytest.approx(188.44)
    assert row["low"] == pytest.approx(183.88)
    assert row["close"] == pytest.approx(185.64)
    assert row["volume"] == pytest.approx(81_964_874)
    assert row["vwap"] == pytest.approx(185.95)
    assert row["num_transactions"] == 1_008_871


# ---------------------------------------------------------------------------
# Tests for backfill_ohlcv_for_ticker
# ---------------------------------------------------------------------------

def test_backfill_ohlcv_single_ticker_inserts_rows(
    db_connection, sample_polygon_bars
) -> None:
    """
    Five valid bars from fetch_ohlcv result in 5 rows in ohlcv_daily for AAPL.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = sample_polygon_bars

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 5)

    count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 5


def test_backfill_ohlcv_calculates_correct_date_range(db_connection) -> None:
    """
    Calling with lookback_years=5 passes a from_date ~5 years ago and to_date=today.

    Asserts that the date range passed to fetch_ohlcv spans approximately 5 years.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = []

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 5)

    assert mock_client.fetch_ohlcv.called
    call_args = mock_client.fetch_ohlcv.call_args
    from_date_arg = call_args[0][1]
    to_date_arg = call_args[0][2]

    today = date.today()
    from_date_parsed = date.fromisoformat(from_date_arg)
    to_date_parsed = date.fromisoformat(to_date_arg)

    assert to_date_parsed == today
    days_diff = (today - from_date_parsed).days
    # 5 years ≈ 1826 days; allow a few days tolerance for leap years
    assert 1820 <= days_diff <= 1832


def test_backfill_ohlcv_validates_each_row(db_connection) -> None:
    """
    When one of 3 bars has close=0, only 2 rows are inserted and 1 alert is logged.
    """
    bars = [
        {"o": 150.0, "h": 155.0, "l": 148.0, "c": 153.0, "v": 5_000_000, "vw": 152.0, "t": 1704171600000, "n": 400_000},
        {"o": 153.0, "h": 157.0, "l": 151.0, "c": 0.0,   "v": 4_800_000, "vw": 154.5, "t": 1704258000000, "n": 380_000},
        {"o": 156.0, "h": 160.0, "l": 154.0, "c": 158.0, "v": 5_200_000, "vw": 157.0, "t": 1704344400000, "n": 420_000},
    ]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = bars

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='AAPL'"
    ).fetchone()[0]

    assert row_count == 2
    assert alert_count >= 1


def test_backfill_ohlcv_skips_row_with_negative_volume(db_connection) -> None:
    """
    A bar with volume=-100 fails validation and is not inserted; one alert is logged.
    """
    bars = [
        {"o": 150.0, "h": 155.0, "l": 148.0, "c": 153.0, "v": -100, "vw": 152.0, "t": 1704171600000, "n": 400_000},
    ]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = bars

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='AAPL'"
    ).fetchone()[0]

    assert row_count == 0
    assert alert_count >= 1


def test_backfill_ohlcv_handles_api_error(db_connection) -> None:
    """
    When fetch_ohlcv returns [], zero rows are inserted and a warning alert is logged.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = []

    result = backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='AAPL' AND severity='warning'"
    ).fetchone()[0]

    assert row_count == 0
    assert alert_count >= 1
    assert result == 0


def test_backfill_ohlcv_handles_empty_results(db_connection) -> None:
    """
    When fetch_ohlcv returns an empty list, function completes without crash and returns 0.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = []

    result = backfill_ohlcv_for_ticker(db_connection, mock_client, "MSFT", 5)

    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='MSFT'"
    ).fetchone()[0]

    assert row_count == 0
    assert result == 0


def test_backfill_ohlcv_is_idempotent(db_connection) -> None:
    """
    Calling backfill_ohlcv_for_ticker twice with the same 3 bars inserts exactly 3 rows.
    """
    bars = [
        {"o": 150.0, "h": 155.0, "l": 148.0, "c": 153.0, "v": 5_000_000, "vw": 152.0, "t": 1704171600000, "n": 400_000},
        {"o": 153.0, "h": 157.0, "l": 151.0, "c": 156.0, "v": 4_800_000, "vw": 154.5, "t": 1704258000000, "n": 380_000},
        {"o": 156.0, "h": 160.0, "l": 154.0, "c": 158.0, "v": 5_200_000, "vw": 157.0, "t": 1704344400000, "n": 420_000},
    ]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = bars

    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)
    backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 1)

    count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_ohlcv_returns_row_count(db_connection, sample_polygon_bars) -> None:
    """
    backfill_ohlcv_for_ticker returns the number of rows successfully inserted.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = sample_polygon_bars

    result = backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 5)

    assert result == 5


def test_backfill_ohlcv_returns_zero_on_failure(db_connection) -> None:
    """
    backfill_ohlcv_for_ticker returns 0 when fetch_ohlcv returns empty list.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = []

    result = backfill_ohlcv_for_ticker(db_connection, mock_client, "AAPL", 5)

    assert result == 0


# ---------------------------------------------------------------------------
# Tests for backfill_all_tickers
# ---------------------------------------------------------------------------

def test_backfill_all_tickers_processes_each_ticker(
    db_connection, sample_tickers_list, sample_polygon_bars
) -> None:
    """
    With 3 tickers, fetch_ohlcv is called exactly 3 times (once per ticker).
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = sample_polygon_bars
    config = {"ohlcv": {"lookback_years": 5}}

    backfill_all_tickers(db_connection, mock_client, sample_tickers_list, config)

    assert mock_client.fetch_ohlcv.call_count == 3


def test_backfill_all_tickers_continues_on_error(
    db_connection, sample_polygon_bars
) -> None:
    """
    When ticker 2 raises an exception, tickers 1 and 3 are still processed.

    An error alert is logged for the failed ticker.
    """
    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.side_effect = [
        sample_polygon_bars,
        RuntimeError("API timeout for MSFT"),
        sample_polygon_bars,
    ]
    config = {"ohlcv": {"lookback_years": 5}}

    backfill_all_tickers(db_connection, mock_client, tickers, config)

    aapl_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='AAPL'"
    ).fetchone()[0]
    jpm_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='JPM'"
    ).fetchone()[0]
    error_alerts = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='MSFT' AND severity='error'"
    ).fetchone()[0]

    assert aapl_count == 5
    assert jpm_count == 5
    assert error_alerts >= 1


def test_backfill_all_tickers_returns_summary(
    db_connection, sample_polygon_bars
) -> None:
    """
    With 3 tickers where 2 succeed and 1 fails, summary reports processed=2 and failed=1.
    """
    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.side_effect = [
        sample_polygon_bars,
        RuntimeError("Connection failed"),
        sample_polygon_bars,
    ]
    config = {"ohlcv": {"lookback_years": 5}}

    summary = backfill_all_tickers(db_connection, mock_client, tickers, config)

    assert summary["processed"] == 2
    assert summary["failed"] == 1
    assert summary["skipped"] == 0
    assert summary["total_rows"] == 10


def test_backfill_all_tickers_uses_progress_tracker(
    db_connection, sample_tickers_list, sample_polygon_bars
) -> None:
    """
    When bot_token and chat_id are provided, send_telegram_message is called once
    (initial message) and edit_telegram_message is called at least 3 times (≥1 per ticker).
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = sample_polygon_bars
    config = {"ohlcv": {"lookback_years": 5}}

    with patch("src.backfiller.ohlcv.send_telegram_message") as mock_send, \
         patch("src.backfiller.ohlcv.edit_telegram_message") as mock_edit:
        mock_send.return_value = 42  # Simulated message_id

        backfill_all_tickers(
            db_connection,
            mock_client,
            sample_tickers_list,
            config,
            bot_token="test_token",
            chat_id="test_chat_id",
        )

    # send_telegram_message called once for initial message, once for final summary
    assert mock_send.call_count >= 1
    # edit_telegram_message called at least once per ticker (3 tickers × 2 updates = ≥6)
    assert mock_edit.call_count >= 3
