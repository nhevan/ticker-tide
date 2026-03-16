"""Tests for src/backfiller/macro.py.

Tests are written first (TDD). All external API and yfinance calls are mocked.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.backfiller.macro import (
    backfill_all_macro,
    backfill_market_benchmarks,
    backfill_market_holidays,
    backfill_sector_etfs,
    backfill_treasury_yields,
    backfill_vix,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vix_dataframe(num_rows: int = 10) -> pd.DataFrame:
    """Return a synthetic VIX DataFrame matching fetch_vix_data's return format."""
    rows = []
    for day_index in range(num_rows):
        rows.append({
            "date": f"2024-01-{day_index + 1:02d}",
            "open": 15.0 + day_index * 0.1,
            "high": 16.0 + day_index * 0.1,
            "low": 14.5 + day_index * 0.1,
            "close": 15.5 + day_index * 0.1,
            "volume": 0,
        })
    return pd.DataFrame(rows)


def _make_treasury_records(num_records: int = 5) -> list[dict]:
    """Return a list of synthetic treasury yield records matching Polygon's format."""
    records = []
    for day_index in range(num_records):
        records.append({
            "date": f"2024-01-{day_index + 1:02d}",
            "1M": 5.1 + day_index * 0.01,
            "3M": 5.2 + day_index * 0.01,
            "6M": 5.3 + day_index * 0.01,
            "1Y": 5.0 + day_index * 0.01,
            "2Y": 4.8 + day_index * 0.01,
            "3Y": 4.7 + day_index * 0.01,
            "5Y": 4.5 + day_index * 0.01,
            "7Y": 4.4 + day_index * 0.01,
            "10Y": 4.3 + day_index * 0.01,
            "20Y": 4.5 + day_index * 0.01,
            "30Y": 4.6 + day_index * 0.01,
        })
    return records


def _make_ohlcv_bars(num_bars: int = 3) -> list[dict]:
    """Return a list of synthetic Polygon OHLCV bars."""
    bars = []
    # Timestamps: 2024-01-02 = 1704171600000, each subsequent day adds 86400000ms
    base_ts = 1704171600000
    for bar_index in range(num_bars):
        bars.append({
            "o": 200.0 + bar_index,
            "h": 205.0 + bar_index,
            "l": 198.0 + bar_index,
            "c": 202.0 + bar_index,
            "v": 3_000_000 + bar_index * 100_000,
            "vw": 201.5 + bar_index,
            "t": base_ts + bar_index * 86_400_000,
            "n": 300_000,
        })
    return bars


# ---------------------------------------------------------------------------
# Sector ETF tests
# ---------------------------------------------------------------------------

def test_backfill_sector_etfs(db_connection) -> None:
    """
    backfill_sector_etfs calls fetch_ohlcv twice (once per ETF) and stores rows for both.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = _make_ohlcv_bars(3)
    sector_etfs = ["XLK", "XLF"]

    backfill_sector_etfs(db_connection, mock_client, sector_etfs, lookback_years=5)

    assert mock_client.fetch_ohlcv.call_count == 2
    xlk_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='XLK'"
    ).fetchone()[0]
    xlf_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='XLF'"
    ).fetchone()[0]
    assert xlk_count == 3
    assert xlf_count == 3


# ---------------------------------------------------------------------------
# Market benchmark tests
# ---------------------------------------------------------------------------

def test_backfill_market_benchmarks_spy_qqq(db_connection) -> None:
    """
    backfill_market_benchmarks calls fetch_ohlcv for SPY and QQQ and stores rows for both.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = _make_ohlcv_bars(3)
    benchmarks = {"spy": "SPY", "qqq": "QQQ"}

    backfill_market_benchmarks(db_connection, mock_client, benchmarks, lookback_years=5)

    called_tickers = [args[0][0] for args in mock_client.fetch_ohlcv.call_args_list]
    assert "SPY" in called_tickers
    assert "QQQ" in called_tickers

    spy_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='SPY'"
    ).fetchone()[0]
    qqq_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='QQQ'"
    ).fetchone()[0]
    assert spy_count == 3
    assert qqq_count == 3


# ---------------------------------------------------------------------------
# VIX tests
# ---------------------------------------------------------------------------

def test_backfill_vix_via_yfinance(db_connection) -> None:
    """
    backfill_vix stores 10 rows in ohlcv_daily with ticker='^VIX' when
    fetch_vix_data returns a 10-row DataFrame.
    """
    vix_df = _make_vix_dataframe(10)

    with patch("src.backfiller.macro.yfinance_client") as mock_yf:
        mock_yf.fetch_vix_data.return_value = vix_df
        count = backfill_vix(db_connection, "2019-01-01", "2024-01-01")

    assert count == 10
    db_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='^VIX'"
    ).fetchone()[0]
    assert db_count == 10


def test_backfill_vix_maps_dataframe_to_db(db_connection) -> None:
    """
    backfill_vix correctly maps DataFrame columns to ohlcv_daily DB columns.

    Verifies exact values for the first row of a single-row DataFrame.
    """
    vix_df = pd.DataFrame([{
        "date": "2024-01-15",
        "open": 13.42,
        "high": 14.87,
        "low": 13.10,
        "close": 14.25,
        "volume": 0,
    }])

    with patch("src.backfiller.macro.yfinance_client") as mock_yf:
        mock_yf.fetch_vix_data.return_value = vix_df
        backfill_vix(db_connection, "2024-01-15", "2024-01-15")

    row = db_connection.execute(
        "SELECT * FROM ohlcv_daily WHERE ticker='^VIX' AND date='2024-01-15'"
    ).fetchone()

    assert row is not None
    assert row["ticker"] == "^VIX"
    assert row["date"] == "2024-01-15"
    assert row["open"] == pytest.approx(13.42)
    assert row["high"] == pytest.approx(14.87)
    assert row["low"] == pytest.approx(13.10)
    assert row["close"] == pytest.approx(14.25)
    assert row["vwap"] is None
    assert row["num_transactions"] is None


def test_backfill_vix_handles_yfinance_error(db_connection) -> None:
    """
    When fetch_vix_data returns an empty DataFrame, backfill_vix completes
    without raising and logs a warning alert.
    """
    empty_df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    with patch("src.backfiller.macro.yfinance_client") as mock_yf:
        mock_yf.fetch_vix_data.return_value = empty_df
        result = backfill_vix(db_connection, "2024-01-01", "2024-01-10")

    assert result == 0
    db_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='^VIX'"
    ).fetchone()[0]
    assert db_count == 0

    alert_count = db_connection.execute(
        "SELECT COUNT(*) FROM alerts_log WHERE ticker='^VIX'"
    ).fetchone()[0]
    assert alert_count >= 1


# ---------------------------------------------------------------------------
# Treasury yield tests
# ---------------------------------------------------------------------------

def test_backfill_treasury_yields(db_connection) -> None:
    """
    backfill_treasury_yields inserts 5 rows into treasury_yields for 5 records.
    """
    mock_client = MagicMock()
    mock_client.fetch_treasury_yields.return_value = _make_treasury_records(5)

    count = backfill_treasury_yields(db_connection, mock_client, lookback_years=5)

    assert count == 5
    db_count = db_connection.execute(
        "SELECT COUNT(*) FROM treasury_yields"
    ).fetchone()[0]
    assert db_count == 5


def test_backfill_treasury_stores_all_maturities(db_connection) -> None:
    """
    A record with all 11 maturity fields is stored with all values populated.
    """
    mock_client = MagicMock()
    mock_client.fetch_treasury_yields.return_value = [{
        "date": "2024-06-01",
        "1M": 5.10,
        "3M": 5.20,
        "6M": 5.30,
        "1Y": 5.00,
        "2Y": 4.80,
        "3Y": 4.70,
        "5Y": 4.50,
        "7Y": 4.40,
        "10Y": 4.30,
        "20Y": 4.50,
        "30Y": 4.60,
    }]

    backfill_treasury_yields(db_connection, mock_client, lookback_years=5)

    row = db_connection.execute(
        "SELECT * FROM treasury_yields WHERE date='2024-06-01'"
    ).fetchone()

    assert row is not None
    assert row["yield_1_month"] == pytest.approx(5.10)
    assert row["yield_3_month"] == pytest.approx(5.20)
    assert row["yield_6_month"] == pytest.approx(5.30)
    assert row["yield_1_year"] == pytest.approx(5.00)
    assert row["yield_2_year"] == pytest.approx(4.80)
    assert row["yield_3_year"] == pytest.approx(4.70)
    assert row["yield_5_year"] == pytest.approx(4.50)
    assert row["yield_7_year"] == pytest.approx(4.40)
    assert row["yield_10_year"] == pytest.approx(4.30)
    assert row["yield_20_year"] == pytest.approx(4.50)
    assert row["yield_30_year"] == pytest.approx(4.60)


def test_backfill_treasury_handles_missing_maturities(db_connection) -> None:
    """
    A record missing the 30-year maturity stores NULL for that column without crashing.
    """
    mock_client = MagicMock()
    mock_client.fetch_treasury_yields.return_value = [{
        "date": "2024-06-01",
        "1M": 5.10,
        "3M": 5.20,
        "6M": 5.30,
        "1Y": 5.00,
        "2Y": 4.80,
        "3Y": 4.70,
        "5Y": 4.50,
        "7Y": 4.40,
        "10Y": 4.30,
        "20Y": 4.50,
        # "30Y" deliberately omitted
    }]

    backfill_treasury_yields(db_connection, mock_client, lookback_years=5)

    row = db_connection.execute(
        "SELECT yield_30_year FROM treasury_yields WHERE date='2024-06-01'"
    ).fetchone()

    assert row is not None
    assert row["yield_30_year"] is None


def test_backfill_treasury_is_idempotent(db_connection) -> None:
    """
    Running backfill_treasury_yields twice with the same data produces no duplicate rows.
    """
    mock_client = MagicMock()
    mock_client.fetch_treasury_yields.return_value = _make_treasury_records(3)

    backfill_treasury_yields(db_connection, mock_client, lookback_years=5)
    backfill_treasury_yields(db_connection, mock_client, lookback_years=5)

    count = db_connection.execute(
        "SELECT COUNT(*) FROM treasury_yields"
    ).fetchone()[0]
    assert count == 3


# ---------------------------------------------------------------------------
# Market holiday tests
# ---------------------------------------------------------------------------

def test_backfill_market_holidays(db_connection) -> None:
    """
    backfill_market_holidays returns a list of date strings from the holiday records.
    """
    mock_client = MagicMock()
    mock_client.fetch_market_holidays.return_value = [
        {"date": "2026-12-25", "name": "Christmas", "status": "closed"},
        {"date": "2027-01-01", "name": "New Year's Day", "status": "closed"},
    ]

    result = backfill_market_holidays(db_connection, mock_client)

    assert isinstance(result, list)
    assert len(result) == 2
    assert "2026-12-25" in result
    assert "2027-01-01" in result


def test_backfill_market_holidays_extracts_dates(db_connection) -> None:
    """
    backfill_market_holidays returns only the date strings from holiday dicts.
    """
    mock_client = MagicMock()
    mock_client.fetch_market_holidays.return_value = [
        {"date": "2026-12-25", "name": "Christmas", "status": "closed"},
    ]

    result = backfill_market_holidays(db_connection, mock_client)

    assert result == ["2026-12-25"]


def test_backfill_market_holidays_filters_closed_only(db_connection) -> None:
    """
    backfill_market_holidays returns only dates with status='closed',
    excluding 'early-close' and other statuses.
    """
    mock_client = MagicMock()
    mock_client.fetch_market_holidays.return_value = [
        {"date": "2026-12-24", "name": "Christmas Eve", "status": "early-close"},
        {"date": "2026-12-25", "name": "Christmas", "status": "closed"},
        {"date": "2027-01-01", "name": "New Year's Day", "status": "closed"},
        {"date": "2027-07-03", "name": "Independence Day Eve", "status": "early-close"},
    ]

    result = backfill_market_holidays(db_connection, mock_client)

    assert "2026-12-25" in result
    assert "2027-01-01" in result
    assert "2026-12-24" not in result
    assert "2027-07-03" not in result
    assert len(result) == 2


# ---------------------------------------------------------------------------
# backfill_all_macro orchestration test
# ---------------------------------------------------------------------------

def test_backfill_macro_uses_progress_tracker(db_connection) -> None:
    """
    backfill_all_macro calls send_telegram_message for progress updates
    when bot_token and chat_id are provided.
    """
    mock_client = MagicMock()
    mock_client.fetch_ohlcv.return_value = _make_ohlcv_bars(2)
    mock_client.fetch_treasury_yields.return_value = _make_treasury_records(2)
    mock_client.fetch_market_holidays.return_value = [
        {"date": "2026-12-25", "name": "Christmas", "status": "closed"},
    ]

    config = {
        "ohlcv": {"lookback_years": 5},
        "macro": {"treasury_lookback_years": 5},
    }
    sector_etfs = ["XLK", "XLF"]
    benchmarks = {"spy": "SPY", "qqq": "QQQ"}

    with patch("src.backfiller.macro.yfinance_client") as mock_yf, \
         patch("src.backfiller.macro.send_telegram_message") as mock_send, \
         patch("src.backfiller.macro.edit_telegram_message") as mock_edit:
        mock_yf.fetch_vix_data.return_value = _make_vix_dataframe(2)
        mock_send.return_value = 99  # Simulated message_id

        backfill_all_macro(
            db_connection,
            mock_client,
            config,
            sector_etfs=sector_etfs,
            benchmarks=benchmarks,
            bot_token="test_token",
            chat_id="test_chat_id",
        )

    assert mock_send.call_count >= 1
