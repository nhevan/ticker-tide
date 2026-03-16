"""Tests for src/backfiller/main.py.

All tests are written first (TDD). All external API calls and sub-backfillers are mocked.
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from src.backfiller.main import run_full_backfill, sync_tickers_from_config


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_tickers_config() -> list[dict]:
    """Return 3 active ticker config dicts matching tickers.json format."""
    return [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": 1},
    ]


@pytest.fixture
def mock_polygon_client() -> MagicMock:
    """Return a MagicMock PolygonClient with fetch_ticker_details returning sample data."""
    client = MagicMock()
    client.fetch_ticker_details.return_value = {
        "name": "Apple Inc.",
        "sic_code": "3571",
        "sic_description": "Electronic Computers",
        "market_cap": 3_000_000_000_000.0,
    }
    return client


# ---------------------------------------------------------------------------
# Tests for sync_tickers_from_config
# ---------------------------------------------------------------------------

def test_sync_tickers_inserts_from_config(
    db_connection, sample_tickers_config, mock_polygon_client
) -> None:
    """3 tickers in config → 3 rows in tickers table."""
    with patch("src.backfiller.main.get_active_tickers", return_value=sample_tickers_config):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    count = db_connection.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
    assert count == 3


def test_sync_tickers_fetches_details_from_polygon(
    db_connection, sample_tickers_config
) -> None:
    """fetch_ticker_details returns name/sic_code/sic_description/market_cap, stored correctly."""
    mock_client = MagicMock()
    mock_client.fetch_ticker_details.return_value = {
        "name": "Apple Inc.",
        "sic_code": "3571",
        "sic_description": "Electronic Computers",
        "market_cap": 3_000_000_000_000.0,
    }
    tickers_two = sample_tickers_config[:1]  # just AAPL

    with patch("src.backfiller.main.get_active_tickers", return_value=tickers_two):
        sync_tickers_from_config(db_connection, mock_client)

    row = db_connection.execute(
        "SELECT * FROM tickers WHERE symbol='AAPL'"
    ).fetchone()
    assert row["name"] == "Apple Inc."
    assert row["sic_code"] == "3571"
    assert row["sic_description"] == "Electronic Computers"
    assert row["market_cap"] == pytest.approx(3_000_000_000_000.0)


def test_sync_tickers_handles_polygon_details_failure(
    db_connection, sample_tickers_config
) -> None:
    """When fetch_ticker_details returns {}, ticker inserted with name/sic as NULL."""
    mock_client = MagicMock()
    mock_client.fetch_ticker_details.return_value = {}
    tickers_one = sample_tickers_config[:1]

    with patch("src.backfiller.main.get_active_tickers", return_value=tickers_one):
        sync_tickers_from_config(db_connection, mock_client)

    row = db_connection.execute(
        "SELECT name, sic_code, sic_description, market_cap FROM tickers WHERE symbol='AAPL'"
    ).fetchone()
    assert row["name"] is None
    assert row["sic_code"] is None
    assert row["sic_description"] is None
    assert row["market_cap"] is None


def test_sync_tickers_detects_new_ticker(
    db_connection, mock_polygon_client
) -> None:
    """2 tickers in DB, 3 in config → function returns the new ticker symbol."""
    # Pre-insert 2 tickers
    db_connection.execute(
        "INSERT INTO tickers (symbol, sector, sector_etf, added_date) VALUES (?, ?, ?, ?)",
        ("AAPL", "Technology", "XLK", "2026-01-01"),
    )
    db_connection.execute(
        "INSERT INTO tickers (symbol, sector, sector_etf, added_date) VALUES (?, ?, ?, ?)",
        ("MSFT", "Technology", "XLK", "2026-01-01"),
    )
    db_connection.commit()

    three_tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "NVDA", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
    ]

    with patch("src.backfiller.main.get_active_tickers", return_value=three_tickers):
        new_tickers = sync_tickers_from_config(db_connection, mock_polygon_client)

    assert "NVDA" in new_tickers


def test_sync_tickers_deactivates_removed_ticker(
    db_connection, mock_polygon_client
) -> None:
    """3 tickers in DB, 2 in config → 3rd has active=0 in DB."""
    for symbol in ("AAPL", "MSFT", "JPM"):
        db_connection.execute(
            "INSERT INTO tickers (symbol, sector, sector_etf, added_date, active) VALUES (?, ?, ?, ?, 1)",
            (symbol, "Technology", "XLK", "2026-01-01"),
        )
    db_connection.commit()

    two_tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
    ]

    with patch("src.backfiller.main.get_active_tickers", return_value=two_tickers):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    jpm_active = db_connection.execute(
        "SELECT active FROM tickers WHERE symbol='JPM'"
    ).fetchone()["active"]
    assert jpm_active == 0


def test_sync_tickers_does_not_delete_data(
    db_connection, mock_polygon_client
) -> None:
    """OHLCV data for deactivated ticker is NOT deleted when ticker is removed from config."""
    db_connection.execute(
        "INSERT INTO tickers (symbol, sector, sector_etf, added_date, active) VALUES (?, ?, ?, ?, 1)",
        ("JPM", "Financials", "XLF", "2026-01-01"),
    )
    db_connection.execute(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES ('JPM', '2024-06-01', 200.0, 205.0, 198.0, 202.0, 5000000)"
    )
    db_connection.commit()

    one_ticker = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
    ]

    with patch("src.backfiller.main.get_active_tickers", return_value=one_ticker):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    ohlcv_count = db_connection.execute(
        "SELECT COUNT(*) FROM ohlcv_daily WHERE ticker='JPM'"
    ).fetchone()[0]
    assert ohlcv_count == 1


def test_sync_tickers_reactivates_returned_ticker(
    db_connection, mock_polygon_client
) -> None:
    """Deactivated ticker added back to config → active=1 in DB."""
    db_connection.execute(
        "INSERT INTO tickers (symbol, sector, sector_etf, added_date, active) VALUES (?, ?, ?, ?, 0)",
        ("AAPL", "Technology", "XLK", "2026-01-01"),
    )
    db_connection.commit()

    config_with_aapl = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
    ]

    with patch("src.backfiller.main.get_active_tickers", return_value=config_with_aapl):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    aapl_active = db_connection.execute(
        "SELECT active FROM tickers WHERE symbol='AAPL'"
    ).fetchone()["active"]
    assert aapl_active == 1


def test_sync_tickers_updates_sector_info(
    db_connection, mock_polygon_client
) -> None:
    """When sector changes in config, DB has updated sector/sector_etf."""
    db_connection.execute(
        "INSERT INTO tickers (symbol, sector, sector_etf, added_date, active) VALUES (?, ?, ?, ?, 1)",
        ("AAPL", "OldSector", "XLY", "2026-01-01"),
    )
    db_connection.commit()

    updated_config = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": 1},
    ]

    with patch("src.backfiller.main.get_active_tickers", return_value=updated_config):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    row = db_connection.execute(
        "SELECT sector, sector_etf FROM tickers WHERE symbol='AAPL'"
    ).fetchone()
    assert row["sector"] == "Technology"
    assert row["sector_etf"] == "XLK"


def test_sync_tickers_sets_updated_at(
    db_connection, sample_tickers_config, mock_polygon_client
) -> None:
    """updated_at is set to a current UTC timestamp for all synced tickers."""
    with patch("src.backfiller.main.get_active_tickers", return_value=sample_tickers_config):
        sync_tickers_from_config(db_connection, mock_polygon_client)

    rows = db_connection.execute("SELECT updated_at FROM tickers").fetchall()
    assert all(row["updated_at"] is not None for row in rows)


# ---------------------------------------------------------------------------
# Tests for run_full_backfill
# ---------------------------------------------------------------------------

PHASE_PATCH_TARGETS = {
    "backfill_all_tickers": "src.backfiller.main.backfill_all_tickers",
    "backfill_all_macro": "src.backfiller.main.backfill_all_macro",
    "backfill_all_fundamentals": "src.backfiller.main.backfill_all_fundamentals",
    "backfill_all_earnings": "src.backfiller.main.backfill_all_earnings",
    "backfill_all_corporate_actions": "src.backfiller.main.backfill_all_corporate_actions",
    "backfill_all_news": "src.backfiller.main.backfill_all_news",
    "backfill_all_filings": "src.backfiller.main.backfill_all_filings",
    "sync_tickers_from_config": "src.backfiller.main.sync_tickers_from_config",
}


def _make_phase_return() -> dict:
    """Return a generic success dict for a mocked phase."""
    return {"tickers_processed": 3, "tickers_failed": 0}


def test_run_full_backfill_calls_all_phases(tmp_path) -> None:
    """Mock all 8 sub-backfillers; verify each is called once in correct order."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]) as mock_sync, \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()) as mock_ohlcv, \
         patch("src.backfiller.main.backfill_all_macro", return_value={}) as mock_macro, \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()) as mock_fund, \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()) as mock_earn, \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()) as mock_ca, \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()) as mock_news, \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()) as mock_filings, \
         patch("src.backfiller.main.log_pipeline_run"):

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1}
        ]

        run_full_backfill(db_path=db_path)

    mock_sync.assert_called_once()
    mock_ohlcv.assert_called_once()
    mock_macro.assert_called_once()
    mock_fund.assert_called_once()
    mock_earn.assert_called_once()
    mock_ca.assert_called_once()
    mock_news.assert_called_once()
    mock_filings.assert_called_once()


def test_run_full_backfill_continues_on_phase_failure(tmp_path) -> None:
    """One phase raises an exception; all other phases still run."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]), \
         patch("src.backfiller.main.backfill_all_tickers", side_effect=RuntimeError("OHLCV failed")), \
         patch("src.backfiller.main.backfill_all_macro", return_value={}) as mock_macro, \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()) as mock_fund, \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()) as mock_earn, \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()) as mock_ca, \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()) as mock_news, \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()) as mock_filings, \
         patch("src.backfiller.main.log_pipeline_run"):

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1}
        ]

        # Should not raise even though OHLCV failed
        run_full_backfill(db_path=db_path)

    # All other phases should still have been called
    mock_macro.assert_called_once()
    mock_fund.assert_called_once()
    mock_earn.assert_called_once()
    mock_ca.assert_called_once()
    mock_news.assert_called_once()
    mock_filings.assert_called_once()


def test_run_full_backfill_logs_pipeline_run(tmp_path) -> None:
    """A pipeline_runs entry is created after backfill completes."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]), \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_macro", return_value={}), \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.log_pipeline_run") as mock_log_run:

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1}
        ]

        run_full_backfill(db_path=db_path)

    mock_log_run.assert_called_once()


def test_run_full_backfill_sends_telegram_summary(tmp_path) -> None:
    """Final Telegram summary is sent when bot_token and chat_id are set."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]), \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_macro", return_value={}), \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.log_pipeline_run"), \
         patch.dict("os.environ", {
             "TELEGRAM_BOT_TOKEN": "test_token",
             "TELEGRAM_CHAT_ID": "test_chat_id",
             "POLYGON_API_KEY": "test_polygon_key",
             "FINNHUB_API_KEY": "test_finnhub_key",
         }), \
         patch("src.backfiller.main.send_telegram_message") as mock_send:

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1}
        ]

        run_full_backfill(db_path=db_path)

    assert mock_send.call_count >= 1


def test_run_backfill_single_ticker(tmp_path) -> None:
    """ticker_filter='AAPL' → only AAPL in sub-backfiller calls."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]), \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()) as mock_ohlcv, \
         patch("src.backfiller.main.backfill_all_macro", return_value={}), \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.log_pipeline_run"):

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        run_full_backfill(db_path=db_path, ticker_filter="AAPL")

    # The tickers passed to backfill_all_tickers should only contain AAPL
    call_args = mock_ohlcv.call_args
    tickers_arg = call_args[0][2]  # positional arg: db_conn, polygon_client, tickers, config
    assert len(tickers_arg) == 1
    assert tickers_arg[0]["symbol"] == "AAPL"


def test_run_backfill_single_phase(tmp_path) -> None:
    """phase_filter='ohlcv' → only OHLCV phase is called, others are skipped."""
    db_path = str(tmp_path / "test.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]) as mock_sync, \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()) as mock_ohlcv, \
         patch("src.backfiller.main.backfill_all_macro", return_value={}) as mock_macro, \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()) as mock_fund, \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()) as mock_earn, \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()) as mock_ca, \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()) as mock_news, \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()) as mock_filings, \
         patch("src.backfiller.main.log_pipeline_run"):

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1}
        ]

        run_full_backfill(db_path=db_path, phase_filter="ohlcv")

    mock_ohlcv.assert_called_once()
    mock_sync.assert_not_called()
    mock_macro.assert_not_called()
    mock_fund.assert_not_called()
    mock_earn.assert_not_called()
    mock_ca.assert_not_called()
    mock_news.assert_not_called()
    mock_filings.assert_not_called()


def test_run_backfill_creates_db_if_not_exists(tmp_path) -> None:
    """When a non-existent db_path is given, the file is created and all tables initialized."""
    new_db_path = str(tmp_path / "subdir" / "new_signals.db")

    with patch("src.backfiller.main.load_env"), \
         patch("src.backfiller.main.load_config") as mock_load_config, \
         patch("src.backfiller.main.get_active_tickers") as mock_tickers, \
         patch("src.backfiller.main.get_sector_etfs", return_value=["XLK"]), \
         patch("src.backfiller.main.get_market_benchmarks", return_value={"spy": "SPY"}), \
         patch("src.backfiller.main.PolygonClient"), \
         patch("src.backfiller.main.FinnhubClient"), \
         patch("src.backfiller.main.sync_tickers_from_config", return_value=[]), \
         patch("src.backfiller.main.backfill_all_tickers", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_macro", return_value={}), \
         patch("src.backfiller.main.backfill_all_fundamentals", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_earnings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_corporate_actions", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_news", return_value=_make_phase_return()), \
         patch("src.backfiller.main.backfill_all_filings", return_value=_make_phase_return()), \
         patch("src.backfiller.main.log_pipeline_run"):

        mock_load_config.return_value = {
            "ohlcv": {"lookback_years": 5},
            "news": {"lookback_months": 3, "finnhub_lookback_months": 1},
            "filings": {"lookback_months": 6},
            "earnings": {"lookback_years": 2},
            "fundamentals": {"lookback_years": 5, "periods": ["quarterly"]},
            "macro": {"treasury_lookback_years": 5},
        }
        mock_tickers.return_value = []

        run_full_backfill(db_path=new_db_path)

    assert Path(new_db_path).exists()
