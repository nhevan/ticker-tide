"""Tests for src/fetcher/main.py.

All tests are written first (TDD). All external API calls and sub-modules are mocked.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.fetcher.main import run_daily_fetch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_notifier_config() -> dict:
    """Return a minimal notifier config dict with sentiment enrichment disabled."""
    return {
        "telegram": {
            "bot_token": "fake-bot-token",
            "admin_chat_id": "111",
            "subscriber_chat_ids": ["222"],
            "display_timezone": "Europe/Amsterdam",
        },
        "sentiment_enrichment": {"enabled": False},
    }


# ---------------------------------------------------------------------------
# Tests: target_date parameter behaviour
# ---------------------------------------------------------------------------

class TestRunDailyFetchTargetDate:
    """
    run_daily_fetch accepts an optional target_date string (YYYY-MM-DD).
    When provided, all idempotency keys and log messages use that date.
    When omitted, it falls back to today in UTC.
    """

    def _make_load_config(self, db_path: str) -> object:
        """Return a side_effect function that returns the right config per key."""
        def _load_config(key: str) -> dict:
            if key == "database":
                return {"path": db_path}
            return _make_notifier_config()
        return _load_config

    def test_uses_provided_target_date_for_idempotency(self, tmp_path) -> None:
        """
        When target_date="2026-03-20" is supplied, the fetcher should check the
        pipeline_events table for "2026-03-20", not for today's UTC date.
        """
        target_date = "2026-03-20"

        with patch("src.fetcher.main.load_config", side_effect=self._make_load_config(str(tmp_path / "test.db"))), \
             patch("src.fetcher.main.load_env"), \
             patch("src.fetcher.main.get_connection", return_value=MagicMock()), \
             patch("src.fetcher.main.get_pipeline_event_status", return_value=None) as mock_event_status, \
             patch("src.fetcher.main.write_pipeline_event"), \
             patch("src.fetcher.main.get_telegram_config", return_value={"bot_token": "x", "admin_chat_id": "1"}):

            run_daily_fetch(target_date=target_date)

        # The idempotency check (get_pipeline_event_status) must use the supplied date
        event_status_args = mock_event_status.call_args[0]
        assert event_status_args[1] == "fetcher_done"
        assert event_status_args[2] == target_date, (
            f"Expected idempotency check for '{target_date}', "
            f"got '{event_status_args[2]}'. "
            "The fetcher is using today's UTC date instead of target_date."
        )

    def test_skips_when_already_completed_for_target_date(self, tmp_path) -> None:
        """
        When target_date="2026-03-20" is already marked completed in pipeline_events,
        run_daily_fetch should return skipped=True without re-processing.
        """
        target_date = "2026-03-20"

        with patch("src.fetcher.main.load_config", side_effect=self._make_load_config(str(tmp_path / "test.db"))), \
             patch("src.fetcher.main.load_env"), \
             patch("src.fetcher.main.get_connection", return_value=MagicMock()), \
             patch("src.fetcher.main.get_pipeline_event_status", return_value="completed"), \
             patch("src.fetcher.main.write_pipeline_event") as mock_write_event, \
             patch("src.fetcher.main.get_telegram_config", return_value={"bot_token": "x", "admin_chat_id": "1"}):

            result = run_daily_fetch(target_date=target_date)

        assert result["skipped"] is True
        assert result["reason"] == "already completed"
        mock_write_event.assert_not_called()

    def test_falls_back_to_utc_today_when_no_target_date(self, tmp_path) -> None:
        """
        When target_date is not provided, the fetcher should use today's UTC date
        as the idempotency key (existing behaviour preserved).
        """
        utc_today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

        with patch("src.fetcher.main.load_config", side_effect=self._make_load_config(str(tmp_path / "test.db"))), \
             patch("src.fetcher.main.load_env"), \
             patch("src.fetcher.main.get_connection", return_value=MagicMock()), \
             patch("src.fetcher.main.get_pipeline_event_status", return_value=None) as mock_event_status, \
             patch("src.fetcher.main.write_pipeline_event"), \
             patch("src.fetcher.main.get_telegram_config", return_value={"bot_token": "x", "admin_chat_id": "1"}):

            run_daily_fetch()  # no target_date

        event_status_args = mock_event_status.call_args[0]
        assert event_status_args[2] == utc_today, (
            f"Expected idempotency key '{utc_today}' (UTC today) when no target_date supplied, "
            f"got '{event_status_args[2]}'."
        )

    def test_returns_expected_summary_keys(self, tmp_path) -> None:
        """
        run_daily_fetch should return a dict with skipped, tickers_processed,
        tickers_failed, and duration_seconds keys.
        """
        with patch("src.fetcher.main.load_config", side_effect=self._make_load_config(str(tmp_path / "test.db"))), \
             patch("src.fetcher.main.load_env"), \
             patch("src.fetcher.main.get_connection", return_value=MagicMock()), \
             patch("src.fetcher.main.get_pipeline_event_status", return_value=None), \
             patch("src.fetcher.main.write_pipeline_event"), \
             patch("src.fetcher.main.get_telegram_config", return_value={"bot_token": "x", "admin_chat_id": "1"}):

            result = run_daily_fetch(target_date="2026-03-20")

        assert "skipped" in result
        assert "tickers_processed" in result
        assert "tickers_failed" in result
        assert "duration_seconds" in result
        assert result["skipped"] is False


# ---------------------------------------------------------------------------
# Tests: sub-module wiring
# ---------------------------------------------------------------------------

def _make_load_config_multi(db_path: str) -> object:
    """Return a side_effect for load_config that handles database, notifier, fetcher, backfiller."""
    def _load_config(key: str) -> dict:
        if key == "database":
            return {"path": db_path}
        if key == "notifier":
            return {
                "telegram": {
                    "bot_token": "fake-bot-token",
                    "admin_chat_id": "111",
                    "subscriber_chat_ids": ["222"],
                    "display_timezone": "Europe/Amsterdam",
                },
                "sentiment_enrichment": {"enabled": False},
            }
        if key == "fetcher":
            return {
                "polling_intervals": {
                    "fundamentals_days": 14,
                    "earnings_calendar_days": 7,
                    "short_interest_days": 15,
                },
                "rate_limit": {
                    "finnhub_delay_seconds": 1.0,
                },
            }
        if key == "backfiller":
            return {
                "skip_if_fresh_days": {"earnings": 7, "fundamentals": 30, "news": 1, "filings": 7},
                "ohlcv": {"lookback_years": 5, "adjusted": True},
                "news": {"lookback_months": 3},
                "filings": {"lookback_months": 6},
                "macro": {"treasury_lookback_years": 5},
            }
        return {}
    return _load_config


def _sub_module_result(tickers_processed: int = 5, tickers_failed: int = 0) -> dict:
    """Return a typical sub-module result dict."""
    return {"tickers_processed": tickers_processed, "tickers_failed": tickers_failed}


# Common patches applied to every test in this section
_COMMON_PATCHES = [
    "src.fetcher.main.load_env",
    "src.fetcher.main.get_connection",
    "src.fetcher.main.get_pipeline_event_status",
    "src.fetcher.main.write_pipeline_event",
    "src.fetcher.main.get_telegram_config",
]


class TestSubModuleWiring:
    """Verify that run_daily_fetch calls all data-fetching sub-modules."""

    def _run_with_mocks(self, tmp_path, extra_patches: dict | None = None, force: bool = False):
        """
        Helper to run run_daily_fetch with all sub-modules mocked.

        Returns (result, mocks_dict) where mocks_dict maps patch target to its Mock.
        """
        import contextlib
        from unittest.mock import patch, MagicMock

        mock_polygon = MagicMock()
        mock_polygon.__enter__ = MagicMock(return_value=mock_polygon)
        mock_polygon.__exit__ = MagicMock(return_value=False)

        sub_module_patches = {
            "src.fetcher.main.backfill_all_tickers": _sub_module_result(),
            "src.fetcher.main.backfill_all_macro": _sub_module_result(),
            "src.fetcher.main.backfill_all_fundamentals": _sub_module_result(),
            "src.fetcher.main.run_periodic_earnings": {"refreshed": 3, "skipped": 2, "failed": 0, "total_rows": 50},
            "src.fetcher.main.backfill_all_corporate_actions": _sub_module_result(),
            "src.fetcher.main.backfill_all_news": _sub_module_result(),
            "src.fetcher.main.backfill_all_filings": _sub_module_result(),
        }
        if extra_patches:
            sub_module_patches.update(extra_patches)

        mocks = {}
        stack = contextlib.ExitStack()

        # Common patches
        for target in _COMMON_PATCHES:
            m = stack.enter_context(patch(target))
            mocks[target] = m

        mocks["src.fetcher.main.get_pipeline_event_status"].return_value = None
        mocks["src.fetcher.main.get_connection"].return_value = MagicMock()
        mocks["src.fetcher.main.get_telegram_config"].return_value = {
            "bot_token": "x", "admin_chat_id": "1",
        }

        # Config
        stack.enter_context(
            patch("src.fetcher.main.load_config", side_effect=_make_load_config_multi(str(tmp_path / "test.db")))
        )

        # API clients
        m_polygon_cls = stack.enter_context(patch("src.fetcher.main.PolygonClient", return_value=mock_polygon))
        m_finnhub_cls = stack.enter_context(patch("src.fetcher.main.FinnhubClient", return_value=MagicMock()))
        mocks["src.fetcher.main.PolygonClient"] = m_polygon_cls
        mocks["src.fetcher.main.FinnhubClient"] = m_finnhub_cls

        # Active tickers / sector ETFs / benchmarks
        stack.enter_context(
            patch("src.fetcher.main.get_active_tickers", return_value=[{"symbol": "AAPL"}, {"symbol": "MSFT"}])
        )
        stack.enter_context(patch("src.fetcher.main.get_sector_etfs", return_value=["XLK", "XLF"]))
        stack.enter_context(patch("src.fetcher.main.get_market_benchmarks", return_value={"spy": "SPY"}))

        # Env vars
        stack.enter_context(patch("os.getenv", side_effect=lambda k, d="": {
            "POLYGON_API_KEY": "fake-polygon-key",
            "FINNHUB_API_KEY": "fake-finnhub-key",
            "TELEGRAM_BOT_TOKEN": "fake-bot",
            "TELEGRAM_ADMIN_CHAT_ID": "111",
        }.get(k, d)))

        # Sub-module patches
        for target, return_val in sub_module_patches.items():
            if isinstance(return_val, Exception):
                m = stack.enter_context(patch(target, side_effect=return_val))
            else:
                m = stack.enter_context(patch(target, return_value=return_val))
            mocks[target] = m

        # log_alert patch (for error handling tests)
        m_log_alert = stack.enter_context(patch("src.fetcher.main.log_alert"))
        mocks["src.fetcher.main.log_alert"] = m_log_alert

        with stack:
            result = run_daily_fetch(target_date="2026-03-20", force=force)

        return result, mocks

    def test_all_sub_modules_are_called(self, tmp_path) -> None:
        """All 7 data-fetching sub-modules must be invoked during a normal run."""
        result, mocks = self._run_with_mocks(tmp_path)

        for target in [
            "src.fetcher.main.backfill_all_tickers",
            "src.fetcher.main.backfill_all_macro",
            "src.fetcher.main.backfill_all_fundamentals",
            "src.fetcher.main.run_periodic_earnings",
            "src.fetcher.main.backfill_all_corporate_actions",
            "src.fetcher.main.backfill_all_news",
            "src.fetcher.main.backfill_all_filings",
        ]:
            assert mocks[target].called, f"{target} was not called"

        assert result["skipped"] is False

    def test_tickers_processed_aggregated(self, tmp_path) -> None:
        """tickers_processed should sum counts from all sub-modules that return it."""
        result, _ = self._run_with_mocks(tmp_path)

        # 6 sub-modules return tickers_processed=5, earnings returns different shape
        assert result["tickers_processed"] == 30
        assert result["tickers_failed"] == 0

    def test_single_phase_failure_does_not_stop_others(self, tmp_path) -> None:
        """If OHLCV fetch raises, all other sub-modules should still be called."""
        result, mocks = self._run_with_mocks(
            tmp_path,
            extra_patches={
                "src.fetcher.main.backfill_all_tickers": RuntimeError("Polygon API down"),
            },
        )

        # OHLCV failed, but others should have been called
        for target in [
            "src.fetcher.main.backfill_all_macro",
            "src.fetcher.main.backfill_all_fundamentals",
            "src.fetcher.main.run_periodic_earnings",
            "src.fetcher.main.backfill_all_corporate_actions",
            "src.fetcher.main.backfill_all_news",
            "src.fetcher.main.backfill_all_filings",
        ]:
            assert mocks[target].called, f"{target} was not called after OHLCV failure"

        # tickers_processed should still count the successful phases
        assert result["tickers_processed"] == 25
        assert result["skipped"] is False

    def test_failed_phase_logged_to_alerts(self, tmp_path) -> None:
        """Failed sub-modules should be logged via log_alert."""
        _, mocks = self._run_with_mocks(
            tmp_path,
            extra_patches={
                "src.fetcher.main.backfill_all_tickers": RuntimeError("API down"),
            },
        )

        mocks["src.fetcher.main.log_alert"].assert_called()
        alert_args = mocks["src.fetcher.main.log_alert"].call_args[0]
        assert "ohlcv" in alert_args[-1].lower() or "OHLCV" in alert_args[-1]

    def test_force_flag_passed_to_sub_modules(self, tmp_path) -> None:
        """force=True should be forwarded to all sub-modules."""
        _, mocks = self._run_with_mocks(tmp_path, force=True)

        # Check OHLCV
        call_kwargs = mocks["src.fetcher.main.backfill_all_tickers"].call_args
        assert call_kwargs[1].get("force") is True or (
            len(call_kwargs[0]) > 0 and any(v is True for v in call_kwargs[0])
        )

        # Check fundamentals
        call_kwargs = mocks["src.fetcher.main.backfill_all_fundamentals"].call_args
        assert call_kwargs[1].get("force") is True

    def test_api_clients_initialized(self, tmp_path) -> None:
        """PolygonClient and FinnhubClient should be instantiated."""
        _, mocks = self._run_with_mocks(tmp_path)

        mocks["src.fetcher.main.PolygonClient"].assert_called_once()
        mocks["src.fetcher.main.FinnhubClient"].assert_called_once()

    def test_multiple_phase_failures_still_completes(self, tmp_path) -> None:
        """Even if multiple phases fail, the fetcher should complete and return results."""
        result, mocks = self._run_with_mocks(
            tmp_path,
            extra_patches={
                "src.fetcher.main.backfill_all_tickers": RuntimeError("OHLCV down"),
                "src.fetcher.main.backfill_all_news": RuntimeError("News API down"),
                "src.fetcher.main.backfill_all_filings": RuntimeError("Filings down"),
            },
        )

        # Should still have called all phases
        for target in [
            "src.fetcher.main.backfill_all_macro",
            "src.fetcher.main.backfill_all_fundamentals",
            "src.fetcher.main.run_periodic_earnings",
            "src.fetcher.main.backfill_all_corporate_actions",
        ]:
            assert mocks[target].called

        # 3 failed, 3 successful (each returning tickers_processed=5) + earnings
        assert result["tickers_processed"] == 15
        assert result["skipped"] is False
        assert len(result.get("failed_phases", [])) == 3
