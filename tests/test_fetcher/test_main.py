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
