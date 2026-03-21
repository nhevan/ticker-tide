"""Tests for scripts/run_daily.py.

All tests are written first (TDD). All external calls and sub-modules are mocked.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

# run_daily lives in scripts/, not src/ — import via path manipulation done by the
# script itself, but for tests we can import directly since conftest adds the root.
import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.run_daily import run_daily_pipeline  # noqa: E402


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_tg_config() -> dict:
    """Return a minimal Telegram config dict for mocking."""
    return {
        "bot_token": "fake-bot-token",
        "admin_chat_id": "111",
        "subscriber_chat_ids": ["222", "333"],
    }


def _make_notifier_config() -> dict:
    """Return a minimal notifier config dict for mocking."""
    return {
        "telegram": {
            "bot_token": "fake-bot-token",
            "admin_chat_id": "111",
            "subscriber_chat_ids": ["222", "333"],
            "display_timezone": "Europe/Amsterdam",
        },
        "sentiment_enrichment": {"enabled": False},
    }


# ---------------------------------------------------------------------------
# Tests: market calendar check uses yesterday (UTC), not today
# ---------------------------------------------------------------------------

class TestMarketCalendarUsesYesterday:
    """
    The pipeline runs at 00:00 UTC. US markets close at ~21:00 UTC, so the
    relevant trading date is always UTC-minus-one-day (yesterday). These tests
    verify that the market calendar check receives yesterday's date, not today's.
    """

    def test_market_check_called_with_previous_day_on_saturday(self) -> None:
        """
        When pipeline runs at 00:00 UTC on Saturday March 21, it should check
        Friday March 20 — not Saturday March 21 — for market open status.
        """
        saturday_midnight_utc = datetime(2026, 3, 21, 0, 0, 0, tzinfo=timezone.utc)
        expected_check_date = date(2026, 3, 20)  # Friday

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=False) as mock_calendar, \
             patch("scripts.run_daily.send_market_closed_notification") as mock_notify, \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = saturday_midnight_utc

            run_daily_pipeline()

            mock_calendar.assert_called_once_with(expected_check_date)

    def test_market_check_called_with_previous_day_on_sunday(self) -> None:
        """
        When pipeline runs at 00:00 UTC on Sunday, it should check Saturday —
        which is not a trading day — and exit cleanly.
        """
        sunday_midnight_utc = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)
        expected_check_date = date(2026, 3, 21)  # Saturday

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=False) as mock_calendar, \
             patch("scripts.run_daily.send_market_closed_notification"), \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = sunday_midnight_utc

            run_daily_pipeline()

            mock_calendar.assert_called_once_with(expected_check_date)

    def test_market_check_called_with_previous_day_on_weekday(self) -> None:
        """
        When pipeline runs at 00:00 UTC on Tuesday, it should check Monday —
        which is a trading day — and proceed.
        """
        tuesday_midnight_utc = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
        expected_check_date = date(2026, 3, 23)  # Monday

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=True) as mock_calendar, \
             patch("scripts.run_daily.run_daily_fetch", return_value={"skipped": False}), \
             patch("scripts.run_daily.run_calculator"), \
             patch("scripts.run_daily.run_scorer", return_value={"skipped": False, "scoring_date": "2026-03-23"}), \
             patch("scripts.run_daily.run_notifier", return_value={"skipped": False}), \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = tuesday_midnight_utc

            run_daily_pipeline()

            mock_calendar.assert_called_once_with(expected_check_date)


# ---------------------------------------------------------------------------
# Tests: market-closed notification uses the correct (previous) date
# ---------------------------------------------------------------------------

class TestMarketClosedNotificationDate:
    """
    When the pipeline exits early due to market closed, the notification should
    report the actual date that was checked (yesterday), not today's date.
    """

    def test_market_closed_notification_uses_friday_date_when_run_on_saturday(self) -> None:
        """
        Running on Saturday should send the notification with Friday's date ("2026-03-20"),
        not Saturday's date ("2026-03-21").
        """
        saturday_midnight_utc = datetime(2026, 3, 21, 0, 0, 0, tzinfo=timezone.utc)

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=False), \
             patch("scripts.run_daily.send_market_closed_notification") as mock_notify, \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = saturday_midnight_utc

            result = run_daily_pipeline()

        assert result == 0
        # First positional arg to send_market_closed_notification is the date string
        actual_date_arg = mock_notify.call_args[0][0]
        assert actual_date_arg == "2026-03-20", (
            f"Expected notification date '2026-03-20' (Friday), got '{actual_date_arg}'. "
            "The pipeline is using today's date instead of yesterday's."
        )

    def test_market_closed_notification_uses_holiday_date(self) -> None:
        """
        Running on a Tuesday after a Monday holiday should send notification
        with Monday's date (the holiday), not Tuesday's date.
        """
        tuesday_midnight_utc = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)  # Tuesday after MLK Day

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=False), \
             patch("scripts.run_daily.send_market_closed_notification") as mock_notify, \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = tuesday_midnight_utc

            run_daily_pipeline()

        actual_date_arg = mock_notify.call_args[0][0]
        assert actual_date_arg == "2026-01-19", (
            f"Expected notification date '2026-01-19' (MLK Day Monday), got '{actual_date_arg}'."
        )


# ---------------------------------------------------------------------------
# Tests: target_date is passed to run_daily_fetch
# ---------------------------------------------------------------------------

class TestTargetDatePassedToFetcher:
    """
    When the market is open, run_daily_pipeline must pass target_date (yesterday UTC)
    to run_daily_fetch so the fetcher processes the correct date's data.
    """

    def test_run_daily_fetch_receives_target_date(self) -> None:
        """
        target_date="2026-03-20" (Friday) should be passed to run_daily_fetch
        when pipeline runs at midnight UTC on Saturday March 21.
        """
        saturday_midnight_utc = datetime(2026, 3, 21, 0, 0, 0, tzinfo=timezone.utc)

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=True), \
             patch("scripts.run_daily.run_daily_fetch", return_value={"skipped": False}) as mock_fetch, \
             patch("scripts.run_daily.run_calculator"), \
             patch("scripts.run_daily.run_scorer", return_value={"skipped": False, "scoring_date": "2026-03-20"}), \
             patch("scripts.run_daily.run_notifier", return_value={"skipped": False}), \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = saturday_midnight_utc

            run_daily_pipeline()

        kwargs = mock_fetch.call_args[1]
        assert kwargs.get("target_date") == "2026-03-20", (
            f"Expected run_daily_fetch to receive target_date='2026-03-20', "
            f"got: {kwargs}"
        )

    def test_run_daily_fetch_receives_correct_weekday_target_date(self) -> None:
        """
        Running on Tuesday at midnight UTC → target_date should be Monday.
        """
        tuesday_midnight_utc = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)

        with patch("scripts.run_daily.datetime") as mock_dt, \
             patch("scripts.run_daily.is_market_open_today", return_value=True), \
             patch("scripts.run_daily.run_daily_fetch", return_value={"skipped": False}) as mock_fetch, \
             patch("scripts.run_daily.run_calculator"), \
             patch("scripts.run_daily.run_scorer", return_value={"skipped": False, "scoring_date": "2026-03-23"}), \
             patch("scripts.run_daily.run_notifier", return_value={"skipped": False}), \
             patch("scripts.run_daily.load_config", return_value=_make_notifier_config()), \
             patch("scripts.run_daily.load_env"), \
             patch("scripts.run_daily.get_telegram_config", return_value=_make_tg_config()):

            mock_dt.now.return_value = tuesday_midnight_utc

            run_daily_pipeline()

        kwargs = mock_fetch.call_args[1]
        assert kwargs.get("target_date") == "2026-03-23", (
            f"Expected run_daily_fetch to receive target_date='2026-03-23' (Monday), "
            f"got: {kwargs}"
        )
