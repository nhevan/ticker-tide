"""
Tests for src/notifier/telegram.py

Covers send_daily_report, send_heartbeat, and send_market_closed_notification.
All Telegram API calls are mocked via send_telegram_message.
"""
from __future__ import annotations

import pytest


class TestSendDailyReport:
    def test_send_daily_report(self, mocker):
        """Sends one message per item in the list; returns True on full success."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=12345,
        )

        result = send_daily_report(["Message 1", "Message 2"], "test-token", "test-chat")

        assert mock_send.call_count == 2
        assert result is True

    def test_send_daily_report_splits_long_message(self, mocker):
        """Already-split list of 3 messages results in 3 send calls."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=100,
        )

        result = send_daily_report(
            ["Part 1", "Part 2", "Part 3"], "token", "chat"
        )

        assert mock_send.call_count == 3
        assert result is True

    def test_send_daily_report_handles_send_failure(self, mocker):
        """Returns False when send_telegram_message returns None; does not crash."""
        from src.notifier.telegram import send_daily_report

        mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=None,
        )

        result = send_daily_report(["Some message"], "token", "chat")

        assert result is False


class TestSendHeartbeat:
    def test_send_heartbeat(self, mocker):
        """Sends the heartbeat text in a single call."""
        from src.notifier.telegram import send_heartbeat

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=99,
        )

        result = send_heartbeat("Pipeline completed 2025-01-15", "token", "chat")

        assert mock_send.call_count == 1
        call_args = mock_send.call_args
        assert "Pipeline completed" in call_args[0][2]
        assert result is True


class TestSendMarketClosedNotification:
    def test_send_market_closed_notification(self, mocker):
        """Sends a message containing 'Market closed'."""
        from src.notifier.telegram import send_market_closed_notification

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=42,
        )

        result = send_market_closed_notification(
            "2026-03-18", "token", "chat", config={}
        )

        assert mock_send.call_count == 1
        sent_text = mock_send.call_args[0][2]
        assert "Market closed" in sent_text
        assert result is True
