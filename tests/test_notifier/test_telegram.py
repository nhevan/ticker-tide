"""
Tests for src/notifier/telegram.py

Covers send_daily_report, send_heartbeat, send_market_closed_notification,
send_pipeline_error_alert, and get_telegram_config.
All Telegram API calls are mocked via send_telegram_message.
"""
from __future__ import annotations

import pytest


class TestSendDailyReport:
    def test_send_daily_report(self, mocker):
        """Sends one message per subscriber; returns sent/failed/total dict on success."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=12345,
        )

        result = send_daily_report(["Message 1", "Message 2"], "test-token", ["test-chat"])

        assert mock_send.call_count == 2
        assert result["sent"] == 1
        assert result["failed"] == 0
        assert result["total_subscribers"] == 1

    def test_send_daily_report_splits_long_message(self, mocker):
        """Already-split list of 3 messages results in 3 send calls per subscriber."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=100,
        )

        result = send_daily_report(
            ["Part 1", "Part 2", "Part 3"], "token", ["chat"]
        )

        assert mock_send.call_count == 3
        assert result["sent"] == 1

    def test_send_daily_report_handles_send_failure(self, mocker):
        """Returns failed=1 when send_telegram_message returns None; does not crash."""
        from src.notifier.telegram import send_daily_report

        mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=None,
        )

        result = send_daily_report(["Some message"], "token", ["chat"])

        assert result["failed"] == 1
        assert result["sent"] == 0

    def test_send_daily_report_to_all_subscribers(self, mocker):
        """Sends one message to each subscriber; verifies 3 calls with same report text."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=100,
        )

        report_text = "Daily report content"
        result = send_daily_report(
            [report_text], "token", ["chat_1", "chat_2", "chat_3"]
        )

        assert mock_send.call_count == 3
        assert result["sent"] == 3
        assert result["failed"] == 0
        assert result["total_subscribers"] == 3
        # Each call should have the same message text
        for call in mock_send.call_args_list:
            assert call[0][2] == report_text

    def test_send_daily_report_excludes_heartbeat_for_subscribers(self, mocker):
        """Subscriber report should not contain the heartbeat section."""
        from src.notifier.formatter import format_full_report

        results = {
            "bullish": [],
            "bearish": [],
            "flips": [],
            "daily_summary": "",
            "market_context_summary": "",
        }
        pipeline_stats = {
            "scoring_date": "2026-03-18",
            "bullish_count": 0,
            "bearish_count": 0,
            "neutral_count": 5,
            "fetcher_duration": 10.0,
            "calculator_duration": 20.0,
            "scorer_duration": 5.0,
        }
        config = {"telegram": {"display_timezone": "Europe/Amsterdam"}}

        subscriber_messages = format_full_report(
            results, pipeline_stats, config, include_heartbeat=False
        )

        full_text = "\n".join(subscriber_messages)
        assert "Pipeline completed" not in full_text

    def test_send_daily_report_continues_on_partial_failure(self, mocker):
        """On partial failure, successful subscribers get the message and failure is logged."""
        from src.notifier.telegram import send_daily_report

        def side_effect(bot_token, chat_id, text):
            if chat_id == "chat_2":
                return None
            return 999

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            side_effect=side_effect,
        )
        mock_logger = mocker.patch("src.notifier.telegram.logger")

        result = send_daily_report(
            ["Report text"], "token", ["chat_1", "chat_2", "chat_3"]
        )

        assert result["sent"] == 2
        assert result["failed"] == 1
        assert result["total_subscribers"] == 3
        # Warning logged for chat_2 failure
        assert mock_logger.warning.called

    def test_send_daily_report_empty_subscribers(self, mocker):
        """Empty subscriber list logs warning and sends no messages."""
        from src.notifier.telegram import send_daily_report

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=100,
        )
        mock_logger = mocker.patch("src.notifier.telegram.logger")

        result = send_daily_report(["Report text"], "token", [])

        assert mock_send.call_count == 0
        assert result["sent"] == 0
        assert result["failed"] == 0
        assert result["total_subscribers"] == 0
        assert mock_logger.warning.called

    def test_admin_also_subscriber(self, mocker):
        """When admin is in subscriber list, they receive the report (without heartbeat)."""
        from src.notifier.telegram import send_daily_report, send_heartbeat

        sent_chat_ids = []

        def capture_send(bot_token, chat_id, text):
            sent_chat_ids.append(chat_id)
            return 100

        mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            side_effect=capture_send,
        )

        admin_id = "111"
        subscriber_ids = ["111", "222"]

        # Admin (111) is in subscriber list — should receive the report
        result = send_daily_report(["Report"], "token", subscriber_ids)
        assert "111" in sent_chat_ids
        assert "222" in sent_chat_ids
        assert result["sent"] == 2

        # Heartbeat goes separately to admin only
        sent_chat_ids.clear()
        send_heartbeat("Pipeline completed at 01:23", "token", admin_id)
        assert sent_chat_ids == [admin_id]


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

    def test_send_heartbeat_to_admin_only(self, mocker):
        """Heartbeat is sent to admin_chat_id only, not to any subscriber."""
        from src.notifier.telegram import send_heartbeat

        sent_to = []

        def capture(bot_token, chat_id, text):
            sent_to.append(chat_id)
            return 99

        mocker.patch("src.notifier.telegram.send_telegram_message", side_effect=capture)

        send_heartbeat("Pipeline completed", "token", "admin_chat")

        assert sent_to == ["admin_chat"]
        assert len(sent_to) == 1


class TestSendMarketClosedNotification:
    def test_send_market_closed_notification(self, mocker):
        """Sends a message containing 'Market closed' to each subscriber."""
        from src.notifier.telegram import send_market_closed_notification

        mock_send = mocker.patch(
            "src.notifier.telegram.send_telegram_message",
            return_value=42,
        )

        result = send_market_closed_notification(
            "2026-03-18", "token", ["chat"], config={}
        )

        assert mock_send.call_count == 1
        sent_text = mock_send.call_args[0][2]
        assert "Market closed" in sent_text
        assert result["sent"] == 1

    def test_send_market_closed_to_all_subscribers(self, mocker):
        """Market closed notification is sent to all subscriber_chat_ids."""
        from src.notifier.telegram import send_market_closed_notification

        sent_to = []

        def capture(bot_token, chat_id, text):
            sent_to.append(chat_id)
            return 42

        mocker.patch("src.notifier.telegram.send_telegram_message", side_effect=capture)

        result = send_market_closed_notification(
            "2026-03-18", "token", ["chat_1", "chat_2", "chat_3"], config={}
        )

        assert result["sent"] == 3
        assert result["failed"] == 0
        assert set(sent_to) == {"chat_1", "chat_2", "chat_3"}


class TestSendPipelineErrorAlert:
    def test_send_pipeline_error_to_admin_only(self, mocker):
        """Pipeline error alert is sent to admin_chat_id only."""
        from src.notifier.telegram import send_pipeline_error_alert

        sent_to = []

        def capture(bot_token, chat_id, text):
            sent_to.append(chat_id)
            return 55

        mocker.patch("src.notifier.telegram.send_telegram_message", side_effect=capture)

        result = send_pipeline_error_alert(
            "fetcher", "connection timeout", "token", "admin_chat", config={}
        )

        assert sent_to == ["admin_chat"]
        assert len(sent_to) == 1
        assert result is True


class TestGetTelegramConfig:
    def test_load_chat_ids_from_env(self, mocker, monkeypatch):
        """When env vars are set, get_telegram_config loads admin and subscriber IDs from env."""
        from src.notifier.telegram import get_telegram_config

        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "env_token")
        monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "env_admin")
        monkeypatch.setenv("TELEGRAM_SUBSCRIBER_CHAT_IDS", "env_sub1,env_sub2")
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        config = {
            "telegram": {
                "admin_chat_id": "config_admin",
                "subscriber_chat_ids": ["config_sub1"],
            }
        }

        result = get_telegram_config(config)

        assert result["bot_token"] == "env_token"
        assert result["admin_chat_id"] == "env_admin"
        assert result["subscriber_chat_ids"] == ["env_sub1", "env_sub2"]

    def test_load_chat_ids_from_config_fallback(self, monkeypatch):
        """When no env vars are set, values come from config file."""
        from src.notifier.telegram import get_telegram_config

        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        monkeypatch.delenv("TELEGRAM_SUBSCRIBER_CHAT_IDS", raising=False)

        config = {
            "telegram": {
                "admin_chat_id": "config_admin",
                "subscriber_chat_ids": ["config_sub1", "config_sub2"],
            }
        }

        result = get_telegram_config(config)

        assert result["admin_chat_id"] == "config_admin"
        assert result["subscriber_chat_ids"] == ["config_sub1", "config_sub2"]

    def test_load_chat_ids_env_overrides_config(self, monkeypatch):
        """Env vars take priority over config file values."""
        from src.notifier.telegram import get_telegram_config

        monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "env_admin_override")
        monkeypatch.setenv("TELEGRAM_SUBSCRIBER_CHAT_IDS", "env_override_1,env_override_2")
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        config = {
            "telegram": {
                "admin_chat_id": "config_admin",
                "subscriber_chat_ids": ["config_sub1"],
            }
        }

        result = get_telegram_config(config)

        assert result["admin_chat_id"] == "env_admin_override"
        assert result["subscriber_chat_ids"] == ["env_override_1", "env_override_2"]

    def test_subscriber_chat_ids_parsed_from_comma_string(self, monkeypatch):
        """TELEGRAM_SUBSCRIBER_CHAT_IDS env var is split on commas into a list."""
        from src.notifier.telegram import get_telegram_config

        monkeypatch.setenv("TELEGRAM_SUBSCRIBER_CHAT_IDS", "111,222,333")
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        config = {"telegram": {"admin_chat_id": "", "subscriber_chat_ids": []}}

        result = get_telegram_config(config)

        assert result["subscriber_chat_ids"] == ["111", "222", "333"]

    def test_legacy_telegram_chat_id_fallback(self, monkeypatch):
        """TELEGRAM_CHAT_ID is accepted as fallback for admin_chat_id."""
        from src.notifier.telegram import get_telegram_config

        monkeypatch.setenv("TELEGRAM_CHAT_ID", "legacy_chat")
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("TELEGRAM_SUBSCRIBER_CHAT_IDS", raising=False)

        config = {"telegram": {"admin_chat_id": "", "subscriber_chat_ids": []}}

        result = get_telegram_config(config)

        assert result["admin_chat_id"] == "legacy_chat"
