"""
Telegram notification sender for the Stock Signal Engine.

Sends formatted messages to configured Telegram chats using
send_telegram_message from src/common/progress.py.

Message routing:
  - Daily signal report  → all subscriber_chat_ids
  - Market closed notice → all subscriber_chat_ids
  - Pipeline heartbeat   → admin_chat_id only
  - Pipeline error alert → admin_chat_id only

Handles message splitting for long reports (Telegram 4096-char limit)
by accepting an already-split list[str] and sending each chunk separately
with a short delay to preserve ordering.

Chat ID configuration is loaded via get_telegram_config(), which checks
environment variables first and falls back to config/notifier.json values.
TELEGRAM_CHAT_ID is accepted as a backward-compatible alias for
TELEGRAM_ADMIN_CHAT_ID.
"""

from __future__ import annotations

import logging
import os
import time

from src.common.progress import send_telegram_message
from src.notifier.formatter import format_market_closed_message, format_pipeline_error_message

logger = logging.getLogger(__name__)

_INTER_MESSAGE_DELAY_SECONDS = 0.5


def get_telegram_config(config: dict) -> dict:
    """
    Load Telegram bot token and chat IDs from environment variables with config fallback.

    Precedence for admin_chat_id:
      1. TELEGRAM_ADMIN_CHAT_ID env var
      2. TELEGRAM_CHAT_ID env var (backward-compatible alias)
      3. config["telegram"]["admin_chat_id"]

    Precedence for subscriber_chat_ids:
      1. TELEGRAM_SUBSCRIBER_CHAT_IDS env var (comma-separated string → list)
      2. config["telegram"]["subscriber_chat_ids"] if non-empty
      3. [admin_chat_id] as sole subscriber (backward-compatible fallback)

    Parameters:
        config: Notifier config dict (from config/notifier.json).

    Returns:
        Dict with keys: bot_token (str), admin_chat_id (str),
        subscriber_chat_ids (list[str]).
    """
    telegram_cfg = config.get("telegram", {})

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")

    admin_chat_id = (
        os.getenv("TELEGRAM_ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or telegram_cfg.get("admin_chat_id", "")
    )

    raw_subscribers = os.getenv("TELEGRAM_SUBSCRIBER_CHAT_IDS")
    if raw_subscribers:
        subscriber_chat_ids = [s.strip() for s in raw_subscribers.split(",") if s.strip()]
    else:
        subscriber_chat_ids = list(telegram_cfg.get("subscriber_chat_ids", []))
        if not subscriber_chat_ids and admin_chat_id:
            subscriber_chat_ids = [admin_chat_id]

    return {
        "bot_token": bot_token,
        "admin_chat_id": admin_chat_id,
        "subscriber_chat_ids": subscriber_chat_ids,
    }


def send_daily_report(
    messages: list[str], bot_token: str, subscriber_chat_ids: list[str]
) -> dict:
    """
    Send the daily signal report to every subscriber.

    Each element of messages is sent as a separate Telegram message (with a
    short delay between sends) to every chat ID in subscriber_chat_ids.
    If a send to one subscriber fails, the function continues with the next
    subscriber and logs a warning.

    Parameters:
        messages: List of pre-formatted message strings (each ≤ 4096 chars).
        bot_token: Telegram Bot API token.
        subscriber_chat_ids: List of target chat/channel IDs.

    Returns:
        Dict with keys: sent (int), failed (int), total_subscribers (int).
    """
    if not subscriber_chat_ids:
        logger.warning("phase=notifier send_daily_report: No subscribers configured")
        return {"sent": 0, "failed": 0, "total_subscribers": 0}

    sent = 0
    failed = 0

    for chat_id in subscriber_chat_ids:
        all_sent_to_subscriber = True
        for index, text in enumerate(messages):
            result = send_telegram_message(bot_token, chat_id, text)
            if result is None:
                logger.warning(
                    f"phase=notifier send_daily_report: message {index + 1}/{len(messages)} "
                    f"failed for chat_id={chat_id}"
                )
                all_sent_to_subscriber = False
            else:
                logger.info(
                    f"phase=notifier send_daily_report: message {index + 1}/{len(messages)} "
                    f"sent to chat_id={chat_id} (id={result})"
                )
            if index < len(messages) - 1:
                time.sleep(_INTER_MESSAGE_DELAY_SECONDS)

        if all_sent_to_subscriber:
            sent += 1
        else:
            failed += 1

    return {"sent": sent, "failed": failed, "total_subscribers": len(subscriber_chat_ids)}


def send_heartbeat(heartbeat_text: str, bot_token: str, admin_chat_id: str) -> bool:
    """
    Send the pipeline heartbeat to the admin chat only.

    Parameters:
        heartbeat_text: Pre-formatted heartbeat string.
        bot_token: Telegram Bot API token.
        admin_chat_id: Admin chat or channel ID.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    result = send_telegram_message(bot_token, admin_chat_id, heartbeat_text)
    if result is None:
        logger.warning("phase=notifier send_heartbeat: heartbeat message failed")
        return False
    logger.info(f"phase=notifier send_heartbeat: sent (id={result})")
    return True


def send_market_closed_notification(
    date: str, bot_token: str, subscriber_chat_ids: list[str], config: dict
) -> dict:
    """
    Format and send the market-closed notification to all subscribers.

    Parameters:
        date: Trading date in YYYY-MM-DD format.
        bot_token: Telegram Bot API token.
        subscriber_chat_ids: List of subscriber chat/channel IDs.
        config: Notifier config dict (passed to formatter for consistency).

    Returns:
        Dict with keys: sent (int), failed (int).
    """
    text = format_market_closed_message(date, config)

    if not subscriber_chat_ids:
        logger.warning(
            f"phase=notifier send_market_closed_notification: No subscribers configured for date={date}"
        )
        return {"sent": 0, "failed": 0}

    sent = 0
    failed = 0

    for chat_id in subscriber_chat_ids:
        result = send_telegram_message(bot_token, chat_id, text)
        if result is None:
            logger.warning(
                f"phase=notifier send_market_closed_notification: send failed for date={date} chat_id={chat_id}"
            )
            failed += 1
        else:
            sent += 1

    return {"sent": sent, "failed": failed}


def send_pipeline_error_alert(
    phase: str,
    error: str,
    bot_token: str,
    admin_chat_id: str,
    config: dict,
) -> bool:
    """
    Format and send a pipeline failure alert to the admin chat only.

    Parameters:
        phase: Pipeline phase where the failure occurred (e.g. "fetcher").
        error: Error message or description.
        bot_token: Telegram Bot API token.
        admin_chat_id: Admin chat or channel ID.
        config: Notifier config dict.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    text = format_pipeline_error_message(phase, error, config)
    result = send_telegram_message(bot_token, admin_chat_id, text)
    if result is None:
        logger.warning(f"phase=notifier send_pipeline_error_alert: send failed for phase={phase}")
        return False
    return True
