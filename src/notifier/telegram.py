"""
Telegram notification sender for the Stock Signal Engine.

Sends formatted messages to the configured Telegram chat using
send_telegram_message from src/common/progress.py.

Handles message splitting for long reports (Telegram 4096-char limit)
by accepting an already-split list[str] and sending each chunk separately
with a short delay to preserve ordering.
"""

from __future__ import annotations

import logging
import time

from src.common.progress import send_telegram_message
from src.notifier.formatter import format_market_closed_message, format_pipeline_error_message

logger = logging.getLogger(__name__)

_INTER_MESSAGE_DELAY_SECONDS = 0.5


def send_daily_report(messages: list[str], bot_token: str, chat_id: str) -> bool:
    """
    Send the daily signal report as one or more Telegram messages.

    Each element of messages is sent as a separate Telegram message with a
    short delay between sends to maintain ordering. If any send fails the
    function still attempts the remaining messages but returns False.

    Parameters:
        messages: List of pre-formatted message strings (each ≤ 4096 chars).
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.

    Returns:
        True if all messages were sent successfully, False if any failed.
    """
    all_sent = True
    for index, text in enumerate(messages):
        result = send_telegram_message(bot_token, chat_id, text)
        if result is None:
            logger.warning(
                f"phase=notifier send_daily_report: message {index + 1}/{len(messages)} failed"
            )
            all_sent = False
        else:
            logger.info(
                f"phase=notifier send_daily_report: message {index + 1}/{len(messages)} sent (id={result})"
            )
        if index < len(messages) - 1:
            time.sleep(_INTER_MESSAGE_DELAY_SECONDS)
    return all_sent


def send_heartbeat(heartbeat_text: str, bot_token: str, chat_id: str) -> bool:
    """
    Send the pipeline heartbeat as a separate Telegram message.

    Parameters:
        heartbeat_text: Pre-formatted heartbeat string.
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    result = send_telegram_message(bot_token, chat_id, heartbeat_text)
    if result is None:
        logger.warning("phase=notifier send_heartbeat: heartbeat message failed")
        return False
    logger.info(f"phase=notifier send_heartbeat: sent (id={result})")
    return True


def send_market_closed_notification(date: str, bot_token: str, chat_id: str, config: dict) -> bool:
    """
    Format and send the market-closed notification message.

    Parameters:
        date: Trading date in YYYY-MM-DD format.
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.
        config: Notifier config dict (passed to formatter for consistency).

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    text = format_market_closed_message(date, config)
    result = send_telegram_message(bot_token, chat_id, text)
    if result is None:
        logger.warning(f"phase=notifier send_market_closed_notification: send failed for date={date}")
        return False
    return True


def send_pipeline_error_alert(
    phase: str,
    error: str,
    bot_token: str,
    chat_id: str,
    config: dict,
) -> bool:
    """
    Format and send a pipeline failure alert message.

    Parameters:
        phase: Pipeline phase where the failure occurred (e.g. "fetcher").
        error: Error message or description.
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.
        config: Notifier config dict.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    text = format_pipeline_error_message(phase, error, config)
    result = send_telegram_message(bot_token, chat_id, text)
    if result is None:
        logger.warning(f"phase=notifier send_pipeline_error_alert: send failed for phase={phase}")
        return False
    return True
