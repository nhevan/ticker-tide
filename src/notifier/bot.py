"""
Telegram bot listener for interactive commands.

Runs as a long-polling bot that listens for commands:
  /detail AAPL [days]            — deep analysis for a ticker
  /scatter N [TICKER] [days_back] — confidence vs forward return scatter plot
  /help                          — list available commands

This runs as a SEPARATE process from the daily pipeline.
The daily pipeline sends messages directly via the API.
The bot listener handles interactive commands.

Uses python-telegram-bot library (async).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from src.common.config import get_active_tickers, load_config
from src.common.db import get_connection
from src.common.events import log_telegram_message
from src.notifier.detail_command import handle_detail_command, send_photo_to_chat
from src.notifier.scatter_command import handle_scatter_command
from src.notifier.tickers_command import handle_tickers_command

logger = logging.getLogger(__name__)


def _extract_command(message_text: str) -> str | None:
    """
    Return the leading slash-command from a message string, or None if absent.

    Extracts the first token if it starts with '/'. For example, '/detail AAPL 30'
    returns '/detail', and 'hello world' returns None.

    Parameters:
        message_text: Raw incoming message string.

    Returns:
        The command token (e.g. '/detail') or None.
    """
    first_token = message_text.strip().split()[0] if message_text.strip() else ""
    return first_token if first_token.startswith("/") else None


_HELP_TEXT = (
    "📋 Available Commands:\n"
    "  /tickers                    — list all watched tickers by sector\n"
    "  /detail AAPL                — deep analysis for a ticker (30 days)\n"
    "  /detail AAPL 90             — deep analysis with 90-day chart\n"
    "  /scatter 10                 — confidence vs 10-day return, all tickers\n"
    "  /scatter 5 AAPL             — confidence vs 5-day return for AAPL\n"
    "  /scatter 20 AAPL 180        — 20-day return, AAPL, last 180 days\n"
    "  /help                       — show this message"
)


async def handle_detail_command_wrapper(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Async wrapper that bridges python-telegram-bot with the synchronous detail handler.

    Extracts chat_id and the full message text (including the command), opens a fresh
    DB connection, loads configs, and delegates to handle_detail_command().

    Parameters:
        update: Telegram Update object.
        context: Telegram bot context (unused; args available via update).

    Returns:
        None
    """
    if update.message is None:
        return

    chat_id = str(update.message.chat_id)
    message_text = update.message.text or ""
    if not message_text.startswith("/detail"):
        # Reconstruct from command + args when telegram strips the command name
        args = context.args or []
        message_text = "/detail " + " ".join(args)

    received_at = datetime.now(tz=timezone.utc).isoformat()
    user = update.message.from_user
    user_id = str(user.id) if user else None
    username = user.username if user else None
    command = _extract_command(message_text)

    db_path = os.getenv("DB_PATH", "data/signals.db")
    log_conn = get_connection(db_path)
    try:
        log_telegram_message(log_conn, chat_id, user_id, username, command, message_text, received_at)
        logger.info("phase=bot chat_id=%s user=%s command=%s message logged", chat_id, username, command)
    except sqlite3.Error as exc:
        logger.warning("phase=bot failed to log telegram message: %s", exc)
    finally:
        log_conn.close()

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")

    notifier_config = load_config("notifier")
    calc_config = load_config("calculator")
    active_tickers = get_active_tickers()

    conn = get_connection(db_path)
    try:
        handle_detail_command(
            conn,
            chat_id,
            message_text,
            bot_token,
            notifier_config,
            active_tickers,
            calc_config,
        )
    except (ValueError, sqlite3.Error, OSError) as exc:
        logger.error("phase=bot handle_detail_command_wrapper unhandled error: %s", exc)
    finally:
        conn.close()


async def handle_scatter_command_wrapper(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Async wrapper that bridges python-telegram-bot with the synchronous scatter handler.

    Extracts chat_id and the full message text, opens a fresh DB connection,
    loads configs, and delegates to handle_scatter_command().

    Parameters:
        update: Telegram Update object.
        context: Telegram bot context (args available via update).

    Returns:
        None
    """
    if update.message is None:
        return

    chat_id = str(update.message.chat_id)
    message_text = update.message.text or ""
    if not message_text.startswith("/scatter"):
        args = context.args or []
        message_text = "/scatter " + " ".join(args)

    received_at = datetime.now(tz=timezone.utc).isoformat()
    user = update.message.from_user
    user_id = str(user.id) if user else None
    username = user.username if user else None
    command = _extract_command(message_text)

    db_path = os.getenv("DB_PATH", "data/signals.db")
    log_conn = get_connection(db_path)
    try:
        log_telegram_message(log_conn, chat_id, user_id, username, command, message_text, received_at)
        logger.info("phase=bot chat_id=%s user=%s command=%s message logged", chat_id, username, command)
    except sqlite3.Error as exc:
        logger.warning("phase=bot failed to log telegram message: %s", exc)
    finally:
        log_conn.close()

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    notifier_config = load_config("notifier")
    active_tickers = get_active_tickers()

    conn = get_connection(db_path)
    try:
        handle_scatter_command(
            conn,
            chat_id,
            message_text,
            bot_token,
            notifier_config,
            active_tickers,
        )
    except (ValueError, sqlite3.Error, OSError) as exc:
        logger.error("phase=bot handle_scatter_command_wrapper unhandled error: %s", exc)
    finally:
        conn.close()


async def handle_help_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Send the help message listing all available commands.

    Parameters:
        update: Telegram Update object.
        context: Telegram bot context (unused).

    Returns:
        None
    """
    if update.message is None:
        return

    received_at = datetime.now(tz=timezone.utc).isoformat()
    chat_id = str(update.message.chat_id)
    user = update.message.from_user
    user_id = str(user.id) if user else None
    username = user.username if user else None
    message_text = update.message.text or "/help"

    db_path = os.getenv("DB_PATH", "data/signals.db")
    log_conn = get_connection(db_path)
    try:
        log_telegram_message(log_conn, chat_id, user_id, username, "/help", message_text, received_at)
        logger.info("phase=bot chat_id=%s user=%s command=/help message logged", chat_id, username)
    except sqlite3.Error as exc:
        logger.warning("phase=bot failed to log telegram message: %s", exc)
    finally:
        log_conn.close()

    await update.message.reply_text(_HELP_TEXT)


def start_bot(config: dict) -> None:
    """
    Initialize and start the Telegram bot in long-polling mode.

    Registers /detail, /scatter, /tickers, and /help command handlers, then
    starts polling indefinitely. This function blocks until the process is
    interrupted.

    Parameters:
        config: Notifier config dict (used for logging; token is loaded
                from TELEGRAM_BOT_TOKEN environment variable).

    Returns:
        None
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is not set.")

    application = Application.builder().token(bot_token).build()

    application.add_handler(CommandHandler("detail", handle_detail_command_wrapper))
    application.add_handler(CommandHandler("scatter", handle_scatter_command_wrapper))
    application.add_handler(CommandHandler("tickers", handle_tickers_command))
    application.add_handler(CommandHandler("help", handle_help_command))

    logger.info("phase=bot Telegram bot started, listening for commands...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
