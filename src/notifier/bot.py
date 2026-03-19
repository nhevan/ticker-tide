"""
Telegram bot listener for interactive commands.

Runs as a long-polling bot that listens for commands:
  /detail AAPL [days] — deep analysis for a ticker
  /help               — list available commands

This runs as a SEPARATE process from the daily pipeline.
The daily pipeline sends messages directly via the API.
The bot listener handles interactive commands.

Uses python-telegram-bot library (async).
"""

from __future__ import annotations

import logging
import os
import sqlite3

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from src.common.config import get_active_tickers, load_config
from src.common.db import get_connection
from src.notifier.detail_command import handle_detail_command, send_photo_to_chat
from src.notifier.tickers_command import handle_tickers_command

logger = logging.getLogger(__name__)

_HELP_TEXT = (
    "📋 Available Commands:\n"
    "  /tickers          — list all watched tickers by sector\n"
    "  /detail AAPL      — deep analysis for a ticker (30 days)\n"
    "  /detail AAPL 90   — deep analysis with 90-day chart\n"
    "  /help             — show this message"
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

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    db_path = os.getenv("DB_PATH", "data/signals.db")

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
    await update.message.reply_text(_HELP_TEXT)


def start_bot(config: dict) -> None:
    """
    Initialize and start the Telegram bot in long-polling mode.

    Registers /detail and /help command handlers, then starts polling
    indefinitely. This function blocks until the process is interrupted.

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
    application.add_handler(CommandHandler("tickers", handle_tickers_command))
    application.add_handler(CommandHandler("help", handle_help_command))

    logger.info("phase=bot Telegram bot started, listening for commands...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
