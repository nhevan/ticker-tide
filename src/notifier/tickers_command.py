"""
Telegram bot command handler for /tickers.

Lists all actively watched tickers grouped by sector, sorted alphabetically.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from src.common.config import get_active_tickers
from src.common.db import get_connection
from src.common.events import log_telegram_message

logger = logging.getLogger(__name__)


def format_tickers_message(tickers: list[dict]) -> str:
    """
    Format a human-readable Telegram message listing active tickers by sector.

    Filters to active tickers only, groups them by sector (sorted A→Z),
    and sorts symbols alphabetically within each sector.

    Parameters:
        tickers: List of ticker dicts, each with keys: symbol, sector, active.

    Returns:
        str: Formatted message ready to send via Telegram.
    """
    active = [t for t in tickers if t.get("active")]

    if not active:
        return "📊 Watching 0 tickers — no active tickers configured."

    by_sector: dict[str, list[str]] = defaultdict(list)
    for ticker in active:
        by_sector[ticker["sector"]].append(ticker["symbol"])

    lines: list[str] = [
        f"📊 Watching {len(active)} tickers across {len(by_sector)} sectors\n"
    ]

    for sector in sorted(by_sector):
        symbols = sorted(by_sector[sector])
        lines.append(f"{sector} ({len(symbols)})")
        lines.append("  " + " · ".join(symbols))
        lines.append("")

    return "\n".join(lines).rstrip()


async def handle_tickers_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Handle the /tickers Telegram command.

    Loads the active ticker list, formats it by sector, and replies
    to the user who issued the command.

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
    message_text = update.message.text or "/tickers"

    logger.info("phase=bot command=/tickers chat_id=%s", chat_id)

    db_path = os.getenv("DB_PATH", "data/signals.db")
    log_conn = get_connection(db_path)
    try:
        log_telegram_message(log_conn, chat_id, user_id, username, "/tickers", message_text, received_at)
    except sqlite3.Error as exc:
        logger.warning("phase=bot failed to log telegram message: %s", exc)
    finally:
        log_conn.close()

    active_tickers = get_active_tickers()
    message = format_tickers_message(active_tickers)
    await update.message.reply_text(message)
