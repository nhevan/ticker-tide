"""
Telegram bot command handler for /tickers.

Lists all actively watched tickers grouped by sector, sorted alphabetically.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from telegram import Update
from telegram.ext import ContextTypes

from src.common.config import get_active_tickers

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

    logger.info("phase=bot command=/tickers chat_id=%s", update.message.chat_id)

    active_tickers = get_active_tickers()
    message = format_tickers_message(active_tickers)
    await update.message.reply_text(message)
