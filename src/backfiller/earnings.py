"""
Earnings calendar backfiller using yfinance.

Fetches historical and upcoming earnings dates, EPS estimates, and actuals.
Stores in the earnings_calendar table.

yfinance returns approximately 50 earnings events per ticker (announcement
dates, EPS estimate, reported EPS, surprise). No API key required.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)
from src.common.yfinance_client import fetch_earnings_dates

logger = logging.getLogger(__name__)


def convert_yfinance_to_earnings_row(record: dict) -> dict:
    """
    Convert a yfinance earnings record to our DB schema format.

    Accepts a dict already in the schema-compatible format returned by
    fetch_earnings_dates(), adds a fetched_at timestamp, and returns a
    complete row dict ready for INSERT into earnings_calendar.

    Args:
        record: Dict with keys: ticker, earnings_date, estimated_eps,
            actual_eps, eps_surprise, fiscal_quarter, fiscal_year,
            revenue_estimated, revenue_actual.

    Returns:
        dict: Row dict matching the earnings_calendar table schema,
            with fetched_at set to the current UTC ISO timestamp.
    """
    return {
        **record,
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def backfill_earnings_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
) -> int:
    """
    Fetch and store earnings calendar records for a single ticker via yfinance.

    Calls fetch_earnings_dates to retrieve all available earnings events,
    converts each to DB format, and inserts using INSERT OR REPLACE for
    idempotency. Raises on fetch failure so the caller can track per-ticker errors.

    Args:
        db_conn: Open SQLite connection with the earnings_calendar and alerts_log tables.
        ticker: Stock ticker symbol to backfill, e.g. 'AAPL'.

    Returns:
        int: Number of rows inserted.

    Raises:
        Exception: Re-raises any exception from fetch_earnings_dates after logging.
    """
    logger.info(f"Starting earnings backfill for ticker={ticker}")

    records = fetch_earnings_dates(ticker)

    count = 0
    for record in records:
        row = convert_yfinance_to_earnings_row(record)
        db_conn.execute(
            """
            INSERT OR REPLACE INTO earnings_calendar
                (ticker, earnings_date, fiscal_quarter, fiscal_year,
                 estimated_eps, actual_eps, eps_surprise,
                 revenue_estimated, revenue_actual, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["ticker"], row["earnings_date"], row["fiscal_quarter"],
                row["fiscal_year"], row["estimated_eps"], row["actual_eps"],
                row["eps_surprise"], row["revenue_estimated"], row["revenue_actual"],
                row["fetched_at"],
            ),
        )
        count += 1

    db_conn.commit()
    logger.info(f"Backfilled {count} earnings records for ticker={ticker}")
    return count


def backfill_all_earnings(
    db_conn: sqlite3.Connection,
    tickers: list[dict],
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Backfill earnings calendar data for all tickers in the provided list.

    Loops through tickers, calls backfill_earnings_for_ticker for each using
    yfinance as the data source, and optionally sends Telegram progress updates.
    Per-ticker failures are logged without stopping the run.

    Args:
        db_conn: Open SQLite connection with earnings_calendar and alerts_log tables.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with keys: processed (int), failed (int), total_rows (int).
    """
    from datetime import date
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    today = date.today().isoformat()

    tracker = ProgressTracker(phase="Backfill Earnings Calendar", tickers=ticker_symbols)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    processed = 0
    failed = 0
    total_rows = 0

    for ticker in ticker_symbols:
        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        try:
            count = backfill_earnings_for_ticker(db_conn, ticker)
            total_rows += count
            processed += 1
            tracker.mark_completed(ticker, details=f"{count} records")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, today,
                "backfiller", "error",
                f"Earnings backfill failed for ticker={ticker}: {exc}",
            )
            tracker.mark_failed(ticker, reason=str(exc))
            logger.error(f"Earnings backfill failed for ticker={ticker}: {exc!r}")

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    duration = (datetime.now(timezone.utc) - tracker.start_time).total_seconds()

    if bot_token and chat_id:
        send_telegram_message(
            bot_token, chat_id,
            tracker.format_final_summary(
                duration,
                extra_stats={"Total rows": f"{total_rows:,}"},
            ),
        )

    logger.info(
        f"Backfill Earnings Calendar complete: processed={processed} failed={failed} "
        f"total_rows={total_rows}"
    )
    return {"processed": processed, "failed": failed, "total_rows": total_rows}

