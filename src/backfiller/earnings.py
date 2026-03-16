"""
Earnings calendar backfiller using Finnhub.

Fetches historical and upcoming earnings dates, estimates, and actuals.
Stores in the earnings_calendar table.

Finnhub free tier: 60 calls/min — rate limiting is enforced by the FinnhubClient.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

from dateutil.relativedelta import relativedelta

from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

logger = logging.getLogger(__name__)


def convert_finnhub_to_earnings_row(record: dict) -> dict:
    """
    Convert a Finnhub earnings record to our DB schema format.

    Maps Finnhub field names to DB column names, computes eps_surprise when
    both actual and estimate are present, formats the fiscal quarter as 'Q{n}',
    and sets fetched_at to the current UTC timestamp.

    Args:
        record: Finnhub earnings dict with keys: symbol, date, epsActual,
            epsEstimate, revenueActual, revenueEstimate, quarter, year.

    Returns:
        dict: Row dict matching the earnings_calendar table schema, ready for INSERT.
    """
    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    actual_eps = record.get("epsActual")
    estimated_eps = record.get("epsEstimate")

    eps_surprise = None
    if actual_eps is not None and estimated_eps is not None:
        eps_surprise = actual_eps - estimated_eps

    quarter = record.get("quarter")
    fiscal_quarter = f"Q{quarter}" if quarter is not None else None

    return {
        "ticker": record.get("symbol"),
        "earnings_date": record.get("date"),
        "fiscal_quarter": fiscal_quarter,
        "fiscal_year": record.get("year"),
        "estimated_eps": estimated_eps,
        "actual_eps": actual_eps,
        "eps_surprise": eps_surprise,
        "revenue_estimated": record.get("revenueEstimate"),
        "revenue_actual": record.get("revenueActual"),
        "fetched_at": fetched_at,
    }


def backfill_earnings_for_ticker(
    db_conn: sqlite3.Connection,
    finnhub_client: object,
    ticker: str,
    from_date: str,
    to_date: str,
) -> int:
    """
    Fetch and store earnings calendar records for a single ticker.

    Calls finnhub_client.fetch_earnings_calendar, filters the results to only
    include records for the requested ticker, converts each to DB format, and
    inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the earnings_calendar and alerts_log tables.
        finnhub_client: FinnhubClient instance with a fetch_earnings_calendar method.
        ticker: Stock ticker symbol to backfill, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        int: Number of rows inserted. Returns 0 on error.
    """
    logger.info(f"Starting earnings backfill for ticker={ticker} from={from_date} to={to_date}")

    try:
        records = finnhub_client.fetch_earnings_calendar(ticker, from_date, to_date)
    except Exception as exc:
        logger.error(f"fetch_earnings_calendar failed for ticker={ticker}: {exc!r}")
        log_alert(db_conn, ticker, to_date, "backfiller", "warning",
                  f"Earnings fetch failed for ticker={ticker}: {exc}")
        return 0

    # Filter to only the requested ticker (Finnhub may return multiple tickers)
    ticker_records = [rec for rec in records if rec.get("symbol") == ticker]

    count = 0
    for record in ticker_records:
        row = convert_finnhub_to_earnings_row(record)
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
    finnhub_client: object,
    tickers: list[dict],
    config: dict,
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Backfill earnings calendar data for all tickers in the provided list.

    Calculates the date range from config (lookback_years), creates a ProgressTracker,
    loops through tickers, calls backfill_earnings_for_ticker for each, and optionally
    sends Telegram progress updates. Per-ticker failures are logged without stopping
    the run.

    Args:
        db_conn: Open SQLite connection with earnings_calendar and alerts_log tables.
        finnhub_client: FinnhubClient instance with rate-limited fetch_earnings_calendar.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Config dict; reads config['earnings']['lookback_years'] (default 2).
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with keys: processed (int), failed (int), total_rows (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    lookback_years = config.get("earnings", {}).get("lookback_years", 2)

    today = date.today()
    to_date = today.strftime("%Y-%m-%d")
    from_date = (today - relativedelta(years=lookback_years)).strftime("%Y-%m-%d")

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
            count = backfill_earnings_for_ticker(
                db_conn, finnhub_client, ticker, from_date, to_date
            )
            total_rows += count
            processed += 1
            tracker.mark_completed(ticker, details=f"{count} records")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, to_date,
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
