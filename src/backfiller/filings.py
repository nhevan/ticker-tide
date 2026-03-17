"""
8-K filings backfiller using Polygon.io.

Fetches SEC 8-K filings for each ticker and stores them in the filings_8k table.
8-K filings cover material events such as earnings announcements, M&A, and leadership changes.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

from src.backfiller.utils import _is_table_data_fresh
from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

logger = logging.getLogger(__name__)


def convert_polygon_filing_to_row(filing: dict) -> dict:
    """
    Map a Polygon 8-K filing dict to the filings_8k table schema.

    Args:
        filing: Polygon filing dict with keys: accession_number, ticker,
            filing_date, form_type, items_text, filing_url.

    Returns:
        dict: Row dict matching the filings_8k table schema, ready for INSERT.
    """
    return {
        "accession_number": filing["accession_number"],
        "ticker": filing.get("ticker"),
        "filing_date": filing.get("filing_date"),
        "form_type": filing.get("form_type"),
        "items_text": filing.get("items_text"),
        "filing_url": filing.get("filing_url"),
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def backfill_8k_for_ticker(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
    from_date: str,
    to_date: str,
    config: dict | None = None,
    force: bool = False,
) -> int:
    """
    Fetch and store 8-K filing records for a single ticker.

    When force=False (default), skips the API call if data for this ticker in
    filings_8k was fetched within config['skip_if_fresh_days']['filings'] days
    (default 7).

    Calls polygon_client.fetch_8k_filings(ticker, from_date, to_date), converts
    each filing to DB format, and inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the filings_8k and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_8k_filings method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        from_date: Start filing date in 'YYYY-MM-DD' format (inclusive).
        to_date: End filing date in 'YYYY-MM-DD' format (inclusive).
        config: Optional backfiller config dict for the freshness threshold.
        force: When True, bypass staleness checks and always fetch.

    Returns:
        int: Number of rows inserted. Returns 0 if no data or skipped.
    """
    threshold = (config or {}).get("skip_if_fresh_days", {}).get("filings", 7)
    if not force and _is_table_data_fresh(db_conn, "filings_8k", ticker, threshold):
        return 0

    logger.info(
        f"Starting 8-K filings backfill for ticker={ticker} from={from_date} to={to_date}"
    )
    filings = polygon_client.fetch_8k_filings(ticker, from_date, to_date)

    rows = [convert_polygon_filing_to_row(filing) for filing in filings]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO filings_8k
                (accession_number, ticker, filing_date, form_type, items_text,
                 filing_url, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (row["accession_number"], row["ticker"], row["filing_date"],
                 row["form_type"], row["items_text"], row["filing_url"],
                 row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(f"Backfilled {len(rows)} 8-K filings for ticker={ticker}")
    return len(rows)


def backfill_all_filings(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    tickers: list[dict],
    config: dict,
    bot_token: str = None,
    chat_id: str = None,
    force: bool = False,
) -> dict:
    """
    Backfill 8-K filings for all tickers.

    For each ticker, fetches filings within the configured lookback window. Per-ticker
    failures are logged without stopping the run. Progress is tracked via ProgressTracker
    and Telegram updates sent if credentials are provided.

    Args:
        db_conn: Open SQLite connection with the filings_8k and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_8k_filings method.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Backfiller config dict containing the filings section.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.
        force: When True, bypass staleness checks and always fetch.

    Returns:
        dict with keys: filings_total (int), tickers_processed (int), tickers_failed (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    today = date.today()
    today_str = today.isoformat()

    lookback_months = config["filings"]["lookback_months"]
    from_date = (today - relativedelta(months=lookback_months)).isoformat()

    tracker = ProgressTracker(phase="Backfill 8-K Filings", tickers=ticker_symbols)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    filings_total = 0
    tickers_processed = 0
    tickers_failed = 0

    for ticker in ticker_symbols:
        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        try:
            count = backfill_8k_for_ticker(db_conn, polygon_client, ticker, from_date, today_str, config=config, force=force)
            filings_total += count
            tickers_processed += 1
            tracker.mark_completed(ticker)
        except Exception as exc:
            tickers_failed += 1
            tracker.mark_failed(ticker)
            log_alert(
                db_conn, ticker, today_str, "backfiller", "error",
                f"8-K filings backfill failed for ticker={ticker}: {exc}",
            )
            logger.error(
                f"8-K filings backfill failed for ticker={ticker}: {exc!r}"
            )

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    duration = (datetime.now(timezone.utc) - tracker.start_time).total_seconds()

    if bot_token and chat_id:
        send_telegram_message(
            bot_token, chat_id,
            tracker.format_final_summary(
                duration,
                extra_stats={"8-K filings": f"{filings_total:,}"},
            ),
        )

    logger.info(
        f"Backfill 8-K Filings complete: tickers_processed={tickers_processed} "
        f"tickers_failed={tickers_failed} filings_total={filings_total}"
    )
    return {
        "filings_total": filings_total,
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
    }
