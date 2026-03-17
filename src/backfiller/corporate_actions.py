"""
Corporate actions backfiller using Polygon.io.

Fetches dividends, stock splits, and short interest data.
Stores in dedicated tables: dividends, splits, short_interest.

Polygon is on paid tier — no rate limiting needed.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

logger = logging.getLogger(__name__)


def convert_polygon_dividend_to_row(record: dict) -> dict:
    """
    Map a Polygon dividend record to the dividends table schema.

    Args:
        record: Polygon dividend dict with keys: id, ticker, ex_dividend_date,
            pay_date, cash_amount, frequency.

    Returns:
        dict: Row dict matching the dividends table schema, ready for INSERT.
    """
    return {
        "id": record["id"],
        "ticker": record["ticker"],
        "ex_dividend_date": record.get("ex_dividend_date"),
        "pay_date": record.get("pay_date"),
        "cash_amount": record.get("cash_amount"),
        "frequency": record.get("frequency"),
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def convert_polygon_split_to_row(record: dict) -> dict:
    """
    Map a Polygon stock split record to the splits table schema.

    Args:
        record: Polygon split dict with keys: id, ticker, execution_date,
            split_from, split_to.

    Returns:
        dict: Row dict matching the splits table schema, ready for INSERT.
    """
    return {
        "id": record["id"],
        "ticker": record["ticker"],
        "execution_date": record.get("execution_date"),
        "split_from": record.get("split_from"),
        "split_to": record.get("split_to"),
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def convert_polygon_short_interest_to_row(record: dict) -> dict:
    """
    Map a Polygon short interest record to the short_interest table schema.

    Args:
        record: Polygon short interest dict with keys: ticker, settlement_date,
            short_interest, avg_daily_volume, days_to_cover.

    Returns:
        dict: Row dict matching the short_interest table schema, ready for INSERT.
    """
    return {
        "ticker": record["ticker"],
        "settlement_date": record["settlement_date"],
        "short_interest": record.get("short_interest"),
        "avg_daily_volume": record.get("avg_daily_volume"),
        "days_to_cover": record.get("days_to_cover"),
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def backfill_dividends_for_ticker(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
) -> int:
    """
    Fetch and store historical dividend records for a single ticker.

    Calls polygon_client.fetch_dividends(ticker), converts each record to DB
    format, and inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the dividends and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_dividends method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        int: Number of rows inserted. Returns 0 if no data or on error.
    """
    logger.info(f"Starting dividends backfill for ticker={ticker}")
    records = polygon_client.fetch_dividends(ticker)

    rows = [convert_polygon_dividend_to_row(record) for record in records]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO dividends
                (id, ticker, ex_dividend_date, pay_date, cash_amount, frequency, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (row["id"], row["ticker"], row["ex_dividend_date"],
                 row["pay_date"], row["cash_amount"], row["frequency"],
                 row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(f"Backfilled {len(rows)} dividend records for ticker={ticker}")
    return len(rows)


def backfill_splits_for_ticker(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
) -> int:
    """
    Fetch and store historical stock split records for a single ticker.

    Calls polygon_client.fetch_splits(ticker), converts each record to DB
    format, and inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the splits and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_splits method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        int: Number of rows inserted. Returns 0 if no data.
    """
    logger.info(f"Starting splits backfill for ticker={ticker}")
    records = polygon_client.fetch_splits(ticker)

    rows = [convert_polygon_split_to_row(record) for record in records]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO splits
                (id, ticker, execution_date, split_from, split_to, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (row["id"], row["ticker"], row["execution_date"],
                 row["split_from"], row["split_to"], row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(f"Backfilled {len(rows)} split records for ticker={ticker}")
    return len(rows)


def backfill_short_interest_for_ticker(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
) -> int:
    """
    Fetch and store short interest records for a single ticker.

    Calls polygon_client.fetch_short_interest(ticker), converts each record to
    DB format, and inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the short_interest and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_short_interest method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        int: Number of rows inserted. Returns 0 if no data.
    """
    logger.info(f"Starting short interest backfill for ticker={ticker}")
    records = polygon_client.fetch_short_interest(ticker)

    rows = [convert_polygon_short_interest_to_row(record) for record in records]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO short_interest
                (ticker, settlement_date, short_interest, avg_daily_volume,
                 days_to_cover, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (row["ticker"], row["settlement_date"], row["short_interest"],
                 row["avg_daily_volume"], row["days_to_cover"], row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(f"Backfilled {len(rows)} short interest records for ticker={ticker}")
    return len(rows)


def backfill_all_corporate_actions(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    tickers: list[dict],
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Backfill dividends, splits, and short interest for all tickers.

    For each ticker, attempts all three data types independently: if dividends
    fails, splits and short interest are still attempted. Progress is tracked
    via ProgressTracker and Telegram updates are sent if credentials are provided.

    Args:
        db_conn: Open SQLite connection with dividends, splits, short_interest,
            and alerts_log tables.
        polygon_client: PolygonClient instance with fetch_dividends, fetch_splits,
            and fetch_short_interest methods.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with keys: dividends_total (int), splits_total (int),
            short_interest_total (int), tickers_processed (int), tickers_failed (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    today = date.today().isoformat()

    tracker = ProgressTracker(phase="Backfill Corporate Actions", tickers=ticker_symbols)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    dividends_total = 0
    splits_total = 0
    short_interest_total = 0
    tickers_processed = 0
    tickers_failed = 0

    for ticker in ticker_symbols:
        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        ticker_had_error = False

        try:
            dividends_count = backfill_dividends_for_ticker(db_conn, polygon_client, ticker)
            dividends_total += dividends_count
        except Exception as exc:
            ticker_had_error = True
            log_alert(db_conn, ticker, today, "backfiller", "error",
                      f"Dividends backfill failed for ticker={ticker}: {exc}")
            logger.error(f"Dividends backfill failed for ticker={ticker}: {exc!r}")

        try:
            splits_count = backfill_splits_for_ticker(db_conn, polygon_client, ticker)
            splits_total += splits_count
        except Exception as exc:
            ticker_had_error = True
            log_alert(db_conn, ticker, today, "backfiller", "error",
                      f"Splits backfill failed for ticker={ticker}: {exc}")
            logger.error(f"Splits backfill failed for ticker={ticker}: {exc!r}")

        try:
            si_count = backfill_short_interest_for_ticker(db_conn, polygon_client, ticker)
            short_interest_total += si_count
        except Exception as exc:
            ticker_had_error = True
            log_alert(db_conn, ticker, today, "backfiller", "error",
                      f"Short interest backfill failed for ticker={ticker}: {exc}")
            logger.error(f"Short interest backfill failed for ticker={ticker}: {exc!r}")

        if ticker_had_error:
            tickers_failed += 1
            tracker.mark_failed(ticker)
        else:
            tickers_processed += 1
            tracker.mark_completed(ticker)

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    duration = (datetime.now(timezone.utc) - tracker.start_time).total_seconds()

    if bot_token and chat_id:
        send_telegram_message(
            bot_token, chat_id,
            tracker.format_final_summary(
                duration,
                extra_stats={
                    "Dividends": f"{dividends_total:,}",
                    "Splits": f"{splits_total:,}",
                    "Short interest": f"{short_interest_total:,}",
                },
            ),
        )

    logger.info(
        f"Backfill Corporate Actions complete: tickers_processed={tickers_processed} "
        f"tickers_failed={tickers_failed} dividends={dividends_total} "
        f"splits={splits_total} short_interest={short_interest_total}"
    )
    return {
        "dividends_total": dividends_total,
        "splits_total": splits_total,
        "short_interest_total": short_interest_total,
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
    }
