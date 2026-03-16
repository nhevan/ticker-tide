"""
Macro data backfiller for the Stock Signal Engine.

Backfills sector ETFs, market benchmarks (SPY, QQQ), VIX, treasury yields,
and market holidays. Reuses backfill_ohlcv_for_ticker for anything that
stores data in ohlcv_daily, and uses dedicated logic for treasury_yields.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

from dateutil.relativedelta import relativedelta

from src.backfiller.ohlcv import backfill_ohlcv_for_ticker
from src.common import yfinance_client
from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)
logger = logging.getLogger(__name__)

# Mapping from Polygon's treasury maturity codes to DB column names
_TREASURY_FIELD_MAP = {
    "1M": "yield_1_month",
    "3M": "yield_3_month",
    "6M": "yield_6_month",
    "1Y": "yield_1_year",
    "2Y": "yield_2_year",
    "3Y": "yield_3_year",
    "5Y": "yield_5_year",
    "7Y": "yield_7_year",
    "10Y": "yield_10_year",
    "20Y": "yield_20_year",
    "30Y": "yield_30_year",
}


def backfill_sector_etfs(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    sector_etfs: list[str],
    lookback_years: int,
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Backfill OHLCV data for all sector ETFs.

    Reuses backfill_ohlcv_for_ticker since sector ETFs use the same Polygon
    aggregates endpoint as individual stocks. Tracks progress via ProgressTracker
    and optionally sends Telegram updates.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_ohlcv method.
        sector_etfs: List of ETF ticker symbols (e.g. ['XLK', 'XLF', ...]).
        lookback_years: Number of years of historical data to fetch.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with keys: processed (int), skipped (int), failed (int), total_rows (int).
    """
    tracker = ProgressTracker(phase="Backfill Sector ETFs", tickers=sector_etfs)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    processed = 0
    skipped = 0
    failed = 0
    total_rows = 0

    for etf in sector_etfs:
        tracker.mark_processing(etf)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        try:
            count = backfill_ohlcv_for_ticker(db_conn, polygon_client, etf, lookback_years)
            total_rows += count
            processed += 1
            tracker.mark_completed(etf, details=f"{count:,} rows")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn,
                etf,
                date.today().isoformat(),
                "backfiller",
                "error",
                f"Sector ETF backfill failed for ticker={etf}: {exc}",
            )
            tracker.mark_failed(etf, reason=str(exc))
            logger.error(f"Sector ETF backfill failed for ticker={etf}: {exc!r}")

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "total_rows": total_rows,
    }


def backfill_market_benchmarks(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    benchmarks: dict,
    lookback_years: int,
) -> dict:
    """
    Backfill OHLCV data for market benchmarks (e.g. SPY, QQQ).

    Iterates over symbol values in the benchmarks dict. VIX is handled
    separately via backfill_vix. Reuses backfill_ohlcv_for_ticker.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_ohlcv method.
        benchmarks: Dict mapping label to symbol, e.g. {'spy': 'SPY', 'qqq': 'QQQ'}.
                    The '^VIX' ticker is skipped here (handled by backfill_vix).
        lookback_years: Number of years of historical data to fetch.

    Returns:
        dict with keys: processed (int), skipped (int), failed (int), total_rows (int).
    """
    processed = 0
    skipped = 0
    failed = 0
    total_rows = 0

    for label, symbol in benchmarks.items():
        # VIX uses yfinance, not Polygon; skip it here
        if symbol == "^VIX":
            skipped += 1
            continue

        try:
            count = backfill_ohlcv_for_ticker(db_conn, polygon_client, symbol, lookback_years)
            total_rows += count
            processed += 1
            logger.info(f"Backfilled benchmark ticker={symbol}: {count:,} rows")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn,
                symbol,
                date.today().isoformat(),
                "backfiller",
                "error",
                f"Benchmark backfill failed for ticker={symbol}: {exc}",
            )
            logger.error(f"Benchmark backfill failed for ticker={symbol}: {exc!r}")

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "total_rows": total_rows,
    }


def backfill_vix(
    db_conn: sqlite3.Connection,
    from_date: str,
    to_date: str,
) -> int:
    """
    Fetch and store historical VIX data into ohlcv_daily with ticker='^VIX'.

    Uses yfinance_client.fetch_vix_data to retrieve VIX OHLCV bars. Each row
    is validated before insertion. vwap and num_transactions are set to None
    since VIX is an index, not a traded instrument.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and alerts_log tables.
        from_date: Start date in 'YYYY-MM-DD' format (inclusive).
        to_date: End date in 'YYYY-MM-DD' format (inclusive).

    Returns:
        Number of rows successfully inserted into ohlcv_daily.
    """
    logger.info(f"Starting VIX backfill from={from_date} to={to_date}")

    df = yfinance_client.fetch_vix_data(from_date, to_date)

    if df.empty:
        logger.warning("VIX data returned empty DataFrame")
        log_alert(
            db_conn,
            "^VIX",
            from_date,
            "backfiller",
            "warning",
            "VIX data returned empty DataFrame",
        )
        return 0

    count = 0
    for _, row_series in df.iterrows():
        row = {
            "ticker": "^VIX",
            "date": str(row_series["date"]),
            "open": row_series["open"],
            "high": row_series["high"],
            "low": row_series["low"],
            "close": row_series["close"],
            "volume": row_series["volume"],
            "vwap": None,
            "num_transactions": None,
        }

        # VIX is an index — volume is always 0, so we skip the standard OHLCV
        # validator (which requires volume > 0) and do a minimal price check.
        vix_date = row.get("date", "unknown")
        if not row["close"] or row["close"] <= 0:
            log_alert(
                db_conn,
                "^VIX",
                vix_date,
                "backfiller",
                "warning",
                f"Invalid VIX row date={vix_date}: close must be > 0",
            )
            continue

        db_conn.execute(
            """
            INSERT OR REPLACE INTO ohlcv_daily
                (ticker, date, open, high, low, close, volume, vwap, num_transactions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["ticker"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["vwap"],
                row["num_transactions"],
            ),
        )
        count += 1

    db_conn.commit()
    logger.info(f"Backfilled {count} VIX rows from={from_date} to={to_date}")
    return count


def backfill_treasury_yields(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    lookback_years: int,
) -> int:
    """
    Fetch and store historical US Treasury yield curve data.

    Fetches yield records from Polygon for all maturities (1M through 30Y).
    Missing maturity fields default to None. Uses INSERT OR REPLACE for
    idempotency since date is the primary key.

    Args:
        db_conn: Open SQLite connection with treasury_yields table.
        polygon_client: PolygonClient instance with a fetch_treasury_yields method.
        lookback_years: Number of years of historical data to fetch.

    Returns:
        Number of rows inserted into treasury_yields.
    """
    today = date.today()
    from_date = (today - relativedelta(years=lookback_years)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    logger.info(f"Starting treasury yields backfill from={from_date} to={to_date}")

    records = polygon_client.fetch_treasury_yields(from_date, to_date)

    if not records:
        logger.warning("No treasury yield data returned")
        return 0

    count = 0
    for record in records:
        record_date = record.get("date")
        if not record_date:
            logger.warning(f"Skipping treasury record with missing date: {record}")
            continue
        yield_1_month = record.get("1M")
        yield_3_month = record.get("3M")
        yield_6_month = record.get("6M")
        yield_1_year = record.get("1Y")
        yield_2_year = record.get("2Y")
        yield_3_year = record.get("3Y")
        yield_5_year = record.get("5Y")
        yield_7_year = record.get("7Y")
        yield_10_year = record.get("10Y")
        yield_20_year = record.get("20Y")
        yield_30_year = record.get("30Y")

        db_conn.execute(
            """
            INSERT OR REPLACE INTO treasury_yields
                (date, yield_1_month, yield_3_month, yield_6_month, yield_1_year,
                 yield_2_year, yield_3_year, yield_5_year, yield_7_year,
                 yield_10_year, yield_20_year, yield_30_year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_date,
                yield_1_month,
                yield_3_month,
                yield_6_month,
                yield_1_year,
                yield_2_year,
                yield_3_year,
                yield_5_year,
                yield_7_year,
                yield_10_year,
                yield_20_year,
                yield_30_year,
            ),
        )
        count += 1

    db_conn.commit()
    logger.info(f"Backfilled {count} treasury yield records")
    return count


def backfill_market_holidays(
    db_conn: sqlite3.Connection,
    polygon_client: object,
) -> list[str]:
    """
    Fetch upcoming market holidays and return only the fully-closed dates.

    Retrieves market holiday records from Polygon and filters to those with
    status='closed'. Holidays with status='early-close' are excluded.

    Args:
        db_conn: Open SQLite connection (unused directly, included for interface consistency).
        polygon_client: PolygonClient instance with a fetch_market_holidays method.

    Returns:
        List of date strings in 'YYYY-MM-DD' format for fully-closed market days.
    """
    holidays = polygon_client.fetch_market_holidays()
    closed_dates = [
        holiday["date"]
        for holiday in holidays
        if holiday.get("status") == "closed"
    ]
    logger.info(f"Fetched {len(closed_dates)} fully-closed market holiday dates")
    return closed_dates


def backfill_all_macro(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    config: dict,
    sector_etfs: list[str] = None,
    benchmarks: dict = None,
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Orchestrate all macro data backfill sub-tasks.

    Runs in order: sector ETFs → market benchmarks → VIX → treasury yields →
    market holidays. Sends Telegram section-header messages for each sub-task
    when bot_token and chat_id are provided.

    Args:
        db_conn: Open SQLite connection with all required tables.
        polygon_client: PolygonClient instance.
        config: Config dict; reads config['ohlcv']['lookback_years'] and
                config['macro']['treasury_lookback_years'].
        sector_etfs: List of sector ETF ticker symbols. Defaults to empty list.
        benchmarks: Dict mapping label to benchmark ticker symbol.
                    Defaults to {'spy': 'SPY', 'qqq': 'QQQ'}.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with per-sub-task row counts and summary totals.
    """
    if sector_etfs is None:
        sector_etfs = []
    if benchmarks is None:
        benchmarks = {"spy": "SPY", "qqq": "QQQ"}

    lookback_years = config.get("ohlcv", {}).get("lookback_years", 5)
    treasury_lookback_years = config.get("macro", {}).get("treasury_lookback_years", 5)

    today = date.today()
    from_date = (today - relativedelta(years=lookback_years)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    results: dict = {}

    # --- Sector ETFs ---
    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, "📊 Backfilling Sector ETFs...")
    etf_result = backfill_sector_etfs(
        db_conn, polygon_client, sector_etfs, lookback_years, bot_token, chat_id
    )
    results["sector_etfs"] = etf_result
    logger.info(f"Sector ETF backfill complete: {etf_result}")

    # --- Market benchmarks ---
    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, "📊 Backfilling Market Benchmarks (SPY, QQQ)...")
    benchmark_result = backfill_market_benchmarks(
        db_conn, polygon_client, benchmarks, lookback_years
    )
    results["benchmarks"] = benchmark_result
    logger.info(f"Benchmark backfill complete: {benchmark_result}")

    # --- VIX ---
    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, "📊 Backfilling VIX...")
    vix_rows = backfill_vix(db_conn, from_date, to_date)
    results["vix_rows"] = vix_rows
    logger.info(f"VIX backfill complete: {vix_rows} rows")

    # --- Treasury yields ---
    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, "📊 Backfilling Treasury Yields...")
    treasury_rows = backfill_treasury_yields(db_conn, polygon_client, treasury_lookback_years)
    results["treasury_rows"] = treasury_rows
    logger.info(f"Treasury yields backfill complete: {treasury_rows} rows")

    # --- Market holidays ---
    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, "📊 Fetching Market Holidays...")
    holiday_dates = backfill_market_holidays(db_conn, polygon_client)
    results["holiday_dates"] = holiday_dates
    logger.info(f"Market holidays fetched: {len(holiday_dates)} closed dates")

    total_ohlcv_rows = (
        etf_result["total_rows"]
        + benchmark_result["total_rows"]
        + vix_rows
    )
    results["total_ohlcv_rows"] = total_ohlcv_rows
    results["total_treasury_rows"] = treasury_rows

    if bot_token and chat_id:
        send_telegram_message(
            bot_token,
            chat_id,
            (
                f"✅ Macro Backfill Complete!\n"
                f"Sector ETFs: {etf_result['processed']} processed\n"
                f"Benchmarks: {benchmark_result['processed']} processed\n"
                f"VIX rows: {vix_rows:,}\n"
                f"Treasury rows: {treasury_rows:,}\n"
                f"Holidays: {len(holiday_dates)}"
            ),
        )

    return results
