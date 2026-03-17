"""
Fundamentals backfiller using yfinance.

Fetches quarterly and annual financial data for each ticker including
income statement, balance sheet, and key ratios. Stores in the
fundamentals table.

Used as a fallback since Polygon's financials endpoints are not
available on our plan (Starter tier).
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timezone

from src.backfiller.utils import _is_table_data_fresh
from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)
from src.common.yfinance_client import _safe_float, fetch_fundamentals_history

logger = logging.getLogger(__name__)


def compute_yoy_growth(
    current_value: float | None,
    prior_value: float | None,
) -> float | None:
    """
    Compute year-over-year growth rate: (current - prior) / abs(prior).

    Returns None if either value is None or prior is 0. Returns the growth
    as a decimal (e.g., 0.12 for 12% growth).

    Args:
        current_value: The current period's value (e.g., Q1 2025 revenue).
        prior_value: The same period's value one year ago (e.g., Q1 2024 revenue).

    Returns:
        float | None: Growth as a decimal fraction, or None if not computable.
    """
    if current_value is None or prior_value is None:
        return None
    if prior_value == 0:
        return None
    return (current_value - prior_value) / abs(prior_value)


def convert_yfinance_to_fundamentals_row(
    ticker: str,
    record: dict,
    prior_record: dict | None = None,
) -> dict:
    """
    Convert a yfinance fundamentals record to our DB schema format.

    Computes revenue_growth_yoy and eps_growth_yoy using the prior_record
    (same quarter, prior year). Maps all financial metric fields and sets
    fetched_at to the current UTC timestamp. Replaces NaN values with None.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        record: Single quarterly record dict from fetch_fundamentals_history,
            with keys: report_date, period, revenue, net_income, eps, and
            optionally pe_ratio, pb_ratio, ps_ratio, debt_to_equity,
            return_on_assets, return_on_equity, free_cash_flow, market_cap,
            dividend_yield.
        prior_record: Optional record for the same quarter one year earlier,
            used to compute YoY growth rates.

    Returns:
        dict: Row dict matching the fundamentals table schema, ready for INSERT.
    """
    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    revenue = _safe_float(record.get("revenue"))
    eps = _safe_float(record.get("eps"))

    prior_revenue = _safe_float(prior_record.get("revenue")) if prior_record else None
    prior_eps = _safe_float(prior_record.get("eps")) if prior_record else None

    return {
        "ticker": ticker,
        "report_date": record["report_date"],
        "period": record.get("period"),
        "revenue": revenue,
        "revenue_growth_yoy": compute_yoy_growth(revenue, prior_revenue),
        "net_income": _safe_float(record.get("net_income")),
        "eps": eps,
        "eps_growth_yoy": compute_yoy_growth(eps, prior_eps),
        "pe_ratio": _safe_float(record.get("pe_ratio")),
        "pb_ratio": _safe_float(record.get("pb_ratio")),
        "ps_ratio": _safe_float(record.get("ps_ratio")),
        "debt_to_equity": _safe_float(record.get("debt_to_equity")),
        "return_on_assets": _safe_float(record.get("return_on_assets")),
        "return_on_equity": _safe_float(record.get("return_on_equity")),
        "free_cash_flow": _safe_float(record.get("free_cash_flow")),
        "market_cap": _safe_float(record.get("market_cap")),
        "dividend_yield": _safe_float(record.get("dividend_yield")),
        "fetched_at": fetched_at,
    }


def backfill_fundamentals_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    lookback_years: int,
    config: dict | None = None,
    force: bool = False,
) -> int:
    """
    Fetch and store historical quarterly fundamentals for a single ticker.

    When force=False (default), skips the API call if data for this ticker in
    the fundamentals table was fetched within the threshold from
    config['skip_if_fresh_days']['fundamentals'] (default 30 days).

    Calls fetch_fundamentals_history to get all available quarterly records,
    matches each record with its same-quarter prior-year record (for YoY growth
    computation), converts each to DB format, and inserts using INSERT OR REPLACE
    for idempotency.

    Args:
        db_conn: Open SQLite connection with the fundamentals and alerts_log tables.
        ticker: Stock ticker symbol to backfill, e.g. 'AAPL'.
        lookback_years: Number of years of quarterly history to fetch.
        config: Optional backfiller config dict; reads
            config['skip_if_fresh_days']['fundamentals'] for the freshness threshold.
        force: When True, bypass staleness checks and always fetch.

    Returns:
        int: Number of rows successfully inserted into the fundamentals table.
            Returns 0 if no data is available, an error occurs, or skipped.
    """
    threshold = (config or {}).get("skip_if_fresh_days", {}).get("fundamentals", 30)
    if not force and _is_table_data_fresh(db_conn, "fundamentals", ticker, threshold):
        return 0

    today = date.today().isoformat()
    logger.info(f"Starting fundamentals backfill for ticker={ticker} lookback_years={lookback_years}")

    try:
        records = fetch_fundamentals_history(ticker, lookback_years)
    except Exception as exc:
        logger.error(f"fetch_fundamentals_history failed for ticker={ticker}: {exc!r}")
        log_alert(db_conn, ticker, today, "backfiller", "warning",
                  f"Fundamentals fetch failed for ticker={ticker}: {exc}")
        return 0

    if not records:
        logger.warning(f"No fundamentals data returned for ticker={ticker}")
        log_alert(db_conn, ticker, today, "backfiller", "warning",
                  f"No fundamentals data returned for ticker={ticker}")
        return 0

    # Build a lookup: {period: {year: record}} for prior-year matching
    records_by_period_year: dict[str, dict[int, dict]] = defaultdict(dict)
    for record in records:
        year = int(record["report_date"][:4])
        period = record.get("period", "")
        records_by_period_year[period][year] = record

    count = 0
    for record in records:
        year = int(record["report_date"][:4])
        period = record.get("period", "")
        prior_record = records_by_period_year[period].get(year - 1)

        row = convert_yfinance_to_fundamentals_row(ticker, record, prior_record=prior_record)

        db_conn.execute(
            """
            INSERT OR REPLACE INTO fundamentals
                (ticker, report_date, period, revenue, revenue_growth_yoy, net_income,
                 eps, eps_growth_yoy, pe_ratio, pb_ratio, ps_ratio, debt_to_equity,
                 return_on_assets, return_on_equity, free_cash_flow, market_cap,
                 dividend_yield, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["ticker"], row["report_date"], row["period"],
                row["revenue"], row["revenue_growth_yoy"], row["net_income"],
                row["eps"], row["eps_growth_yoy"],
                row["pe_ratio"], row["pb_ratio"], row["ps_ratio"],
                row["debt_to_equity"], row["return_on_assets"], row["return_on_equity"],
                row["free_cash_flow"], row["market_cap"], row["dividend_yield"],
                row["fetched_at"],
            ),
        )
        count += 1

    db_conn.commit()
    logger.info(f"Backfilled {count} fundamentals rows for ticker={ticker}")
    return count


def backfill_all_fundamentals(
    db_conn: sqlite3.Connection,
    tickers: list[dict],
    config: dict,
    bot_token: str = None,
    chat_id: str = None,
    force: bool = False,
) -> dict:
    """
    Backfill fundamentals for all tickers in the provided list.

    Iterates over all ticker configs, calls backfill_fundamentals_for_ticker for each,
    tracks progress via ProgressTracker, and optionally sends Telegram updates.
    Per-ticker failures are caught and logged without stopping the run.

    Args:
        db_conn: Open SQLite connection with fundamentals and alerts_log tables.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Config dict; reads config['fundamentals']['lookback_years'] (default 5).
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.
        force: When True, bypass staleness checks and always fetch.

    Returns:
        dict with keys: processed (int), failed (int), total_rows (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    lookback_years = config.get("fundamentals", {}).get("lookback_years", 5)

    tracker = ProgressTracker(phase="Backfill Fundamentals", tickers=ticker_symbols)
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
            count = backfill_fundamentals_for_ticker(db_conn, ticker, lookback_years, config=config, force=force)
            total_rows += count
            processed += 1
            tracker.mark_completed(ticker, details=f"{count} quarters")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, date.today().isoformat(),
                "backfiller", "error",
                f"Fundamentals backfill failed for ticker={ticker}: {exc}",
            )
            tracker.mark_failed(ticker, reason=str(exc))
            logger.error(f"Fundamentals backfill failed for ticker={ticker}: {exc!r}")

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
        f"Backfill Fundamentals complete: processed={processed} failed={failed} "
        f"total_rows={total_rows}"
    )
    return {"processed": processed, "failed": failed, "total_rows": total_rows}
