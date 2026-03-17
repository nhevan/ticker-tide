"""
Periodic earnings calendar refresh using yfinance.

Refreshes earnings dates, EPS estimates, and actuals for all active tickers.
Respects the earnings_calendar_days interval from fetcher.json to avoid
re-fetching recently updated data.

yfinance returns approximately 50 earnings events per ticker (announcement
dates, EPS estimate, reported EPS, surprise). No API key required.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

from src.common.events import log_alert
from src.common.yfinance_client import fetch_earnings_dates

logger = logging.getLogger(__name__)


def _is_earnings_stale(
    db_conn: sqlite3.Connection,
    ticker: str,
    refresh_days: int,
) -> bool:
    """
    Check whether the earnings data for a ticker needs refreshing.

    Looks at the most recent fetched_at timestamp in earnings_calendar for
    the given ticker. Returns True if no data exists or the newest record
    is older than refresh_days.

    Args:
        db_conn: Open SQLite connection with the earnings_calendar table.
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        refresh_days: Number of days after which data is considered stale.

    Returns:
        bool: True if the data should be refreshed, False if still fresh.
    """
    row = db_conn.execute(
        "SELECT MAX(fetched_at) AS last_fetched FROM earnings_calendar WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    if row is None or row["last_fetched"] is None:
        return True

    try:
        last_fetched_dt = datetime.fromisoformat(row["last_fetched"])
        # Ensure timezone-aware comparison
        if last_fetched_dt.tzinfo is None:
            last_fetched_dt = last_fetched_dt.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(tz=timezone.utc)
        age_days = (now_utc - last_fetched_dt).days
        return age_days >= refresh_days
    except (ValueError, TypeError):
        return True


def refresh_earnings_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
) -> int:
    """
    Fetch and store the latest earnings calendar records for a single ticker.

    Calls fetch_earnings_dates from yfinance_client and upserts all returned
    records into earnings_calendar using INSERT OR REPLACE for idempotency.
    Raises on fetch failure so the caller can track per-ticker errors.

    Args:
        db_conn: Open SQLite connection with the earnings_calendar and alerts_log tables.
        ticker: Stock ticker symbol to refresh, e.g. 'AAPL'.

    Returns:
        int: Number of rows upserted.

    Raises:
        Exception: Re-raises any exception from fetch_earnings_dates after logging.
    """
    logger.info(f"Refreshing earnings calendar for ticker={ticker}")

    records = fetch_earnings_dates(ticker)

    fetched_at = datetime.now(tz=timezone.utc).isoformat()
    count = 0

    for record in records:
        db_conn.execute(
            """
            INSERT OR REPLACE INTO earnings_calendar
                (ticker, earnings_date, fiscal_quarter, fiscal_year,
                 estimated_eps, actual_eps, eps_surprise,
                 revenue_estimated, revenue_actual, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["ticker"], record["earnings_date"], record["fiscal_quarter"],
                record["fiscal_year"], record["estimated_eps"], record["actual_eps"],
                record["eps_surprise"], record["revenue_estimated"],
                record["revenue_actual"], fetched_at,
            ),
        )
        count += 1

    db_conn.commit()
    logger.info(f"Refreshed {count} earnings records for ticker={ticker}")
    return count


def run_periodic_earnings(
    db_conn: sqlite3.Connection,
    tickers: list[dict],
    config: dict,
) -> dict:
    """
    Periodically refresh earnings calendar data for all tickers.

    Reads earnings_calendar_days from config to determine the staleness
    threshold. Skips tickers whose data was fetched more recently than
    that threshold. Per-ticker failures are logged without stopping the run.

    Args:
        db_conn: Open SQLite connection with earnings_calendar and alerts_log tables.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Fetcher config dict. Reads
            config['polling_intervals']['earnings_calendar_days'] (default 7).

    Returns:
        dict with keys: refreshed (int), skipped (int), failed (int), total_rows (int).
    """
    refresh_days = (
        config
        .get("polling_intervals", {})
        .get("earnings_calendar_days", 7)
    )
    today = date.today().isoformat()

    refreshed = 0
    skipped = 0
    failed = 0
    total_rows = 0

    for ticker_config in tickers:
        ticker = ticker_config["symbol"]

        if not _is_earnings_stale(db_conn, ticker, refresh_days):
            logger.info(f"Skipping earnings refresh for ticker={ticker} (data is fresh)")
            skipped += 1
            continue

        try:
            count = refresh_earnings_for_ticker(db_conn, ticker)
            total_rows += count
            refreshed += 1
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, today, "fetcher", "error",
                f"Periodic earnings refresh failed for ticker={ticker}: {exc}",
            )
            logger.error(
                f"Periodic earnings refresh failed for ticker={ticker}: {exc!r}"
            )

    logger.info(
        f"Periodic earnings refresh complete: refreshed={refreshed} "
        f"skipped={skipped} failed={failed} total_rows={total_rows}"
    )
    return {"refreshed": refreshed, "skipped": skipped, "failed": failed, "total_rows": total_rows}
