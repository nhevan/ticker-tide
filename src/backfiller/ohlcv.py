"""
OHLCV backfiller for the Stock Signal Engine.

Fetches 5 years of daily OHLCV bars from Polygon for all active tickers
and populates the ohlcv_daily table. Tracks per-ticker progress via
ProgressTracker and optionally sends updates to Telegram.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone

from dateutil.relativedelta import relativedelta

from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)
from src.common.validators import validate_ohlcv_row

logger = logging.getLogger(__name__)


def convert_polygon_timestamp_to_date(timestamp_ms: int) -> str:
    """
    Convert a Polygon Unix millisecond timestamp to a YYYY-MM-DD date string.

    Polygon timestamps represent the start of a trading day in UTC
    (e.g., midnight Eastern = 05:00 UTC). Taking the UTC date from the
    timestamp gives the correct Eastern trading date.

    Args:
        timestamp_ms: Unix timestamp in milliseconds (UTC).

    Returns:
        Date string in 'YYYY-MM-DD' format.
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def convert_polygon_bar_to_ohlcv_row(ticker: str, bar: dict) -> dict:
    """
    Map a Polygon OHLCV bar dict to the ohlcv_daily DB row format.

    Polygon field names (o, h, l, c, v, vw, t, n) are translated to their
    corresponding database column names. The timestamp field 't' is converted
    to a YYYY-MM-DD date string.

    Args:
        ticker: Ticker symbol (e.g. 'AAPL').
        bar: Polygon bar dict with keys: o, h, l, c, v, vw, t, n.

    Returns:
        dict with keys: ticker, date, open, high, low, close, volume, vwap, num_transactions.
    """
    return {
        "ticker": ticker,
        "date": convert_polygon_timestamp_to_date(bar["t"]),
        "open": bar["o"],
        "high": bar["h"],
        "low": bar["l"],
        "close": bar["c"],
        "volume": bar["v"],
        "vwap": bar.get("vw"),
        "num_transactions": bar.get("n"),
    }


def backfill_ohlcv_for_ticker(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
    lookback_years: int,
    former_symbol: str | None = None,
    symbol_since: str | None = None,
    force: bool = False,
) -> int:
    """
    Fetch and store historical OHLCV data for a single ticker.

    When force=False (default), staleness checks avoid redundant API calls:
      - If the most recent date in ohlcv_daily for this ticker is >= yesterday,
        the data is considered current and 0 is returned immediately.
      - If data exists but is older, from_date is set to max_date + 1 day so
        only missing bars are fetched (incremental update).
      - If no data exists, the full lookback range is used.

    When force=True, the full lookback range is always used regardless of what
    is already in the database.

    When a ticker has a known historical name change (e.g. FB → META), supply
    former_symbol and symbol_since. If symbol_since falls within the computed
    from_date–to_date range, two Polygon fetches are made:
      - current ticker: symbol_since → to_date
      - former ticker:  from_date → day before symbol_since
    Both sets of rows are stored under the current ticker symbol.

    If symbol_since is before from_date (the change predates the fetch window),
    all available history is already under the current ticker and a single fetch
    is performed.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_ohlcv method.
        ticker: Ticker symbol to backfill (e.g. 'META').
        lookback_years: Number of years of historical data to fetch.
        former_symbol: Optional prior ticker symbol (e.g. 'FB'). Used only when
            the ticker was renamed within the fetch window.
        symbol_since: ISO date string (YYYY-MM-DD) on which the current ticker
            symbol became active. Required when former_symbol is provided.
        force: When True, bypass staleness checks and re-fetch from full
            lookback. Defaults to False.

    Returns:
        Number of rows successfully inserted into ohlcv_daily.
    """
    today = date.today()
    to_date = today.strftime("%Y-%m-%d")

    if not force:
        max_date_row = db_conn.execute(
            "SELECT MAX(date) AS max_date FROM ohlcv_daily WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        max_date = max_date_row["max_date"] if max_date_row else None

        if max_date is not None:
            today_minus_one = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            if max_date >= today_minus_one:
                logger.info(
                    f"Skipping OHLCV for ticker={ticker}: data is fresh (max_date={max_date})"
                )
                return 0
            from_date = (
                date.fromisoformat(max_date) + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        else:
            from_date = (today - relativedelta(years=lookback_years)).strftime("%Y-%m-%d")
    else:
        from_date = (today - relativedelta(years=lookback_years)).strftime("%Y-%m-%d")

    logger.info(
        f"Starting OHLCV backfill for ticker={ticker} from={from_date} to={to_date}"
    )

    use_split_fetch = (
        former_symbol is not None
        and symbol_since is not None
        and symbol_since >= from_date
    )

    if use_split_fetch:
        symbol_since_date = date.fromisoformat(symbol_since)
        day_before = (symbol_since_date - timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(
            f"Ticker {ticker} was formerly {former_symbol}; "
            f"fetching {ticker} from {symbol_since} and {former_symbol} from {from_date} to {day_before}"
        )

        current_bars = polygon_client.fetch_ohlcv(ticker, symbol_since, to_date)
        former_bars = polygon_client.fetch_ohlcv(former_symbol, from_date, day_before)
        all_bars_with_ticker = (
            [(ticker, bar) for bar in (current_bars or [])]
            + [(ticker, bar) for bar in (former_bars or [])]
        )
    else:
        bars = polygon_client.fetch_ohlcv(ticker, from_date, to_date)
        all_bars_with_ticker = [(ticker, bar) for bar in (bars or [])]

    if not all_bars_with_ticker:
        logger.warning(f"No OHLCV data returned for ticker={ticker}")
        log_alert(
            db_conn,
            ticker,
            to_date,
            "backfiller",
            "warning",
            f"No OHLCV data returned for ticker={ticker}",
        )
        return 0

    count = 0
    for store_as_ticker, bar in all_bars_with_ticker:
        row = convert_polygon_bar_to_ohlcv_row(store_as_ticker, bar)
        is_valid, reasons = validate_ohlcv_row(row)

        if not is_valid:
            for reason in reasons:
                log_alert(
                    db_conn,
                    store_as_ticker,
                    row.get("date", "unknown"),
                    "backfiller",
                    "warning",
                    f"Invalid OHLCV row for ticker={store_as_ticker} date={row.get('date','unknown')}: {reason}",
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
    logger.info(f"Backfilled {count} days for ticker={ticker}")
    return count


def backfill_all_tickers(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    tickers: list[dict],
    config: dict,
    bot_token: str = None,
    chat_id: str = None,
    force: bool = False,
) -> dict:
    """
    Backfill OHLCV data for all tickers in the provided list.

    Iterates over all ticker configs, calls backfill_ohlcv_for_ticker for each,
    tracks progress via ProgressTracker, and optionally sends real-time updates
    to Telegram. Catches and logs per-ticker failures without stopping the run.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_ohlcv method.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Config dict; reads config['ohlcv']['lookback_years'] (default 5).
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.
        force: When True, bypass staleness checks and re-fetch all data.

    Returns:
        dict with keys: processed (int), skipped (int), failed (int), total_rows (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    lookback_years = config.get("ohlcv", {}).get("lookback_years", 5)

    tracker = ProgressTracker(phase="Backfill OHLCV", tickers=ticker_symbols)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    processed = 0
    skipped = 0
    failed = 0
    total_rows = 0

    for ticker_config in tickers:
        ticker = ticker_config["symbol"]
        former_symbol = ticker_config.get("former_symbol")
        symbol_since = ticker_config.get("symbol_since")

        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        try:
            count = backfill_ohlcv_for_ticker(
                db_conn, polygon_client, ticker, lookback_years,
                former_symbol=former_symbol,
                symbol_since=symbol_since,
                force=force,
            )
            total_rows += count
            processed += 1
            tracker.mark_completed(ticker, details=f"{count:,} rows")
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn,
                ticker,
                date.today().isoformat(),
                "backfiller",
                "error",
                f"Backfill failed for ticker={ticker}: {exc}",
            )
            tracker.mark_failed(ticker, reason=str(exc))
            logger.error(f"Backfill failed for ticker={ticker}: {exc!r}")

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    duration = (datetime.now(timezone.utc) - tracker.start_time).total_seconds()

    if bot_token and chat_id:
        send_telegram_message(
            bot_token,
            chat_id,
            tracker.format_final_summary(
                duration,
                extra_stats={"Total rows": f"{total_rows:,}"},
            ),
        )

    logger.info(
        f"Backfill OHLCV complete: processed={processed} skipped={skipped} "
        f"failed={failed} total_rows={total_rows}"
    )

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "total_rows": total_rows,
    }
