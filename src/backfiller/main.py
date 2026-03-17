"""
Backfill orchestrator for the Stock Signal Engine.

Provides sync_tickers_from_config to synchronise the tickers table with tickers.json,
and run_full_backfill to execute all backfill phases in the correct order.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from src.backfiller.corporate_actions import backfill_all_corporate_actions
from src.backfiller.earnings import backfill_all_earnings
from src.backfiller.filings import backfill_all_filings
from src.backfiller.fundamentals import backfill_all_fundamentals
from src.backfiller.macro import backfill_all_macro
from src.backfiller.news import backfill_all_news
from src.backfiller.ohlcv import backfill_all_tickers
from src.common.api_client import FinnhubClient, PolygonClient
from src.common.config import (
    get_active_tickers,
    get_market_benchmarks,
    get_sector_etfs,
    load_config,
    load_env,
)
from src.common.db import create_all_tables, get_connection
from src.common.events import log_alert, log_pipeline_run
from src.common.progress import send_telegram_message

logger = logging.getLogger(__name__)


def sync_tickers_from_config(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    bot_token: str = None,
    chat_id: str = None,
) -> list[str]:
    """
    Synchronise the tickers table with the active tickers in tickers.json.

    For each active ticker in config:
    - If new: insert with added_date=today and all available fields.
    - If existing: update name, sic_code, sic_description, market_cap, sector,
      sector_etf, active, and updated_at (preserving added_date).

    Also deactivates (sets active=0) any DB tickers that are no longer in the
    active config. Sends a Telegram summary if credentials are provided.

    Args:
        db_conn: Open SQLite connection with the tickers table.
        polygon_client: PolygonClient instance with a fetch_ticker_details method.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        list[str]: List of newly added ticker symbols not previously in the DB.
    """
    active_tickers = get_active_tickers()
    today = date.today().isoformat()
    now_utc = datetime.now(tz=timezone.utc).isoformat()

    config_symbols = {ticker["symbol"] for ticker in active_tickers}

    existing_rows = db_conn.execute(
        "SELECT symbol FROM tickers"
    ).fetchall()
    db_symbols = {row["symbol"] for row in existing_rows}

    new_symbols: list[str] = []

    for ticker_config in active_tickers:
        symbol = ticker_config["symbol"]
        sector = ticker_config.get("sector")
        sector_etf = ticker_config.get("sector_etf")
        added_date = ticker_config.get("added", today)

        logger.info(f"Syncing ticker={symbol}")

        details = polygon_client.fetch_ticker_details(symbol)
        name = details.get("name") if details else None
        sic_code = details.get("sic_code") if details else None
        sic_description = details.get("sic_description") if details else None
        market_cap = details.get("market_cap") if details else None

        # Insert on first occurrence only (preserves added_date)
        db_conn.execute(
            """
            INSERT OR IGNORE INTO tickers (symbol, sector, sector_etf, added_date)
            VALUES (?, ?, ?, ?)
            """,
            (symbol, sector, sector_etf, added_date),
        )

        # Update all mutable fields
        db_conn.execute(
            """
            UPDATE tickers
            SET name=?, sic_code=?, sic_description=?, market_cap=?,
                sector=?, sector_etf=?, active=1, updated_at=?
            WHERE symbol=?
            """,
            (name, sic_code, sic_description, market_cap,
             sector, sector_etf, now_utc, symbol),
        )

        if symbol not in db_symbols:
            new_symbols.append(symbol)
            logger.info(f"New ticker added: symbol={symbol}")

    # Deactivate tickers no longer in active config
    for db_symbol in db_symbols:
        if db_symbol not in config_symbols:
            db_conn.execute(
                "UPDATE tickers SET active=0, updated_at=? WHERE symbol=?",
                (now_utc, db_symbol),
            )
            logger.warning(
                f"Ticker deactivated (removed from config): symbol={db_symbol}"
            )

    db_conn.commit()

    logger.info(
        f"Ticker sync complete: config_symbols={len(config_symbols)} "
        f"new={len(new_symbols)} deactivated={len(db_symbols - config_symbols)}"
    )

    if bot_token and chat_id and new_symbols:
        send_telegram_message(
            bot_token, chat_id,
            f"Ticker sync: {len(new_symbols)} new ticker(s) added: {', '.join(new_symbols)}",
        )

    return new_symbols


def run_full_backfill(
    db_path: str = None,
    ticker_filter: str = None,
    phase_filter: str = None,
) -> None:
    """
    Run the full historical data backfill pipeline.

    Executes all backfill phases in order: sync, ohlcv, macro, fundamentals,
    earnings, corporate_actions, news, filings. Per-phase failures are logged
    and the pipeline continues. A pipeline_runs entry is written on completion.

    Args:
        db_path: Optional path to the SQLite database file. Defaults to the
            path from config/database.json.
        ticker_filter: Optional ticker symbol to restrict all phases to a single
            ticker, e.g. 'AAPL'.
        phase_filter: Optional phase name to run only that phase, e.g. 'ohlcv'.
            When set, the 'sync' phase is NOT run. Valid values: ohlcv, macro,
            fundamentals, earnings, corporate_actions, news, filings.

    Returns:
        None
    """
    load_env()
    backfiller_config = load_config("backfiller")
    db_config = load_config("database")

    resolved_db_path = db_path or db_config["path"]
    Path(resolved_db_path).parent.mkdir(parents=True, exist_ok=True)

    db_conn = get_connection(resolved_db_path)
    create_all_tables(db_conn)

    polygon_api_key = os.getenv("POLYGON_API_KEY", "")
    finnhub_api_key = os.getenv("FINNHUB_API_KEY", "")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    finnhub_client = FinnhubClient(finnhub_api_key)

    started_at = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()

    all_tickers = get_active_tickers()
    if ticker_filter:
        tickers = [ticker for ticker in all_tickers if ticker["symbol"] == ticker_filter]
    else:
        tickers = all_tickers

    with PolygonClient(polygon_api_key) as polygon_client:
        phases = {
            "ohlcv": lambda: backfill_all_tickers(
                db_conn, polygon_client, tickers, backfiller_config, bot_token, chat_id
            ),
            "macro": lambda: backfill_all_macro(
                db_conn, polygon_client, backfiller_config,
                sector_etfs=get_sector_etfs(),
                benchmarks=get_market_benchmarks(),
                bot_token=bot_token, chat_id=chat_id,
            ),
            "fundamentals": lambda: backfill_all_fundamentals(
                db_conn, tickers, backfiller_config, bot_token, chat_id
            ),
            "earnings": lambda: backfill_all_earnings(
                db_conn, finnhub_client, tickers, backfiller_config, bot_token, chat_id
            ),
            "corporate_actions": lambda: backfill_all_corporate_actions(
                db_conn, polygon_client, tickers, bot_token, chat_id
            ),
            "news": lambda: backfill_all_news(
                db_conn, polygon_client, finnhub_client, tickers, backfiller_config, bot_token, chat_id
            ),
            "filings": lambda: backfill_all_filings(
                db_conn, polygon_client, tickers, backfiller_config, bot_token, chat_id
            ),
        }

        phase_order = [
            "ohlcv", "macro", "fundamentals", "earnings",
            "corporate_actions", "news", "filings",
        ]

        tickers_processed_total = 0
        tickers_failed_total = 0
        failed_phases: list[str] = []

        if phase_filter:
            phases_to_run = [phase_filter] if phase_filter in phases else []
        else:
            # Run sync first, then all other phases
            logger.info("Running ticker sync phase")
            try:
                sync_tickers_from_config(db_conn, polygon_client, bot_token, chat_id)
            except Exception as exc:
                logger.error(f"Ticker sync phase failed: {exc!r}")
                log_alert(db_conn, None, today, "backfiller", "error",
                          f"Ticker sync failed: {exc}")
                failed_phases.append("sync")

            phases_to_run = phase_order

        for phase_name in phases_to_run:
            if phase_name not in phases:
                logger.warning(f"Unknown phase: {phase_name} — skipping")
                continue

            logger.info(f"Starting backfill phase: {phase_name}")
            try:
                result = phases[phase_name]()
                if isinstance(result, dict):
                    tickers_processed_total += result.get("tickers_processed", 0)
                    tickers_failed_total += result.get("tickers_failed", 0)
                logger.info(f"Completed backfill phase: {phase_name}")
            except Exception as exc:
                failed_phases.append(phase_name)
                logger.error(f"Backfill phase '{phase_name}' failed: {exc!r}")
                log_alert(db_conn, None, today, "backfiller", "error",
                          f"Backfill phase '{phase_name}' failed: {exc}")

    completed_at = datetime.now(timezone.utc).isoformat()
    duration_seconds = (
        datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
    ).total_seconds()

    status = "success" if not failed_phases else (
        "failed" if len(failed_phases) == len(phases_to_run) else "partial"
    )
    error_summary = f"Failed phases: {', '.join(failed_phases)}" if failed_phases else None

    log_pipeline_run(
        db_conn=db_conn,
        date=today,
        phase="backfill",
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        tickers_processed=tickers_processed_total,
        tickers_skipped=0,
        tickers_failed=tickers_failed_total,
        api_calls_made=0,
        status=status,
        error_summary=error_summary,
    )

    if bot_token and chat_id:
        summary_lines = [
            f"Backfill complete — status: {status}",
            f"Duration: {duration_seconds:.1f}s",
            f"Tickers processed: {tickers_processed_total}",
            f"Tickers failed: {tickers_failed_total}",
        ]
        if failed_phases:
            summary_lines.append(f"Failed phases: {', '.join(failed_phases)}")
        send_telegram_message(bot_token, chat_id, "\n".join(summary_lines))

    logger.info(
        f"Full backfill complete: status={status} duration={duration_seconds:.1f}s "
        f"tickers_processed={tickers_processed_total} tickers_failed={tickers_failed_total} "
        f"failed_phases={failed_phases}"
    )
