"""
Daily fetcher orchestrator — Phase 2a of the pipeline.

Fetches OHLCV, fundamentals, news, macro, and earnings data for all active
tickers. Writes a 'fetcher_done' pipeline event on completion.

NOTE: Full per-source fetch logic lives in the individual sub-modules.
This orchestrator wires them together and manages idempotency.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from src.backfiller.corporate_actions import backfill_all_corporate_actions
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
from src.common.db import get_connection
from src.common.events import get_pipeline_event_status, log_alert, write_pipeline_event
from src.fetcher.earnings import run_periodic_earnings
from src.notifier.telegram import get_telegram_config

logger = logging.getLogger(__name__)


def run_daily_fetch(
    db_path: str | None = None,
    force: bool = False,
    target_date: str | None = None,
) -> dict:
    """
    Run the daily data fetch phase (Phase 2a).

    Fetches OHLCV, fundamentals, earnings, news, and macro data for all active
    tickers. Writes a 'fetcher_done' pipeline event on successful completion.

    Parameters:
        db_path: Optional override for the database file path.
        force: When True, bypass the 'already completed' check and re-run.
        target_date: The trading date to fetch data for, as "YYYY-MM-DD". Defaults to
            today in UTC when not provided. The daily pipeline passes yesterday's UTC
            date here because it runs after market close.

    Returns:
        Summary dict with keys: skipped, tickers_processed, tickers_failed,
        duration_seconds.
    """
    load_env()
    db_config = load_config("database")
    resolved_db_path = db_path or db_config["path"]

    db_conn = get_connection(resolved_db_path)
    today = target_date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    status = get_pipeline_event_status(db_conn, "fetcher_done", today)
    if status == "completed" and not force:
        logger.info(f"fetcher: already completed for {today} — skipping (use force=True to override)")
        db_conn.close()
        return {"skipped": True, "reason": "already completed"}

    write_pipeline_event(db_conn, "fetcher_done", today, "processing")
    start_ts = datetime.now(tz=timezone.utc)
    logger.info(f"phase=fetcher date={today} Starting daily fetch")

    # --- Initialise API clients and load configs / tickers ---
    fetcher_config = load_config("fetcher")
    backfiller_config = load_config("backfiller")

    polygon_api_key = os.getenv("POLYGON_API_KEY", "")
    finnhub_api_key = os.getenv("FINNHUB_API_KEY", "")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")

    finnhub_client = FinnhubClient(finnhub_api_key)
    tickers = get_active_tickers()

    tickers_processed = 0
    tickers_failed = 0
    failed_phases: list[str] = []

    with PolygonClient(polygon_api_key) as polygon_client:
        # Phase: OHLCV
        try:
            result = backfill_all_tickers(
                db_conn, polygon_client, tickers, backfiller_config,
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("ohlcv")
            logger.error(f"phase=fetcher date={today} OHLCV fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"OHLCV fetch failed: {exc}")

        # Phase: Macro (sector ETFs, benchmarks, VIX, treasury yields)
        try:
            result = backfill_all_macro(
                db_conn, polygon_client, backfiller_config,
                sector_etfs=get_sector_etfs(),
                benchmarks=get_market_benchmarks(),
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("macro")
            logger.error(f"phase=fetcher date={today} Macro fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"Macro fetch failed: {exc}")

        # Phase: Fundamentals (periodic, staleness-checked)
        try:
            result = backfill_all_fundamentals(
                db_conn, tickers, backfiller_config,
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("fundamentals")
            logger.error(f"phase=fetcher date={today} Fundamentals fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"Fundamentals fetch failed: {exc}")

        # Phase: Earnings (periodic refresh via fetcher module)
        try:
            run_periodic_earnings(db_conn, tickers, fetcher_config)
        except Exception as exc:
            failed_phases.append("earnings")
            logger.error(f"phase=fetcher date={today} Earnings refresh failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"Earnings refresh failed: {exc}")

        # Phase: Corporate actions (dividends, splits, short interest)
        try:
            result = backfill_all_corporate_actions(
                db_conn, polygon_client, tickers, backfiller_config,
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("corporate_actions")
            logger.error(f"phase=fetcher date={today} Corporate actions fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"Corporate actions fetch failed: {exc}")

        # Phase: News (Polygon + Finnhub)
        try:
            result = backfill_all_news(
                db_conn, polygon_client, finnhub_client, tickers, backfiller_config,
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("news")
            logger.error(f"phase=fetcher date={today} News fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"News fetch failed: {exc}")

        # Phase: SEC filings (8-K)
        try:
            result = backfill_all_filings(
                db_conn, polygon_client, tickers, backfiller_config,
                bot_token=bot_token, chat_id=chat_id, force=force,
            )
            tickers_processed += result.get("tickers_processed", 0)
            tickers_failed += result.get("tickers_failed", 0)
        except Exception as exc:
            failed_phases.append("filings")
            logger.error(f"phase=fetcher date={today} Filings fetch failed: {exc!r}")
            log_alert(db_conn, None, today, "fetcher", "error", f"Filings fetch failed: {exc}")

    # Post-processing: Enrich Finnhub articles with Claude sentiment
    try:
        notifier_config = load_config("notifier")
        se_config = notifier_config.get("sentiment_enrichment", {})
        if se_config.get("enabled", False):
            from src.notifier.sentiment_enrichment import run_sentiment_enrichment

            tg_config = get_telegram_config(notifier_config)
            enrichment_result = run_sentiment_enrichment(
                db_conn,
                notifier_config,
                bot_token=tg_config.get("bot_token"),
                admin_chat_id=tg_config.get("admin_chat_id"),
            )
            if enrichment_result and not enrichment_result.get("skipped"):
                logger.info(
                    f"phase=fetcher date={today} Sentiment enrichment: "
                    f"{enrichment_result['enriched']} articles enriched"
                )
    except Exception as exc:
        logger.warning(f"phase=fetcher date={today} Sentiment enrichment failed (non-critical): {exc}")

    duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
    write_pipeline_event(db_conn, "fetcher_done", today, "completed")
    db_conn.close()

    if failed_phases:
        logger.warning(f"phase=fetcher date={today} Completed with failures: {', '.join(failed_phases)}")
    logger.info(
        f"phase=fetcher date={today} Completed in {duration:.1f}s "
        f"tickers_processed={tickers_processed} tickers_failed={tickers_failed}"
    )
    return {
        "skipped": False,
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
        "failed_phases": failed_phases,
        "duration_seconds": duration,
    }
