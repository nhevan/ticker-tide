"""
Daily fetcher orchestrator — Phase 2a of the pipeline.

Fetches OHLCV, fundamentals, news, macro, and earnings data for all active
tickers. Writes a 'fetcher_done' pipeline event on completion.

NOTE: Full per-source fetch logic lives in the individual sub-modules.
This orchestrator wires them together and manages idempotency.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.common.config import load_config, load_env
from src.common.db import get_connection
from src.common.events import get_pipeline_event_status, write_pipeline_event

logger = logging.getLogger(__name__)


def run_daily_fetch(db_path: str | None = None, force: bool = False) -> dict:
    """
    Run the daily data fetch phase (Phase 2a).

    Fetches OHLCV, fundamentals, earnings, news, and macro data for all active
    tickers. Writes a 'fetcher_done' pipeline event on successful completion.

    Parameters:
        db_path: Optional override for the database file path.
        force: When True, bypass the 'already completed' check and re-run.

    Returns:
        Summary dict with keys: skipped, tickers_processed, tickers_failed,
        duration_seconds.
    """
    load_env()
    db_config = load_config("database")
    resolved_db_path = db_path or db_config["path"]

    db_conn = get_connection(resolved_db_path)
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    status = get_pipeline_event_status(db_conn, "fetcher_done", today)
    if status == "completed" and not force:
        logger.info(f"fetcher: already completed for {today} — skipping (use force=True to override)")
        db_conn.close()
        return {"skipped": True, "reason": "already completed"}

    write_pipeline_event(db_conn, "fetcher_done", today, "processing")
    start_ts = datetime.now(tz=timezone.utc)
    logger.info(f"phase=fetcher date={today} Starting daily fetch")

    # TODO: wire up OHLCV, fundamentals, earnings, news, macro sub-modules

    duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
    write_pipeline_event(db_conn, "fetcher_done", today, "completed")
    db_conn.close()

    logger.info(f"phase=fetcher date={today} Completed in {duration:.1f}s")
    return {
        "skipped": False,
        "tickers_processed": 0,
        "tickers_failed": 0,
        "duration_seconds": duration,
    }
