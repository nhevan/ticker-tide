#!/usr/bin/env python3
"""
One-shot backfill script for the 5 realized-return columns on scores_daily.

Iterates all (or a subset of) scores_daily rows and populates:
  realized_trading_days, realized_ticker_return, benchmark_return,
  realized_excess, realized_computed_at

Run this after the Migration 7 schema change to populate historical rows.
The daily pipeline's populate_realized_returns call handles the incremental
path going forward, so this script typically only needs to be run once.

SPY closes are pre-loaded into an in-memory dict as a minor optimization
before the main loop. Note: per-row ticker OHLCV lookups are the actual
bottleneck — SPY pre-load saves a small fraction of total runtime on a full
backfill (~100k rows × 2 SPY reads each = ~200k queries avoided).

Usage:
  python scripts/backfill_realized_returns.py
  python scripts/backfill_realized_returns.py --force
  python scripts/backfill_realized_returns.py --dry-run
  python scripts/backfill_realized_returns.py --ticker AAPL
  python scripts/backfill_realized_returns.py --limit 5000
  python scripts/backfill_realized_returns.py --db-path /custom/signals.db
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.common.config import load_config, load_env  # noqa: E402
from src.common.db import get_connection, run_migrations  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402
from src.scorer.realized_returns import populate_realized_returns  # noqa: E402

logger = logging.getLogger(__name__)

_FALLBACK_DB = "data/signals.db"
_LOG_PROGRESS_EVERY = 1000


def main() -> int:
    """
    Entry point for the backfill script.

    Runs run_migrations first to ensure the 5 new columns exist, then calls
    populate_realized_returns for the selected row range.

    Returns:
        0 on success, 1 on unrecoverable error.
    """
    setup_root_logging()
    load_env()

    parser = argparse.ArgumentParser(
        description="Backfill realized forward returns on scores_daily.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/backfill_realized_returns.py
  python scripts/backfill_realized_returns.py --force
  python scripts/backfill_realized_returns.py --dry-run
  python scripts/backfill_realized_returns.py --ticker AAPL
  python scripts/backfill_realized_returns.py --limit 5000 --force
        """,
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override database file path.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute and overwrite rows that already have realized_computed_at set.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run computation but do not write to the database.",
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Limit backfill to a single ticker symbol.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process at most N rows (useful for testing).",
    )
    args = parser.parse_args()

    db_path = args.db_path or load_config("database").get("db_path", _FALLBACK_DB)
    logger.info("Connecting to database: %s", db_path)

    conn = get_connection(db_path)
    try:
        run_migrations(conn)
        logger.info("Migrations applied.")

        start_time = time.monotonic()

        result = populate_realized_returns(
            conn,
            force=args.force,
            dry_run=args.dry_run,
            batch_size=500,
            ticker=args.ticker,
            limit=args.limit,
        )

        elapsed = time.monotonic() - start_time

        logger.info(
            "Backfill complete in %.1fs — scanned=%d updated=%d "
            "skipped_no_forward=%d skipped_already_populated=%d spy_fallbacks=%d dry_run=%s",
            elapsed,
            result["rows_scanned"],
            result["rows_updated"],
            result["rows_skipped_no_forward"],
            result["rows_skipped_already_populated"],
            result["spy_missing_fallbacks"],
            args.dry_run,
        )

        # Send Telegram summary
        try:
            from src.notifier.telegram import get_telegram_config, send_pipeline_error_alert
            from src.common.progress import send_telegram_message

            notifier_cfg = load_config("notifier")
            tg_cfg = get_telegram_config(notifier_cfg)
            bot_token = tg_cfg["bot_token"]
            admin_chat_id = tg_cfg["admin_chat_id"]

            summary = (
                f"📊 backfill_realized_returns complete ({elapsed:.0f}s)\n"
                f"  scanned: {result['rows_scanned']}\n"
                f"  updated: {result['rows_updated']}\n"
                f"  skipped (no forward): {result['rows_skipped_no_forward']}\n"
                f"  skipped (already done): {result['rows_skipped_already_populated']}\n"
                f"  SPY-missing fallbacks: {result['spy_missing_fallbacks']}\n"
                f"  dry_run: {args.dry_run}"
            )
            send_telegram_message(bot_token, admin_chat_id, summary)
        except Exception as tg_exc:
            logger.warning("Failed to send Telegram summary: %s", tg_exc)

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
