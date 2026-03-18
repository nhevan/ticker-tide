#!/usr/bin/env python3
"""
Daily pipeline orchestrator — runs all 4 phases in sequence.

This is the script that cron calls every day at 00:00 UTC (01:00 CET).

Flow:
  1. Check if market is open today → if not, send 'market closed' and exit 0.
  2. Phase 2a: run_daily_fetch() — fetch OHLCV, news, macro.
  3. Phase 2b: run_calculator(mode='incremental') — compute indicators.
  4. Phase 3: run_scorer() — generate signals.
  5. Phase 4: run_notifier() — AI reasoning + Telegram report.

Error handling:
  - Fetcher fails: stop pipeline (can't calculate without data), exit 1.
  - Calculator fails: stop pipeline (can't score without indicators), exit 1.
  - Scorer fails: still run notifier (it can report the error), exit 1.
  - Notifier fails: log error (pipeline ran, notification failed), exit 1.
  - Any failure sends a Telegram alert.

Usage:
  python scripts/run_daily.py
  python scripts/run_daily.py --db-path /custom/path/signals.db
  python scripts/run_daily.py --force
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# Ensure project root is on sys.path regardless of invocation directory
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.calculator.main import run_calculator  # noqa: E402
from src.common.config import load_config, load_env  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402
from src.fetcher.main import run_daily_fetch  # noqa: E402
from src.fetcher.market_calendar import is_market_open_today  # noqa: E402
from src.notifier.main import run_notifier  # noqa: E402
from src.notifier.telegram import (  # noqa: E402
    get_telegram_config,
    send_market_closed_notification,
    send_pipeline_error_alert,
)
from src.scorer.main import run_scorer  # noqa: E402


def run_daily_pipeline(db_path: str | None = None, force: bool = False) -> int:
    """
    Run the complete daily pipeline across all 4 phases.

    Collects per-phase timing stats and passes them to the notifier so the
    heartbeat message shows accurate durations. Sends a Telegram alert if any
    phase fails.

    Parameters:
        db_path: Optional override for the database file path.
        force: When True, bypass idempotency checks in each phase.

    Returns:
        0 on full success, 1 if any phase failed.
    """
    load_env()
    notifier_config = load_config("notifier")

    tg_config = get_telegram_config(notifier_config)
    bot_token = tg_config["bot_token"]
    admin_chat_id = tg_config["admin_chat_id"]
    subscriber_chat_ids = tg_config["subscriber_chat_ids"]

    pipeline_stats: dict = {
        "start_time": datetime.now(tz=timezone.utc).isoformat(),
        "scoring_date": None,
        "fetcher_duration": None,
        "calculator_duration": None,
        "scorer_duration": None,
        "notifier_duration": None,
        "tickers_processed": 0,
        "tickers_total": 0,
        "tickers_failed": 0,
        "failed_tickers": [],
        "bullish_count": 0,
        "bearish_count": 0,
        "neutral_count": 0,
        "display_timezone": notifier_config.get("telegram", {}).get(
            "display_timezone", "Europe/Amsterdam"
        ),
        "status": "started",
        "phases_completed": [],
        "error": None,
    }

    exit_code = 0
    pipeline_start = time.monotonic()

    try:
        # Step 1: Market calendar check
        if not is_market_open_today():
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            send_market_closed_notification(today, bot_token, subscriber_chat_ids, notifier_config)
            pipeline_stats["status"] = "market_closed"
            return 0

        # Step 2a: Fetcher
        phase_start = time.monotonic()
        try:
            fetch_result = run_daily_fetch(db_path=db_path, force=force)
            pipeline_stats["fetcher_duration"] = time.monotonic() - phase_start
            if not fetch_result.get("skipped"):
                pipeline_stats["phases_completed"].append("fetcher")
        except Exception as exc:
            pipeline_stats["fetcher_duration"] = time.monotonic() - phase_start
            pipeline_stats["error"] = str(exc)
            pipeline_stats["status"] = "failed"
            send_pipeline_error_alert("fetcher", str(exc), bot_token, admin_chat_id, notifier_config)
            return 1

        # Step 2b: Calculator
        phase_start = time.monotonic()
        try:
            run_calculator(db_path=db_path, mode="incremental", force=force)
            pipeline_stats["calculator_duration"] = time.monotonic() - phase_start
            pipeline_stats["phases_completed"].append("calculator")
        except Exception as exc:
            pipeline_stats["calculator_duration"] = time.monotonic() - phase_start
            pipeline_stats["error"] = str(exc)
            pipeline_stats["status"] = "failed"
            send_pipeline_error_alert("calculator", str(exc), bot_token, admin_chat_id, notifier_config)
            return 1

        # Step 3: Scorer
        scorer_failed = False
        phase_start = time.monotonic()
        try:
            scorer_result = run_scorer(db_path=db_path, force=force)
            pipeline_stats["scorer_duration"] = time.monotonic() - phase_start
            if not scorer_result.get("skipped"):
                pipeline_stats["phases_completed"].append("scorer")
                pipeline_stats["scoring_date"] = scorer_result.get("scoring_date")
                pipeline_stats["tickers_processed"] = scorer_result.get("tickers_processed", 0)
                pipeline_stats["tickers_total"] = scorer_result.get("tickers_total", 0)
                pipeline_stats["tickers_failed"] = scorer_result.get("tickers_failed", 0)
                pipeline_stats["bullish_count"] = scorer_result.get("bullish_count", 0)
                pipeline_stats["bearish_count"] = scorer_result.get("bearish_count", 0)
                pipeline_stats["neutral_count"] = scorer_result.get("neutral_count", 0)
        except Exception as exc:
            pipeline_stats["scorer_duration"] = time.monotonic() - phase_start
            scorer_failed = True
            exit_code = 1
            pipeline_stats["error"] = str(exc)
            send_pipeline_error_alert("scorer", str(exc), bot_token, admin_chat_id, notifier_config)

        # Step 4: Notifier (runs even if scorer failed)
        phase_start = time.monotonic()
        try:
            notifier_result = run_notifier(
                db_path=db_path,
                pipeline_stats=pipeline_stats,
                force=force,
            )
            pipeline_stats["notifier_duration"] = time.monotonic() - phase_start
            if not notifier_result.get("skipped"):
                pipeline_stats["phases_completed"].append("notifier")
        except Exception as exc:
            pipeline_stats["notifier_duration"] = time.monotonic() - phase_start
            exit_code = 1
            send_pipeline_error_alert("notifier", str(exc), bot_token, admin_chat_id, notifier_config)

        if not scorer_failed and exit_code == 0:
            pipeline_stats["status"] = "completed"

    except Exception as exc:
        pipeline_stats["status"] = "failed"
        pipeline_stats["error"] = str(exc)
        send_pipeline_error_alert("pipeline", str(exc), bot_token, admin_chat_id, notifier_config)
        exit_code = 1

    finally:
        total_duration = time.monotonic() - pipeline_start

        def _fmt(val: float | None) -> str:
            if val is None:
                return "skipped"
            return f"{val:.1f}s"

        print(
            f"\nDaily Pipeline {'Complete' if pipeline_stats['status'] == 'completed' else pipeline_stats['status'].upper()}"
        )
        print(f"  Date:     {pipeline_stats.get('scoring_date', 'unknown')}")
        print(f"  Status:   {pipeline_stats['status']}")
        print(f"  Duration: {total_duration:.1f}s")
        for phase in ["fetcher", "calculator", "scorer", "notifier"]:
            dur = pipeline_stats.get(f"{phase}_duration")
            status_icon = "✅" if phase in pipeline_stats.get("phases_completed", []) else "❌"
            print(f"    {status_icon} {phase}: {_fmt(dur)}")

    return exit_code


if __name__ == "__main__":
    setup_root_logging()
    parser = argparse.ArgumentParser(
        description="Run the daily signal pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_daily.py
  python scripts/run_daily.py --db-path /custom/path/signals.db
  python scripts/run_daily.py --force
        """,
    )
    parser.add_argument("--db-path", metavar="PATH", help="Override database path.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-run of all phases even if already completed today.",
    )
    args = parser.parse_args()
    sys.exit(run_daily_pipeline(db_path=args.db_path, force=args.force))
