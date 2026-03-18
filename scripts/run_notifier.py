#!/usr/bin/env python3
"""
Entry point to run only the Notifier phase (Phase 4).

Usage:
  python scripts/run_notifier.py                    # run notifier for latest scores
  python scripts/run_notifier.py --db-path /path    # override database path
  python scripts/run_notifier.py --force            # re-run even if already completed today

Useful for testing the Telegram output without re-running the full pipeline.
"""

from __future__ import annotations

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.common.logger import setup_root_logging  # noqa: E402
from src.notifier.main import run_notifier  # noqa: E402


def main() -> int:
    """
    Parse CLI arguments, run the notifier phase, and print results.

    Returns:
        int: Exit code — 0 on success, 1 on error.
    """
    setup_root_logging()
    parser = argparse.ArgumentParser(
        description="Run the Notifier pipeline (Phase 4) for the Stock Signal Engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_notifier.py
  python scripts/run_notifier.py --db-path /custom/path/signals.db
  python scripts/run_notifier.py --force
        """,
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override the database file path from config/database.json.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if notifier_done is already completed for today.",
    )
    args = parser.parse_args()

    try:
        result = run_notifier(db_path=args.db_path, force=args.force)

        if result.get("skipped"):
            print(f"Notifier skipped: {result.get('reason', 'unknown')}")
            return 0

        print(
            f"Notifier complete.\n"
            f"  Date:            {result['scoring_date']}\n"
            f"  Bullish:         {result['bullish_count']}\n"
            f"  Bearish:         {result['bearish_count']}\n"
            f"  Neutral:         {result['neutral_count']}\n"
            f"  Flips:           {result['flips_count']}\n"
            f"  Tickers reasoned:{result['tickers_reasoned']}\n"
            f"  Telegram sent:   {result['telegram_sent']}\n"
            f"  Duration:        {result['duration_seconds']:.1f}s"
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
