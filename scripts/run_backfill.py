#!/usr/bin/env python3
"""
Entry point for the full historical data backfill pipeline.

Usage:
    python scripts/run_backfill.py
    python scripts/run_backfill.py --ticker AAPL
    python scripts/run_backfill.py --phase ohlcv
    python scripts/run_backfill.py --ticker AAPL --phase news
    python scripts/run_backfill.py --db-path /custom/path/signals.db
"""

import argparse
import os
import sys

# Ensure the project root is on sys.path so src.* can be imported
# regardless of the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.backfiller.main import run_full_backfill  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402

VALID_PHASES = [
    "ohlcv",
    "macro",
    "fundamentals",
    "earnings",
    "corporate_actions",
    "news",
    "filings",
]


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Configured parser with --ticker, --phase,
            --db-path, and --force options.
    """
    parser = argparse.ArgumentParser(
        description="Run the full historical data backfill pipeline for the Stock Signal Engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_backfill.py
  python scripts/run_backfill.py --ticker AAPL
  python scripts/run_backfill.py --phase ohlcv
  python scripts/run_backfill.py --ticker AAPL --phase news
  python scripts/run_backfill.py --db-path /custom/path/signals.db
  python scripts/run_backfill.py --force
  python scripts/run_backfill.py --phase ohlcv --force
        """,
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Restrict backfill to a single ticker symbol, e.g. AAPL.",
    )
    parser.add_argument(
        "--phase",
        choices=VALID_PHASES,
        metavar="PHASE",
        help=(
            f"Run only a specific phase. Choices: {', '.join(VALID_PHASES)}. "
            "When set, the ticker sync phase is skipped."
        ),
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override the database file path from config/database.json.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass all staleness checks and re-fetch all data from scratch.",
    )
    return parser


def main() -> int:
    """
    Parse CLI arguments and run the backfill pipeline.

    Returns:
        int: Exit code — 0 on success, 1 on error.
    """
    setup_root_logging()
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        run_full_backfill(
            db_path=args.db_path,
            ticker_filter=args.ticker,
            phase_filter=args.phase,
            force=args.force,
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
