#!/usr/bin/env python3
"""
Entry point script to run the Calculator (Phase 2b).

Usage:
    python scripts/run_calculator.py                         # full computation, all tickers
    python scripts/run_calculator.py --mode incremental      # incremental (daily pipeline)
    python scripts/run_calculator.py --ticker AAPL           # single ticker only
    python scripts/run_calculator.py --mode full --ticker AAPL  # full recompute for AAPL
    python scripts/run_calculator.py --db-path /custom/path/signals.db

The script:
    1. Parses command-line arguments.
    2. Calls run_calculator() with the appropriate options.
    3. Prints the results summary to stdout.
    4. Sends Telegram progress updates (if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
       are configured in .env).
    5. Exits with code 0 on success, 1 on failure.
"""

import argparse
import os
import sys

# Ensure the project root is on sys.path so src.* imports work regardless of
# the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.calculator.main import run_calculator  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Configured parser with --mode, --ticker,
            and --db-path options.
    """
    parser = argparse.ArgumentParser(
        description="Run the Calculator pipeline (Phase 2b) for the Stock Signal Engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_calculator.py
  python scripts/run_calculator.py --mode incremental
  python scripts/run_calculator.py --ticker AAPL
  python scripts/run_calculator.py --mode full --ticker AAPL
  python scripts/run_calculator.py --db-path /custom/path/signals.db
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help=(
            "Computation mode: 'full' recomputes everything from scratch; "
            "'incremental' computes only new data (requires fetcher_done event). "
            "Default: full."
        ),
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Restrict computation to a single ticker symbol, e.g. AAPL.",
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override the database file path from config/database.json.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Bypass the 'already completed today' check and re-run even if "
            "calculator_done is already marked completed for today."
        ),
    )
    return parser


def main() -> int:
    """
    Parse CLI arguments, run the calculator pipeline, and print the results.

    Returns:
        int: Exit code — 0 on success, 1 on error.
    """
    setup_root_logging()
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        result = run_calculator(
            db_path=args.db_path,
            mode=args.mode,
            ticker_filter=args.ticker,
            force=args.force,
        )

        print(
            f"Calculator complete.\n"
            f"  Mode:               {args.mode}\n"
            f"  Tickers processed:  {result['tickers_processed']}\n"
            f"  Tickers failed:     {result['tickers_failed']}\n"
            f"  Duration:           {result['duration_seconds']:.1f}s\n"
            f"  Indicators rows:    {result['indicators_rows']}\n"
            f"  Patterns found:     {result['patterns_found']}\n"
            f"  Divergences found:  {result['divergences_found']}\n"
            f"  Weekly candles:     {result['weekly_candles']}\n"
            f"  Profiles computed:  {result['profiles_computed']}\n"
            f"  News summaries:     {result['news_summaries']}"
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
