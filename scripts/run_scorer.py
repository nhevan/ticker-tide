#!/usr/bin/env python3
"""
Entry point script to run the Scorer (Phase 3).

Usage:
  python scripts/run_scorer.py                           # score today
  python scripts/run_scorer.py --ticker AAPL             # score AAPL only
  python scripts/run_scorer.py --historical              # run historical scoring (Option E)
  python scripts/run_scorer.py --historical --ticker AAPL # historical for AAPL only

The script:
1. Parses command-line arguments
2. Calls run_scorer() or run_historical_scoring()
3. Prints results to stdout
4. Sends Telegram notifications
"""

import argparse
import os
import sys

# Ensure project root is on sys.path regardless of invocation directory.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.scorer.main import run_historical_scoring, run_scorer  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Configured parser with --ticker, --historical,
            and --db-path options.
    """
    parser = argparse.ArgumentParser(
        description="Run the Scorer pipeline (Phase 3) for the Stock Signal Engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_scorer.py
  python scripts/run_scorer.py --ticker AAPL
  python scripts/run_scorer.py --historical
  python scripts/run_scorer.py --historical --ticker AAPL
  python scripts/run_scorer.py --db-path /custom/path/signals.db
        """,
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Restrict scoring to a single ticker symbol, e.g. AAPL.",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help=(
            "Run historical scoring (Option E): last 12 months daily + "
            "older months weekly (up to 60 months)."
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
        help=(
            "Bypass the 'already completed' check and re-score even if "
            "scorer_done is already marked completed for the date."
        ),
    )
    return parser


def main() -> int:
    """
    Parse CLI arguments, run the scorer pipeline, and print results.

    Returns:
        int: Exit code — 0 on success, 1 on error.
    """
    setup_root_logging()
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        if args.historical:
            result = run_historical_scoring(
                db_path=args.db_path,
                ticker_filter=args.ticker,
                mode="both",
            )
            print(
                f"Historical scoring complete.\n"
                f"  Mode:          {result['mode']}\n"
                f"  Tickers:       {result['tickers']}\n"
                f"  Total scores:  {result['total_scores']}\n"
                f"  Duration:      {result['duration_seconds']:.1f}s"
            )
        else:
            result = run_scorer(
                db_path=args.db_path,
                ticker_filter=args.ticker,
                force=args.force,
            )

            if result.get("skipped"):
                print(f"Scorer skipped: {result.get('reason', 'unknown')}")
                return 0

            print(
                f"Scorer complete.\n"
                f"  Date:              {result['scoring_date']}\n"
                f"  Tickers:           {result['tickers_processed']}/{result['tickers_total']}\n"
                f"  Bullish:           {result['bullish_count']}\n"
                f"  Bearish:           {result['bearish_count']}\n"
                f"  Neutral:           {result['neutral_count']}\n"
                f"  Signal flips:      {result['flips_detected']}\n"
                f"  Failed:            {result['tickers_failed']}\n"
                f"  Duration:          {result['duration_seconds']:.1f}s"
            )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
