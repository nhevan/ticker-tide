#!/usr/bin/env python3
"""
Standalone script to enrich Finnhub articles with Claude-based sentiment.

Queries the news_articles table for NULL-sentiment articles (Finnhub sourced),
batches them through Claude Haiku for classification, updates the articles,
and recomputes news_daily_summary for affected ticker/date pairs.

Usage:
  python scripts/enrich_finnhub_sentiment.py
  python scripts/enrich_finnhub_sentiment.py --all
  python scripts/enrich_finnhub_sentiment.py --ticker AAPL
  python scripts/enrich_finnhub_sentiment.py --dry-run
  python scripts/enrich_finnhub_sentiment.py --db-path /custom/path/signals.db
"""

import argparse
import os
import sys
from collections import defaultdict
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.common.config import load_config, load_env  # noqa: E402
from src.common.db import get_connection  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402
from src.notifier.sentiment_enrichment import (  # noqa: E402
    get_articles_needing_sentiment,
    run_sentiment_enrichment,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser configured with all supported flags.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Enrich Finnhub news articles with Claude-based sentiment classification.\n\n"
            "Finnhub articles arrive without sentiment scores. This script uses\n"
            "Claude Haiku to classify each article as positive, negative, or neutral,\n"
            "then recomputes news_daily_summary for affected ticker/date pairs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/enrich_finnhub_sentiment.py
  python scripts/enrich_finnhub_sentiment.py --all
  python scripts/enrich_finnhub_sentiment.py --ticker AAPL
  python scripts/enrich_finnhub_sentiment.py --dry-run
  python scripts/enrich_finnhub_sentiment.py --db-path /custom/path/signals.db
        """,
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Remove the max_articles_per_run cap and process ALL null-sentiment articles.",
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Only enrich articles for this ticker symbol (e.g. AAPL).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed (count + cost estimate) without calling Claude.",
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override the database file path from config/database.json.",
    )
    return parser


def _run_dry_run(db_conn, config: dict, ticker_filter: Optional[str]) -> None:
    """
    Display a dry-run summary: article counts per ticker and estimated cost.

    Args:
        db_conn: Open SQLite connection.
        config: Notifier config dict.
        ticker_filter: Optional ticker to restrict the preview to.
    """
    se_config = config.get("sentiment_enrichment", {})
    max_articles = se_config.get("max_articles_per_run", 500)

    # Fetch all NULL-sentiment articles (ignoring limit for full picture)
    all_articles = get_articles_needing_sentiment(db_conn, limit=999_999)

    if ticker_filter:
        all_articles = [a for a in all_articles if a["ticker"] == ticker_filter.upper()]

    if not all_articles:
        print("No NULL-sentiment articles found — nothing to enrich.")
        return

    counts_by_ticker: dict[str, int] = defaultdict(int)
    for article in all_articles:
        counts_by_ticker[article["ticker"]] += 1

    total = len(all_articles)
    cost_estimate = total * 0.001

    print(f"\n{'=' * 50}")
    print(f"DRY RUN — Sentiment Enrichment Preview")
    print(f"{'=' * 50}")
    print(f"Total NULL-sentiment articles: {total}")
    print(f"Max articles per run (config):  {max_articles}")
    print(f"Would process in this run:      {min(total, max_articles)}")
    print(f"Estimated cost:                 ${min(total, max_articles) * 0.001:.3f}")
    print(f"{'=' * 50}")
    print(f"\nBreakdown by ticker:")
    for ticker in sorted(counts_by_ticker.keys()):
        print(f"  {ticker:<10} {counts_by_ticker[ticker]:>5} articles")
    print(f"\n  {'TOTAL':<10} {total:>5} articles")
    print(f"  Full cost estimate (all):      ${cost_estimate:.3f}")
    print()


def main() -> int:
    """
    Parse CLI arguments and run the Finnhub sentiment enrichment.

    Returns:
        Exit code — 0 on success, 1 on error.
    """
    setup_root_logging()
    parser = build_argument_parser()
    args = parser.parse_args()

    load_env()
    db_config = load_config("database")
    notifier_config = load_config("notifier")
    se_config = notifier_config.get("sentiment_enrichment", {})

    resolved_db_path = args.db_path or db_config.get("path", "data/signals.db")
    db_conn = get_connection(resolved_db_path)

    try:
        # --dry-run: show preview and exit
        if args.dry_run:
            _run_dry_run(db_conn, notifier_config, args.ticker)
            return 0

        # Apply --ticker filter by wrapping the config if needed
        if args.ticker:
            # Patch: temporarily limit query to this ticker by overriding config
            ticker_upper = args.ticker.upper()
            print(f"Restricting enrichment to ticker: {ticker_upper}")

            # Query articles for this ticker only
            from src.notifier.sentiment_enrichment import (
                get_articles_needing_sentiment,
                enrich_batch,
                recompute_affected_news_summaries,
            )
            from datetime import datetime, timezone

            max_articles = se_config.get("max_articles_per_run", 500)
            all_articles = get_articles_needing_sentiment(db_conn, limit=999_999)
            ticker_articles = [a for a in all_articles if a["ticker"] == ticker_upper][:max_articles]

            if not ticker_articles:
                print(f"No NULL-sentiment articles found for {ticker_upper}.")
                return 0

            print(f"Found {len(ticker_articles)} NULL-sentiment articles for {ticker_upper}")
            batch_size = se_config.get("batch_size", 20)
            total_enriched = 0
            total_failed = 0
            total_cost = 0.0
            affected: dict[str, set] = {ticker_upper: set()}

            start_ts = datetime.now(tz=timezone.utc)
            for batch_start in range(0, len(ticker_articles), batch_size):
                batch = ticker_articles[batch_start : batch_start + batch_size]
                result = enrich_batch(db_conn, batch, notifier_config)
                total_enriched += result["enriched"]
                total_failed += result["failed"]
                total_cost += result["cost_estimate"]
                for article in batch:
                    affected[ticker_upper].add(article["date"])

            summaries = recompute_affected_news_summaries(db_conn, affected)
            duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()

            print(f"\nSentiment Enrichment Complete")
            print(f"  Ticker:             {ticker_upper}")
            print(f"  Articles enriched:  {total_enriched}/{len(ticker_articles)}")
            print(f"  Failed:             {total_failed}")
            print(f"  Summaries updated:  {summaries}")
            print(f"  Estimated cost:     ${total_cost:.3f}")
            print(f"  Duration:           {int(duration)}s")
            return 0

        # --all: remove the max_articles_per_run cap
        if args.all:
            notifier_config = {
                **notifier_config,
                "sentiment_enrichment": {
                    **se_config,
                    "max_articles_per_run": 999_999,
                },
            }
            print("--all flag set: processing ALL NULL-sentiment articles (no cap)")

        result = run_sentiment_enrichment(db_conn, notifier_config)

        if result.get("skipped"):
            print(f"Skipped: {result.get('reason', 'unknown')}")
            return 0

        print(f"\nSentiment Enrichment Complete")
        print(f"  Articles enriched:  {result['enriched']}/{result['total']}")
        print(f"  Failed:             {result['failed']}")
        print(f"  Summaries updated:  {result['summaries_recomputed']}")
        print(f"  Duration:           {result['duration_seconds']:.1f}s")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        db_conn.close()


if __name__ == "__main__":
    sys.exit(main())
