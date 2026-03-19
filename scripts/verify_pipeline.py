#!/usr/bin/env python3
"""
Entry point to verify pipeline computed data.

Validates all computed data (indicators, scores, patterns, profiles) for
mathematical consistency and range validity. Companion to verify_backfill.py,
which checks raw data only.

Usage:
  python scripts/verify_pipeline.py                    # full verification
  python scripts/verify_pipeline.py --ticker AAPL      # single ticker
  python scripts/verify_pipeline.py --quiet             # only warnings/failures
  python scripts/verify_pipeline.py --no-telegram       # skip Telegram notification
  python scripts/verify_pipeline.py --date 2026-03-16  # specific scoring date
  python scripts/verify_pipeline.py --db-path PATH      # custom database file

Run after:
- Any config/threshold change
- Adding a new indicator or pattern
- Modifying scoring logic
- Weekly health check (Sundays 06:00 UTC via cron)

Exits with code 0 if overall status is PASS, 1 if FAIL.
"""

import argparse
import logging
import os
import sys

# Ensure the project root is on sys.path so src.* can be imported
# regardless of the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.backfiller.verify import VerificationReport  # noqa: E402
from src.backfiller.verify_pipeline import (  # noqa: E402
    format_pipeline_verification_report,
    run_full_pipeline_verification,
)
from src.common.config import load_env  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402
from src.common.progress import send_telegram_message  # noqa: E402


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser for verify_pipeline.py.

    Returns:
        argparse.ArgumentParser configured with all supported options.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Verify completeness and consistency of computed pipeline data "
            "(indicators, scores, patterns, profiles)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/verify_pipeline.py
  python scripts/verify_pipeline.py --date 2026-03-16
  python scripts/verify_pipeline.py --quiet
  python scripts/verify_pipeline.py --no-telegram
  python scripts/verify_pipeline.py --db-path /custom/path/signals.db
        """,
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Verify only one ticker's data (e.g. AAPL). Currently informational.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print warnings and failures; suppress passing checks.",
    )
    parser.add_argument(
        "--no-telegram",
        dest="no_telegram",
        action="store_true",
        help="Skip sending the verification report to Telegram.",
    )
    parser.add_argument(
        "--date",
        dest="scoring_date",
        metavar="YYYY-MM-DD",
        help=(
            "Scoring date to use for score-related checks. "
            "Defaults to the latest date in scores_daily."
        ),
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        metavar="PATH",
        help="Override the database file path (default: from database.json config).",
    )
    return parser


def _print_report(report: VerificationReport, quiet: bool) -> None:
    """
    Print the verification report to stdout.

    In quiet mode only checks with status 'warn' or 'fail' are shown.

    Args:
        report: The VerificationReport to display.
        quiet: If True, suppress checks that passed.
    """
    if quiet:
        from src.backfiller.verify import VerificationReport as VR
        filtered = [c for c in report.checks if c.status in ("warn", "fail")]
        display_report = VR(
            checks=filtered,
            overall_status=report.overall_status,
            pass_count=report.pass_count,
            warn_count=report.warn_count,
            fail_count=report.fail_count,
            timestamp=report.timestamp,
        )
        print(format_pipeline_verification_report(display_report))
    else:
        print(format_pipeline_verification_report(report))


def _send_telegram_report(report: VerificationReport) -> None:
    """
    Send the verification report to the admin Telegram chat if credentials are configured.

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID as a
    backward-compatible fallback) from the environment. If either is missing, logs a
    warning and skips the notification.

    Args:
        report: The VerificationReport to send.
    """
    logger = logging.getLogger(__name__)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set — "
            "skipping Telegram notification"
        )
        return

    formatted = format_pipeline_verification_report(report)
    message_id = send_telegram_message(bot_token, chat_id, formatted)
    if message_id:
        logger.info(f"Telegram pipeline verification report sent (message_id={message_id})")
    else:
        logger.warning("Failed to send Telegram pipeline verification report")


def main() -> int:
    """
    Run the pipeline verification and return an exit code.

    Parses CLI arguments, runs the full pipeline verification, prints the
    report to stdout, and optionally sends it to Telegram.

    Returns:
        0 if overall status is PASS, 1 if FAIL.
    """
    setup_root_logging()
    load_env()

    parser = build_argument_parser()
    args = parser.parse_args()

    report = run_full_pipeline_verification(
        db_path=args.db_path,
        scoring_date=args.scoring_date,
    )

    _print_report(report, quiet=args.quiet)

    if not args.no_telegram:
        _send_telegram_report(report)

    return 0 if report.overall_status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
