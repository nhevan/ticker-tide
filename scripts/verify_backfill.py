#!/usr/bin/env python3
"""
Entry point script to verify backfilled data.

Usage:
  python scripts/verify_backfill.py                    # full verification
  python scripts/verify_backfill.py --ticker AAPL      # verify single ticker
  python scripts/verify_backfill.py --quiet             # only show warnings/failures
  python scripts/verify_backfill.py --no-telegram       # skip Telegram notification

The script:
1. Runs all verification checks against the backfilled database
2. Prints a detailed report to console
3. Sends a summary to Telegram (unless --no-telegram)
4. Exits with code 0 if PASS, 1 if FAIL
"""

import argparse
import os
import sys

# Ensure the project root is on sys.path so src.* can be imported
# regardless of the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.backfiller.verify import (  # noqa: E402
    CheckResult,
    VerificationReport,
    check_data_freshness,
    check_date_gaps,
    check_date_range,
    check_null_coverage,
    check_table_row_counts,
    check_ticker_coverage,
    check_value_sanity,
    format_verification_report,
    run_full_verification,
)
from src.common.config import load_env  # noqa: E402
from src.common.db import get_connection  # noqa: E402
from src.common.logger import setup_root_logging  # noqa: E402
from src.common.progress import send_telegram_message  # noqa: E402


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser for verify_backfill.py.

    Returns:
        argparse.ArgumentParser configured with all supported options.
    """
    parser = argparse.ArgumentParser(
        description="Verify completeness and consistency of the backfilled stock database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/verify_backfill.py
  python scripts/verify_backfill.py --ticker AAPL
  python scripts/verify_backfill.py --quiet
  python scripts/verify_backfill.py --no-telegram
  python scripts/verify_backfill.py --db-path /custom/path/signals.db
        """,
    )
    parser.add_argument(
        "--ticker",
        metavar="SYMBOL",
        help="Verify only one ticker's data (e.g. AAPL).",
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
        filtered_checks = [c for c in report.checks if c.status in ("warn", "fail")]
        from src.backfiller.verify import VerificationReport as VR
        display_report = VR(
            checks=filtered_checks,
            overall_status=report.overall_status,
            pass_count=report.pass_count,
            warn_count=report.warn_count,
            fail_count=report.fail_count,
            timestamp=report.timestamp,
        )
        print(format_verification_report(display_report))
    else:
        print(format_verification_report(report))


def _send_telegram_report(report: VerificationReport) -> None:
    """
    Send the verification report to the admin Telegram chat if credentials are configured.

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID as a
    backward-compatible fallback) from the environment. If either is missing, logs a
    warning and skips the notification.

    Args:
        report: The VerificationReport to send.
    """
    import logging
    logger = logging.getLogger(__name__)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set — skipping Telegram notification"
        )
        return

    formatted = format_verification_report(report)
    message_id = send_telegram_message(bot_token, chat_id, formatted)
    if message_id:
        logger.info(f"Telegram verification report sent (message_id={message_id})")
    else:
        logger.warning("Failed to send Telegram verification report")


def main() -> int:
    """
    Run the backfill verification and return an exit code.

    Parses CLI arguments, runs the full verification (or single-ticker checks),
    prints the report, and optionally sends it to Telegram.

    Returns:
        0 if overall status is PASS, 1 if FAIL.
    """
    setup_root_logging()
    load_env()

    parser = build_argument_parser()
    args = parser.parse_args()

    report = run_full_verification(db_path=args.db_path)

    _print_report(report, quiet=args.quiet)

    if not args.no_telegram:
        _send_telegram_report(report)

    return 0 if report.overall_status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
