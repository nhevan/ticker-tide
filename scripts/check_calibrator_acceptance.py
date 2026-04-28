#!/usr/bin/env python3
"""
Calibrator distribution acceptance gate CLI.

This tool exists to safely flip `weekly_score_method` (or
`monthly_score_method`) from `v1_4cat` to `v2_8cat` in `config/scorer.json`.
v2 produces a different scalar than v1 for tickers with weekly/monthly events,
which propagates into the calibrator's training data. We need to confirm that
the distribution of `calibrated_score` does not catastrophically shift across
the flip — otherwise downstream signal/confidence numbers degrade silently.

Workflow (see OPERATIONS.md "Flipping weekly_score_method" for full procedure):

  1. snapshot the current (pre-flip) distribution to a baseline JSON file
  2. flip the config flag, re-run `scripts/run_scorer.py --historical --force`
     to regenerate the 365-day calibrator training window
  3. (optional) snapshot the post-flip distribution for archival
  4. run `check` against the pre-flip baseline; the gate compares
     calibrated_score on the same scoring_date in the current DB and emits
     PASS / WARNING / FAIL based on thresholds in
     `config/scorer.json["calibrator_acceptance"]`.

Exit codes:
  0 = PASS (no warning)
  0 = PASS WITH WARNING (warning flag in output, exit code unchanged — non-blocking)
  1 = FAIL
  2 = INSUFFICIENT_DATA (date mismatch, missing rows, or count below min_sample_size)

Telegram is best-effort: failures are logged and never alter the exit code.

Usage:
    python scripts/check_calibrator_acceptance.py snapshot --output baselines/pre.json [--scoring-date 2026-04-25]
    python scripts/check_calibrator_acceptance.py check --baseline baselines/pre.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from typing import Optional

# Allow running directly from the project root without an installed package.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv  # noqa: E402

from src.common.config import load_config  # noqa: E402
from src.common.progress import send_telegram_message  # noqa: E402
from src.scorer.acceptance_gate import (  # noqa: E402
    compare_distributions,
    compute_calibrated_score_distribution,
    find_latest_scoring_date_with_calibration,
    validate_snapshot_compatibility,
)


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_FALLBACK_DB = "data/signals.db"

# Exit code constants — keep aligned with module docstring + OPERATIONS.md.
_EXIT_PASS = 0
_EXIT_FAIL = 1
_EXIT_INSUFFICIENT = 2


# ---------------------------------------------------------------------------
# DB path resolution
# ---------------------------------------------------------------------------

def _resolve_db_path(override: Optional[str]) -> str:
    """
    Return the DB path: CLI override → config/database.json → fallback.

    Parameters:
        override: --db-path value from argparse (None if absent).

    Returns:
        Resolved path string suitable for sqlite3.connect().
    """
    if override:
        return override
    try:
        db_config = load_config("database")
        return db_config.get("path", _FALLBACK_DB)
    except (FileNotFoundError, json.JSONDecodeError):
        return _FALLBACK_DB


# ---------------------------------------------------------------------------
# Telegram (wrapped)
# ---------------------------------------------------------------------------

def _send_telegram_safe(text: str) -> None:
    """
    Best-effort Telegram send — never raises, never affects exit code.

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID
    fallback) from the environment. Logs a warning if either is missing.
    Any exception during the send is caught and logged at ERROR level.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get(
        "TELEGRAM_CHAT_ID"
    )
    if not bot_token or not chat_id:
        logger.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set — "
            "skipping Telegram notification"
        )
        return
    try:
        send_telegram_message(bot_token, chat_id, text)
    except Exception as exc:  # noqa: BLE001 — Telegram failure must never fail the gate
        logger.error("Telegram send failed: %s", exc)


# ---------------------------------------------------------------------------
# Snapshot subcommand
# ---------------------------------------------------------------------------

def _run_snapshot(args: argparse.Namespace) -> int:
    """
    Snapshot the calibrated_score distribution to a JSON file.

    When --scoring-date is omitted, auto-discovers the latest non-NULL
    calibrated date and logs the choice at INFO. Operators are nonetheless
    instructed to always pass --scoring-date explicitly so pre/post snapshots
    share the same anchor.

    Parameters:
        args: argparse Namespace (output, scoring_date, db_path).

    Returns:
        0 on success, 2 on INSUFFICIENT_DATA (no calibrated rows anywhere).
    """
    db_path = _resolve_db_path(args.db_path)
    logger.info("Using database: %s", db_path)

    conn = sqlite3.connect(db_path)
    try:
        scoring_date = args.scoring_date
        if not scoring_date:
            scoring_date = find_latest_scoring_date_with_calibration(conn)
            if not scoring_date:
                _send_telegram_safe(
                    "calibrator acceptance snapshot: INSUFFICIENT_DATA — "
                    "no calibrated_score rows found in scores_daily."
                )
                logger.error(
                    "INSUFFICIENT_DATA: no scores_daily rows with non-NULL "
                    "calibrated_score; cannot snapshot."
                )
                return _EXIT_INSUFFICIENT
            logger.info(
                "Auto-discovered scoring_date=%s (latest non-NULL calibrated row)",
                scoring_date,
            )
        else:
            logger.info("Using explicit scoring_date=%s", scoring_date)

        snapshot = compute_calibrated_score_distribution(conn, scoring_date)
    finally:
        conn.close()

    output_path = args.output
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w") as fh:
        json.dump(snapshot, fh, indent=2, sort_keys=True)

    logger.info(
        "Snapshot written to %s — scoring_date=%s count=%d mean=%.3f std=%.3f",
        output_path,
        snapshot["scoring_date"],
        snapshot["count"],
        snapshot["mean"],
        snapshot["std"],
    )
    _send_telegram_safe(
        f"calibrator acceptance snapshot:\n"
        f"  scoring_date={snapshot['scoring_date']}\n"
        f"  count={snapshot['count']}\n"
        f"  mean={snapshot['mean']:.3f}\n"
        f"  std={snapshot['std']:.3f}\n"
        f"  output={output_path}"
    )
    return _EXIT_PASS


# ---------------------------------------------------------------------------
# Check subcommand
# ---------------------------------------------------------------------------

def _load_baseline(path: str) -> dict:
    """Load and return a baseline snapshot dict from disk."""
    with open(path) as fh:
        return json.load(fh)


def _load_thresholds() -> dict:
    """
    Load the `calibrator_acceptance` block from config/scorer.json.

    Returns:
        Dict with keys max_mean_delta, max_std_delta, max_ticker_delta,
        min_sample_size.

    Raises:
        KeyError: if the block is missing — fail loudly so the operator
                  knows to add it rather than silently using bad defaults.
    """
    scorer_config = load_config("scorer")
    if "calibrator_acceptance" not in scorer_config:
        raise KeyError(
            "scorer.json is missing the required 'calibrator_acceptance' block. "
            "See CONFIG.md for the schema."
        )
    return scorer_config["calibrator_acceptance"]


def _format_check_message(
    verdict: str,
    result: dict,
    scoring_date: str,
) -> str:
    """Build the Telegram body for a check result (PASS / WARNING / FAIL)."""
    header = f"calibrator acceptance check: {verdict}"
    lines = [
        header,
        "",
        f"scoring_date: {scoring_date}",
        f"mean_delta:   {result['mean_delta']:+.3f}",
        f"std_delta:    {result['std_delta']:+.3f}",
        f"tickers exceeding per-ticker delta: {result['tickers_exceeding_delta']}",
        f"max |per-ticker delta|: {result['max_per_ticker_delta']:.3f}",
        "",
        f"reason: {result['reason']}",
    ]
    if verdict == "FAIL":
        lines.append("")
        lines.append("blocked — investigate and revert the flag if needed")
    elif verdict == "WARNING":
        lines.append("")
        lines.append("did not block; investigate per-ticker deltas")
    return "\n".join(lines)


def _run_check(args: argparse.Namespace) -> int:
    """
    Run the acceptance check against a previously-saved baseline.

    The scoring_date is derived strictly from the baseline file. If that date
    has no calibrated rows in the current DB, exits 2 (INSUFFICIENT_DATA) —
    we never silently fall back to a different date.

    Returns:
        0 on PASS (with or without WARNING),
        1 on FAIL,
        2 on INSUFFICIENT_DATA.
    """
    db_path = _resolve_db_path(args.db_path)
    logger.info("Using database: %s", db_path)

    baseline = _load_baseline(args.baseline)
    thresholds = _load_thresholds()
    min_sample_size = int(thresholds["min_sample_size"])

    scoring_date = baseline.get("scoring_date")
    if not scoring_date:
        msg = "INSUFFICIENT_DATA: baseline file is missing 'scoring_date'."
        logger.error(msg)
        _send_telegram_safe(f"calibrator acceptance check: {msg}")
        return _EXIT_INSUFFICIENT

    conn = sqlite3.connect(db_path)
    try:
        current = compute_calibrated_score_distribution(conn, scoring_date)
    finally:
        conn.close()

    try:
        validate_snapshot_compatibility(baseline, current, min_sample_size)
    except ValueError as exc:
        msg = f"INSUFFICIENT_DATA: {exc}"
        logger.error(msg)
        _send_telegram_safe(f"calibrator acceptance check: {msg}")
        return _EXIT_INSUFFICIENT

    result = compare_distributions(baseline, current, thresholds)

    if not result["passed"]:
        verdict = "FAIL"
        exit_code = _EXIT_FAIL
    elif result["warning"]:
        verdict = "WARNING"
        exit_code = _EXIT_PASS
    else:
        verdict = "PASS"
        exit_code = _EXIT_PASS

    message = _format_check_message(verdict, result, scoring_date)
    logger.info(message)
    _send_telegram_safe(message)
    return exit_code


# ---------------------------------------------------------------------------
# Argparse + main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level argparse parser with snapshot + check subcommands."""
    parser = argparse.ArgumentParser(
        description=(
            "Snapshot or check the calibrator's calibrated_score distribution "
            "for safe weekly_score_method / monthly_score_method flips."
        ),
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    snap = subparsers.add_parser(
        "snapshot",
        help="Write a JSON snapshot of calibrated_score on a scoring_date.",
    )
    snap.add_argument("--output", required=True, help="Path to write the snapshot JSON.")
    snap.add_argument(
        "--scoring-date",
        dest="scoring_date",
        default=None,
        help=(
            "YYYY-MM-DD scoring date (auto-discovers the latest non-NULL "
            "calibrated date if omitted; explicit is recommended)."
        ),
    )
    snap.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="Override the database file path.",
    )

    check = subparsers.add_parser(
        "check",
        help="Compare current calibrated_score distribution against a baseline file.",
    )
    check.add_argument(
        "--baseline", required=True, help="Path to the baseline snapshot JSON."
    )
    check.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="Override the database file path.",
    )

    return parser


def main() -> int:
    """
    CLI entry point.

    Returns:
        0 on PASS (or PASS-with-WARNING),
        1 on FAIL,
        2 on INSUFFICIENT_DATA.
    """
    load_dotenv()  # populate TELEGRAM_* env vars from .env if present
    parser = _build_parser()
    args = parser.parse_args()

    if args.subcommand == "snapshot":
        return _run_snapshot(args)
    if args.subcommand == "check":
        return _run_check(args)

    parser.error(f"Unknown subcommand: {args.subcommand}")
    return _EXIT_FAIL  # unreachable; argparse exits before this


if __name__ == "__main__":
    sys.exit(main())
