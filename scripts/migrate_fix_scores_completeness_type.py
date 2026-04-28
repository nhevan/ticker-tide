#!/usr/bin/env python3
"""
Migration: fix the ``data_completeness`` column type on scores_weekly + scores_monthly.

Commit 1's parity migration created both tables with ``data_completeness REAL``.
That was wrong — the daily ``scores_daily`` table uses ``data_completeness TEXT``
and stores ``json.dumps(...)``. The persistence helpers added in commit 6
write the same JSON-string shape into the weekly/monthly tables, which would
silently coerce to NULL on a REAL column.

This migration drops + recreates the affected tables when their column type is
REAL. It is **safe by construction**:

  - Idempotent: if the column is already TEXT, the script is a no-op.
  - Aborts on data: before any DROP we count rows; if either table holds rows,
    the migration logs an ERROR and raises rather than silently destroying
    work. Commit 6 is the first writer of these tables, so on a normal
    rollout they are empty when this runs.

Sends a Telegram completion notice to the admin chat when both
TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID) are set.

Usage:
    python scripts/migrate_fix_scores_completeness_type.py [--db-path PATH]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys

# Allow running directly from the project root without an installed package.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.common.progress import send_telegram_message  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "database.json")
_FALLBACK_DB = "data/signals.db"


_SCORES_WEEKLY_DDL = """CREATE TABLE IF NOT EXISTS scores_weekly (
    ticker TEXT NOT NULL,
    week_start TEXT NOT NULL,
    composite_score REAL NOT NULL,
    regime TEXT,
    trend_score REAL,
    momentum_score REAL,
    volume_score REAL,
    volatility_score REAL,
    candlestick_score REAL,
    structural_score REAL,
    fundamental_score REAL,
    macro_score REAL,
    data_completeness TEXT,
    key_signals TEXT,
    PRIMARY KEY (ticker, week_start)
)"""

_SCORES_WEEKLY_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_scores_weekly_ticker_week_start "
    "ON scores_weekly(ticker, week_start)"
)

_SCORES_MONTHLY_DDL = """CREATE TABLE IF NOT EXISTS scores_monthly (
    ticker TEXT NOT NULL,
    month_start TEXT NOT NULL,
    composite_score REAL NOT NULL,
    regime TEXT,
    trend_score REAL,
    momentum_score REAL,
    volume_score REAL,
    volatility_score REAL,
    candlestick_score REAL,
    structural_score REAL,
    fundamental_score REAL,
    macro_score REAL,
    data_completeness TEXT,
    key_signals TEXT,
    PRIMARY KEY (ticker, month_start)
)"""

_SCORES_MONTHLY_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_scores_monthly_ticker_month_start "
    "ON scores_monthly(ticker, month_start)"
)


def _resolve_default_db_path() -> str:
    """Return the database path from config/database.json, falling back to data/signals.db."""
    try:
        with open(_CONFIG_PATH) as fh:
            db_config = json.load(fh)
            return db_config.get("path", _FALLBACK_DB)
    except (OSError, json.JSONDecodeError):
        return _FALLBACK_DB


def _get_column_type(
    conn: sqlite3.Connection, table: str, column: str
) -> str | None:
    """
    Return the declared type of ``column`` on ``table``, or None when missing.

    Looks up the column via ``PRAGMA table_info(<table>)`` and returns the
    declared type uppercased. Returns None when the table does not exist or
    when the column is not defined on it.
    """
    try:
        cursor = conn.execute(f"PRAGMA table_info({table})")
    except sqlite3.OperationalError:
        return None
    for row in cursor.fetchall():
        # PRAGMA table_info layout: (cid, name, type, notnull, dflt_value, pk).
        if row[1] == column:
            return (row[2] or "").upper()
    return None


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    """
    Return the number of rows in ``table``, or 0 when the table doesn't exist.
    """
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
    except sqlite3.OperationalError:
        return 0
    row = cursor.fetchone()
    return int(row[0]) if row is not None else 0


def _fix_one_table(
    conn: sqlite3.Connection,
    table: str,
    create_sql: str,
    create_index_sql: str,
) -> str:
    """
    Apply the data_completeness type fix to a single scores table.

    Returns:
        ``"already_text"`` when the column was already TEXT (no-op),
        ``"recreated"``    when the table was dropped + recreated.

    Raises:
        RuntimeError when the table contains rows AND the column type is REAL —
        we refuse to silently destroy data even though commit 6 is the first
        writer. The caller must drain or back the rows up first.
    """
    current_type = _get_column_type(conn, table, "data_completeness")
    if current_type is None:
        # Table doesn't exist yet — create it fresh from the corrected DDL.
        logger.info("Table %r missing — creating with TEXT data_completeness", table)
        conn.execute(create_sql)
        conn.execute(create_index_sql)
        return "recreated"

    if current_type == "TEXT":
        logger.info(
            "Table %r data_completeness already TEXT — no-op", table
        )
        return "already_text"

    rows = _row_count(conn, table)
    if rows > 0:
        logger.error(
            "ABORT: %r holds %d row(s) but data_completeness is %s — refusing to drop. "
            "Drain or back up the table before re-running this migration.",
            table, rows, current_type,
        )
        raise RuntimeError(
            f"{table} has {rows} row(s); cannot drop a non-empty scores table"
        )

    logger.warning(
        "Table %r has data_completeness=%s (expected TEXT); recreating with corrected schema",
        table, current_type,
    )
    conn.execute(f"DROP TABLE {table}")
    conn.execute(create_sql)
    conn.execute(create_index_sql)
    return "recreated"


def _send_telegram_completion(results: dict[str, str]) -> None:
    """
    Post a completion notice to the admin Telegram chat.

    Mirrors the shape of commit 1's parity migration: skips when env vars are
    missing and never raises (Telegram failure must not fail the migration).
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get(
        "TELEGRAM_CHAT_ID"
    )
    if not bot_token or not chat_id:
        logger.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set — "
            "skipping Telegram completion notice"
        )
        return

    body_lines = ["migrate_fix_scores_completeness_type: complete", ""]
    for table_name, outcome in results.items():
        body_lines.append(f"  - {table_name}: {outcome}")
    body = "\n".join(body_lines)

    message_id = send_telegram_message(bot_token, chat_id, body)
    if message_id:
        logger.info("Telegram completion notice sent (message_id=%s)", message_id)
    else:
        logger.warning("Failed to send Telegram completion notice")


def migrate(db_path: str) -> dict[str, str]:
    """
    Apply the data_completeness type fix to ``scores_weekly`` + ``scores_monthly``.

    Parameters:
        db_path: Path to the SQLite database file. The file must already exist.

    Returns:
        Dict mapping table name → outcome ("already_text" or "recreated").

    Raises:
        RuntimeError when either table holds rows AND its data_completeness
        column is the wrong type.
    """
    logger.info("Starting data_completeness-type migration on %s", db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        results: dict[str, str] = {}
        results["scores_weekly"] = _fix_one_table(
            conn, "scores_weekly", _SCORES_WEEKLY_DDL, _SCORES_WEEKLY_INDEX
        )
        results["scores_monthly"] = _fix_one_table(
            conn, "scores_monthly", _SCORES_MONTHLY_DDL, _SCORES_MONTHLY_INDEX
        )
        conn.commit()
    finally:
        conn.close()

    return results


def main() -> int:
    """
    CLI entry point.

    Returns process exit code (0 = success, 1 = failure).
    """
    parser = argparse.ArgumentParser(
        description=(
            "Fix scores_weekly/scores_monthly.data_completeness column type to TEXT."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to SQLite database (default: read from config/database.json)",
    )
    args = parser.parse_args()

    resolved_db_path = args.db_path or _resolve_default_db_path()
    logger.info("Using database: %s", resolved_db_path)

    try:
        results = migrate(resolved_db_path)
    except Exception as exc:  # noqa: BLE001 — top-level CLI guard
        logger.error("Migration failed: %s", exc)
        return 1

    _send_telegram_completion(results)
    logger.info("Migration complete — %s", results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
