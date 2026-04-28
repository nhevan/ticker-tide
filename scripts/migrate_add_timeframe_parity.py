#!/usr/bin/env python3
"""
Migration: add weekly/monthly parity tables that mirror the existing daily structures.

Creates 14 new tables (if they don't exist):
  - swing_points_weekly, swing_points_monthly
  - support_resistance_weekly, support_resistance_monthly
  - patterns_weekly, patterns_monthly
  - divergences_weekly, divergences_monthly
  - crossovers_weekly, crossovers_monthly
  - indicator_profiles_weekly, indicator_profiles_monthly
  - scores_weekly  (PRIMARY KEY (ticker, week_start))
  - scores_monthly (PRIMARY KEY (ticker, month_start))

Each table also gets a matching idx_<table>_ticker_<datecol> index. Idempotent —
safe to re-run; uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS
throughout.

Sends a Telegram completion notice to the admin chat when both TELEGRAM_BOT_TOKEN
and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID) are set in the environment.
Notification failure does not fail the migration.

Usage:
    python scripts/migrate_add_timeframe_parity.py [--db-path PATH]

Defaults to the path defined in config/database.json (typically data/signals.db).
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


# ── Table + index DDL ─────────────────────────────────────────────────────────

# Each entry: (table_name, create_table_sql, index_name, create_index_sql).
_PARITY_DDL: list[tuple[str, str, str, str]] = [
    (
        "swing_points_weekly",
        """CREATE TABLE IF NOT EXISTS swing_points_weekly (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            type TEXT,
            price REAL,
            strength INTEGER,
            UNIQUE(ticker, week_start, type)
        )""",
        "idx_swing_points_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_swing_points_weekly_ticker_week_start "
        "ON swing_points_weekly(ticker, week_start)",
    ),
    (
        "swing_points_monthly",
        """CREATE TABLE IF NOT EXISTS swing_points_monthly (
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            type TEXT,
            price REAL,
            strength INTEGER,
            UNIQUE(ticker, month_start, type)
        )""",
        "idx_swing_points_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_swing_points_monthly_ticker_month_start "
        "ON swing_points_monthly(ticker, month_start)",
    ),
    (
        "support_resistance_weekly",
        """CREATE TABLE IF NOT EXISTS support_resistance_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            level_price REAL,
            level_type TEXT,
            touch_count INTEGER,
            first_touch TEXT,
            last_touch TEXT,
            strength TEXT,
            broken BOOLEAN DEFAULT 0,
            broken_date TEXT
        )""",
        "idx_support_resistance_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_support_resistance_weekly_ticker_week_start "
        "ON support_resistance_weekly(ticker, week_start)",
    ),
    (
        "support_resistance_monthly",
        """CREATE TABLE IF NOT EXISTS support_resistance_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            level_price REAL,
            level_type TEXT,
            touch_count INTEGER,
            first_touch TEXT,
            last_touch TEXT,
            strength TEXT,
            broken BOOLEAN DEFAULT 0,
            broken_date TEXT
        )""",
        "idx_support_resistance_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_support_resistance_monthly_ticker_month_start "
        "ON support_resistance_monthly(ticker, month_start)",
    ),
    (
        "patterns_weekly",
        """CREATE TABLE IF NOT EXISTS patterns_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            pattern_name TEXT,
            pattern_category TEXT,
            pattern_type TEXT,
            direction TEXT,
            strength INTEGER,
            confirmed BOOLEAN DEFAULT 0,
            details TEXT
        )""",
        "idx_patterns_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_patterns_weekly_ticker_week_start "
        "ON patterns_weekly(ticker, week_start)",
    ),
    (
        "patterns_monthly",
        """CREATE TABLE IF NOT EXISTS patterns_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            pattern_name TEXT,
            pattern_category TEXT,
            pattern_type TEXT,
            direction TEXT,
            strength INTEGER,
            confirmed BOOLEAN DEFAULT 0,
            details TEXT
        )""",
        "idx_patterns_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_patterns_monthly_ticker_month_start "
        "ON patterns_monthly(ticker, month_start)",
    ),
    (
        "divergences_weekly",
        """CREATE TABLE IF NOT EXISTS divergences_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            indicator TEXT,
            divergence_type TEXT,
            price_swing_1_date TEXT,
            price_swing_1_value REAL,
            price_swing_2_date TEXT,
            price_swing_2_value REAL,
            indicator_swing_1_value REAL,
            indicator_swing_2_value REAL,
            strength INTEGER
        )""",
        "idx_divergences_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_divergences_weekly_ticker_week_start "
        "ON divergences_weekly(ticker, week_start)",
    ),
    (
        "divergences_monthly",
        """CREATE TABLE IF NOT EXISTS divergences_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            indicator TEXT,
            divergence_type TEXT,
            price_swing_1_date TEXT,
            price_swing_1_value REAL,
            price_swing_2_date TEXT,
            price_swing_2_value REAL,
            indicator_swing_1_value REAL,
            indicator_swing_2_value REAL,
            strength INTEGER
        )""",
        "idx_divergences_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_divergences_monthly_ticker_month_start "
        "ON divergences_monthly(ticker, month_start)",
    ),
    (
        "crossovers_weekly",
        """CREATE TABLE IF NOT EXISTS crossovers_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            crossover_type TEXT,
            direction TEXT,
            days_ago INTEGER
        )""",
        "idx_crossovers_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_crossovers_weekly_ticker_week_start "
        "ON crossovers_weekly(ticker, week_start)",
    ),
    (
        "crossovers_monthly",
        """CREATE TABLE IF NOT EXISTS crossovers_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            crossover_type TEXT,
            direction TEXT,
            days_ago INTEGER
        )""",
        "idx_crossovers_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_crossovers_monthly_ticker_month_start "
        "ON crossovers_monthly(ticker, month_start)",
    ),
    (
        "indicator_profiles_weekly",
        """CREATE TABLE IF NOT EXISTS indicator_profiles_weekly (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL,
            p20 REAL,
            p50 REAL,
            p80 REAL,
            p95 REAL,
            mean REAL,
            std REAL,
            window_start TEXT,
            window_end TEXT,
            computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",
        "idx_indicator_profiles_weekly_ticker_indicator",
        "CREATE INDEX IF NOT EXISTS idx_indicator_profiles_weekly_ticker_indicator "
        "ON indicator_profiles_weekly(ticker, indicator)",
    ),
    (
        "indicator_profiles_monthly",
        """CREATE TABLE IF NOT EXISTS indicator_profiles_monthly (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL,
            p20 REAL,
            p50 REAL,
            p80 REAL,
            p95 REAL,
            mean REAL,
            std REAL,
            window_start TEXT,
            window_end TEXT,
            computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",
        "idx_indicator_profiles_monthly_ticker_indicator",
        "CREATE INDEX IF NOT EXISTS idx_indicator_profiles_monthly_ticker_indicator "
        "ON indicator_profiles_monthly(ticker, indicator)",
    ),
    (
        "scores_weekly",
        """CREATE TABLE IF NOT EXISTS scores_weekly (
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
        )""",
        "idx_scores_weekly_ticker_week_start",
        "CREATE INDEX IF NOT EXISTS idx_scores_weekly_ticker_week_start "
        "ON scores_weekly(ticker, week_start)",
    ),
    (
        "scores_monthly",
        """CREATE TABLE IF NOT EXISTS scores_monthly (
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
        )""",
        "idx_scores_monthly_ticker_month_start",
        "CREATE INDEX IF NOT EXISTS idx_scores_monthly_ticker_month_start "
        "ON scores_monthly(ticker, month_start)",
    ),
]


def _resolve_default_db_path() -> str:
    """
    Return the database path from config/database.json, falling back to data/signals.db.

    Returns:
        Resolved path string suitable for sqlite3.connect().
    """
    try:
        with open(_CONFIG_PATH) as fh:
            db_config = json.load(fh)
            return db_config.get("path", _FALLBACK_DB)
    except (OSError, json.JSONDecodeError):
        return _FALLBACK_DB


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """
    Check whether a table already exists in the connected database.

    Parameters:
        conn: An open sqlite3.Connection.
        table: The table name to look up in sqlite_master.

    Returns:
        True if the table exists, False otherwise.
    """
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _send_telegram_completion(created_tables: list[str]) -> None:
    """
    Post a completion notice to the admin Telegram chat.

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_CHAT_ID
    as a fallback) from the environment. If either is missing, logs a warning
    and returns without attempting a network call. Never raises — Telegram
    failure must not fail the migration.

    Parameters:
        created_tables: The list of table names that were created (or already
                        existed) by the migration. Used to populate the
                        notification body.
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

    body_lines = ["migrate_add_timeframe_parity: complete", ""]
    body_lines.append(f"Tables ensured ({len(created_tables)}):")
    for table_name in created_tables:
        body_lines.append(f"  - {table_name}")
    body = "\n".join(body_lines)

    message_id = send_telegram_message(bot_token, chat_id, body)
    if message_id:
        logger.info(
            "Telegram completion notice sent (message_id=%s)", message_id
        )
    else:
        logger.warning("Failed to send Telegram completion notice")


def migrate(db_path: str) -> None:
    """
    Apply the timeframe-parity migration to the target database.

    Creates each of the 14 weekly/monthly mirror tables (and their indexes)
    if they do not already exist. Logs the status (created vs already existed)
    for each, then sends a completion notice to the admin Telegram chat.

    Parameters:
        db_path: Path to the SQLite database file. The file must already exist
                 (the migration does not bootstrap an empty database).
    """
    logger.info("Starting timeframe-parity migration on %s", db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        ensured: list[str] = []
        for table_name, create_table_sql, _index_name, create_index_sql in _PARITY_DDL:
            existed = table_exists(conn, table_name)
            conn.execute(create_table_sql)
            conn.execute(create_index_sql)
            if existed:
                logger.info("Table %r already exists — index ensured", table_name)
            else:
                logger.info("Created table %r and its index", table_name)
            ensured.append(table_name)
        conn.commit()
    finally:
        conn.close()

    _send_telegram_completion(ensured)
    logger.info("Migration complete — %d tables ensured", len(ensured))


def main() -> int:
    """
    CLI entry point for the migration script.

    Parses --db-path, resolves the default from config/database.json when
    omitted, runs migrate(), and returns 0 on success or 1 on unhandled error.

    Returns:
        Process exit code (0 = success, 1 = failure).
    """
    parser = argparse.ArgumentParser(
        description="Add weekly/monthly parity tables that mirror daily structures."
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
        migrate(resolved_db_path)
    except Exception as exc:  # noqa: BLE001 — top-level CLI guard
        logger.error("Migration failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
