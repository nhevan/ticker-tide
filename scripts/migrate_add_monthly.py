#!/usr/bin/env python3
"""
Migration: add monthly timeframe tables and column.

Creates two new tables (if they don't exist):
  - monthly_candles    — aggregated OHLCV candles per month
  - indicators_monthly — technical indicators computed on monthly candles

Adds one new column to scores_daily (if it doesn't exist):
  - monthly_score REAL — composite score computed from monthly indicators

SQLite supports ALTER TABLE ADD COLUMN and CREATE TABLE IF NOT EXISTS,
so this migration is safe to run multiple times.

Usage:
    python scripts/migrate_add_monthly.py [--db-path PATH]

Defaults to the path defined in config/database.json (typically data/signals.db).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config", "database.json"
)
_FALLBACK_DB = "data/signals.db"


def _resolve_default_db_path() -> str:
    """Return the DB path from config/database.json, falling back to data/signals.db."""
    try:
        with open(_CONFIG_PATH) as fh:
            db_config = json.load(fh)
            return db_config.get("path", _FALLBACK_DB)
    except (OSError, json.JSONDecodeError):
        return _FALLBACK_DB


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column already exists in a table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    return column in existing_columns


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Check if a table already exists in the database."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def migrate(db_path: str) -> None:
    """
    Apply the monthly migration to the target database.

    Creates monthly_candles and indicators_monthly tables if absent,
    and adds monthly_score column to scores_daily if absent.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # ── monthly_candles ──────────────────────────────────────────────────────
    if table_exists(conn, "monthly_candles"):
        logger.info("Table 'monthly_candles' already exists — skipping")
    else:
        conn.execute("""
            CREATE TABLE monthly_candles (
                ticker      TEXT    NOT NULL,
                month_start TEXT    NOT NULL,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      REAL,
                UNIQUE(ticker, month_start)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monthly_candles_ticker_month "
            "ON monthly_candles(ticker, month_start)"
        )
        logger.info("Created table 'monthly_candles'")

    # ── indicators_monthly ───────────────────────────────────────────────────
    if table_exists(conn, "indicators_monthly"):
        logger.info("Table 'indicators_monthly' already exists — skipping")
    else:
        conn.execute("""
            CREATE TABLE indicators_monthly (
                ticker          TEXT    NOT NULL,
                month_start     TEXT    NOT NULL,
                ema_9           REAL,
                ema_21          REAL,
                ema_50          REAL,
                macd_line       REAL,
                macd_signal     REAL,
                macd_histogram  REAL,
                adx             REAL,
                rsi_14          REAL,
                stoch_k         REAL,
                stoch_d         REAL,
                cci_20          REAL,
                williams_r      REAL,
                obv             REAL,
                cmf_20          REAL,
                ad_line         REAL,
                bb_upper        REAL,
                bb_lower        REAL,
                bb_pctb         REAL,
                atr_14          REAL,
                keltner_upper   REAL,
                keltner_lower   REAL,
                UNIQUE(ticker, month_start)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_indicators_monthly_ticker_month "
            "ON indicators_monthly(ticker, month_start)"
        )
        logger.info("Created table 'indicators_monthly'")

    # ── monthly_score column in scores_daily ─────────────────────────────────
    if column_exists(conn, "scores_daily", "monthly_score"):
        logger.info("Column 'monthly_score' in scores_daily already exists — skipping")
    else:
        conn.execute("ALTER TABLE scores_daily ADD COLUMN monthly_score REAL")
        logger.info("Added column 'monthly_score REAL' to scores_daily")

    conn.commit()
    conn.close()
    logger.info("Migration complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add monthly timeframe tables and column")
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
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
