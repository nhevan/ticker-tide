#!/usr/bin/env python3
"""
Migration: add calibration columns to scores_daily.

Adds three new columns:
  - calibrated_score REAL  — predicted excess return from ridge regression
  - raw_composite_score REAL — the old static weighted composite score
  - model_r2 REAL — in-sample R² of the training window

SQLite supports ALTER TABLE ADD COLUMN, so this is a simple migration
(no table rebuild needed).

Usage:
    python scripts/migrate_add_calibration_columns.py [--db-path PATH]

Defaults to the path defined in config/database.json (typically data/signals.db).
Safe to run multiple times — skips columns that already exist.
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


NEW_COLUMNS = [
    ("calibrated_score", "REAL"),
    ("raw_composite_score", "REAL"),
    ("model_r2", "REAL"),
]


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column already exists in a table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    return column in existing_columns


def migrate(db_path: str) -> None:
    """Add calibration columns to scores_daily if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    added = 0
    for col_name, col_type in NEW_COLUMNS:
        if column_exists(conn, "scores_daily", col_name):
            logger.info("Column '%s' already exists — skipping", col_name)
            continue

        sql = f"ALTER TABLE scores_daily ADD COLUMN {col_name} {col_type}"
        conn.execute(sql)
        logger.info("Added column '%s %s' to scores_daily", col_name, col_type)
        added += 1

    conn.commit()
    conn.close()

    if added > 0:
        logger.info("Migration complete — %d column(s) added", added)
    else:
        logger.info("Migration complete — no changes needed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add calibration columns to scores_daily")
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
