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

Defaults to data/ticker_tide.db if --db-path is not specified.
Safe to run multiple times — skips columns that already exist.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

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
    parser.add_argument("--db-path", default="data/ticker_tide.db", help="Path to SQLite database")
    args = parser.parse_args()

    try:
        migrate(args.db_path)
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
