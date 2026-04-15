#!/usr/bin/env python3
"""
Migration: fix final_score column and remove raw_composite_score.

Fixes the mixed-scales bug where final_score stored either the calibrated
ridge-regression prediction (≈ ±2–15%) or the raw composite (±100) depending
on calibration state.

After this migration:
  - final_score  = always the ±100 merged timeframe composite
  - calibrated_score = ridge regression prediction (unchanged)
  - raw_composite_score column = dropped (was a redundant patch)

Run once against the production database:
    python scripts/migrate_scores_final_score.py

Or against a specific DB path:
    python scripts/migrate_scores_final_score.py --db /path/to/custom.db
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.db import get_connection

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config", "database.json"
)
_FALLBACK_DB = "data/signals.db"


def _resolve_default_db_path() -> str:
    """Return the DB path from config/database.json, falling back to data/signals.db."""
    try:
        with open(_CONFIG_PATH) as fh:
            return json.load(fh).get("path", _FALLBACK_DB)
    except (OSError, json.JSONDecodeError):
        return _FALLBACK_DB


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if `column` exists in `table`."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def run_migration(db_path: str) -> None:
    """
    Execute the two-step migration on the given database.

    Step 1: Copy raw_composite_score → final_score for rows where
            raw_composite_score differs from final_score (i.e. rows written
            while calibration was warm and final_score held the calibrated value).
    Step 2: Drop the raw_composite_score column.

    Parameters:
        db_path: Absolute path to the SQLite database file.
    """
    conn = get_connection(db_path)
    conn.row_factory = sqlite3.Row

    if not _column_exists(conn, "scores_daily", "raw_composite_score"):
        print("raw_composite_score column not found — migration already applied or not needed.")
        conn.close()
        return

    # Step 1: repair final_score rows where calibration was warm.
    # A warm-calibration row has:
    #   raw_composite_score IS NOT NULL          (the real ±100 value was saved here)
    #   final_score != raw_composite_score       (final_score held the small calibrated value)
    result = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM scores_daily
        WHERE raw_composite_score IS NOT NULL
          AND ABS(final_score - raw_composite_score) > 0.001
        """
    ).fetchone()
    rows_to_fix = result["cnt"]

    conn.execute(
        """
        UPDATE scores_daily
        SET final_score = raw_composite_score
        WHERE raw_composite_score IS NOT NULL
          AND ABS(final_score - raw_composite_score) > 0.001
        """
    )
    conn.commit()
    print(f"Step 1 complete: repaired final_score for {rows_to_fix} row(s).")

    # Step 2: drop the now-redundant column (requires SQLite >= 3.35.0).
    sqlite_version = tuple(int(x) for x in sqlite3.sqlite_version.split("."))
    if sqlite_version < (3, 35, 0):
        print(
            f"WARNING: SQLite {sqlite3.sqlite_version} does not support DROP COLUMN "
            f"(requires 3.35.0+). Column raw_composite_score was NOT dropped. "
            f"Rows have been repaired — the application code no longer reads or writes "
            f"this column, so it is safe to leave in place."
        )
        conn.close()
        return

    conn.execute("ALTER TABLE scores_daily DROP COLUMN raw_composite_score")
    conn.commit()
    print("Step 2 complete: raw_composite_score column dropped.")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=None,
        help="Path to the SQLite database. Defaults to the path from config/database.json.",
    )
    args = parser.parse_args()

    db_path = args.db if args.db else _resolve_default_db_path()

    if not os.path.exists(db_path):
        print(f"ERROR: database not found at {db_path!r}")
        sys.exit(1)

    print(f"Running migration on: {db_path}")
    run_migration(db_path)
    print("Migration complete.")


if __name__ == "__main__":
    main()
