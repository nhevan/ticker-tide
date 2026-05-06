"""
Tests for run_migrations() in src/common/db.py.

Written first (TDD). All tests use pytest's tmp_path fixture so no real
database files are created. Each test is fully isolated.
"""

import sqlite3
from pathlib import Path

from src.common.db import create_all_tables, get_connection, run_migrations


# ── Helpers ────────────────────────────────────────────────────────────────────


def _get_column_names(conn: sqlite3.Connection, table: str) -> list:
    """
    Return the list of column names for the given table using PRAGMA table_info.

    Parameters:
        conn: An open sqlite3.Connection.
        table: The table name to inspect.

    Returns:
        A list of column name strings.
    """
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_fresh_db_has_key_signals_data_after_migration(tmp_path: Path) -> None:
    """
    Fresh DB: create_all_tables then run_migrations should produce a
    key_signals_data column in scores_daily without raising.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    columns = _get_column_names(conn, "scores_daily")
    assert "key_signals_data" in columns
    conn.close()


def test_run_migrations_is_idempotent(tmp_path: Path) -> None:
    """
    Idempotency: calling run_migrations twice must not raise and the column
    must appear exactly once (no duplicate columns).
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)

    run_migrations(conn)
    run_migrations(conn)

    columns = _get_column_names(conn, "scores_daily")
    assert columns.count("key_signals_data") == 1
    conn.close()


def test_migration_adds_column_to_old_schema(tmp_path: Path) -> None:
    """
    Pre-existing old schema: if scores_daily was created without key_signals_data,
    run_migrations must add it.
    """
    db_file = str(tmp_path / "signals.db")
    conn = sqlite3.connect(db_file)

    # Minimal old-schema scores_daily without key_signals_data
    conn.execute(
        """CREATE TABLE scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            key_signals TEXT,
            UNIQUE(ticker, date)
        )"""
    )
    conn.commit()

    # Confirm the column is absent before migration
    columns_before = _get_column_names(conn, "scores_daily")
    assert "key_signals_data" not in columns_before

    run_migrations(conn)

    columns_after = _get_column_names(conn, "scores_daily")
    assert "key_signals_data" in columns_after
    conn.close()


def test_weekly_and_monthly_untouched_after_migration(tmp_path: Path) -> None:
    """
    After migration, scores_weekly and scores_monthly must NOT have a
    key_signals_data column — this feature is daily-only by design.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    weekly_columns = _get_column_names(conn, "scores_weekly")
    monthly_columns = _get_column_names(conn, "scores_monthly")

    assert "key_signals_data" not in weekly_columns
    assert "key_signals_data" not in monthly_columns
    conn.close()
