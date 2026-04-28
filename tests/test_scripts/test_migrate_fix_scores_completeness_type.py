"""
Tests for scripts/migrate_fix_scores_completeness_type.py

Covers:
  - REAL → TEXT recreation when the table exists with the wrong type and is empty
  - No-op when the column is already TEXT (idempotent)
  - ABORT when REAL + non-empty (refuses to silently destroy data)
"""

from __future__ import annotations

import os
import sqlite3
import sys

import pytest

# Allow importing the script directly without making it a package.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPTS_DIR = os.path.join(_REPO_ROOT, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import migrate_fix_scores_completeness_type as migration_module  # noqa: E402


_OLD_REAL_SCORES_WEEKLY = """
CREATE TABLE scores_weekly (
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
    data_completeness REAL,
    key_signals TEXT,
    PRIMARY KEY (ticker, week_start)
)
"""

_OLD_REAL_SCORES_MONTHLY = """
CREATE TABLE scores_monthly (
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
    data_completeness REAL,
    key_signals TEXT,
    PRIMARY KEY (ticker, month_start)
)
"""


def _make_db_with_real_columns(db_path: str) -> None:
    """Create a fresh DB with the *broken* (REAL) data_completeness columns."""
    conn = sqlite3.connect(db_path)
    conn.execute(_OLD_REAL_SCORES_WEEKLY)
    conn.execute(_OLD_REAL_SCORES_MONTHLY)
    conn.commit()
    conn.close()


def _column_type(conn: sqlite3.Connection, table: str, column: str) -> str:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    for row in cursor.fetchall():
        if row[1] == column:
            return (row[2] or "").upper()
    raise AssertionError(f"{table}.{column} not found")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_recreates_when_real_and_empty(tmp_path) -> None:
    """REAL + 0 rows → tables recreated with TEXT data_completeness."""
    db_path = str(tmp_path / "fix.db")
    _make_db_with_real_columns(db_path)

    results = migration_module.migrate(db_path)

    assert results == {"scores_weekly": "recreated", "scores_monthly": "recreated"}
    conn = sqlite3.connect(db_path)
    assert _column_type(conn, "scores_weekly", "data_completeness") == "TEXT"
    assert _column_type(conn, "scores_monthly", "data_completeness") == "TEXT"
    conn.close()


def test_idempotent_when_already_text(tmp_path) -> None:
    """Already-TEXT tables → no-op on a re-run."""
    db_path = str(tmp_path / "fix.db")
    _make_db_with_real_columns(db_path)

    migration_module.migrate(db_path)  # first run flips REAL → TEXT
    results = migration_module.migrate(db_path)  # second run should be a no-op

    assert results == {
        "scores_weekly": "already_text",
        "scores_monthly": "already_text",
    }


def test_aborts_when_real_with_data(tmp_path) -> None:
    """
    REAL + at least one row → migration ABORTS rather than dropping data.

    Commit 6 is the first writer of these tables, so on a normal rollout this
    code path is exercised only when something has gone wrong. Refuse to
    silently destroy work.
    """
    db_path = str(tmp_path / "fix.db")
    _make_db_with_real_columns(db_path)

    # Seed a row in scores_weekly.
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-04-20", 42.0),
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="cannot drop a non-empty"):
        migration_module.migrate(db_path)
