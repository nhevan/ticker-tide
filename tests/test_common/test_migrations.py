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


def test_weekly_and_monthly_have_key_signals_data_after_migration(tmp_path: Path) -> None:
    """
    After migration, both scores_weekly (Migration 3) and scores_monthly
    (Migration 8) must have key_signals_data.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    weekly_columns = _get_column_names(conn, "scores_weekly")
    monthly_columns = _get_column_names(conn, "scores_monthly")

    assert "key_signals_data" in weekly_columns
    assert "key_signals_data" in monthly_columns
    conn.close()


def test_migration_8_adds_key_signals_data_to_legacy_monthly_db(tmp_path: Path) -> None:
    """
    Migration 8: if scores_monthly was created without key_signals_data (legacy
    schema), run_migrations must ALTER TABLE to add it. Idempotent on re-run.
    """
    db_file = str(tmp_path / "signals.db")
    conn = sqlite3.connect(db_file)
    # Create a minimal legacy scores_monthly without key_signals_data.
    conn.execute(
        """CREATE TABLE scores_monthly (
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            composite_score REAL NOT NULL,
            regime TEXT,
            data_completeness TEXT,
            key_signals TEXT,
            PRIMARY KEY (ticker, month_start)
        )"""
    )
    conn.commit()

    columns_before = _get_column_names(conn, "scores_monthly")
    assert "key_signals_data" not in columns_before

    run_migrations(conn)

    columns_after = _get_column_names(conn, "scores_monthly")
    assert "key_signals_data" in columns_after

    # Idempotent — running a second time must not raise.
    run_migrations(conn)
    columns_second = _get_column_names(conn, "scores_monthly")
    assert columns_second.count("key_signals_data") == 1
    conn.close()


def test_migration_creates_indicator_scores_sidecar_tables(tmp_path: Path) -> None:
    """
    run_migrations on a DB that lacks the indicator_scores sidecar tables must
    create all three: indicator_scores_daily, indicator_scores_weekly,
    indicator_scores_monthly.
    """
    db_file = str(tmp_path / "signals.db")
    # Bare DB with only a minimal scores_daily (no sidecar tables).
    conn = sqlite3.connect(db_file)
    conn.execute(
        """CREATE TABLE scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            UNIQUE(ticker, date)
        )"""
    )
    conn.commit()

    # Verify sidecar tables are absent before migration.
    existing_before = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "indicator_scores_daily" not in existing_before
    assert "indicator_scores_weekly" not in existing_before
    assert "indicator_scores_monthly" not in existing_before

    run_migrations(conn)

    existing_after = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "indicator_scores_daily" in existing_after, (
        "run_migrations must create indicator_scores_daily"
    )
    assert "indicator_scores_weekly" in existing_after, (
        "run_migrations must create indicator_scores_weekly"
    )
    assert "indicator_scores_monthly" in existing_after, (
        "run_migrations must create indicator_scores_monthly"
    )
    conn.close()


def test_migration_sidecar_tables_idempotent(tmp_path: Path) -> None:
    """
    Calling run_migrations twice on a fresh DB must not raise.
    The sidecar tables use CREATE TABLE IF NOT EXISTS, so the second call is a no-op.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)

    run_migrations(conn)
    run_migrations(conn)  # must not raise

    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "indicator_scores_daily" in tables
    assert "indicator_scores_weekly" in tables
    assert "indicator_scores_monthly" in tables
    conn.close()


def test_migration_adds_raw_daily_score_and_sector_etf_score_to_old_schema(
    tmp_path: Path,
) -> None:
    """
    Migration 4: if scores_daily was created without raw_daily_score and
    sector_etf_score, run_migrations must add both columns.
    """
    db_file = str(tmp_path / "signals.db")
    conn = sqlite3.connect(db_file)

    # Minimal old-schema scores_daily missing the two new columns.
    conn.execute(
        """CREATE TABLE scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            key_signals TEXT,
            key_signals_data TEXT,
            UNIQUE(ticker, date)
        )"""
    )
    conn.commit()

    columns_before = _get_column_names(conn, "scores_daily")
    assert "raw_daily_score" not in columns_before
    assert "sector_etf_score" not in columns_before

    run_migrations(conn)

    columns_after = _get_column_names(conn, "scores_daily")
    assert "raw_daily_score" in columns_after, (
        "run_migrations must add raw_daily_score to scores_daily"
    )
    assert "sector_etf_score" in columns_after, (
        "run_migrations must add sector_etf_score to scores_daily"
    )
    conn.close()


def test_migration_4_is_idempotent(tmp_path: Path) -> None:
    """
    Running run_migrations twice on a fresh DB must not raise and the two
    new columns must appear exactly once each (no duplicate columns).
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)

    run_migrations(conn)
    run_migrations(conn)  # must not raise

    columns = _get_column_names(conn, "scores_daily")
    assert columns.count("raw_daily_score") == 1
    assert columns.count("sector_etf_score") == 1
    conn.close()


def test_fresh_db_has_raw_daily_score_and_sector_etf_score_after_create(
    tmp_path: Path,
) -> None:
    """
    A database created via create_all_tables already has the new columns
    (they are in the CREATE TABLE statement); migration is a no-op for them.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    columns = _get_column_names(conn, "scores_daily")
    assert "raw_daily_score" in columns
    assert "sector_etf_score" in columns
    conn.close()


def test_migration_5_adds_calibrator_payload_to_old_schema(tmp_path: Path) -> None:
    """
    Migration 5: if scores_daily was created without calibrator_payload,
    run_migrations must add the column.
    """
    db_file = str(tmp_path / "signals.db")
    conn = sqlite3.connect(db_file)

    # Minimal old-schema scores_daily without calibrator_payload.
    conn.execute(
        """CREATE TABLE scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            key_signals TEXT,
            key_signals_data TEXT,
            raw_daily_score REAL,
            sector_etf_score REAL,
            UNIQUE(ticker, date)
        )"""
    )
    conn.commit()

    columns_before = _get_column_names(conn, "scores_daily")
    assert "calibrator_payload" not in columns_before

    run_migrations(conn)

    columns_after = _get_column_names(conn, "scores_daily")
    assert "calibrator_payload" in columns_after, (
        "run_migrations must add calibrator_payload to scores_daily"
    )
    conn.close()


def test_migration_5_is_idempotent(tmp_path: Path) -> None:
    """
    Calling run_migrations twice must not raise and calibrator_payload must
    appear exactly once — no duplicate columns.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)

    run_migrations(conn)
    run_migrations(conn)  # must not raise

    columns = _get_column_names(conn, "scores_daily")
    assert columns.count("calibrator_payload") == 1
    conn.close()


def test_fresh_db_has_calibrator_payload_after_create(tmp_path: Path) -> None:
    """
    A database created via create_all_tables already has calibrator_payload
    (it is in the CREATE TABLE statement); migration is a no-op for it.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    columns = _get_column_names(conn, "scores_daily")
    assert "calibrator_payload" in columns
    conn.close()


def test_migration_7_adds_realized_columns_idempotently(tmp_path: Path) -> None:
    """
    Migration 7: if scores_daily was created without the 5 realized-return
    columns, run_migrations must add all five. Calling run_migrations twice
    must not raise (idempotent — column already present guard fires on second call).
    """
    db_file = str(tmp_path / "signals.db")
    conn = sqlite3.connect(db_file)

    # Minimal old-schema scores_daily without any realized columns.
    conn.execute(
        """CREATE TABLE scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            key_signals TEXT,
            key_signals_data TEXT,
            raw_daily_score REAL,
            sector_etf_score REAL,
            calibrator_payload TEXT,
            confidence_modifiers TEXT,
            confidence_base REAL,
            UNIQUE(ticker, date)
        )"""
    )
    conn.commit()

    columns_before = _get_column_names(conn, "scores_daily")
    assert "realized_trading_days" not in columns_before
    assert "realized_ticker_return" not in columns_before
    assert "benchmark_return" not in columns_before
    assert "realized_excess" not in columns_before
    assert "realized_computed_at" not in columns_before

    run_migrations(conn)

    columns_after = _get_column_names(conn, "scores_daily")
    for col in (
        "realized_trading_days",
        "realized_ticker_return",
        "benchmark_return",
        "realized_excess",
        "realized_computed_at",
    ):
        assert col in columns_after, f"run_migrations must add {col} to scores_daily"

    # Idempotency: second call must not raise
    run_migrations(conn)

    columns_final = _get_column_names(conn, "scores_daily")
    for col in (
        "realized_trading_days",
        "realized_ticker_return",
        "benchmark_return",
        "realized_excess",
        "realized_computed_at",
    ):
        assert columns_final.count(col) == 1, f"Column {col} appears more than once"

    conn.close()


def test_fresh_db_has_realized_columns_after_create(tmp_path: Path) -> None:
    """
    A database created via create_all_tables already has all 5 realized columns
    (they are in the CREATE TABLE statement); run_migrations is a no-op for them.
    """
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    run_migrations(conn)

    columns = _get_column_names(conn, "scores_daily")
    for col in (
        "realized_trading_days",
        "realized_ticker_return",
        "benchmark_return",
        "realized_excess",
        "realized_computed_at",
    ):
        assert col in columns, f"scores_daily should have {col} after create_all_tables"
    conn.close()
