"""
Tests for scripts/migrate_add_timeframe_parity.py.

Verifies the migration:
- Creates all 14 weekly/monthly parity tables on a fresh database.
- Is idempotent (running twice does not raise or duplicate structures).
- Skips existing structures cleanly when run against an already-migrated DB.

External calls (Telegram) are mocked so no real notifications are sent.
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "migrate_add_timeframe_parity.py"


def _load_migration_module():
    """
    Load scripts/migrate_add_timeframe_parity.py as a module.

    The scripts directory is not a package, so we load it via spec_from_file_location.

    Returns:
        The loaded module object.
    """
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    spec = importlib.util.spec_from_file_location(
        "migrate_add_timeframe_parity", str(_SCRIPT_PATH)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PARITY_TABLES = [
    "swing_points_weekly",
    "swing_points_monthly",
    "support_resistance_weekly",
    "support_resistance_monthly",
    "patterns_weekly",
    "patterns_monthly",
    "divergences_weekly",
    "divergences_monthly",
    "crossovers_weekly",
    "crossovers_monthly",
    "indicator_profiles_weekly",
    "indicator_profiles_monthly",
    "scores_weekly",
    "scores_monthly",
]


@pytest.fixture
def empty_db_path(tmp_path) -> str:
    """Return a path to an empty SQLite file (no tables yet)."""
    db_path = str(tmp_path / "empty_signals.db")
    # Touch the file by opening + closing a connection.
    sqlite3.connect(db_path).close()
    return db_path


def _existing_tables(db_path: str) -> set:
    """Return the set of user table names currently in the database at db_path."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    conn.close()
    return {row[0] for row in rows}


def _existing_indexes(db_path: str) -> set:
    """Return the set of user index names currently in the database at db_path."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
    conn.close()
    return {row[0] for row in rows}


def test_migrate_creates_all_parity_tables(empty_db_path: str) -> None:
    """Running the migration on a fresh DB creates every parity table."""
    module = _load_migration_module()
    with mock.patch.object(module, "_send_telegram_completion", return_value=None):
        module.migrate(empty_db_path)

    tables = _existing_tables(empty_db_path)
    for table_name in PARITY_TABLES:
        assert table_name in tables, f"Migration did not create '{table_name}'"


def test_migrate_creates_expected_indexes(empty_db_path: str) -> None:
    """Each parity table has its idx_<table>_ticker_<datecol> index."""
    module = _load_migration_module()
    with mock.patch.object(module, "_send_telegram_completion", return_value=None):
        module.migrate(empty_db_path)

    indexes = _existing_indexes(empty_db_path)
    expected = {
        "idx_swing_points_weekly_ticker_week_start",
        "idx_swing_points_monthly_ticker_month_start",
        "idx_support_resistance_weekly_ticker_week_start",
        "idx_support_resistance_monthly_ticker_month_start",
        "idx_patterns_weekly_ticker_week_start",
        "idx_patterns_monthly_ticker_month_start",
        "idx_divergences_weekly_ticker_week_start",
        "idx_divergences_monthly_ticker_month_start",
        "idx_crossovers_weekly_ticker_week_start",
        "idx_crossovers_monthly_ticker_month_start",
        "idx_indicator_profiles_weekly_ticker_indicator",
        "idx_indicator_profiles_monthly_ticker_indicator",
        "idx_scores_weekly_ticker_week_start",
        "idx_scores_monthly_ticker_month_start",
    }
    missing = expected - indexes
    assert not missing, f"Migration did not create indexes: {sorted(missing)}"


def test_migrate_is_idempotent(empty_db_path: str) -> None:
    """
    Running the migration twice on the same DB must not raise or duplicate tables.
    """
    module = _load_migration_module()

    with mock.patch.object(module, "_send_telegram_completion", return_value=None):
        module.migrate(empty_db_path)
        tables_after_first = _existing_tables(empty_db_path)
        indexes_after_first = _existing_indexes(empty_db_path)

        # Second invocation must succeed without raising.
        module.migrate(empty_db_path)
        tables_after_second = _existing_tables(empty_db_path)
        indexes_after_second = _existing_indexes(empty_db_path)

    # Tables and indexes are unchanged across the second run (no duplicates,
    # no removals).
    assert tables_after_first == tables_after_second
    assert indexes_after_first == indexes_after_second
    for table_name in PARITY_TABLES:
        assert table_name in tables_after_second


def test_migrate_inserts_into_scores_weekly_after_migration(empty_db_path: str) -> None:
    """After migration, scores_weekly accepts an INSERT and respects its PK."""
    module = _load_migration_module()
    with mock.patch.object(module, "_send_telegram_completion", return_value=None):
        module.migrate(empty_db_path)

    conn = sqlite3.connect(empty_db_path)
    conn.execute(
        "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-16", 50.0),
    )
    conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
            "VALUES (?, ?, ?)",
            ("AAPL", "2026-03-16", 99.0),
        )
    conn.close()


def test_migrate_telegram_notice_called(empty_db_path: str) -> None:
    """The migration calls the Telegram completion notifier on success."""
    module = _load_migration_module()
    with mock.patch.object(
        module, "_send_telegram_completion", return_value=None
    ) as mock_send:
        module.migrate(empty_db_path)
    assert mock_send.called, "Migration should call _send_telegram_completion"


def test_migrate_telegram_notice_skipped_when_credentials_missing(
    empty_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    When TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID is not set, the notifier
    must not raise — it just logs a warning and returns.
    """
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    module = _load_migration_module()
    # No HTTP call should be made; we still assert it does not raise.
    module._send_telegram_completion(["table_a"])


def test_migrate_telegram_notice_calls_send_when_credentials_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    When credentials are present, _send_telegram_completion should call
    send_telegram_message exactly once with a non-empty body.
    """
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token")
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "fake-chat")

    module = _load_migration_module()
    with mock.patch.object(module, "send_telegram_message", return_value=123) as send:
        module._send_telegram_completion(["table_a", "table_b"])

    assert send.call_count == 1
    bot_token, chat_id, text = send.call_args.args
    assert bot_token == "fake-token"
    assert chat_id == "fake-chat"
    assert "table_a" in text
    assert "table_b" in text
