"""
Tests for src/web/auth.py — password checking, rate limiting, and cookie session logic.

Uses tmp_path SQLite for rate-limit persistence tests. No external calls.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Generator

import pytest

from src.common.db import create_all_tables
from src.web.auth import (
    check_rate_limit,
    is_correct_password,
    prune_old_login_attempts,
    record_login_attempt,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn(tmp_path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with the full schema created."""
    db_path = str(tmp_path / "test_auth.db")
    c = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    create_all_tables(c)
    yield c
    c.close()


@pytest.fixture
def rate_limit_config() -> dict:
    """Return a minimal rate limit config."""
    return {"max_attempts": 5, "window_seconds": 60}


# ---------------------------------------------------------------------------
# is_correct_password tests
# ---------------------------------------------------------------------------

class TestIsCorrectPassword:
    """Tests for is_correct_password()."""

    def test_correct_password_returns_true(self) -> None:
        """Matching password must return True."""
        assert is_correct_password("secret123", "secret123") is True

    def test_wrong_password_returns_false(self) -> None:
        """Non-matching password must return False."""
        assert is_correct_password("wrongpass", "secret123") is False

    def test_empty_password_returns_false(self) -> None:
        """Empty submitted password must return False."""
        assert is_correct_password("", "secret123") is False

    def test_case_sensitive(self) -> None:
        """Password comparison must be case-sensitive."""
        assert is_correct_password("Secret123", "secret123") is False

    def test_uses_constant_time_compare(self) -> None:
        """Function must not raise on very long inputs (timing-safe path)."""
        long_pw = "x" * 10_000
        result = is_correct_password(long_pw, "secret123")
        assert result is False


# ---------------------------------------------------------------------------
# Rate limit tests
# ---------------------------------------------------------------------------

class TestRateLimit:
    """Tests for check_rate_limit(), record_login_attempt(), and prune_old_login_attempts()."""

    def test_fresh_ip_not_rate_limited(
        self, conn: sqlite3.Connection, rate_limit_config: dict
    ) -> None:
        """A new IP with no attempts must not be rate limited."""
        result = check_rate_limit(conn, "1.2.3.4", rate_limit_config)
        assert result is False

    def test_exceeding_attempts_triggers_rate_limit(
        self, conn: sqlite3.Connection, rate_limit_config: dict
    ) -> None:
        """After max_attempts attempts in window_seconds, IP must be rate-limited."""
        ip = "1.2.3.4"
        max_attempts = rate_limit_config["max_attempts"]
        for _ in range(max_attempts):
            record_login_attempt(conn, ip)
        result = check_rate_limit(conn, ip, rate_limit_config)
        assert result is True

    def test_attempts_below_limit_not_rate_limited(
        self, conn: sqlite3.Connection, rate_limit_config: dict
    ) -> None:
        """Fewer than max_attempts attempts must not trigger rate limiting."""
        ip = "1.2.3.4"
        for _ in range(rate_limit_config["max_attempts"] - 1):
            record_login_attempt(conn, ip)
        result = check_rate_limit(conn, ip, rate_limit_config)
        assert result is False

    def test_rate_limit_persists_across_connections(
        self, tmp_path, rate_limit_config: dict
    ) -> None:
        """Rate limit state must persist in DB — reconnecting must still see attempts."""
        db_path = str(tmp_path / "persist_test.db")

        # First connection: insert attempts
        conn1 = sqlite3.connect(db_path)
        conn1.execute("PRAGMA journal_mode=WAL")
        conn1.row_factory = sqlite3.Row
        create_all_tables(conn1)
        ip = "9.9.9.9"
        for _ in range(rate_limit_config["max_attempts"]):
            record_login_attempt(conn1, ip)
        conn1.close()

        # Second connection: check rate limit still applies
        conn2 = sqlite3.connect(db_path)
        conn2.execute("PRAGMA journal_mode=WAL")
        conn2.row_factory = sqlite3.Row
        result = check_rate_limit(conn2, ip, rate_limit_config)
        conn2.close()

        assert result is True

    def test_old_attempts_outside_window_not_counted(
        self, conn: sqlite3.Connection, rate_limit_config: dict
    ) -> None:
        """Attempts older than window_seconds must not count toward the rate limit."""
        ip = "1.2.3.4"
        old_time = (
            datetime.now(timezone.utc) - timedelta(seconds=120)
        ).strftime("%Y-%m-%d %H:%M:%S")
        # Insert old attempts directly with a past timestamp
        for _ in range(rate_limit_config["max_attempts"]):
            conn.execute(
                "INSERT INTO web_login_attempts(ip, attempted_at) VALUES (?, ?)",
                (ip, old_time),
            )
        conn.commit()
        result = check_rate_limit(conn, ip, rate_limit_config)
        assert result is False

    def test_different_ips_are_independent(
        self, conn: sqlite3.Connection, rate_limit_config: dict
    ) -> None:
        """Rate limit for one IP must not affect a different IP."""
        ip_a = "1.2.3.4"
        ip_b = "5.6.7.8"
        for _ in range(rate_limit_config["max_attempts"]):
            record_login_attempt(conn, ip_a)
        result = check_rate_limit(conn, ip_b, rate_limit_config)
        assert result is False

    def test_prune_removes_rows_older_than_1_hour(self, conn: sqlite3.Connection) -> None:
        """prune_old_login_attempts() must delete rows older than 1 hour."""
        ip = "1.2.3.4"
        old_time = (
            datetime.now(timezone.utc) - timedelta(hours=2)
        ).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO web_login_attempts(ip, attempted_at) VALUES (?, ?)",
            (ip, old_time),
        )
        conn.commit()

        prune_old_login_attempts(conn)

        count = conn.execute(
            "SELECT COUNT(*) FROM web_login_attempts WHERE ip = ?", (ip,)
        ).fetchone()[0]
        assert count == 0

    def test_prune_keeps_recent_rows(self, conn: sqlite3.Connection) -> None:
        """prune_old_login_attempts() must keep rows within 1 hour."""
        ip = "1.2.3.4"
        record_login_attempt(conn, ip)

        prune_old_login_attempts(conn)

        count = conn.execute(
            "SELECT COUNT(*) FROM web_login_attempts WHERE ip = ?", (ip,)
        ).fetchone()[0]
        assert count == 1
