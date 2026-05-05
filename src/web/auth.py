"""
Authentication helpers for the web UI.

Provides constant-time password comparison, SQLite-backed login rate limiting,
and periodic pruning of stale rate-limit rows.
"""

from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def is_correct_password(submitted: str, expected: str) -> bool:
    """
    Compare a submitted password against the expected password using constant-time comparison.

    Uses secrets.compare_digest to prevent timing-based side-channel attacks.
    Returns False for empty submitted passwords.

    Parameters:
        submitted: The password string submitted via the login form.
        expected: The expected password string (from WEB_PASSWORD env var).

    Returns:
        True if the passwords match, False otherwise.
    """
    if not submitted:
        return False
    return secrets.compare_digest(submitted, expected)


def record_login_attempt(conn: sqlite3.Connection, ip: str) -> None:
    """
    Record a login attempt for an IP address in the web_login_attempts table.

    Uses UTC timestamp for the attempted_at column. The record is used by
    check_rate_limit() to enforce a sliding-window rate limit.

    Parameters:
        conn: Open SQLite connection with write access.
        ip: IP address string of the requesting client.

    Returns:
        None
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO web_login_attempts(ip, attempted_at) VALUES (?, ?)",
        (ip, now_utc),
    )
    conn.commit()
    logger.info(f"Recorded login attempt: ip={ip!r}")


def check_rate_limit(
    conn: sqlite3.Connection,
    ip: str,
    rate_limit_config: dict,
) -> bool:
    """
    Check whether an IP address has exceeded the login rate limit.

    Counts the number of login attempts for the IP within the configured
    window_seconds. If the count >= max_attempts, returns True (rate-limited).
    Uses UTC time for consistent comparisons.

    Parameters:
        conn: Open SQLite connection with read access.
        ip: IP address string to check.
        rate_limit_config: Dict with keys 'max_attempts' (int) and
                           'window_seconds' (int).

    Returns:
        True if the IP is rate-limited, False otherwise.
    """
    max_attempts: int = rate_limit_config.get("max_attempts", 5)
    window_seconds: int = rate_limit_config.get("window_seconds", 60)

    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    ).strftime("%Y-%m-%d %H:%M:%S")

    count_row = conn.execute(
        "SELECT COUNT(*) FROM web_login_attempts "
        "WHERE ip = ? AND attempted_at >= ?",
        (ip, cutoff),
    ).fetchone()
    count = count_row[0] if count_row else 0

    if count >= max_attempts:
        logger.warning(
            f"Rate limit triggered: ip={ip!r}, attempts={count}, "
            f"max={max_attempts}, window={window_seconds}s"
        )
        return True
    return False


def prune_old_login_attempts(conn: sqlite3.Connection) -> None:
    """
    Delete web_login_attempts rows older than 1 hour.

    Should be called on each login attempt to prevent unbounded table growth.
    Uses UTC time for consistent comparisons.

    Parameters:
        conn: Open SQLite connection with write access.

    Returns:
        None
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).strftime("%Y-%m-%d %H:%M:%S")

    result = conn.execute(
        "DELETE FROM web_login_attempts WHERE attempted_at < ?",
        (cutoff,),
    )
    conn.commit()
    if result.rowcount > 0:
        logger.info(f"Pruned {result.rowcount} stale login attempt rows older than 1 hour")
