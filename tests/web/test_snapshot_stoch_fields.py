"""
Tests for Stochastic %K-related fields added to the daily snapshot section.

Verifies:
- stoch_sparkline, stoch_k_profile, stoch_zone_label are present in the daily section.
- Missing profile → stoch_k_profile is None and label uses fallback path.
- Sparkline rows have both stoch_k and stoch_d keys per row.
- Null stoch_k in indicators_daily → stoch_zone_label is None while sparkline and
  profile still serialize correctly with their independent null/empty values (Revision 3).

Note: Fixtures in this file are written in parallel (not reusing RSI fixtures) to keep
the RSI test file stable and avoid cross-contamination. Both fixture sets are minimal and
unambiguous about which indicator is under test.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Generator

import pytest

from src.common.db import create_all_tables
from src.web.queries import fetch_snapshot


_SCORER_CONFIG = {
    "indicator_thresholds": {
        "rsi_14": {"oversold": 30.0, "overbought": 70.0},
        "stoch_k": {"oversold": 20.0, "overbought": 80.0},
    },
}

_WEB_CONFIG = {
    "sparkline": {
        "daily_days": 15,
        "weekly_weeks": 6,
        "monthly_months": 6,
        "stoch_sparkline_days": 100,
    },
    "why_bullets": {"limit": 3},
    "signal_flip_lookback_days": 14,
    "pattern_row_limit": 5,
}


@pytest.fixture
def conn(tmp_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with the full schema created."""
    db_path = str(tmp_path / "test_stoch_fields.db")
    c = sqlite3.connect(db_path)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    create_all_tables(c)
    yield c
    c.close()


def _insert_ticker(conn: sqlite3.Connection, ticker: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO tickers(symbol, name, active) VALUES (?, ?, 1)",
        (ticker, ticker),
    )
    conn.commit()


def _insert_daily_score(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
) -> None:
    """Insert a minimal scores_daily row."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ticker, date, "BULLISH", 72.5, 55.0, "ranging",
            40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42,
        ),
    )
    conn.commit()


def _insert_indicators_with_stoch(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    stoch_k: float | None = 45.0,
    stoch_d: float | None = 40.0,
) -> None:
    """Insert an indicators_daily row with optional stoch_k and stoch_d."""
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily(ticker, date, stoch_k, stoch_d)
           VALUES (?, ?, ?, ?)""",
        (ticker, date, stoch_k, stoch_d),
    )
    conn.commit()


def _insert_stoch_profile(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert a stoch_k indicator_profiles row."""
    conn.execute(
        """INSERT OR REPLACE INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std)
           VALUES (?, 'stoch_k', 5.0, 20.0, 50.0, 80.0, 95.0, 50.0, 25.0)""",
        (ticker,),
    )
    conn.commit()


def _insert_stoch_sparkline_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float, float | None]],
) -> None:
    """Insert multiple (date, stoch_k, stoch_d) rows into indicators_daily."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, stoch_k, stoch_d) VALUES (?, ?, ?, ?)",
        [(ticker, date, stoch_k, stoch_d) for date, stoch_k, stoch_d in rows],
    )
    conn.commit()


def _snap(conn: sqlite3.Connection, ticker: str, date: str) -> dict:
    """Fetch the daily section of the snapshot for a ticker/date."""
    return fetch_snapshot(
        conn, ticker, date, config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG
    )["daily"]


class TestSnapshotStochFields:
    """Daily section includes stoch_sparkline, stoch_k_profile, stoch_zone_label."""

    def test_stoch_sparkline_key_present(self, conn: sqlite3.Connection) -> None:
        """stoch_sparkline key is present in daily section."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "stoch_sparkline" in daily

    def test_stoch_sparkline_empty_when_no_data(self, conn: sqlite3.Connection) -> None:
        """stoch_sparkline is [] (not None) when no stoch_k rows exist."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_sparkline"] == []
        assert daily["stoch_sparkline"] is not None

    def test_stoch_sparkline_rows_have_stoch_k_and_stoch_d_keys(
        self, conn: sqlite3.Connection
    ) -> None:
        """Each stoch_sparkline row has stoch_k and stoch_d keys."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_stoch_sparkline_rows(conn, "AAPL", [
            ("2026-04-24", 30.0, 25.0),
            ("2026-04-25", 45.0, 35.0),
        ])
        daily = _snap(conn, "AAPL", "2026-04-25")
        sparkline = daily["stoch_sparkline"]
        assert len(sparkline) == 2
        for row in sparkline:
            assert "stoch_k" in row
            assert "stoch_d" in row

    def test_stoch_k_profile_key_present(self, conn: sqlite3.Connection) -> None:
        """stoch_k_profile key is present in daily section."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "stoch_k_profile" in daily

    def test_stoch_k_profile_populated_when_profile_exists(
        self, conn: sqlite3.Connection
    ) -> None:
        """stoch_k_profile is a dict with percentile keys when indicator_profiles has a stoch_k row."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_stoch_profile(conn, "AAPL")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_k_profile"] is not None
        profile = daily["stoch_k_profile"]
        for key in ("p5", "p20", "p50", "p80", "p95", "mean", "std"):
            assert key in profile

    def test_stoch_k_profile_is_none_when_no_profile(self, conn: sqlite3.Connection) -> None:
        """stoch_k_profile is None when indicator_profiles has no stoch_k row."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_k_profile"] is None

    def test_stoch_zone_label_key_present(self, conn: sqlite3.Connection) -> None:
        """stoch_zone_label key is present in daily section."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "stoch_zone_label" in daily

    def test_stoch_zone_label_is_string_when_stoch_k_available(
        self, conn: sqlite3.Connection
    ) -> None:
        """stoch_zone_label is a non-empty string when stoch_k is available in indicators."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=45.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_zone_label"] is not None
        assert isinstance(daily["stoch_zone_label"], str)
        assert len(daily["stoch_zone_label"]) > 0

    def test_missing_profile_zone_label_uses_fallback(self, conn: sqlite3.Connection) -> None:
        """Without a profile, stoch_zone_label still has a value (fallback path)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        # stoch_k=10 is below oversold=20 threshold → fallback gives "oversold"
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=10.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_k_profile"] is None
        assert daily["stoch_zone_label"] == "oversold"

    def test_with_profile_zone_label_uses_profile_path(self, conn: sqlite3.Connection) -> None:
        """With a profile present, stoch_zone_label uses the profile-path six-zone set."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_stoch_profile(conn, "AAPL")
        # stoch_k=2 is below p5=5 in the profile → extreme_oversold
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=2.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_k_profile"] is not None
        assert daily["stoch_zone_label"] == "extreme_oversold"


class TestSnapshotStochFieldsNullStochK:
    """
    Revision 3: Explicit null stoch_k → null stoch_zone_label.

    When stoch_k IS NULL in indicators_daily for the picked date (e.g. ticker is too new
    for a 14-bar Stochastic warmup), stoch_zone_label must be None.
    stoch_sparkline and stoch_k_profile serialize independently and are not affected.
    """

    def test_null_stoch_k_in_indicators_gives_null_zone_label(
        self, conn: sqlite3.Connection
    ) -> None:
        """
        Insert a fixture where stoch_k IS NULL in indicators_daily for the picked date
        while the score row exists. Assert stoch_zone_label is None.
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        # Insert indicators row with explicit NULL stoch_k
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=None, stoch_d=None)
        _insert_stoch_profile(conn, "AAPL")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_zone_label"] is None

    def test_null_stoch_k_stoch_sparkline_still_serializes(
        self, conn: sqlite3.Connection
    ) -> None:
        """
        stoch_sparkline is [] (not None) when stoch_k IS NULL for all rows at or
        before the picked date. The sparkline field must still be present and serializable.
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=None, stoch_d=None)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "stoch_sparkline" in daily
        assert daily["stoch_sparkline"] == []
        assert daily["stoch_sparkline"] is not None

    def test_null_stoch_k_profile_still_serializes(self, conn: sqlite3.Connection) -> None:
        """
        stoch_k_profile is still returned correctly when stoch_k IS NULL in indicators
        (profile data is independent of the current indicator value).
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_stoch(conn, "AAPL", "2026-04-25", stoch_k=None, stoch_d=None)
        _insert_stoch_profile(conn, "AAPL")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["stoch_k_profile"] is not None
        assert "p5" in daily["stoch_k_profile"]
