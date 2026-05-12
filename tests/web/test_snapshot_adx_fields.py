"""
Tests for ADX-related fields added to the daily snapshot section.

Verifies:
- adx_sparkline and adx_zone_label are present in the daily section.
- adx_profile is NOT present (ADX is in PROFILE_FREE_INDICATORS; exposing it
  would mislead callers — the key must be absent, not None).
- Sparkline rows have the 'adx' key per row (single-series).
- Null ADX in indicators_daily → adx_zone_label is None AND adx_sparkline is [] (or shorter).
- adx_sparkline_days from config is respected.
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
        "adx": {"ranging_max": 20.0, "weak_max": 25.0, "developing_max": 40.0},
    },
}

_WEB_CONFIG = {
    "sparkline": {
        "daily_days": 15,
        "weekly_weeks": 6,
        "monthly_months": 6,
        "adx_sparkline_days": 100,
    },
    "why_bullets": {"limit": 3},
    "signal_flip_lookback_days": 14,
    "pattern_row_limit": 5,
}


@pytest.fixture
def conn(tmp_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with the full schema created."""
    db_path = str(tmp_path / "test_adx_fields.db")
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
            ticker, date, "BULLISH", 72.5, 55.0, "trending",
            40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42,
        ),
    )
    conn.commit()


def _insert_indicators_with_adx(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    adx: float | None = 30.0,
) -> None:
    """Insert an indicators_daily row with optional adx value."""
    conn.execute(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, adx) VALUES (?, ?, ?)",
        (ticker, date, adx),
    )
    conn.commit()


def _insert_adx_sparkline_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float]],
) -> None:
    """Insert multiple (date, adx) rows into indicators_daily."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, adx) VALUES (?, ?, ?)",
        [(ticker, date, adx) for date, adx in rows],
    )
    conn.commit()


def _snap(conn: sqlite3.Connection, ticker: str, date: str, config: dict | None = None) -> dict:
    """Fetch the daily section of the snapshot for a ticker/date."""
    return fetch_snapshot(
        conn, ticker, date,
        config=config if config is not None else _WEB_CONFIG,
        scorer_config=_SCORER_CONFIG,
    )["daily"]


class TestSnapshotAdxFields:
    """Daily section includes adx_sparkline and adx_zone_label; adx_profile must be absent."""

    def test_adx_sparkline_key_present(self, conn: sqlite3.Connection) -> None:
        """adx_sparkline key is present in daily section."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "adx_sparkline" in daily

    def test_adx_sparkline_empty_when_no_data(self, conn: sqlite3.Connection) -> None:
        """adx_sparkline is [] (not None) when no adx rows exist."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["adx_sparkline"] == []
        assert daily["adx_sparkline"] is not None

    def test_adx_sparkline_rows_have_adx_key(self, conn: sqlite3.Connection) -> None:
        """Each adx_sparkline row has 'date' and 'adx' keys (single-series)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_adx_sparkline_rows(conn, "AAPL", [
            ("2026-04-24", 25.0),
            ("2026-04-25", 30.0),
        ])
        daily = _snap(conn, "AAPL", "2026-04-25")
        sparkline = daily["adx_sparkline"]
        assert len(sparkline) == 2
        for row in sparkline:
            assert "date" in row
            assert "adx" in row

    def test_adx_zone_label_key_present(self, conn: sqlite3.Connection) -> None:
        """adx_zone_label key is present in daily section."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "adx_zone_label" in daily

    def test_adx_zone_label_is_string_when_adx_available(
        self, conn: sqlite3.Connection
    ) -> None:
        """adx_zone_label is a non-empty string when adx is available in indicators."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=30.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["adx_zone_label"] is not None
        assert isinstance(daily["adx_zone_label"], str)
        assert len(daily["adx_zone_label"]) > 0

    def test_adx_zone_label_correct_value_developing_trend(
        self, conn: sqlite3.Connection
    ) -> None:
        """adx=30 → developing_trend (between 25 and 40)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=30.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["adx_zone_label"] == "developing_trend"

    def test_adx_zone_label_correct_value_ranging(
        self, conn: sqlite3.Connection
    ) -> None:
        """adx=15 → ranging (below 20)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=15.0)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["adx_zone_label"] == "ranging"

    def test_adx_profile_not_in_result(self, conn: sqlite3.Connection) -> None:
        """
        adx_profile must NOT be present in the daily section.

        ADX is in PROFILE_FREE_INDICATORS — the profile is not used for scoring.
        Use 'not in' (not 'is None') to confirm the key is absent, not merely null.
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "adx_profile" not in daily


class TestSnapshotAdxFieldsNullAdx:
    """Null ADX in indicators_daily → adx_zone_label is None; sparkline is empty."""

    def test_null_adx_in_indicators_gives_null_zone_label(
        self, conn: sqlite3.Connection
    ) -> None:
        """When adx IS NULL in indicators_daily, adx_zone_label is None."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=None)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert daily["adx_zone_label"] is None

    def test_null_adx_sparkline_still_empty_list(self, conn: sqlite3.Connection) -> None:
        """adx_sparkline is [] (not None) when adx IS NULL for all rows at or before picked_date."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=None)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "adx_sparkline" in daily
        assert daily["adx_sparkline"] == []
        assert daily["adx_sparkline"] is not None

    def test_adx_profile_not_in_result_when_null_adx(self, conn: sqlite3.Connection) -> None:
        """
        adx_profile must NOT be present even when adx is NULL in indicators_daily.

        Negative assertion: assert 'adx_profile' not in result (NOT 'is None').
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_indicators_with_adx(conn, "AAPL", "2026-04-25", adx=None)
        daily = _snap(conn, "AAPL", "2026-04-25")
        assert "adx_profile" not in daily


class TestSnapshotAdxSparklineDaysConfig:
    """adx_sparkline_days from config is respected."""

    def test_adx_sparkline_days_from_config_respected(self, conn: sqlite3.Connection) -> None:
        """
        Build a fixture with 5 rows; pass adx_sparkline_days=2 via web config;
        assert only the 2 most recent rows are in adx_sparkline.
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_adx_sparkline_rows(conn, "AAPL", [
            ("2026-04-21", 20.0),
            ("2026-04-22", 22.0),
            ("2026-04-23", 24.0),
            ("2026-04-24", 26.0),
            ("2026-04-25", 28.0),
        ])
        config_2_days = {
            "sparkline": {
                "daily_days": 15,
                "weekly_weeks": 6,
                "monthly_months": 6,
                "adx_sparkline_days": 2,
            },
            "why_bullets": {"limit": 3},
            "signal_flip_lookback_days": 14,
            "pattern_row_limit": 5,
        }
        daily = _snap(conn, "AAPL", "2026-04-25", config=config_2_days)
        assert len(daily["adx_sparkline"]) == 2
