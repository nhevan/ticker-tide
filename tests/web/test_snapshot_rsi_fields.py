"""
Tests for RSI-related fields added to the daily snapshot section.

Verifies:
- regime, rsi_profile, rsi_zone_label, contributions_payload are present.
- When key_signals_data is NULL (legacy row), contributions_payload is null (not absent).
- Other fields are still populated in the legacy-row case.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Generator

import pytest

from src.common.db import create_all_tables
from src.web.queries import fetch_snapshot


_SCORER_CONFIG = {
    "indicator_thresholds": {
        "rsi_14": {"oversold": 30.0, "overbought": 70.0},
    },
}

_WEB_CONFIG = {
    "sparkline": {"daily_days": 15, "weekly_weeks": 6, "monthly_months": 6},
    "why_bullets": {"limit": 3},
    "signal_flip_lookback_days": 14,
    "pattern_row_limit": 5,
}

# Minimal contributions payload — matches the shape written by build_contributions_payload.
_SAMPLE_PAYLOAD = {
    "expansion_factor": 1.5,
    "items": [
        {
            "name": "rsi_14",
            "category": "momentum",
            "score": -55.0,
            "raw_value": -55.0,
            "category_weight": 0.25,
            "contribution": -8.25,
        }
    ],
}


@pytest.fixture
def conn(tmp_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with the full schema created."""
    db_path = str(tmp_path / "test_rsi_fields.db")
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
    *,
    key_signals_data: str | None = None,
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score, key_signals_data
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ticker, date, "BULLISH", 72.5, 55.0, "ranging",
            40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42,
            key_signals_data,
        ),
    )
    conn.commit()


def _insert_rsi_profile(conn: sqlite3.Connection, ticker: str) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std)
           VALUES (?, 'rsi_14', 25.0, 35.0, 50.0, 65.0, 78.0, 50.0, 12.0)""",
        (ticker,),
    )
    conn.commit()


def _insert_indicators(conn: sqlite3.Connection, ticker: str, date: str) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily(ticker, date, rsi_14)
           VALUES (?, ?, 45.0)""",
        (ticker, date),
    )
    conn.commit()


def _insert_rsi_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float | None]],
) -> None:
    """Insert multiple (date, rsi_14) rows into indicators_daily for a ticker."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, rsi_14) VALUES (?, ?, ?)",
        [(ticker, date, value) for date, value in rows],
    )
    conn.commit()


class TestSnapshotRsiFields:
    """Daily section includes regime, rsi_profile, rsi_zone_label, contributions_payload."""

    def test_regime_present_in_daily(self, conn: sqlite3.Connection) -> None:
        """regime field is populated from scores_daily.regime."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25",
                             key_signals_data=json.dumps(_SAMPLE_PAYLOAD))
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert daily["data_available"] is True
        assert "regime" in daily
        assert daily["regime"] == "ranging"

    def test_rsi_profile_present_when_profile_exists(self, conn: sqlite3.Connection) -> None:
        """rsi_profile is populated when indicator_profiles has a rsi_14 row."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25",
                             key_signals_data=json.dumps(_SAMPLE_PAYLOAD))
        _insert_rsi_profile(conn, "AAPL")
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert daily["rsi_profile"] is not None
        profile = daily["rsi_profile"]
        assert "p5" in profile
        assert "p50" in profile
        assert "p95" in profile

    def test_rsi_profile_is_none_when_no_profile(self, conn: sqlite3.Connection) -> None:
        """rsi_profile is None when indicator_profiles has no rsi_14 row."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25",
                             key_signals_data=json.dumps(_SAMPLE_PAYLOAD))
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert daily["rsi_profile"] is None

    def test_rsi_zone_label_present_when_rsi_available(self, conn: sqlite3.Connection) -> None:
        """rsi_zone_label is a non-empty string when rsi_14 is available."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25",
                             key_signals_data=json.dumps(_SAMPLE_PAYLOAD))
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert daily["rsi_zone_label"] is not None
        assert isinstance(daily["rsi_zone_label"], str)
        assert len(daily["rsi_zone_label"]) > 0

    def test_contributions_payload_present_with_data(self, conn: sqlite3.Connection) -> None:
        """contributions_payload is populated when key_signals_data is not NULL."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25",
                             key_signals_data=json.dumps(_SAMPLE_PAYLOAD))
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert daily["contributions_payload"] is not None
        assert "items" in daily["contributions_payload"]

    def test_contributions_payload_null_for_legacy_row(self, conn: sqlite3.Connection) -> None:
        """
        When key_signals_data is NULL (legacy row), contributions_payload is null
        (the key must be present, not absent; its value must be None, not raise).
        """
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25", key_signals_data=None)
        _insert_indicators(conn, "AAPL", "2026-04-25")
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert "contributions_payload" in daily
        assert daily["contributions_payload"] is None
        # Other fields should still be present.
        assert daily["data_available"] is True
        assert daily["signal"] == "BULLISH"
        assert daily["regime"] == "ranging"


class TestRsiSparkline:
    """rsi_sparkline field is always present in the daily section and has correct shape."""

    def _make_snap(self, conn: sqlite3.Connection, ticker: str, date: str) -> dict:
        """Insert a scores_daily row and fetch the snapshot for the given ticker/date."""
        _insert_ticker(conn, ticker)
        _insert_daily_score(conn, ticker, date)
        return fetch_snapshot(conn, ticker, date, config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)

    def test_rsi_sparkline_key_present_with_rsi_data(self, conn: sqlite3.Connection) -> None:
        """rsi_sparkline key is present in daily section when RSI data exists."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [("2026-04-24", 48.0), ("2026-04-25", 52.0)])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert "rsi_sparkline" in daily

    def test_rsi_sparkline_ordered_ascending(self, conn: sqlite3.Connection) -> None:
        """rsi_sparkline rows are ordered ascending by date."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [
            ("2026-04-23", 44.0),
            ("2026-04-24", 48.0),
            ("2026-04-25", 52.0),
        ])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        dates = [item["date"] for item in snap["daily"]["rsi_sparkline"]]
        assert dates == sorted(dates)

    def test_rsi_sparkline_length_at_most_configured_days(self, conn: sqlite3.Connection) -> None:
        """rsi_sparkline length is bounded by rsi_sparkline_days config (default 100)."""
        from datetime import date as date_cls, timedelta
        # Insert 110 RSI rows starting 2026-01-01.
        base_date = date_cls.fromisoformat("2026-01-01")
        rows = []
        d = base_date
        for i in range(110):
            rows.append((d.isoformat(), 40.0 + (i % 30)))
            d += timedelta(days=1)
        last_rsi_date = (d - timedelta(days=1)).isoformat()
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", last_rsi_date)
        _insert_rsi_rows(conn, "AAPL", rows)
        snap = fetch_snapshot(conn, "AAPL", last_rsi_date,
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        assert len(snap["daily"]["rsi_sparkline"]) <= 100

    def test_rsi_sparkline_item_shape(self, conn: sqlite3.Connection) -> None:
        """Each rsi_sparkline item has keys 'date' (str) and 'value' (float)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [("2026-04-25", 55.5)])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        sparkline = snap["daily"]["rsi_sparkline"]
        assert len(sparkline) == 1
        item = sparkline[0]
        assert set(item.keys()) == {"date", "value"}
        assert isinstance(item["date"], str)
        assert isinstance(item["value"], float)

    def test_rsi_sparkline_bounded_by_picked_date(self, conn: sqlite3.Connection) -> None:
        """All rsi_sparkline entries have date <= picked_date."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [
            ("2026-04-24", 48.0),
            ("2026-04-25", 52.0),
            ("2026-04-26", 60.0),  # after picked_date — must be excluded
        ])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        sparkline = snap["daily"]["rsi_sparkline"]
        for item in sparkline:
            assert item["date"] <= "2026-04-25"

    def test_rsi_sparkline_excludes_null_rsi_rows(self, conn: sqlite3.Connection) -> None:
        """Rows where rsi_14 IS NULL are excluded from rsi_sparkline."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [
            ("2026-04-23", 44.0),
            ("2026-04-24", None),   # NULL — must be excluded
            ("2026-04-25", 52.0),
        ])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        sparkline = snap["daily"]["rsi_sparkline"]
        dates_in_result = [item["date"] for item in sparkline]
        assert "2026-04-24" not in dates_in_result
        assert len(sparkline) == 2

    def test_rsi_sparkline_partial_data_returns_what_exists(self, conn: sqlite3.Connection) -> None:
        """When fewer than 100 rows exist, returns whatever is available (no padding)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_rsi_rows(conn, "AAPL", [("2026-04-25", 50.0)])
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        sparkline = snap["daily"]["rsi_sparkline"]
        assert len(sparkline) == 1
        assert sparkline[0]["value"] == 50.0

    def test_rsi_sparkline_empty_list_when_no_rsi_data(self, conn: sqlite3.Connection) -> None:
        """When no RSI data exists, rsi_sparkline is [] (key present, empty list, not None)."""
        _insert_ticker(conn, "AAPL")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        # No indicators_daily rows inserted.
        snap = fetch_snapshot(conn, "AAPL", "2026-04-25",
                              config=_WEB_CONFIG, scorer_config=_SCORER_CONFIG)
        daily = snap["daily"]
        assert "rsi_sparkline" in daily
        assert daily["rsi_sparkline"] == []
        assert daily["rsi_sparkline"] is not None
