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
