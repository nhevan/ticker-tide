"""
Tests for src/calculator/support_resistance.py

Covers:
- cluster_into_sr_levels: clustering, separation, config tolerance, level_type,
  strength, min_touches, first/last touch tracking
- check_broken_levels: support broken, not broken
- save_sr_levels_to_db
- detect_support_resistance_for_ticker: end-to-end
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.support_resistance import (
    check_broken_levels,
    cluster_into_sr_levels,
    detect_support_resistance_for_ticker,
    save_sr_levels_to_db,
)


# ── Helpers ──────────────────────────────────────────────────────────────────────


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _make_swing_lows(prices: list[float], base_offset: int = 0) -> list[dict]:
    return [
        {"date": _make_date(base_offset + i * 5), "type": "low", "price": p, "strength": 3}
        for i, p in enumerate(prices)
    ]


def _make_swing_highs(prices: list[float], base_offset: int = 0) -> list[dict]:
    return [
        {"date": _make_date(base_offset + i * 5), "type": "high", "price": p, "strength": 3}
        for i, p in enumerate(prices)
    ]


def _make_ohlcv_df(closes: list[float]) -> pd.DataFrame:
    records = [
        {
            "date": _make_date(i),
            "open": c,
            "high": c + 1.0,
            "low": c - 1.0,
            "close": c,
            "volume": 200_000.0,
        }
        for i, c in enumerate(closes)
    ]
    return pd.DataFrame(records)


# ── cluster_into_sr_levels ───────────────────────────────────────────────────────


def test_cluster_swing_points_into_levels() -> None:
    """5 swing lows within 1.5% of each other form a single support level."""
    swing_pts = _make_swing_lows([98.5, 99.0, 99.2, 98.8, 99.1])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=1.5, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["touch_count"] == 5
    assert levels[0]["level_price"] == pytest.approx(98.92, abs=0.2)
    assert levels[0]["level_type"] == "support"


def test_cluster_separates_distinct_levels() -> None:
    """Swing lows at ~100 and ~120 form 2 separate S/R levels."""
    cluster_a = _make_swing_lows([100.0, 100.5, 101.0], base_offset=0)
    cluster_b = _make_swing_lows([120.0, 120.3, 121.0], base_offset=30)
    levels = cluster_into_sr_levels(cluster_a + cluster_b, price_tolerance_pct=1.5, min_touches=2)
    assert len(levels) == 2


def test_cluster_uses_config_tolerance() -> None:
    """With 0.5% tolerance, swing lows at 100 and 102 (2% apart) stay separate."""
    swing_pts = _make_swing_lows([100.0, 102.0])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=0.5, min_touches=2)
    # Each group has only 1 touch → both filtered out by min_touches=2
    assert len(levels) == 0


def test_sr_level_type_support() -> None:
    """A level formed from swing LOWS is typed as 'support'."""
    swing_pts = _make_swing_lows([99.0, 99.5, 100.0])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["level_type"] == "support"


def test_sr_level_type_resistance() -> None:
    """A level formed from swing HIGHS is typed as 'resistance'."""
    swing_pts = _make_swing_highs([120.0, 120.5, 121.0])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["level_type"] == "resistance"


def test_sr_level_strength_weak() -> None:
    """Level with exactly 2 touches → strength='weak'."""
    swing_pts = _make_swing_lows([100.0, 100.5])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["strength"] == "weak"


def test_sr_level_strength_moderate() -> None:
    """Level with 3 touches → strength='moderate'."""
    swing_pts = _make_swing_lows([100.0, 100.3, 100.6])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["strength"] == "moderate"


def test_sr_level_strength_strong() -> None:
    """Level with 4+ touches → strength='strong'."""
    swing_pts = _make_swing_lows([100.0, 100.2, 100.4, 100.6])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["strength"] == "strong"


def test_sr_level_min_touches() -> None:
    """A single swing point does not form a level when min_touches=2."""
    swing_pts = _make_swing_lows([100.0])
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=2.0, min_touches=2)
    assert len(levels) == 0


def test_sr_level_tracks_first_and_last_touch() -> None:
    """first_touch and last_touch reflect the earliest and latest dates in the cluster."""
    swing_pts = [
        {"date": "2024-01-05", "type": "low", "price": 100.0, "strength": 3},
        {"date": "2024-03-10", "type": "low", "price": 100.3, "strength": 3},
        {"date": "2024-06-20", "type": "low", "price": 100.1, "strength": 3},
    ]
    levels = cluster_into_sr_levels(swing_pts, price_tolerance_pct=1.5, min_touches=2)
    assert len(levels) == 1
    assert levels[0]["first_touch"] == "2024-01-05"
    assert levels[0]["last_touch"] == "2024-06-20"


# ── check_broken_levels ──────────────────────────────────────────────────────────


def test_sr_level_broken() -> None:
    """Support level at 100; price closes below 100 → broken=True, broken_date set."""
    sr_levels = [
        {
            "level_price": 100.0,
            "level_type": "support",
            "touch_count": 3,
            "first_touch": "2024-01-05",
            "last_touch": "2024-03-01",
            "strength": "moderate",
            "broken": False,
            "broken_date": None,
        }
    ]
    closes = [102.0, 101.0, 100.5, 99.0]  # last close 99 < 100
    ohlcv_df = _make_ohlcv_df(closes)
    updated = check_broken_levels(sr_levels, ohlcv_df)
    assert updated[0]["broken"] is True
    assert updated[0]["broken_date"] == ohlcv_df.iloc[-1]["date"]


def test_sr_level_not_broken() -> None:
    """Price stays above support level → broken=False."""
    sr_levels = [
        {
            "level_price": 100.0,
            "level_type": "support",
            "touch_count": 3,
            "first_touch": "2024-01-05",
            "last_touch": "2024-03-01",
            "strength": "moderate",
            "broken": False,
            "broken_date": None,
        }
    ]
    closes = [102.0, 103.0, 104.0, 105.0]
    ohlcv_df = _make_ohlcv_df(closes)
    updated = check_broken_levels(sr_levels, ohlcv_df)
    assert updated[0]["broken"] is False
    assert updated[0]["broken_date"] is None


# ── save_sr_levels_to_db ─────────────────────────────────────────────────────────


def test_save_sr_levels_to_db(db_connection: sqlite3.Connection) -> None:
    """Detected S/R levels are persisted to support_resistance with all expected fields."""
    sr_levels = [
        {
            "level_price": 100.0,
            "level_type": "support",
            "touch_count": 3,
            "first_touch": "2024-01-05",
            "last_touch": "2024-03-01",
            "strength": "moderate",
            "broken": False,
            "broken_date": None,
        }
    ]
    count = save_sr_levels_to_db(db_connection, "AAPL", sr_levels)
    assert count == 1

    rows = db_connection.execute(
        "SELECT * FROM support_resistance WHERE ticker = 'AAPL'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["level_type"] == "support"
    assert rows[0]["level_price"] == pytest.approx(100.0)
    assert rows[0]["touch_count"] == 3
    assert rows[0]["strength"] == "moderate"
    assert rows[0]["broken"] == 0
    assert rows[0]["broken_date"] is None


# ── detect_support_resistance_for_ticker ─────────────────────────────────────────


def test_detect_sr_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV + swing points into DB; call detect_support_resistance_for_ticker;
    verify support_resistance table is populated."""
    ticker = "AAPL"
    # Insert OHLCV
    ohlcv_rows = [
        (ticker, _make_date(i), 100.0, 101.0, 99.0, 100.0, 200_000.0)
        for i in range(30)
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        ohlcv_rows,
    )
    # Insert swing points with prices clustered around 99
    swing_rows = [
        (ticker, _make_date(i * 5), "low", 99.0 + i * 0.1, 3)
        for i in range(3)
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        swing_rows,
    )
    db_connection.commit()

    config = {
        "support_resistance": {
            "price_tolerance_pct": 1.5,
            "min_touches": 2,
            "lookback_days": 120,
        }
    }
    count = detect_support_resistance_for_ticker(db_connection, ticker, config)
    assert count >= 1

    rows = db_connection.execute(
        "SELECT * FROM support_resistance WHERE ticker = ?", (ticker,)
    ).fetchall()
    assert len(rows) >= 1
