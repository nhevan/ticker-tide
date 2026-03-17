"""
Tests for src/calculator/swing_points.py

Covers:
- detect_swing_points: swing high, swing low, no swing, boundary, config lookback,
  multiple swing points, strength computation
- save_swing_points_to_db: insert, idempotency
- detect_swing_points_for_ticker: end-to-end
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.swing_points import (
    detect_swing_points,
    detect_swing_points_for_ticker,
    save_swing_points_to_db,
)


# ── Helpers ─────────────────────────────────────────────────────────────────────


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _make_flat_ohlcv(n: int, price: float = 100.0) -> pd.DataFrame:
    """Flat OHLCV — no swing points."""
    records = [
        {
            "date": _make_date(i),
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price,
            "volume": 200_000.0,
        }
        for i in range(n)
    ]
    return pd.DataFrame(records)


def _make_spike_ohlcv(n: int, spike_index: int, spike_high: float, base_price: float = 100.0) -> pd.DataFrame:
    """OHLCV with a single spike high at spike_index."""
    records = []
    for i in range(n):
        high = spike_high if i == spike_index else base_price + 0.5
        records.append({
            "date": _make_date(i),
            "open": base_price,
            "high": high,
            "low": base_price - 0.5,
            "close": base_price,
            "volume": 200_000.0,
        })
    return pd.DataFrame(records)


def _make_trough_ohlcv(n: int, trough_index: int, trough_low: float, base_price: float = 100.0) -> pd.DataFrame:
    """OHLCV with a single trough low at trough_index."""
    records = []
    for i in range(n):
        low = trough_low if i == trough_index else base_price - 0.5
        records.append({
            "date": _make_date(i),
            "open": base_price,
            "high": base_price + 0.5,
            "low": low,
            "close": base_price,
            "volume": 200_000.0,
        })
    return pd.DataFrame(records)


def _insert_ohlcv(db_conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> None:
    rows = [
        (ticker, row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"])
        for _, row in df.iterrows()
    ]
    db_conn.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    db_conn.commit()


# ── detect_swing_points ──────────────────────────────────────────────────────────


def test_detect_swing_high() -> None:
    """Row 5 has the highest high; all 5 neighbors on each side are lower → swing high at row 5."""
    n = 11
    df = _make_spike_ohlcv(n, spike_index=5, spike_high=110.0, base_price=100.0)
    result = detect_swing_points(df, lookback_candles=5)
    highs = [p for p in result if p["type"] == "high"]
    assert len(highs) == 1
    assert highs[0]["date"] == df.iloc[5]["date"]
    assert highs[0]["price"] == pytest.approx(110.0)


def test_detect_swing_low() -> None:
    """Row 5 has the lowest low; all 5 neighbors on each side are higher → swing low at row 5."""
    n = 11
    df = _make_trough_ohlcv(n, trough_index=5, trough_low=90.0, base_price=100.0)
    result = detect_swing_points(df, lookback_candles=5)
    lows = [p for p in result if p["type"] == "low"]
    assert len(lows) == 1
    assert lows[0]["date"] == df.iloc[5]["date"]
    assert lows[0]["price"] == pytest.approx(90.0)


def test_detect_no_swing_point() -> None:
    """Flat data has no candle dominating all neighbors → no swing points detected."""
    df = _make_flat_ohlcv(15)
    result = detect_swing_points(df, lookback_candles=5)
    assert result == []


def test_detect_swing_point_at_boundary() -> None:
    """The first and last N rows cannot be swing points (insufficient neighbors)."""
    n = 15
    # Put spikes at index 0 and 14 — cannot have lookback=5 neighbors on both sides
    df = _make_spike_ohlcv(n, spike_index=0, spike_high=200.0, base_price=100.0)
    result = detect_swing_points(df, lookback_candles=5)
    highs = [p for p in result if p["type"] == "high"]
    # index 0 does not have 5 candles before it
    assert not any(p["date"] == df.iloc[0]["date"] for p in highs)

    df2 = _make_spike_ohlcv(n, spike_index=14, spike_high=200.0, base_price=100.0)
    result2 = detect_swing_points(df2, lookback_candles=5)
    highs2 = [p for p in result2 if p["type"] == "high"]
    assert not any(p["date"] == df2.iloc[14]["date"] for p in highs2)


def test_detect_swing_point_uses_config_lookback() -> None:
    """With lookback=3, a spike at index 3 (only 3 neighbors needed each side) IS detected.
    With lookback=5 it would NOT be (not enough right-side neighbors in a 7-row dataset)."""
    # 7 rows, spike at index 3 — needs exactly 3 neighbors each side
    n = 7
    df = _make_spike_ohlcv(n, spike_index=3, spike_high=110.0, base_price=100.0)

    result_lb3 = detect_swing_points(df, lookback_candles=3)
    highs_lb3 = [p for p in result_lb3 if p["type"] == "high"]
    assert len(highs_lb3) == 1
    assert highs_lb3[0]["date"] == df.iloc[3]["date"]

    # With lookback=5 the spike at index 3 has only 3 candles after it → not detected
    result_lb5 = detect_swing_points(df, lookback_candles=5)
    highs_lb5 = [p for p in result_lb5 if p["type"] == "high"]
    assert len(highs_lb5) == 0


def test_detect_multiple_swing_points() -> None:
    """60 rows of oscillating data with 3 clear peaks and 3 clear troughs."""
    records = []
    base = "2024-01-02"
    prices = []
    # Generate 3 peaks and 3 troughs in a sine-like pattern
    for i in range(60):
        if i in (10, 30, 50):
            h, l = 120.0, 99.5
        elif i in (20, 40):
            h, l = 100.5, 80.0
        else:
            h, l = 101.0, 99.0
        records.append({
            "date": (date.fromisoformat(base) + timedelta(days=i)).isoformat(),
            "open": 100.0,
            "high": h,
            "low": l,
            "close": 100.0,
            "volume": 200_000.0,
        })
    df = pd.DataFrame(records)
    result = detect_swing_points(df, lookback_candles=5)
    highs = [p for p in result if p["type"] == "high"]
    lows = [p for p in result if p["type"] == "low"]
    assert len(highs) == 3
    assert len(lows) == 2  # only 2 troughs fully surrounded by 5 neighbors each side


def test_swing_point_strength() -> None:
    """A spike that dominates more than the minimum lookback has higher strength."""
    # 21 rows: spike at index 10 dominates all 10 neighbors each side
    n = 21
    df = _make_spike_ohlcv(n, spike_index=10, spike_high=150.0, base_price=100.0)
    result = detect_swing_points(df, lookback_candles=5)
    highs = [p for p in result if p["type"] == "high"]
    assert len(highs) == 1
    # Strength should be >= 5 (it dominates at least 5 on each side, likely more)
    assert highs[0]["strength"] >= 5


def test_save_swing_points_to_db(db_connection: sqlite3.Connection) -> None:
    """Detected swing points are persisted to swing_points table with correct fields."""
    swing_pts = [
        {"date": "2024-01-10", "type": "high", "price": 110.0, "strength": 5},
        {"date": "2024-01-20", "type": "low", "price": 90.0, "strength": 3},
    ]
    count = save_swing_points_to_db(db_connection, "AAPL", swing_pts)
    assert count == 2

    rows = db_connection.execute(
        "SELECT * FROM swing_points WHERE ticker = 'AAPL' ORDER BY date"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["type"] == "high"
    assert rows[0]["price"] == pytest.approx(110.0)
    assert rows[1]["type"] == "low"
    assert rows[1]["price"] == pytest.approx(90.0)


def test_save_swing_points_is_idempotent(db_connection: sqlite3.Connection) -> None:
    """Saving twice produces no duplicates (UNIQUE on ticker, date, type)."""
    swing_pts = [
        {"date": "2024-01-10", "type": "high", "price": 110.0, "strength": 5},
    ]
    save_swing_points_to_db(db_connection, "AAPL", swing_pts)
    save_swing_points_to_db(db_connection, "AAPL", swing_pts)

    rows = db_connection.execute(
        "SELECT * FROM swing_points WHERE ticker = 'AAPL'"
    ).fetchall()
    assert len(rows) == 1


def test_detect_swing_points_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV into DB, call detect_swing_points_for_ticker, verify swing_points is populated."""
    n = 15
    df = _make_spike_ohlcv(n, spike_index=7, spike_high=120.0, base_price=100.0)
    _insert_ohlcv(db_connection, "AAPL", df)

    config = {"swing_points": {"lookback_candles": 5}}
    count = detect_swing_points_for_ticker(db_connection, "AAPL", config)
    assert count >= 1

    rows = db_connection.execute(
        "SELECT * FROM swing_points WHERE ticker = 'AAPL'"
    ).fetchall()
    assert len(rows) >= 1
    assert any(r["type"] == "high" for r in rows)
