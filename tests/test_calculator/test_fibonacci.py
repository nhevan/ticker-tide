"""
Tests for src/calculator/fibonacci.py

Covers:
- compute_fibonacci_levels: uptrend, downtrend, config levels
- check_price_near_level: near, not near
- find_significant_swing_pair: finds recent significant pair
- compute_fibonacci_for_ticker: full analysis, no swing points
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from src.calculator.fibonacci import (
    check_price_near_level,
    compute_fibonacci_for_ticker,
    compute_fibonacci_levels,
    find_significant_swing_pair,
)


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _swing(d: str, swing_type: str, price: float) -> dict:
    return {"date": d, "type": swing_type, "price": price, "strength": 3}


_DEFAULT_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]


# ── compute_fibonacci_levels ─────────────────────────────────────────────────────


def test_compute_fibonacci_levels_uptrend() -> None:
    """Retracement levels from low=100 to high=150 (uptrend pullback from high)."""
    levels = compute_fibonacci_levels(100.0, 150.0, _DEFAULT_LEVELS)
    prices = {round(lv["level_pct"], 3): lv["price"] for lv in levels}

    assert prices[0.236] == pytest.approx(138.20, abs=0.05)
    assert prices[0.382] == pytest.approx(130.90, abs=0.05)
    assert prices[0.5] == pytest.approx(125.00, abs=0.05)
    assert prices[0.618] == pytest.approx(119.10, abs=0.05)
    assert prices[0.786] == pytest.approx(110.70, abs=0.05)


def test_compute_fibonacci_levels_downtrend() -> None:
    """With high=150, low=100 the retracement levels go upward from the low."""
    levels = compute_fibonacci_levels(100.0, 150.0, _DEFAULT_LEVELS)
    # All levels are between low and high
    prices = [lv["price"] for lv in levels]
    assert all(100.0 <= p <= 150.0 for p in prices)
    # 61.8% < 50% < 38.2% direction (from high downward)
    level_map = {lv["level_pct"]: lv["price"] for lv in levels}
    assert level_map[0.236] > level_map[0.382] > level_map[0.5] > level_map[0.618] > level_map[0.786]


def test_compute_fibonacci_uses_config_levels() -> None:
    """Exactly 5 levels computed when config provides 5 ratios."""
    levels = compute_fibonacci_levels(100.0, 150.0, [0.236, 0.382, 0.5, 0.618, 0.786])
    assert len(levels) == 5


# ── check_price_near_level ───────────────────────────────────────────────────────


def test_check_price_near_fibonacci_level() -> None:
    """Price $130.50 is 0.31% away from fib level $130.90 (< 1%) → flagged as near."""
    fib_levels = [{"level_pct": 0.382, "price": 130.90}]
    result = check_price_near_level(130.50, fib_levels, proximity_pct=1.0)
    assert result is not None
    assert result["is_near"] is True
    assert result["level_pct"] == pytest.approx(0.382)
    assert result["distance_pct"] == pytest.approx(0.306, abs=0.05)


def test_check_price_not_near_fibonacci_level() -> None:
    """Price $135.00 is 3.1% from nearest fib level $130.90 (> 1%) → not near."""
    fib_levels = [{"level_pct": 0.382, "price": 130.90}]
    result = check_price_near_level(135.00, fib_levels, proximity_pct=1.0)
    assert result is None


# ── find_significant_swing_pair ──────────────────────────────────────────────────


def test_find_significant_swing_pair() -> None:
    """Returns the most recent high/low pair with > 5% price range."""
    swing_pts = [
        _swing(_make_date(0), "low", 100.0),
        _swing(_make_date(10), "high", 120.0),   # 20% range — significant
        _swing(_make_date(20), "low", 118.0),
        _swing(_make_date(30), "high", 119.5),   # only 1.3% range from low 118 — not significant
    ]
    result = find_significant_swing_pair(swing_pts, min_range_pct=5.0)
    assert result is not None
    swing_low, swing_high = result
    assert swing_low["price"] == pytest.approx(100.0)
    assert swing_high["price"] == pytest.approx(120.0)


def test_find_significant_swing_pair_no_pair() -> None:
    """Only swing lows, no highs → no pair found."""
    swing_pts = [
        _swing(_make_date(0), "low", 100.0),
        _swing(_make_date(5), "low", 99.0),
    ]
    result = find_significant_swing_pair(swing_pts, min_range_pct=5.0)
    assert result is None


# ── compute_fibonacci_for_ticker ─────────────────────────────────────────────────


def test_fibonacci_analysis_for_ticker(db_connection: sqlite3.Connection) -> None:
    """Full fibonacci analysis: finds swing pair, computes levels, checks current price."""
    ticker = "AAPL"
    # Insert OHLCV with current price ~130
    ohlcv_rows = [
        (ticker, _make_date(i), 130.0, 131.0, 129.0, 130.0, 200_000.0)
        for i in range(10)
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        ohlcv_rows,
    )
    # Insert swing points: low=100, high=150 → 50 point range (50% retracement at 125, near 130)
    db_connection.execute(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        (ticker, _make_date(0), "low", 100.0, 5),
    )
    db_connection.execute(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        (ticker, _make_date(5), "high", 150.0, 5),
    )
    db_connection.commit()

    config = {
        "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0}
    }
    result = compute_fibonacci_for_ticker(db_connection, ticker, config)
    assert result is not None
    assert "levels" in result
    assert "current_price" in result
    assert "nearest_level" in result
    assert "is_near_level" in result
    assert len(result["levels"]) == 5
    assert result["current_price"] == pytest.approx(130.0)


def test_fibonacci_no_swing_points(db_connection: sqlite3.Connection) -> None:
    """No swing points in DB → fibonacci analysis returns None."""
    ticker = "MSFT"
    config = {
        "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0}
    }
    result = compute_fibonacci_for_ticker(db_connection, ticker, config)
    assert result is None
