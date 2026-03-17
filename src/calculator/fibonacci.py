"""
Fibonacci retracement level computation.

Computes Fibonacci levels from the most recent significant swing high/low pair.
Checks if the current price is near any Fibonacci level.

Levels: 23.6%, 38.2%, 50%, 61.8%, 78.6% (configurable)

Fibonacci levels act as potential support/resistance zones. When a fib level
coincides with an existing S/R level, the confluence adds extra weight.
"""

from __future__ import annotations

import logging
import sqlite3
from itertools import product

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_FIB_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]


def compute_fibonacci_levels(
    swing_low_price: float,
    swing_high_price: float,
    levels: list[float] | None = None,
) -> list[dict]:
    """
    Compute Fibonacci retracement levels for an uptrend (pullback from high).

    Each level price = swing_high - (swing_high - swing_low) * level_pct

    Args:
        swing_low_price: Price of the swing low (base of the move).
        swing_high_price: Price of the swing high (top of the move).
        levels: List of Fibonacci ratios (e.g. [0.236, 0.382, 0.5, 0.618, 0.786]).
            Defaults to [0.236, 0.382, 0.5, 0.618, 0.786].

    Returns:
        List of dicts with keys: level_pct (float), price (float).
    """
    if levels is None:
        levels = _DEFAULT_FIB_LEVELS

    price_range = swing_high_price - swing_low_price
    result = []
    for level_pct in levels:
        price = swing_high_price - price_range * level_pct
        result.append({"level_pct": level_pct, "price": price})
    return result


def find_significant_swing_pair(
    swing_points: list[dict], min_range_pct: float = 5.0
) -> tuple[dict, dict] | None:
    """
    Find the most recent swing high + swing low pair with a significant price range.

    'Significant' means the percentage range between the high and low exceeds
    min_range_pct. Examines the most recent swing points first.

    Args:
        swing_points: List of swing point dicts (date, type, price, strength).
        min_range_pct: Minimum percentage range (high - low) / low * 100 required.

    Returns:
        (swing_low_dict, swing_high_dict) tuple for the most recent significant pair,
        or None if no significant pair exists.
    """
    sorted_pts = sorted(swing_points, key=lambda p: p["date"], reverse=True)
    lows = [p for p in sorted_pts if p["type"] == "low"]
    highs = [p for p in sorted_pts if p["type"] == "high"]

    if not lows or not highs:
        return None

    best_pair: tuple[dict, dict] | None = None
    best_range_pct = 0.0

    for low_pt, high_pt in product(lows[:10], highs[:10]):
        if low_pt["price"] >= high_pt["price"]:
            continue
        range_pct = (high_pt["price"] - low_pt["price"]) / low_pt["price"] * 100.0
        if range_pct >= min_range_pct and range_pct > best_range_pct:
            best_range_pct = range_pct
            best_pair = (low_pt, high_pt)

    return best_pair


def check_price_near_level(
    current_price: float,
    fib_levels: list[dict],
    proximity_pct: float = 1.0,
) -> dict | None:
    """
    Check if current_price is within proximity_pct of any Fibonacci level.

    Args:
        current_price: The current market price to test.
        fib_levels: List of level dicts from compute_fibonacci_levels().
        proximity_pct: Maximum percentage distance to consider 'near'.

    Returns:
        Dict with keys: level_pct, level_price, distance_pct, is_near=True
        for the nearest level within proximity_pct, or None if not near any level.
    """
    nearest: dict | None = None
    nearest_distance = float("inf")

    for level in fib_levels:
        level_price = level["price"]
        if level_price == 0:
            continue
        distance_pct = abs(current_price - level_price) / level_price * 100.0
        if distance_pct <= proximity_pct and distance_pct < nearest_distance:
            nearest_distance = distance_pct
            nearest = {
                "level_pct": level["level_pct"],
                "level_price": level_price,
                "distance_pct": distance_pct,
                "is_near": True,
            }

    return nearest


def compute_fibonacci_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> dict | None:
    """
    Run full Fibonacci analysis for a ticker: find the most recent significant
    swing pair, compute retracement levels, and check if the current price is
    near any level.

    Note: Results are not stored in a dedicated table — they are computed on-the-fly
    and returned for use by the scorer.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["fibonacci"].

    Returns:
        Dict with keys: swing_low, swing_high, levels, current_price,
        nearest_level, is_near_level. Returns None if no significant swing pair
        is found or if there is no OHLCV data.
    """
    fib_cfg = config.get("fibonacci", {})
    fib_levels_config = fib_cfg.get("levels", _DEFAULT_FIB_LEVELS)
    proximity_pct = fib_cfg.get("proximity_pct", 1.0)
    min_range_pct = fib_cfg.get("min_range_pct", 5.0)

    swing_rows = db_conn.execute(
        "SELECT date, type, price, strength FROM swing_points WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not swing_rows:
        logger.warning("ticker=%s phase=fibonacci no swing points found", ticker)
        return None

    swing_points = [dict(r) for r in swing_rows]
    pair = find_significant_swing_pair(swing_points, min_range_pct=min_range_pct)
    if pair is None:
        logger.warning("ticker=%s phase=fibonacci no significant swing pair found", ticker)
        return None

    swing_low, swing_high = pair
    levels = compute_fibonacci_levels(swing_low["price"], swing_high["price"], fib_levels_config)

    latest_ohlcv = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if not latest_ohlcv:
        logger.warning("ticker=%s phase=fibonacci no OHLCV found", ticker)
        return None

    current_price = float(latest_ohlcv["close"])
    nearest_level = check_price_near_level(current_price, levels, proximity_pct)

    return {
        "swing_low": swing_low,
        "swing_high": swing_high,
        "levels": levels,
        "current_price": current_price,
        "nearest_level": nearest_level,
        "is_near_level": nearest_level is not None,
    }
