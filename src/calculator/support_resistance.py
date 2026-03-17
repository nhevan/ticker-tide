"""
Support and resistance level detection.

Clusters nearby swing points into price levels. A level is formed when 2+
swing points occur at approximately the same price (within a configurable
tolerance). More touches = stronger level.

Also detects when levels are broken (price closes beyond the level).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def cluster_into_sr_levels(
    swing_points: list[dict],
    price_tolerance_pct: float = 1.5,
    min_touches: int = 2,
) -> list[dict]:
    """
    Group swing points whose prices are within price_tolerance_pct of each other.

    For each cluster with >= min_touches:
        - level_price = average price of all points in the cluster
        - level_type = 'support' if majority are swing lows, 'resistance' if swing highs
        - touch_count = number of points in cluster
        - first_touch = earliest date, last_touch = latest date
        - strength = 'weak' if 2, 'moderate' if 3, 'strong' if 4+

    Uses a greedy clustering approach: sort by price, then greedily assign each
    point to an existing cluster if within tolerance, else start a new cluster.

    Args:
        swing_points: List of swing point dicts (date, type, price, strength).
        price_tolerance_pct: Maximum percentage difference between prices to be
            considered the same level.
        min_touches: Minimum number of swing points required to form a level.

    Returns:
        List of S/R level dicts.
    """
    if not swing_points:
        return []

    sorted_pts = sorted(swing_points, key=lambda p: p["price"])
    clusters: list[list[dict]] = []

    for point in sorted_pts:
        added = False
        for cluster in clusters:
            cluster_avg = sum(p["price"] for p in cluster) / len(cluster)
            diff_pct = abs(point["price"] - cluster_avg) / cluster_avg * 100.0
            if diff_pct <= price_tolerance_pct:
                cluster.append(point)
                added = True
                break
        if not added:
            clusters.append([point])

    levels: list[dict] = []
    for cluster in clusters:
        if len(cluster) < min_touches:
            continue

        level_price = sum(p["price"] for p in cluster) / len(cluster)
        high_count = sum(1 for p in cluster if p["type"] == "high")
        low_count = len(cluster) - high_count
        level_type = "resistance" if high_count >= low_count else "support"

        dates = sorted(p["date"] for p in cluster)
        touch_count = len(cluster)
        strength = _classify_strength(touch_count)

        levels.append({
            "level_price": level_price,
            "level_type": level_type,
            "touch_count": touch_count,
            "first_touch": dates[0],
            "last_touch": dates[-1],
            "strength": strength,
            "broken": False,
            "broken_date": None,
        })

    return levels


def _classify_strength(touch_count: int) -> str:
    """Return 'weak', 'moderate', or 'strong' based on touch count."""
    if touch_count >= 4:
        return "strong"
    if touch_count == 3:
        return "moderate"
    return "weak"


def check_broken_levels(
    sr_levels: list[dict], ohlcv_df: pd.DataFrame
) -> list[dict]:
    """
    For each S/R level, check if the most recent close has broken through it.

    Support broken: most recent close < level_price
    Resistance broken: most recent close > level_price

    Args:
        sr_levels: List of S/R level dicts.
        ohlcv_df: DataFrame with columns: date, close. Must have at least one row.

    Returns:
        Updated sr_levels list with broken and broken_date fields set.
    """
    if ohlcv_df.empty:
        return sr_levels

    last_row = ohlcv_df.iloc[-1]
    last_close = float(last_row["close"])
    last_date = str(last_row["date"])

    updated = []
    for level in sr_levels:
        level = dict(level)
        if level["level_type"] == "support" and last_close < level["level_price"]:
            level["broken"] = True
            level["broken_date"] = last_date
        elif level["level_type"] == "resistance" and last_close > level["level_price"]:
            level["broken"] = True
            level["broken_date"] = last_date
        updated.append(level)

    return updated


def save_sr_levels_to_db(
    db_conn: sqlite3.Connection, ticker: str, sr_levels: list[dict]
) -> int:
    """
    Delete existing S/R levels for this ticker and insert fresh ones.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        sr_levels: List of level dicts from cluster_into_sr_levels().

    Returns:
        Number of rows inserted.
    """
    now_utc = datetime.now(timezone.utc).date().isoformat()
    db_conn.execute("DELETE FROM support_resistance WHERE ticker = ?", (ticker,))
    if not sr_levels:
        db_conn.commit()
        return 0

    rows = [
        (
            ticker,
            now_utc,
            level["level_price"],
            level["level_type"],
            level["touch_count"],
            level["first_touch"],
            level["last_touch"],
            level["strength"],
            1 if level.get("broken") else 0,
            level.get("broken_date"),
        )
        for level in sr_levels
    ]
    db_conn.executemany(
        """INSERT INTO support_resistance
           (ticker, date_computed, level_price, level_type, touch_count,
            first_touch, last_touch, strength, broken, broken_date)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_conn.commit()
    logger.info("ticker=%s phase=support_resistance saved=%d levels", ticker, len(rows))
    return len(rows)


def detect_support_resistance_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> int:
    """
    Load swing points from DB, cluster into S/R levels, check for broken levels,
    and save results.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["support_resistance"].

    Returns:
        Number of S/R levels detected and saved.
    """
    sr_cfg = config.get("support_resistance", {})
    tolerance = sr_cfg.get("price_tolerance_pct", 1.5)
    min_touches = sr_cfg.get("min_touches", 2)

    swing_rows = db_conn.execute(
        "SELECT date, type, price, strength FROM swing_points WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not swing_rows:
        logger.warning("ticker=%s phase=support_resistance no swing points found", ticker)
        return 0

    swing_points = [dict(r) for r in swing_rows]

    try:
        sr_levels = cluster_into_sr_levels(swing_points, tolerance, min_touches)
    except Exception as exc:
        logger.error("ticker=%s phase=support_resistance cluster_error=%s", ticker, exc)
        db_conn.execute(
            "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
            (ticker, "sr_error", str(exc), datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
        return 0

    ohlcv_rows = db_conn.execute(
        "SELECT date, close FROM ohlcv_daily WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()
    ohlcv_df = pd.DataFrame([dict(r) for r in ohlcv_rows]) if ohlcv_rows else pd.DataFrame()

    if not ohlcv_df.empty:
        sr_levels = check_broken_levels(sr_levels, ohlcv_df)

    return save_sr_levels_to_db(db_conn, ticker, sr_levels)
