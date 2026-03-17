"""
Swing point detection.

A swing high is a candle whose high is higher than the N candles on BOTH sides.
A swing low is a candle whose low is lower than the N candles on BOTH sides.

N is configurable via calculator.json (default 5).

Swing points are the foundation for:
  - Support/Resistance levels
  - Double Top/Bottom patterns
  - Divergence detection (comparing indicator values at swing points)
  - Fibonacci retracement levels
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def detect_swing_points(ohlcv_df: pd.DataFrame, lookback_candles: int = 5) -> list[dict]:
    """
    Detect swing highs and swing lows in an OHLCV DataFrame.

    A swing high is a candle whose high exceeds all highs in the lookback_candles
    candles immediately before and after it. A swing low is similarly the lowest low.

    Strength is computed by extending the comparison beyond lookback_candles and
    counting how many candles on each side the swing point dominates. Strength equals
    the minimum of the left and right domination counts.

    Args:
        ohlcv_df: DataFrame with columns: date, open, high, low, close, volume.
            Must be sorted by date ascending.
        lookback_candles: Number of candles required on each side for detection.

    Returns:
        List of dicts with keys: date (str), type ('high' or 'low'), price (float),
        strength (int).
    """
    df = ohlcv_df.reset_index(drop=True)
    n = len(df)
    swing_points: list[dict] = []

    for i in range(lookback_candles, n - lookback_candles):
        curr_high = df.iloc[i]["high"]
        curr_low = df.iloc[i]["low"]
        curr_date = df.iloc[i]["date"]

        # ── Swing High check ────────────────────────────────────────────────
        left_highs = df.iloc[i - lookback_candles: i]["high"]
        right_highs = df.iloc[i + 1: i + lookback_candles + 1]["high"]

        if (curr_high > left_highs).all() and (curr_high > right_highs).all():
            strength = _compute_strength(df, i, "high", lookback_candles, n)
            swing_points.append({
                "date": curr_date,
                "type": "high",
                "price": float(curr_high),
                "strength": strength,
            })

        # ── Swing Low check ─────────────────────────────────────────────────
        left_lows = df.iloc[i - lookback_candles: i]["low"]
        right_lows = df.iloc[i + 1: i + lookback_candles + 1]["low"]

        if (curr_low < left_lows).all() and (curr_low < right_lows).all():
            strength = _compute_strength(df, i, "low", lookback_candles, n)
            swing_points.append({
                "date": curr_date,
                "type": "low",
                "price": float(curr_low),
                "strength": strength,
            })

    return swing_points


def _compute_strength(
    df: pd.DataFrame, index: int, point_type: str, min_lookback: int, n: int
) -> int:
    """
    Extend comparison beyond the minimum lookback and count how many candles
    on each side the swing point dominates.

    Args:
        df: Full OHLCV DataFrame (reset index).
        index: Row index of the swing point.
        point_type: 'high' or 'low'.
        min_lookback: Minimum lookback already confirmed.
        n: Total number of rows in df.

    Returns:
        Strength as the minimum of left and right domination counts.
    """
    value = df.iloc[index]["high"] if point_type == "high" else df.iloc[index]["low"]
    col = "high" if point_type == "high" else "low"

    left_count = 0
    for j in range(index - 1, -1, -1):
        if point_type == "high" and df.iloc[j][col] < value:
            left_count += 1
        elif point_type == "low" and df.iloc[j][col] > value:
            left_count += 1
        else:
            break

    right_count = 0
    for j in range(index + 1, n):
        if point_type == "high" and df.iloc[j][col] < value:
            right_count += 1
        elif point_type == "low" and df.iloc[j][col] > value:
            right_count += 1
        else:
            break

    return min(left_count, right_count)


def save_swing_points_to_db(
    db_conn: sqlite3.Connection, ticker: str, swing_points: list[dict]
) -> int:
    """
    Delete existing swing points for this ticker and insert fresh ones.

    Uses INSERT OR REPLACE to handle the UNIQUE(ticker, date, type) constraint.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        swing_points: List of dicts from detect_swing_points().

    Returns:
        Number of rows inserted.
    """
    db_conn.execute("DELETE FROM swing_points WHERE ticker = ?", (ticker,))
    if not swing_points:
        db_conn.commit()
        return 0

    rows = [
        (ticker, sp["date"], sp["type"], sp["price"], sp["strength"])
        for sp in swing_points
    ]
    db_conn.executemany(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        rows,
    )
    db_conn.commit()
    logger.info("ticker=%s phase=swing_points saved=%d swing points", ticker, len(rows))
    return len(rows)


def detect_swing_points_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> int:
    """
    Load OHLCV from DB, detect swing points, save results to DB.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["swing_points"]["lookback_candles"].

    Returns:
        Number of swing points detected and saved.
    """
    lookback = config.get("swing_points", {}).get("lookback_candles", 5)

    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        logger.warning("ticker=%s phase=swing_points no OHLCV data found", ticker)
        return 0

    ohlcv_df = pd.DataFrame([dict(r) for r in rows])

    try:
        swing_points = detect_swing_points(ohlcv_df, lookback_candles=lookback)
    except Exception as exc:
        logger.error("ticker=%s phase=swing_points error=%s", ticker, exc)
        db_conn.execute(
            "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
            (ticker, "swing_points_error", str(exc), datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
        return 0

    return save_swing_points_to_db(db_conn, ticker, swing_points)
