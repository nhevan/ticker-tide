"""
Price gap detection and classification.

Detects gaps between consecutive trading days and classifies them:
    - Gap Up:   today's low > yesterday's high
    - Gap Down: today's high < yesterday's low

Classification based on volume and trend context:
    - Breakaway:    gap on volume > 2x average, regardless of trend (strong conviction)
    - Exhaustion:   gap after extended trend (20+ days) with declining volume momentum
    - Continuation: gap in middle of existing trend (10+ days of directional move)
    - Common:       normal volume, no strong trend context
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

_TREND_CONTINUATION_DAYS = 10
_TREND_EXHAUSTION_DAYS = 20


def detect_gaps(ohlcv_df: pd.DataFrame, config: dict) -> list[dict]:
    """
    Walk through consecutive OHLCV rows and detect price gaps.

    Gap Up:   row[i].low > row[i-1].high
    Gap Down: row[i].high < row[i-1].low

    Computes:
        direction:    'up' or 'down'
        gap_size_pct: percentage size relative to previous close
            Gap Up:   (low_today - high_yesterday) / close_yesterday * 100
            Gap Down: (high_today - low_yesterday) / close_yesterday * 100 (negative)
        volume_ratio: today's volume / rolling average volume over the last N days
        date:         date of the gap day

    Args:
        ohlcv_df: DataFrame with columns: date, open, high, low, close, volume.
        config: Calculator config containing config["gaps"]["volume_average_period"].

    Returns:
        List of dicts with keys: date, direction, gap_size_pct, volume_ratio, gap_index.
    """
    gap_cfg = config.get("gaps", {})
    avg_period = gap_cfg.get("volume_average_period", 20)

    df = ohlcv_df.reset_index(drop=True)
    gaps: list[dict] = []

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        is_gap_up = curr["low"] > prev["high"]
        is_gap_down = curr["high"] < prev["low"]

        if not is_gap_up and not is_gap_down:
            continue

        direction = "up" if is_gap_up else "down"

        if is_gap_up:
            gap_size_pct = (curr["low"] - prev["high"]) / prev["close"] * 100
        else:
            gap_size_pct = (curr["high"] - prev["low"]) / prev["close"] * 100

        start = max(0, i - avg_period)
        avg_volume = df.iloc[start:i]["volume"].mean()
        volume_ratio = curr["volume"] / avg_volume if avg_volume > 0 else 1.0

        gaps.append(
            {
                "date": curr["date"],
                "direction": direction,
                "gap_size_pct": gap_size_pct,
                "volume_ratio": volume_ratio,
                "gap_index": i,
            }
        )

    return gaps


def classify_gap(
    gap: dict, ohlcv_df: pd.DataFrame, gap_index: int, config: dict
) -> str:
    """
    Classify a detected gap based on volume and trend context.

    Classification rules (in priority order):
        1. Breakaway:    volume_ratio > breakaway_threshold (default 2.0)
        2. Exhaustion:   gap after 20+ day trend AND declining volume momentum
        3. Continuation: gap in middle of 10+ day trend in same direction
        4. Common:       default

    Args:
        gap: Gap dict with keys: direction, volume_ratio.
        ohlcv_df: Full OHLCV DataFrame.
        gap_index: Row index of the gap day in ohlcv_df.
        config: Calculator config.

    Returns:
        Gap type string: 'breakaway', 'exhaustion', 'continuation', or 'common'.
    """
    gap_cfg = config.get("gaps", {})
    breakaway_threshold = gap_cfg.get("volume_breakaway_threshold", 2.0)
    direction = gap["direction"]
    volume_ratio = gap["volume_ratio"]

    if volume_ratio >= breakaway_threshold:
        return "breakaway"

    lookback = ohlcv_df.iloc[max(0, gap_index - _TREND_EXHAUSTION_DAYS):gap_index]
    exhaustion_trend = _is_trending(lookback, direction, min_days=_TREND_EXHAUSTION_DAYS)
    if exhaustion_trend and _is_volume_declining(lookback):
        return "exhaustion"

    cont_lookback = ohlcv_df.iloc[max(0, gap_index - _TREND_CONTINUATION_DAYS):gap_index]
    if _is_trending(cont_lookback, direction, min_days=_TREND_CONTINUATION_DAYS):
        return "continuation"

    return "common"


def _is_trending(df_slice: pd.DataFrame, direction: str, min_days: int) -> bool:
    """
    Return True if the slice shows a trend of at least min_days in the given direction.

    A trend is defined as the close price consistently moving in the given direction:
    more than half the days are up-days (for 'up') or down-days (for 'down').

    Args:
        df_slice: Subset of OHLCV DataFrame.
        direction: 'up' or 'down'.
        min_days: Minimum number of rows required.

    Returns:
        True if the trend condition is met.
    """
    if len(df_slice) < min_days:
        return False

    closes = df_slice["close"].values
    if len(closes) < 2:
        return False

    daily_changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    if direction == "up":
        positive_days = sum(1 for d in daily_changes if d > 0)
        return positive_days > len(daily_changes) * 0.6
    else:
        negative_days = sum(1 for d in daily_changes if d < 0)
        return negative_days > len(daily_changes) * 0.6


def _is_volume_declining(df_slice: pd.DataFrame) -> bool:
    """
    Return True if volume has been declining over the slice.

    Compares the average volume of the first half vs the second half.

    Args:
        df_slice: Subset of OHLCV DataFrame.

    Returns:
        True if later volume is lower than earlier volume.
    """
    if len(df_slice) < 4:
        return False

    midpoint = len(df_slice) // 2
    first_half_avg = df_slice.iloc[:midpoint]["volume"].mean()
    second_half_avg = df_slice.iloc[midpoint:]["volume"].mean()
    return second_half_avg < first_half_avg


def detect_and_classify_gaps(ohlcv_df: pd.DataFrame, config: dict) -> list[dict]:
    """
    Detect all gaps and classify each one.

    Calls detect_gaps to find all gaps, then classify_gap for each to determine type.

    Args:
        ohlcv_df: DataFrame with columns: date, open, high, low, close, volume.
        config: Calculator config dict.

    Returns:
        List of dicts with keys: date, direction, gap_type, gap_size_pct,
        volume_ratio, filled.
    """
    raw_gaps = detect_gaps(ohlcv_df, config)
    result: list[dict] = []

    for gap in raw_gaps:
        gap_type = classify_gap(gap, ohlcv_df, gap["gap_index"], config)
        result.append(
            {
                "date": gap["date"],
                "direction": gap["direction"],
                "gap_type": gap_type,
                "gap_size_pct": gap["gap_size_pct"],
                "volume_ratio": gap["volume_ratio"],
                "filled": False,
            }
        )

    return result


def save_gaps_to_db(
    db_conn: sqlite3.Connection, ticker: str, gaps: list[dict]
) -> int:
    """
    Replace all gap records for a ticker with fresh data.

    Deletes existing gaps for the ticker and inserts all new records.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        gaps: List of gap dicts with keys: date, direction, gap_type,
              gap_size_pct, volume_ratio, filled.

    Returns:
        Number of rows saved.
    """
    db_conn.execute("DELETE FROM gaps_daily WHERE ticker = ?", (ticker,))

    for gap in gaps:
        db_conn.execute(
            """INSERT INTO gaps_daily(ticker, date, gap_type, direction, gap_size_pct, volume_ratio, filled)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                ticker,
                gap["date"],
                gap["gap_type"],
                gap["direction"],
                gap["gap_size_pct"],
                gap["volume_ratio"],
                1 if gap.get("filled") else 0,
            ),
        )

    db_conn.commit()
    return len(gaps)


def detect_gaps_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> int:
    """
    Load OHLCV from DB, detect and classify all gaps, and save to gaps_daily.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.

    Returns:
        Number of gaps found and saved.
    """
    try:
        cursor = db_conn.execute(
            """SELECT date, open, high, low, close, volume
               FROM ohlcv_daily
               WHERE ticker = ?
               ORDER BY date ASC""",
            (ticker,),
        )
        rows = cursor.fetchall()

        if not rows:
            logger.warning(f"No OHLCV data found for {ticker} in detect_gaps_for_ticker")
            return 0

        ohlcv_df = pd.DataFrame([dict(row) for row in rows])
        gaps = detect_and_classify_gaps(ohlcv_df, config)
        count = save_gaps_to_db(db_conn, ticker, gaps)
        logger.info(f"Detected {count} gaps for {ticker}")
        return count

    except Exception as exc:
        logger.error(f"Failed to detect gaps for {ticker}: {exc}", exc_info=True)
        _log_alert(db_conn, ticker, "calculator-gaps", str(exc))
        return 0


def _log_alert(
    db_conn: sqlite3.Connection, ticker: str, phase: str, message: str
) -> None:
    """Write a failure record to alerts_log."""
    now = datetime.now(tz=timezone.utc).isoformat()
    try:
        db_conn.execute(
            """INSERT INTO alerts_log(ticker, date, phase, severity, message, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ticker, now[:10], phase, "ERROR", message, now),
        )
        db_conn.commit()
    except Exception:
        pass
