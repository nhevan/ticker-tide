"""
Candlestick and structural pattern detection.

Candlestick patterns (7):
  Bullish Engulfing, Bearish Engulfing, Hammer, Shooting Star,
  Doji, Morning Star, Evening Star

Structural patterns (computed from swing points + S/R):
  Double Top, Double Bottom, Bull Flag, Bear Flag,
  Breakout, Breakdown, False Breakout

All pattern parameters come from config/calculator.json.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# Number of preceding candles required to establish trend context for hammer/shooting star
_TREND_CONTEXT_CANDLES = 5


def detect_candlestick_patterns(ohlcv_df: pd.DataFrame, config: dict) -> list[dict]:
    """
    Walk through the OHLCV DataFrame and detect all 7 candlestick patterns.

    Patterns detected:
        - Bullish Engulfing: prior bearish body fully engulfed by current bullish body
        - Bearish Engulfing: prior bullish body fully engulfed by current bearish body
        - Hammer: small body at top, lower wick > 2x body, upper wick < 0.5x body,
                  requires preceding downtrend (5 declining closes)
        - Shooting Star: small body at bottom, upper wick > 2x body, lower wick < 0.5x body,
                         requires preceding uptrend (5 rising closes)
        - Doji: body < 10% of the candle's total range
        - Morning Star: 3-candle — large bearish, small body, large bullish closing above
                        midpoint of candle 1
        - Evening Star: 3-candle — large bullish, small body, large bearish closing below
                        midpoint of candle 1

    Strength is computed as a 1–5 score based on the body size relative to the
    rolling average body size over the preceding 10 candles.

    Args:
        ohlcv_df: DataFrame with columns: date, open, high, low, close, volume.
            Must be sorted by date ascending.
        config: Calculator config dict (currently unused for candlestick params but
            kept for API consistency with structural patterns).

    Returns:
        List of pattern dicts with keys: date, pattern_name, pattern_category,
        pattern_type, direction, strength, confirmed, details.
    """
    df = ohlcv_df.reset_index(drop=True)
    patterns: list[dict] = []

    # Precompute average body sizes for strength calculation
    bodies = (df["close"] - df["open"]).abs()

    for i in range(len(df)):
        row = df.iloc[i]
        body = abs(float(row["close"]) - float(row["open"]))
        candle_range = float(row["high"]) - float(row["low"])
        avg_body = float(bodies.iloc[max(0, i - 10): i].mean()) if i > 0 else body

        # ── Doji ─────────────────────────────────────────────────────────────
        if candle_range > 0 and body < 0.1 * candle_range:
            patterns.append(_make_pattern(
                date=row["date"],
                name="doji",
                pattern_type="indecision",
                direction="neutral",
                strength=_body_strength(body, avg_body),
            ))

        # ── Single-candle: Hammer & Shooting Star ─────────────────────────────
        if i >= _TREND_CONTEXT_CANDLES and body > 0:
            open_ = float(row["open"])
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])
            lower_wick = min(open_, close) - low
            upper_wick = high - max(open_, close)

            # Hammer: lower_wick > 2x body, upper_wick <= 0.5x body, downtrend context
            if lower_wick > 2.0 * body and upper_wick <= 0.5 * body:
                if _is_downtrend(df, i, _TREND_CONTEXT_CANDLES):
                    patterns.append(_make_pattern(
                        date=row["date"],
                        name="hammer",
                        pattern_type="reversal",
                        direction="bullish",
                        strength=_body_strength(body, avg_body),
                    ))

            # Shooting Star: upper_wick > 2x body, lower_wick <= 0.5x body, uptrend context
            if upper_wick > 2.0 * body and lower_wick <= 0.5 * body:
                if _is_uptrend(df, i, _TREND_CONTEXT_CANDLES):
                    patterns.append(_make_pattern(
                        date=row["date"],
                        name="shooting_star",
                        pattern_type="reversal",
                        direction="bearish",
                        strength=_body_strength(body, avg_body),
                    ))

        # ── Two-candle patterns ───────────────────────────────────────────────
        if i >= 1:
            prev = df.iloc[i - 1]
            prev_open = float(prev["open"])
            prev_close = float(prev["close"])
            curr_open = float(row["open"])
            curr_close = float(row["close"])
            prev_body = abs(prev_close - prev_open)

            # Bullish Engulfing: prev bearish, curr bullish, curr engulfs prev
            if (prev_close < prev_open  # prev bearish
                    and curr_close > curr_open  # curr bullish
                    and curr_open <= prev_close
                    and curr_close >= prev_open):
                patterns.append(_make_pattern(
                    date=row["date"],
                    name="bullish_engulfing",
                    pattern_type="reversal",
                    direction="bullish",
                    strength=_body_strength(body, avg_body),
                ))

            # Bearish Engulfing: prev bullish, curr bearish, curr engulfs prev
            if (prev_close > prev_open  # prev bullish
                    and curr_close < curr_open  # curr bearish
                    and curr_open >= prev_close
                    and curr_close <= prev_open):
                patterns.append(_make_pattern(
                    date=row["date"],
                    name="bearish_engulfing",
                    pattern_type="reversal",
                    direction="bearish",
                    strength=_body_strength(body, avg_body),
                ))

        # ── Three-candle patterns ─────────────────────────────────────────────
        if i >= 2:
            d1 = df.iloc[i - 2]
            d2 = df.iloc[i - 1]
            d3 = df.iloc[i]

            d1_open, d1_close = float(d1["open"]), float(d1["close"])
            d2_open, d2_close = float(d2["open"]), float(d2["close"])
            d3_open, d3_close = float(d3["open"]), float(d3["close"])
            d1_body = abs(d1_close - d1_open)
            d2_body = abs(d2_close - d2_open)
            d3_body = abs(d3_close - d3_open)
            avg3 = float(bodies.iloc[max(0, i - 10): i - 2].mean()) if i > 2 else d1_body

            d1_midpoint = (d1_open + d1_close) / 2.0

            # Morning Star: large bearish, small body, large bullish above d1 midpoint
            if (d1_close < d1_open  # day 1 bearish
                    and d1_body > avg3 * 0.8
                    and d2_body < d1_body * 0.4
                    and d3_close > d3_open  # day 3 bullish
                    and d3_body > avg3 * 0.8
                    and d3_close > d1_midpoint):
                patterns.append(_make_pattern(
                    date=d3["date"],
                    name="morning_star",
                    pattern_type="reversal",
                    direction="bullish",
                    strength=_body_strength(d3_body, avg3),
                ))

            # Evening Star: large bullish, small body, large bearish below d1 midpoint
            if (d1_close > d1_open  # day 1 bullish
                    and d1_body > avg3 * 0.8
                    and d2_body < d1_body * 0.4
                    and d3_close < d3_open  # day 3 bearish
                    and d3_body > avg3 * 0.8
                    and d3_close < d1_midpoint):
                patterns.append(_make_pattern(
                    date=d3["date"],
                    name="evening_star",
                    pattern_type="reversal",
                    direction="bearish",
                    strength=_body_strength(d3_body, avg3),
                ))

    return patterns


def _make_pattern(
    date: str,
    name: str,
    pattern_type: str,
    direction: str,
    strength: int,
) -> dict:
    """Build a candlestick pattern dict with standard fields."""
    return {
        "date": date,
        "pattern_name": name,
        "pattern_category": "candlestick",
        "pattern_type": pattern_type,
        "direction": direction,
        "strength": strength,
        "confirmed": False,
        "details": None,
    }


def _body_strength(body: float, avg_body: float) -> int:
    """Compute 1–5 strength based on current body relative to average body."""
    if avg_body <= 0:
        return 1
    ratio = body / avg_body
    if ratio >= 2.5:
        return 5
    if ratio >= 1.8:
        return 4
    if ratio >= 1.2:
        return 3
    if ratio >= 0.7:
        return 2
    return 1


def _is_downtrend(df: pd.DataFrame, index: int, lookback: int) -> bool:
    """Return True if closes have been generally declining over the prior lookback candles."""
    closes = [float(df.iloc[index - lookback + i]["close"]) for i in range(lookback)]
    declining = sum(1 for j in range(1, len(closes)) if closes[j] < closes[j - 1])
    return declining >= lookback * 0.6


def _is_uptrend(df: pd.DataFrame, index: int, lookback: int) -> bool:
    """Return True if closes have been generally rising over the prior lookback candles."""
    closes = [float(df.iloc[index - lookback + i]["close"]) for i in range(lookback)]
    rising = sum(1 for j in range(1, len(closes)) if closes[j] > closes[j - 1])
    return rising >= lookback * 0.6


# ── Structural pattern detection ─────────────────────────────────────────────────


def detect_structural_patterns(
    ohlcv_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    swing_points: list[dict],
    sr_levels: list[dict],
    config: dict,
) -> list[dict]:
    """
    Detect Double Top, Double Bottom, Bull Flag, Bear Flag, Breakout, Breakdown,
    and False Breakout patterns.

    Args:
        ohlcv_df: DataFrame with OHLCV data sorted by date ascending.
        indicators_df: DataFrame with indicator data (currently unused but kept for
            future indicator-based structural signals).
        swing_points: List of swing point dicts (date, type, price, strength).
        sr_levels: List of S/R level dicts.
        config: Calculator config dict containing config["patterns"].

    Returns:
        List of structural pattern dicts with pattern_category="structural".
    """
    patterns_cfg = config.get("patterns", {})
    patterns: list[dict] = []

    patterns.extend(_detect_double_patterns(ohlcv_df, swing_points, patterns_cfg))
    patterns.extend(_detect_flags(ohlcv_df, patterns_cfg))
    patterns.extend(_detect_breakouts(ohlcv_df, sr_levels, patterns_cfg))

    return patterns


def _detect_double_patterns(
    ohlcv_df: pd.DataFrame, swing_points: list[dict], patterns_cfg: dict
) -> list[dict]:
    """Detect Double Top and Double Bottom patterns from swing points.

    Only the most recent qualifying adjacent pair is used for each pattern type.
    Iterates from the most recent adjacent pair backward and stops at the first match,
    preventing multiple detections of stale formations across 5 years of data.
    """
    dt_cfg = patterns_cfg.get("double_top_bottom", {})
    tolerance_pct = dt_cfg.get("price_tolerance_pct", 1.5)
    min_days = dt_cfg.get("min_days_between", 10)
    max_days = dt_cfg.get("max_days_between", 60)

    if ohlcv_df.empty:
        return []

    last_close = float(ohlcv_df.iloc[-1]["close"])
    patterns: list[dict] = []

    highs = sorted([p for p in swing_points if p["type"] == "high"], key=lambda p: p["date"])
    lows = sorted([p for p in swing_points if p["type"] == "low"], key=lambda p: p["date"])

    # ── Double Top: scan from most recent adjacent high pair backward, stop at first match ──
    for i in range(len(highs) - 2, -1, -1):
        peak1, peak2 = highs[i], highs[i + 1]
        distance_days = (
            date.fromisoformat(peak2["date"]) - date.fromisoformat(peak1["date"])
        ).days
        if distance_days < min_days or distance_days > max_days:
            continue
        price_diff_pct = abs(peak2["price"] - peak1["price"]) / peak1["price"] * 100.0
        if price_diff_pct > tolerance_pct:
            continue

        # Find trough between the two peaks
        trough_lows = [lo for lo in lows if peak1["date"] < lo["date"] < peak2["date"]]
        if not trough_lows:
            continue
        neckline = min(trough_lows, key=lambda lo: lo["price"])["price"]

        # Signal when price closes below neckline
        if last_close >= neckline:
            continue

        peak_price = (peak1["price"] + peak2["price"]) / 2.0
        patterns.append({
            "date": ohlcv_df.iloc[-1]["date"],
            "pattern_name": "double_top",
            "pattern_category": "structural",
            "pattern_type": "reversal",
            "direction": "bearish",
            "strength": 4,
            "confirmed": True,
            "details": json.dumps({
                "peak_price": peak_price,
                "neckline_price": neckline,
                "distance_days": distance_days,
            }),
        })
        break  # only the most recent qualifying formation

    # ── Double Bottom: scan from most recent adjacent low pair backward, stop at first match ──
    for i in range(len(lows) - 2, -1, -1):
        trough1, trough2 = lows[i], lows[i + 1]
        distance_days = (
            date.fromisoformat(trough2["date"]) - date.fromisoformat(trough1["date"])
        ).days
        if distance_days < min_days or distance_days > max_days:
            continue
        price_diff_pct = abs(trough2["price"] - trough1["price"]) / trough1["price"] * 100.0
        if price_diff_pct > tolerance_pct:
            continue

        # Find peak between the two troughs (neckline for double bottom)
        peak_highs = [hi for hi in highs if trough1["date"] < hi["date"] < trough2["date"]]
        if not peak_highs:
            continue
        neckline = max(peak_highs, key=lambda hi: hi["price"])["price"]

        # Signal when price closes above neckline
        if last_close <= neckline:
            continue

        trough_price = (trough1["price"] + trough2["price"]) / 2.0
        patterns.append({
            "date": ohlcv_df.iloc[-1]["date"],
            "pattern_name": "double_bottom",
            "pattern_category": "structural",
            "pattern_type": "reversal",
            "direction": "bullish",
            "strength": 4,
            "confirmed": True,
            "details": json.dumps({
                "trough_price": trough_price,
                "neckline_price": neckline,
                "distance_days": distance_days,
            }),
        })
        break  # only the most recent qualifying formation

    return patterns


def _detect_flags(ohlcv_df: pd.DataFrame, patterns_cfg: dict) -> list[dict]:
    """Detect Bull Flag and Bear Flag patterns from OHLCV data.

    Scans backward from the most recent candle. Stops as soon as it finds one
    qualifying bull_flag and one qualifying bear_flag, guaranteeing at most 2
    flag patterns regardless of dataset length.

    The search is also bounded to the last 90 candles so only recent formations
    are returned — historical flags from 3+ years ago are not relevant to the
    current signal.
    """
    flag_cfg = patterns_cfg.get("flag", {})
    pole_min_atr = flag_cfg.get("pole_min_atr_multiple", 2.0)
    pole_max_days = flag_cfg.get("pole_max_days", 10)
    flag_min_days = flag_cfg.get("flag_min_days", 5)
    flag_max_days = flag_cfg.get("flag_max_days", 15)
    flag_ret_min = flag_cfg.get("flag_retracement_min_pct", 20)
    flag_ret_max = flag_cfg.get("flag_retracement_max_pct", 50)

    if len(ohlcv_df) < pole_max_days + flag_min_days + 5:
        return []

    df = ohlcv_df.reset_index(drop=True)
    n = len(df)

    # Compute ATR as simple rolling high-low range average over first 20 candles
    atr_window = min(20, n // 2)
    atr = float((df["high"] - df["low"]).iloc[:atr_window].mean())
    if atr <= 0:
        return []

    # Limit search to the most recent 90 candles to find current formations only
    search_start = max(0, n - 90)

    patterns: list[dict] = []
    found_bull = False
    found_bear = False

    # Scan from most recent pole_end backward — stop once both types found
    pole_end_start = n - flag_min_days - 1
    pole_end_stop = max(search_start, pole_max_days) - 1
    for pole_end in range(pole_end_start, pole_end_stop, -1):
        if found_bull and found_bear:
            break

        pole_start_start = pole_end - 1
        pole_start_stop = max(search_start, pole_end - pole_max_days) - 1
        for pole_start in range(pole_start_start, pole_start_stop, -1):
            pole_move = float(df.iloc[pole_end]["close"]) - float(df.iloc[pole_start]["open"])
            pole_range = abs(pole_move)

            if pole_range < pole_min_atr * atr:
                continue

            is_bull = pole_move > 0
            is_bear = pole_move < 0

            if not (is_bull or is_bear):
                continue

            # Skip if we already found this direction
            if is_bull and found_bull:
                continue
            if is_bear and found_bear:
                continue

            # Verify pole is a consecutive directional move
            pole_candles = df.iloc[pole_start: pole_end + 1]
            if is_bull:
                if not all(
                    float(pole_candles.iloc[j]["close"]) >= float(pole_candles.iloc[j - 1]["open"])
                    for j in range(1, len(pole_candles))
                ):
                    continue
            else:
                if not all(
                    float(pole_candles.iloc[j]["close"]) <= float(pole_candles.iloc[j - 1]["open"])
                    for j in range(1, len(pole_candles))
                ):
                    continue

            # Scan for first qualifying flag after the pole
            for flag_end in range(pole_end + flag_min_days, min(pole_end + flag_max_days + 1, n)):
                flag_candles = df.iloc[pole_end + 1: flag_end + 1]
                if len(flag_candles) < flag_min_days:
                    continue

                flag_move = float(flag_candles.iloc[-1]["close"]) - float(flag_candles.iloc[0]["open"])
                retracement_pct = abs(flag_move) / pole_range * 100.0

                if not (flag_ret_min <= retracement_pct <= flag_ret_max):
                    continue

                # Bull flag: flag retraces downward against the up-pole
                if is_bull and flag_move < 0:
                    patterns.append({
                        "date": df.iloc[flag_end]["date"],
                        "pattern_name": "bull_flag",
                        "pattern_category": "structural",
                        "pattern_type": "continuation",
                        "direction": "bullish",
                        "strength": 3,
                        "confirmed": False,
                        "details": json.dumps({
                            "pole_start_price": float(df.iloc[pole_start]["open"]),
                            "pole_end_price": float(df.iloc[pole_end]["close"]),
                            "flag_retracement_pct": round(retracement_pct, 2),
                        }),
                    })
                    found_bull = True
                    break  # first qualifying flag_end per pole

                # Bear flag: flag retraces upward against the down-pole
                elif is_bear and flag_move > 0:
                    patterns.append({
                        "date": df.iloc[flag_end]["date"],
                        "pattern_name": "bear_flag",
                        "pattern_category": "structural",
                        "pattern_type": "continuation",
                        "direction": "bearish",
                        "strength": 3,
                        "confirmed": False,
                        "details": json.dumps({
                            "pole_start_price": float(df.iloc[pole_start]["open"]),
                            "pole_end_price": float(df.iloc[pole_end]["close"]),
                            "flag_retracement_pct": round(retracement_pct, 2),
                        }),
                    })
                    found_bear = True
                    break  # first qualifying flag_end per pole

            if found_bull and found_bear:
                break

    return patterns


def _detect_breakouts(
    ohlcv_df: pd.DataFrame, sr_levels: list[dict], patterns_cfg: dict
) -> list[dict]:
    """Detect Breakout, Breakdown, and False Breakout using S/R levels and volume."""
    volume_threshold = patterns_cfg.get("breakout_volume_threshold", 1.5)
    volume_avg_period = 20

    if len(ohlcv_df) < volume_avg_period + 1 or not sr_levels:
        return []

    df = ohlcv_df.reset_index(drop=True)
    n = len(df)
    patterns: list[dict] = []

    for level in sr_levels:
        level_price = level["level_price"]
        level_type = level["level_type"]

        for i in range(volume_avg_period, n):
            curr = df.iloc[i]
            curr_close = float(curr["close"])
            prev_close = float(df.iloc[i - 1]["close"])
            curr_volume = float(curr["volume"])

            avg_volume = float(df.iloc[i - volume_avg_period: i]["volume"].mean())
            if avg_volume <= 0:
                continue
            volume_ratio = curr_volume / avg_volume

            if volume_ratio < volume_threshold:
                continue

            # ── Breakout: only on the crossing transition (prev below, curr above) ────
            if level_type == "resistance" and curr_close > level_price and prev_close <= level_price:
                # Check for false breakout: reversal within 2 candles
                if i + 1 < n:
                    look_ahead = min(2, n - i - 1)
                    future_closes = [float(df.iloc[i + j]["close"]) for j in range(1, look_ahead + 1)]
                    if any(fc < level_price for fc in future_closes):
                        patterns.append({
                            "date": curr["date"],
                            "pattern_name": "false_breakout",
                            "pattern_category": "structural",
                            "pattern_type": "reversal",
                            "direction": "bearish",
                            "strength": 2,
                            "confirmed": False,
                            "details": json.dumps({"level_price": level_price}),
                        })
                        continue

                patterns.append({
                    "date": curr["date"],
                    "pattern_name": "breakout",
                    "pattern_category": "structural",
                    "pattern_type": "continuation",
                    "direction": "bullish",
                    "strength": 3,
                    "confirmed": True,
                    "details": json.dumps({
                        "level_price": level_price,
                        "volume_ratio": round(volume_ratio, 2),
                    }),
                })

            # ── Breakdown: only on the crossing transition (prev above, curr below) ────
            elif level_type == "support" and curr_close < level_price and prev_close >= level_price:
                patterns.append({
                    "date": curr["date"],
                    "pattern_name": "breakdown",
                    "pattern_category": "structural",
                    "pattern_type": "continuation",
                    "direction": "bearish",
                    "strength": 3,
                    "confirmed": True,
                    "details": json.dumps({
                        "level_price": level_price,
                        "volume_ratio": round(volume_ratio, 2),
                    }),
                })

    return patterns


def _deduplicate_patterns(patterns: list[dict]) -> list[dict]:
    """
    Deduplicate a list of pattern dicts by (date, pattern_name, direction).

    When multiple candidates share the same key, keep only the one with the
    highest strength value.

    Args:
        patterns: Raw list of pattern dicts, possibly containing duplicates.

    Returns:
        Deduplicated list with at most one entry per (date, pattern_name, direction).
    """
    best: dict[tuple, dict] = {}
    for p in patterns:
        key = (p["date"], p["pattern_name"], p["direction"])
        existing = best.get(key)
        if existing is None or p["strength"] > existing["strength"]:
            best[key] = p
    return list(best.values())


def save_patterns_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    patterns: list[dict],
    category: str,
) -> int:
    """
    Delete existing patterns for this ticker and category, then insert fresh ones.

    Candlestick and structural patterns are managed independently so re-running
    one category does not delete the other.

    Before inserting, deduplicates by (date, pattern_name, direction), keeping only
    the candidate with the highest strength for each unique combination.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        patterns: List of pattern dicts.
        category: 'candlestick' or 'structural'.

    Returns:
        Number of rows inserted.
    """
    db_conn.execute(
        "DELETE FROM patterns_daily WHERE ticker = ? AND pattern_category = ?",
        (ticker, category),
    )
    if not patterns:
        db_conn.commit()
        return 0

    deduped = _deduplicate_patterns(patterns)

    rows = [
        (
            ticker,
            p["date"],
            p["pattern_name"],
            p["pattern_category"],
            p["pattern_type"],
            p["direction"],
            p["strength"],
            1 if p.get("confirmed") else 0,
            p.get("details"),
        )
        for p in deduped
    ]
    db_conn.executemany(
        """INSERT INTO patterns_daily
           (ticker, date, pattern_name, pattern_category, pattern_type,
            direction, strength, confirmed, details)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_conn.commit()
    logger.info(
        "ticker=%s phase=patterns category=%s saved=%d patterns",
        ticker, category, len(rows),
    )
    return len(rows)


def detect_all_patterns_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> dict:
    """
    Run full pattern detection for a ticker: load OHLCV, indicators, swing points,
    and S/R levels from DB, detect candlestick and structural patterns, save both.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["patterns"].

    Returns:
        Dict with keys: candlestick_count (int), structural_count (int).
    """
    ohlcv_rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()
    ohlcv_df = pd.DataFrame([dict(r) for r in ohlcv_rows]) if ohlcv_rows else pd.DataFrame()

    ind_rows = db_conn.execute(
        "SELECT * FROM indicators_daily WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()
    indicators_df = pd.DataFrame([dict(r) for r in ind_rows]) if ind_rows else pd.DataFrame()

    swing_rows = db_conn.execute(
        "SELECT date, type, price, strength FROM swing_points WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()
    swing_points = [dict(r) for r in swing_rows]

    sr_rows = db_conn.execute(
        "SELECT level_price, level_type, touch_count, first_touch, last_touch, "
        "strength, broken, broken_date FROM support_resistance WHERE ticker = ?",
        (ticker,),
    ).fetchall()
    sr_levels = [dict(r) for r in sr_rows]

    candlestick_count = 0
    structural_count = 0

    if not ohlcv_df.empty:
        try:
            candlestick_patterns = detect_candlestick_patterns(ohlcv_df, config)
            candlestick_count = save_patterns_to_db(db_conn, ticker, candlestick_patterns, "candlestick")
        except Exception as exc:
            logger.error("ticker=%s phase=patterns category=candlestick error=%s", ticker, exc)
            db_conn.execute(
                "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
                (ticker, "patterns_candlestick_error", str(exc), datetime.now(timezone.utc).isoformat()),
            )
            db_conn.commit()

        try:
            structural_patterns = detect_structural_patterns(
                ohlcv_df, indicators_df, swing_points, sr_levels, config
            )
            structural_count = save_patterns_to_db(db_conn, ticker, structural_patterns, "structural")
        except Exception as exc:
            logger.error("ticker=%s phase=patterns category=structural error=%s", ticker, exc)
            db_conn.execute(
                "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
                (ticker, "patterns_structural_error", str(exc), datetime.now(timezone.utc).isoformat()),
            )
            db_conn.commit()

    return {"candlestick_count": candlestick_count, "structural_count": structural_count}
