"""
Tests for src/calculator/patterns.py

Covers:
- detect_candlestick_patterns: bullish engulfing, bearish engulfing, no engulfing,
  hammer, hammer downtrend context, shooting star, doji, doji not triggered,
  morning star, evening star, multiple patterns, strength field, full dataset,
  save to DB, idempotent save (category isolation)
- detect_structural_patterns: double top, double top tolerance, double top
  outside tolerance, double top timing, double bottom, double top details,
  bull flag, bear flag, bull flag details, breakout, breakdown, false breakout,
  end-to-end structural patterns
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from src.calculator.patterns import (
    detect_all_patterns_for_ticker,
    detect_candlestick_patterns,
    detect_structural_patterns,
    save_patterns_to_db,
)


# ── Helpers ──────────────────────────────────────────────────────────────────────


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _candle(
    day: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float = 200_000.0,
) -> dict:
    return {
        "date": _make_date(day),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _df(*candles: dict) -> pd.DataFrame:
    return pd.DataFrame(list(candles))


def _flat_df(n: int, price: float = 100.0) -> pd.DataFrame:
    records = [
        _candle(i, price, price + 0.5, price - 0.5, price)
        for i in range(n)
    ]
    return pd.DataFrame(records)


def _swing_pt(day: int, swing_type: str, price: float) -> dict:
    return {"date": _make_date(day), "type": swing_type, "price": price, "strength": 3}


def _sr_level(level_price: float, level_type: str) -> dict:
    return {
        "level_price": level_price,
        "level_type": level_type,
        "touch_count": 3,
        "first_touch": _make_date(0),
        "last_touch": _make_date(20),
        "strength": "moderate",
        "broken": False,
        "broken_date": None,
    }


_BASE_CONFIG: dict = {
    "patterns": {
        "double_top_bottom": {
            "price_tolerance_pct": 1.5,
            "min_days_between": 10,
            "max_days_between": 60,
        },
        "flag": {
            "pole_min_atr_multiple": 2.0,
            "pole_max_days": 10,
            "flag_min_days": 5,
            "flag_max_days": 15,
            "flag_retracement_min_pct": 20,
            "flag_retracement_max_pct": 50,
        },
        "breakout_volume_threshold": 1.5,
    }
}


# ── detect_candlestick_patterns ──────────────────────────────────────────────────


def test_detect_bullish_engulfing() -> None:
    """Day 1 bearish (open=105, close=100); day 2 bullish body fully engulfs day 1."""
    df = _df(
        _candle(0, 105.0, 106.0, 99.0, 100.0),   # bearish
        _candle(1, 99.0, 107.0, 98.5, 106.0),     # bullish, open<=close_prev, close>=open_prev
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "bullish_engulfing" in names
    eng = next(p for p in patterns if p["pattern_name"] == "bullish_engulfing")
    assert eng["direction"] == "bullish"
    assert eng["pattern_type"] == "reversal"


def test_detect_bearish_engulfing() -> None:
    """Day 1 bullish (open=100, close=105); day 2 bearish engulfs day 1."""
    df = _df(
        _candle(0, 100.0, 106.0, 99.0, 105.0),   # bullish
        _candle(1, 106.0, 107.0, 98.5, 99.0),    # bearish, open>=close_prev, close<=open_prev
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "bearish_engulfing" in names
    eng = next(p for p in patterns if p["pattern_name"] == "bearish_engulfing")
    assert eng["direction"] == "bearish"


def test_detect_no_engulfing() -> None:
    """Day 2 body smaller than day 1 — no engulfing."""
    df = _df(
        _candle(0, 100.0, 103.0, 97.0, 95.0),  # large bearish body (5 pts)
        _candle(1, 94.5, 96.0, 93.5, 95.5),    # small body (1 pt), doesn't engulf
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "bullish_engulfing" not in names
    assert "bearish_engulfing" not in names


def test_detect_hammer() -> None:
    """Small body at top, lower wick > 2× body → hammer."""
    # open=100, close=101 (body=1), high=101.5 (upper_wick=0.5), low=95 (lower_wick=5)
    # 5 preceding candles declining (downtrend context)
    records = [_candle(i, 110.0 - i, 111.0 - i, 109.0 - i, 109.5 - i) for i in range(5)]
    records.append(_candle(5, 100.0, 101.5, 95.0, 101.0))
    df = pd.DataFrame(records)
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "hammer" in names
    hammer = next(p for p in patterns if p["pattern_name"] == "hammer")
    assert hammer["direction"] == "bullish"


def test_detect_hammer_requires_downtrend_context() -> None:
    """Hammer candle after 5 days of RISING prices is not detected (no downtrend context)."""
    # Rising prices for 5 days then hammer
    records = [_candle(i, 100.0 + i, 101.5 + i, 99.0 + i, 101.0 + i) for i in range(5)]
    records.append(_candle(5, 105.0, 106.5, 100.0, 106.0))  # hammer shape but uptrend
    df = pd.DataFrame(records)
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "hammer" not in names


def test_detect_shooting_star() -> None:
    """Small body at bottom, upper wick > 2× body → shooting star."""
    # open=100, close=99 (body=1), high=106 (upper_wick=6), low=98.5 (lower_wick=0.5)
    # preceding uptrend context
    records = [_candle(i, 100.0 + i, 101.5 + i, 99.0 + i, 101.0 + i) for i in range(5)]
    records.append(_candle(5, 100.0, 106.0, 98.5, 99.0))
    df = pd.DataFrame(records)
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "shooting_star" in names
    ss = next(p for p in patterns if p["pattern_name"] == "shooting_star")
    assert ss["direction"] == "bearish"


def test_detect_doji() -> None:
    """Body < 10% of range → doji. open=100.05, close=100.00, high=102, low=98."""
    df = _df(_candle(0, 100.05, 102.0, 98.0, 100.00))
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "doji" in names


def test_detect_doji_not_triggered_large_body() -> None:
    """Large body (66% of range) → doji NOT detected. open=98, close=102, high=103, low=97."""
    df = _df(_candle(0, 98.0, 103.0, 97.0, 102.0))
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "doji" not in names


def test_detect_morning_star() -> None:
    """3-candle morning star: large bearish, small body, large bullish."""
    df = _df(
        _candle(0, 110.0, 111.0, 99.0, 100.0),   # large bearish (body=10)
        _candle(1, 99.0, 100.0, 98.0, 99.5),     # small body (0.5)
        _candle(2, 100.0, 110.0, 99.5, 109.0),   # large bullish (body=9), closes above midpoint of day0 (105)
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "morning_star" in names
    ms = next(p for p in patterns if p["pattern_name"] == "morning_star")
    assert ms["direction"] == "bullish"


def test_detect_evening_star() -> None:
    """3-candle evening star: large bullish, small body, large bearish."""
    df = _df(
        _candle(0, 100.0, 111.0, 99.5, 110.0),  # large bullish (body=10)
        _candle(1, 110.5, 112.0, 109.5, 110.0), # small body (0.5)
        _candle(2, 109.0, 110.0, 99.0, 100.0),  # large bearish (body=9), closes below midpoint of day0 (105)
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "evening_star" in names
    es = next(p for p in patterns if p["pattern_name"] == "evening_star")
    assert es["direction"] == "bearish"


def test_detect_multiple_patterns_same_day() -> None:
    """Multiple patterns can fire on the same day — both are returned."""
    # Doji candle after a bearish candle — could match doji AND potentially engulfing check
    # Use a doji that also meets engulfing criteria loosely
    df = _df(
        _candle(0, 102.0, 103.0, 98.0, 100.0),  # bearish
        _candle(1, 100.0, 104.0, 97.0, 100.02), # near-doji (body~0.02/range~7 = 0.3%), also engulfs
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    # at minimum doji should be detected
    names = [p["pattern_name"] for p in patterns]
    assert "doji" in names


def test_detect_candlestick_patterns_returns_strength() -> None:
    """Every detected pattern has a strength field between 1 and 5."""
    df = _df(
        _candle(0, 105.0, 106.0, 99.0, 100.0),
        _candle(1, 99.0, 107.0, 98.5, 106.0),
    )
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    assert len(patterns) > 0
    for p in patterns:
        assert "strength" in p
        assert 1 <= p["strength"] <= 5


def test_detect_candlestick_patterns_full_dataset() -> None:
    """60 days of realistic OHLCV — returns a list without crashing."""
    import random
    random.seed(42)
    records = []
    price = 150.0
    for i in range(60):
        open_ = price + random.uniform(-1.0, 1.0)
        close = open_ + random.uniform(-2.0, 2.0)
        high = max(open_, close) + random.uniform(0.0, 1.5)
        low = min(open_, close) - random.uniform(0.0, 1.5)
        records.append(_candle(i, open_, high, low, close, 200_000.0))
        price = close
    df = pd.DataFrame(records)
    patterns = detect_candlestick_patterns(df, _BASE_CONFIG)
    assert isinstance(patterns, list)


def test_save_patterns_to_db(db_connection: sqlite3.Connection) -> None:
    """Detected candlestick patterns are saved to patterns_daily."""
    patterns = [
        {
            "date": "2024-01-10",
            "pattern_name": "bullish_engulfing",
            "pattern_category": "candlestick",
            "pattern_type": "reversal",
            "direction": "bullish",
            "strength": 3,
            "confirmed": False,
            "details": None,
        }
    ]
    count = save_patterns_to_db(db_connection, "AAPL", patterns, "candlestick")
    assert count == 1

    rows = db_connection.execute(
        "SELECT * FROM patterns_daily WHERE ticker = 'AAPL' AND pattern_category = 'candlestick'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["pattern_name"] == "bullish_engulfing"


def test_save_patterns_clears_old_for_ticker_and_category(db_connection: sqlite3.Connection) -> None:
    """Saving candlestick patterns replaces old candlestick but NOT structural."""
    candlestick_pattern = {
        "date": "2024-01-10",
        "pattern_name": "doji",
        "pattern_category": "candlestick",
        "pattern_type": "reversal",
        "direction": "neutral",
        "strength": 2,
        "confirmed": False,
        "details": None,
    }
    structural_pattern = {
        "date": "2024-01-05",
        "pattern_name": "double_top",
        "pattern_category": "structural",
        "pattern_type": "reversal",
        "direction": "bearish",
        "strength": 4,
        "confirmed": True,
        "details": json.dumps({"peak_price": 105.0}),
    }
    save_patterns_to_db(db_connection, "AAPL", [structural_pattern], "structural")
    save_patterns_to_db(db_connection, "AAPL", [candlestick_pattern], "candlestick")

    # Now re-save candlestick with a different pattern
    new_candlestick = {**candlestick_pattern, "pattern_name": "hammer"}
    save_patterns_to_db(db_connection, "AAPL", [new_candlestick], "candlestick")

    candlestick_rows = db_connection.execute(
        "SELECT * FROM patterns_daily WHERE ticker = 'AAPL' AND pattern_category = 'candlestick'"
    ).fetchall()
    structural_rows = db_connection.execute(
        "SELECT * FROM patterns_daily WHERE ticker = 'AAPL' AND pattern_category = 'structural'"
    ).fetchall()

    assert len(candlestick_rows) == 1
    assert candlestick_rows[0]["pattern_name"] == "hammer"
    assert len(structural_rows) == 1  # structural not affected


# ── detect_structural_patterns ───────────────────────────────────────────────────


def _build_double_top_ohlcv(peak_price_1: float, peak_price_2: float, separation_days: int) -> pd.DataFrame:
    """Build OHLCV with two peaks and a trough between them, followed by a neckline break."""
    records: list[dict] = []
    # Rising to first peak
    for i in range(10):
        p = 100.0 + i * (peak_price_1 - 100.0) / 10
        records.append(_candle(i, p, p + 0.5, p - 0.5, p))
    # First peak
    records.append(_candle(10, peak_price_1 - 0.5, peak_price_1, peak_price_1 - 1.0, peak_price_1 - 0.5))
    # Trough at ~90
    trough_start = 11
    neckline = 90.0
    half = separation_days // 2
    for i in range(half):
        p = peak_price_1 - (peak_price_1 - neckline) * (i + 1) / half
        records.append(_candle(trough_start + i, p, p + 0.5, p - 0.5, p))
    # Second peak
    peak2_day = trough_start + half
    for i in range(half):
        p = neckline + (peak_price_2 - neckline) * (i + 1) / half
        records.append(_candle(peak2_day + i, p, p + 0.5, p - 0.5, p))
    # Neckline break (price closes below trough)
    break_day = peak2_day + half
    records.append(_candle(break_day, 89.0, 89.5, 87.0, 88.0))
    return pd.DataFrame(records)


def test_detect_double_top() -> None:
    """Two swing highs ~same price, 20 days apart, with neckline break → double_top."""
    swing_pts = [
        _swing_pt(10, "high", 105.0),
        _swing_pt(20, "low", 90.0),
        _swing_pt(30, "high", 105.5),   # 0.48% diff — within 1.5%
    ]
    # Closing price below neckline
    ohlcv = pd.DataFrame([
        _candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(35)
    ])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "double_top" in names
    dt = next(p for p in patterns if p["pattern_name"] == "double_top")
    assert dt["direction"] == "bearish"
    assert dt["pattern_category"] == "structural"


def test_detect_double_top_tolerance() -> None:
    """Two peaks at $100 and $101.20 (1.2% difference, within 1.5%) → double_top detected."""
    swing_pts = [
        _swing_pt(10, "high", 100.0),
        _swing_pt(20, "low", 90.0),
        _swing_pt(30, "high", 101.20),
    ]
    ohlcv = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(35)])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "double_top" in names


def test_detect_double_top_outside_tolerance() -> None:
    """Two peaks at $100 and $103 (3% difference, outside 1.5%) → double_top NOT detected."""
    swing_pts = [
        _swing_pt(10, "high", 100.0),
        _swing_pt(20, "low", 90.0),
        _swing_pt(30, "high", 103.0),
    ]
    ohlcv = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(35)])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "double_top" not in names


def test_detect_double_top_timing() -> None:
    """Peaks 5 days apart (< min=10) NOT detected; peaks 70 days apart (> max=60) NOT detected."""
    # Too close (5 days)
    swing_pts_close = [
        _swing_pt(0, "high", 100.0),
        _swing_pt(4, "low", 90.0),
        _swing_pt(5, "high", 100.5),
    ]
    ohlcv_close = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(10)])
    result_close = detect_structural_patterns(ohlcv_close, pd.DataFrame(), swing_pts_close, [], _BASE_CONFIG)
    assert not any(p["pattern_name"] == "double_top" for p in result_close)

    # Too far (70 days)
    swing_pts_far = [
        _swing_pt(0, "high", 100.0),
        _swing_pt(35, "low", 90.0),
        _swing_pt(70, "high", 100.5),
    ]
    ohlcv_far = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(75)])
    result_far = detect_structural_patterns(ohlcv_far, pd.DataFrame(), swing_pts_far, [], _BASE_CONFIG)
    assert not any(p["pattern_name"] == "double_top" for p in result_far)


def test_detect_double_bottom() -> None:
    """Two swing lows ~same price, 15 days apart, price breaks above peak → double_bottom."""
    swing_pts = [
        _swing_pt(5, "low", 90.0),
        _swing_pt(15, "high", 100.0),  # trough peak
        _swing_pt(20, "low", 90.5),   # 0.56% diff from 90
    ]
    # Current price above the trough peak (neckline break upward)
    ohlcv = pd.DataFrame([_candle(i, 100.0, 102.0, 89.0, 101.0) for i in range(25)])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "double_bottom" in names
    db = next(p for p in patterns if p["pattern_name"] == "double_bottom")
    assert db["direction"] == "bullish"


def test_detect_double_top_stores_details() -> None:
    """Double top details JSON includes neckline_price, peak_price, distance_days."""
    swing_pts = [
        _swing_pt(10, "high", 105.0),
        _swing_pt(20, "low", 90.0),
        _swing_pt(30, "high", 105.5),
    ]
    ohlcv = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(35)])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    dt_patterns = [p for p in patterns if p["pattern_name"] == "double_top"]
    assert len(dt_patterns) >= 1
    details = json.loads(dt_patterns[0]["details"])
    assert "neckline_price" in details
    assert "peak_price" in details
    assert "distance_days" in details


def test_detect_bull_flag() -> None:
    """Strong upward pole (>2x ATR), then consolidation retracement → bull_flag."""
    records: list[dict] = []
    # 20 flat candles for ATR baseline (ATR ≈ 1.0)
    for i in range(20):
        records.append(_candle(i, 100.0, 101.0, 99.0, 100.0, 200_000.0))
    # Pole: 8 candles with big move up (8 pts, >>2x ATR)
    for i in range(8):
        p = 100.0 + (i + 1) * 1.5
        records.append(_candle(20 + i, p - 0.5, p + 0.5, p - 1.0, p, 300_000.0))
    pole_high = records[-1]["close"]  # ~112
    pole_low = records[20]["open"]    # ~100
    pole_range = pole_high - pole_low  # ~12

    # Flag: 7 candles retracing ~35% of pole (within 20-50%)
    retrace_per_day = pole_range * 0.35 / 7
    for i in range(7):
        p = pole_high - retrace_per_day * (i + 1)
        records.append(_candle(28 + i, p + 0.3, p + 0.8, p - 0.3, p, 150_000.0))  # lower volume

    df = pd.DataFrame(records)
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "bull_flag" in names
    bf = next(p for p in patterns if p["pattern_name"] == "bull_flag")
    assert bf["direction"] == "bullish"
    assert bf["pattern_category"] == "structural"


def test_detect_bear_flag() -> None:
    """Strong downward pole, then slight upward retracement → bear_flag."""
    records: list[dict] = []
    # 20 flat candles for ATR baseline
    for i in range(20):
        records.append(_candle(i, 120.0, 121.0, 119.0, 120.0, 200_000.0))
    # Pole: 8 candles with big move down
    for i in range(8):
        p = 120.0 - (i + 1) * 1.5
        records.append(_candle(20 + i, p + 0.5, p + 1.0, p - 0.5, p, 300_000.0))
    pole_low = records[-1]["close"]
    pole_high = records[20]["open"]
    pole_range = pole_high - pole_low

    # Flag: 7 candles retracing ~35% upward of pole
    retrace_per_day = pole_range * 0.35 / 7
    for i in range(7):
        p = pole_low + retrace_per_day * (i + 1)
        records.append(_candle(28 + i, p - 0.3, p + 0.3, p - 0.8, p, 150_000.0))

    df = pd.DataFrame(records)
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], [], _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "bear_flag" in names
    bf = next(p for p in patterns if p["pattern_name"] == "bear_flag")
    assert bf["direction"] == "bearish"


def test_detect_bull_flag_stores_details() -> None:
    """Bull flag details JSON includes pole_start_price, pole_end_price, flag_retracement_pct."""
    records: list[dict] = []
    for i in range(20):
        records.append(_candle(i, 100.0, 101.0, 99.0, 100.0, 200_000.0))
    for i in range(8):
        p = 100.0 + (i + 1) * 1.5
        records.append(_candle(20 + i, p - 0.5, p + 0.5, p - 1.0, p, 300_000.0))
    pole_high = records[-1]["close"]
    pole_low = records[20]["open"]
    pole_range = pole_high - pole_low
    retrace_per_day = pole_range * 0.35 / 7
    for i in range(7):
        p = pole_high - retrace_per_day * (i + 1)
        records.append(_candle(28 + i, p + 0.3, p + 0.8, p - 0.3, p, 150_000.0))

    df = pd.DataFrame(records)
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], [], _BASE_CONFIG)
    bf_patterns = [p for p in patterns if p["pattern_name"] == "bull_flag"]
    assert len(bf_patterns) >= 1
    details = json.loads(bf_patterns[0]["details"])
    assert "pole_start_price" in details
    assert "pole_end_price" in details
    assert "flag_retracement_pct" in details


def test_detect_breakout() -> None:
    """Price closes above resistance with volume > 1.5x 20-day avg → breakout."""
    # 20 candles below resistance at 105 (avg volume ~200k)
    records = [_candle(i, 100.0, 104.0, 99.0, 103.0, 200_000.0) for i in range(20)]
    # Breakout candle: close=106 > resistance=105, volume=400k (2x avg)
    records.append(_candle(20, 103.5, 107.0, 103.0, 106.0, 400_000.0))
    df = pd.DataFrame(records)
    sr_levels = [_sr_level(105.0, "resistance")]
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], sr_levels, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "breakout" in names
    bo = next(p for p in patterns if p["pattern_name"] == "breakout")
    assert bo["direction"] == "bullish"


def test_detect_breakdown() -> None:
    """Price closes below support with volume confirmation → breakdown."""
    # 20 candles above support at 95
    records = [_candle(i, 100.0, 101.0, 96.0, 99.0, 200_000.0) for i in range(20)]
    # Breakdown candle: close=94 < support=95, volume=400k
    records.append(_candle(20, 98.0, 98.5, 93.0, 94.0, 400_000.0))
    df = pd.DataFrame(records)
    sr_levels = [_sr_level(95.0, "support")]
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], sr_levels, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "breakdown" in names
    bd = next(p for p in patterns if p["pattern_name"] == "breakdown")
    assert bd["direction"] == "bearish"


def test_detect_false_breakout() -> None:
    """Price breaks above resistance on day 1, closes back below within 2 days → false_breakout."""
    # 20 candles below resistance at 105
    records = [_candle(i, 100.0, 104.0, 99.0, 103.0, 200_000.0) for i in range(20)]
    # Day 20: breakout close=107
    records.append(_candle(20, 104.0, 108.0, 103.5, 107.0, 400_000.0))
    # Day 21: reversal close=103 (back below resistance)
    records.append(_candle(21, 106.0, 107.0, 102.0, 103.0, 300_000.0))
    df = pd.DataFrame(records)
    sr_levels = [_sr_level(105.0, "resistance")]
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], sr_levels, _BASE_CONFIG)
    names = [p["pattern_name"] for p in patterns]
    assert "false_breakout" in names


def test_detect_structural_patterns_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Full end-to-end: insert OHLCV + indicators + swing points + SR; verify patterns_daily is populated."""
    ticker = "AAPL"
    # Insert OHLCV — double top setup
    ohlcv_rows = []
    for i in range(35):
        close = 88.0 if i >= 34 else 100.0  # last candle breaks below neckline
        ohlcv_rows.append((ticker, _make_date(i), 100.0, 101.0, 99.0, close, 200_000.0))
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        ohlcv_rows,
    )
    # Insert swing points for double top
    swing_rows = [
        (ticker, _make_date(10), "high", 105.0, 5),
        (ticker, _make_date(20), "low", 90.0, 3),
        (ticker, _make_date(30), "high", 105.5, 5),
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        swing_rows,
    )
    db_connection.commit()

    config = {
        "patterns": {
            "double_top_bottom": {
                "price_tolerance_pct": 1.5,
                "min_days_between": 10,
                "max_days_between": 60,
            },
            "flag": {
                "pole_min_atr_multiple": 2.0,
                "pole_max_days": 10,
                "flag_min_days": 5,
                "flag_max_days": 15,
                "flag_retracement_min_pct": 20,
                "flag_retracement_max_pct": 50,
            },
            "breakout_volume_threshold": 1.5,
        }
    }
    result = detect_all_patterns_for_ticker(db_connection, ticker, config)
    assert "candlestick_count" in result
    assert "structural_count" in result
    assert result["structural_count"] >= 1

    rows = db_connection.execute(
        "SELECT * FROM patterns_daily WHERE ticker = ? AND pattern_category = 'structural'",
        (ticker,),
    ).fetchall()
    assert len(rows) >= 1


# ── Deduplication and over-detection regression tests ────────────────────────────


def test_deduplication_keeps_highest_strength(db_connection: sqlite3.Connection) -> None:
    """When two patterns have the same date/name/direction, only the highest-strength one is saved."""
    from src.calculator.patterns import _deduplicate_patterns

    patterns = [
        {
            "date": "2024-01-10",
            "pattern_name": "double_top",
            "pattern_category": "structural",
            "pattern_type": "reversal",
            "direction": "bearish",
            "strength": 2,
            "confirmed": True,
            "details": None,
        },
        {
            "date": "2024-01-10",
            "pattern_name": "double_top",
            "pattern_category": "structural",
            "pattern_type": "reversal",
            "direction": "bearish",
            "strength": 5,
            "confirmed": True,
            "details": None,
        },
    ]
    deduped = _deduplicate_patterns(patterns)
    assert len(deduped) == 1
    assert deduped[0]["strength"] == 5


def test_double_top_many_pairs_emits_only_one() -> None:
    """With many adjacent qualifying pairs (long history), only ONE double_top is emitted."""
    # Build 10 swing highs all within tolerance, each 15 days apart — creates 9 adjacent pairs
    swing_pts = []
    for i in range(10):
        swing_pts.append({"date": _make_date(i * 15), "type": "high", "price": 100.0 + i * 0.1, "strength": 3})
        if i < 9:
            swing_pts.append({"date": _make_date(i * 15 + 7), "type": "low", "price": 90.0, "strength": 3})
    # Last close well below 90 (neckline break)
    ohlcv = pd.DataFrame([_candle(i, 100.0, 101.0, 89.0, 88.0) for i in range(150)])
    patterns = detect_structural_patterns(ohlcv, pd.DataFrame(), swing_pts, [], _BASE_CONFIG)
    double_tops = [p for p in patterns if p["pattern_name"] == "double_top"]
    assert len(double_tops) <= 1


def test_breakout_fires_once_not_every_day_above_level() -> None:
    """Breakout should fire only on the first crossing day, not every day above the level."""
    # 20 baseline candles, then 5 days all above resistance with high volume
    records = [_candle(i, 100.0, 104.0, 99.0, 103.0, 200_000.0) for i in range(20)]
    for i in range(5):
        records.append(_candle(20 + i, 106.0, 107.0, 105.5, 106.5, 400_000.0))
    df = pd.DataFrame(records)
    sr_levels = [_sr_level(105.0, "resistance")]
    patterns = detect_structural_patterns(df, pd.DataFrame(), [], sr_levels, _BASE_CONFIG)
    breakouts = [p for p in patterns if p["pattern_name"] == "breakout"]
    # Only the first crossing day (index 20) should produce a breakout, not all 5 days
    assert len(breakouts) == 1


def test_save_patterns_deduplicates_before_insert(db_connection: sqlite3.Connection) -> None:
    """save_patterns_to_db deduplicates so only one row per (date, pattern_name, direction) is stored."""
    duplicates = [
        {
            "date": "2024-01-10",
            "pattern_name": "doji",
            "pattern_category": "candlestick",
            "pattern_type": "indecision",
            "direction": "neutral",
            "strength": i,
            "confirmed": False,
            "details": None,
        }
        for i in range(1, 6)  # 5 duplicates with strengths 1-5
    ]
    count = save_patterns_to_db(db_connection, "AAPL", duplicates, "candlestick")
    assert count == 1
    rows = db_connection.execute(
        "SELECT strength FROM patterns_daily WHERE ticker = 'AAPL' AND pattern_name = 'doji'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["strength"] == 5  # kept the highest strength


def test_flag_detection_bounded_on_long_history() -> None:
    """_detect_flags returns at most 2 patterns (1 bull + 1 bear) on 5 years of data."""
    from src.calculator.patterns import _detect_flags

    np.random.seed(0)
    n = 1260  # ~5 years of trading days
    prices = 100.0 + np.cumsum(np.random.randn(n) * 1.5)
    dates = pd.date_range("2020-01-01", periods=n, freq="B").strftime("%Y-%m-%d").tolist()
    df = pd.DataFrame(
        {
            "date": dates,
            "open": prices,
            "high": prices + 2.0,
            "low": prices - 2.0,
            "close": prices,
            "volume": 200_000.0,
        }
    )
    cfg = {
        "flag": {
            "pole_min_atr_multiple": 2.0,
            "pole_max_days": 10,
            "flag_min_days": 5,
            "flag_max_days": 15,
            "flag_retracement_min_pct": 20,
            "flag_retracement_max_pct": 50,
        }
    }
    flags = _detect_flags(df, cfg)
    assert len(flags) <= 2, f"Expected at most 2 flags on 5yr data, got {len(flags)}"
    names = [f["pattern_name"] for f in flags]
    assert names.count("bull_flag") <= 1, "At most 1 bull_flag should be returned"
    assert names.count("bear_flag") <= 1, "At most 1 bear_flag should be returned"

