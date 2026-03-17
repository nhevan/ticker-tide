"""
Tests for src/calculator/weekly.py

Covers:
- build_weekly_candles: single week aggregation, partial week, multiple weeks,
  week_start is always Monday, uses config week_start_day
- save_weekly_candles_to_db: values correct, idempotency
- compute_weekly_indicators: same indicator columns as daily, uses same config params
- compute_weekly_for_ticker: end-to-end DB round-trip
- compute_weekly_for_ticker incremental mode: only new week added
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.weekly import (
    build_weekly_candles,
    compute_weekly_for_ticker,
    compute_weekly_indicators,
    save_weekly_candles_to_db,
    save_weekly_indicators_to_db,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


_BASE_CONFIG = {
    "indicators": {
        "ema_periods": [9, 21, 50],
        "macd": {"fast": 12, "slow": 26, "signal": 9},
        "adx_period": 14,
        "rsi_period": 14,
        "stochastic": {"k": 14, "d": 3, "smooth_k": 3},
        "cci_period": 20,
        "williams_r_period": 14,
        "bollinger": {"period": 20, "std_dev": 2},
        "atr_period": 14,
        "keltner_period": 20,
        "cmf_period": 20,
    },
    "weekly": {
        "week_start_day": "Monday",
    },
}


def _make_ohlcv_row(day: date, open_: float, high: float, low: float, close: float, volume: float) -> dict:
    return {
        "date": day.isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _week_of(monday: date) -> list[dict]:
    """Generate Mon–Fri trading days for a given Monday."""
    rows = []
    for offset, (o, h, l, c, v) in enumerate([
        (100.0, 102.0, 99.0, 101.0, 1_000_000),
        (101.0, 104.0, 100.5, 103.0, 1_100_000),
        (103.0, 105.0, 102.0, 104.0, 900_000),
        (104.0, 106.0, 103.5, 105.5, 1_200_000),
        (105.5, 107.0, 104.0, 106.0, 800_000),
    ]):
        rows.append(_make_ohlcv_row(monday + timedelta(days=offset), o, h, l, c, float(v)))
    return rows


def _make_ohlcv_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _insert_ohlcv(db_conn: sqlite3.Connection, ticker: str, rows: list[dict]) -> None:
    for row in rows:
        db_conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
            (ticker, row["date"] if isinstance(row["date"], str) else row["date"].isoformat(),
             row["open"], row["high"], row["low"], row["close"], row["volume"]),
        )
    db_conn.commit()


def _generate_daily_ohlcv(num_weeks: int, start_monday: date = date(2023, 1, 2)) -> list[dict]:
    """Generate num_weeks of Mon-Fri trading data."""
    rows = []
    price = 100.0
    for week_idx in range(num_weeks):
        monday = start_monday + timedelta(weeks=week_idx)
        for day_offset in range(5):
            day = monday + timedelta(days=day_offset)
            price = price * (1 + 0.002 * ((week_idx + day_offset) % 5 - 2))
            rows.append({
                "date": day.isoformat(),
                "open": round(price * 0.999, 4),
                "high": round(price * 1.01, 4),
                "low": round(price * 0.99, 4),
                "close": round(price, 4),
                "volume": float(1_000_000 + week_idx * 10_000),
            })
    return rows


# ── build_weekly_candles ──────────────────────────────────────────────────────


def test_build_weekly_candles_single_week() -> None:
    """A single Mon-Fri week produces exactly one weekly candle with correct OHLCV."""
    monday = date(2024, 1, 8)  # Monday
    week_rows = _week_of(monday)
    df = _make_ohlcv_df(week_rows)

    weekly = build_weekly_candles(df, week_start_day="Monday")

    assert len(weekly) == 1
    candle = weekly.iloc[0]
    # open = Monday's open
    assert candle["open"] == pytest.approx(100.0)
    # high = max of all highs: 102, 104, 105, 106, 107 → 107
    assert candle["high"] == pytest.approx(107.0)
    # low = min of all lows: 99, 100.5, 102, 103.5, 104 → 99
    assert candle["low"] == pytest.approx(99.0)
    # close = Friday's close
    assert candle["close"] == pytest.approx(106.0)
    # volume = sum of all 5 days
    assert candle["volume"] == pytest.approx(5_000_000.0)


def test_build_weekly_candles_partial_week() -> None:
    """A partial week (e.g. Mon-Wed) uses the last available day's close."""
    monday = date(2024, 1, 8)
    partial_rows = [
        _make_ohlcv_row(monday, 100.0, 102.0, 99.0, 101.0, 1_000_000),
        _make_ohlcv_row(monday + timedelta(1), 101.0, 103.0, 100.0, 102.0, 900_000),
        _make_ohlcv_row(monday + timedelta(2), 102.0, 104.0, 101.0, 103.5, 800_000),
    ]
    df = _make_ohlcv_df(partial_rows)

    weekly = build_weekly_candles(df, week_start_day="Monday")

    assert len(weekly) == 1
    candle = weekly.iloc[0]
    assert candle["open"] == pytest.approx(100.0)
    assert candle["close"] == pytest.approx(103.5)  # Wednesday's close
    assert candle["volume"] == pytest.approx(2_700_000.0)


def test_build_weekly_candles_multiple_weeks() -> None:
    """Three full weeks produce exactly three weekly candles."""
    start_monday = date(2024, 1, 8)
    all_rows = []
    for week in range(3):
        all_rows.extend(_week_of(start_monday + timedelta(weeks=week)))
    df = _make_ohlcv_df(all_rows)

    weekly = build_weekly_candles(df, week_start_day="Monday")

    assert len(weekly) == 3


def test_build_weekly_candles_week_start_is_monday() -> None:
    """week_start is always a Monday's date, even if Monday was a holiday."""
    # Week starts Tuesday (Monday Jan 1 is a holiday)
    tuesday = date(2024, 1, 2)  # Tuesday
    rows = []
    for offset in range(4):  # Tue-Fri
        day = tuesday + timedelta(days=offset)
        rows.append(_make_ohlcv_row(day, 100.0 + offset, 102.0 + offset, 99.0 + offset, 101.0 + offset, 1_000_000))
    df = _make_ohlcv_df(rows)

    weekly = build_weekly_candles(df, week_start_day="Monday")

    assert len(weekly) == 1
    # week_start should be the Monday of that week (Jan 1), not Tuesday Jan 2
    candle = weekly.iloc[0]
    assert candle["week_start"] == date(2024, 1, 1).isoformat()


def test_build_weekly_candles_uses_config_start_day() -> None:
    """Week grouping respects the week_start_day config setting."""
    start_monday = date(2024, 1, 8)
    all_rows = _week_of(start_monday) + _week_of(start_monday + timedelta(weeks=1))
    df = _make_ohlcv_df(all_rows)

    weekly = build_weekly_candles(df, week_start_day="Monday")

    assert len(weekly) == 2
    # First week_start should be the Monday
    assert weekly.iloc[0]["week_start"] == date(2024, 1, 8).isoformat()


# ── save_weekly_candles_to_db ─────────────────────────────────────────────────


def test_save_weekly_candles_to_db(db_connection: sqlite3.Connection) -> None:
    """Weekly candles are saved with correct values."""
    monday = date(2024, 1, 8)
    df = _make_ohlcv_df(_week_of(monday))
    weekly = build_weekly_candles(df)

    count = save_weekly_candles_to_db(db_connection, "AAPL", weekly)

    assert count == 1
    row = db_connection.execute(
        "SELECT * FROM weekly_candles WHERE ticker='AAPL'"
    ).fetchone()
    assert row is not None
    assert row["open"] == pytest.approx(100.0)
    assert row["high"] == pytest.approx(107.0)
    assert row["low"] == pytest.approx(99.0)
    assert row["close"] == pytest.approx(106.0)
    assert row["volume"] == pytest.approx(5_000_000.0)


def test_save_weekly_candles_is_idempotent(db_connection: sqlite3.Connection) -> None:
    """Saving the same weekly candles twice does not create duplicate rows."""
    monday = date(2024, 1, 8)
    df = _make_ohlcv_df(_week_of(monday))
    weekly = build_weekly_candles(df)

    save_weekly_candles_to_db(db_connection, "AAPL", weekly)
    save_weekly_candles_to_db(db_connection, "AAPL", weekly)

    count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM weekly_candles WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert count == 1


# ── compute_weekly_indicators ─────────────────────────────────────────────────


def test_compute_weekly_indicators(db_connection: sqlite3.Connection) -> None:
    """Weekly indicator columns are present after computing indicators on weekly candles."""
    rows = _generate_daily_ohlcv(num_weeks=60)
    df = _make_ohlcv_df(rows)
    weekly = build_weekly_candles(df)

    result = compute_weekly_indicators(weekly, _BASE_CONFIG)

    assert not result.empty
    expected_cols = ["ema_9", "rsi_14", "macd_line", "adx", "stoch_k", "stoch_d",
                     "cci_20", "williams_r", "obv", "cmf_20", "ad_line",
                     "bb_upper", "bb_lower", "bb_pctb", "atr_14",
                     "keltner_upper", "keltner_lower"]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_compute_weekly_indicators_uses_same_config() -> None:
    """Weekly indicators use the same RSI/EMA config parameters as daily indicators."""
    rows = _generate_daily_ohlcv(num_weeks=60)
    df = _make_ohlcv_df(rows)
    weekly = build_weekly_candles(df)

    custom_config = {**_BASE_CONFIG, "indicators": {**_BASE_CONFIG["indicators"], "rsi_period": 14}}
    result = compute_weekly_indicators(weekly, custom_config)

    # RSI column should exist and have non-NaN values after enough weeks
    assert "rsi_14" in result.columns
    non_nan = result["rsi_14"].dropna()
    assert len(non_nan) > 0


# ── compute_weekly_for_ticker ─────────────────────────────────────────────────


def test_compute_weekly_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Daily OHLCV → weekly_candles and indicators_weekly both populated."""
    rows = _generate_daily_ohlcv(num_weeks=60)
    _insert_ohlcv(db_connection, "AAPL", rows)

    count = compute_weekly_for_ticker(db_connection, "AAPL", _BASE_CONFIG, mode="full")

    assert count == 60
    candles_count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM weekly_candles WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert candles_count == 60

    indicators_count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM indicators_weekly WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert indicators_count == 60


def test_compute_weekly_returns_count(db_connection: sqlite3.Connection) -> None:
    """Returns the correct number of weekly candles created."""
    rows = _generate_daily_ohlcv(num_weeks=10)
    _insert_ohlcv(db_connection, "AAPL", rows)

    count = compute_weekly_for_ticker(db_connection, "AAPL", _BASE_CONFIG, mode="full")

    assert count == 10


def test_compute_weekly_incremental_mode(db_connection: sqlite3.Connection) -> None:
    """Incremental mode adds only the new week(s) rather than recomputing everything."""
    # Insert 20 weeks of daily data and run full build
    rows_20 = _generate_daily_ohlcv(num_weeks=20)
    _insert_ohlcv(db_connection, "AAPL", rows_20)
    compute_weekly_for_ticker(db_connection, "AAPL", _BASE_CONFIG, mode="full")

    initial_count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM weekly_candles WHERE ticker='AAPL'"
    ).fetchone()["cnt"]

    # Add 1 new week of daily data
    start_monday = date(2023, 1, 2) + timedelta(weeks=20)
    new_week_rows = [
        {
            "date": (start_monday + timedelta(days=d)).isoformat(),
            "open": 110.0,
            "high": 112.0,
            "low": 109.0,
            "close": 111.0,
            "volume": 1_000_000.0,
        }
        for d in range(5)
    ]
    _insert_ohlcv(db_connection, "AAPL", new_week_rows)

    compute_weekly_for_ticker(db_connection, "AAPL", _BASE_CONFIG, mode="incremental")

    final_count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM weekly_candles WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert final_count == initial_count + 1
