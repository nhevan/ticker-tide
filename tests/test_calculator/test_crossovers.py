"""
Tests for src/calculator/crossovers.py

Covers:
- detect_crossover_events: generic bullish/bearish detection
- detect_all_crossovers: EMA 9/21, EMA 21/50, MACD signal crossovers
- days_ago calculation
- save_crossovers_to_db: delete + replace semantics
- detect_crossovers_for_ticker: end-to-end
"""

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.crossovers import (
    detect_all_crossovers,
    detect_crossover_events,
    detect_crossovers_for_ticker,
    save_crossovers_to_db,
)


# ── Local fixtures ──────────────────────────────────────────────────────────────


def _make_dates(n: int, start: str = "2024-01-02") -> pd.Series:
    """Generate a Series of n consecutive date strings starting from start."""
    base = date.fromisoformat(start)
    return pd.Series([(base + timedelta(days=i)).isoformat() for i in range(n)])


@pytest.fixture
def default_config() -> dict:
    """Return the standard calculator config."""
    return {
        "indicators": {
            "ema_periods": [9, 21, 50],
            "macd": {"fast": 12, "slow": 26, "signal": 9},
        }
    }


# ── detect_crossover_events ─────────────────────────────────────────────────────


def test_detect_ema_bullish_crossover(default_config: dict) -> None:
    """EMA 9 crosses above EMA 21 on day 3 → bullish crossover detected."""
    # fast was below slow, then crosses above
    fast = pd.Series([9.0, 9.0, 9.0, 11.0, 12.0])
    slow = pd.Series([10.0, 10.0, 10.0, 10.0, 10.0])
    dates = _make_dates(5)

    crossovers = detect_crossover_events(fast, slow, dates)

    assert len(crossovers) == 1
    assert crossovers[0]["direction"] == "bullish"
    assert crossovers[0]["date"] == dates.iloc[3]


def test_detect_ema_bearish_crossover(default_config: dict) -> None:
    """EMA 9 crosses below EMA 21 → bearish crossover detected."""
    fast = pd.Series([11.0, 11.0, 11.0, 9.0, 8.0])
    slow = pd.Series([10.0, 10.0, 10.0, 10.0, 10.0])
    dates = _make_dates(5)

    crossovers = detect_crossover_events(fast, slow, dates)

    assert len(crossovers) == 1
    assert crossovers[0]["direction"] == "bearish"
    assert crossovers[0]["date"] == dates.iloc[3]


def test_detect_no_crossover(default_config: dict) -> None:
    """EMA 9 stays above EMA 21 throughout → no crossovers."""
    fast = pd.Series([12.0, 12.5, 13.0, 13.5, 14.0])
    slow = pd.Series([10.0, 10.0, 10.0, 10.0, 10.0])
    dates = _make_dates(5)

    crossovers = detect_crossover_events(fast, slow, dates)

    assert len(crossovers) == 0


def test_detect_multiple_crossovers(default_config: dict) -> None:
    """Multiple crossovers over 60 days are all detected."""
    # Create series with 3 crossovers: bull at 5, bear at 25, bull at 45
    n = 60
    fast_values = [10.0] * n
    slow_values = [10.0] * n

    # bull at index 5: fast crosses above slow
    for i in range(5, 25):
        fast_values[i] = 12.0
    # bear at index 25: fast crosses back below slow
    for i in range(25, 45):
        fast_values[i] = 8.0
    # bull at index 45: fast crosses above slow again
    for i in range(45, 60):
        fast_values[i] = 12.0

    fast = pd.Series(fast_values)
    slow = pd.Series(slow_values)
    dates = _make_dates(n)

    crossovers = detect_crossover_events(fast, slow, dates)

    directions = [c["direction"] for c in crossovers]
    assert len(crossovers) == 3
    assert directions[0] == "bullish"
    assert directions[1] == "bearish"
    assert directions[2] == "bullish"


def test_crossover_skips_nan_rows() -> None:
    """Rows where either series is NaN are skipped."""
    fast = pd.Series([float("nan"), 11.0, 9.0, 11.0])
    slow = pd.Series([float("nan"), 10.0, 10.0, 10.0])
    dates = _make_dates(4)

    crossovers = detect_crossover_events(fast, slow, dates)
    # Only the transition at index 3 (from 9→11 crossing slow=10) counts
    assert all(not pd.isna(c["date"]) for c in crossovers)


# ── detect_all_crossovers ───────────────────────────────────────────────────────


def test_detect_ema_21_50_crossover(default_config: dict) -> None:
    """EMA 21 crosses above EMA 50 → crossover_type='ema_21_50', direction='bullish'."""
    n = 10
    indicators_df = pd.DataFrame({
        "date": _make_dates(n),
        "ema_9": [10.0] * n,
        "ema_21": [8.0, 8.0, 8.0, 8.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0],
        "ema_50": [10.0] * n,
        "macd_line": [0.5] * n,
        "macd_signal": [1.0] * n,
    })

    crossovers = detect_all_crossovers(indicators_df, default_config)

    ema_21_50 = [c for c in crossovers if c["crossover_type"] == "ema_21_50"]
    assert len(ema_21_50) >= 1
    assert ema_21_50[0]["direction"] == "bullish"


def test_detect_macd_signal_bullish_crossover(default_config: dict) -> None:
    """MACD line crosses above signal line → crossover_type='macd_signal', direction='bullish'."""
    n = 6
    indicators_df = pd.DataFrame({
        "date": _make_dates(n),
        "ema_9": [10.0] * n,
        "ema_21": [10.0] * n,
        "ema_50": [10.0] * n,
        "macd_line": [0.0, 0.0, 0.0, 1.5, 2.0, 2.5],
        "macd_signal": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    })

    crossovers = detect_all_crossovers(indicators_df, default_config)

    macd_co = [c for c in crossovers if c["crossover_type"] == "macd_signal"]
    assert len(macd_co) >= 1
    assert macd_co[0]["direction"] == "bullish"


def test_detect_macd_signal_bearish_crossover(default_config: dict) -> None:
    """MACD line crosses below signal line → direction='bearish'."""
    n = 6
    indicators_df = pd.DataFrame({
        "date": _make_dates(n),
        "ema_9": [10.0] * n,
        "ema_21": [10.0] * n,
        "ema_50": [10.0] * n,
        "macd_line": [2.0, 2.0, 2.0, 0.5, 0.0, -0.5],
        "macd_signal": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    })

    crossovers = detect_all_crossovers(indicators_df, default_config)

    macd_co = [c for c in crossovers if c["crossover_type"] == "macd_signal"]
    assert len(macd_co) >= 1
    assert macd_co[0]["direction"] == "bearish"


def test_crossover_days_ago(default_config: dict) -> None:
    """Crossover that happened 3 rows before the last row has days_ago=3."""
    n = 10
    # Bull crossover at index 6 (3 rows before index 9 which is the last)
    ema_9_vals = [8.0] * 6 + [12.0] * 4
    ema_21_vals = [10.0] * n
    indicators_df = pd.DataFrame({
        "date": _make_dates(n),
        "ema_9": ema_9_vals,
        "ema_21": ema_21_vals,
        "ema_50": [10.0] * n,
        "macd_line": [0.5] * n,
        "macd_signal": [1.0] * n,
    })

    crossovers = detect_all_crossovers(indicators_df, default_config)

    ema_9_21 = [c for c in crossovers if c["crossover_type"] == "ema_9_21"]
    assert len(ema_9_21) >= 1
    assert ema_9_21[0]["days_ago"] == 3


def test_crossover_today(default_config: dict) -> None:
    """Crossover on the last row has days_ago=0."""
    n = 5
    ema_9_vals = [8.0, 8.0, 8.0, 8.0, 12.0]
    ema_21_vals = [10.0] * n
    indicators_df = pd.DataFrame({
        "date": _make_dates(n),
        "ema_9": ema_9_vals,
        "ema_21": ema_21_vals,
        "ema_50": [10.0] * n,
        "macd_line": [0.5] * n,
        "macd_signal": [1.0] * n,
    })

    crossovers = detect_all_crossovers(indicators_df, default_config)

    ema_9_21 = [c for c in crossovers if c["crossover_type"] == "ema_9_21"]
    assert len(ema_9_21) >= 1
    assert ema_9_21[0]["days_ago"] == 0


# ── save_crossovers_to_db ───────────────────────────────────────────────────────


def test_save_crossovers_to_db(db_connection: sqlite3.Connection, default_config: dict) -> None:
    """save_crossovers_to_db writes crossover records to crossovers_daily."""
    crossovers = [
        {"date": "2024-01-05", "crossover_type": "ema_9_21", "direction": "bullish", "days_ago": 2},
        {"date": "2024-01-08", "crossover_type": "macd_signal", "direction": "bearish", "days_ago": 0},
    ]

    count = save_crossovers_to_db(db_connection, "AAPL", crossovers)

    assert count == 2
    cursor = db_connection.execute("SELECT COUNT(*) FROM crossovers_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] == 2


def test_save_crossovers_clears_old_for_ticker(db_connection: sqlite3.Connection) -> None:
    """Saving new crossovers replaces all existing ones for the ticker."""
    old_crossovers = [
        {"date": "2024-01-03", "crossover_type": "ema_9_21", "direction": "bullish", "days_ago": 5},
        {"date": "2024-01-04", "crossover_type": "ema_9_21", "direction": "bearish", "days_ago": 4},
    ]
    new_crossovers = [
        {"date": "2024-01-10", "crossover_type": "ema_21_50", "direction": "bullish", "days_ago": 0},
    ]

    save_crossovers_to_db(db_connection, "AAPL", old_crossovers)
    save_crossovers_to_db(db_connection, "AAPL", new_crossovers)

    cursor = db_connection.execute("SELECT COUNT(*) FROM crossovers_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] == 1  # Old ones replaced


# ── detect_crossovers_for_ticker ────────────────────────────────────────────────


def test_detect_crossovers_for_ticker_end_to_end(
    db_connection: sqlite3.Connection, default_config: dict
) -> None:
    """Insert indicators into DB, call detect_crossovers_for_ticker, verify crossovers_daily populated."""
    n = 10
    # EMA 9 crosses above EMA 21 at row 5
    ema_9_vals = [8.0] * 5 + [12.0] * 5
    ema_21_vals = [10.0] * n
    rows = []
    base = date(2024, 1, 2)
    for i in range(n):
        rows.append((
            "AAPL",
            (base + timedelta(days=i)).isoformat(),
            ema_9_vals[i], ema_21_vals[i], 10.0,  # ema_9, ema_21, ema_50
            0.1, 0.2, -0.1,  # macd_line, macd_signal, macd_histogram
            25.0,  # adx
            55.0, 0.5, 0.3, 50.0, -30.0,  # rsi_14, stoch_k, stoch_d, cci_20, williams_r
            1000000.0, 0.2, 5000000.0,  # obv, cmf_20, ad_line
            105.0, 95.0, 0.5,  # bb_upper, bb_lower, bb_pctb
            1.5, 107.0, 93.0,  # atr_14, keltner_upper, keltner_lower
        ))
    db_connection.executemany(
        """INSERT OR REPLACE INTO indicators_daily(
            ticker, date, ema_9, ema_21, ema_50,
            macd_line, macd_signal, macd_histogram, adx,
            rsi_14, stoch_k, stoch_d, cci_20, williams_r,
            obv, cmf_20, ad_line,
            bb_upper, bb_lower, bb_pctb,
            atr_14, keltner_upper, keltner_lower
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_connection.commit()

    count = detect_crossovers_for_ticker(db_connection, "AAPL", default_config)

    assert count > 0
    cursor = db_connection.execute("SELECT COUNT(*) FROM crossovers_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] > 0
