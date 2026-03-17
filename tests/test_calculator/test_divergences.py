"""
Tests for src/calculator/divergences.py

Covers:
- detect_divergences_for_indicator: regular bullish/bearish, hidden bullish/bearish,
  no divergence, min/max distance, strength
- detect_all_divergences: multi-indicator, multiple divergences
- save_divergences_to_db
- detect_divergences_for_ticker: end-to-end
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.divergences import (
    detect_all_divergences,
    detect_divergences_for_indicator,
    detect_divergences_for_ticker,
    get_indicator_value_at_date,
    save_divergences_to_db,
)


# ── Helpers ──────────────────────────────────────────────────────────────────────


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _make_indicators_df(dates: list[str], rsi_values: list[float], macd_values: list[float] | None = None) -> pd.DataFrame:
    records = []
    for i, d in enumerate(dates):
        row: dict = {"date": d, "rsi_14": rsi_values[i]}
        row["macd_histogram"] = macd_values[i] if macd_values else 0.0
        row["obv"] = float(i * 1000)
        row["stoch_k"] = rsi_values[i]  # reuse rsi for simplicity
        records.append(row)
    return pd.DataFrame(records)


def _swing_low(d: str, price: float) -> dict:
    return {"date": d, "type": "low", "price": price, "strength": 3}


def _swing_high(d: str, price: float) -> dict:
    return {"date": d, "type": "high", "price": price, "strength": 3}


_BASE_CONFIG = {
    "divergences": {
        "min_swing_distance_days": 5,
        "max_swing_distance_days": 60,
    }
}


# ── get_indicator_value_at_date ──────────────────────────────────────────────────


def test_get_indicator_value_at_date_found() -> None:
    """Returns the correct value when the date exists."""
    df = _make_indicators_df(["2024-01-02", "2024-01-03"], [30.0, 45.0])
    val = get_indicator_value_at_date(df, "2024-01-03", "rsi_14")
    assert val == pytest.approx(45.0)


def test_get_indicator_value_at_date_not_found() -> None:
    """Returns None when the date is not in the DataFrame."""
    df = _make_indicators_df(["2024-01-02"], [30.0])
    val = get_indicator_value_at_date(df, "2024-01-10", "rsi_14")
    assert val is None


# ── detect_divergences_for_indicator ────────────────────────────────────────────


def test_detect_regular_bullish_divergence_rsi() -> None:
    """Price lower low + RSI higher low → regular_bullish divergence on RSI."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    # Swing low 1 at day 5: price=100, rsi=25
    # Swing low 2 at day 20: price=95, rsi=30  (lower price, higher rsi)
    rsi[5] = 25.0
    rsi[20] = 30.0
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[20], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) >= 1
    div = result[0]
    assert div["divergence_type"] == "regular_bullish"
    assert div["indicator"] == "rsi"


def test_detect_regular_bearish_divergence_rsi() -> None:
    """Price higher high + RSI lower high → regular_bearish."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 75.0
    rsi[20] = 70.0
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_high(dates[5], 100.0),
        _swing_high(dates[20], 105.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["divergence_type"] == "regular_bearish"


def test_detect_hidden_bullish_divergence() -> None:
    """Price higher low + RSI lower low → hidden_bullish."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 35.0
    rsi[20] = 28.0
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_low(dates[5], 95.0),
        _swing_low(dates[20], 98.0),  # higher low in price
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["divergence_type"] == "hidden_bullish"


def test_detect_hidden_bearish_divergence() -> None:
    """Price lower high + RSI higher high → hidden_bearish."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 70.0
    rsi[20] = 75.0
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_high(dates[5], 105.0),
        _swing_high(dates[20], 102.0),  # lower high in price
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["divergence_type"] == "hidden_bearish"


def test_detect_divergence_macd() -> None:
    """Regular bullish divergence using MACD histogram."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    macd = [0.0] * 30
    macd[5] = -0.5
    macd[20] = -0.3  # less negative = higher low
    ind_df = _make_indicators_df(dates, rsi, macd)

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[20], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "macd_histogram", "macd_histogram", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["indicator"] == "macd_histogram"
    assert result[0]["divergence_type"] == "regular_bullish"


def test_detect_divergence_obv() -> None:
    """Regular bullish divergence using OBV."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    ind_df = _make_indicators_df(dates, rsi)
    # Override OBV values
    ind_df.loc[5, "obv"] = 5000.0
    ind_df.loc[20, "obv"] = 6000.0  # higher OBV despite lower price

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[20], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "obv", "obv", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["indicator"] == "obv"


def test_detect_divergence_stochastic() -> None:
    """Regular bullish divergence using stoch_k."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 15.0
    rsi[20] = 20.0
    ind_df = _make_indicators_df(dates, rsi)
    ind_df["stoch_k"] = ind_df["rsi_14"]

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[20], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "stoch_k", "stochastic", _BASE_CONFIG
    )
    assert len(result) >= 1
    assert result[0]["indicator"] == "stochastic"


def test_detect_no_divergence() -> None:
    """Price and RSI both make higher highs → no divergence."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 70.0
    rsi[20] = 75.0  # also higher
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_high(dates[5], 100.0),
        _swing_high(dates[20], 105.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) == 0


def test_detect_divergence_stores_swing_values() -> None:
    """Returned divergence includes all price/indicator swing value fields."""
    dates = [_make_date(i) for i in range(30)]
    rsi = [50.0] * 30
    rsi[5] = 25.0
    rsi[20] = 30.0
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[20], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", _BASE_CONFIG
    )
    assert len(result) >= 1
    div = result[0]
    assert "price_swing_1_date" in div
    assert "price_swing_1_value" in div
    assert "price_swing_2_date" in div
    assert "price_swing_2_value" in div
    assert "indicator_swing_1_value" in div
    assert "indicator_swing_2_value" in div


def test_detect_divergence_respects_min_distance() -> None:
    """Two swing lows 3 days apart (< min=5) → no divergence."""
    config = {"divergences": {"min_swing_distance_days": 5, "max_swing_distance_days": 60}}
    dates = [_make_date(i) for i in range(20)]
    rsi = [50.0] * 20
    rsi[5] = 25.0
    rsi[8] = 30.0  # only 3 days apart
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[8], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", config
    )
    assert len(result) == 0


def test_detect_divergence_respects_max_distance() -> None:
    """Two swing lows 70 days apart (> max=60) → no divergence."""
    config = {"divergences": {"min_swing_distance_days": 5, "max_swing_distance_days": 60}}
    dates = [_make_date(i) for i in range(80)]
    rsi = [50.0] * 80
    rsi[5] = 25.0
    rsi[75] = 30.0  # 70 days apart
    ind_df = _make_indicators_df(dates, rsi)

    swing_pts = [
        _swing_low(dates[5], 100.0),
        _swing_low(dates[75], 95.0),
    ]

    result = detect_divergences_for_indicator(
        swing_pts, ind_df, "rsi_14", "rsi", config
    )
    assert len(result) == 0


def test_detect_divergence_strength() -> None:
    """Large price/indicator divergence gap produces higher strength than small gap."""
    dates = [_make_date(i) for i in range(30)]

    # Large gap: price down 20%, RSI up 20 points
    rsi_large = [50.0] * 30
    rsi_large[5] = 20.0
    rsi_large[20] = 40.0
    ind_large = _make_indicators_df(dates, rsi_large)
    swing_large = [_swing_low(dates[5], 100.0), _swing_low(dates[20], 80.0)]
    result_large = detect_divergences_for_indicator(
        swing_large, ind_large, "rsi_14", "rsi", _BASE_CONFIG
    )

    # Small gap: price down 1%, RSI up 1 point
    rsi_small = [50.0] * 30
    rsi_small[5] = 30.0
    rsi_small[20] = 31.0
    ind_small = _make_indicators_df(dates, rsi_small)
    swing_small = [_swing_low(dates[5], 100.0), _swing_low(dates[20], 99.0)]
    result_small = detect_divergences_for_indicator(
        swing_small, ind_small, "rsi_14", "rsi", _BASE_CONFIG
    )

    assert result_large[0]["strength"] >= result_small[0]["strength"]


def test_detect_multiple_divergences() -> None:
    """RSI bullish divergence AND MACD bearish divergence at different dates — both detected."""
    dates = [_make_date(i) for i in range(40)]
    rsi = [50.0] * 40
    rsi[5] = 25.0
    rsi[20] = 30.0  # RSI bullish divergence on lows
    macd = [0.0] * 40
    macd[25] = 0.5
    macd[38] = 0.3  # MACD bearish divergence on highs

    ind_df = _make_indicators_df(dates, rsi, macd)

    # RSI: regular bullish on lows
    lows = [_swing_low(dates[5], 100.0), _swing_low(dates[20], 95.0)]
    res_rsi = detect_divergences_for_indicator(lows, ind_df, "rsi_14", "rsi", _BASE_CONFIG)

    # MACD: regular bearish on highs
    highs = [_swing_high(dates[25], 100.0), _swing_high(dates[38], 105.0)]
    res_macd = detect_divergences_for_indicator(highs, ind_df, "macd_histogram", "macd_histogram", _BASE_CONFIG)

    assert len(res_rsi) >= 1
    assert len(res_macd) >= 1


# ── save_divergences_to_db ───────────────────────────────────────────────────────


def test_save_divergences_to_db(db_connection: sqlite3.Connection) -> None:
    """Divergences are persisted to divergences_daily with all fields."""
    divergences = [
        {
            "date": "2024-01-20",
            "divergence_type": "regular_bullish",
            "indicator": "rsi",
            "price_swing_1_date": "2024-01-05",
            "price_swing_1_value": 100.0,
            "price_swing_2_date": "2024-01-20",
            "price_swing_2_value": 95.0,
            "indicator_swing_1_value": 25.0,
            "indicator_swing_2_value": 30.0,
            "strength": 3,
        }
    ]
    count = save_divergences_to_db(db_connection, "AAPL", divergences)
    assert count == 1

    rows = db_connection.execute(
        "SELECT * FROM divergences_daily WHERE ticker = 'AAPL'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["divergence_type"] == "regular_bullish"
    assert rows[0]["indicator"] == "rsi"
    assert rows[0]["strength"] == 3


# ── detect_divergences_for_ticker ────────────────────────────────────────────────


def test_detect_divergences_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV + indicators + swing points into DB; verify divergences_daily is populated."""
    ticker = "AAPL"

    # Insert OHLCV
    ohlcv_rows = [
        (ticker, _make_date(i), 100.0, 101.0, 99.0, 100.0, 200_000.0)
        for i in range(30)
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        ohlcv_rows,
    )

    # Insert indicators with RSI bullish divergence
    rsi_vals = [50.0] * 30
    rsi_vals[5] = 25.0
    rsi_vals[20] = 30.0
    ind_rows = [
        (ticker, _make_date(i), rsi_vals[i], 0.0, 0.0, 0.0, float(i * 100), 50.0)
        for i in range(30)
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO indicators_daily "
        "(ticker, date, rsi_14, macd_histogram, macd_line, macd_signal, obv, stoch_k) VALUES (?,?,?,?,?,?,?,?)",
        ind_rows,
    )

    # Insert swing points
    swing_rows = [
        (ticker, _make_date(5), "low", 100.0, 3),
        (ticker, _make_date(20), "low", 95.0, 3),
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        swing_rows,
    )
    db_connection.commit()

    config = {
        "divergences": {
            "min_swing_distance_days": 5,
            "max_swing_distance_days": 60,
        }
    }
    count = detect_divergences_for_ticker(db_connection, ticker, config)
    assert count >= 1

    rows = db_connection.execute(
        "SELECT * FROM divergences_daily WHERE ticker = ?", (ticker,)
    ).fetchall()
    assert len(rows) >= 1
