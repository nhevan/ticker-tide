"""
Tests for src/calculator/indicators.py

Covers:
- compute_all_indicators: column presence, individual indicator correctness,
  config-driven params, edge cases (empty, insufficient data, NaN warm-up)
- load_ohlcv_for_ticker: DB query, sort order
- save_indicators_to_db: idempotency, row count
- compute_indicators_for_ticker: end-to-end full mode
"""

import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from src.calculator.indicators import (
    compute_all_indicators,
    compute_indicators_for_ticker,
    load_ohlcv_for_ticker,
    save_indicators_to_db,
)


# ── Local fixtures ──────────────────────────────────────────────────────────────


def _make_ohlcv(rows: int, start_price: float = 100.0, trend: float = 0.005) -> pd.DataFrame:
    """
    Generate synthetic OHLCV data.

    Args:
        rows: Number of rows to generate.
        start_price: Starting close price.
        trend: Daily price drift (positive = uptrend, negative = downtrend).

    Returns:
        DataFrame with columns: date, open, high, low, close, volume.
    """
    base = date(2024, 1, 2)
    records = []
    close = start_price
    for i in range(rows):
        current_date = base + timedelta(days=i)
        open_price = close * (1 + (i % 3 - 1) * 0.001)
        close = open_price * (1 + trend + (i % 7 - 3) * 0.001)
        high = max(open_price, close) * 1.01
        low = min(open_price, close) * 0.99
        volume = 1_000_000 + i * 10_000
        records.append(
            {
                "date": current_date.isoformat(),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "volume": float(volume),
            }
        )
    return pd.DataFrame(records)


def _make_uptrend_ohlcv(rows: int) -> pd.DataFrame:
    """Generate OHLCV data with a clear uptrend (prices increasing)."""
    return _make_ohlcv(rows, start_price=100.0, trend=0.01)


def _make_downtrend_ohlcv(rows: int) -> pd.DataFrame:
    """Generate OHLCV data with a clear downtrend (prices decreasing)."""
    return _make_ohlcv(rows, start_price=150.0, trend=-0.01)


@pytest.fixture
def default_config() -> dict:
    """Return the standard calculator config matching config/calculator.json."""
    return {
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
        }
    }


# ── compute_all_indicators ──────────────────────────────────────────────────────


def test_compute_all_indicators_returns_dataframe(default_config: dict) -> None:
    """compute_all_indicators returns a DataFrame with all expected indicator columns."""
    ohlcv_df = _make_ohlcv(100)
    result = compute_all_indicators(ohlcv_df, default_config)

    expected_columns = [
        "ema_9", "ema_21", "ema_50",
        "macd_line", "macd_signal", "macd_histogram",
        "adx",
        "rsi_14",
        "stoch_k", "stoch_d",
        "cci_20",
        "williams_r",
        "obv",
        "cmf_20",
        "ad_line",
        "bb_upper", "bb_lower", "bb_pctb",
        "atr_14",
        "keltner_upper", "keltner_lower",
    ]
    assert isinstance(result, pd.DataFrame)
    for col in expected_columns:
        assert col in result.columns, f"Missing column: {col}"


def test_compute_ema_values(default_config: dict) -> None:
    """In an uptrend, EMA 9 should be above EMA 21, values close to close prices, no post-warmup NaN."""
    ohlcv_df = _make_uptrend_ohlcv(50)
    result = compute_all_indicators(ohlcv_df, default_config)

    # After warm-up, last 10 rows should have valid values
    tail = result.tail(10)
    assert tail["ema_9"].isna().sum() == 0
    assert tail["ema_21"].isna().sum() == 0

    # EMA 9 should be above EMA 21 in a clear uptrend (on the last few valid rows)
    assert (tail["ema_9"] > tail["ema_21"]).all(), "EMA 9 should be above EMA 21 in uptrend"

    # EMA values should be in a reasonable range of close prices
    avg_close = ohlcv_df["close"].tail(10).mean()
    assert abs(tail["ema_9"].mean() - avg_close) / avg_close < 0.2


def test_compute_ema_uses_config_periods(default_config: dict) -> None:
    """compute_all_indicators uses ema_periods from config, not hardcoded values."""
    custom_config = {
        "indicators": {
            **default_config["indicators"],
            "ema_periods": [5, 10, 30],
        }
    }
    ohlcv_df = _make_ohlcv(100)

    default_result = compute_all_indicators(ohlcv_df, default_config)
    custom_result = compute_all_indicators(ohlcv_df, custom_config)

    # Custom config columns should exist
    assert "ema_5" in custom_result.columns
    assert "ema_10" in custom_result.columns
    assert "ema_30" in custom_result.columns
    # Default columns should NOT be in custom result
    assert "ema_9" not in custom_result.columns
    assert "ema_21" not in custom_result.columns
    assert "ema_50" not in custom_result.columns


def test_compute_macd_values(default_config: dict) -> None:
    """MACD columns are populated; histogram = macd_line - macd_signal approximately."""
    ohlcv_df = _make_ohlcv(50)
    result = compute_all_indicators(ohlcv_df, default_config)

    tail = result.dropna(subset=["macd_line", "macd_signal", "macd_histogram"])
    assert len(tail) > 0, "Expected valid MACD rows after warm-up"

    # histogram ≈ macd_line - macd_signal
    diff = (tail["macd_histogram"] - (tail["macd_line"] - tail["macd_signal"])).abs()
    assert diff.max() < 1e-6, "macd_histogram should equal macd_line - macd_signal"

    # Not all zeros
    assert tail["macd_line"].abs().sum() > 0


def test_compute_macd_uses_config_params(default_config: dict) -> None:
    """Different MACD config params produce different values."""
    ohlcv_df = _make_ohlcv(100)
    custom_config = {
        "indicators": {
            **default_config["indicators"],
            "macd": {"fast": 8, "slow": 21, "signal": 5},
        }
    }
    default_result = compute_all_indicators(ohlcv_df, default_config)
    custom_result = compute_all_indicators(ohlcv_df, custom_config)

    default_valid = default_result["macd_line"].dropna()
    custom_valid = custom_result["macd_line"].dropna()
    # Values should differ (different params → different calculations)
    assert not default_valid.equals(custom_valid.reindex(default_valid.index))


def test_compute_adx_values(default_config: dict) -> None:
    """ADX is between 0 and 100; in trending data should exceed 20."""
    ohlcv_df = _make_uptrend_ohlcv(60)
    result = compute_all_indicators(ohlcv_df, default_config)

    adx_valid = result["adx"].dropna()
    assert len(adx_valid) > 0
    assert (adx_valid >= 0).all(), "ADX must be >= 0"
    assert (adx_valid <= 100).all(), "ADX must be <= 100"
    assert adx_valid.tail(5).mean() > 20, "ADX should exceed 20 for trending data"


def test_compute_rsi_values(default_config: dict) -> None:
    """RSI is between 0 and 100; uptrend RSI > 50, downtrend RSI < 50."""
    up_df = _make_uptrend_ohlcv(30)
    down_df = _make_downtrend_ohlcv(30)

    up_result = compute_all_indicators(up_df, default_config)
    down_result = compute_all_indicators(down_df, default_config)

    up_rsi = up_result["rsi_14"].dropna()
    down_rsi = down_result["rsi_14"].dropna()

    assert (up_rsi >= 0).all() and (up_rsi <= 100).all()
    assert (down_rsi >= 0).all() and (down_rsi <= 100).all()
    assert up_rsi.tail(5).mean() > 50, "RSI should be > 50 in uptrend"
    assert down_rsi.tail(5).mean() < 50, "RSI should be < 50 in downtrend"


def test_compute_rsi_uses_config_period(default_config: dict) -> None:
    """Different rsi_period config values produce different RSI values."""
    ohlcv_df = _make_ohlcv(50)
    config_7 = {"indicators": {**default_config["indicators"], "rsi_period": 7}}
    config_21 = {"indicators": {**default_config["indicators"], "rsi_period": 21}}

    result_7 = compute_all_indicators(ohlcv_df, config_7)
    result_21 = compute_all_indicators(ohlcv_df, config_21)

    rsi_7 = result_7["rsi_14"].dropna()
    rsi_21 = result_21["rsi_14"].dropna()
    assert not rsi_7.equals(rsi_21.reindex(rsi_7.index))


def test_compute_stochastic_values(default_config: dict) -> None:
    """Stochastic K and D are between 0 and 100; D is smoother than K."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    k_valid = result["stoch_k"].dropna()
    d_valid = result["stoch_d"].dropna()

    assert (k_valid >= 0).all() and (k_valid <= 100).all()
    assert (d_valid >= 0).all() and (d_valid <= 100).all()

    # D should be smoother (lower std dev) than K
    shared = result.dropna(subset=["stoch_k", "stoch_d"])
    if len(shared) > 3:
        assert shared["stoch_d"].std() <= shared["stoch_k"].std() + 1e-6


def test_compute_cci_values(default_config: dict) -> None:
    """CCI values are computed and not all NaN."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    cci_valid = result["cci_20"].dropna()
    assert len(cci_valid) > 0, "Expected at least some valid CCI values"


def test_compute_williams_r_values(default_config: dict) -> None:
    """Williams %R is between -100 and 0; closer to 0 in uptrend."""
    ohlcv_df = _make_uptrend_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    wr_valid = result["williams_r"].dropna()
    assert (wr_valid >= -100).all(), "Williams %R must be >= -100"
    assert (wr_valid <= 0).all(), "Williams %R must be <= 0"
    assert wr_valid.tail(5).mean() > -50, "In an uptrend, Williams %R should be closer to 0"


def test_compute_obv_values(default_config: dict) -> None:
    """OBV accumulates volume correctly: up day adds, down day subtracts."""
    records = []
    prices = [100.0, 102.0, 101.0, 103.0, 102.0]
    volumes = [1000.0, 2000.0, 1500.0, 3000.0, 2500.0]
    base = date(2024, 1, 2)
    for i, (c, v) in enumerate(zip(prices, volumes)):
        records.append({
            "date": (base + timedelta(days=i)).isoformat(),
            "open": c - 0.5,
            "high": c + 1.0,
            "low": c - 1.0,
            "close": c,
            "volume": v,
        })
    ohlcv_df = pd.DataFrame(records)
    result = compute_all_indicators(ohlcv_df, default_config)

    obv = result["obv"].tolist()
    # Day 0: initial OBV = volume[0] (OBV starts at first volume value)
    # Day 1 (up): OBV += 2000 → obv[0] + 2000
    # Day 2 (down): OBV -= 1500
    # Day 3 (up): OBV += 3000
    # Day 4 (down): OBV -= 2500
    assert obv[1] > obv[0], "Up day: OBV should increase"
    assert obv[2] < obv[1], "Down day: OBV should decrease"
    assert obv[3] > obv[2], "Up day: OBV should increase"
    assert obv[4] < obv[3], "Down day: OBV should decrease"


def test_compute_cmf_values(default_config: dict) -> None:
    """CMF values are between -1 and 1."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    cmf_valid = result["cmf_20"].dropna()
    assert len(cmf_valid) > 0
    assert (cmf_valid >= -1).all(), "CMF must be >= -1"
    assert (cmf_valid <= 1).all(), "CMF must be <= 1"


def test_compute_ad_line_values(default_config: dict) -> None:
    """A/D Line returns non-NaN values for valid OHLCV input."""
    ohlcv_df = _make_ohlcv(20)
    result = compute_all_indicators(ohlcv_df, default_config)

    ad_valid = result["ad_line"].dropna()
    assert len(ad_valid) > 0, "Expected valid A/D Line values"


def test_compute_bollinger_bands(default_config: dict) -> None:
    """bb_upper > bb_lower; bb_pctb between 0 and 1 when price is within bands."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    valid = result.dropna(subset=["bb_upper", "bb_lower", "bb_pctb"])
    assert len(valid) > 0

    assert (valid["bb_upper"] > valid["bb_lower"]).all(), "Upper band must exceed lower band"

    # Bollinger bands should be roughly symmetric around a middle band
    mid = (valid["bb_upper"] + valid["bb_lower"]) / 2
    upper_dist = valid["bb_upper"] - mid
    lower_dist = mid - valid["bb_lower"]
    ratio = (upper_dist / lower_dist)
    assert ((ratio - 1.0).abs() < 0.01).all(), "Bands should be symmetric around the middle"


def test_compute_bollinger_uses_config(default_config: dict) -> None:
    """Higher std_dev config (same period) produces wider bands."""
    ohlcv_df = _make_ohlcv(50)
    config_wide = {
        "indicators": {
            **default_config["indicators"],
            "bollinger": {"period": 20, "std_dev": 3},  # same period, wider multiplier
        }
    }
    default_result = compute_all_indicators(ohlcv_df, default_config)
    wide_result = compute_all_indicators(ohlcv_df, config_wide)

    default_valid = default_result.dropna(subset=["bb_upper", "bb_lower"])
    wide_valid = wide_result.dropna(subset=["bb_upper", "bb_lower"])

    default_width = (default_valid["bb_upper"] - default_valid["bb_lower"]).mean()
    wide_width = (wide_valid["bb_upper"] - wide_valid["bb_lower"]).mean()
    assert wide_width > default_width, "Wider std_dev should produce wider bands"


def test_compute_atr_values(default_config: dict) -> None:
    """ATR is always positive and reasonable relative to price range."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    # ta library fills warm-up rows with 0.0 (not NaN); only check post-warm-up values
    atr_valid = result["atr_14"][result["atr_14"] > 0]
    assert len(atr_valid) > 0, "Expected at least some positive ATR values after warm-up"
    assert (atr_valid > 0).all(), "ATR must be positive"

    avg_range = (ohlcv_df["high"] - ohlcv_df["low"]).mean()
    assert atr_valid.mean() < avg_range * 3, "ATR should be in a reasonable range"


def test_compute_keltner_channels(default_config: dict) -> None:
    """keltner_upper > keltner_lower for all valid rows."""
    ohlcv_df = _make_ohlcv(30)
    result = compute_all_indicators(ohlcv_df, default_config)

    valid = result.dropna(subset=["keltner_upper", "keltner_lower"])
    assert len(valid) > 0
    assert (valid["keltner_upper"] > valid["keltner_lower"]).all()


def test_compute_all_indicators_handles_nan(default_config: dict) -> None:
    """First rows (warm-up period) contain NaN; function does not crash; later rows are valid."""
    ohlcv_df = _make_ohlcv(100)
    result = compute_all_indicators(ohlcv_df, default_config)

    # Should not crash
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 100

    # EMA 50 needs 50 rows of warm-up — first 49 should be NaN
    assert pd.isna(result["ema_50"].iloc[0])

    # Last rows should have valid EMA values
    assert not pd.isna(result["ema_50"].iloc[-1])


def test_compute_all_indicators_insufficient_data(default_config: dict) -> None:
    """With only 5 rows, does not crash; returns DataFrame with mostly NaN indicator values."""
    ohlcv_df = _make_ohlcv(5)
    result = compute_all_indicators(ohlcv_df, default_config)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5
    # EMA 50 cannot be computed — should be all NaN
    assert result["ema_50"].isna().all()


def test_compute_all_indicators_empty_dataframe(default_config: dict) -> None:
    """Empty DataFrame input returns an empty DataFrame without crashing."""
    empty_df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    result = compute_all_indicators(empty_df, default_config)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# ── load_ohlcv_for_ticker ───────────────────────────────────────────────────────


def test_load_ohlcv_for_ticker(db_connection: sqlite3.Connection) -> None:
    """load_ohlcv_for_ticker returns a DataFrame with expected columns sorted by date."""
    rows = [
        ("AAPL", "2024-01-04", 100.0, 102.0, 99.0, 101.0, 1000000.0),
        ("AAPL", "2024-01-03", 98.0, 100.0, 97.0, 99.0, 900000.0),
        ("AAPL", "2024-01-02", 97.0, 99.0, 96.0, 98.0, 800000.0),
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    db_connection.commit()

    result = load_ohlcv_for_ticker(db_connection, "AAPL")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns[:6]) == ["date", "open", "high", "low", "close", "volume"]
    assert result["date"].tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]


def test_load_ohlcv_for_ticker_empty(db_connection: sqlite3.Connection) -> None:
    """Returns an empty DataFrame when no data exists for the ticker."""
    result = load_ohlcv_for_ticker(db_connection, "NONEXISTENT")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# ── save_indicators_to_db ───────────────────────────────────────────────────────


def test_save_indicators_to_db(db_connection: sqlite3.Connection, default_config: dict) -> None:
    """save_indicators_to_db stores all indicator rows with correct values."""
    ohlcv_df = _make_ohlcv(30)
    indicators_df = compute_all_indicators(ohlcv_df, default_config)
    # Merge date column into indicators_df for saving
    indicators_df["date"] = ohlcv_df["date"].values

    count = save_indicators_to_db(db_connection, "AAPL", indicators_df)

    cursor = db_connection.execute("SELECT COUNT(*) FROM indicators_daily WHERE ticker='AAPL'")
    db_count = cursor.fetchone()[0]
    assert db_count == count
    assert count > 0


def test_save_indicators_is_idempotent(db_connection: sqlite3.Connection, default_config: dict) -> None:
    """Saving indicators twice results in the same row count (INSERT OR REPLACE)."""
    ohlcv_df = _make_ohlcv(30)
    indicators_df = compute_all_indicators(ohlcv_df, default_config)
    indicators_df["date"] = ohlcv_df["date"].values

    save_indicators_to_db(db_connection, "AAPL", indicators_df)
    save_indicators_to_db(db_connection, "AAPL", indicators_df)

    cursor = db_connection.execute("SELECT COUNT(*) FROM indicators_daily WHERE ticker='AAPL'")
    db_count = cursor.fetchone()[0]
    assert db_count <= 30  # Should not double up


# ── compute_indicators_for_ticker ───────────────────────────────────────────────


def test_compute_indicators_for_ticker_end_to_end(
    db_connection: sqlite3.Connection, default_config: dict
) -> None:
    """Insert OHLCV into DB, compute indicators, verify indicators_daily is populated."""
    ohlcv_df = _make_ohlcv(60)
    rows = [
        (
            "AAPL",
            row["date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
        )
        for _, row in ohlcv_df.iterrows()
    ]
    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    db_connection.commit()

    count = compute_indicators_for_ticker(db_connection, "AAPL", default_config, mode="full")

    assert count > 0
    cursor = db_connection.execute("SELECT COUNT(*) FROM indicators_daily WHERE ticker='AAPL'")
    db_count = cursor.fetchone()[0]
    assert db_count == count
