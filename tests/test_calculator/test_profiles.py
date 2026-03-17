"""
Tests for src/calculator/profiles.py

Covers:
- compute_percentiles: correct values, NaN handling, all-NaN, insufficient data
- compute_profile_for_ticker: single indicator, all indicators, rolling window,
  insufficient data, computed_at timestamp, idempotency
- compute_sector_profile: combined data across tickers
- blend_profiles: math correctness, alpha=0/1 edge cases
- calculate_alpha: formula, capping at max
- compute_all_profiles: all tickers processed
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from src.calculator.profiles import (
    blend_profiles,
    calculate_alpha,
    compute_all_profiles,
    compute_percentiles,
    compute_profile_for_ticker,
    compute_sector_profile,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


_BASE_CONFIG = {
    "profiles": {
        "rolling_window_days": 504,
        "recompute_frequency": "weekly",
        "blend_alpha_max": 0.85,
        "blend_alpha_denominator": 756,
    },
}


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _insert_indicators(
    db_conn: sqlite3.Connection,
    ticker: str,
    num_rows: int,
    rsi_start: float = 50.0,
    rsi_step: float = 0.0,
) -> None:
    """Insert num_rows rows of indicator data for the given ticker."""
    for i in range(num_rows):
        rsi_value = rsi_start + i * rsi_step
        db_conn.execute(
            """
            INSERT OR REPLACE INTO indicators_daily
                (ticker, date, rsi_14, stoch_k, stoch_d, cci_20, williams_r,
                 cmf_20, bb_pctb, adx, macd_histogram, atr_14, obv, ad_line,
                 macd_line, macd_signal, ema_9, ema_21, ema_50)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker, _make_date(i),
                rsi_value, 55.0, 52.0, 80.0, -30.0,
                0.2, 0.6, 22.0, 0.05, 1.5, 1_000_000.0, 500_000.0,
                0.3, 0.2, 101.0, 99.5, 97.0,
            ),
        )
    db_conn.commit()


def _insert_ticker(
    db_conn: sqlite3.Connection,
    symbol: str,
    sector: str = "Technology",
    sector_etf: str = "XLK",
) -> None:
    db_conn.execute(
        "INSERT OR REPLACE INTO tickers (symbol, sector, sector_etf, active) VALUES (?, ?, ?, 1)",
        (symbol, sector, sector_etf),
    )
    db_conn.commit()


# ── compute_percentiles ────────────────────────────────────────────────────────


def test_compute_percentiles_for_indicator() -> None:
    """Returns a dict with all required keys and valid ordering for a normal series."""
    rng = np.random.default_rng(42)
    series = pd.Series(rng.uniform(20, 80, 504))

    result = compute_percentiles(series)

    assert result is not None
    assert set(result.keys()) == {"p5", "p20", "p50", "p80", "p95", "mean", "std"}
    assert result["p5"] < result["p20"] < result["p50"] < result["p80"] < result["p95"]
    assert result["std"] > 0


def test_compute_percentiles_correct_values() -> None:
    """Verifies specific percentile values for a known sequence [1..100]."""
    series = pd.Series(list(range(1, 101)))  # 1, 2, ..., 100

    result = compute_percentiles(series)

    assert result is not None
    assert result["p5"] == pytest.approx(np.percentile(series, 5), rel=1e-6)
    assert result["p50"] == pytest.approx(np.percentile(series, 50), rel=1e-6)
    assert result["p95"] == pytest.approx(np.percentile(series, 95), rel=1e-6)
    assert result["mean"] == pytest.approx(series.mean(), rel=1e-6)
    assert result["std"] == pytest.approx(series.std(), rel=1e-4)


def test_compute_percentiles_handles_nan() -> None:
    """NaN values are dropped before computation; result is still valid."""
    values = list(range(1, 51)) + [float("nan")] * 20 + list(range(51, 81))
    series = pd.Series(values)

    result = compute_percentiles(series)

    assert result is not None
    # 80 valid values remain; percentiles should be ordered
    assert result["p5"] < result["p50"] < result["p95"]


def test_compute_percentiles_all_nan() -> None:
    """All-NaN series returns None (cannot compute percentiles)."""
    series = pd.Series([float("nan")] * 50)

    result = compute_percentiles(series)

    assert result is None


def test_compute_percentiles_insufficient_data() -> None:
    """Series with fewer than 30 valid values returns None."""
    series = pd.Series(list(range(1, 30)))  # exactly 29 values

    result = compute_percentiles(series)

    assert result is None


def test_compute_percentiles_exactly_minimum_data() -> None:
    """Series with exactly 30 valid values returns a valid result."""
    series = pd.Series(list(range(1, 31)))  # exactly 30 values

    result = compute_percentiles(series)

    assert result is not None
    assert "p50" in result


# ── compute_profile_for_ticker ────────────────────────────────────────────────


def test_compute_profile_for_ticker_single_indicator(db_connection: sqlite3.Connection) -> None:
    """Profile is written for a ticker when only one indicator column is populated."""
    _insert_ticker(db_connection, "AAPL")
    # Insert 504 rows with only rsi_14 populated
    for i in range(504):
        db_connection.execute(
            "INSERT OR REPLACE INTO indicators_daily (ticker, date, rsi_14) VALUES (?, ?, ?)",
            ("AAPL", _make_date(i), 50.0 + (i % 30)),
        )
    db_connection.commit()

    count = compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    assert count >= 1
    row = db_connection.execute(
        "SELECT * FROM indicator_profiles WHERE ticker='AAPL' AND indicator='rsi_14'"
    ).fetchone()
    assert row is not None
    assert row["p5"] is not None
    assert row["p20"] is not None
    assert row["p50"] is not None
    assert row["p80"] is not None
    assert row["p95"] is not None
    assert row["mean"] is not None
    assert row["std"] is not None


def test_compute_profile_for_ticker_all_indicators(db_connection: sqlite3.Connection) -> None:
    """A profile row is written for each profiled indicator when all are populated."""
    _insert_ticker(db_connection, "AAPL")
    _insert_indicators(db_connection, "AAPL", 504)

    compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    expected_indicators = [
        "rsi_14", "stoch_k", "stoch_d", "cci_20", "williams_r",
        "cmf_20", "bb_pctb", "adx", "macd_histogram", "atr_14",
        "obv", "ad_line", "macd_line", "macd_signal",
        "ema_9", "ema_21", "ema_50",
    ]
    rows = db_connection.execute(
        "SELECT indicator FROM indicator_profiles WHERE ticker='AAPL'"
    ).fetchall()
    saved_indicators = {row["indicator"] for row in rows}

    for ind in expected_indicators:
        assert ind in saved_indicators, f"Missing profile for indicator: {ind}"

    # bb_upper, bb_lower, keltner_upper, keltner_lower must NOT be profiled
    for excluded in ("bb_upper", "bb_lower", "keltner_upper", "keltner_lower"):
        assert excluded not in saved_indicators


def test_compute_profile_uses_rolling_window(db_connection: sqlite3.Connection) -> None:
    """Profile uses only the last rolling_window_days rows, not all 600."""
    _insert_ticker(db_connection, "AAPL")
    _insert_indicators(db_connection, "AAPL", 600, rsi_start=30.0, rsi_step=0.05)

    config = {
        "profiles": {
            "rolling_window_days": 504,
            "recompute_frequency": "weekly",
            "blend_alpha_max": 0.85,
            "blend_alpha_denominator": 756,
        }
    }
    compute_profile_for_ticker(db_connection, "AAPL", config)

    row = db_connection.execute(
        "SELECT window_start, window_end FROM indicator_profiles WHERE ticker='AAPL' AND indicator='rsi_14'"
    ).fetchone()
    assert row is not None
    # window should span the LAST 504 days of the 600 inserted
    expected_window_start = _make_date(600 - 504)
    assert row["window_start"] == expected_window_start
    assert row["window_end"] == _make_date(599)


def test_compute_profile_insufficient_data_still_computes(db_connection: sqlite3.Connection) -> None:
    """When fewer than rolling_window_days rows exist, profile is still computed from available data."""
    _insert_ticker(db_connection, "AAPL")
    _insert_indicators(db_connection, "AAPL", 100)

    count = compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    assert count >= 1
    row = db_connection.execute(
        "SELECT * FROM indicator_profiles WHERE ticker='AAPL' AND indicator='rsi_14'"
    ).fetchone()
    assert row is not None


def test_compute_profile_sets_computed_at(db_connection: sqlite3.Connection) -> None:
    """computed_at is set to a non-null UTC ISO string."""
    _insert_ticker(db_connection, "AAPL")
    _insert_indicators(db_connection, "AAPL", 504)

    compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    row = db_connection.execute(
        "SELECT computed_at FROM indicator_profiles WHERE ticker='AAPL' AND indicator='rsi_14'"
    ).fetchone()
    assert row is not None
    assert row["computed_at"] is not None
    assert "T" in row["computed_at"]  # ISO 8601 datetime


def test_compute_profile_is_idempotent(db_connection: sqlite3.Connection) -> None:
    """Running compute_profile_for_ticker twice does not duplicate rows."""
    _insert_ticker(db_connection, "AAPL")
    _insert_indicators(db_connection, "AAPL", 504)

    compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)
    compute_profile_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM indicator_profiles WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    # Should have exactly one row per indicator, not doubled
    all_rows = db_connection.execute(
        "SELECT indicator FROM indicator_profiles WHERE ticker='AAPL'"
    ).fetchall()
    indicators = [r["indicator"] for r in all_rows]
    assert len(indicators) == len(set(indicators)), "Duplicate indicator rows found"


# ── compute_sector_profile ────────────────────────────────────────────────────


def test_compute_sector_profile(db_connection: sqlite3.Connection) -> None:
    """Sector profile is computed from combined data of all tickers in the sector."""
    for symbol in ("AAPL", "MSFT", "NVDA"):
        _insert_ticker(db_connection, symbol, sector="Technology", sector_etf="XLK")
        _insert_indicators(db_connection, symbol, 200, rsi_start=30.0, rsi_step=0.2)

    result = compute_sector_profile(db_connection, "Technology", _BASE_CONFIG)

    assert result is not None
    assert "rsi_14" in result
    rsi_profile = result["rsi_14"]
    assert rsi_profile is not None
    assert "p50" in rsi_profile
    # Sector profile uses 3×200=600 combined rows → more data than any single ticker
    assert rsi_profile["p5"] < rsi_profile["p50"] < rsi_profile["p95"]


def test_compute_sector_profile_unknown_sector(db_connection: sqlite3.Connection) -> None:
    """Returns an empty dict (or all-None values) for a sector with no tickers."""
    result = compute_sector_profile(db_connection, "Unicorn", _BASE_CONFIG)

    # Either empty dict or all None indicator profiles
    assert result == {} or all(v is None for v in result.values())


# ── blend_profiles ────────────────────────────────────────────────────────────


def test_blend_stock_and_sector_profiles() -> None:
    """Blended percentiles are correctly alpha-weighted combinations."""
    stock = {"p5": 25.0, "p20": 35.0, "p50": 50.0, "p80": 67.0, "p95": 75.0, "mean": 50.0, "std": 10.0}
    sector = {"p5": 26.0, "p20": 36.0, "p50": 51.0, "p80": 68.0, "p95": 76.0, "mean": 51.0, "std": 11.0}
    alpha = 0.8

    blended = blend_profiles(stock, sector, alpha)

    # p80: 0.8 * 67 + 0.2 * 68 = 53.6 + 13.6 = 67.2
    assert blended["p80"] == pytest.approx(0.8 * 67.0 + 0.2 * 68.0, rel=1e-6)
    assert blended["p50"] == pytest.approx(0.8 * 50.0 + 0.2 * 51.0, rel=1e-6)
    assert blended["mean"] == pytest.approx(0.8 * 50.0 + 0.2 * 51.0, rel=1e-6)


def test_blend_profiles_no_sector() -> None:
    """When sector_profile is None, stock profile is used entirely (alpha=1)."""
    stock = {"p5": 25.0, "p20": 35.0, "p50": 50.0, "p80": 67.0, "p95": 75.0, "mean": 50.0, "std": 10.0}

    blended = blend_profiles(stock, None, alpha=0.8)

    assert blended["p80"] == pytest.approx(67.0, rel=1e-6)
    assert blended["p50"] == pytest.approx(50.0, rel=1e-6)


def test_blend_profiles_no_stock() -> None:
    """When stock_profile is None, sector profile is used entirely (alpha=0)."""
    sector = {"p5": 26.0, "p20": 36.0, "p50": 51.0, "p80": 68.0, "p95": 76.0, "mean": 51.0, "std": 11.0}

    blended = blend_profiles(None, sector, alpha=0.8)

    assert blended["p80"] == pytest.approx(68.0, rel=1e-6)


# ── calculate_alpha ────────────────────────────────────────────────────────────


def test_blend_alpha_calculation_caps_at_max() -> None:
    """Alpha is capped at blend_alpha_max when data exceeds blend_alpha_denominator."""
    # 1260 days / 756 = 1.666 → capped at 0.85
    alpha = calculate_alpha(1260, _BASE_CONFIG)
    assert alpha == pytest.approx(0.85, rel=1e-6)


def test_blend_alpha_with_less_data() -> None:
    """Alpha is proportional when data is less than blend_alpha_denominator."""
    # 500 / 756 = 0.6614...
    alpha = calculate_alpha(500, _BASE_CONFIG)
    assert alpha == pytest.approx(500 / 756, rel=1e-6)
    assert alpha < 0.85


def test_blend_alpha_zero_days() -> None:
    """Zero days of data gives alpha=0 (full weight on sector)."""
    alpha = calculate_alpha(0, _BASE_CONFIG)
    assert alpha == pytest.approx(0.0, rel=1e-6)


# ── compute_all_profiles ──────────────────────────────────────────────────────


def test_compute_all_profiles(db_connection: sqlite3.Connection) -> None:
    """Profiles are computed for all tickers in the list."""
    tickers = [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK"},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK"},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF"},
    ]
    for t in tickers:
        _insert_ticker(db_connection, t["symbol"], t["sector"], t["sector_etf"])
        _insert_indicators(db_connection, t["symbol"], 200)

    result = compute_all_profiles(db_connection, tickers, _BASE_CONFIG)

    assert result["processed"] == 3
    assert result["failed"] == 0
    for t in tickers:
        count = db_connection.execute(
            "SELECT COUNT(*) AS cnt FROM indicator_profiles WHERE ticker=?",
            (t["symbol"],),
        ).fetchone()["cnt"]
        assert count > 0, f"No profiles written for {t['symbol']}"
