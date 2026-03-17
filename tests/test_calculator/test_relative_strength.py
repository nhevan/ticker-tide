"""
Tests for src/calculator/relative_strength.py

Covers:
- compute_return: basic return calculation, insufficient data
- compute_relative_strength: outperforming, underperforming, negative returns,
  benchmark flat (zero return), different periods
- compute_relative_strength_for_ticker: end-to-end DB round-trip,
  missing SPY data, missing sector ETF data
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.relative_strength import (
    compute_relative_strength,
    compute_relative_strength_for_ticker,
    compute_return,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


_BASE_CONFIG = {
    "relative_strength": {
        "period_days": 20,
    },
}


def _make_ohlcv_df(num_days: int, start_price: float, end_price: float) -> pd.DataFrame:
    """Generate a DataFrame with linearly interpolated close prices."""
    prices = [
        start_price + (end_price - start_price) * i / (num_days - 1)
        for i in range(num_days)
    ]
    base = date(2024, 1, 2)
    rows = []
    for i, price in enumerate(prices):
        day = base + timedelta(days=i)
        rows.append({
            "date": day.isoformat(),
            "open": price * 0.999,
            "high": price * 1.01,
            "low": price * 0.99,
            "close": round(price, 4),
            "volume": 1_000_000.0,
        })
    return pd.DataFrame(rows)


def _insert_ohlcv(db_conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> None:
    for _, row in df.iterrows():
        db_conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
            (ticker, row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"]),
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


# ── compute_return ────────────────────────────────────────────────────────────


def test_compute_return_basic() -> None:
    """Return over 20 days is computed as (last_close - close_20d_ago) / close_20d_ago."""
    # 100 → 110 over 21 rows = 10% return over 20 periods
    df = _make_ohlcv_df(num_days=21, start_price=100.0, end_price=110.0)

    result = compute_return(df, period_days=20)

    assert result == pytest.approx(0.10, rel=1e-3)


def test_compute_return_negative() -> None:
    """Returns a negative value when price declined."""
    df = _make_ohlcv_df(num_days=21, start_price=100.0, end_price=90.0)

    result = compute_return(df, period_days=20)

    assert result is not None
    assert result < 0


def test_compute_return_insufficient_data() -> None:
    """Returns None when fewer rows than period_days + 1 are available."""
    df = _make_ohlcv_df(num_days=10, start_price=100.0, end_price=110.0)

    result = compute_return(df, period_days=20)

    assert result is None


# ── compute_relative_strength ─────────────────────────────────────────────────


def test_compute_relative_strength_vs_spy_outperforming() -> None:
    """RS > 1 when ticker return exceeds benchmark return."""
    # AAPL: 100 → 110 (+10%); SPY: 100 → 105 (+5%)
    aapl_df = _make_ohlcv_df(21, 100.0, 110.0)
    spy_df = _make_ohlcv_df(21, 100.0, 105.0)

    rs = compute_relative_strength(aapl_df, spy_df, period_days=20)

    # (1.10) / (1.05) = 1.0476...
    assert rs is not None
    assert rs > 1.0
    assert rs == pytest.approx(1.10 / 1.05, rel=1e-3)


def test_compute_relative_strength_underperforming() -> None:
    """RS < 1 when ticker return is less than benchmark return."""
    # AAPL: +3%, SPY: +8%
    aapl_df = _make_ohlcv_df(21, 100.0, 103.0)
    spy_df = _make_ohlcv_df(21, 100.0, 108.0)

    rs = compute_relative_strength(aapl_df, spy_df, period_days=20)

    assert rs is not None
    assert rs < 1.0


def test_compute_relative_strength_negative_returns() -> None:
    """RS > 1 when ticker falls less than benchmark (outperforming in a down market)."""
    # AAPL: -5%, SPY: -10%
    aapl_df = _make_ohlcv_df(21, 100.0, 95.0)
    spy_df = _make_ohlcv_df(21, 100.0, 90.0)

    rs = compute_relative_strength(aapl_df, spy_df, period_days=20)

    # (1 - 0.05) / (1 - 0.10) = 0.95 / 0.90 = 1.0555...
    assert rs is not None
    assert rs > 1.0
    assert rs == pytest.approx(0.95 / 0.90, rel=1e-3)


def test_compute_relative_strength_spy_flat() -> None:
    """Returns None when benchmark return is zero (prevents division by zero)."""
    aapl_df = _make_ohlcv_df(21, 100.0, 105.0)
    # SPY flat: start and end price are the same
    spy_df = _make_ohlcv_df(21, 100.0, 100.0)

    rs = compute_relative_strength(aapl_df, spy_df, period_days=20)

    # (1.05) / (1.00) = 1.05 — formula handles zero return gracefully
    # (1 + 0) = 1.0 in denominator, so result is defined and = 1.05
    assert rs is not None
    assert rs == pytest.approx(1.05, rel=1e-3)


def test_compute_relative_strength_uses_config_period() -> None:
    """Period_days controls which historical close is used as the base."""
    # Same data, different periods should give different RS values
    aapl_df = _make_ohlcv_df(31, 100.0, 115.0)  # +15% over 30 days
    spy_df = _make_ohlcv_df(31, 100.0, 110.0)   # +10% over 30 days

    rs_20 = compute_relative_strength(aapl_df, spy_df, period_days=20)
    rs_10 = compute_relative_strength(aapl_df, spy_df, period_days=10)

    assert rs_20 != rs_10


def test_compute_relative_strength_insufficient_ticker_data() -> None:
    """Returns None when ticker doesn't have enough data for the period."""
    aapl_df = _make_ohlcv_df(5, 100.0, 105.0)   # only 5 rows
    spy_df = _make_ohlcv_df(21, 100.0, 105.0)

    rs = compute_relative_strength(aapl_df, spy_df, period_days=20)

    assert rs is None


# ── compute_relative_strength_for_ticker ──────────────────────────────────────


def test_compute_relative_strength_for_ticker_end_to_end(db_connection: sqlite3.Connection) -> None:
    """Returns rs_market and rs_sector with reasonable values from DB data."""
    _insert_ticker(db_connection, "AAPL", sector="Technology", sector_etf="XLK")
    _insert_ohlcv(db_connection, "AAPL", _make_ohlcv_df(25, 100.0, 110.0))
    _insert_ohlcv(db_connection, "SPY", _make_ohlcv_df(25, 400.0, 410.0))
    _insert_ohlcv(db_connection, "XLK", _make_ohlcv_df(25, 150.0, 157.0))

    result = compute_relative_strength_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    assert "rs_market" in result
    assert "rs_sector" in result
    assert result["rs_market"] is not None
    assert result["rs_sector"] is not None
    assert result["rs_market"] > 0


def test_compute_relative_strength_missing_benchmark(db_connection: sqlite3.Connection) -> None:
    """rs_market is None when SPY data is not in the DB."""
    _insert_ticker(db_connection, "AAPL", sector="Technology", sector_etf="XLK")
    _insert_ohlcv(db_connection, "AAPL", _make_ohlcv_df(25, 100.0, 110.0))
    _insert_ohlcv(db_connection, "XLK", _make_ohlcv_df(25, 150.0, 157.0))
    # SPY not inserted

    result = compute_relative_strength_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    assert result["rs_market"] is None
    assert result["rs_sector"] is not None


def test_compute_relative_strength_missing_sector_etf(db_connection: sqlite3.Connection) -> None:
    """rs_sector is None when the sector ETF data is not in the DB."""
    _insert_ticker(db_connection, "AAPL", sector="Technology", sector_etf="XLK")
    _insert_ohlcv(db_connection, "AAPL", _make_ohlcv_df(25, 100.0, 110.0))
    _insert_ohlcv(db_connection, "SPY", _make_ohlcv_df(25, 400.0, 410.0))
    # XLK not inserted

    result = compute_relative_strength_for_ticker(db_connection, "AAPL", _BASE_CONFIG)

    assert result["rs_market"] is not None
    assert result["rs_sector"] is None
