"""
Relative strength computation vs market and sector benchmarks.

Computes how a ticker is performing relative to:
  - SPY (market benchmark):   RS_market = (1 + ticker_return) / (1 + SPY_return)
  - Sector ETF benchmark:     RS_sector = (1 + ticker_return) / (1 + sector_ETF_return)

RS > 1 means the ticker is outperforming its benchmark.
RS < 1 means the ticker is underperforming.

The formula (1 + r_ticker) / (1 + r_benchmark) handles negative returns correctly:
  - AAPL -5%, SPY -10%:  (1-0.05)/(1-0.10) = 0.95/0.90 = 1.055 → outperforming ✓
  - AAPL +3%, SPY +8%:   (1.03)/(1.08) = 0.954 → underperforming ✓
  - AAPL flat, SPY flat: (1.00)/(1.00) = 1.0 → neutral ✓

Results are NOT stored in a table — the scorer calls compute_relative_strength_for_ticker
on-the-fly when building the macro category score.

The period defaults to 20 trading days (≈ 1 month), configurable via
config['relative_strength']['period_days'].
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_MARKET_BENCHMARK = "SPY"


def compute_return(ohlcv_df: pd.DataFrame, period_days: int) -> Optional[float]:
    """
    Compute the price return over the last period_days for a given OHLCV DataFrame.

    Return = (last_close - close_at_period_start) / close_at_period_start

    Requires at least (period_days + 1) rows so that both the starting close
    and the ending close are available.

    Args:
        ohlcv_df: DataFrame with at least a 'close' column, sorted by date ascending.
        period_days: Number of trading days to look back. E.g. 20 for 1 month.

    Returns:
        Float return as a decimal (e.g. 0.08 for 8%), or None if insufficient data.
    """
    if ohlcv_df is None or len(ohlcv_df) < period_days + 1:
        return None

    closes = ohlcv_df["close"].to_numpy(dtype=float)
    start_close = closes[-(period_days + 1)]
    end_close = closes[-1]

    if start_close == 0:
        return None

    return (end_close - start_close) / start_close


def compute_relative_strength(
    ticker_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    period_days: int,
) -> Optional[float]:
    """
    Compute relative strength of ticker vs a benchmark over period_days.

    Uses the formula: RS = (1 + ticker_return) / (1 + benchmark_return)

    This handles all return sign combinations correctly:
      - Both positive: normal outperformance ratio
      - Ticker negative, benchmark more negative: RS > 1 (ticker fell less)
      - Benchmark return of exactly -100% is handled as None (division by zero)

    Args:
        ticker_df: OHLCV DataFrame for the ticker, sorted ascending.
        benchmark_df: OHLCV DataFrame for the benchmark (e.g. SPY), sorted ascending.
        period_days: Number of trading days for the return computation.

    Returns:
        RS ratio as a float, or None if data is insufficient for either series.
    """
    ticker_return = compute_return(ticker_df, period_days)
    benchmark_return = compute_return(benchmark_df, period_days)

    if ticker_return is None or benchmark_return is None:
        return None

    denominator = 1.0 + benchmark_return
    if denominator == 0:
        return None

    return (1.0 + ticker_return) / denominator


def compute_relative_strength_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
) -> dict:
    """
    Compute relative strength for a ticker vs SPY and vs its sector ETF.

    Loads OHLCV data for the ticker, SPY, and the ticker's sector ETF from
    the ohlcv_daily table. The sector ETF is looked up from the tickers table.

    If either benchmark's data is missing or insufficient, that RS value is
    returned as None (graceful degradation).

    Args:
        db_conn: Open SQLite connection with ohlcv_daily and tickers tables.
        ticker: Ticker symbol to evaluate, e.g. 'AAPL'.
        config: Calculator config dict. Reads
            config['relative_strength']['period_days'] (default 20).

    Returns:
        Dict with keys:
            'rs_market' (float | None): RS vs SPY
            'rs_sector' (float | None): RS vs sector ETF
    """
    period_days = config.get("relative_strength", {}).get("period_days", 20)

    ticker_df = _load_ohlcv(db_conn, ticker)
    spy_df = _load_ohlcv(db_conn, _MARKET_BENCHMARK)
    sector_etf = _get_sector_etf(db_conn, ticker)
    sector_df = _load_ohlcv(db_conn, sector_etf) if sector_etf else None

    if ticker_df.empty:
        logger.warning(f"No OHLCV data for ticker={ticker}, cannot compute relative strength")
        return {"rs_market": None, "rs_sector": None}

    rs_market: Optional[float] = None
    if not spy_df.empty:
        rs_market = compute_relative_strength(ticker_df, spy_df, period_days)
        if rs_market is None:
            logger.warning(
                f"Insufficient data to compute rs_market for ticker={ticker}: "
                f"ticker_rows={len(ticker_df)}, spy_rows={len(spy_df)}, period={period_days}"
            )
    else:
        logger.warning(f"No SPY data in ohlcv_daily, rs_market=None for ticker={ticker}")

    rs_sector: Optional[float] = None
    if sector_df is not None and not sector_df.empty:
        rs_sector = compute_relative_strength(ticker_df, sector_df, period_days)
        if rs_sector is None:
            logger.warning(
                f"Insufficient data to compute rs_sector for ticker={ticker}: "
                f"sector_etf={sector_etf}"
            )
    else:
        logger.warning(f"No sector ETF data for ticker={ticker} (etf={sector_etf}), rs_sector=None")

    return {"rs_market": rs_market, "rs_sector": rs_sector}


def _load_ohlcv(db_conn: sqlite3.Connection, ticker: str) -> pd.DataFrame:
    """
    Load OHLCV data for a ticker from ohlcv_daily, sorted by date ascending.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol to query.

    Returns:
        DataFrame with columns: date, open, high, low, close, volume.
        Returns an empty DataFrame if the ticker is not found.
    """
    if not ticker:
        return pd.DataFrame()

    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    return pd.DataFrame([dict(row) for row in rows])


def _get_sector_etf(db_conn: sqlite3.Connection, ticker: str) -> Optional[str]:
    """
    Look up the sector ETF symbol for a given ticker from the tickers table.

    Args:
        db_conn: Open SQLite connection with the tickers table.
        ticker: Ticker symbol.

    Returns:
        Sector ETF symbol string, or None if not found.
    """
    row = db_conn.execute(
        "SELECT sector_etf FROM tickers WHERE symbol = ?",
        (ticker,),
    ).fetchone()

    if row is None or row["sector_etf"] is None:
        return None

    return row["sector_etf"]
