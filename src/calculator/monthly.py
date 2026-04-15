"""
Monthly candle construction and monthly indicator computation.

Constructs monthly OHLCV candles from daily data:
  - open   = first trading day's open for the month
  - high   = maximum high across all trading days in the month
  - low    = minimum low across all trading days in the month
  - close  = last trading day's close for the month
  - volume = sum of all trading days' volume

Each month is keyed by YYYY-MM-01 (the first calendar day of the month),
regardless of whether that day is a trading day.

Then computes the same 15 technical indicators on the monthly candles by
reusing compute_all_indicators() from indicators.py.

Monthly indicators are used for triple timeframe confirmation in the scorer:
    Final Score = (Daily × w_d) + (Weekly × w_w) + (Monthly × w_m)
where weights are regime-adaptive (see config/scorer.json).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.calculator.indicators import compute_all_indicators

logger = logging.getLogger(__name__)


def build_monthly_candles(
    ohlcv_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate daily OHLCV into monthly candles.

    Each month is identified by its first calendar day (YYYY-MM-01). The
    candle is built as:
      - month_start: YYYY-MM-01 (ISO string, always the 1st of the month)
      - open:   first trading day's open in the month
      - high:   max(all highs in the month)
      - low:    min(all lows in the month)
      - close:  last trading day's close in the month
      - volume: sum(all volumes in the month)

    Args:
        ohlcv_df: DataFrame with columns: date (datetime or string), open, high,
                  low, close, volume. Must be sorted by date ascending.

    Returns:
        DataFrame with columns: month_start (ISO string), open, high, low, close,
        volume. Sorted by month_start ascending.
    """
    if ohlcv_df.empty:
        return pd.DataFrame(columns=["month_start", "open", "high", "low", "close", "volume"])

    df = ohlcv_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Compute YYYY-MM-01 for each date
    df["_month_start"] = df["date"].apply(
        lambda dt: dt.replace(day=1).date().isoformat()
    )

    records = []
    for month_start_key, group in df.groupby("_month_start", sort=True):
        if group.empty:
            continue
        records.append(
            {
                "month_start": month_start_key,
                "open": float(group["open"].iloc[0]),
                "high": float(group["high"].max()),
                "low": float(group["low"].min()),
                "close": float(group["close"].iloc[-1]),
                "volume": float(group["volume"].sum()),
            }
        )

    result = pd.DataFrame(records)
    return result.sort_values("month_start").reset_index(drop=True)


def save_monthly_candles_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    monthly_df: pd.DataFrame,
) -> int:
    """
    Persist monthly candles to the monthly_candles table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the monthly_candles table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        monthly_df: DataFrame returned by build_monthly_candles().

    Returns:
        Number of rows saved.
    """
    count = 0
    for _, row in monthly_df.iterrows():
        db_conn.execute(
            """
            INSERT OR REPLACE INTO monthly_candles
                (ticker, month_start, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                row["month_start"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            ),
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} monthly candles for ticker={ticker}")
    return count


def compute_monthly_indicators(
    monthly_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Compute technical indicators on monthly candle data.

    Reuses compute_all_indicators() from indicators.py with the same config
    parameters as the daily/weekly computation. The monthly DataFrame uses
    month_start as the date column (renamed internally for compatibility).

    Args:
        monthly_df: DataFrame with columns: month_start, open, high, low, close, volume.
        config: Calculator config dict (same as used for daily/weekly indicators).

    Returns:
        Copy of monthly_df with all indicator columns added. month_start is preserved.
    """
    if monthly_df.empty:
        return monthly_df.copy()

    df = monthly_df.copy()
    # compute_all_indicators expects a 'date' column; rename for compatibility
    df = df.rename(columns={"month_start": "date"})
    df_with_indicators = compute_all_indicators(df, config)
    # Rename back to month_start
    df_with_indicators = df_with_indicators.rename(columns={"date": "month_start"})
    return df_with_indicators


def save_monthly_indicators_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    indicators_df: pd.DataFrame,
) -> int:
    """
    Persist monthly indicator values to the indicators_monthly table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the indicators_monthly table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        indicators_df: DataFrame returned by compute_monthly_indicators().

    Returns:
        Number of rows saved.
    """
    indicator_cols = [
        "ema_9", "ema_21", "ema_50",
        "macd_line", "macd_signal", "macd_histogram",
        "adx", "rsi_14", "stoch_k", "stoch_d",
        "cci_20", "williams_r", "obv", "cmf_20", "ad_line",
        "bb_upper", "bb_lower", "bb_pctb",
        "atr_14", "keltner_upper", "keltner_lower",
    ]

    def _nan_to_none(val: object) -> object:
        if isinstance(val, float) and (val != val):  # NaN check
            return None
        try:
            import math
            if math.isnan(float(val)):  # type: ignore[arg-type]
                return None
        except (TypeError, ValueError):
            pass
        return val

    count = 0
    for _, row in indicators_df.iterrows():
        values = [_nan_to_none(row.get(col)) for col in indicator_cols]
        db_conn.execute(
            f"""
            INSERT OR REPLACE INTO indicators_monthly
                (ticker, month_start, {', '.join(indicator_cols)})
            VALUES (?, ?, {', '.join(['?'] * len(indicator_cols))})
            """,
            [ticker, row["month_start"]] + values,
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} monthly indicator rows for ticker={ticker}")
    return count


def compute_monthly_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    mode: str = "full",
) -> int:
    """
    End-to-end monthly computation for one ticker: build candles and compute indicators.

    Modes:
      'full':        Load ALL daily OHLCV, rebuild all monthly candles from scratch.
      'incremental': Find the latest month_start in monthly_candles, load daily OHLCV
                     from 2 months before that date onward (to recompute the last
                     partial month), build candles for the new period. For indicator
                     computation, loads existing monthly candles + new ones so the
                     indicator warm-up window is satisfied.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily, monthly_candles,
                 and indicators_monthly tables.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict.
        mode: 'full' or 'incremental'. Defaults to 'full'.

    Returns:
        Number of monthly candles created/updated.
    """
    if mode == "incremental":
        return _compute_monthly_incremental(db_conn, ticker, config)

    # --- full mode ---
    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        logger.warning(f"No OHLCV data found for ticker={ticker}")
        return 0

    ohlcv_df = pd.DataFrame([dict(row) for row in rows])
    ohlcv_df["date"] = pd.to_datetime(ohlcv_df["date"])

    monthly_df = build_monthly_candles(ohlcv_df)
    if monthly_df.empty:
        return 0

    save_monthly_candles_to_db(db_conn, ticker, monthly_df)
    indicators_df = compute_monthly_indicators(monthly_df, config)
    save_monthly_indicators_to_db(db_conn, ticker, indicators_df)

    return len(monthly_df)


def _compute_monthly_incremental(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
) -> int:
    """
    Incremental monthly computation: only process new/updated months.

    Finds the latest month_start already in monthly_candles, loads daily OHLCV
    from ~2 months before that date (to recompute the last potentially partial
    month), and merges with existing monthly candles for indicator computation.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.

    Returns:
        Number of new monthly candle rows saved.
    """
    latest_row = db_conn.execute(
        "SELECT MAX(month_start) AS latest FROM monthly_candles WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    if latest_row is None or latest_row["latest"] is None:
        # No existing data — fall back to full mode
        logger.info(f"No existing monthly data for ticker={ticker}, running full computation")
        return compute_monthly_for_ticker(db_conn, ticker, config, mode="full")

    latest_month_start = latest_row["latest"]
    # Load daily OHLCV from ~2 months before the latest month_start
    latest_dt = datetime.fromisoformat(latest_month_start)
    # Go back ~62 days (2 calendar months) to ensure we catch the full previous month
    cutoff_date = (latest_dt - timedelta(days=62)).date().isoformat()

    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? AND date >= ? ORDER BY date ASC",
        (ticker, cutoff_date),
    ).fetchall()

    if not rows:
        logger.info(f"No new OHLCV data for ticker={ticker} since {cutoff_date}")
        return 0

    new_ohlcv_df = pd.DataFrame([dict(row) for row in rows])
    new_ohlcv_df["date"] = pd.to_datetime(new_ohlcv_df["date"])
    new_monthly = build_monthly_candles(new_ohlcv_df)

    if new_monthly.empty:
        return 0

    # Save only genuinely new months (month_start > previous latest)
    new_months = new_monthly[new_monthly["month_start"] > latest_month_start]
    if new_months.empty:
        logger.info(f"No new months for ticker={ticker}")
        return 0

    save_monthly_candles_to_db(db_conn, ticker, new_months)

    # For indicator computation, load ALL existing monthly candles + new ones
    # so that warm-up periods are satisfied
    all_candle_rows = db_conn.execute(
        "SELECT month_start, open, high, low, close, volume FROM monthly_candles "
        "WHERE ticker = ? ORDER BY month_start ASC",
        (ticker,),
    ).fetchall()
    all_monthly_df = pd.DataFrame([dict(row) for row in all_candle_rows])
    indicators_df = compute_monthly_indicators(all_monthly_df, config)

    # Save only the new months' indicators
    new_month_starts = set(new_months["month_start"].tolist())
    new_indicators = indicators_df[indicators_df["month_start"].isin(new_month_starts)]
    save_monthly_indicators_to_db(db_conn, ticker, new_indicators)

    return len(new_months)
