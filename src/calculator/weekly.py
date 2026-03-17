"""
Weekly candle construction and weekly indicator computation.

Constructs weekly OHLCV candles from daily data:
  - open   = first trading day's open for the week
  - high   = maximum high across all trading days in the week
  - low    = minimum low across all trading days in the week
  - close  = last trading day's close for the week
  - volume = sum of all trading days' volume

The week is keyed by its Monday calendar date (regardless of whether Monday
was a trading day). Uses pd.Grouper(freq='W-MON') for grouping.

Then computes the same 15 technical indicators on the weekly candles by
reusing compute_all_indicators() from indicators.py.

Weekly indicators are used for dual timeframe confirmation in the scorer:
    Final Score = (Daily Score × 0.6) + (Weekly Score × 0.4)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from src.calculator.indicators import compute_all_indicators

logger = logging.getLogger(__name__)


def build_weekly_candles(
    ohlcv_df: pd.DataFrame,
    week_start_day: str = "Monday",
) -> pd.DataFrame:
    """
    Aggregate daily OHLCV into weekly candles.

    Each week is identified by its Monday calendar date. The candle is built as:
      - week_start: Monday's date (ISO string, even if Monday was a market holiday)
      - open:   first trading day's open in the week
      - high:   max(all highs in the week)
      - low:    min(all lows in the week)
      - close:  last trading day's close in the week
      - volume: sum(all volumes in the week)

    Args:
        ohlcv_df: DataFrame with columns: date (datetime or string), open, high, low,
                  close, volume. Must be sorted by date ascending.
        week_start_day: Day that defines the start of a week. Defaults to 'Monday'.
                        Only 'Monday' is currently supported.

    Returns:
        DataFrame with columns: week_start (ISO string), open, high, low, close, volume.
        Sorted by week_start ascending.
    """
    if ohlcv_df.empty:
        return pd.DataFrame(columns=["week_start", "open", "high", "low", "close", "volume"])

    df = ohlcv_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Compute the Monday of each date's ISO week:
    #   weekday() = 0 for Monday, ..., 4 for Friday
    #   Subtracting weekday() days always gives the preceding (or same-day) Monday.
    df["_week_start"] = df["date"].apply(
        lambda dt: (dt - pd.Timedelta(days=dt.weekday())).date().isoformat()
    )

    records = []
    for week_start_key, group in df.groupby("_week_start", sort=True):
        if group.empty:
            continue
        records.append(
            {
                "week_start": week_start_key,
                "open": float(group["open"].iloc[0]),
                "high": float(group["high"].max()),
                "low": float(group["low"].min()),
                "close": float(group["close"].iloc[-1]),
                "volume": float(group["volume"].sum()),
            }
        )

    result = pd.DataFrame(records)
    return result.sort_values("week_start").reset_index(drop=True)


def save_weekly_candles_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    weekly_df: pd.DataFrame,
) -> int:
    """
    Persist weekly candles to the weekly_candles table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the weekly_candles table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        weekly_df: DataFrame returned by build_weekly_candles().

    Returns:
        Number of rows saved.
    """
    count = 0
    for _, row in weekly_df.iterrows():
        db_conn.execute(
            """
            INSERT OR REPLACE INTO weekly_candles
                (ticker, week_start, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                row["week_start"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            ),
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} weekly candles for ticker={ticker}")
    return count


def compute_weekly_indicators(
    weekly_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Compute technical indicators on weekly candle data.

    Reuses compute_all_indicators() from indicators.py with the same config
    parameters as the daily computation. The weekly DataFrame uses week_start
    as the date column (renamed internally for compatibility).

    Args:
        weekly_df: DataFrame with columns: week_start, open, high, low, close, volume.
        config: Calculator config dict (same as used for daily indicators).

    Returns:
        Copy of weekly_df with all indicator columns added. week_start is preserved.
    """
    if weekly_df.empty:
        return weekly_df.copy()

    df = weekly_df.copy()
    # compute_all_indicators expects a 'date' column; rename for compatibility
    df = df.rename(columns={"week_start": "date"})
    df_with_indicators = compute_all_indicators(df, config)
    # Rename back to week_start
    df_with_indicators = df_with_indicators.rename(columns={"date": "week_start"})
    return df_with_indicators


def save_weekly_indicators_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    indicators_df: pd.DataFrame,
) -> int:
    """
    Persist weekly indicator values to the indicators_weekly table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the indicators_weekly table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        indicators_df: DataFrame returned by compute_weekly_indicators().

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
            INSERT OR REPLACE INTO indicators_weekly
                (ticker, week_start, {', '.join(indicator_cols)})
            VALUES (?, ?, {', '.join(['?'] * len(indicator_cols))})
            """,
            [ticker, row["week_start"]] + values,
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} weekly indicator rows for ticker={ticker}")
    return count


def compute_weekly_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    mode: str = "full",
) -> int:
    """
    End-to-end weekly computation for one ticker: build candles and compute indicators.

    Modes:
      'full':        Load ALL daily OHLCV, rebuild all weekly candles from scratch.
      'incremental': Find the latest week_start in weekly_candles, load daily OHLCV
                     from 2 weeks before that date onward (to recompute the last
                     partial week), build candles for the new period. For indicator
                     computation, loads existing weekly candles + new ones so the
                     indicator warm-up window is satisfied.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily, weekly_candles,
                 and indicators_weekly tables.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict.
        mode: 'full' or 'incremental'. Defaults to 'full'.

    Returns:
        Number of weekly candles created/updated.
    """
    week_start_day = config.get("weekly", {}).get("week_start_day", "Monday")

    if mode == "incremental":
        return _compute_weekly_incremental(db_conn, ticker, config, week_start_day)

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

    weekly_df = build_weekly_candles(ohlcv_df, week_start_day=week_start_day)
    if weekly_df.empty:
        return 0

    save_weekly_candles_to_db(db_conn, ticker, weekly_df)
    indicators_df = compute_weekly_indicators(weekly_df, config)
    save_weekly_indicators_to_db(db_conn, ticker, indicators_df)

    return len(weekly_df)


def _compute_weekly_incremental(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    week_start_day: str,
) -> int:
    """
    Incremental weekly computation: only process new/updated weeks.

    Finds the latest week_start already in weekly_candles, loads daily OHLCV
    from 14 days before that date (to recompute the last potentially partial week),
    and merges with existing weekly candles for indicator computation.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
        week_start_day: Week start day string.

    Returns:
        Number of new weekly candle rows saved.
    """
    latest_row = db_conn.execute(
        "SELECT MAX(week_start) AS latest FROM weekly_candles WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    if latest_row is None or latest_row["latest"] is None:
        # No existing data — fall back to full mode
        logger.info(f"No existing weekly data for ticker={ticker}, running full computation")
        return compute_weekly_for_ticker(db_conn, ticker, config, mode="full")

    latest_week_start = latest_row["latest"]
    # Load daily OHLCV from 2 weeks before the latest week_start
    latest_dt = datetime.fromisoformat(latest_week_start)
    cutoff_date = (latest_dt - timedelta(days=14)).date().isoformat()

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
    new_weekly = build_weekly_candles(new_ohlcv_df, week_start_day=week_start_day)

    if new_weekly.empty:
        return 0

    # Save only genuinely new weeks (week_start > previous latest)
    new_weeks = new_weekly[new_weekly["week_start"] > latest_week_start]
    if new_weeks.empty:
        logger.info(f"No new weeks for ticker={ticker}")
        return 0

    save_weekly_candles_to_db(db_conn, ticker, new_weeks)

    # For indicator computation, load ALL existing weekly candles + new ones
    # so that warm-up periods are satisfied
    all_candle_rows = db_conn.execute(
        "SELECT week_start, open, high, low, close, volume FROM weekly_candles "
        "WHERE ticker = ? ORDER BY week_start ASC",
        (ticker,),
    ).fetchall()
    all_weekly_df = pd.DataFrame([dict(row) for row in all_candle_rows])
    indicators_df = compute_weekly_indicators(all_weekly_df, config)

    # Save only the new weeks' indicators
    new_week_starts = set(new_weeks["week_start"].tolist())
    new_indicators = indicators_df[indicators_df["week_start"].isin(new_week_starts)]
    save_weekly_indicators_to_db(db_conn, ticker, new_indicators)

    return len(new_weeks)
