"""
Crossover event detection.

Detects when moving averages or MACD lines cross each other and stores
these as events with a date, type, direction, and days_ago value.

Crossovers detected:
    - EMA 9/21:      fast EMA crosses slow EMA
    - EMA 21/50:     medium EMA crosses slow EMA
    - MACD signal:   MACD line crosses signal line

Each crossover records:
    - date:           ISO date string when the crossover occurred
    - crossover_type: 'ema_9_21', 'ema_21_50', or 'macd_signal'
    - direction:      'bullish' (fast crosses above slow) or 'bearish'
    - days_ago:       trading days before the most recent date in the dataset
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def detect_crossover_events(
    series_fast: pd.Series,
    series_slow: pd.Series,
    dates: pd.Series,
) -> list[dict]:
    """
    Detect all crossover events between two series.

    A bullish crossover occurs when series_fast crosses above series_slow:
        series_fast[i-1] <= series_slow[i-1] AND series_fast[i] > series_slow[i]

    A bearish crossover occurs when series_fast crosses below series_slow:
        series_fast[i-1] >= series_slow[i-1] AND series_fast[i] < series_slow[i]

    Rows where either series is NaN are skipped.

    Args:
        series_fast: The faster (more reactive) indicator series.
        series_slow: The slower indicator series.
        dates: Corresponding date strings (same index as the series).

    Returns:
        List of dicts with keys: 'date', 'direction'.
    """
    crossovers: list[dict] = []
    fast = series_fast.reset_index(drop=True)
    slow = series_slow.reset_index(drop=True)
    dates_reset = dates.reset_index(drop=True)

    for i in range(1, len(fast)):
        prev_fast = fast.iloc[i - 1]
        prev_slow = slow.iloc[i - 1]
        curr_fast = fast.iloc[i]
        curr_slow = slow.iloc[i]

        if pd.isna(prev_fast) or pd.isna(prev_slow) or pd.isna(curr_fast) or pd.isna(curr_slow):
            continue

        if prev_fast <= prev_slow and curr_fast > curr_slow:
            crossovers.append({"date": dates_reset.iloc[i], "direction": "bullish"})
        elif prev_fast >= prev_slow and curr_fast < curr_slow:
            crossovers.append({"date": dates_reset.iloc[i], "direction": "bearish"})

    return crossovers


def detect_all_crossovers(indicators_df: pd.DataFrame, config: dict) -> list[dict]:
    """
    Detect all configured crossover events in an indicators DataFrame.

    Detects crossovers for:
        - EMA 9 vs EMA 21  → crossover_type='ema_9_21'
        - EMA 21 vs EMA 50 → crossover_type='ema_21_50'
        - MACD vs Signal   → crossover_type='macd_signal'

    Computes days_ago for each event relative to the last date in the DataFrame
    (0 = last row, 1 = second to last, etc.).

    Args:
        indicators_df: DataFrame containing at minimum: date, ema_9, ema_21, ema_50,
                       macd_line, macd_signal.
        config: Calculator config (currently unused but included for extensibility).

    Returns:
        List of dicts with keys: date, crossover_type, direction, days_ago.
    """
    df = indicators_df.reset_index(drop=True)
    dates = df["date"]
    last_index = len(df) - 1

    all_crossovers: list[dict] = []

    crossover_pairs = [
        ("ema_9", "ema_21", "ema_9_21"),
        ("ema_21", "ema_50", "ema_21_50"),
        ("macd_line", "macd_signal", "macd_signal"),
    ]

    for fast_col, slow_col, crossover_type in crossover_pairs:
        if fast_col not in df.columns or slow_col not in df.columns:
            continue

        raw_events = detect_crossover_events(df[fast_col], df[slow_col], dates)
        for event in raw_events:
            row_index = dates[dates == event["date"]].index
            if len(row_index) == 0:
                continue
            days_ago = last_index - row_index[-1]
            all_crossovers.append(
                {
                    "date": event["date"],
                    "crossover_type": crossover_type,
                    "direction": event["direction"],
                    "days_ago": int(days_ago),
                }
            )

    return all_crossovers


def save_crossovers_to_db(
    db_conn: sqlite3.Connection, ticker: str, crossovers: list[dict]
) -> int:
    """
    Replace all crossover records for a ticker with fresh data.

    Deletes existing crossovers for the ticker and inserts all new records.
    This reflects the recompute-from-scratch semantic.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        crossovers: List of crossover dicts with keys: date, crossover_type,
                    direction, days_ago.

    Returns:
        Number of rows saved.
    """
    db_conn.execute("DELETE FROM crossovers_daily WHERE ticker = ?", (ticker,))

    for event in crossovers:
        db_conn.execute(
            """INSERT INTO crossovers_daily(ticker, date, crossover_type, direction, days_ago)
               VALUES (?, ?, ?, ?, ?)""",
            (
                ticker,
                event["date"],
                event["crossover_type"],
                event["direction"],
                event["days_ago"],
            ),
        )

    db_conn.commit()
    return len(crossovers)


def detect_crossovers_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> int:
    """
    Load indicators from DB, detect all crossovers, and save to crossovers_daily.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.

    Returns:
        Number of crossover events found and saved.
    """
    try:
        cursor = db_conn.execute(
            """SELECT date, ema_9, ema_21, ema_50, macd_line, macd_signal
               FROM indicators_daily
               WHERE ticker = ?
               ORDER BY date ASC""",
            (ticker,),
        )
        rows = cursor.fetchall()

        if not rows:
            logger.warning(f"No indicator data found for {ticker} in indicators_daily")
            return 0

        indicators_df = pd.DataFrame([dict(row) for row in rows])
        crossovers = detect_all_crossovers(indicators_df, config)
        count = save_crossovers_to_db(db_conn, ticker, crossovers)
        logger.info(f"Detected {count} crossovers for {ticker}")
        return count

    except Exception as exc:
        logger.error(f"Failed to detect crossovers for {ticker}: {exc}", exc_info=True)
        _log_alert(db_conn, ticker, "calculator-crossovers", str(exc))
        return 0


def _log_alert(
    db_conn: sqlite3.Connection, ticker: str, phase: str, message: str
) -> None:
    """Write a failure record to alerts_log."""
    now = datetime.now(tz=timezone.utc).isoformat()
    try:
        db_conn.execute(
            """INSERT INTO alerts_log(ticker, date, phase, severity, message, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ticker, now[:10], phase, "ERROR", message, now),
        )
        db_conn.commit()
    except Exception:
        pass
