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

Timeframe parametrization
-------------------------
``save_crossovers_to_db`` and ``detect_crossovers_for_ticker`` accept
keyword-only arguments selecting the source indicators table and destination
crossovers table + date-column name. Defaults preserve the original daily
behaviour. Identifiers are validated against an explicit whitelist.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


# ── Whitelists ──────────────────────────────────────────────────────────────────
_ALLOWED_SOURCE_INDICATORS_TABLES: frozenset[str] = frozenset(
    {"indicators_daily", "indicators_weekly", "indicators_monthly"}
)
_ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_DEST_TABLES: frozenset[str] = frozenset(
    {"crossovers_daily", "crossovers_weekly", "crossovers_monthly"}
)
_ALLOWED_DEST_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)


def _validate_identifier(name: str, allowed: frozenset[str], field: str) -> str:
    """
    Validate that an identifier (table or column name) is in the allow-list.

    SQLite parameter binding does not cover identifiers, so any table or
    column name interpolated into SQL must be checked against an explicit
    set of permitted values.

    Args:
        name: Candidate identifier.
        allowed: Frozenset of permitted identifier strings.
        field: Human-readable label used in the error message.

    Returns:
        The validated identifier verbatim.

    Raises:
        ValueError: If ``name`` is not in ``allowed``.
    """
    if name not in allowed:
        raise ValueError(
            f"Invalid {field}={name!r}; expected one of {sorted(allowed)}"
        )
    return name


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
    db_conn: sqlite3.Connection,
    ticker: str,
    crossovers: list[dict],
    *,
    dest_table: str = "crossovers_daily",
    date_column_name: str = "date",
) -> int:
    """
    Replace all crossover records for a ticker with fresh data.

    Deletes existing crossovers for the ticker and inserts all new records.
    This reflects the recompute-from-scratch semantic. The destination table
    and its date-column are validated against a whitelist before being
    interpolated into SQL.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        crossovers: List of crossover dicts with keys: date, crossover_type,
                    direction, days_ago.
        dest_table: Destination crossovers table. Must be one of
            ``crossovers_daily``, ``crossovers_weekly`` or
            ``crossovers_monthly``. Defaults to ``crossovers_daily``.
        date_column_name: Name of the date / period-key column in
            ``dest_table``. Must be one of ``date``, ``week_start`` or
            ``month_start``. Defaults to ``date``.

    Returns:
        Number of rows saved.

    Raises:
        ValueError: If either identifier is not in the allow-list.
    """
    safe_dest_table = _validate_identifier(
        dest_table, _ALLOWED_DEST_TABLES, "dest_table"
    )
    safe_date_col = _validate_identifier(
        date_column_name, _ALLOWED_DEST_DATE_COLUMNS, "date_column_name"
    )

    db_conn.execute(f"DELETE FROM {safe_dest_table} WHERE ticker = ?", (ticker,))

    for event in crossovers:
        db_conn.execute(
            f"""INSERT INTO {safe_dest_table}(ticker, {safe_date_col}, crossover_type, direction, days_ago)
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
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    *,
    source_indicators_table: str = "indicators_daily",
    source_indicators_date_column: str = "date",
    dest_table: str = "crossovers_daily",
    dest_date_column: str = "date",
) -> int:
    """
    Load indicators from DB, detect all crossovers, and save to a crossovers table.

    Defaults preserve the original daily behaviour: read indicators from
    ``indicators_daily`` keyed by ``date`` and write to ``crossovers_daily``
    keyed by ``date``. To run against the weekly or monthly mirrors, override
    the keyword-only parameters.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
        source_indicators_table: Indicators source table. Must be one of
            ``indicators_daily``, ``indicators_weekly`` or
            ``indicators_monthly``.
        source_indicators_date_column: Date-column name in
            ``source_indicators_table``. Must be one of ``date``,
            ``week_start`` or ``month_start``.
        dest_table: Destination crossovers table. Must be one of
            ``crossovers_daily``, ``crossovers_weekly`` or
            ``crossovers_monthly``.
        dest_date_column: Date-column name in ``dest_table``.

    Returns:
        Number of crossover events found and saved.

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    safe_source_table = _validate_identifier(
        source_indicators_table,
        _ALLOWED_SOURCE_INDICATORS_TABLES,
        "source_indicators_table",
    )
    safe_source_date_col = _validate_identifier(
        source_indicators_date_column,
        _ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS,
        "source_indicators_date_column",
    )
    # Validate dest identifiers eagerly so misuse fails fast.
    _validate_identifier(dest_table, _ALLOWED_DEST_TABLES, "dest_table")
    _validate_identifier(
        dest_date_column, _ALLOWED_DEST_DATE_COLUMNS, "dest_date_column"
    )

    try:
        cursor = db_conn.execute(
            f"""SELECT {safe_source_date_col} AS date, ema_9, ema_21, ema_50, macd_line, macd_signal
               FROM {safe_source_table}
               WHERE ticker = ?
               ORDER BY {safe_source_date_col} ASC""",
            (ticker,),
        )
        rows = cursor.fetchall()

        if not rows:
            logger.warning(
                f"No indicator data found for {ticker} in {safe_source_table}"
            )
            return 0

        indicators_df = pd.DataFrame([dict(row) for row in rows])
        crossovers = detect_all_crossovers(indicators_df, config)
        count = save_crossovers_to_db(
            db_conn,
            ticker,
            crossovers,
            dest_table=dest_table,
            date_column_name=dest_date_column,
        )
        logger.info(
            f"Detected {count} crossovers for {ticker} dest={dest_table}"
        )
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
