"""
Swing point detection.

A swing high is a candle whose high is higher than the N candles on BOTH sides.
A swing low is a candle whose low is lower than the N candles on BOTH sides.

N is configurable via calculator.json (default 5).

Swing points are the foundation for:
  - Support/Resistance levels
  - Double Top/Bottom patterns
  - Divergence detection (comparing indicator values at swing points)
  - Fibonacci retracement levels

Timeframe parametrization
-------------------------
The persistence-layer functions (``save_swing_points_to_db`` and
``detect_swing_points_for_ticker``) accept keyword-only arguments that select
the source candles table, source date-column name, destination swing-points
table and destination date-column name. Defaults preserve the original daily
behaviour (``ohlcv_daily`` -> ``swing_points`` keyed on ``date``).

Allowed values are validated against an explicit whitelist to prevent SQL
injection — SQLite cannot bind table or column identifiers.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# ── Whitelists ──────────────────────────────────────────────────────────────────
# Identifiers below are inlined into SQL via f-strings, so they must be validated
# against an explicit allow-list. No user input ever flows in here, but the
# whitelist also documents the supported timeframe surface.
_ALLOWED_SOURCE_CANDLES_TABLES: frozenset[str] = frozenset(
    {"ohlcv_daily", "weekly_candles", "monthly_candles"}
)
_ALLOWED_SOURCE_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_DEST_TABLES: frozenset[str] = frozenset(
    {"swing_points", "swing_points_weekly", "swing_points_monthly"}
)
_ALLOWED_DEST_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)


def _validate_identifier(name: str, allowed: frozenset[str], field: str) -> str:
    """
    Validate that an identifier (table or column name) is in the allow-list.

    SQLite bind parameters cannot stand in for identifiers, so any table or
    column name interpolated into a SQL string must be checked against an
    explicit set of known-good values to prevent injection.

    Args:
        name: The candidate identifier.
        allowed: Frozenset of permitted identifier strings.
        field: Human-readable label used in the error message (e.g. "dest_table").

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


def detect_swing_points(ohlcv_df: pd.DataFrame, lookback_candles: int = 5) -> list[dict]:
    """
    Detect swing highs and swing lows in an OHLCV DataFrame.

    A swing high is a candle whose high exceeds all highs in the lookback_candles
    candles immediately before and after it. A swing low is similarly the lowest low.

    Strength is computed by extending the comparison beyond lookback_candles and
    counting how many candles on each side the swing point dominates. Strength equals
    the minimum of the left and right domination counts.

    Args:
        ohlcv_df: DataFrame with columns: date, open, high, low, close, volume.
            Must be sorted by date ascending. The ``date`` column is treated as
            an opaque period key — it may carry daily ``date``, weekly
            ``week_start`` or monthly ``month_start`` values; the caller is
            responsible for choosing the right source.
        lookback_candles: Number of candles required on each side for detection.

    Returns:
        List of dicts with keys: date (str), type ('high' or 'low'), price (float),
        strength (int).
    """
    df = ohlcv_df.reset_index(drop=True)
    n = len(df)
    swing_points: list[dict] = []

    for i in range(lookback_candles, n - lookback_candles):
        curr_high = df.iloc[i]["high"]
        curr_low = df.iloc[i]["low"]
        curr_date = df.iloc[i]["date"]

        # ── Swing High check ────────────────────────────────────────────────
        left_highs = df.iloc[i - lookback_candles: i]["high"]
        right_highs = df.iloc[i + 1: i + lookback_candles + 1]["high"]

        if (curr_high > left_highs).all() and (curr_high > right_highs).all():
            strength = _compute_strength(df, i, "high", lookback_candles, n)
            swing_points.append({
                "date": curr_date,
                "type": "high",
                "price": float(curr_high),
                "strength": strength,
            })

        # ── Swing Low check ─────────────────────────────────────────────────
        left_lows = df.iloc[i - lookback_candles: i]["low"]
        right_lows = df.iloc[i + 1: i + lookback_candles + 1]["low"]

        if (curr_low < left_lows).all() and (curr_low < right_lows).all():
            strength = _compute_strength(df, i, "low", lookback_candles, n)
            swing_points.append({
                "date": curr_date,
                "type": "low",
                "price": float(curr_low),
                "strength": strength,
            })

    return swing_points


def _compute_strength(
    df: pd.DataFrame, index: int, point_type: str, min_lookback: int, n: int
) -> int:
    """
    Extend comparison beyond the minimum lookback and count how many candles
    on each side the swing point dominates.

    Args:
        df: Full OHLCV DataFrame (reset index).
        index: Row index of the swing point.
        point_type: 'high' or 'low'.
        min_lookback: Minimum lookback already confirmed.
        n: Total number of rows in df.

    Returns:
        Strength as the minimum of left and right domination counts.
    """
    value = df.iloc[index]["high"] if point_type == "high" else df.iloc[index]["low"]
    col = "high" if point_type == "high" else "low"

    left_count = 0
    for j in range(index - 1, -1, -1):
        if point_type == "high" and df.iloc[j][col] < value:
            left_count += 1
        elif point_type == "low" and df.iloc[j][col] > value:
            left_count += 1
        else:
            break

    right_count = 0
    for j in range(index + 1, n):
        if point_type == "high" and df.iloc[j][col] < value:
            right_count += 1
        elif point_type == "low" and df.iloc[j][col] > value:
            right_count += 1
        else:
            break

    return min(left_count, right_count)


def save_swing_points_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    swing_points: list[dict],
    *,
    dest_table: str = "swing_points",
    date_column_name: str = "date",
) -> int:
    """
    Delete existing swing points for this ticker and insert fresh ones.

    Uses INSERT OR REPLACE to handle the UNIQUE(ticker, <date_col>, type) constraint.
    The destination table and date-column name are validated against a whitelist
    so identifiers can be safely interpolated into SQL — SQLite parameter
    binding does not cover identifiers.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        swing_points: List of dicts from detect_swing_points(). The ``date``
            key in each dict is written under ``date_column_name`` in the
            destination table.
        dest_table: Destination table name. Must be one of ``swing_points``,
            ``swing_points_weekly`` or ``swing_points_monthly``. Defaults to
            ``swing_points`` (daily).
        date_column_name: Name of the date column in ``dest_table``. Must be
            one of ``date``, ``week_start`` or ``month_start``. Defaults to
            ``date`` (daily).

    Returns:
        Number of rows inserted.

    Raises:
        ValueError: If ``dest_table`` or ``date_column_name`` is not in the
            allow-list.
    """
    safe_dest_table = _validate_identifier(
        dest_table, _ALLOWED_DEST_TABLES, "dest_table"
    )
    safe_date_col = _validate_identifier(
        date_column_name, _ALLOWED_DEST_DATE_COLUMNS, "date_column_name"
    )

    db_conn.execute(
        f"DELETE FROM {safe_dest_table} WHERE ticker = ?", (ticker,)
    )
    if not swing_points:
        db_conn.commit()
        return 0

    rows = [
        (ticker, sp["date"], sp["type"], sp["price"], sp["strength"])
        for sp in swing_points
    ]
    db_conn.executemany(
        f"INSERT OR REPLACE INTO {safe_dest_table} "
        f"(ticker, {safe_date_col}, type, price, strength) VALUES (?,?,?,?,?)",
        rows,
    )
    db_conn.commit()
    logger.info(
        "ticker=%s phase=swing_points dest=%s saved=%d swing points",
        ticker,
        safe_dest_table,
        len(rows),
    )
    return len(rows)


def detect_swing_points_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    *,
    source_candles_table: str = "ohlcv_daily",
    source_date_column: str = "date",
    dest_table: str = "swing_points",
    date_column_name: str = "date",
) -> int:
    """
    Load OHLCV from the configured source table, detect swing points, and save
    results to the configured destination table.

    Defaults preserve the original daily behaviour: read from ``ohlcv_daily``
    keyed by ``date`` and write to ``swing_points`` keyed by ``date``. To run
    against weekly or monthly candles, override the four keyword-only
    parameters; the source date column is aliased to ``date`` internally so the
    pure detection logic is timeframe-agnostic.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["swing_points"]["lookback_candles"].
        source_candles_table: Table to read OHLCV-like rows from. Must be one
            of ``ohlcv_daily``, ``weekly_candles`` or ``monthly_candles``.
        source_date_column: Date-column name in ``source_candles_table``. Must
            be one of ``date``, ``week_start`` or ``month_start``.
        dest_table: Destination swing-points table. Must be one of
            ``swing_points``, ``swing_points_weekly`` or ``swing_points_monthly``.
        date_column_name: Date-column name in ``dest_table``. Must be one of
            ``date``, ``week_start`` or ``month_start``.

    Returns:
        Number of swing points detected and saved.

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    safe_source_table = _validate_identifier(
        source_candles_table, _ALLOWED_SOURCE_CANDLES_TABLES, "source_candles_table"
    )
    safe_source_date_col = _validate_identifier(
        source_date_column, _ALLOWED_SOURCE_DATE_COLUMNS, "source_date_column"
    )
    # Validate dest identifiers eagerly so misuse fails fast even when the
    # source returns no rows.
    _validate_identifier(dest_table, _ALLOWED_DEST_TABLES, "dest_table")
    _validate_identifier(
        date_column_name, _ALLOWED_DEST_DATE_COLUMNS, "date_column_name"
    )

    lookback = config.get("swing_points", {}).get("lookback_candles", 5)

    rows = db_conn.execute(
        f"SELECT {safe_source_date_col} AS date, open, high, low, close, volume "
        f"FROM {safe_source_table} WHERE ticker = ? "
        f"ORDER BY {safe_source_date_col} ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        logger.warning(
            "ticker=%s phase=swing_points source=%s no OHLCV data found",
            ticker,
            safe_source_table,
        )
        return 0

    ohlcv_df = pd.DataFrame([dict(r) for r in rows])

    try:
        swing_points = detect_swing_points(ohlcv_df, lookback_candles=lookback)
    except Exception as exc:
        logger.error("ticker=%s phase=swing_points error=%s", ticker, exc)
        db_conn.execute(
            "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
            (ticker, "swing_points_error", str(exc), datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
        return 0

    return save_swing_points_to_db(
        db_conn,
        ticker,
        swing_points,
        dest_table=dest_table,
        date_column_name=date_column_name,
    )
