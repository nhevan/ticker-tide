"""
Support and resistance level detection.

Clusters nearby swing points into price levels. A level is formed when 2+
swing points occur at approximately the same price (within a configurable
tolerance). More touches = stronger level.

Also detects when levels are broken (price closes beyond the level).

Timeframe parametrization
-------------------------
``detect_support_resistance_for_ticker`` and ``save_sr_levels_to_db`` accept
keyword-only arguments selecting the source swing table, source candles table,
destination S/R table and the destination date-column name. Defaults preserve
the original daily behaviour (reads ``swing_points`` + ``ohlcv_daily``, writes
``support_resistance`` keyed by ``date_computed``). Identifiers are validated
against an explicit whitelist.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# ── Whitelists ──────────────────────────────────────────────────────────────────
_ALLOWED_SOURCE_SWING_TABLES: frozenset[str] = frozenset(
    {"swing_points", "swing_points_weekly", "swing_points_monthly"}
)
_ALLOWED_SOURCE_SWING_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_SOURCE_CANDLES_TABLES: frozenset[str] = frozenset(
    {"ohlcv_daily", "weekly_candles", "monthly_candles"}
)
_ALLOWED_SOURCE_CANDLES_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_DEST_TABLES: frozenset[str] = frozenset(
    {"support_resistance", "support_resistance_weekly", "support_resistance_monthly"}
)
# Daily uses ``date_computed`` (ISO date the row was written), while the
# weekly/monthly mirrors use the period key (``week_start`` / ``month_start``).
_ALLOWED_DEST_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date_computed", "week_start", "month_start"}
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


def cluster_into_sr_levels(
    swing_points: list[dict],
    price_tolerance_pct: float = 1.5,
    min_touches: int = 2,
) -> list[dict]:
    """
    Group swing points whose prices are within price_tolerance_pct of each other.

    For each cluster with >= min_touches:
        - level_price = average price of all points in the cluster
        - level_type = 'support' if majority are swing lows, 'resistance' if swing highs
        - touch_count = number of points in cluster
        - first_touch = earliest date, last_touch = latest date
        - strength = 'weak' if 2, 'moderate' if 3, 'strong' if 4+

    Uses a greedy clustering approach: sort by price, then greedily assign each
    point to an existing cluster if within tolerance, else start a new cluster.

    Args:
        swing_points: List of swing point dicts (date, type, price, strength).
        price_tolerance_pct: Maximum percentage difference between prices to be
            considered the same level.
        min_touches: Minimum number of swing points required to form a level.

    Returns:
        List of S/R level dicts.
    """
    if not swing_points:
        return []

    sorted_pts = sorted(swing_points, key=lambda p: p["price"])
    clusters: list[list[dict]] = []

    for point in sorted_pts:
        added = False
        for cluster in clusters:
            cluster_avg = sum(p["price"] for p in cluster) / len(cluster)
            diff_pct = abs(point["price"] - cluster_avg) / cluster_avg * 100.0
            if diff_pct <= price_tolerance_pct:
                cluster.append(point)
                added = True
                break
        if not added:
            clusters.append([point])

    levels: list[dict] = []
    for cluster in clusters:
        if len(cluster) < min_touches:
            continue

        level_price = sum(p["price"] for p in cluster) / len(cluster)
        high_count = sum(1 for p in cluster if p["type"] == "high")
        low_count = len(cluster) - high_count
        level_type = "resistance" if high_count >= low_count else "support"

        dates = sorted(p["date"] for p in cluster)
        touch_count = len(cluster)
        strength = _classify_strength(touch_count)

        levels.append({
            "level_price": level_price,
            "level_type": level_type,
            "touch_count": touch_count,
            "first_touch": dates[0],
            "last_touch": dates[-1],
            "strength": strength,
            "broken": False,
            "broken_date": None,
        })

    return levels


def _classify_strength(touch_count: int) -> str:
    """Return 'weak', 'moderate', or 'strong' based on touch count."""
    if touch_count >= 4:
        return "strong"
    if touch_count == 3:
        return "moderate"
    return "weak"


def check_broken_levels(
    sr_levels: list[dict], ohlcv_df: pd.DataFrame
) -> list[dict]:
    """
    For each S/R level, check if the most recent close has broken through it.

    Support broken: most recent close < level_price
    Resistance broken: most recent close > level_price

    Args:
        sr_levels: List of S/R level dicts.
        ohlcv_df: DataFrame with columns: date, close. Must have at least one row.

    Returns:
        Updated sr_levels list with broken and broken_date fields set.
    """
    if ohlcv_df.empty:
        return sr_levels

    last_row = ohlcv_df.iloc[-1]
    last_close = float(last_row["close"])
    last_date = str(last_row["date"])

    updated = []
    for level in sr_levels:
        level = dict(level)
        if level["level_type"] == "support" and last_close < level["level_price"]:
            level["broken"] = True
            level["broken_date"] = last_date
        elif level["level_type"] == "resistance" and last_close > level["level_price"]:
            level["broken"] = True
            level["broken_date"] = last_date
        updated.append(level)

    return updated


def save_sr_levels_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    sr_levels: list[dict],
    *,
    dest_table: str = "support_resistance",
    date_column_name: str = "date_computed",
) -> int:
    """
    Delete existing S/R levels for this ticker and insert fresh ones.

    The destination table and its date-style column are validated against a
    whitelist before being interpolated into SQL.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        sr_levels: List of level dicts from cluster_into_sr_levels().
        dest_table: Destination S/R table. Must be one of
            ``support_resistance``, ``support_resistance_weekly`` or
            ``support_resistance_monthly``. Defaults to ``support_resistance``.
        date_column_name: Name of the period-key/date column in
            ``dest_table``. Daily uses ``date_computed``; the weekly and
            monthly mirrors use ``week_start`` / ``month_start`` respectively.

    Returns:
        Number of rows inserted.

    Raises:
        ValueError: If either identifier is not in the allow-list.
    """
    safe_dest_table = _validate_identifier(
        dest_table, _ALLOWED_DEST_TABLES, "dest_table"
    )
    safe_date_col = _validate_identifier(
        date_column_name, _ALLOWED_DEST_DATE_COLUMNS, "date_column_name"
    )

    now_utc = datetime.now(timezone.utc).date().isoformat()
    db_conn.execute(
        f"DELETE FROM {safe_dest_table} WHERE ticker = ?", (ticker,)
    )
    if not sr_levels:
        db_conn.commit()
        return 0

    rows = [
        (
            ticker,
            now_utc,
            level["level_price"],
            level["level_type"],
            level["touch_count"],
            level["first_touch"],
            level["last_touch"],
            level["strength"],
            1 if level.get("broken") else 0,
            level.get("broken_date"),
        )
        for level in sr_levels
    ]
    db_conn.executemany(
        f"""INSERT INTO {safe_dest_table}
            (ticker, {safe_date_col}, level_price, level_type, touch_count,
             first_touch, last_touch, strength, broken, broken_date)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_conn.commit()
    logger.info(
        "ticker=%s phase=support_resistance dest=%s saved=%d levels",
        ticker,
        safe_dest_table,
        len(rows),
    )
    return len(rows)


def detect_support_resistance_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    *,
    source_swing_table: str = "swing_points",
    source_swing_date_column: str = "date",
    source_candles_table: str = "ohlcv_daily",
    source_candles_date_column: str = "date",
    dest_table: str = "support_resistance",
    dest_date_column: str = "date_computed",
) -> int:
    """
    Load swing points from DB, cluster into S/R levels, check for broken levels,
    and save results.

    Defaults preserve the original daily behaviour: read swing points from
    ``swing_points`` (keyed by ``date``), read OHLCV from ``ohlcv_daily``
    (keyed by ``date``), and write levels to ``support_resistance`` keyed by
    ``date_computed``. To run against the weekly or monthly mirrors, override
    the keyword-only parameters.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict containing config["support_resistance"].
        source_swing_table: Swing-points source table. Must be one of
            ``swing_points``, ``swing_points_weekly`` or
            ``swing_points_monthly``.
        source_swing_date_column: Date-column name in ``source_swing_table``.
            Must be one of ``date``, ``week_start`` or ``month_start``.
        source_candles_table: OHLCV-like source for the broken-level check.
            Must be one of ``ohlcv_daily``, ``weekly_candles`` or
            ``monthly_candles``.
        source_candles_date_column: Date-column name in
            ``source_candles_table``. Must be one of ``date``, ``week_start``
            or ``month_start``.
        dest_table: Destination S/R table. Must be one of
            ``support_resistance``, ``support_resistance_weekly`` or
            ``support_resistance_monthly``.
        dest_date_column: Period/date column on ``dest_table``. Must be one of
            ``date_computed``, ``week_start`` or ``month_start``.

    Returns:
        Number of S/R levels detected and saved.

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    safe_swing_table = _validate_identifier(
        source_swing_table, _ALLOWED_SOURCE_SWING_TABLES, "source_swing_table"
    )
    safe_swing_date_col = _validate_identifier(
        source_swing_date_column,
        _ALLOWED_SOURCE_SWING_DATE_COLUMNS,
        "source_swing_date_column",
    )
    safe_candles_table = _validate_identifier(
        source_candles_table,
        _ALLOWED_SOURCE_CANDLES_TABLES,
        "source_candles_table",
    )
    safe_candles_date_col = _validate_identifier(
        source_candles_date_column,
        _ALLOWED_SOURCE_CANDLES_DATE_COLUMNS,
        "source_candles_date_column",
    )
    # Validate dest identifiers eagerly so misuse fails fast.
    _validate_identifier(dest_table, _ALLOWED_DEST_TABLES, "dest_table")
    _validate_identifier(
        dest_date_column, _ALLOWED_DEST_DATE_COLUMNS, "dest_date_column"
    )

    sr_cfg = config.get("support_resistance", {})
    tolerance = sr_cfg.get("price_tolerance_pct", 1.5)
    min_touches = sr_cfg.get("min_touches", 2)

    swing_rows = db_conn.execute(
        f"SELECT {safe_swing_date_col} AS date, type, price, strength "
        f"FROM {safe_swing_table} WHERE ticker = ? "
        f"ORDER BY {safe_swing_date_col} ASC",
        (ticker,),
    ).fetchall()

    if not swing_rows:
        logger.warning(
            "ticker=%s phase=support_resistance source=%s no swing points found",
            ticker,
            safe_swing_table,
        )
        return 0

    swing_points = [dict(r) for r in swing_rows]

    try:
        sr_levels = cluster_into_sr_levels(swing_points, tolerance, min_touches)
    except Exception as exc:
        logger.error("ticker=%s phase=support_resistance cluster_error=%s", ticker, exc)
        db_conn.execute(
            "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
            (ticker, "sr_error", str(exc), datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
        return 0

    ohlcv_rows = db_conn.execute(
        f"SELECT {safe_candles_date_col} AS date, close "
        f"FROM {safe_candles_table} WHERE ticker = ? "
        f"ORDER BY {safe_candles_date_col} ASC",
        (ticker,),
    ).fetchall()
    ohlcv_df = pd.DataFrame([dict(r) for r in ohlcv_rows]) if ohlcv_rows else pd.DataFrame()

    if not ohlcv_df.empty:
        sr_levels = check_broken_levels(sr_levels, ohlcv_df)

    return save_sr_levels_to_db(
        db_conn,
        ticker,
        sr_levels,
        dest_table=dest_table,
        date_column_name=dest_date_column,
    )
