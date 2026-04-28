"""
Divergence detection between price and indicators.

Compares swing point prices with indicator values at those same swing points.
When price and an indicator move in opposite directions, it signals a divergence.

Types:
  Regular Bullish:  price lower low  + indicator higher low  → reversal up
  Regular Bearish:  price higher high + indicator lower high  → reversal down
  Hidden Bullish:   price higher low  + indicator lower low  → continuation up
  Hidden Bearish:   price lower high  + indicator higher high → continuation down

Applied to: RSI (stored as ``rsi_14``), MACD histogram, OBV, Stochastic %K.

The indicator name persisted in the ``indicator`` column matches the indicator
column name in ``indicators_*`` (e.g. ``rsi_14``) so downstream filters in
the scorer can use the same key. Historical note: prior to this commit RSI
divergences were stored as ``"rsi"`` while the scorer filtered for
``"rsi_14"``, silently zero-ing the daily RSI divergence score.

Timeframe parametrization
-------------------------
``save_divergences_to_db`` and ``detect_divergences_for_ticker`` accept
keyword-only arguments selecting the source swing table, source indicators
table and destination divergences table + date-column name. Defaults preserve
the original daily behaviour. Identifiers are validated against an explicit
whitelist before being interpolated into SQL.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


# ── Whitelists ──────────────────────────────────────────────────────────────────
_ALLOWED_SOURCE_SWING_TABLES: frozenset[str] = frozenset(
    {"swing_points", "swing_points_weekly", "swing_points_monthly"}
)
_ALLOWED_SOURCE_SWING_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_SOURCE_INDICATORS_TABLES: frozenset[str] = frozenset(
    {"indicators_daily", "indicators_weekly", "indicators_monthly"}
)
_ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_DEST_TABLES: frozenset[str] = frozenset(
    {"divergences_daily", "divergences_weekly", "divergences_monthly"}
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


def get_indicator_value_at_date(
    indicators_df: pd.DataFrame, target_date: str, indicator_column: str
) -> float | None:
    """
    Look up the indicator value at the given date.

    Args:
        indicators_df: DataFrame with a 'date' column and indicator columns.
        target_date: ISO date string to look up.
        indicator_column: Name of the column to read.

    Returns:
        Float value, or None if the date is not found or the value is NaN.
    """
    if indicator_column not in indicators_df.columns:
        return None
    matches = indicators_df[indicators_df["date"] == target_date]
    if matches.empty:
        return None
    value = matches.iloc[0][indicator_column]
    if pd.isna(value):
        return None
    return float(value)


def detect_divergences_for_indicator(
    swing_points: list[dict],
    indicators_df: pd.DataFrame,
    indicator_column: str,
    indicator_name: str,
    config: dict,
) -> list[dict]:
    """
    Detect all four divergence types for a single indicator.

    Examines consecutive pairs of swing lows (for bullish/bearish lows) and
    consecutive pairs of swing highs (for bearish/hidden bearish) that fall within
    the configured min/max day distance.

    Args:
        swing_points: List of swing point dicts (date, type, price, strength).
        indicators_df: DataFrame with date and indicator columns.
        indicator_column: Column name in indicators_df to compare (e.g. 'rsi_14').
        indicator_name: Human-readable name stored in the result (e.g. 'rsi').
        config: Calculator config dict containing config["divergences"].

    Returns:
        List of divergence dicts with keys: date, divergence_type, indicator,
        price_swing_1_date, price_swing_1_value, price_swing_2_date,
        price_swing_2_value, indicator_swing_1_value, indicator_swing_2_value,
        strength.
    """
    div_cfg = config.get("divergences", {})
    min_days = div_cfg.get("min_swing_distance_days", 5)
    max_days = div_cfg.get("max_swing_distance_days", 60)

    lows = sorted([p for p in swing_points if p["type"] == "low"], key=lambda p: p["date"])
    highs = sorted([p for p in swing_points if p["type"] == "high"], key=lambda p: p["date"])

    divergences: list[dict] = []

    # ── Check consecutive swing low pairs ────────────────────────────────────
    for i in range(len(lows) - 1):
        sw1, sw2 = lows[i], lows[i + 1]
        distance_days = (
            date.fromisoformat(sw2["date"]) - date.fromisoformat(sw1["date"])
        ).days

        if distance_days < min_days or distance_days > max_days:
            continue

        ind1 = get_indicator_value_at_date(indicators_df, sw1["date"], indicator_column)
        ind2 = get_indicator_value_at_date(indicators_df, sw2["date"], indicator_column)
        if ind1 is None or ind2 is None:
            continue

        price1, price2 = sw1["price"], sw2["price"]
        div_type: str | None = None

        if price2 < price1 and ind2 > ind1:
            div_type = "regular_bullish"
        elif price2 > price1 and ind2 < ind1:
            div_type = "hidden_bullish"

        if div_type:
            strength = _compute_divergence_strength(price1, price2, ind1, ind2)
            divergences.append({
                "date": sw2["date"],
                "divergence_type": div_type,
                "indicator": indicator_name,
                "price_swing_1_date": sw1["date"],
                "price_swing_1_value": price1,
                "price_swing_2_date": sw2["date"],
                "price_swing_2_value": price2,
                "indicator_swing_1_value": ind1,
                "indicator_swing_2_value": ind2,
                "strength": strength,
            })

    # ── Check consecutive swing high pairs ───────────────────────────────────
    for i in range(len(highs) - 1):
        sw1, sw2 = highs[i], highs[i + 1]
        distance_days = (
            date.fromisoformat(sw2["date"]) - date.fromisoformat(sw1["date"])
        ).days

        if distance_days < min_days or distance_days > max_days:
            continue

        ind1 = get_indicator_value_at_date(indicators_df, sw1["date"], indicator_column)
        ind2 = get_indicator_value_at_date(indicators_df, sw2["date"], indicator_column)
        if ind1 is None or ind2 is None:
            continue

        price1, price2 = sw1["price"], sw2["price"]
        div_type = None

        if price2 > price1 and ind2 < ind1:
            div_type = "regular_bearish"
        elif price2 < price1 and ind2 > ind1:
            div_type = "hidden_bearish"

        if div_type:
            strength = _compute_divergence_strength(price1, price2, ind1, ind2)
            divergences.append({
                "date": sw2["date"],
                "divergence_type": div_type,
                "indicator": indicator_name,
                "price_swing_1_date": sw1["date"],
                "price_swing_1_value": price1,
                "price_swing_2_date": sw2["date"],
                "price_swing_2_value": price2,
                "indicator_swing_1_value": ind1,
                "indicator_swing_2_value": ind2,
                "strength": strength,
            })

    return divergences


def _compute_divergence_strength(
    price1: float, price2: float, ind1: float, ind2: float
) -> int:
    """
    Compute divergence strength 1–5 based on combined price and indicator gap magnitude.

    Larger price divergence and larger indicator divergence → higher strength.

    Args:
        price1: Price at first swing point.
        price2: Price at second swing point.
        ind1: Indicator value at first swing point.
        ind2: Indicator value at second swing point.

    Returns:
        Integer 1–5.
    """
    if price1 == 0:
        return 1
    price_gap_pct = abs(price2 - price1) / price1 * 100.0
    ind_gap_pct = abs(ind2 - ind1) / (abs(ind1) + 1e-9) * 100.0
    combined = (price_gap_pct + ind_gap_pct) / 2.0

    if combined >= 20:
        return 5
    if combined >= 12:
        return 4
    if combined >= 6:
        return 3
    if combined >= 2:
        return 2
    return 1


def detect_all_divergences(
    swing_points: list[dict], indicators_df: pd.DataFrame, config: dict
) -> list[dict]:
    """
    Run divergence detection for all four configured indicators.

    Indicators checked:
        - RSI (column: rsi_14, name: 'rsi_14')
        - MACD histogram (column: macd_histogram, name: 'macd_histogram')
        - OBV (column: obv, name: 'obv')
        - Stochastic %K (column: stoch_k, name: 'stochastic')

    Note: The RSI indicator is persisted as ``"rsi_14"`` (not ``"rsi"``) so
    that the stored ``indicator`` column matches the column name in the
    indicators tables. The scorer filters divergences by ``"rsi_14"`` —
    storing ``"rsi"`` here previously caused daily RSI divergence scores to
    be silently zero.

    Args:
        swing_points: List of swing point dicts.
        indicators_df: DataFrame with indicator columns.
        config: Calculator config dict.

    Returns:
        Combined list of all divergences across all indicators.
    """
    indicator_map = [
        ("rsi_14", "rsi_14"),
        ("macd_histogram", "macd_histogram"),
        ("obv", "obv"),
        ("stoch_k", "stochastic"),
    ]

    all_divergences: list[dict] = []
    for column, name in indicator_map:
        divergences = detect_divergences_for_indicator(
            swing_points, indicators_df, column, name, config
        )
        all_divergences.extend(divergences)

    return all_divergences


def save_divergences_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    divergences: list[dict],
    *,
    dest_table: str = "divergences_daily",
    date_column_name: str = "date",
) -> int:
    """
    Delete existing divergences for this ticker and insert fresh ones.

    The destination table and its date-column are validated against a
    whitelist before being interpolated into SQL.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        divergences: List of divergence dicts from detect_all_divergences().
        dest_table: Destination divergences table. Must be one of
            ``divergences_daily``, ``divergences_weekly`` or
            ``divergences_monthly``. Defaults to ``divergences_daily``.
        date_column_name: Name of the date / period-key column in
            ``dest_table``. Must be one of ``date``, ``week_start`` or
            ``month_start``. Defaults to ``date`` (daily).

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

    db_conn.execute(f"DELETE FROM {safe_dest_table} WHERE ticker = ?", (ticker,))
    if not divergences:
        db_conn.commit()
        return 0

    rows = [
        (
            ticker,
            div["date"],
            div["indicator"],
            div["divergence_type"],
            div["price_swing_1_date"],
            div["price_swing_1_value"],
            div["price_swing_2_date"],
            div["price_swing_2_value"],
            div["indicator_swing_1_value"],
            div["indicator_swing_2_value"],
            div["strength"],
        )
        for div in divergences
    ]
    db_conn.executemany(
        f"""INSERT INTO {safe_dest_table}
           (ticker, {safe_date_col}, indicator, divergence_type,
            price_swing_1_date, price_swing_1_value,
            price_swing_2_date, price_swing_2_value,
            indicator_swing_1_value, indicator_swing_2_value, strength)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_conn.commit()
    logger.info(
        "ticker=%s phase=divergences dest=%s saved=%d divergences",
        ticker,
        safe_dest_table,
        len(rows),
    )
    return len(rows)


def detect_divergences_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    *,
    source_swing_table: str = "swing_points",
    source_swing_date_column: str = "date",
    source_indicators_table: str = "indicators_daily",
    source_indicators_date_column: str = "date",
    dest_table: str = "divergences_daily",
    dest_date_column: str = "date",
) -> int:
    """
    Load swing points and indicators from DB, detect all divergences, save to DB.

    Defaults preserve the original daily behaviour: read swing points from
    ``swing_points`` (keyed by ``date``), read indicators from
    ``indicators_daily`` (keyed by ``date``), and write divergences to
    ``divergences_daily`` keyed by ``date``. To run against the weekly or
    monthly mirrors, override the keyword-only parameters.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
        source_swing_table: Swing-points source table. Must be one of
            ``swing_points``, ``swing_points_weekly`` or
            ``swing_points_monthly``.
        source_swing_date_column: Date-column name in ``source_swing_table``.
            Must be one of ``date``, ``week_start`` or ``month_start``.
        source_indicators_table: Indicators source table. Must be one of
            ``indicators_daily``, ``indicators_weekly`` or
            ``indicators_monthly``.
        source_indicators_date_column: Date-column name in
            ``source_indicators_table``.
        dest_table: Destination divergences table. Must be one of
            ``divergences_daily``, ``divergences_weekly`` or
            ``divergences_monthly``.
        dest_date_column: Date-column name in ``dest_table``.

    Returns:
        Number of divergences detected and saved.

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
    safe_indicators_table = _validate_identifier(
        source_indicators_table,
        _ALLOWED_SOURCE_INDICATORS_TABLES,
        "source_indicators_table",
    )
    safe_indicators_date_col = _validate_identifier(
        source_indicators_date_column,
        _ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS,
        "source_indicators_date_column",
    )
    # Validate dest identifiers eagerly so misuse fails fast.
    _validate_identifier(dest_table, _ALLOWED_DEST_TABLES, "dest_table")
    _validate_identifier(
        dest_date_column, _ALLOWED_DEST_DATE_COLUMNS, "dest_date_column"
    )

    swing_rows = db_conn.execute(
        f"SELECT {safe_swing_date_col} AS date, type, price, strength "
        f"FROM {safe_swing_table} WHERE ticker = ? "
        f"ORDER BY {safe_swing_date_col} ASC",
        (ticker,),
    ).fetchall()

    if not swing_rows:
        logger.warning(
            "ticker=%s phase=divergences source=%s no swing points found",
            ticker,
            safe_swing_table,
        )
        return 0

    swing_points = [dict(r) for r in swing_rows]

    ind_rows = db_conn.execute(
        f"SELECT {safe_indicators_date_col} AS date, rsi_14, macd_histogram, "
        f"macd_line, macd_signal, obv, stoch_k "
        f"FROM {safe_indicators_table} WHERE ticker = ? "
        f"ORDER BY {safe_indicators_date_col} ASC",
        (ticker,),
    ).fetchall()

    if not ind_rows:
        logger.warning(
            "ticker=%s phase=divergences source=%s no indicator data found",
            ticker,
            safe_indicators_table,
        )
        return 0

    indicators_df = pd.DataFrame([dict(r) for r in ind_rows])

    try:
        divergences = detect_all_divergences(swing_points, indicators_df, config)
    except Exception as exc:
        logger.error("ticker=%s phase=divergences error=%s", ticker, exc)
        db_conn.execute(
            "INSERT INTO alerts_log (ticker, alert_type, message, created_at) VALUES (?,?,?,?)",
            (ticker, "divergences_error", str(exc), datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
        return 0

    return save_divergences_to_db(
        db_conn,
        ticker,
        divergences,
        dest_table=dest_table,
        date_column_name=dest_date_column,
    )
