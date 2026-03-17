"""
Divergence detection between price and indicators.

Compares swing point prices with indicator values at those same swing points.
When price and an indicator move in opposite directions, it signals a divergence.

Types:
  Regular Bullish:  price lower low  + indicator higher low  → reversal up
  Regular Bearish:  price higher high + indicator lower high  → reversal down
  Hidden Bullish:   price higher low  + indicator lower low  → continuation up
  Hidden Bearish:   price lower high  + indicator higher high → continuation down

Applied to: RSI, MACD histogram, OBV, Stochastic %K
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


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
        - RSI (column: rsi_14, name: 'rsi')
        - MACD histogram (column: macd_histogram, name: 'macd_histogram')
        - OBV (column: obv, name: 'obv')
        - Stochastic %K (column: stoch_k, name: 'stochastic')

    Args:
        swing_points: List of swing point dicts.
        indicators_df: DataFrame with indicator columns.
        config: Calculator config dict.

    Returns:
        Combined list of all divergences across all indicators.
    """
    indicator_map = [
        ("rsi_14", "rsi"),
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
    db_conn: sqlite3.Connection, ticker: str, divergences: list[dict]
) -> int:
    """
    Delete existing divergences for this ticker and insert fresh ones.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        divergences: List of divergence dicts from detect_all_divergences().

    Returns:
        Number of rows inserted.
    """
    db_conn.execute("DELETE FROM divergences_daily WHERE ticker = ?", (ticker,))
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
        """INSERT INTO divergences_daily
           (ticker, date, indicator, divergence_type,
            price_swing_1_date, price_swing_1_value,
            price_swing_2_date, price_swing_2_value,
            indicator_swing_1_value, indicator_swing_2_value, strength)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db_conn.commit()
    logger.info("ticker=%s phase=divergences saved=%d divergences", ticker, len(rows))
    return len(rows)


def detect_divergences_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, config: dict
) -> int:
    """
    Load swing points and indicators from DB, detect all divergences, save to DB.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.

    Returns:
        Number of divergences detected and saved.
    """
    swing_rows = db_conn.execute(
        "SELECT date, type, price, strength FROM swing_points WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not swing_rows:
        logger.warning("ticker=%s phase=divergences no swing points found", ticker)
        return 0

    swing_points = [dict(r) for r in swing_rows]

    ind_rows = db_conn.execute(
        "SELECT date, rsi_14, macd_histogram, macd_line, macd_signal, obv, stoch_k "
        "FROM indicators_daily WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not ind_rows:
        logger.warning("ticker=%s phase=divergences no indicator data found", ticker)
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

    return save_divergences_to_db(db_conn, ticker, divergences)
