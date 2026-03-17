"""
Market regime detection per ticker.

Classifies the current market regime for each ticker:
  - Trending: ADX > 25 (strong directional trend)
  - Ranging: ADX < 20 (no clear trend, sideways)
  - Volatile: ATR > 1.5x its 20-day average OR VIX > 25

Priority: Volatile > Trending > Ranging
If no condition is clearly met, defaults to "ranging".

The regime determines which adaptive weights are applied to the 9
scoring categories.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


def detect_regime(
    adx: Optional[float],
    atr: Optional[float],
    atr_sma_20: Optional[float],
    vix_close: Optional[float],
    config: dict,
) -> str:
    """
    Classify the current market regime for a ticker.

    Checks conditions in priority order: volatile > trending > ranging.
    Defaults to "ranging" when no condition is met or data is missing.

    Parameters:
        adx: Current ADX value, or None if unavailable.
        atr: Current ATR value, or None if unavailable.
        atr_sma_20: 20-day SMA of ATR, or None if unavailable.
        vix_close: Most recent VIX close, or None if unavailable.
        config: Full scorer config dict containing regime_detection thresholds.

    Returns:
        One of "trending", "ranging", or "volatile".
    """
    regime_cfg = config.get("regime_detection", {})
    vix_threshold: float = regime_cfg.get("vix_volatile_threshold", 25)
    atr_multiplier: float = regime_cfg.get("atr_volatile_multiplier", 1.5)
    adx_trending: float = regime_cfg.get("adx_trending_threshold", 25)
    adx_ranging: float = regime_cfg.get("adx_ranging_threshold", 20)

    # Volatile takes highest priority
    if vix_close is not None and vix_close > vix_threshold:
        logger.debug(f"Regime=volatile (VIX={vix_close:.1f} > {vix_threshold})")
        return "volatile"

    if atr is not None and atr_sma_20 is not None and atr_sma_20 > 0:
        if atr > atr_sma_20 * atr_multiplier:
            logger.debug(f"Regime=volatile (ATR={atr:.4f} > {atr_multiplier}x SMA={atr_sma_20:.4f})")
            return "volatile"

    # Trending second
    if adx is not None and adx > adx_trending:
        logger.debug(f"Regime=trending (ADX={adx:.1f} > {adx_trending})")
        return "trending"

    # Ranging third
    if adx is not None and adx < adx_ranging:
        logger.debug(f"Regime=ranging (ADX={adx:.1f} < {adx_ranging})")
        return "ranging"

    # Default
    logger.debug("Regime=ranging (default — no condition clearly met)")
    return "ranging"


def get_regime_weights(regime: str, config: dict) -> dict:
    """
    Return the adaptive weight dict for the given market regime.

    Reads weights from config['adaptive_weights'][regime]. Logs a warning if
    the weights do not sum to 1.0 (accounting for floating-point tolerance).

    Parameters:
        regime: One of "trending", "ranging", "volatile".
        config: Full scorer config dict.

    Returns:
        Dict mapping category names to float weights.
    """
    adaptive_weights: dict = config.get("adaptive_weights", {})
    weights: dict = adaptive_weights.get(regime, {})

    if not weights:
        logger.warning(f"No adaptive weights found for regime={regime!r}. Returning empty dict.")
        return {}

    total = sum(weights.values())
    if abs(total - 1.0) > 1e-6:
        logger.warning(f"Regime={regime!r} weights sum to {total:.6f}, expected 1.0")

    return weights


def get_current_vix(db_conn: sqlite3.Connection) -> Optional[float]:
    """
    Query the most recent VIX close from ohlcv_daily.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.

    Returns:
        The most recent VIX close value, or None if not available.
    """
    row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        ("^VIX",),
    ).fetchone()
    if row is None:
        logger.debug("VIX data not found in ohlcv_daily")
        return None
    return float(row["close"])


def get_atr_sma(
    db_conn: sqlite3.Connection,
    ticker: str,
    lookback: int = 20,
) -> Optional[float]:
    """
    Compute the simple moving average of ATR over the last `lookback` trading days.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        lookback: Number of trading days to average (default 20).

    Returns:
        The SMA of ATR values, or None if not enough data.
    """
    rows = db_conn.execute(
        "SELECT atr_14 FROM indicators_daily WHERE ticker = ? AND atr_14 IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, lookback),
    ).fetchall()

    if len(rows) < lookback:
        logger.debug(f"{ticker}: only {len(rows)} ATR values, need {lookback} for SMA")
        return None

    atr_values = [float(row["atr_14"]) for row in rows]
    return sum(atr_values) / len(atr_values)
