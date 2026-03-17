"""
Sector adjustment.

Computes a simple trend score for the ticker's sector ETF and adjusts
the ticker's raw score based on whether the sector is bullish or bearish.

If the sector is bullish (ETF score > +30): add +5 to +10 to the ticker's score
If the sector is bearish (ETF score < -30): subtract 5 to 10
If neutral: no adjustment
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


def compute_sector_etf_score(db_conn: sqlite3.Connection, sector_etf: str) -> Optional[float]:
    """
    Compute a simple trend score for the given sector ETF.

    Uses the most recent indicators_daily row for the ETF and combines:
      - EMA alignment (close vs ema_9, ema_21, ema_50)
      - MACD direction (positive/negative histogram)
      - RSI level (above/below 50)

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        sector_etf: Ticker symbol for the sector ETF (e.g. "XLK").

    Returns:
        Float score between -100 and +100, or None if no data.
    """
    row = db_conn.execute(
        "SELECT i.ema_9, i.ema_21, i.ema_50, i.macd_histogram, i.rsi_14, o.close "
        "FROM indicators_daily i "
        "JOIN ohlcv_daily o ON i.ticker = o.ticker AND i.date = o.date "
        "WHERE i.ticker = ? "
        "ORDER BY i.date DESC LIMIT 1",
        (sector_etf,),
    ).fetchone()

    if row is None:
        logger.debug(f"{sector_etf}: no indicator data found for sector ETF")
        return None

    close = row["close"]
    ema_9 = row["ema_9"]
    ema_21 = row["ema_21"]
    ema_50 = row["ema_50"]
    macd_hist = row["macd_histogram"]
    rsi = row["rsi_14"]

    component_scores: list[float] = []

    # EMA alignment score
    if all(v is not None for v in (close, ema_9, ema_21, ema_50)):
        conditions_bullish = sum([
            close > ema_9,
            ema_9 > ema_21,
            ema_21 > ema_50,
        ])
        conditions_bearish = sum([
            close < ema_9,
            ema_9 < ema_21,
            ema_21 < ema_50,
        ])
        ema_score = (conditions_bullish - conditions_bearish) / 3 * 100
        component_scores.append(float(ema_score))

    # MACD direction
    if macd_hist is not None:
        macd_score = 50.0 if macd_hist > 0 else -50.0
        component_scores.append(macd_score)

    # RSI level (above 50 = bullish bias, below 50 = bearish bias)
    if rsi is not None:
        rsi_score = (rsi - 50.0) * 2.0  # maps 0→-100, 50→0, 100→+100
        component_scores.append(max(-100.0, min(100.0, rsi_score)))

    if not component_scores:
        return None

    avg = sum(component_scores) / len(component_scores)
    return max(-100.0, min(100.0, avg))


def apply_sector_adjustment(
    raw_score: float,
    sector_etf_score: Optional[float],
    config: dict,
) -> float:
    """
    Adjust a ticker's raw score based on sector trend direction.

    If sector is bullish (ETF score > bullish_threshold): add +5 to +max_adjustment.
    If sector is bearish (ETF score < bearish_threshold): subtract 5 to max_adjustment.
    If neutral or no data: no adjustment.

    Adjustment is linearly interpolated based on how far the ETF score is from the
    threshold. The result is clamped to [-100, +100].

    Parameters:
        raw_score: The ticker's pre-adjustment composite score (-100 to +100).
        sector_etf_score: Sector ETF trend score, or None if unavailable.
        config: Full scorer config dict containing sector_adjustment sub-dict.

    Returns:
        Float adjusted score clamped to [-100, +100].
    """
    if sector_etf_score is None:
        return raw_score

    adj_cfg = config.get("sector_adjustment", {})
    bullish_threshold: float = adj_cfg.get("bullish_sector_threshold", 30)
    bearish_threshold: float = adj_cfg.get("bearish_sector_threshold", -30)
    max_adjustment: float = adj_cfg.get("max_adjustment", 10)

    adjustment = 0.0

    if sector_etf_score > bullish_threshold:
        # How far above the threshold (0 to ~70 for etf_score up to 100)
        headroom = 100.0 - bullish_threshold
        t = min(1.0, (sector_etf_score - bullish_threshold) / headroom)
        adjustment = 5.0 + t * (max_adjustment - 5.0)

    elif sector_etf_score < bearish_threshold:
        headroom = abs(bearish_threshold) + 100.0 - abs(bearish_threshold)
        headroom = abs(bearish_threshold - (-100.0))
        t = min(1.0, (bearish_threshold - sector_etf_score) / headroom)
        adjustment = -(5.0 + t * (max_adjustment - 5.0))

    return max(-100.0, min(100.0, raw_score + adjustment))
