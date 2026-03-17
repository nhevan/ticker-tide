"""
Dual timeframe confirmation.

Merges daily and weekly scores using configurable weights:
  Final Score = (Daily Score × 0.6) + (Weekly Score × 0.4)

If weekly score is not available, falls back to daily score only.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


def merge_timeframes(
    daily_score: float,
    weekly_score: Optional[float],
    config: dict,
) -> float:
    """
    Merge daily and weekly composite scores into a final score.

    Uses configurable weights from config['timeframe_weights'].
    Falls back to 100% daily score if weekly is unavailable.
    Result is clamped to [-100, +100].

    Parameters:
        daily_score: Daily composite score (-100 to +100).
        weekly_score: Weekly composite score (-100 to +100), or None if unavailable.
        config: Scorer config dict containing timeframe_weights.

    Returns:
        Float merged score clamped to [-100, +100].
    """
    if weekly_score is None:
        logger.debug("Weekly score not available — using daily score only")
        return max(-100.0, min(100.0, daily_score))

    weights = config.get("timeframe_weights", {})
    daily_weight: float = weights.get("daily", 0.6)
    weekly_weight: float = weights.get("weekly", 0.4)

    merged = daily_score * daily_weight + weekly_score * weekly_weight
    return max(-100.0, min(100.0, merged))


def compute_weekly_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
) -> Optional[float]:
    """
    Compute a simplified composite score from weekly indicator data.

    Loads the most recent weekly indicators from indicators_weekly and runs a
    simplified scoring pipeline: EMA alignment, MACD histogram, RSI, ADX,
    CMF, and Bollinger %B. Does not include patterns, divergences, or news.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        config: Scorer config dict.

    Returns:
        Float composite score (-100 to +100), or None if no weekly data.
    """
    # Import here to avoid circular dependencies at module level
    from src.scorer.indicator_scorer import (
        score_adx,
        score_ema_alignment,
        score_macd_histogram,
        score_rsi,
    )
    from src.scorer.category_scorer import apply_adaptive_weights, rollup_category
    from src.scorer.regime import get_regime_weights

    row = db_conn.execute(
        "SELECT w.close, i.ema_9, i.ema_21, i.ema_50, i.macd_histogram, "
        "       i.rsi_14, i.adx, i.cmf_20, i.bb_pctb, i.atr_14 "
        "FROM indicators_weekly i "
        "JOIN weekly_candles w ON i.ticker = w.ticker AND i.week_start = w.week_start "
        "WHERE i.ticker = ? "
        "ORDER BY i.week_start DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if row is None:
        logger.debug(f"{ticker}: no weekly indicator data found")
        return None

    close = row["close"]
    ema_9 = row["ema_9"]
    ema_21 = row["ema_21"]
    ema_50 = row["ema_50"]
    macd_hist = row["macd_histogram"]
    rsi = row["rsi_14"]
    adx = row["adx"]
    cmf = row["cmf_20"]
    bb_pctb = row["bb_pctb"]

    component_scores: list[float] = []

    # EMA alignment
    if all(v is not None for v in (close, ema_9, ema_21, ema_50)):
        component_scores.append(score_ema_alignment(close, ema_9, ema_21, ema_50))

    # MACD histogram
    if macd_hist is not None:
        component_scores.append(score_macd_histogram(macd_hist, profile=None))

    # RSI
    if rsi is not None:
        component_scores.append(score_rsi(rsi, profile=None))

    # ADX
    if adx is not None:
        component_scores.append(score_adx(adx))

    # CMF
    if cmf is not None:
        cmf_score = max(-100.0, min(100.0, cmf * 200.0))
        component_scores.append(cmf_score)

    # BB %B
    if bb_pctb is not None:
        bb_score = max(-100.0, min(100.0, (0.5 - bb_pctb) * 100.0))
        component_scores.append(bb_score)

    if not component_scores:
        logger.debug(f"{ticker}: no usable weekly indicators")
        return None

    avg = sum(component_scores) / len(component_scores)
    return max(-100.0, min(100.0, avg))
