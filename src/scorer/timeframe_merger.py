"""
Dual timeframe confirmation.

Merges daily and weekly scores using configurable weights:
  Final Score = (Daily Score × 0.2) + (Weekly Score × 0.8)

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
    regime: str = "ranging",
) -> float:
    """
    Merge daily and weekly composite scores into a final score.

    Uses regime-adaptive weights from config['timeframe_weights']. The config
    supports two formats:
      - Nested (regime-specific): {"trending": {"daily": 0.2, "weekly": 0.8}, ...}
      - Flat (backward-compatible): {"daily": 0.2, "weekly": 0.8}

    Falls back to 100% daily score if weekly is unavailable.
    Result is clamped to [-100, +100].

    Parameters:
        daily_score: Daily composite score (-100 to +100).
        weekly_score: Weekly composite score (-100 to +100), or None if unavailable.
        config: Scorer config dict containing timeframe_weights.
        regime: Market regime — "trending", "ranging", or "volatile".

    Returns:
        Float merged score clamped to [-100, +100].
    """
    if weekly_score is None:
        logger.debug("Weekly score not available — using daily score only")
        return max(-100.0, min(100.0, daily_score))

    tf_weights = config.get("timeframe_weights", {})

    # Support nested (regime-specific) or flat format
    if regime in tf_weights and isinstance(tf_weights[regime], dict):
        weights = tf_weights[regime]
    elif "daily" in tf_weights and isinstance(tf_weights["daily"], (int, float)):
        weights = tf_weights
    else:
        weights = tf_weights.get("ranging", {"daily": 0.5, "weekly": 0.5})

    daily_weight: float = weights.get("daily", 0.2)
    weekly_weight: float = weights.get("weekly", 0.8)

    merged = daily_score * daily_weight + weekly_score * weekly_weight
    return max(-100.0, min(100.0, merged))


def compute_weekly_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[float]:
    """
    Compute a composite score from weekly indicator data.

    Loads the most recent weekly indicators from indicators_weekly with a
    week_start on or before scoring_date (no look-ahead), then scores all
    available indicators using the same primitives as daily scoring:
    score_all_indicators for individual scores, rollup_category for the 4
    indicator-based categories (trend, momentum, volume, volatility), and
    apply_adaptive_weights with a weekly-specific weight set and expansion
    factor.

    Does not include patterns, divergences, crossovers, news, fundamentals,
    or macro — those have no weekly equivalents.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        config: Scorer config dict containing weekly_adaptive_weights and
            scoring.score_expansion_factor.
        scoring_date: The date being scored (YYYY-MM-DD). Only weekly candles
            with week_start <= scoring_date are considered.
        regime: Market regime — "trending", "ranging", or "volatile".
            Defaults to "ranging".

    Returns:
        Float composite score (-100 to +100), or None if no weekly data.
    """
    # Import here to avoid circular dependencies at module level
    from src.scorer.indicator_scorer import (
        load_profile_for_ticker,
        score_all_indicators,
    )
    from src.scorer.category_scorer import apply_adaptive_weights, rollup_category

    row = db_conn.execute(
        "SELECT w.close, i.* "
        "FROM indicators_weekly i "
        "JOIN weekly_candles w ON i.ticker = w.ticker AND i.week_start = w.week_start "
        "WHERE i.ticker = ? AND i.week_start <= ? "
        "ORDER BY i.week_start DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()

    if row is None:
        logger.debug(f"{ticker}: no weekly indicator data found")
        return None

    close = row["close"]
    if close is None:
        logger.debug(f"{ticker}: weekly candle has no close price")
        return None

    indicators = dict(row)

    # Load daily profiles as fallback (reasonable proxy for weekly distributions)
    profiles = load_profile_for_ticker(db_conn, ticker)

    # Score all 14 indicators using the same function as daily
    indicator_scores = score_all_indicators(
        indicators=indicators,
        close=close,
        profiles=profiles,
        config=config,
        regime=regime,
    )

    # Build 4 weekly-applicable category scores (no patterns/sentiment/fundamental/macro)
    trend_score = rollup_category("weekly_trend", {
        "ema_alignment": indicator_scores.get("ema_alignment"),
        "macd_line": indicator_scores.get("macd_line"),
        "macd_histogram": indicator_scores.get("macd_histogram"),
        "adx": indicator_scores.get("adx"),
    })
    momentum_score = rollup_category("weekly_momentum", {
        "rsi_14": indicator_scores.get("rsi_14"),
        "stoch_k": indicator_scores.get("stoch_k"),
        "cci_20": indicator_scores.get("cci_20"),
        "williams_r": indicator_scores.get("williams_r"),
    })
    volume_score = rollup_category("weekly_volume", {
        "obv": indicator_scores.get("obv"),
        "cmf_20": indicator_scores.get("cmf_20"),
        "ad_line": indicator_scores.get("ad_line"),
    })
    volatility_score = rollup_category("weekly_volatility", {
        "bb_pctb": indicator_scores.get("bb_pctb"),
        "atr_14": indicator_scores.get("atr_14"),
    })

    category_scores = {
        "trend": trend_score,
        "momentum": momentum_score,
        "volume": volume_score,
        "volatility": volatility_score,
    }

    # Check that at least one category produced a non-zero score
    if all(score == 0.0 for score in category_scores.values()):
        logger.debug(f"{ticker}: no usable weekly indicators")
        return None

    # Apply weekly adaptive weights (4 categories summing to 1.0)
    weekly_weights_cfg = config.get("weekly_adaptive_weights", {})
    regime_weights = weekly_weights_cfg.get(regime, {})

    # Fallback: re-normalize daily adaptive weights to the 4 applicable categories
    if not regime_weights:
        daily_weights_cfg = config.get("adaptive_weights", {})
        daily_regime = daily_weights_cfg.get(regime, {})
        applicable = {
            key: daily_regime.get(key, 0.0)
            for key in ("trend", "momentum", "volume", "volatility")
        }
        total = sum(applicable.values())
        if total > 0:
            regime_weights = {key: val / total for key, val in applicable.items()}
        else:
            regime_weights = {"trend": 0.25, "momentum": 0.25, "volume": 0.25, "volatility": 0.25}

    expansion_factor = config.get("scoring", {}).get("score_expansion_factor", 1.0)

    return apply_adaptive_weights(category_scores, regime_weights, expansion_factor)
