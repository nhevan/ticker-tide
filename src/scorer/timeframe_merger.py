"""
Triple timeframe confirmation.

Merges daily, weekly, and monthly scores using configurable weights:
  Final Score = (Daily × w_d) + (Weekly × w_w) + (Monthly × w_m)

Weights are regime-adaptive. If a timeframe is unavailable, its weight
is redistributed proportionally across the remaining timeframes.
Falls back to 100% daily score if only daily is available.
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
    monthly_score: Optional[float] = None,
) -> float:
    """
    Merge daily, weekly, and optional monthly scores into a final score.

    Uses regime-adaptive weights from config['timeframe_weights']. Each regime
    entry may contain 'daily', 'weekly', and optionally 'monthly' keys.

    When a timeframe score is None its weight is redistributed proportionally
    across the remaining available timeframes. Falls back to 100% daily if both
    weekly and monthly are unavailable.

    Result is clamped to [-100, +100].

    Parameters:
        daily_score:   Daily composite score (-100 to +100).
        weekly_score:  Weekly composite score (-100 to +100), or None.
        config:        Scorer config dict containing timeframe_weights.
        regime:        Market regime — "trending", "ranging", or "volatile".
        monthly_score: Monthly composite score (-100 to +100), or None.

    Returns:
        Float merged score clamped to [-100, +100].
    """
    if weekly_score is None and monthly_score is None:
        logger.debug("Weekly and monthly scores not available — using daily score only")
        return max(-100.0, min(100.0, daily_score))

    tf_weights = config.get("timeframe_weights", {})

    # Resolve regime-specific or flat weight dict
    if regime in tf_weights and isinstance(tf_weights[regime], dict):
        weights = tf_weights[regime]
    elif "daily" in tf_weights and isinstance(tf_weights["daily"], (int, float)):
        weights = tf_weights
    else:
        weights = tf_weights.get("ranging", {"daily": 0.5, "weekly": 0.5})

    daily_w: float = weights.get("daily", 0.5)
    weekly_w: float = weights.get("weekly", 0.5)
    monthly_w: float = weights.get("monthly", 0.0)

    # Zero out unavailable timeframes and renormalize
    scores: dict[str, tuple[float, float]] = {"daily": (daily_score, daily_w)}
    if weekly_score is not None:
        scores["weekly"] = (weekly_score, weekly_w)
    if monthly_score is not None:
        scores["monthly"] = (monthly_score, monthly_w)

    total_w = sum(w for _, w in scores.values())
    if total_w <= 0:
        return max(-100.0, min(100.0, daily_score))

    merged = sum(score * (w / total_w) for score, w in scores.values())
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


def compute_monthly_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[float]:
    """
    Compute a composite score from monthly indicator data.

    Loads the most recent monthly indicators from indicators_monthly with a
    month_start on or before scoring_date (no look-ahead), then scores all
    available indicators using the same primitives as daily/weekly scoring:
    score_all_indicators for individual scores, rollup_category for the 4
    indicator-based categories (trend, momentum, volume, volatility), and
    apply_adaptive_weights with a monthly-specific weight set and expansion
    factor.

    Does not include patterns, divergences, crossovers, news, fundamentals,
    or macro — those have no monthly equivalents.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        config:       Scorer config dict containing monthly_adaptive_weights and
                      scoring.score_expansion_factor.
        scoring_date: The date being scored (YYYY-MM-DD). Only monthly candles
                      with month_start <= scoring_date are considered.
        regime:       Market regime — "trending", "ranging", or "volatile".
                      Defaults to "ranging".

    Returns:
        Float composite score (-100 to +100), or None if no monthly data.
    """
    from src.scorer.indicator_scorer import (
        load_profile_for_ticker,
        score_all_indicators,
    )
    from src.scorer.category_scorer import apply_adaptive_weights, rollup_category

    row = db_conn.execute(
        "SELECT m.close, i.* "
        "FROM indicators_monthly i "
        "JOIN monthly_candles m ON i.ticker = m.ticker AND i.month_start = m.month_start "
        "WHERE i.ticker = ? AND i.month_start <= ? "
        "ORDER BY i.month_start DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()

    if row is None:
        logger.debug(f"{ticker}: no monthly indicator data found")
        return None

    close = row["close"]
    if close is None:
        logger.debug(f"{ticker}: monthly candle has no close price")
        return None

    indicators = dict(row)

    # Load daily profiles as proxy for monthly distributions
    profiles = load_profile_for_ticker(db_conn, ticker)

    indicator_scores = score_all_indicators(
        indicators=indicators,
        close=close,
        profiles=profiles,
        config=config,
        regime=regime,
    )

    # Build 4 monthly-applicable category scores
    trend_score = rollup_category("monthly_trend", {
        "ema_alignment": indicator_scores.get("ema_alignment"),
        "macd_line": indicator_scores.get("macd_line"),
        "macd_histogram": indicator_scores.get("macd_histogram"),
        "adx": indicator_scores.get("adx"),
    })
    momentum_score = rollup_category("monthly_momentum", {
        "rsi_14": indicator_scores.get("rsi_14"),
        "stoch_k": indicator_scores.get("stoch_k"),
        "cci_20": indicator_scores.get("cci_20"),
        "williams_r": indicator_scores.get("williams_r"),
    })
    volume_score = rollup_category("monthly_volume", {
        "obv": indicator_scores.get("obv"),
        "cmf_20": indicator_scores.get("cmf_20"),
        "ad_line": indicator_scores.get("ad_line"),
    })
    volatility_score = rollup_category("monthly_volatility", {
        "bb_pctb": indicator_scores.get("bb_pctb"),
        "atr_14": indicator_scores.get("atr_14"),
    })

    category_scores = {
        "trend": trend_score,
        "momentum": momentum_score,
        "volume": volume_score,
        "volatility": volatility_score,
    }

    if all(score == 0.0 for score in category_scores.values()):
        logger.debug(f"{ticker}: no usable monthly indicators")
        return None

    # Apply monthly adaptive weights (4 categories summing to 1.0)
    monthly_weights_cfg = config.get("monthly_adaptive_weights", {})
    regime_weights = monthly_weights_cfg.get(regime, {})

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

