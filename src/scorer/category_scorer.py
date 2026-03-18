"""
Category score rollup and adaptive weight application.

Rolls up individual indicator/pattern scores into the 9 category scores,
then applies regime-based adaptive weights to produce a raw composite score.

Categories:
  1. Trend — EMA alignment, MACD, ADX, crossovers
  2. Momentum — RSI, Stochastic, CCI, Williams %R, divergences (RSI/MACD/Stoch)
  3. Volume — OBV, CMF, A/D Line, OBV divergence
  4. Volatility — Bollinger Bands, ATR, Keltner
  5. Candlestick — candlestick pattern score
  6. Structural — structural patterns, gaps, Fibonacci
  7. Sentiment — news sentiment, short interest, filing flag
  8. Fundamental — P/E, EPS growth, revenue growth, D/E
  9. Macro — SPY trend, VIX, sector ETF, treasury, relative strength
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def rollup_category(category_name: str, component_scores: dict) -> float:
    """
    Roll up non-None component scores into a single category score using
    magnitude-weighted averaging.

    Each component is weighted by its absolute value relative to the total
    absolute value of all components. This prevents weak or neutral components
    from diluting strong signals — e.g. RSI=+57, Stoch=+69, CCI=+75,
    Williams=+69, MACD_divergence=-9 yields ~+65.6 rather than +52.2.

    Falls back to 0 if all components are None or all are exactly zero.
    Result is clamped to [-100, +100].

    Parameters:
        category_name: Human-readable category name (for logging only).
        component_scores: Dict mapping component name → score (float or None).

    Returns:
        Float magnitude-weighted average score clamped to [-100, +100].
    """
    available = [v for v in component_scores.values() if v is not None]
    if not available:
        logger.debug(f"Category '{category_name}': all components None → returning 0")
        return 0.0

    abs_sum = sum(abs(s) for s in available)
    if abs_sum == 0.0:
        logger.debug(f"Category '{category_name}': all components zero → returning 0")
        return 0.0

    # weight_i = abs(score_i) / sum(abs(scores)), so weighted avg = sum(score_i * abs(score_i)) / abs_sum
    weighted = sum(s * abs(s) for s in available) / abs_sum
    clamped = max(-100.0, min(100.0, weighted))
    logger.debug(
        f"Category '{category_name}': {len(available)} components, "
        f"magnitude-weighted={weighted:.1f}, clamped={clamped:.1f}"
    )
    return clamped


def compute_all_category_scores(
    indicator_scores: dict,
    pattern_scores: dict,
    sentiment_scores: dict,
    fundamental_score: float,
    macro_score: float,
) -> dict:
    """
    Map individual indicator and pattern scores into the 9 scoring categories.

    Each category is computed by calling rollup_category with the relevant
    component scores. Missing components (None) are handled gracefully.

    Parameters:
        indicator_scores: Dict from score_all_indicators() with keys like
                          "ema_alignment", "macd_histogram", "rsi_14", etc.
        pattern_scores: Dict with keys like "candlestick_pattern_score",
                        "structural_pattern_score", "gap_score", "fibonacci_score",
                        "divergence_rsi", "divergence_macd", "crossover_ema_9_21", etc.
        sentiment_scores: Dict with keys "news_sentiment_score", "short_interest_score".
        fundamental_score: Pre-computed fundamental score (float).
        macro_score: Pre-computed macro score (float).

    Returns:
        Dict with 9 category names mapped to scores between -100 and +100.
    """
    # 1. Trend: EMA alignment, MACD line, MACD histogram, ADX, crossovers
    trend_components = {
        "ema_alignment": indicator_scores.get("ema_alignment"),
        "macd_line": indicator_scores.get("macd_line"),
        "macd_histogram": indicator_scores.get("macd_histogram"),
        "adx": indicator_scores.get("adx"),
        "crossover_ema_9_21": pattern_scores.get("crossover_ema_9_21"),
        "crossover_ema_21_50": pattern_scores.get("crossover_ema_21_50"),
        "crossover_macd": pattern_scores.get("crossover_macd_signal"),
    }

    # 2. Momentum: RSI, Stochastic, CCI, Williams %R, divergences
    momentum_components = {
        "rsi_14": indicator_scores.get("rsi_14"),
        "stoch_k": indicator_scores.get("stoch_k"),
        "cci_20": indicator_scores.get("cci_20"),
        "williams_r": indicator_scores.get("williams_r"),
        "divergence_rsi": pattern_scores.get("divergence_rsi"),
        "divergence_macd": pattern_scores.get("divergence_macd"),
        "divergence_stoch": pattern_scores.get("divergence_stoch"),
    }

    # 3. Volume: OBV, CMF, A/D Line, OBV divergence
    volume_components = {
        "obv": indicator_scores.get("obv"),
        "cmf_20": indicator_scores.get("cmf_20"),
        "ad_line": indicator_scores.get("ad_line"),
        "divergence_obv": pattern_scores.get("divergence_obv"),
    }

    # 4. Volatility: Bollinger Bands %B, ATR, Keltner
    volatility_components = {
        "bb_pctb": indicator_scores.get("bb_pctb"),
        "atr_14": indicator_scores.get("atr_14"),
        "keltner": indicator_scores.get("keltner"),
    }

    # 5. Candlestick: single candlestick pattern score
    candlestick_components = {
        "candlestick_pattern_score": pattern_scores.get("candlestick_pattern_score"),
    }

    # 6. Structural: structural patterns, gaps, Fibonacci
    structural_components = {
        "structural_pattern_score": pattern_scores.get("structural_pattern_score"),
        "gap_score": pattern_scores.get("gap_score"),
        "fibonacci_score": pattern_scores.get("fibonacci_score"),
    }

    # 7. Sentiment: news sentiment, short interest
    sentiment_components = {
        "news_sentiment_score": sentiment_scores.get("news_sentiment_score"),
        "short_interest_score": sentiment_scores.get("short_interest_score"),
    }

    # 8. Fundamental: single pre-computed score
    fundamental_components = {"fundamental_score": fundamental_score}

    # 9. Macro: single pre-computed score
    macro_components = {"macro_score": macro_score}

    return {
        "trend": rollup_category("trend", trend_components),
        "momentum": rollup_category("momentum", momentum_components),
        "volume": rollup_category("volume", volume_components),
        "volatility": rollup_category("volatility", volatility_components),
        "candlestick": rollup_category("candlestick", candlestick_components),
        "structural": rollup_category("structural", structural_components),
        "sentiment": rollup_category("sentiment", sentiment_components),
        "fundamental": rollup_category("fundamental", fundamental_components),
        "macro": rollup_category("macro", macro_components),
    }


def apply_adaptive_weights(
    category_scores: dict,
    regime_weights: dict,
    expansion_factor: float = 1.0,
) -> float:
    """
    Apply regime-based adaptive weights to category scores and produce a composite score.

    Multiplies each category score by its weight, sums the result, then applies
    an optional expansion multiplier to widen the score distribution before
    clamping to [-100, +100].

    Parameters:
        category_scores: Dict mapping category names to scores (-100 to +100).
        regime_weights: Dict mapping category names to float weights (sum should = 1.0).
        expansion_factor: Multiplier applied to the weighted sum before clamping.
                          Values > 1.0 widen the score distribution. Default 1.0
                          (no expansion). Loaded from config['scoring']['score_expansion_factor'].

    Returns:
        Float weighted composite score clamped to [-100, +100].
    """
    weighted_sum = 0.0
    for category, weight in regime_weights.items():
        score = category_scores.get(category, 0.0)
        weighted_sum += score * weight

    expanded = weighted_sum * expansion_factor
    return max(-100.0, min(100.0, expanded))
