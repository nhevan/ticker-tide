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

#: Maps each key produced by ``score_all_indicators`` to its scoring category.
#:
#: **Maintenance obligation**: whenever you add an indicator to
#: ``score_all_indicators`` in ``src/scorer/indicator_scorer.py``, you MUST
#: also add a ``(key, category)`` entry here.  The 9 valid category names are:
#: trend, momentum, volume, volatility, candlestick, structural, sentiment,
#: fundamental, macro.
#:
#: Removed entries:
#: - ``"atr_14"`` was here (volatility) but ``score_all_indicators`` always
#:   returns 0.0 for it (it is a confidence-modifier input, not directional).
#:   Keeping a zero-valued entry causes it to dilute genuine volatility signals
#:   via the magnitude-weighted rollup, so it is excluded.
#: - ``"keltner"`` was here (volatility) but ``score_all_indicators`` never
#:   emits a ``"keltner"`` key — the entry always resolved to None and
#:   contributed nothing to category rollups.
INDICATOR_CATEGORY_MAP: dict[str, str] = {
    # --- trend ---
    "ema_alignment": "trend",
    "macd_line": "trend",
    "macd_histogram": "trend",
    "adx": "trend",
    # --- momentum ---
    "rsi_14": "momentum",
    "stoch_k": "momentum",
    "cci_20": "momentum",
    "williams_r": "momentum",
    # --- volume ---
    "obv": "volume",
    "cmf_20": "volume",
    "ad_line": "volume",
    # --- volatility ---
    "bb_pctb": "volatility",
}

#: Maps each key in the ``pattern_scores`` dict (assembled in
#: ``src/scorer/main.py``) to its scoring category.
#:
#: **Maintenance obligation**: whenever you add a pattern key to the
#: ``pattern_scores`` dict in ``src/scorer/main.py``, you MUST also add a
#: ``(key, category)`` entry here.  The 9 valid category names are:
#: trend, momentum, volume, volatility, candlestick, structural, sentiment,
#: fundamental, macro.
PATTERN_CATEGORY_MAP: dict[str, str] = {
    # --- trend (crossovers) ---
    "crossover_ema_9_21": "trend",
    "crossover_ema_21_50": "trend",
    "crossover_macd_signal": "trend",
    # --- momentum (divergences) ---
    "divergence_rsi": "momentum",
    "divergence_macd": "momentum",
    "divergence_stoch": "momentum",
    # --- volume (divergences) ---
    "divergence_obv": "volume",
    # --- candlestick ---
    "candlestick_pattern_score": "candlestick",
    # --- structural ---
    "structural_pattern_score": "structural",
    "gap_score": "structural",
    "fibonacci_score": "structural",
}

# All 9 category names used throughout the scoring pipeline.
_ALL_CATEGORIES = frozenset({
    "trend", "momentum", "volume", "volatility",
    "candlestick", "structural", "sentiment", "fundamental", "macro",
})


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
    # Build per-category component dicts from the module-level maps.
    components_by_category: dict[str, dict[str, float]] = {cat: {} for cat in _ALL_CATEGORIES}

    for ind_name, cat in INDICATOR_CATEGORY_MAP.items():
        components_by_category[cat][ind_name] = indicator_scores.get(ind_name)

    for pat_name, cat in PATTERN_CATEGORY_MAP.items():
        components_by_category[cat][pat_name] = pattern_scores.get(pat_name)

    # Sentiment, fundamental, and macro are passed in as pre-computed scalars
    # rather than being looked up from indicator/pattern dicts.
    components_by_category["sentiment"] = {
        "news_sentiment_score": sentiment_scores.get("news_sentiment_score"),
        "short_interest_score": sentiment_scores.get("short_interest_score"),
    }
    components_by_category["fundamental"] = {"fundamental_score": fundamental_score}
    components_by_category["macro"] = {"macro_score": macro_score}

    return {
        cat: rollup_category(cat, components_by_category[cat])
        for cat in (
            "trend", "momentum", "volume", "volatility",
            "candlestick", "structural", "sentiment", "fundamental", "macro",
        )
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
