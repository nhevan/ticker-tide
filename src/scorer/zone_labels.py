"""
Zone label classifiers for scored indicators.

Maps an indicator value to a human-readable zone string that describes
where the value sits in either:
  (a) the ticker's historical percentile distribution (profile path), or
  (b) fixed threshold bands (fallback path).

Relationship to score_with_percentile zones
-------------------------------------------
score_with_percentile (higher_is_bullish=False) uses six zones:
  value < p5        → score +80 to +100  ("extreme_oversold")
  p5 ≤ value < p20  → score +40 to +80   ("oversold")
  p20 ≤ value < p50 → score +10 to +40   ("below_mid")
  p50 ≤ value < p80 → score -40 to +10   ("above_mid")
  p80 ≤ value < p95 → score -40 to -80   ("overbought")
  value ≥ p95       → score -80 to -100  ("extreme_overbought")

The LABEL semantics describe the value's position in the historical distribution,
not the score direction. For RSI (higher_is_bullish=False), low-value zones are
bullish and high-value zones are bearish — but the label itself is neutral about
direction; callers decide how to translate labels to human text.

Boundary convention: inclusive-lower, exclusive-upper → [lower, upper).
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def zone_label_for_rsi(
    value: float,
    profile: Optional[dict],
    thresholds: dict,
) -> str:
    """
    Return a zone label describing where the RSI value sits.

    Uses the ticker's historical percentile profile when available (six labels),
    or falls back to fixed oversold/overbought thresholds (four labels).

    Profile path (six labels, boundary convention [lower, upper)):
      value < p5            → "extreme_oversold"
      p5 ≤ value < p20      → "oversold"
      p20 ≤ value < p50     → "below_mid"
      p50 ≤ value < p80     → "above_mid"
      p80 ≤ value < p95     → "overbought"
      value ≥ p95           → "extreme_overbought"

    Fallback path (four labels, midpoint = (oversold + overbought) / 2):
      value < oversold      → "oversold"
      oversold ≤ value < midpoint → "below_mid"
      midpoint ≤ value < overbought → "above_mid"
      value ≥ overbought    → "overbought"

    Parameters:
        value: Current RSI value (typically 0–100).
        profile: Dict with keys p5, p20, p50, p80, p95 (and optionally mean, std),
                 or None when no per-ticker profile exists.
        thresholds: Dict with keys "oversold" and "overbought" (floats).
                    Used as fallback thresholds when profile is None.

    Returns:
        Zone label string. One of:
          Profile path: "extreme_oversold", "oversold", "below_mid",
                        "above_mid", "overbought", "extreme_overbought"
          Fallback path: "oversold", "below_mid", "above_mid", "overbought"
    """
    if profile is not None:
        label = _zone_label_profile(value, profile)
        logger.debug(
            f"zone_label_for_rsi: value={value}, path=profile, label={label!r}"
        )
        return label

    label = _zone_label_fallback(value, thresholds)
    logger.debug(
        f"zone_label_for_rsi: value={value}, path=fallback, "
        f"oversold={thresholds.get('oversold')}, overbought={thresholds.get('overbought')}, "
        f"label={label!r}"
    )
    return label


def zone_label_for_stoch_k(
    value: float,
    profile: Optional[dict],
    thresholds: dict,
) -> str:
    """
    Return a zone label describing where the Stochastic %K (0–100) value sits.

    Uses the ticker's historical percentile profile when available (six labels),
    or falls back to fixed oversold/overbought thresholds (four labels).

    Profile path (six labels, boundary convention [lower, upper)):
      value < p5            → "extreme_oversold"
      p5 ≤ value < p20      → "oversold"
      p20 ≤ value < p50     → "below_mid"
      p50 ≤ value < p80     → "above_mid"
      p80 ≤ value < p95     → "overbought"
      value ≥ p95           → "extreme_overbought"

    Fallback path (four labels, midpoint = (oversold + overbought) / 2):
      value < oversold      → "oversold"
      oversold ≤ value < midpoint → "below_mid"
      midpoint ≤ value < overbought → "above_mid"
      value ≥ overbought    → "overbought"

    Parameters:
        value: Current Stochastic %K value (0–100).
        profile: Dict with keys p5, p20, p50, p80, p95 (and optionally mean, std),
                 or None when no per-ticker profile exists.
        thresholds: Dict with keys "oversold" and "overbought" (floats).
                    Used as fallback thresholds when profile is None.
                    Standard defaults: oversold=20.0, overbought=80.0.

    Returns:
        Zone label string. One of:
          Profile path: "extreme_oversold", "oversold", "below_mid",
                        "above_mid", "overbought", "extreme_overbought"
          Fallback path: "oversold", "below_mid", "above_mid", "overbought"
    """
    if profile is not None:
        label = _zone_label_profile(value, profile)
        logger.debug(
            f"zone_label_for_stoch_k: value={value}, path=profile, label={label!r}"
        )
        return label

    label = _zone_label_fallback(value, thresholds)
    logger.debug(
        f"zone_label_for_stoch_k: value={value}, path=fallback, "
        f"oversold={thresholds.get('oversold')}, overbought={thresholds.get('overbought')}, "
        f"label={label!r}"
    )
    return label


def zone_label_for_adx(value: float) -> str:
    """
    Return the ADX trend-strength zone label for a value.

    Single-arg signature (no profile, no thresholds dict) because ADX is in
    PROFILE_FREE_INDICATORS — there is no profile path and no config-driven
    fallback. Thresholds mirror score_adx's `>=` semantics exactly so
    label/score sign agree at every boundary.

    NOTE: This function intentionally does NOT consult FIXED_LADDER['adx'].
    FIXED_LADDER contains a dead (50.0, "strong trend") fourth (final) entry that has
    no effect on scoring (score_adx caps at >= 40 → 80.0). See
    indicator_scorer.py:381-407 for the authoritative thresholds. The
    follow-up cleanup of FIXED_LADDER is tracked separately.

    Parameters:
        value: Current ADX value (typically 0–100).

    Returns:
        Zone label string. One of: "ranging", "weak_trend_developing",
        "developing_trend", "strong_trend".
    """
    if value >= 40.0:
        return "strong_trend"
    if value >= 25.0:
        return "developing_trend"
    if value >= 20.0:
        return "weak_trend_developing"
    return "ranging"


def _zone_label_profile(value: float, profile: dict) -> str:
    """
    Return a zone label using the ticker's percentile profile.

    Boundary convention: [lower, upper) — inclusive-lower, exclusive-upper.

    Parameters:
        value: Current RSI value.
        profile: Dict with keys p5, p20, p50, p80, p95.

    Returns:
        One of: "extreme_oversold", "oversold", "below_mid",
                "above_mid", "overbought", "extreme_overbought".
    """
    p5 = profile["p5"]
    p20 = profile["p20"]
    p50 = profile["p50"]
    p80 = profile["p80"]
    p95 = profile["p95"]

    if value < p5:
        return "extreme_oversold"
    if value < p20:
        return "oversold"
    if value < p50:
        return "below_mid"
    if value < p80:
        return "above_mid"
    if value < p95:
        return "overbought"
    return "extreme_overbought"


def _zone_label_fallback(value: float, thresholds: dict) -> str:
    """
    Return a zone label using fixed oversold/overbought thresholds.

    Four zones, boundary convention: [lower, upper).
    midpoint = (oversold + overbought) / 2

    Parameters:
        value: Current RSI value.
        thresholds: Dict with keys "oversold" and "overbought" (floats).

    Returns:
        One of: "oversold", "below_mid", "above_mid", "overbought".
    """
    oversold = float(thresholds.get("oversold", 30.0))
    overbought = float(thresholds.get("overbought", 70.0))
    midpoint = (oversold + overbought) / 2

    if value < oversold:
        return "oversold"
    if value < midpoint:
        return "below_mid"
    if value < overbought:
        return "above_mid"
    return "overbought"
