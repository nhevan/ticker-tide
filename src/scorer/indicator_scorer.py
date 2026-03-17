"""
Individual indicator scoring.

Maps each indicator's current value to a score between -100 and +100
using the ticker's own calibrated percentile profile (blended with sector).

Scoring approach:
  - Bounded indicators (RSI, Stochastic, Williams %R, BB %B):
    Use percentile-based thresholds from the stock's profile
  - Unbounded indicators (MACD histogram, CCI, OBV slope, ATR):
    Use z-score normalization from the stock's profile
  - Special indicators (EMA alignment, ADX):
    Custom scoring logic

The profile provides: p5, p20, p50, p80, p95, mean, std
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Fixed RSI thresholds when no profile is available
_RSI_FIXED_OVERSOLD = 30.0
_RSI_FIXED_OVERBOUGHT = 70.0


def load_profile_for_ticker(db_conn: sqlite3.Connection, ticker: str) -> dict:
    """
    Load the indicator percentile profiles for a ticker from indicator_profiles.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.

    Returns:
        Dict mapping indicator_name → {p5, p20, p50, p80, p95, mean, std}.
        Returns empty dict if no profiles exist for this ticker.
    """
    rows = db_conn.execute(
        "SELECT indicator, p5, p20, p50, p80, p95, mean, std "
        "FROM indicator_profiles WHERE ticker = ?",
        (ticker,),
    ).fetchall()
    if not rows:
        logger.debug(f"{ticker}: no indicator profiles found")
        return {}
    return {
        row["indicator"]: {
            "p5": row["p5"],
            "p20": row["p20"],
            "p50": row["p50"],
            "p80": row["p80"],
            "p95": row["p95"],
            "mean": row["mean"],
            "std": row["std"],
        }
        for row in rows
    }


def score_with_percentile(value: float, profile: dict, higher_is_bullish: bool = True) -> float:
    """
    Score a value against its percentile profile, returning -100 to +100.

    Zones and scores (before direction flip):
      Below p5:      +80 to +100 (extreme low = bullish when higher_is_bullish=False)
      p5  to p20:    +40 to +80
      p20 to p50:    +10 to +40
      p50 to p80:    -40 to +10 (crosses neutral at p50)
      p80 to p95:    -40 to -80
      Above p95:     -80 to -100

    Linear interpolation is used within each zone.

    Parameters:
        value: The current indicator value to score.
        profile: Dict with keys p5, p20, p50, p80, p95.
        higher_is_bullish: If True, high values are bullish (positive scores).
                           If False, low values are bullish (e.g., RSI oversold = bullish).

    Returns:
        Float score between -100 and +100.
    """
    p5 = profile["p5"]
    p20 = profile["p20"]
    p50 = profile["p50"]
    p80 = profile["p80"]
    p95 = profile["p95"]

    def _interp(val: float, lo: float, hi: float, score_lo: float, score_hi: float) -> float:
        if hi == lo:
            return (score_lo + score_hi) / 2
        t = (val - lo) / (hi - lo)
        t = max(0.0, min(1.0, t))
        return score_lo + t * (score_hi - score_lo)

    # Score where LOW value = bullish (+), HIGH value = bearish (-)
    if value < p5:
        raw = _interp(value, p5 - (p20 - p5), p5, 100.0, 80.0)
    elif value < p20:
        raw = _interp(value, p5, p20, 80.0, 40.0)
    elif value < p50:
        raw = _interp(value, p20, p50, 40.0, 0.0)
    elif value < p80:
        raw = _interp(value, p50, p80, 0.0, -40.0)
    elif value < p95:
        raw = _interp(value, p80, p95, -40.0, -80.0)
    else:
        raw = _interp(value, p95, p95 + (p95 - p80), -80.0, -100.0)

    # Flip sign if higher values should be bullish
    if higher_is_bullish:
        raw = -raw

    return max(-100.0, min(100.0, raw))


def score_with_zscore(value: float, mean: float, std: float) -> float:
    """
    Score a value using z-score normalization, returning -100 to +100.

    Positive z-score = above mean = bullish (for indicators where higher is bullish).
    Zone mapping:
      z > +2.0:          +80 to +100
      z +1.0 to +2.0:    +40 to +80
      z -1.0 to +1.0:    -40 to +40 (linear through 0)
      z -2.0 to -1.0:    -40 to -80
      z < -2.0:          -80 to -100

    Parameters:
        value: The current indicator value.
        mean: Mean of the indicator's historical distribution.
        std: Standard deviation of the indicator's historical distribution.

    Returns:
        Float score between -100 and +100.
    """
    if std == 0:
        return 0.0

    z = (value - mean) / std

    def _interp(val: float, lo: float, hi: float, score_lo: float, score_hi: float) -> float:
        if hi == lo:
            return (score_lo + score_hi) / 2
        t = (val - lo) / (hi - lo)
        t = max(0.0, min(1.0, t))
        return score_lo + t * (score_hi - score_lo)

    if z > 2.0:
        raw = _interp(z, 2.0, 3.0, 80.0, 100.0)
    elif z > 1.0:
        raw = _interp(z, 1.0, 2.0, 40.0, 80.0)
    elif z > -1.0:
        raw = _interp(z, -1.0, 1.0, -40.0, 40.0)
    elif z > -2.0:
        raw = _interp(z, -2.0, -1.0, -80.0, -40.0)
    else:
        raw = _interp(z, -3.0, -2.0, -100.0, -80.0)

    return max(-100.0, min(100.0, raw))


def score_ema_alignment(close: float, ema_9: float, ema_21: float, ema_50: float) -> float:
    """
    Score the EMA stack alignment relative to price.

    Scoring:
      Perfect bullish (close > ema_9 > ema_21 > ema_50):  +100
      close > ema_9 > ema_21, ema_9 < ema_50:             +40
      close > ema_9, ema_9 < ema_21:                      +10 to +20
      Mixed:                                               -10 to +10
      Perfect bearish (close < ema_9 < ema_21 < ema_50):  -100

    Parameters:
        close: Current price.
        ema_9: 9-period EMA.
        ema_21: 21-period EMA.
        ema_50: 50-period EMA.

    Returns:
        Float score between -100 and +100.
    """
    # Count the number of "correct" orderings for a bullish stack
    bullish_conditions = [
        close > ema_9,
        ema_9 > ema_21,
        ema_21 > ema_50,
        close > ema_50,
    ]
    bullish_count = sum(bullish_conditions)

    bearish_conditions = [
        close < ema_9,
        ema_9 < ema_21,
        ema_21 < ema_50,
        close < ema_50,
    ]
    bearish_count = sum(bearish_conditions)

    # Perfect stacks
    if bullish_count == 4:
        return 100.0
    if bearish_count == 4:
        return -100.0

    # Partial stacks — scale linearly based on net alignment
    if bullish_count == 3 and close > ema_9 and ema_9 > ema_21:
        return 40.0
    if bearish_count == 3 and close < ema_9 and ema_9 < ema_21:
        return -40.0

    if close > ema_9:
        return 15.0
    if close < ema_9:
        return -15.0

    return 0.0


def score_rsi(value: float, profile: Optional[dict]) -> float:
    """
    Score the RSI indicator.

    Uses percentile-based scoring from the stock's profile when available.
    Falls back to fixed thresholds (70=overbought, 30=oversold) when not.
    High RSI = overbought = bearish (higher_is_bullish=False).

    Parameters:
        value: Current RSI value (0-100).
        profile: Percentile profile for RSI, or None for fixed fallback.

    Returns:
        Float score between -100 and +100.
    """
    if profile is not None:
        return score_with_percentile(value, profile, higher_is_bullish=False)

    # Fixed fallback
    if value <= _RSI_FIXED_OVERSOLD:
        t = (_RSI_FIXED_OVERSOLD - value) / _RSI_FIXED_OVERSOLD
        return min(100.0, 50.0 + t * 50.0)
    if value >= _RSI_FIXED_OVERBOUGHT:
        t = (value - _RSI_FIXED_OVERBOUGHT) / (100 - _RSI_FIXED_OVERBOUGHT)
        return max(-100.0, -(50.0 + t * 50.0))
    # Neutral zone (30-70)
    mid = (_RSI_FIXED_OVERSOLD + _RSI_FIXED_OVERBOUGHT) / 2
    t = (value - mid) / ((_RSI_FIXED_OVERBOUGHT - _RSI_FIXED_OVERSOLD) / 2)
    return max(-100.0, min(100.0, -t * 30.0))


def score_macd_histogram(value: float, profile: Optional[dict]) -> float:
    """
    Score the MACD histogram.

    Uses z-score normalization when a profile is available.
    Falls back to a simple linear mapping (positive = bullish) otherwise.
    Positive histogram = bullish momentum.

    Parameters:
        value: Current MACD histogram value.
        profile: Profile dict with 'mean' and 'std', or None for simple fallback.

    Returns:
        Float score between -100 and +100.
    """
    if profile is not None and profile.get("std") and profile.get("std", 0) > 0:
        return score_with_zscore(value, mean=profile["mean"], std=profile["std"])

    # Simple fallback: positive = bullish, scale by magnitude
    if value > 0:
        return min(100.0, value * 20)
    if value < 0:
        return max(-100.0, value * 20)
    return 0.0


def score_adx(value: float) -> float:
    """
    Score ADX trend strength.

    ADX measures trend STRENGTH, not direction — score is non-directional.
    ADX > 40:    +80 (very strong trend)
    ADX 25-40:   +40 to +80 (moderate to strong trend)
    ADX 20-25:   0 to +20 (weak trend)
    ADX < 20:    -20 to 0 (ranging/no trend)

    Parameters:
        value: Current ADX value.

    Returns:
        Float score between -20 and +80 (ADX is never strongly bearish).
    """
    if value >= 40:
        return 80.0
    if value >= 25:
        t = (value - 25) / (40 - 25)
        return 40.0 + t * 40.0
    if value >= 20:
        t = (value - 20) / (25 - 20)
        return t * 20.0
    # value < 20 → ranging, slight negative
    t = value / 20
    return -20.0 + t * 20.0


def score_obv(obv_values: pd.Series, profile: Optional[dict]) -> float:
    """
    Score On-Balance Volume by computing its slope over the last 20 values.

    Positive slope = accumulation (bullish), negative = distribution (bearish).
    Uses z-score normalization of the slope if a profile is available.

    Parameters:
        obv_values: Series of OBV values ordered by date (oldest first).
        profile: Profile dict with slope mean/std, or None for simple fallback.

    Returns:
        Float score between -100 and +100.
    """
    if len(obv_values) < 2:
        return 0.0

    # Use last 20 values for slope computation
    recent = obv_values.iloc[-20:] if len(obv_values) >= 20 else obv_values
    if len(recent) < 2:
        return 0.0

    x = np.arange(len(recent), dtype=float)
    y = recent.values.astype(float)
    slope = float(np.polyfit(x, y, 1)[0])

    if profile is not None and profile.get("std") and profile["std"] > 0:
        return score_with_zscore(slope, mean=profile.get("mean", 0.0), std=profile["std"])

    # Simple fallback: normalize by the mean of absolute OBV to get a ratio
    obv_scale = float(np.abs(recent).mean())
    if obv_scale == 0:
        return 0.0
    ratio = slope / obv_scale * 100
    return max(-100.0, min(100.0, ratio))


def score_all_indicators(
    indicators: dict,
    close: float,
    profiles: dict,
    config: dict,
) -> dict:
    """
    Score every indicator in the indicators dict.

    Applies the appropriate scoring function for each known indicator. Skips
    indicators that are None. Returns None for unrecognized indicators not
    explicitly handled.

    Parameters:
        indicators: Dict with keys like "rsi_14", "macd_histogram", "ema_9", etc.
        close: Current closing price (needed for EMA alignment).
        profiles: Dict mapping indicator_name → profile dict.
        config: Scorer config (currently unused but passed for extensibility).

    Returns:
        Dict mapping indicator names to scores (-100 to +100), or None for
        skipped/missing indicators. Adds "ema_alignment" as a synthetic key.
    """
    result: dict = {}

    # RSI
    rsi = indicators.get("rsi_14")
    result["rsi_14"] = None if rsi is None else score_rsi(rsi, profiles.get("rsi_14"))

    # MACD histogram
    macd_hist = indicators.get("macd_histogram")
    result["macd_histogram"] = (
        None if macd_hist is None
        else score_macd_histogram(macd_hist, profiles.get("macd_histogram"))
    )

    # MACD line
    macd_line = indicators.get("macd_line")
    if macd_line is not None:
        result["macd_line"] = score_macd_histogram(macd_line, profiles.get("macd_line"))
    else:
        result["macd_line"] = None

    # EMA alignment (composite)
    ema_9 = indicators.get("ema_9")
    ema_21 = indicators.get("ema_21")
    ema_50 = indicators.get("ema_50")
    if all(v is not None for v in (ema_9, ema_21, ema_50)):
        result["ema_alignment"] = score_ema_alignment(close, ema_9, ema_21, ema_50)
    else:
        result["ema_alignment"] = None

    # ADX
    adx = indicators.get("adx")
    result["adx"] = None if adx is None else score_adx(adx)

    # Stochastic %K (overbought/oversold, higher_is_bullish=False)
    stoch_k = indicators.get("stoch_k")
    if stoch_k is not None:
        profile_stoch = profiles.get("stoch_k")
        if profile_stoch:
            result["stoch_k"] = score_with_percentile(stoch_k, profile_stoch, higher_is_bullish=False)
        else:
            # Fixed fallback: 80=overbought, 20=oversold
            if stoch_k >= 80:
                result["stoch_k"] = -60.0
            elif stoch_k <= 20:
                result["stoch_k"] = 60.0
            else:
                result["stoch_k"] = (50.0 - stoch_k) / 50.0 * 30.0
    else:
        result["stoch_k"] = None

    # Williams %R (ranges -100 to 0; near -100 = oversold = bullish)
    williams_r = indicators.get("williams_r")
    if williams_r is not None:
        profile_wr = profiles.get("williams_r")
        if profile_wr:
            # Low Williams %R (near -100) = oversold = bullish → higher_is_bullish=False
            result["williams_r"] = score_with_percentile(williams_r, profile_wr, higher_is_bullish=False)
        else:
            # Fixed fallback: near -100 = oversold = bullish; near 0 = overbought = bearish
            if williams_r <= -80:
                result["williams_r"] = 60.0
            elif williams_r >= -20:
                result["williams_r"] = -60.0
            else:
                result["williams_r"] = (-40.0 - williams_r) / 40.0 * 30.0
    else:
        result["williams_r"] = None

    # CCI (higher_is_bullish=True for trend direction; extreme high=overbought)
    cci = indicators.get("cci_20")
    if cci is not None:
        profile_cci = profiles.get("cci_20")
        if profile_cci:
            result["cci_20"] = score_with_percentile(cci, profile_cci, higher_is_bullish=False)
        else:
            # Fixed fallback: > 100 = overbought, < -100 = oversold
            if cci >= 200:
                result["cci_20"] = -80.0
            elif cci >= 100:
                t = (cci - 100) / 100
                result["cci_20"] = -(t * 60.0 + 20.0)
            elif cci <= -200:
                result["cci_20"] = 80.0
            elif cci <= -100:
                t = (-cci - 100) / 100
                result["cci_20"] = t * 60.0 + 20.0
            else:
                result["cci_20"] = cci / 100 * (-20.0)
    else:
        result["cci_20"] = None

    # BB %B (near 0 = lower band = bullish in ranging context; near 1 = upper = bearish)
    bb_pctb = indicators.get("bb_pctb")
    if bb_pctb is not None:
        profile_bb = profiles.get("bb_pctb")
        if profile_bb:
            result["bb_pctb"] = score_with_percentile(bb_pctb, profile_bb, higher_is_bullish=False)
        else:
            # Fixed fallback: 0.0 = oversold, 1.0 = overbought
            result["bb_pctb"] = (0.5 - bb_pctb) * 100.0
    else:
        result["bb_pctb"] = None

    # ATR (not directional — returns raw pct-of-normal)
    atr = indicators.get("atr_14")
    result["atr_14"] = None if atr is None else 0.0  # Neutral; used for confidence

    # OBV — score against profile (z-score of current level vs historical distribution);
    # slope-based scoring requires a series and is handled separately via score_obv().
    obv = indicators.get("obv")
    if obv is None:
        result["obv"] = None
    else:
        profile_obv = profiles.get("obv")
        if profile_obv and profile_obv.get("std") and profile_obv["std"] > 0:
            result["obv"] = score_with_zscore(obv, mean=profile_obv["mean"], std=profile_obv["std"])
        else:
            result["obv"] = None  # Cannot score a scalar OBV without historical context

    # CMF
    cmf = indicators.get("cmf_20")
    if cmf is not None:
        profile_cmf = profiles.get("cmf_20")
        if profile_cmf:
            result["cmf_20"] = score_with_zscore(cmf, mean=profile_cmf["mean"], std=profile_cmf["std"])
        else:
            result["cmf_20"] = max(-100.0, min(100.0, cmf * 200.0))
    else:
        result["cmf_20"] = None

    # A/D Line — same approach as OBV: z-score vs profile if available, else None.
    ad_line = indicators.get("ad_line")
    if ad_line is None:
        result["ad_line"] = None
    else:
        profile_ad = profiles.get("ad_line")
        if profile_ad and profile_ad.get("std") and profile_ad["std"] > 0:
            result["ad_line"] = score_with_zscore(ad_line, mean=profile_ad["mean"], std=profile_ad["std"])
        else:
            result["ad_line"] = None  # Cannot score a scalar A/D Line without historical context

    return result
