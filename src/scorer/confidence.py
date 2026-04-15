"""
Confidence calculation and signal classification.

Converts the final composite score into:
  - Signal: BULLISH / BEARISH / NEUTRAL (based on thresholds)
  - Confidence: 0-100% (base from score magnitude + modifiers)

Confidence modifiers adjust the base confidence up or down based on:
  - Timeframe agreement (daily vs weekly)
  - Volume confirmation
  - Indicator consensus
  - Earnings proximity
  - VIX level
  - ATR expansion
  - Data completeness

Also builds:
  - data_completeness: JSON dict of which data sources were available
  - key_signals: list of top contributing signal descriptions
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

# Neutral zone threshold: if either daily or weekly score is within ±NEUTRAL_ZONE,
# the timeframe direction is considered ambiguous (not a clear agree/disagree).
_NEUTRAL_ZONE = 10.0

# Volume confirmation neutral zone: skip if either score is very small
_VOLUME_NEUTRAL_ZONE = 5.0


def classify_signal(final_score: float, config: dict) -> str:
    """
    Classify a composite score as BULLISH, BEARISH, or NEUTRAL.

    Uses inclusive thresholds: >= bullish_threshold → BULLISH,
    <= bearish_threshold → BEARISH, otherwise NEUTRAL.

    Parameters:
        final_score: Merged composite score (-100 to +100).
        config: Scorer config dict containing signal_thresholds.

    Returns:
        One of "BULLISH", "BEARISH", or "NEUTRAL".
    """
    thresholds = config.get("signal_thresholds", {})
    bullish_threshold: float = thresholds.get("bullish", 30)
    bearish_threshold: float = thresholds.get("bearish", -30)

    if final_score >= bullish_threshold:
        return "BULLISH"
    if final_score <= bearish_threshold:
        return "BEARISH"
    return "NEUTRAL"


def compute_confidence_modifiers(
    daily_score: float,
    weekly_score: Optional[float],
    category_scores: dict,
    indicator_scores: dict,
    earnings_date: Optional[str],
    scoring_date: str,
    vix: Optional[float],
    atr: Optional[float],
    atr_sma: Optional[float],
    news_available: bool,
    fundamentals_available: bool,
    config: dict,
) -> dict:
    """
    Compute individual confidence modifiers based on market data quality and agreement.

    Each modifier either adds to or subtracts from the base confidence score (which
    is simply the absolute value of the final composite score). Returns a dict keyed
    by modifier name with its numeric value.

    Parameters:
        daily_score: Daily composite score (-100 to +100).
        weekly_score: Weekly composite score (-100 to +100), or None if unavailable.
        category_scores: Dict of the 9 category scores (trend, volume, etc.).
        indicator_scores: Dict of individual indicator scores; None values are skipped.
        earnings_date: Next earnings date as a YYYY-MM-DD string, or None.
        scoring_date: The date being scored in YYYY-MM-DD format.
        vix: Current VIX value, or None if unavailable.
        atr: Current ATR value for the ticker, or None.
        atr_sma: 20-day SMA of ATR for the ticker, or None.
        news_available: Whether news data was available for this ticker.
        fundamentals_available: Whether fundamentals data was available.
        config: Scorer config dict containing confidence_modifiers sub-dict.

    Returns:
        Dict mapping modifier name → float modifier value.
    """
    mods_cfg = config.get("confidence_modifiers", {})

    return {
        "timeframe_agreement": _modifier_timeframe(daily_score, weekly_score, mods_cfg),
        "volume_confirmation": _modifier_volume(category_scores, mods_cfg),
        "indicator_consensus": _modifier_indicator_consensus(indicator_scores, daily_score, mods_cfg),
        "earnings_proximity": _modifier_earnings(earnings_date, scoring_date, mods_cfg),
        "vix_extreme": _modifier_vix(vix, mods_cfg),
        "atr_expanding": _modifier_atr(atr, atr_sma, mods_cfg),
        "missing_data": _modifier_missing_data(news_available, fundamentals_available, mods_cfg),
    }


def _modifier_timeframe(
    daily_score: float,
    weekly_score: Optional[float],
    mods_cfg: dict,
) -> float:
    """Return timeframe agreement/disagreement modifier."""
    if weekly_score is None:
        return 0.0

    # If either score is in the neutral zone, direction is ambiguous
    if abs(daily_score) <= _NEUTRAL_ZONE or abs(weekly_score) <= _NEUTRAL_ZONE:
        return 0.0

    # Both in same direction (both positive or both negative)
    if (daily_score > 0 and weekly_score > 0) or (daily_score < 0 and weekly_score < 0):
        return float(mods_cfg.get("timeframe_agree", 10))

    # Opposite directions
    return float(mods_cfg.get("timeframe_disagree", -15))


def _modifier_volume(category_scores: dict, mods_cfg: dict) -> float:
    """Return volume confirmation/divergence modifier."""
    volume_score = category_scores.get("volume", 0.0) or 0.0
    trend_score = category_scores.get("trend", 0.0) or 0.0

    # If either is near zero, it's not a clear confirmation or divergence
    if abs(volume_score) <= _VOLUME_NEUTRAL_ZONE or abs(trend_score) <= _VOLUME_NEUTRAL_ZONE:
        return 0.0

    if (volume_score > 0 and trend_score > 0) or (volume_score < 0 and trend_score < 0):
        return float(mods_cfg.get("volume_confirms", 10))

    return float(mods_cfg.get("volume_diverges", -10))


def _modifier_indicator_consensus(
    indicator_scores: dict,
    daily_score: float,
    mods_cfg: dict,
) -> float:
    """Return indicator consensus modifier based on how many indicators agree with direction."""
    non_none = {k: v for k, v in indicator_scores.items() if v is not None}
    if not non_none:
        return 0.0

    total = len(non_none)
    # Direction of final signal
    bullish_direction = daily_score >= 0

    if bullish_direction:
        agreeing = sum(1 for v in non_none.values() if v > 0)
    else:
        agreeing = sum(1 for v in non_none.values() if v < 0)

    ratio = agreeing / total

    if ratio > 0.60:
        return float(mods_cfg.get("indicator_consensus", 5))
    if ratio < 0.50:
        return float(mods_cfg.get("indicator_mixed", -10))
    return 0.0


def _modifier_earnings(
    earnings_date: Optional[str],
    scoring_date: str,
    mods_cfg: dict,
) -> float:
    """Return earnings proximity penalty if earnings are within the configured window."""
    if earnings_date is None:
        return 0.0

    try:
        ed = date.fromisoformat(earnings_date)
        sd = date.fromisoformat(scoring_date)
        days_until = (ed - sd).days
    except (ValueError, TypeError):
        return 0.0

    within_days: int = mods_cfg.get("earnings_within_days", 7)
    if 0 <= days_until <= within_days:
        return float(mods_cfg.get("earnings_penalty", -15))
    return 0.0


def _modifier_vix(vix: Optional[float], mods_cfg: dict) -> float:
    """Return VIX extreme penalty when VIX is above threshold."""
    if vix is None:
        return 0.0
    threshold: float = mods_cfg.get("vix_extreme_threshold", 30)
    if vix > threshold:
        return float(mods_cfg.get("vix_extreme_penalty", -10))
    return 0.0


def _modifier_atr(
    atr: Optional[float],
    atr_sma: Optional[float],
    mods_cfg: dict,
) -> float:
    """Return ATR expansion penalty when ATR is more than 1.5x its moving average."""
    if atr is None or atr_sma is None or atr_sma <= 0:
        return 0.0
    if atr > atr_sma * 1.5:
        return float(mods_cfg.get("atr_expanding_penalty", -5))
    return 0.0


def _modifier_missing_data(
    news_available: bool,
    fundamentals_available: bool,
    mods_cfg: dict,
) -> float:
    """Return combined missing data penalty."""
    penalty = 0.0
    if not news_available:
        penalty += float(mods_cfg.get("missing_news_penalty", -5))
    if not fundamentals_available:
        penalty += float(mods_cfg.get("missing_fundamentals_penalty", -3))
    return penalty


def compute_confidence(final_score: float, modifiers: dict) -> float:
    """
    Compute the final confidence score from the base (absolute value of score) plus modifiers.

    Result is clamped to [0, 100].

    Parameters:
        final_score: Merged composite score; base confidence = abs(final_score).
        modifiers: Dict of modifier name → value (from compute_confidence_modifiers).

    Returns:
        Float confidence clamped to [0.0, 100.0].
    """
    base = abs(final_score)
    total_modifier = sum(modifiers.values()) if modifiers else 0.0
    confidence = base + total_modifier
    return max(0.0, min(100.0, confidence))


def compute_full_confidence(
    final_score: float,
    daily_score: float,
    weekly_score: Optional[float],
    category_scores: dict,
    indicator_scores: dict,
    earnings_date: Optional[str],
    scoring_date: str,
    vix: Optional[float],
    atr: Optional[float],
    atr_sma: Optional[float],
    news_available: bool,
    fundamentals_available: bool,
    config: dict,
) -> dict:
    """
    Compute full confidence with all modifiers, returning a structured result.

    Combines compute_confidence_modifiers and compute_confidence into a single call
    that returns a dict with keys: confidence, base, modifiers.

    Parameters:
        final_score: Merged composite score (-100 to +100).
        daily_score: Daily composite score.
        weekly_score: Weekly composite score, or None.
        category_scores: Dict of the 9 category scores.
        indicator_scores: Dict of individual indicator scores.
        earnings_date: Next earnings date string or None.
        scoring_date: Current scoring date string.
        vix: Current VIX value or None.
        atr: Current ATR value or None.
        atr_sma: 20-day SMA of ATR or None.
        news_available: Whether news data is available.
        fundamentals_available: Whether fundamentals data is available.
        config: Full scorer config dict.

    Returns:
        Dict with:
            "confidence": float — final clamped confidence value
            "base": float — abs(final_score)
            "modifiers": dict — each modifier name → value
    """
    modifiers = compute_confidence_modifiers(
        daily_score=daily_score,
        weekly_score=weekly_score,
        category_scores=category_scores,
        indicator_scores=indicator_scores,
        earnings_date=earnings_date,
        scoring_date=scoring_date,
        vix=vix,
        atr=atr,
        atr_sma=atr_sma,
        news_available=news_available,
        fundamentals_available=fundamentals_available,
        config=config,
    )
    confidence = compute_confidence(final_score, modifiers)
    return {
        "confidence": confidence,
        "base": abs(final_score),
        "modifiers": modifiers,
    }


def build_data_completeness(
    news_available: bool,
    fundamentals_available: bool,
    weekly_available: bool,
    filings_available: bool,
    short_interest_available: bool,
    earnings_available: bool,
    monthly_available: bool = False,
) -> dict:
    """
    Build a dict describing which data sources were available for this score.

    This is stored as JSON in scores_daily.data_completeness and used by the
    AI reasoner to understand data quality.

    Parameters:
        news_available: Whether news articles were available.
        fundamentals_available: Whether fundamental financials were available.
        weekly_available: Whether weekly indicator data was available.
        filings_available: Whether 8-K SEC filing data was available.
        short_interest_available: Whether short interest data was available.
        earnings_available: Whether earnings calendar data was available.
        monthly_available: Whether monthly indicator data was available.

    Returns:
        Dict with boolean values for each data source.
    """
    return {
        "news": news_available,
        "fundamentals": fundamentals_available,
        "weekly": weekly_available,
        "monthly": monthly_available,
        "filings": filings_available,
        "short_interest": short_interest_available,
        "earnings": earnings_available,
    }


def build_key_signals(
    indicator_scores: dict,
    pattern_scores: dict,
    regime: str,
    category_scores: dict,
    final_score: float,
    signal: str,
) -> list[str]:
    """
    Build a list of 3-7 human-readable strings describing the top contributing signals.

    Sorts all available scores by absolute magnitude, takes the strongest,
    and formats each as a descriptive text string. Used by the AI reasoner
    and stored in scores_daily.key_signals.

    Parameters:
        indicator_scores: Dict of indicator name → score (None values skipped).
        pattern_scores: Dict of pattern name → score (None values skipped).
        regime: Current market regime ("trending", "ranging", "volatile").
        category_scores: Dict of the 9 category scores.
        final_score: Final composite score for context.
        signal: Classified signal ("BULLISH", "BEARISH", "NEUTRAL").

    Returns:
        List of 0-7 human-readable signal description strings.
    """
    # Collect all named scores with their absolute magnitude
    candidates: list[tuple[str, float, float]] = []  # (name, score, abs_score)

    all_scores: dict = {**indicator_scores, **pattern_scores, **category_scores}
    for name, score in all_scores.items():
        if score is None:
            continue
        candidates.append((name, float(score), abs(float(score))))

    if not candidates:
        return []

    # Sort by absolute value descending, take top 7
    candidates.sort(key=lambda x: x[2], reverse=True)
    top = candidates[:7]

    descriptions: list[str] = []
    for name, score, _ in top:
        desc = _format_signal_description(name, score, signal)
        if desc:
            descriptions.append(desc)

    return descriptions[:7]


def _format_signal_description(name: str, score: float, signal: str) -> str:
    """
    Format a single indicator/pattern score as a human-readable description.

    Parameters:
        name: The indicator or pattern name.
        score: Its score value.
        signal: Overall signal direction for context.

    Returns:
        A human-readable string, or empty string if no useful description can be formed.
    """
    direction = "bullish" if score > 0 else "bearish"

    descriptions = {
        "ema_alignment": (
            f"{'Bullish' if score > 0 else 'Bearish'} EMA stack — "
            f"price {'above' if score > 0 else 'below'} all EMAs"
        ),
        "macd_histogram": (
            f"MACD histogram {'expanding positive' if score > 0 else 'expanding negative'} "
            f"({direction} momentum)"
        ),
        "macd_line": f"MACD line {'above' if score > 0 else 'below'} signal line ({direction})",
        "rsi_14": (
            f"RSI {'oversold — bullish reversal signal' if score > 0 else 'overbought — bearish reversal signal'}"
            if abs(score) > 50
            else f"RSI {'rising above 50' if score > 0 else 'falling below 50'} ({direction})"
        ),
        "stoch_k": (
            f"Stochastic {'oversold — bullish' if score > 0 else 'overbought — bearish'}"
            if abs(score) > 50
            else f"Stochastic momentum {direction}"
        ),
        "adx": f"ADX trend strength {'strong (trending)' if score > 0 else 'weak (ranging)'}",
        "obv": f"On-balance volume {'accumulating (bullish)' if score > 0 else 'distributing (bearish)'}",
        "cmf_20": f"Chaikin Money Flow {'positive — buying pressure' if score > 0 else 'negative — selling pressure'}",
        "ad_line": f"A/D Line {'rising (accumulation)' if score > 0 else 'falling (distribution)'}",
        "bb_pctb": (
            f"Price {'approaching lower Bollinger Band (oversold)' if score > 0 else 'near upper Bollinger Band (overbought)'}"
        ),
        "cci_20": f"CCI {'oversold — mean-reversion bullish' if score > 0 else 'overbought — mean-reversion bearish'}",
        "williams_r": f"Williams %R {'oversold' if score > 0 else 'overbought'}",
        "keltner": f"Price {'inside Keltner — neutral' if abs(score) < 30 else f'{direction} Keltner squeeze'}",
        "atr_14": f"ATR {'elevated — increased volatility' if score < 0 else 'compressed — low volatility'}",
        "candlestick_pattern_score": f"{'Bullish' if score > 0 else 'Bearish'} candlestick pattern detected",
        "structural_pattern_score": f"{'Bullish' if score > 0 else 'Bearish'} structural pattern detected",
        "gap_score": f"{'Bullish gap-up' if score > 0 else 'Bearish gap-down'} — momentum signal",
        "fibonacci_score": (
            f"Price at Fibonacci {'support level' if score > 0 else 'resistance level'}"
        ),
        "divergence_rsi": f"{'Bullish' if score > 0 else 'Bearish'} RSI divergence — momentum {'shifting higher' if score > 0 else 'shifting lower'}",
        "divergence_macd": f"{'Bullish' if score > 0 else 'Bearish'} MACD divergence detected",
        "divergence_stoch": f"{'Bullish' if score > 0 else 'Bearish'} Stochastic divergence detected",
        "divergence_obv": f"{'Bullish' if score > 0 else 'Bearish'} OBV divergence — volume {'supporting' if score > 0 else 'opposing'} price",
        "crossover_ema_9_21": f"{'Bullish' if score > 0 else 'Bearish'} EMA 9/21 crossover recently",
        "crossover_ema_21_50": f"{'Bullish' if score > 0 else 'Bearish'} EMA 21/50 crossover recently",
        "crossover_macd_signal": f"{'Bullish' if score > 0 else 'Bearish'} MACD/signal crossover recently",
        "trend": f"Trend category score {direction} ({score:+.0f})",
        "momentum": f"Momentum category score {direction} ({score:+.0f})",
        "volume": f"Volume category score {direction} ({score:+.0f})",
        "structural": f"Structural category score {direction} ({score:+.0f})",
        "macro": f"Macro environment {direction} ({score:+.0f})",
        "sentiment": f"Market sentiment {direction} ({score:+.0f})",
    }

    return descriptions.get(name, f"{name.replace('_', ' ').title()}: {direction} ({score:+.0f})")


def get_next_earnings_date(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[str]:
    """
    Query the next upcoming earnings date for a ticker on or after the scoring date.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        scoring_date: Current scoring date as YYYY-MM-DD.

    Returns:
        The next earnings date string (YYYY-MM-DD), or None if no upcoming earnings found.
    """
    row = db_conn.execute(
        "SELECT earnings_date FROM earnings_calendar "
        "WHERE ticker = ? AND earnings_date >= ? "
        "ORDER BY earnings_date ASC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()

    if row is None:
        return None
    return row["earnings_date"]
