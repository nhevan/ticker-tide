"""
Pattern and signal scoring.

Converts detected patterns, divergences, crossovers, gaps, and Fibonacci
proximity into scores between -100 and +100.

Scores are computed for the MOST RECENT date (today or last trading day).
Older patterns receive a recency decay — a pattern from 30 days ago matters
less than one from yesterday.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Calendar days used as a proxy for trading days in decay calculations
# (conservative — some decay windows use calendar days for simplicity)
_CANDLESTICK_WINDOW_DAYS = 7   # ~5 trading days
_STRUCTURAL_WINDOW_DAYS = 28   # ~20 trading days
_DIVERGENCE_WINDOW_DAYS = 42   # ~30 trading days
_CROSSOVER_WINDOW_DAYS = 14    # ~10 trading days
_GAP_WINDOW_DAYS = 14          # ~10 trading days

# Structural pattern base scores (per unit of strength)
_STRUCTURAL_BASE = {
    "double_bottom": 25,
    "double_top": 25,
    "breakout": 20,
    "breakdown": 20,
    "bull_flag": 20,
    "bear_flag": 20,
    "false_breakout": 15,
}

# Gap type base scores (per unit of strength / multiplier)
_GAP_BASE = {
    "breakaway": 60,
    "continuation": 40,
    "exhaustion": 30,
    "common": 10,
}

# Crossover base scores (before direction and decay)
_CROSSOVER_BASE = {
    "ema_9_21": 40,
    "ema_21_50": 50,
    "macd_signal": 45,
}

# Fibonacci level scores
_FIB_LEVEL_SCORES = {
    61.8: 40,
    78.6: 35,
    50.0: 30,
    38.2: 25,
    23.6: 15,
}


def _parse_date(date_str: str) -> date:
    """Parse a YYYY-MM-DD string to a date object."""
    return date.fromisoformat(date_str)


def _days_between(scoring_date: str, event_date: str) -> int:
    """Return the number of calendar days between event_date and scoring_date."""
    return (_parse_date(scoring_date) - _parse_date(event_date)).days


def _recency_decay(days_ago: int, window: int) -> float:
    """
    Compute recency decay multiplier.

    Returns 1.0 if days_ago=0, 0.0 if days_ago >= window.
    Linear decay between 0 and window.
    """
    if days_ago >= window:
        return 0.0
    return 1.0 - days_ago / window


def _direction_multiplier(direction: str) -> int:
    """Return +1 for bullish, -1 for bearish, 0 for neutral."""
    direction_lower = direction.lower() if direction else ""
    if direction_lower == "bullish":
        return 1
    if direction_lower == "bearish":
        return -1
    return 0


def score_candlestick_patterns(patterns: list[dict], scoring_date: str) -> float:
    """
    Score candlestick patterns within the last 5 trading days.

    For each candlestick pattern:
      base_score = strength × 20 × direction_multiplier
      decayed_score = base_score × (1 - days_ago / 5)

    Parameters:
        patterns: List of pattern dicts with keys: date, pattern_category,
                  direction, strength, pattern_name.
        scoring_date: Reference date (YYYY-MM-DD) for recency decay.

    Returns:
        Float score clamped to [-100, +100]. Returns 0 if no patterns.
    """
    total = 0.0
    for p in patterns:
        if p.get("pattern_category") != "candlestick":
            continue
        days_ago = _days_between(scoring_date, p["date"])
        decay = _recency_decay(days_ago, _CANDLESTICK_WINDOW_DAYS)
        if decay <= 0:
            continue
        direction = _direction_multiplier(p.get("direction", ""))
        strength = p.get("strength", 1) or 1
        base = strength * 20 * direction
        total += base * decay

    return max(-100.0, min(100.0, total))


def score_structural_patterns(patterns: list[dict], scoring_date: str) -> float:
    """
    Score structural chart patterns within the last 20 trading days.

    Pattern-specific base scores per unit of strength:
      double_top/bottom: 25, breakout/breakdown: 20, bull/bear_flag: 20, false_breakout: 15

    Parameters:
        patterns: List of pattern dicts with keys: date, pattern_category,
                  pattern_name, direction, strength.
        scoring_date: Reference date for recency decay.

    Returns:
        Float score clamped to [-100, +100].
    """
    total = 0.0
    for p in patterns:
        if p.get("pattern_category") != "structural":
            continue
        days_ago = _days_between(scoring_date, p["date"])
        decay = _recency_decay(days_ago, _STRUCTURAL_WINDOW_DAYS)
        if decay <= 0:
            continue
        pattern_name = p.get("pattern_name", "")
        base_per_unit = _STRUCTURAL_BASE.get(pattern_name, 15)
        strength = p.get("strength", 1) or 1
        direction = _direction_multiplier(p.get("direction", ""))
        base = strength * base_per_unit * direction
        total += base * decay

    return max(-100.0, min(100.0, total))


def score_divergences(divergences: list[dict], scoring_date: str) -> float:
    """
    Score technical divergences within the last 30 trading days.

    Regular divergences: strength × 20 per divergence
    Hidden divergences:  strength × 15 per divergence

    Parameters:
        divergences: List of divergence dicts with keys: date, divergence_type,
                     strength. divergence_type format: "regular_bullish",
                     "regular_bearish", "hidden_bullish", "hidden_bearish".
        scoring_date: Reference date for recency decay.

    Returns:
        Float score clamped to [-100, +100].
    """
    total = 0.0
    for div in divergences:
        days_ago = _days_between(scoring_date, div["date"])
        decay = _recency_decay(days_ago, _DIVERGENCE_WINDOW_DAYS)
        if decay <= 0:
            continue
        div_type = (div.get("divergence_type") or "").lower()
        strength = div.get("strength", 1) or 1

        if "bullish" in div_type:
            direction = 1
        elif "bearish" in div_type:
            direction = -1
        else:
            continue

        if div_type.startswith("regular"):
            base_per_unit = 20
        elif div_type.startswith("hidden"):
            base_per_unit = 15
        else:
            base_per_unit = 15

        total += strength * base_per_unit * direction * decay

    return max(-100.0, min(100.0, total))


def score_crossovers(crossovers: list[dict], scoring_date: str) -> float:
    """
    Score EMA and MACD crossovers within the last 10 trading days.

    Base scores by type: EMA 9/21=40, EMA 21/50=50, MACD signal=45
    Recency decay over 10 trading days.

    Parameters:
        crossovers: List of crossover dicts with keys: date, crossover_type,
                    direction, days_ago.
        scoring_date: Reference date for recency decay.

    Returns:
        Float score clamped to [-100, +100].
    """
    total = 0.0
    for co in crossovers:
        days_ago = _days_between(scoring_date, co["date"])
        decay = _recency_decay(days_ago, _CROSSOVER_WINDOW_DAYS)
        if decay <= 0:
            continue
        crossover_type = co.get("crossover_type", "")
        base = _CROSSOVER_BASE.get(crossover_type, 30)
        direction = _direction_multiplier(co.get("direction", ""))
        total += base * direction * decay

    return max(-100.0, min(100.0, total))


def score_gaps(gaps: list[dict], scoring_date: str) -> float:
    """
    Score price gaps within the last 10 trading days.

    Gap type base scores: breakaway=60, continuation=40, exhaustion=30, common=10
    Exhaustion gaps signal reversal (up exhaustion → bearish, flip direction).
    Recency decay over 10 trading days.

    Parameters:
        gaps: List of gap dicts with keys: date, gap_type, direction.
        scoring_date: Reference date for recency decay.

    Returns:
        Float score clamped to [-100, +100].
    """
    total = 0.0
    for gap in gaps:
        days_ago = _days_between(scoring_date, gap["date"])
        decay = _recency_decay(days_ago, _GAP_WINDOW_DAYS)
        if decay <= 0:
            continue
        gap_type = (gap.get("gap_type") or "").lower()
        direction_str = (gap.get("direction") or "").lower()
        direction = 1 if direction_str == "up" else -1
        base = _GAP_BASE.get(gap_type, 10)

        # Exhaustion gaps signal reversal — flip the direction
        if gap_type == "exhaustion":
            direction = -direction

        total += base * direction * decay

    return max(-100.0, min(100.0, total))


def score_fibonacci(fib_result: Optional[dict]) -> float:
    """
    Score Fibonacci proximity to a retracement level.

    Accepts both the real output of compute_fibonacci_for_ticker() and a
    simplified test-friendly dict. Key lookup order:

      is_near:    "is_near_level" (calculator output) or "near_level" (legacy)
      level_pct:  fib_result["nearest_level"]["level_pct"] (calculator output)
                  or fib_result["level_pct"] (legacy)
      direction:  if explicit "direction" key present, use it; otherwise derived:
                  current_price >= level_price → "support" (bullish),
                  current_price <  level_price → "resistance" (bearish)

    Level scores: 61.8% = ±40, 78.6% = ±35, 50.0% = ±30, 38.2% = ±25, 23.6% = ±15

    Parameters:
        fib_result: Dict from compute_fibonacci_for_ticker() or a simplified
                    test dict. Returns 0 if None or not near a level.

    Returns:
        Float score between -100 and +100. Positive for support, negative for resistance.
    """
    if not fib_result:
        return 0.0

    # Resolve is_near — support both key names
    is_near = fib_result.get("is_near_level") or fib_result.get("near_level")
    if not is_near:
        return 0.0

    # Resolve level_pct — nested in nearest_level (calculator) or top-level (legacy)
    nearest_level: Optional[dict] = fib_result.get("nearest_level")
    if nearest_level is not None:
        level_pct: float = nearest_level.get("level_pct", 0.0)
        level_price: Optional[float] = nearest_level.get("level_price")
    else:
        level_pct = fib_result.get("level_pct", 0.0)
        level_price = None

    # Resolve direction
    if "direction" in fib_result:
        direction: str = fib_result["direction"]
    else:
        # Derive direction from position within the swing range:
        #   Price above midpoint → retracing from swing_high → fib level is support → bullish
        #   Price below midpoint → recovering from swing_low → fib level is resistance → bearish
        current_price_val: Optional[float] = fib_result.get("current_price")
        swing_low_data = fib_result.get("swing_low")
        swing_high_data = fib_result.get("swing_high")

        if (current_price_val is not None
                and isinstance(swing_low_data, dict)
                and isinstance(swing_high_data, dict)):
            swing_low_price = swing_low_data.get("price", 0.0)
            swing_high_price = swing_high_data.get("price", 0.0)
            midpoint = (swing_low_price + swing_high_price) / 2
            direction = "support" if current_price_val >= midpoint else "resistance"
        else:
            direction = "support"  # conservative default

    # Find the matching Fibonacci level score
    best_score = 0
    best_diff = float("inf")
    for fib_level, fib_score in _FIB_LEVEL_SCORES.items():
        diff = abs(level_pct - fib_level)
        if diff < best_diff:
            best_diff = diff
            best_score = fib_score

    # Support zone = bullish (positive), resistance zone = bearish (negative)
    if direction == "resistance":
        best_score = -best_score

    return float(best_score)


def score_short_interest(days_to_cover: Optional[float]) -> float:
    """
    Score short interest using days-to-cover ratio.

    High short interest = bearish consensus (or squeeze potential, still bearish-flagged).
      days_to_cover > 7:   -50 to -80
      days_to_cover 4-7:   -20 to -50
      days_to_cover 2-4:   -10 to -20
      days_to_cover < 2:   0 (normal)

    Parameters:
        days_to_cover: Ratio of shares short to average daily volume. None → 0.

    Returns:
        Float score between -80 and 0.
    """
    if days_to_cover is None:
        return 0.0
    if days_to_cover > 7:
        t = min(1.0, (days_to_cover - 7) / 5)
        return -(50.0 + t * 30.0)
    if days_to_cover >= 4:
        t = (days_to_cover - 4) / 3
        return -(20.0 + t * 30.0)
    if days_to_cover >= 2:
        t = (days_to_cover - 2) / 2
        return -(10.0 + t * 10.0)
    return 0.0


def score_news_sentiment(
    avg_sentiment: Optional[float],
    article_count: int,
    filing_flag: bool,
) -> float:
    """
    Score news sentiment for a ticker.

    Maps avg_sentiment (-1 to +1) to score range (-80 to +80).
    Boosts score by 20% when article_count > 10 (more data = more confidence).
    Adds ±10 if a recent SEC filing was detected (uncertainty factor).

    Parameters:
        avg_sentiment: Average sentiment score (-1 to +1), or None if no articles.
        article_count: Number of articles in the sentiment calculation.
        filing_flag: True if a recent 8-K or material filing was detected.

    Returns:
        Float score clamped to [-100, +100]. Returns 0 if no sentiment data.
    """
    if avg_sentiment is None or article_count == 0:
        return 0.0

    score = avg_sentiment * 80.0

    if article_count > 10:
        score *= 1.2

    if filing_flag:
        score += 10.0 if avg_sentiment >= 0 else -10.0

    return max(-100.0, min(100.0, score))


def score_fundamentals(fundamentals: Optional[dict]) -> float:
    """
    Score fundamental metrics for a ticker.

    Components:
      - P/E vs sector median: undervalued → positive, overvalued → negative
      - EPS growth YoY: positive growth → positive score
      - Revenue growth YoY: positive growth → positive score
      - Debt-to-equity: low → positive (healthy), high → negative

    Parameters:
        fundamentals: Dict with keys pe_ratio, sector_pe_median, eps_growth_yoy,
                      revenue_growth_yoy, debt_to_equity. Returns 0 if None.

    Returns:
        Float score (average of components) clamped to [-100, +100].
    """
    if fundamentals is None:
        return 0.0

    component_scores: list[float] = []

    # P/E vs sector median
    pe = fundamentals.get("pe_ratio")
    sector_pe = fundamentals.get("sector_pe_median")
    if pe is not None and sector_pe is not None and sector_pe > 0 and pe > 0:
        ratio = pe / sector_pe
        pe_score = (1.0 - ratio) * 50.0  # undervalued (ratio < 1) → positive
        component_scores.append(max(-100.0, min(100.0, pe_score)))

    # EPS growth
    eps_growth = fundamentals.get("eps_growth_yoy")
    if eps_growth is not None:
        eps_score = max(-100.0, min(100.0, eps_growth * 200.0))
        component_scores.append(eps_score)

    # Revenue growth
    rev_growth = fundamentals.get("revenue_growth_yoy")
    if rev_growth is not None:
        rev_score = max(-100.0, min(100.0, rev_growth * 200.0))
        component_scores.append(rev_score)

    # Debt-to-equity
    de = fundamentals.get("debt_to_equity")
    if de is not None:
        if de < 0.5:
            de_score = 50.0
        elif de < 1.0:
            de_score = 20.0
        elif de < 2.0:
            de_score = -20.0
        else:
            de_score = -50.0
        component_scores.append(de_score)

    if not component_scores:
        return 0.0

    avg = sum(component_scores) / len(component_scores)
    return max(-100.0, min(100.0, avg))


def score_macro(
    spy_trend: float,
    vix_score: float,
    sector_etf_trend: float,
    treasury_trend: float,
    rs_market: Optional[float],
    rs_sector: Optional[float],
) -> float:
    """
    Combine macro inputs into a single macro score.

    Components:
      - spy_trend: Positive SPY trend → positive score
      - vix_score: High VIX → fear → negative; low VIX → positive (passed in as a score already)
      - sector_etf_trend: Positive trend → positive
      - treasury_trend: Rising yields (positive value) → slightly negative
      - rs_market: > 0 = outperforming market → positive
      - rs_sector: > 0 = outperforming sector → positive

    Parameters:
        spy_trend: Score for SPY trend direction (-100 to +100).
        vix_score: Score for VIX level (-100 to +100; negative = fear).
        sector_etf_trend: Score for sector ETF trend (-100 to +100).
        treasury_trend: Score for treasury yield trend (rising = negative for equities).
        rs_market: Relative strength vs market score (-100 to +100), or None.
        rs_sector: Relative strength vs sector score (-100 to +100), or None.

    Returns:
        Float average score clamped to [-100, +100].
    """
    components = [spy_trend, vix_score, sector_etf_trend, -treasury_trend]
    if rs_market is not None:
        components.append(rs_market)
    if rs_sector is not None:
        components.append(rs_sector)

    avg = sum(components) / len(components)
    return max(-100.0, min(100.0, avg))
