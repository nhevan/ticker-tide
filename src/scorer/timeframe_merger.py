"""
Triple timeframe confirmation.

Merges daily, weekly, and monthly scores using configurable weights:
  Final Score = (Daily × w_d) + (Weekly × w_w) + (Monthly × w_m)

Weights are regime-adaptive. If a timeframe is unavailable, its weight
is redistributed proportionally across the remaining timeframes.
Falls back to 100% daily score if only daily is available.

Weekly and monthly composites can be computed in two modes (controlled by
config['weekly_score_method'] and config['monthly_score_method']):

  v1_4cat (default): 4-category breakdown (trend, momentum, volume, volatility)
                     using indicators only. Existing behaviour, unchanged.
  v2_8cat:           6-applicable-category breakdown for weekly/monthly. Adds
                     candlestick + structural categories from patterns_*, and
                     mirrors daily's wiring of crossovers→trend and
                     divergences→momentum/volume. Monthly candlestick is always
                     None (decay window mismatch — see compute_monthly_score_breakdown).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# Lookback windows (in days) when loading events from weekly/monthly mirror
# tables for v2 scoring. Mirror the daily load helpers in src/scorer/main.py.
_WEEKLY_PATTERNS_LOOKBACK_DAYS = 60     # ~8 weeks; structural decay = 28d
_WEEKLY_DIVERGENCES_LOOKBACK_DAYS = 84  # ~12 weeks; divergence decay = 42d
_WEEKLY_CROSSOVERS_LOOKBACK_DAYS = 28   # ~4 weeks; crossover decay = 14d

_MONTHLY_PATTERNS_LOOKBACK_DAYS = 90    # ~3 months; structural decay = 28d (still useful)
_MONTHLY_DIVERGENCES_LOOKBACK_DAYS = 120  # ~4 months
_MONTHLY_CROSSOVERS_LOOKBACK_DAYS = 60  # ~2 months

# Track whether we've already emitted the per-timeframe-profile-empty fallback
# notice. We log it once per process via a module-level set keyed by ticker.
_PROFILE_FALLBACK_LOGGED: set[tuple[str, str]] = set()


# ---------------------------------------------------------------------------
# merge_timeframes
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Event loaders for v2 mode
# ---------------------------------------------------------------------------

def _load_weekly_events_for_scoring(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> dict:
    """
    Load patterns, divergences, and crossovers from the weekly mirror tables
    for v2 weekly scoring.

    The pattern_scorer functions read ``p["date"]`` for recency decay; the
    weekly mirror tables store the bar timestamp in ``week_start``. We alias
    ``week_start AS date`` in every SELECT so the scorers receive a "date"
    column without needing per-timeframe code paths.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        scoring_date: Reference date (YYYY-MM-DD).

    Returns:
        Dict with keys "patterns", "divergences", "crossovers", each mapping
        to a list of row dicts. Each dict contains a "date" key (aliased
        from week_start).
    """
    patterns_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_WEEKLY_PATTERNS_LOOKBACK_DAYS)
    ).isoformat()
    div_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_WEEKLY_DIVERGENCES_LOOKBACK_DAYS)
    ).isoformat()
    co_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_WEEKLY_CROSSOVERS_LOOKBACK_DAYS)
    ).isoformat()

    pat_rows = db_conn.execute(
        "SELECT *, week_start AS date FROM patterns_weekly "
        "WHERE ticker = ? AND week_start >= ? AND week_start <= ? "
        "ORDER BY week_start DESC",
        (ticker, patterns_cutoff, scoring_date),
    ).fetchall()
    div_rows = db_conn.execute(
        "SELECT *, week_start AS date FROM divergences_weekly "
        "WHERE ticker = ? AND week_start >= ? AND week_start <= ? "
        "ORDER BY week_start DESC",
        (ticker, div_cutoff, scoring_date),
    ).fetchall()
    co_rows = db_conn.execute(
        "SELECT *, week_start AS date FROM crossovers_weekly "
        "WHERE ticker = ? AND week_start >= ? AND week_start <= ? "
        "ORDER BY week_start DESC",
        (ticker, co_cutoff, scoring_date),
    ).fetchall()
    return {
        "patterns": [dict(r) for r in pat_rows],
        "divergences": [dict(r) for r in div_rows],
        "crossovers": [dict(r) for r in co_rows],
    }


def _load_monthly_events_for_scoring(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> dict:
    """
    Load patterns, divergences, and crossovers from the monthly mirror tables.

    Aliases ``month_start AS date`` so the pattern_scorer functions can read
    the recency-decay date without timeframe-aware branches.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        scoring_date: Reference date (YYYY-MM-DD).

    Returns:
        Dict with keys "patterns", "divergences", "crossovers".
    """
    patterns_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_MONTHLY_PATTERNS_LOOKBACK_DAYS)
    ).isoformat()
    div_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_MONTHLY_DIVERGENCES_LOOKBACK_DAYS)
    ).isoformat()
    co_cutoff = (
        date.fromisoformat(scoring_date) - timedelta(days=_MONTHLY_CROSSOVERS_LOOKBACK_DAYS)
    ).isoformat()

    pat_rows = db_conn.execute(
        "SELECT *, month_start AS date FROM patterns_monthly "
        "WHERE ticker = ? AND month_start >= ? AND month_start <= ? "
        "ORDER BY month_start DESC",
        (ticker, patterns_cutoff, scoring_date),
    ).fetchall()
    div_rows = db_conn.execute(
        "SELECT *, month_start AS date FROM divergences_monthly "
        "WHERE ticker = ? AND month_start >= ? AND month_start <= ? "
        "ORDER BY month_start DESC",
        (ticker, div_cutoff, scoring_date),
    ).fetchall()
    co_rows = db_conn.execute(
        "SELECT *, month_start AS date FROM crossovers_monthly "
        "WHERE ticker = ? AND month_start >= ? AND month_start <= ? "
        "ORDER BY month_start DESC",
        (ticker, co_cutoff, scoring_date),
    ).fetchall()
    return {
        "patterns": [dict(r) for r in pat_rows],
        "divergences": [dict(r) for r in div_rows],
        "crossovers": [dict(r) for r in co_rows],
    }


# ---------------------------------------------------------------------------
# Pattern-score aggregation for v2 mode
# ---------------------------------------------------------------------------

def _score_events_for_v2(
    events: dict,
    scoring_date: str,
    *,
    skip_candlestick: bool,
) -> dict:
    """
    Run the same pattern_scorer primitives daily uses, but on weekly/monthly
    event rows.

    Parameters:
        events:           Output of ``_load_weekly_events_for_scoring`` or its
                          monthly equivalent — keys "patterns", "divergences",
                          "crossovers".
        scoring_date:     Reference date for recency decay.
        skip_candlestick: If True, candlestick scoring is skipped and the
                          returned dict has ``candlestick_pattern_score=None``.
                          Used for monthly (decay window incompatible with
                          monthly bar cadence — see compute_monthly_score_breakdown).

    Returns:
        Dict in the same shape as the daily ``pattern_scores`` dict, restricted
        to the keys relevant to weekly/monthly scoring (no gaps_score,
        fibonacci_score: no weekly/monthly equivalents wired in commit 5).
    """
    from src.scorer.pattern_scorer import (
        score_candlestick_patterns,
        score_crossovers,
        score_divergences,
        score_structural_patterns,
    )

    patterns = events["patterns"]
    divergences = events["divergences"]
    crossovers = events["crossovers"]

    if skip_candlestick:
        candlestick_score: Optional[float] = None
    else:
        candlestick_score = score_candlestick_patterns(patterns, scoring_date)
    structural_score = score_structural_patterns(patterns, scoring_date)

    div_rsi = score_divergences(
        [d for d in divergences if d.get("indicator") == "rsi_14"], scoring_date
    )
    div_macd = score_divergences(
        [d for d in divergences if d.get("indicator") == "macd_histogram"], scoring_date
    )
    div_stoch = score_divergences(
        [d for d in divergences if d.get("indicator") == "stoch_k"], scoring_date
    )
    div_obv = score_divergences(
        [d for d in divergences if d.get("indicator") == "obv"], scoring_date
    )

    crossover_ema_9_21 = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "ema_9_21"], scoring_date
    )
    crossover_ema_21_50 = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "ema_21_50"], scoring_date
    )
    crossover_macd = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "macd_signal"], scoring_date
    )

    return {
        "candlestick_pattern_score": candlestick_score,
        "structural_pattern_score": structural_score,
        "divergence_rsi": div_rsi,
        "divergence_macd": div_macd,
        "divergence_stoch": div_stoch,
        "divergence_obv": div_obv,
        "crossover_ema_9_21": crossover_ema_9_21,
        "crossover_ema_21_50": crossover_ema_21_50,
        "crossover_macd_signal": crossover_macd,
    }


# ---------------------------------------------------------------------------
# Category rollup helpers
# ---------------------------------------------------------------------------

def _compute_v1_categories(indicator_scores: dict, prefix: str) -> dict:
    """
    Build the v1 (4-category) breakdown for a given timeframe.

    Mirrors the existing weekly/monthly behaviour: indicator scores only,
    no patterns/divergences/crossovers.

    Parameters:
        indicator_scores: Output of ``score_all_indicators``.
        prefix:           Logging-friendly prefix (e.g. "weekly", "monthly").

    Returns:
        Dict with 4 keys: trend, momentum, volume, volatility.
    """
    from src.scorer.category_scorer import rollup_category

    trend_score = rollup_category(f"{prefix}_trend", {
        "ema_alignment": indicator_scores.get("ema_alignment"),
        "macd_line": indicator_scores.get("macd_line"),
        "macd_histogram": indicator_scores.get("macd_histogram"),
        "adx": indicator_scores.get("adx"),
    })
    momentum_score = rollup_category(f"{prefix}_momentum", {
        "rsi_14": indicator_scores.get("rsi_14"),
        "stoch_k": indicator_scores.get("stoch_k"),
        "cci_20": indicator_scores.get("cci_20"),
        "williams_r": indicator_scores.get("williams_r"),
    })
    volume_score = rollup_category(f"{prefix}_volume", {
        "obv": indicator_scores.get("obv"),
        "cmf_20": indicator_scores.get("cmf_20"),
        "ad_line": indicator_scores.get("ad_line"),
    })
    volatility_score = rollup_category(f"{prefix}_volatility", {
        "bb_pctb": indicator_scores.get("bb_pctb"),
        "atr_14": indicator_scores.get("atr_14"),
    })
    return {
        "trend": trend_score,
        "momentum": momentum_score,
        "volume": volume_score,
        "volatility": volatility_score,
    }


def _compute_v2_categories(
    indicator_scores: dict,
    pattern_scores: dict,
    prefix: str,
) -> dict:
    """
    Build the v2 (6-applicable-category) breakdown for a given timeframe.

    Mirrors daily's category wiring (see ``compute_all_category_scores``):
    crossovers → trend, divergences → momentum/volume, candlestick → its own
    category, structural → its own category. Sentiment, fundamental, macro
    are still NOT included — those have no per-timeframe data sources.

    Parameters:
        indicator_scores: Output of ``score_all_indicators``.
        pattern_scores:   Output of ``_score_events_for_v2``. May contain
                          ``candlestick_pattern_score=None`` (monthly).
        prefix:           Logging-friendly prefix.

    Returns:
        Dict with 6 keys: trend, momentum, volume, volatility, candlestick,
        structural. ``candlestick`` is None when pattern_scores'
        candlestick_pattern_score is None (monthly path); otherwise it's a
        rolled-up float.
    """
    from src.scorer.category_scorer import rollup_category

    trend_score = rollup_category(f"{prefix}_trend", {
        "ema_alignment": indicator_scores.get("ema_alignment"),
        "macd_line": indicator_scores.get("macd_line"),
        "macd_histogram": indicator_scores.get("macd_histogram"),
        "adx": indicator_scores.get("adx"),
        "crossover_ema_9_21": pattern_scores.get("crossover_ema_9_21"),
        "crossover_ema_21_50": pattern_scores.get("crossover_ema_21_50"),
        "crossover_macd": pattern_scores.get("crossover_macd_signal"),
    })
    momentum_score = rollup_category(f"{prefix}_momentum", {
        "rsi_14": indicator_scores.get("rsi_14"),
        "stoch_k": indicator_scores.get("stoch_k"),
        "cci_20": indicator_scores.get("cci_20"),
        "williams_r": indicator_scores.get("williams_r"),
        "divergence_rsi": pattern_scores.get("divergence_rsi"),
        "divergence_macd": pattern_scores.get("divergence_macd"),
        "divergence_stoch": pattern_scores.get("divergence_stoch"),
    })
    volume_score = rollup_category(f"{prefix}_volume", {
        "obv": indicator_scores.get("obv"),
        "cmf_20": indicator_scores.get("cmf_20"),
        "ad_line": indicator_scores.get("ad_line"),
        "divergence_obv": pattern_scores.get("divergence_obv"),
    })
    volatility_score = rollup_category(f"{prefix}_volatility", {
        "bb_pctb": indicator_scores.get("bb_pctb"),
        "atr_14": indicator_scores.get("atr_14"),
    })

    cdl_raw = pattern_scores.get("candlestick_pattern_score")
    if cdl_raw is None:
        candlestick_score: Optional[float] = None
    else:
        candlestick_score = rollup_category(
            f"{prefix}_candlestick",
            {"candlestick_pattern_score": cdl_raw},
        )
    structural_score = rollup_category(
        f"{prefix}_structural",
        {"structural_pattern_score": pattern_scores.get("structural_pattern_score")},
    )
    return {
        "trend": trend_score,
        "momentum": momentum_score,
        "volume": volume_score,
        "volatility": volatility_score,
        "candlestick": candlestick_score,
        "structural": structural_score,
    }


# ---------------------------------------------------------------------------
# Weight resolution
# ---------------------------------------------------------------------------

def _resolve_v1_weights(config: dict, regime: str, timeframe: str) -> dict:
    """
    Resolve the v1 4-category weights for a given timeframe and regime.

    Falls back to renormalized daily ``adaptive_weights`` (4-category subset)
    when the per-timeframe weights are missing, then to a uniform 0.25/each
    if everything is missing.

    Parameters:
        config:    Scorer config dict.
        regime:    Market regime.
        timeframe: "weekly" or "monthly".

    Returns:
        Dict with keys trend, momentum, volume, volatility (sum ≈ 1.0).
    """
    cfg_key = f"{timeframe}_adaptive_weights"
    weights_cfg = config.get(cfg_key, {})
    regime_weights = weights_cfg.get(regime, {})
    if regime_weights:
        return regime_weights

    daily_weights_cfg = config.get("adaptive_weights", {})
    daily_regime = daily_weights_cfg.get(regime, {})
    applicable = {
        key: daily_regime.get(key, 0.0)
        for key in ("trend", "momentum", "volume", "volatility")
    }
    total = sum(applicable.values())
    if total > 0:
        return {key: val / total for key, val in applicable.items()}
    return {"trend": 0.25, "momentum": 0.25, "volume": 0.25, "volatility": 0.25}


def _resolve_v2_weights(config: dict, regime: str, timeframe: str) -> dict:
    """
    Resolve the v2 6-category weights (or fall back to v1 + zero cdl/struct).

    Parameters:
        config:    Scorer config dict.
        regime:    Market regime.
        timeframe: "weekly" or "monthly".

    Returns:
        Dict with 6 keys: trend, momentum, volume, volatility, candlestick,
        structural.
    """
    cfg_key = f"{timeframe}_adaptive_weights_v2"
    weights_cfg = config.get(cfg_key, {})
    regime_weights = weights_cfg.get(regime, {})
    if regime_weights:
        return regime_weights

    # Fallback: v1 weights with cdl/struct = 0
    v1 = _resolve_v1_weights(config, regime, timeframe)
    return {**v1, "candlestick": 0.0, "structural": 0.0}


# ---------------------------------------------------------------------------
# Profile loading with per-timeframe table + daily fallback
# ---------------------------------------------------------------------------

def _load_profiles_with_fallback(
    db_conn: sqlite3.Connection,
    ticker: str,
    timeframe: str,
) -> dict:
    """
    Load profiles from the timeframe-specific table; fall back to daily if empty.

    Logs an INFO once per (ticker, timeframe) when the per-timeframe profile
    table is empty and the daily fallback is used.

    Parameters:
        db_conn:   Open SQLite connection with row_factory=sqlite3.Row.
        ticker:    Ticker symbol.
        timeframe: "weekly" or "monthly".

    Returns:
        Dict mapping indicator_name → profile dict (may be empty if neither
        per-timeframe nor daily profiles exist).
    """
    from src.scorer.indicator_scorer import load_profile_for_ticker

    timeframe_table = f"indicator_profiles_{timeframe}"
    try:
        profiles = load_profile_for_ticker(db_conn, ticker, source_table=timeframe_table)
    except sqlite3.OperationalError as exc:
        # Table doesn't exist yet (e.g. very old DBs pre-migration, or test
        # fixtures that only create the daily profile table). Treat as empty
        # and fall back to daily.
        logger.debug(f"{ticker}: {timeframe_table} unavailable ({exc}); using daily")
        profiles = {}
    if profiles:
        return profiles

    fallback_key = (ticker, timeframe)
    if fallback_key not in _PROFILE_FALLBACK_LOGGED:
        logger.info(
            f"{ticker}: {timeframe_table} empty, falling back to daily proxy"
        )
        _PROFILE_FALLBACK_LOGGED.add(fallback_key)
    try:
        return load_profile_for_ticker(db_conn, ticker, source_table="indicator_profiles")
    except sqlite3.OperationalError:
        return {}


# ---------------------------------------------------------------------------
# Composite assembly
# ---------------------------------------------------------------------------

def _assemble_composite(
    category_scores: dict,
    config: dict,
    regime: str,
    timeframe: str,
    method: str,
) -> float:
    """
    Apply adaptive weights and the expansion factor to produce a composite score.

    None-valued categories (e.g. monthly candlestick in v2) are treated as 0
    by ``apply_adaptive_weights`` (they're absent from category_scores when
    we filter them out before this call, or zero-weighted when present).

    Parameters:
        category_scores: Dict of category name → score (None values stripped).
        config:          Scorer config dict.
        regime:          Market regime.
        timeframe:       "weekly" or "monthly".
        method:          "v1_4cat" or "v2_8cat".

    Returns:
        Float composite score clamped to [-100, +100].
    """
    from src.scorer.category_scorer import apply_adaptive_weights

    if method == "v2_8cat":
        regime_weights = _resolve_v2_weights(config, regime, timeframe)
    else:
        regime_weights = _resolve_v1_weights(config, regime, timeframe)

    expansion_factor = config.get("scoring", {}).get("score_expansion_factor", 1.0)
    # Strip None-valued categories before the weighted sum (apply_adaptive_weights
    # would treat them as 0 anyway, but explicit is clearer).
    safe_scores = {k: (v if v is not None else 0.0) for k, v in category_scores.items()}
    return apply_adaptive_weights(safe_scores, regime_weights, expansion_factor)


# ---------------------------------------------------------------------------
# Public breakdown functions
# ---------------------------------------------------------------------------

def compute_weekly_score_breakdown(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[dict]:
    """
    Compute a per-category weekly score breakdown.

    Loads the most recent weekly indicators with ``week_start <= scoring_date``
    (no look-ahead); this includes the in-progress current week. Persistence
    of weekly scores (commit 6) filters to closed weeks only — this function
    is the live composite that feeds ``merge_timeframes`` and intentionally
    uses the partial bar.

    Mode is gated on ``config['weekly_score_method']``:
      - "v1_4cat" (default): 4 indicator-only categories. Returns
        ``candlestick_score=None``, ``structural_score=None``.
      - "v2_8cat": adds candlestick + structural categories from
        ``patterns_weekly``, plus crossovers→trend, divergences→momentum/volume
        (mirroring daily's category wiring).

    Profiles come from ``indicator_profiles_weekly``; falls back to daily
    profiles if the weekly table is empty (logs INFO once per ticker).

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        config:       Scorer config dict.
        scoring_date: Reference date (YYYY-MM-DD).
        regime:       Market regime — "trending", "ranging", or "volatile".

    Returns:
        Dict with keys: composite_score, trend_score, momentum_score,
        volume_score, volatility_score, candlestick_score, structural_score.
        candlestick_score and structural_score are None in v1 mode.
        Returns None when no weekly indicator data is available.
    """
    from src.scorer.indicator_scorer import score_all_indicators

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
    profiles = _load_profiles_with_fallback(db_conn, ticker, "weekly")

    indicator_scores = score_all_indicators(
        indicators=indicators,
        close=close,
        profiles=profiles,
        config=config,
        regime=regime,
    )

    method = config.get("weekly_score_method", "v1_4cat")
    if method == "v2_8cat":
        events = _load_weekly_events_for_scoring(db_conn, ticker, scoring_date)
        pattern_scores = _score_events_for_v2(events, scoring_date, skip_candlestick=False)
        category_scores = _compute_v2_categories(indicator_scores, pattern_scores, "weekly")
    else:
        category_scores = _compute_v1_categories(indicator_scores, "weekly")

    # If every category came back zero (no usable indicators or events), bail.
    numeric_categories = [v for v in category_scores.values() if v is not None]
    if not numeric_categories or all(score == 0.0 for score in numeric_categories):
        logger.debug(f"{ticker}: no usable weekly signals")
        return None

    composite = _assemble_composite(category_scores, config, regime, "weekly", method)

    # In v1 mode, leave candlestick/structural as None for shape consistency.
    if method != "v2_8cat":
        return {
            "composite_score": composite,
            "trend_score": category_scores["trend"],
            "momentum_score": category_scores["momentum"],
            "volume_score": category_scores["volume"],
            "volatility_score": category_scores["volatility"],
            "candlestick_score": None,
            "structural_score": None,
        }
    return {
        "composite_score": composite,
        "trend_score": category_scores["trend"],
        "momentum_score": category_scores["momentum"],
        "volume_score": category_scores["volume"],
        "volatility_score": category_scores["volatility"],
        "candlestick_score": category_scores["candlestick"],
        "structural_score": category_scores["structural"],
    }


def compute_monthly_score_breakdown(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[dict]:
    """
    Compute a per-category monthly score breakdown.

    Same shape and gating as ``compute_weekly_score_breakdown``, applied to
    monthly bars. Loads the most recent monthly indicators with
    ``month_start <= scoring_date`` (no look-ahead); includes the in-progress
    current month. Persistence (commit 6) filters to closed months only.

    **Monthly candlestick is permanently disabled** (returns None even in v2
    mode). The candlestick decay window is 7 days; a single monthly bar is
    typically 0–30+ days behind ``scoring_date``, so candlestick patterns
    detected on the bar would mostly score 0 and the few non-zero results
    would alias on the timing of when scoring runs vs. month-end. Structural
    patterns (28-day window) and divergences (42-day window) are still
    accepted on monthly bars.

    Profiles come from ``indicator_profiles_monthly`` with daily fallback.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        config:       Scorer config dict.
        scoring_date: Reference date (YYYY-MM-DD).
        regime:       Market regime.

    Returns:
        Dict with the same 7 keys as the weekly breakdown.
        ``candlestick_score`` is always None.
        Returns None when no monthly indicator data is available.
    """
    from src.scorer.indicator_scorer import score_all_indicators

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
    profiles = _load_profiles_with_fallback(db_conn, ticker, "monthly")

    indicator_scores = score_all_indicators(
        indicators=indicators,
        close=close,
        profiles=profiles,
        config=config,
        regime=regime,
    )

    method = config.get("monthly_score_method", "v1_4cat")
    if method == "v2_8cat":
        events = _load_monthly_events_for_scoring(db_conn, ticker, scoring_date)
        # F3: monthly candlestick is permanently disabled.
        pattern_scores = _score_events_for_v2(events, scoring_date, skip_candlestick=True)
        category_scores = _compute_v2_categories(indicator_scores, pattern_scores, "monthly")
    else:
        category_scores = _compute_v1_categories(indicator_scores, "monthly")

    numeric_categories = [v for v in category_scores.values() if v is not None]
    if not numeric_categories or all(score == 0.0 for score in numeric_categories):
        logger.debug(f"{ticker}: no usable monthly signals")
        return None

    composite = _assemble_composite(category_scores, config, regime, "monthly", method)

    if method != "v2_8cat":
        return {
            "composite_score": composite,
            "trend_score": category_scores["trend"],
            "momentum_score": category_scores["momentum"],
            "volume_score": category_scores["volume"],
            "volatility_score": category_scores["volatility"],
            "candlestick_score": None,
            "structural_score": None,
        }
    return {
        "composite_score": composite,
        "trend_score": category_scores["trend"],
        "momentum_score": category_scores["momentum"],
        "volume_score": category_scores["volume"],
        "volatility_score": category_scores["volatility"],
        "candlestick_score": None,  # F3: always None on monthly
        "structural_score": category_scores["structural"],
    }


# ---------------------------------------------------------------------------
# Thin scalar shims (back-compat with existing call sites)
# ---------------------------------------------------------------------------

def compute_weekly_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[float]:
    """
    Compute a composite weekly score (scalar shim).

    Wraps ``compute_weekly_score_breakdown`` and returns just the
    composite_score field. Existing call sites (``src/scorer/main.py``)
    continue to use this scalar API unchanged.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        config:       Scorer config dict.
        scoring_date: Reference date (YYYY-MM-DD).
        regime:       Market regime.

    Returns:
        Float composite score (-100 to +100), or None if no weekly data.
    """
    breakdown = compute_weekly_score_breakdown(
        db_conn, ticker, config, scoring_date=scoring_date, regime=regime
    )
    return breakdown["composite_score"] if breakdown is not None else None


def compute_monthly_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    scoring_date: str,
    regime: str = "ranging",
) -> Optional[float]:
    """
    Compute a composite monthly score (scalar shim).

    Wraps ``compute_monthly_score_breakdown`` and returns just the
    composite_score field.

    Parameters:
        db_conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol.
        config:       Scorer config dict.
        scoring_date: Reference date (YYYY-MM-DD).
        regime:       Market regime.

    Returns:
        Float composite score (-100 to +100), or None if no monthly data.
    """
    breakdown = compute_monthly_score_breakdown(
        db_conn, ticker, config, scoring_date=scoring_date, regime=regime
    )
    return breakdown["composite_score"] if breakdown is not None else None
