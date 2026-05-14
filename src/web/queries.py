"""
Database query layer for the web UI.

Provides read-only queries for snapshot data (daily/weekly/monthly scores,
indicators, patterns, sparkline), active ticker lists, and date ranges.
All queries use parameterized SQL and return plain dicts or lists.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Any, Optional

# Imported directly from the scorer to keep the matrix's recency window
# in lockstep with score_candlestick_patterns / score_structural_patterns.
# Private-prefixed names are accepted here as the canonical source of truth.
from src.scorer.calibrator import (
    DEFAULT_MIN_TRAINING_SAMPLES,
    DEFAULT_RIDGE_LAMBDA,
    FEATURE_METADATA,
    FEATURE_NAMES,
    build_shrinkage_lambdas,
    compute_shrinkage_path,
    fetch_training_data,
)
from src.common.config import get_training_excluded_tickers
from src.scorer.pattern_scorer import _CANDLESTICK_WINDOW_DAYS, _STRUCTURAL_WINDOW_DAYS
from src.scorer.zone_labels import zone_label_for_adx, zone_label_for_cci, zone_label_for_rsi, zone_label_for_stoch_k

logger = logging.getLogger(__name__)

# ── Category arrays (contract for UI rendering) ───────────────────────────────
_DAILY_CATEGORIES = [
    "trend", "momentum", "volume", "volatility",
    "candlestick", "structural", "sentiment", "fundamental", "macro",
]
_WEEKLY_CATEGORIES = [
    "trend", "momentum", "volume", "volatility", "candlestick", "structural",
]
# Monthly deliberately omits candlestick (decay-window mismatch — see DESIGN.md §12)
_MONTHLY_CATEGORIES = [
    "trend", "momentum", "volume", "volatility", "structural",
]

# ── Recent-patterns helper constants ─────────────────────────────────────────
_ALLOWED_PATTERN_TABLES: set[str] = {"patterns_daily", "patterns_weekly", "patterns_monthly"}
_ALLOWED_PERIOD_COLUMNS: set[str] = {"date", "week_start", "month_start"}
_WINDOW_BY_CATEGORY: dict[str, int] = {
    "candlestick": _CANDLESTICK_WINDOW_DAYS,
    "structural": _STRUCTURAL_WINDOW_DAYS,
}


def fetch_shrinkage_path(
    conn: sqlite3.Connection,
    scoring_date: Optional[str],
    scorer_config: dict,
) -> dict:
    """
    Build the ridge regression shrinkage path payload for the given scoring date.

    Fetches training data for the date (or the latest scored date when scoring_date
    is None), computes the shrinkage path across DEFAULT_SHRINKAGE_LAMBDAS, and
    returns a response dict the frontend can render directly.

    Cold-start conditions (returns cold_start=True, no lambdas/features keys):
      - scores_daily table is empty (no MAX(date) to resolve).
      - Training data has fewer rows than min_training_samples from the calibration
        sub-config.

    Parameters:
        conn:          Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date:  ISO date string (YYYY-MM-DD) or None to use latest date.
        scorer_config: Full scorer config dict. Calibration settings are read
                       from scorer_config["calibration"].

    Returns:
        Dict with keys:
            cold_start (bool)
            scoring_date (str | None)
            production_lambda (float)
            training_samples (int)
            lambdas (list[float])      — present only when cold_start is False
            features (list[dict])      — present only when cold_start is False
                Each feature dict has: name, label, category, coefs (list[float])
    """
    calibration_cfg = scorer_config.get("calibration", {})
    ridge_lambda: float = calibration_cfg.get("ridge_lambda", DEFAULT_RIDGE_LAMBDA)
    min_training_samples: int = calibration_cfg.get(
        "min_training_samples", DEFAULT_MIN_TRAINING_SAMPLES
    )
    lambdas = build_shrinkage_lambdas(ridge_lambda)

    resolved_date: Optional[str] = scoring_date
    if resolved_date is None:
        row = conn.execute("SELECT MAX(date) AS max_date FROM scores_daily").fetchone()
        resolved_date = row["max_date"] if row else None

    cold_start_base = {
        "cold_start": True,
        "scoring_date": resolved_date,
        "training_samples": 0,
        "production_lambda": ridge_lambda,
    }

    if resolved_date is None:
        return cold_start_base

    excluded_tickers = get_training_excluded_tickers()
    X_train, y_train = fetch_training_data(
        conn, resolved_date, calibration_cfg, excluded_tickers=excluded_tickers
    )
    n_samples = X_train.shape[0]

    if n_samples < min_training_samples:
        return {**cold_start_base, "training_samples": n_samples, "scoring_date": resolved_date}

    path = compute_shrinkage_path(X_train, y_train, lambdas)

    features = [
        {
            "name": name,
            "label": FEATURE_METADATA[name]["label"],
            "category": FEATURE_METADATA[name]["category"],
            "coefs": path[:, idx].tolist(),
        }
        for idx, name in enumerate(FEATURE_NAMES)
    ]

    return {
        "cold_start": False,
        "scoring_date": resolved_date,
        "production_lambda": ridge_lambda,
        "training_samples": n_samples,
        "lambdas": lambdas,
        "features": features,
    }


def fetch_active_tickers(conn: sqlite3.Connection) -> list[str]:
    """
    Return an alphabetized list of active ticker symbols from the tickers table.

    Filters by active=1 (truthy) and sorts alphabetically. ETFs that appear in
    the active tickers list (e.g. QQQ, VOO, DIA) are included. Benchmark-only
    tickers without active=1 (e.g. SPY stored as benchmark only) are excluded.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.

    Returns:
        Sorted list of active ticker symbol strings.
    """
    rows = conn.execute(
        "SELECT symbol FROM tickers WHERE active = 1 ORDER BY symbol ASC"
    ).fetchall()
    return [row["symbol"] for row in rows]


def fetch_date_range(conn: sqlite3.Connection, ticker: str) -> dict[str, Optional[str]]:
    """
    Return the min and max dates available in scores_daily for a ticker.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').

    Returns:
        Dict with keys 'min' and 'max', each a date string (YYYY-MM-DD) or None
        if no data exists for the ticker.
    """
    row = conn.execute(
        "SELECT MIN(date) AS min_date, MAX(date) AS max_date "
        "FROM scores_daily WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    if row is None:
        return {"min": None, "max": None}
    return {"min": row["min_date"], "max": row["max_date"]}


def fetch_tickers_list(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Return one row per active ticker with the latest snapshot fields needed
    by the Tickers listing page.

    Joins:
        - tickers (active = 1) — symbol, name, sector, market_cap source.
        - Latest scores_daily row per ticker via ROW_NUMBER() window.
        - Latest ohlcv_daily row per ticker for the close price.
        - Latest fundamentals row per ticker for pe_ratio. The fundamentals
          table stores quarterly rows (period in {'Q1','Q2','Q3','Q4'});
          we pick the most recent (report_date DESC, fetched_at DESC,
          period DESC) tuple per ticker to make the choice deterministic
          when multiple rows share a report_date.

    Tickers with no scores_daily row are excluded by the INNER JOIN — a
    ticker with no signal data is not useful in a signal listing.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.

    Returns:
        List of dicts (snake_case keys), one per active scored ticker, sorted
        by symbol ascending. Each dict has these keys:
            symbol, name, sector, market_cap, price,
            signal, confidence, final_score, regime,
            daily_score, weekly_score, monthly_score,
            pe_ratio, latest_date.
        `latest_date` is the YYYY-MM-DD of the latest scores_daily row used
        for the snapshot — surfaced so the listing page can deep-link to the
        Ticker Detail page at the correct date.
        Nullable fields may be None when source data is missing.
    """
    query = """
        WITH latest_scores AS (
            SELECT
                ticker, date, signal, confidence, final_score, regime,
                daily_score, weekly_score, monthly_score,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM scores_daily
        ),
        latest_close AS (
            SELECT
                ticker, close,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM ohlcv_daily
        ),
        latest_fundamentals AS (
            SELECT
                ticker, pe_ratio,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker
                    ORDER BY report_date DESC, fetched_at DESC, period DESC
                ) AS rn
            FROM fundamentals
        )
        SELECT
            t.symbol           AS symbol,
            t.name             AS name,
            t.sector           AS sector,
            t.market_cap       AS market_cap,
            lc.close           AS price,
            ls.signal          AS signal,
            ls.confidence      AS confidence,
            ls.final_score     AS final_score,
            ls.regime          AS regime,
            ls.daily_score     AS daily_score,
            ls.weekly_score    AS weekly_score,
            ls.monthly_score   AS monthly_score,
            lf.pe_ratio        AS pe_ratio,
            ls.date            AS latest_date
        FROM tickers t
        INNER JOIN latest_scores ls
            ON ls.ticker = t.symbol AND ls.rn = 1
        LEFT JOIN latest_close lc
            ON lc.ticker = t.symbol AND lc.rn = 1
        LEFT JOIN latest_fundamentals lf
            ON lf.ticker = t.symbol AND lf.rn = 1
        WHERE t.active = 1
        ORDER BY t.symbol ASC
    """
    rows = conn.execute(query).fetchall()
    return [dict(row) for row in rows]


def fetch_snapshot(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    config: dict,
    scorer_config: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Build the full three-card snapshot dict for a ticker and picked date.

    Resolves daily, weekly, and monthly data independently. Each section
    includes data_available, categories (the UI rendering contract), scores,
    indicators, patterns, recent_patterns, sparkline, and period metadata.

    For daily: exact match on scores_daily.date.
    For weekly: most-recent scores_weekly.week_start <= picked_date.
    For monthly: most-recent scores_monthly.month_start <= picked_date.

    Section shape (all three timeframes):
        patterns: list[dict]  — exact-date / exact-period patterns (existing field,
                               consumed by PatternsList.tsx; unchanged).
        recent_patterns: list[dict]  — patterns within the scorer's canonical recency
                               window, with tone metadata. Daily rows include a
                               'days_ago' int key; weekly/monthly rows do not.

    The daily section also includes RSI explainer fields:
        regime: str | None — market regime from scores_daily.
        rsi_profile: dict | None — percentile profile for rsi_14 from indicator_profiles.
        rsi_zone_label: str | None — zone label from zone_label_for_rsi().
        contributions_payload: dict | None — parsed key_signals_data JSON, or None for
                               legacy rows where that column is NULL.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        picked_date: ISO date string (YYYY-MM-DD) selected by the user.
        config: Web config dict containing the 'sparkline' section and
                'pattern_row_limit'.
        scorer_config: Scorer config dict used for RSI zone label computation.
                       When None, an empty dict is used (fallback thresholds apply).

    Returns:
        Dict with keys 'daily', 'weekly', 'monthly', each a section dict.
    """
    sparkline_cfg = config.get("sparkline", {})
    daily_days = sparkline_cfg.get("daily_days", 15)
    weekly_weeks = sparkline_cfg.get("weekly_weeks", 6)
    monthly_months = sparkline_cfg.get("monthly_months", 6)
    rsi_sparkline_days = sparkline_cfg.get("rsi_sparkline_days", 100)
    macd_sparkline_days = sparkline_cfg.get("macd_sparkline_days", 100)
    stoch_sparkline_days = sparkline_cfg.get("stoch_sparkline_days", 100)
    adx_sparkline_days = sparkline_cfg.get("adx_sparkline_days", 100)
    cci_sparkline_days = sparkline_cfg.get("cci_sparkline_days", 100)

    why_limit = config.get("why_bullets", {}).get("limit", 3)
    signal_flip_lookback = config.get("signal_flip_lookback_days", 14)
    pattern_row_limit = int(config.get("pattern_row_limit", 5))
    resolved_scorer_config = scorer_config if scorer_config is not None else {}

    return {
        "daily": _build_daily_section(
            conn, ticker, picked_date, daily_days,
            why_limit=why_limit,
            signal_flip_lookback_days=signal_flip_lookback,
            pattern_row_limit=pattern_row_limit,
            scorer_config=resolved_scorer_config,
            rsi_sparkline_days=rsi_sparkline_days,
            macd_sparkline_days=macd_sparkline_days,
            stoch_sparkline_days=stoch_sparkline_days,
            adx_sparkline_days=adx_sparkline_days,
            cci_sparkline_days=cci_sparkline_days,
        ),
        "weekly": _build_weekly_section(
            conn, ticker, picked_date, weekly_weeks,
            pattern_row_limit=pattern_row_limit,
        ),
        "monthly": _build_monthly_section(
            conn, ticker, picked_date, monthly_months,
            pattern_row_limit=pattern_row_limit,
        ),
    }


def _parse_calibrator_payload(raw: Optional[str]) -> Optional[dict]:
    """
    Parse a calibrator payload from its stored JSON string representation.

    Treats SQL NULL (None) and malformed JSON as a valid absent-payload state,
    returning None without raising. This is consistent with the convention that
    the column is nullable — rows written before this column was added will have
    NULL, and callers should render graceful fallbacks.

    Parameters:
        raw: The raw TEXT value from a calibrator_payload column, or None.

    Returns:
        Parsed dict, or None when raw is None or JSON is malformed.
    """
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Malformed calibrator_payload JSON — returning None: %s",
            exc,
        )
        return None


def _parse_contributions_payload(raw: Optional[str]) -> Optional[dict]:
    """
    Parse a contributions payload from its stored JSON string representation.

    Treats SQL NULL (None) and malformed JSON as a valid absent-payload state,
    returning None without raising. This is consistent with the convention that
    the column is nullable — legacy rows written before key_signals_data was
    added will have NULL, and callers should render graceful fallbacks.

    Parameters:
        raw: The raw TEXT value from a key_signals_data column, or None.

    Returns:
        Parsed dict, or None when raw is None or JSON is malformed.
    """
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Malformed contributions_payload JSON in key_signals_data — returning None: %s",
            exc,
        )
        return None


def _build_daily_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_days: int,
    why_limit: int = 3,
    signal_flip_lookback_days: int = 14,
    pattern_row_limit: int = 5,
    scorer_config: Optional[dict] = None,
    rsi_sparkline_days: int = 100,
    macd_sparkline_days: int = 100,
    stoch_sparkline_days: int = 100,
    adx_sparkline_days: int = 100,
    cci_sparkline_days: int = 100,
) -> dict[str, Any]:
    """
    Build the daily card data for a ticker and exact date.

    Returns a dict with data_available=False if no row exists for that date.
    When available, includes all 9 categories, scores, indicators, patterns,
    recent_patterns, sparkline, signal, confidence, calibrated_score,
    resolved_period, key_signals (top N why-bullets), earnings
    (next + last_surprise), signal_flip (most recent flip within the
    lookback window), and RSI explainer fields.

    RSI explainer fields added:
        regime: market regime string from scores_daily, or None.
        rsi_profile: dict with p5/p20/p50/p80/p95/mean/std for rsi_14 from
                     indicator_profiles, or None if no profile exists.
        rsi_zone_label: zone label string from zone_label_for_rsi(), or None
                        if rsi_14 is not available.
        contributions_payload: parsed key_signals_data JSON dict, or None for
                               legacy rows where that column is NULL.

    ADX explainer fields added:
        adx_sparkline: list of {date, adx} dicts from _fetch_adx_sparkline().
                       Always present when data_available is True; empty list
                       when no qualifying rows exist; never None.
        adx_zone_label: zone label string from zone_label_for_adx(), or None
                        when adx is not available in indicators_daily for
                        the picked date.

    Note: adx_profile is intentionally NOT included. ADX is in
    PROFILE_FREE_INDICATORS — the indicator_profiles table stores a row for
    ADX but it is unused for scoring (score_adx() uses hardcoded literals).
    Exposing adx_profile on the snapshot would mislead callers into thinking
    the profile influences the score.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Exact date to look up (YYYY-MM-DD).
        sparkline_days: Number of trading days to include in the sparkline.
        why_limit: Maximum number of key_signals items to include (from config).
        signal_flip_lookback_days: Number of days to look back for a signal flip.
        pattern_row_limit: Max pattern rows returned per category for
            recent_patterns (from config["pattern_row_limit"]).
        scorer_config: Scorer config dict for RSI zone computation. When None,
                       an empty dict is used (zone_label_for_rsi uses fallback thresholds).
        rsi_sparkline_days: Number of trading days to include in the RSI sparkline
                            (bounded by <= picked_date). Rows with rsi_14 IS NULL are
                            excluded. Returns [] when no data exists.
        stoch_sparkline_days: Number of trading days to include in the Stochastic %K/%D
                              sparkline (bounded by <= picked_date). Rows with stoch_k IS NULL
                              are excluded. Returns [] when no data exists.
        adx_sparkline_days: Number of trading days to include in the ADX sparkline
                            (bounded by <= picked_date). Rows with adx IS NULL are
                            excluded. Returns [] when no data exists.
        cci_sparkline_days: Number of trading days to include in the CCI(20) sparkline
                            (bounded by <= picked_date). Rows with cci_20 IS NULL are
                            excluded. Returns [] when no data exists.

    Returns:
        Daily section dict. Includes rsi_sparkline, stoch_sparkline, adx_sparkline,
        cci_sparkline: list[dict[str, Any]] — always present (may be empty list), never None.
        Also includes stoch_k_profile (dict or None), stoch_zone_label (str or None),
        adx_zone_label (str or None), cci_20_profile (dict or None),
        cci_zone_label (str or None). adx_profile is deliberately absent — see note above.
    """
    resolved_scorer_config = scorer_config if scorer_config is not None else {}

    score_row = conn.execute(
        """
        SELECT sd.*, t.sector_etf
        FROM scores_daily sd
        LEFT JOIN tickers t ON sd.ticker = t.symbol
        WHERE sd.ticker = ? AND sd.date = ?
        """,
        (ticker, picked_date),
    ).fetchone()

    if score_row is None:
        return {
            "data_available": False,
            "categories": _DAILY_CATEGORIES,
            "resolved_period": picked_date,
        }

    score_dict = dict(score_row)
    indicators = _fetch_daily_indicators(conn, ticker, picked_date)
    patterns = _fetch_daily_patterns(conn, ticker, picked_date)
    recent_patterns = _fetch_recent_patterns(
        conn, ticker, picked_date,
        table_name="patterns_daily", period_column="date",
        allowed_categories=("candlestick", "structural"),
        top_n=pattern_row_limit, compute_days_ago=True,
    )
    sparkline = _fetch_daily_sparkline(conn, ticker, picked_date, sparkline_days)
    rsi_sparkline = _fetch_rsi_sparkline(conn, ticker, picked_date, rsi_sparkline_days)
    macd_sparkline = _fetch_macd_sparkline(conn, ticker, picked_date, macd_sparkline_days)
    stoch_sparkline = _fetch_stoch_sparkline(conn, ticker, picked_date, stoch_sparkline_days)
    adx_sparkline = _fetch_adx_sparkline(conn, ticker, picked_date, adx_sparkline_days)
    cci_sparkline = _fetch_cci_sparkline(conn, ticker, picked_date, cci_sparkline_days)
    key_signals = _extract_key_signals(score_dict, limit=why_limit)
    earnings = _fetch_earnings(conn, ticker, picked_date)
    signal_flip = _fetch_signal_flip(
        conn, ticker, picked_date, lookback_days=signal_flip_lookback_days
    )
    indicator_scores = _fetch_daily_indicator_scores(conn, ticker, picked_date)

    # RSI explainer fields.
    regime = score_dict.get("regime")
    rsi_profile = _fetch_rsi_profile(conn, ticker)
    macd_line_profile = _fetch_macd_line_profile(conn, ticker)
    rsi_value = indicators.get("rsi_14") if indicators else None
    rsi_zone_label: Optional[str]
    if rsi_value is not None:
        rsi_thresholds = resolved_scorer_config.get(
            "indicator_thresholds", {}
        ).get("rsi_14", {"oversold": 30.0, "overbought": 70.0})
        rsi_zone_label = zone_label_for_rsi(
            float(rsi_value), rsi_profile, rsi_thresholds
        )
    else:
        rsi_zone_label = None

    # Stoch %K explainer fields.
    stoch_k_profile = _fetch_stoch_k_profile(conn, ticker)
    stoch_k_value = indicators.get("stoch_k") if indicators else None
    stoch_zone_label: Optional[str]
    if stoch_k_value is not None:
        stoch_thresholds = resolved_scorer_config.get(
            "indicator_thresholds", {}
        ).get("stoch_k", {"oversold": 20.0, "overbought": 80.0})
        stoch_zone_label = zone_label_for_stoch_k(
            float(stoch_k_value), stoch_k_profile, stoch_thresholds
        )
    else:
        stoch_zone_label = None

    # ADX explainer fields. No profile fetch — ADX is in PROFILE_FREE_INDICATORS
    # and adx_profile is intentionally not exposed (see docstring).
    adx_value = indicators.get("adx") if indicators else None
    adx_zone_label: Optional[str]
    if adx_value is not None:
        adx_zone_label = zone_label_for_adx(float(adx_value))
    else:
        adx_zone_label = None

    # CCI(20) explainer fields.
    cci_20_profile = _fetch_cci_profile(conn, ticker)
    cci_value = indicators.get("cci_20") if indicators else None
    cci_zone_label: Optional[str]
    if cci_value is not None:
        cci_zone_label = zone_label_for_cci(float(cci_value), cci_20_profile)
    else:
        cci_zone_label = None

    # Parse contributions payload; treat NULL as None without raising.
    contributions_payload: Optional[dict] = _parse_contributions_payload(
        score_dict.get("key_signals_data")
    )

    # Parse calibrator payload; treat NULL as None without raising.
    calibrator_payload: Optional[dict] = _parse_calibrator_payload(
        score_dict.get("calibrator_payload")
    )

    return {
        "data_available": True,
        "categories": _DAILY_CATEGORIES,
        "scores": _extract_daily_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "recent_patterns": recent_patterns,
        "sparkline": sparkline,
        "rsi_sparkline": rsi_sparkline,
        "macd_sparkline": macd_sparkline,
        "stoch_sparkline": stoch_sparkline,
        "signal": score_dict.get("signal"),
        "confidence": score_dict.get("confidence"),
        "calibrated_score": score_dict.get("calibrated_score"),
        "composite_score": score_dict.get("final_score"),
        "daily_score": score_dict.get("daily_score"),
        "resolved_period": picked_date,
        "key_signals": key_signals,
        "earnings": earnings,
        "signal_flip": signal_flip,
        "indicator_scores": indicator_scores,
        "regime": regime,
        "rsi_profile": rsi_profile,
        "macd_line_profile": macd_line_profile,
        "rsi_zone_label": rsi_zone_label,
        "stoch_k_profile": stoch_k_profile,
        "stoch_zone_label": stoch_zone_label,
        "adx_sparkline": adx_sparkline,
        "adx_zone_label": adx_zone_label,
        "cci_sparkline": cci_sparkline,
        "cci_20_profile": cci_20_profile,
        "cci_zone_label": cci_zone_label,
        "contributions_payload": contributions_payload,
        "calibrator_payload": calibrator_payload,
        "raw_daily_score": score_dict.get("raw_daily_score"),
        "sector_etf_score": score_dict.get("sector_etf_score"),
        "sector_etf": score_dict.get("sector_etf"),
        "weekly_score": score_dict.get("weekly_score"),
        "confidence_modifiers": json.loads(score_dict["confidence_modifiers"]) if score_dict.get("confidence_modifiers") else None,
        "confidence_base": score_dict.get("confidence_base"),
    }


def _build_weekly_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_weeks: int,
    pattern_row_limit: int = 5,
) -> dict[str, Any]:
    """
    Build the weekly card data for a ticker as of the picked date.

    Resolves to the most recent week_start <= picked_date in scores_weekly.
    Sets is_fallback=True when resolved_period < picked_date.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        sparkline_weeks: Number of weekly bars to include in the sparkline.
        pattern_row_limit: Max pattern rows returned per category for
            recent_patterns (from config["pattern_row_limit"]).

    Returns:
        Weekly section dict.
    """
    score_row = conn.execute(
        "SELECT * FROM scores_weekly WHERE ticker = ? AND week_start <= ? "
        "ORDER BY week_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    if score_row is None:
        return {
            "data_available": False,
            "categories": _WEEKLY_CATEGORIES,
            "resolved_period": None,
            "resolved_period_label": None,
            "is_fallback": False,
        }

    score_dict = dict(score_row)
    week_start = score_dict["week_start"]
    is_fallback = week_start < picked_date

    indicators = _fetch_weekly_indicators(conn, ticker, week_start)
    patterns = _fetch_weekly_patterns(conn, ticker, week_start)
    recent_patterns = _fetch_recent_patterns(
        conn, ticker, week_start,  # CRITICAL: resolved period, NOT picked_date
        table_name="patterns_weekly", period_column="week_start",
        allowed_categories=("candlestick", "structural"),
        top_n=pattern_row_limit, compute_days_ago=False,
    )
    sparkline = _fetch_weekly_sparkline(conn, ticker, picked_date, sparkline_weeks)
    period_label = _format_weekly_period_label(week_start)
    indicator_scores = _fetch_weekly_indicator_scores(conn, ticker, week_start)
    contributions_payload: Optional[dict] = _parse_contributions_payload(
        score_dict.get("key_signals_data")
    )

    return {
        "data_available": True,
        "categories": _WEEKLY_CATEGORIES,
        "scores": _extract_timeframe_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "recent_patterns": recent_patterns,
        "sparkline": sparkline,
        "composite_score": score_dict.get("composite_score"),
        "resolved_period": week_start,
        "resolved_period_label": period_label,
        "is_fallback": is_fallback,
        "indicator_scores": indicator_scores,
        "contributions_payload": contributions_payload,
    }


def _build_monthly_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_months: int,
    pattern_row_limit: int = 5,
) -> dict[str, Any]:
    """
    Build the monthly card data for a ticker as of the picked date.

    Resolves to the most recent month_start <= picked_date in scores_monthly.
    Sets is_fallback=True when resolved_period < picked_date.
    Candlestick is intentionally excluded from the categories array even though
    candlestick_score exists as a column (always NULL for monthly — decay mismatch).
    recent_patterns for monthly includes only structural patterns (candlestick
    excluded by design — decay-window mismatch).

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        sparkline_months: Number of monthly bars to include in the sparkline.
        pattern_row_limit: Max pattern rows returned per category for
            recent_patterns (from config["pattern_row_limit"]).

    Returns:
        Monthly section dict.
    """
    score_row = conn.execute(
        "SELECT * FROM scores_monthly WHERE ticker = ? AND month_start <= ? "
        "ORDER BY month_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    if score_row is None:
        return {
            "data_available": False,
            "categories": _MONTHLY_CATEGORIES,
            "resolved_period": None,
            "resolved_period_label": None,
            "is_fallback": False,
        }

    score_dict = dict(score_row)
    month_start = score_dict["month_start"]
    is_fallback = month_start < picked_date

    indicators = _fetch_monthly_indicators(conn, ticker, month_start)
    patterns = _fetch_monthly_patterns(conn, ticker, month_start)
    recent_patterns = _fetch_recent_patterns(
        conn, ticker, month_start,  # CRITICAL: resolved period, NOT picked_date
        table_name="patterns_monthly", period_column="month_start",
        allowed_categories=("structural",),  # monthly excludes candlestick
        top_n=pattern_row_limit, compute_days_ago=False,
    )
    sparkline = _fetch_monthly_sparkline(conn, ticker, picked_date, sparkline_months)
    period_label = _format_monthly_period_label(month_start)
    indicator_scores = _fetch_monthly_indicator_scores(conn, ticker, month_start)
    contributions_payload: Optional[dict] = _parse_contributions_payload(
        score_dict.get("key_signals_data")
    )

    return {
        "data_available": True,
        "categories": _MONTHLY_CATEGORIES,
        "scores": _extract_timeframe_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "recent_patterns": recent_patterns,
        "sparkline": sparkline,
        "composite_score": score_dict.get("composite_score"),
        "resolved_period": month_start,
        "resolved_period_label": period_label,
        "is_fallback": is_fallback,
        "indicator_scores": indicator_scores,
        "contributions_payload": contributions_payload,
    }


# ── Indicator scores sidecar fetch helpers ───────────────────────────────────

def _fetch_daily_indicator_scores(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
) -> dict[str, float | None]:
    """
    Fetch per-indicator signed scores from indicator_scores_daily for a ticker and date.

    Returns an empty dict if no rows exist or if the table does not yet exist
    (OperationalError path — handles databases that have not been migrated yet).

    Parameters:
        conn:     Open SQLite connection.
        ticker:   Ticker symbol.
        date_str: Exact date (YYYY-MM-DD).

    Returns:
        Dict mapping indicator_name to score (float or None).
    """
    try:
        rows = conn.execute(
            "SELECT indicator_name, score FROM indicator_scores_daily "
            "WHERE ticker = ? AND date = ?",
            (ticker, date_str),
        ).fetchall()
        return {row["indicator_name"]: row["score"] for row in rows}
    except sqlite3.OperationalError:
        return {}


def _fetch_rsi_profile(
    conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """
    Fetch the rsi_14 percentile profile from indicator_profiles for a ticker.

    Returns a dict with p5, p20, p50, p80, p95, mean, std keys, or None if
    no profile row exists for this ticker and indicator.

    Parameters:
        conn:   Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.

    Returns:
        Profile dict or None.
    """
    try:
        row = conn.execute(
            "SELECT p5, p20, p50, p80, p95, mean, std FROM indicator_profiles "
            "WHERE ticker = ? AND indicator = 'rsi_14'",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return {
        "p5": row["p5"],
        "p20": row["p20"],
        "p50": row["p50"],
        "p80": row["p80"],
        "p95": row["p95"],
        "mean": row["mean"],
        "std": row["std"],
    }


def _fetch_macd_line_profile(
    conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """
    Fetch the macd_line z-score profile from indicator_profiles for a ticker.

    MACD scoring uses z-score normalisation (mean + std), unlike RSI which
    uses percentiles. Returns a dict with mean + std keys, or None if no
    profile row exists for this ticker and indicator.

    Parameters:
        conn:   Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.

    Returns:
        Profile dict with mean and std, or None.
    """
    try:
        row = conn.execute(
            "SELECT mean, std FROM indicator_profiles "
            "WHERE ticker = ? AND indicator = 'macd_line'",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return {"mean": row["mean"], "std": row["std"]}


def _fetch_weekly_indicator_scores(
    conn: sqlite3.Connection,
    ticker: str,
    week_start: str,
) -> dict[str, float | None]:
    """
    Fetch per-indicator signed scores from indicator_scores_weekly for a ticker and week_start.

    Returns an empty dict if no rows exist or if the table does not yet exist.

    Parameters:
        conn:       Open SQLite connection.
        ticker:     Ticker symbol.
        week_start: Resolved week_start date string (YYYY-MM-DD).

    Returns:
        Dict mapping indicator_name to score (float or None).
    """
    try:
        rows = conn.execute(
            "SELECT indicator_name, score FROM indicator_scores_weekly "
            "WHERE ticker = ? AND week_start = ?",
            (ticker, week_start),
        ).fetchall()
        return {row["indicator_name"]: row["score"] for row in rows}
    except sqlite3.OperationalError:
        return {}


def _fetch_monthly_indicator_scores(
    conn: sqlite3.Connection,
    ticker: str,
    month_start: str,
) -> dict[str, float | None]:
    """
    Fetch per-indicator signed scores from indicator_scores_monthly for a ticker and month_start.

    Returns an empty dict if no rows exist or if the table does not yet exist.

    Parameters:
        conn:        Open SQLite connection.
        ticker:      Ticker symbol.
        month_start: Resolved month_start date string (YYYY-MM-DD).

    Returns:
        Dict mapping indicator_name to score (float or None).
    """
    try:
        rows = conn.execute(
            "SELECT indicator_name, score FROM indicator_scores_monthly "
            "WHERE ticker = ? AND month_start = ?",
            (ticker, month_start),
        ).fetchall()
        return {row["indicator_name"]: row["score"] for row in rows}
    except sqlite3.OperationalError:
        return {}


# ── Score extraction helpers ──────────────────────────────────────────────────

def _extract_daily_scores(score_dict: dict) -> dict[str, Any]:
    """
    Extract the category score values from a scores_daily row dict.

    Parameters:
        score_dict: Dict built from a scores_daily sqlite3.Row.

    Returns:
        Dict mapping category name to score value (float or None).
    """
    return {
        "trend": score_dict.get("trend_score"),
        "momentum": score_dict.get("momentum_score"),
        "volume": score_dict.get("volume_score"),
        "volatility": score_dict.get("volatility_score"),
        "candlestick": score_dict.get("candlestick_score"),
        "structural": score_dict.get("structural_score"),
        "sentiment": score_dict.get("sentiment_score"),
        "fundamental": score_dict.get("fundamental_score"),
        "macro": score_dict.get("macro_score"),
        "composite": score_dict.get("final_score"),
    }


def _extract_timeframe_scores(score_dict: dict) -> dict[str, Any]:
    """
    Extract the category score values from a scores_weekly or scores_monthly row dict.

    Includes candlestick_score in the dict even though it is NULL for monthly rows —
    the UI keys off the categories array, not the dict keys.

    Parameters:
        score_dict: Dict built from a scores_weekly or scores_monthly sqlite3.Row.

    Returns:
        Dict mapping category name to score value (float or None).
    """
    return {
        "trend": score_dict.get("trend_score"),
        "momentum": score_dict.get("momentum_score"),
        "volume": score_dict.get("volume_score"),
        "volatility": score_dict.get("volatility_score"),
        "candlestick": score_dict.get("candlestick_score"),
        "structural": score_dict.get("structural_score"),
        "fundamental": score_dict.get("fundamental_score"),
        "macro": score_dict.get("macro_score"),
        "composite": score_dict.get("composite_score"),
    }


# ── Indicator fetch helpers ───────────────────────────────────────────────────

def _fetch_daily_indicators(
    conn: sqlite3.Connection, ticker: str, date_str: str
) -> dict[str, Any]:
    """
    Fetch the indicators_daily row for a ticker and exact date.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        date_str: Exact date (YYYY-MM-DD).

    Returns:
        Dict of indicator values, or empty dict if no row found.
    """
    row = conn.execute(
        "SELECT * FROM indicators_daily WHERE ticker = ? AND date = ?",
        (ticker, date_str),
    ).fetchone()
    return dict(row) if row else {}


def _fetch_weekly_indicators(
    conn: sqlite3.Connection, ticker: str, week_start: str
) -> dict[str, Any]:
    """
    Fetch the indicators_weekly row for a ticker and resolved week_start.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        week_start: Resolved week_start date string (YYYY-MM-DD).

    Returns:
        Dict of indicator values, or empty dict if no row found.
    """
    row = conn.execute(
        "SELECT * FROM indicators_weekly WHERE ticker = ? AND week_start = ?",
        (ticker, week_start),
    ).fetchone()
    return dict(row) if row else {}


def _fetch_monthly_indicators(
    conn: sqlite3.Connection, ticker: str, month_start: str
) -> dict[str, Any]:
    """
    Fetch the indicators_monthly row for a ticker and resolved month_start.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        month_start: Resolved month_start date string (YYYY-MM-DD).

    Returns:
        Dict of indicator values, or empty dict if no row found.
    """
    row = conn.execute(
        "SELECT * FROM indicators_monthly WHERE ticker = ? AND month_start = ?",
        (ticker, month_start),
    ).fetchone()
    return dict(row) if row else {}


# ── Recent-patterns helper ───────────────────────────────────────────────────

def _fetch_recent_patterns(
    conn: sqlite3.Connection,
    ticker: str,
    period_date: str,
    table_name: str,
    period_column: str,
    allowed_categories: Sequence[str],
    top_n: int,
    compute_days_ago: bool,
) -> list[dict]:
    """
    Fetch recent pattern rows for the matrix table.

    For each category in allowed_categories that has an entry in
    _WINDOW_BY_CATEGORY, fetches up to top_n rows from table_name where
    period_column is within (period_date - window_days) and period_date,
    ordered by (period_column DESC, strength DESC). Unknown categories
    (no window defined) are silently skipped.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        period_date: ISO date string (YYYY-MM-DD) used as the upper bound
            and as the reference for days_ago computation. For weekly /
            monthly sections, callers MUST pass the resolved week_start /
            month_start, not the user's picked_date.
        table_name: Must be one of _ALLOWED_PATTERN_TABLES.
        period_column: Must be one of _ALLOWED_PERIOD_COLUMNS.
        allowed_categories: Sequence of pattern categories to fetch.
            Categories without a window in _WINDOW_BY_CATEGORY are skipped.
        top_n: Maximum rows returned per category.
        compute_days_ago: If True, each returned dict includes a
            "days_ago" int key (clamped to ≥0); if False, the key is omitted.

    Returns:
        List of pattern dicts. Empty list if no matches.

    Raises:
        ValueError: if table_name or period_column is not in the allowlist.
    """
    if table_name not in _ALLOWED_PATTERN_TABLES:
        raise ValueError(f"Invalid table_name: {table_name!r}")
    if period_column not in _ALLOWED_PERIOD_COLUMNS:
        raise ValueError(f"Invalid period_column: {period_column!r}")

    period_date_obj = date.fromisoformat(period_date)
    out: list[dict] = []
    for category in allowed_categories:
        window_days = _WINDOW_BY_CATEGORY.get(category)
        if window_days is None:
            continue
        min_date = (period_date_obj - timedelta(days=window_days)).isoformat()
        sql = (
            f"SELECT pattern_name, pattern_category, direction, strength, confirmed, "
            f"{period_column} AS period_date "
            f"FROM {table_name} "
            f"WHERE ticker = ? AND pattern_category = ? "
            f"AND {period_column} <= ? AND {period_column} >= ? "
            f"ORDER BY {period_column} DESC, strength DESC "
            f"LIMIT ?"
        )
        rows = conn.execute(
            sql, (ticker, category, period_date, min_date, top_n)
        ).fetchall()
        for row in rows:
            row_dict: dict = {
                "pattern_name": row["pattern_name"],
                "pattern_category": row["pattern_category"],
                "direction": row["direction"],
                "strength": row["strength"],
                "confirmed": bool(row["confirmed"]),
            }
            if compute_days_ago:
                row_period_obj = date.fromisoformat(row["period_date"])
                row_dict["days_ago"] = max(0, (period_date_obj - row_period_obj).days)
            out.append(row_dict)
    return out


# ── Pattern fetch helpers ─────────────────────────────────────────────────────

def _fetch_daily_patterns(
    conn: sqlite3.Connection, ticker: str, date_str: str
) -> list[dict[str, Any]]:
    """
    Fetch all patterns_daily rows for a ticker on a specific date.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        date_str: Exact date (YYYY-MM-DD).

    Returns:
        List of pattern dicts ordered by strength descending.
    """
    rows = conn.execute(
        "SELECT pattern_name, pattern_category, direction, strength, confirmed "
        "FROM patterns_daily WHERE ticker = ? AND date = ? "
        "ORDER BY strength DESC",
        (ticker, date_str),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_weekly_patterns(
    conn: sqlite3.Connection, ticker: str, week_start: str
) -> list[dict[str, Any]]:
    """
    Fetch all patterns_weekly rows for a ticker on a resolved week_start.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        week_start: Resolved week_start date string.

    Returns:
        List of pattern dicts ordered by strength descending.
    """
    rows = conn.execute(
        "SELECT pattern_name, pattern_category, direction, strength, confirmed "
        "FROM patterns_weekly WHERE ticker = ? AND week_start = ? "
        "ORDER BY strength DESC",
        (ticker, week_start),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_monthly_patterns(
    conn: sqlite3.Connection, ticker: str, month_start: str
) -> list[dict[str, Any]]:
    """
    Fetch all patterns_monthly rows for a ticker on a resolved month_start.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        month_start: Resolved month_start date string.

    Returns:
        List of pattern dicts ordered by strength descending.
    """
    rows = conn.execute(
        "SELECT pattern_name, pattern_category, direction, strength, confirmed "
        "FROM patterns_monthly WHERE ticker = ? AND month_start = ? "
        "ORDER BY strength DESC",
        (ticker, month_start),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Sparkline fetch helpers ───────────────────────────────────────────────────

def _fetch_daily_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days OHLCV rows for a ticker up to and including picked_date.

    Applies a strict <= picked_date bound so sparkline reflects "as of" the picked date.
    Returns rows in chronological (ascending) order.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD). No rows after this date are included.
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date, close.
    """
    rows = conn.execute(
        "SELECT date, close FROM ohlcv_daily "
        "WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    ).fetchall()
    return [{"date": r["date"], "close": r["close"]} for r in reversed(rows)]


def _fetch_weekly_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_weeks: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_weeks weekly_candles rows for a ticker with week_start <= picked_date.

    Returns rows in chronological (ascending) order.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        num_weeks: Maximum number of weekly bars to return.

    Returns:
        List of dicts with keys: date (week_start), close.
    """
    rows = conn.execute(
        "SELECT week_start, close FROM weekly_candles "
        "WHERE ticker = ? AND week_start <= ? "
        "ORDER BY week_start DESC LIMIT ?",
        (ticker, picked_date, num_weeks),
    ).fetchall()
    return [{"date": r["week_start"], "close": r["close"]} for r in reversed(rows)]


def _fetch_rsi_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days rsi_14 values from indicators_daily for a ticker up to
    and including picked_date.

    Applies a strict <= picked_date bound and excludes rows where rsi_14 IS NULL.
    Returns rows in chronological (ascending) order. Returns an empty list when no
    qualifying rows exist.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD). No rows after this date are included.
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date (str), value (float). Empty list if no data.
    """
    cur = conn.execute(
        "SELECT date, rsi_14 FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? AND rsi_14 IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    )
    rows = cur.fetchall()
    return [{"date": r["date"], "value": float(r["rsi_14"])} for r in reversed(rows)]


def _fetch_macd_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days MACD line / signal / histogram values from
    indicators_daily for a ticker up to and including picked_date.

    Excludes rows where macd_line IS NULL (insufficient bars for the 26-EMA);
    signal and histogram are independently nullable and are emitted as None
    when null.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date (str), macd_line (float),
        signal (float | None), histogram (float | None). Empty list if no data.
    """
    cur = conn.execute(
        "SELECT date, macd_line, macd_signal, macd_histogram FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? AND macd_line IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    )
    rows = cur.fetchall()
    return [
        {
            "date": r["date"],
            "macd_line": float(r["macd_line"]),
            "signal": float(r["macd_signal"]) if r["macd_signal"] is not None else None,
            "histogram": float(r["macd_histogram"]) if r["macd_histogram"] is not None else None,
        }
        for r in reversed(rows)
    ]


def _fetch_stoch_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days stoch_k / stoch_d values from indicators_daily for a ticker
    up to and including picked_date.

    Excludes rows where stoch_k IS NULL (insufficient bars for the Stochastic calculation).
    stoch_d is a 3-period SMA of stoch_k and may be null for the first rows after stoch_k
    becomes available (warm-up period). Such rows are kept in the result with stoch_d=None.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD). No rows after this date are included.
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date (str), stoch_k (float), stoch_d (float | None).
        Empty list if no qualifying rows exist.
    """
    cur = conn.execute(
        "SELECT date, stoch_k, stoch_d FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? AND stoch_k IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    )
    rows = cur.fetchall()
    return [
        {
            "date": r["date"],
            "stoch_k": float(r["stoch_k"]),
            "stoch_d": float(r["stoch_d"]) if r["stoch_d"] is not None else None,
        }
        for r in reversed(rows)
    ]


def _fetch_adx_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days ADX values from indicators_daily for a ticker up to
    and including picked_date.

    ADX is a single-series sparkline (unlike Stochastic's %K/%D dual series).
    Applies a strict <= picked_date bound and excludes rows where adx IS NULL.
    Returns rows in chronological (ascending) order. Returns an empty list when
    no qualifying rows exist.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD). No rows after this date are included.
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date (str), adx (float). Empty list if no data.
    """
    cur = conn.execute(
        "SELECT date, adx FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? AND adx IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    )
    rows = cur.fetchall()
    return [{"date": r["date"], "adx": float(r["adx"])} for r in reversed(rows)]


def _fetch_stoch_k_profile(
    conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """
    Fetch the stoch_k percentile profile from indicator_profiles for a ticker.

    Returns a dict with p5, p20, p50, p80, p95, mean, std keys, or None if
    no profile row exists for this ticker and indicator.

    Parameters:
        conn:   Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.

    Returns:
        Profile dict or None.
    """
    try:
        row = conn.execute(
            "SELECT p5, p20, p50, p80, p95, mean, std FROM indicator_profiles "
            "WHERE ticker = ? AND indicator = 'stoch_k'",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return {
        "p5": row["p5"],
        "p20": row["p20"],
        "p50": row["p50"],
        "p80": row["p80"],
        "p95": row["p95"],
        "mean": row["mean"],
        "std": row["std"],
    }


def _fetch_cci_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_days: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_days cci_20 values from indicators_daily for a ticker up to
    and including picked_date.

    Applies a strict <= picked_date bound and excludes rows where cci_20 IS NULL.
    Returns rows in chronological (ascending) order. Returns an empty list when no
    qualifying rows exist.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD). No rows after this date are included.
        num_days: Maximum number of rows to return.

    Returns:
        List of dicts with keys: date (str), cci (float). Empty list if no data.
    """
    cur = conn.execute(
        "SELECT date, cci_20 FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? AND cci_20 IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (ticker, picked_date, num_days),
    )
    rows = cur.fetchall()
    return [{"date": r["date"], "cci": float(r["cci_20"])} for r in reversed(rows)]


def _fetch_cci_profile(
    conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """
    Fetch the cci_20 percentile profile from indicator_profiles for a ticker.

    Looks up by ticker and indicator='cci_20'. Returns a dict with p5, p20,
    p50, p80, p95, mean, std keys, or None if no profile row exists.

    Parameters:
        conn:   Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.

    Returns:
        Profile dict or None.
    """
    try:
        row = conn.execute(
            "SELECT p5, p20, p50, p80, p95, mean, std FROM indicator_profiles "
            "WHERE ticker = ? AND indicator = 'cci_20'",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return {
        "p5": row["p5"],
        "p20": row["p20"],
        "p50": row["p50"],
        "p80": row["p80"],
        "p95": row["p95"],
        "mean": row["mean"],
        "std": row["std"],
    }


def _fetch_monthly_sparkline(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    num_months: int,
) -> list[dict[str, Any]]:
    """
    Fetch the last num_months monthly_candles rows for a ticker with month_start <= picked_date.

    Returns rows in chronological (ascending) order.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        num_months: Maximum number of monthly bars to return.

    Returns:
        List of dicts with keys: date (month_start), close.
    """
    rows = conn.execute(
        "SELECT month_start, close FROM monthly_candles "
        "WHERE ticker = ? AND month_start <= ? "
        "ORDER BY month_start DESC LIMIT ?",
        (ticker, picked_date, num_months),
    ).fetchall()
    return [{"date": r["month_start"], "close": r["close"]} for r in reversed(rows)]


# ── New enrichment helpers (daily-only) ──────────────────────────────────────

def _extract_key_signals(score_dict: dict, limit: int) -> list[str]:
    """
    Extract the top N items from the key_signals JSON column in a scores_daily row.

    Decodes the JSON-encoded string list stored in score_dict["key_signals"].
    Returns an empty list if the column is missing, None, contains invalid JSON,
    or parses to a non-list value.

    Parameters:
        score_dict: Dict built from a scores_daily sqlite3.Row.
        limit: Maximum number of items to return. Comes from config["why_bullets"]["limit"].

    Returns:
        List of up to `limit` signal description strings, or [] on any failure.
    """
    raw = score_dict.get("key_signals")
    if raw is None:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to parse key_signals JSON: {raw!r}")
        return []
    if not isinstance(parsed, list):
        logger.warning(f"key_signals parsed to non-list type {type(parsed).__name__!r}")
        return []
    return parsed[:limit]


def _fetch_earnings(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
) -> dict[str, Any]:
    """
    Fetch the next upcoming earnings and last earnings surprise for a ticker.

    Next earnings: first future row (earnings_date > picked_date) with actual_eps IS NULL.
    Last surprise: most recent past row (earnings_date <= picked_date) with actual_eps IS NOT NULL.

    Both subkeys may be None independently if no qualifying row exists.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: ISO date string (YYYY-MM-DD) used as the boundary for past/future.

    Returns:
        Dict with shape:
            {
              "next": {"date": str, "days_until": int, "estimated_eps": float | None} | None,
              "last_surprise": {
                  "date": str, "actual_eps": float, "surprise": float | None, "beat": bool | None
              } | None
            }
    """
    next_row = conn.execute(
        "SELECT earnings_date, estimated_eps "
        "FROM earnings_calendar "
        "WHERE ticker = ? AND earnings_date > ? AND actual_eps IS NULL "
        "ORDER BY earnings_date ASC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    last_row = conn.execute(
        "SELECT earnings_date, actual_eps, eps_surprise "
        "FROM earnings_calendar "
        "WHERE ticker = ? AND earnings_date <= ? AND actual_eps IS NOT NULL "
        "ORDER BY earnings_date DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    next_data: Optional[dict[str, Any]] = None
    if next_row is not None:
        earnings_date_str = next_row["earnings_date"]
        try:
            earnings_date_obj = datetime.strptime(earnings_date_str, "%Y-%m-%d").date()
            picked_date_obj = datetime.strptime(picked_date, "%Y-%m-%d").date()
            days_until = (earnings_date_obj - picked_date_obj).days
        except ValueError:
            logger.warning(
                f"Could not parse earnings_date {earnings_date_str!r} or picked_date {picked_date!r}"
            )
            days_until = None
        next_data = {
            "date": earnings_date_str,
            "days_until": days_until,
            "estimated_eps": next_row["estimated_eps"],
        }

    last_data: Optional[dict[str, Any]] = None
    if last_row is not None:
        eps_surprise = last_row["eps_surprise"]
        beat: Optional[bool] = None
        if eps_surprise is not None:
            beat = eps_surprise > 0
        last_data = {
            "date": last_row["earnings_date"],
            "actual_eps": last_row["actual_eps"],
            "surprise": eps_surprise,
            "beat": beat,
        }

    return {"next": next_data, "last_surprise": last_data}


def _fetch_signal_flip(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    lookback_days: int,
) -> Optional[dict[str, Any]]:
    """
    Fetch the most recent signal flip for a ticker within the lookback window.

    The lookback floor is picked_date - lookback_days (inclusive). When multiple rows
    exist on the same date (production duplicates / contradictions), the row with the
    highest id is selected (ORDER BY date DESC, id DESC).

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: ISO date string (YYYY-MM-DD) as the upper bound (inclusive).
        lookback_days: Number of days to look back from picked_date. Comes from
                       config["signal_flip_lookback_days"].

    Returns:
        Dict with keys date, previous_signal, new_signal, days_ago; or None if no
        qualifying row exists.
    """
    try:
        picked_date_obj = datetime.strptime(picked_date, "%Y-%m-%d").date()
        floor_date_obj = picked_date_obj - timedelta(days=lookback_days)
        floor_date_str = floor_date_obj.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Could not compute signal flip floor for picked_date {picked_date!r}")
        return None

    row = conn.execute(
        "SELECT date, previous_signal, new_signal "
        "FROM signal_flips "
        "WHERE ticker = ? AND date <= ? AND date >= ? "
        "ORDER BY date DESC, id DESC LIMIT 1",
        (ticker, picked_date, floor_date_str),
    ).fetchone()

    if row is None:
        return None

    flip_date_str = row["date"]
    try:
        flip_date_obj = datetime.strptime(flip_date_str, "%Y-%m-%d").date()
        days_ago = (picked_date_obj - flip_date_obj).days
    except ValueError:
        logger.warning(f"Could not parse signal_flip date {flip_date_str!r}")
        days_ago = None

    return {
        "date": flip_date_str,
        "previous_signal": row["previous_signal"],
        "new_signal": row["new_signal"],
        "days_ago": days_ago,
    }


# ── Period label helpers ──────────────────────────────────────────────────────

def _format_weekly_period_label(week_start: str) -> str:
    """
    Format a week_start date string into a human-readable weekly period label.

    The label shows the end of the week (week_start + 6 days) in 'Week ending Mon DD' format.

    Parameters:
        week_start: ISO date string for the start of the week (YYYY-MM-DD).

    Returns:
        Label string, e.g. 'Week ending Apr 25'.
    """
    try:
        start_date = datetime.strptime(week_start, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=6)
        return f"Week ending {end_date.strftime('%b %-d')}"
    except ValueError:
        logger.warning(f"Could not parse week_start date: {week_start!r}")
        return f"Week of {week_start}"


def _format_monthly_period_label(month_start: str) -> str:
    """
    Format a month_start date string into a human-readable monthly period label.

    Parameters:
        month_start: ISO date string for the start of the month (YYYY-MM-DD).

    Returns:
        Label string, e.g. 'Apr 2026'.
    """
    try:
        start_date = datetime.strptime(month_start, "%Y-%m-%d").date()
        return start_date.strftime("%b %Y")
    except ValueError:
        logger.warning(f"Could not parse month_start date: {month_start!r}")
        return month_start


# ── Price chart range defaults ────────────────────────────────────────────────
_PRICE_CHART_RANGE_DEFAULTS: dict[str, int] = {
    "1M": 22,
    "3M": 66,
    "6M": 132,
    "1Y": 252,
    "ALL": 5000,
}


def fetch_price_chart(
    conn: sqlite3.Connection,
    ticker: str,
    range_key: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """
    Fetch OHLCV bars for a candlestick price chart from ohlcv_daily.

    Rows are fetched in descending date order (LIMIT applied) then reversed
    to ascending for the response. Bars with any null value in open/high/low/close
    are dropped with a WARNING. Bars with null volume are kept with volume
    substituted to 0 and a WARNING logged.

    Parameters:
        conn:      Open SQLite connection with row_factory=sqlite3.Row.
        ticker:    Ticker symbol (e.g. "AAPL"). Should already be uppercased.
        range_key: One of "1M", "3M", "6M", "1Y", "ALL". Raises ValueError for
                   any other value.
        config:    Web config dict. Reads config["price_chart"]["range_days"][range_key]
                   for the bar limit; falls back to built-in defaults when the
                   price_chart block is absent.

    Returns:
        Dict with keys:
            ticker (str)
            range  (str)
            bars   (list[dict]) — ascending by date; each bar has:
                       date (str), open (float), high (float), low (float),
                       close (float), volume (int)

    Raises:
        ValueError: when range_key is not one of the five recognised values.
    """
    if range_key not in _PRICE_CHART_RANGE_DEFAULTS:
        raise ValueError(f"Unknown range: {range_key}")

    configured = (
        config
        .get("price_chart", {})
        .get("range_days", {})
        .get(range_key, _PRICE_CHART_RANGE_DEFAULTS[range_key])
    )
    try:
        num_days = int(configured)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid price_chart.range_days[{range_key}] = {configured!r}; "
            "expected positive integer."
        ) from exc
    if num_days <= 0:
        raise ValueError(
            f"Invalid price_chart.range_days[{range_key}] = {num_days}; "
            "expected positive integer."
        )

    rows = conn.execute(
        "SELECT date, open, high, low, close, volume "
        "FROM ohlcv_daily "
        "WHERE ticker = ? "
        "ORDER BY date DESC "
        "LIMIT ?",
        (ticker, num_days),
    ).fetchall()

    # Reverse to ascending order
    rows = list(reversed(rows))

    bars: list[dict[str, Any]] = []
    for row in rows:
        row_date = row["date"]
        null_ohlc_cols = [
            col for col in ("open", "high", "low", "close") if row[col] is None
        ]
        if null_ohlc_cols:
            logger.warning(
                f"fetch_price_chart: dropping bar for {ticker} on {row_date} — "
                f"null OHLC columns: {null_ohlc_cols}"
            )
            continue

        volume: int = row["volume"]
        if volume is None:
            logger.warning(
                f"fetch_price_chart: null volume for {ticker} on {row_date} — substituting 0"
            )
            volume = 0

        bars.append({
            "date": row_date,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(volume),
        })

    return {"ticker": ticker, "range": range_key, "bars": bars}
