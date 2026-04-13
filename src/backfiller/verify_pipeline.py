"""
Pipeline data verification.

Validates all COMPUTED data (indicators, scores, patterns, profiles)
for mathematical consistency and range validity.

Companion to verify.py (which checks RAW data only).

Run after:
- Config/threshold changes
- Adding new indicators or patterns
- Modifying scoring logic
- Weekly health check

Checks performed:
1.  Indicator value ranges (RSI 0-100, ADX 0-100, etc.)
2.  Indicator coverage (all active tickers have data)
3.  Indicator-OHLCV date alignment
4.  Indicator NULL percentage (warn if > threshold)
5.  Score ranges (-100 to +100)
6.  Category score ranges (all 9 category scores)
7.  Confidence range (0–100%)
8.  Signal-score consistency (BULLISH matches positive score)
9.  Signal distribution (not all same signal)
10. Confidence distribution (not all 0%)
11. Weighted score math verification (0.2×daily + 0.8×weekly ≈ final)
12. Regime validity (trending/ranging/volatile only)
13. JSON field validity (data_completeness, key_signals)
14. Pattern counts and validity
15. Pattern duplicates
16. Pattern field validity (direction, strength, category)
17. Divergence counts
18. Divergence consistency (type + indicator + swing values)
19. Crossover validity (type, direction, days_ago)
20. Profile coverage (all tickers have profiles)
21. Profile percentile ordering (p5 < p20 < p50 < p80 < p95)
22. Profile freshness (window_end not stale)
23. Weekly candle validity (OHLC consistency, volume)
24. Weekly indicator coverage
25. News summary count consistency
26. Cross-table: scores have indicators
27. Cross-table: indicators have OHLCV
28. S/R levels within historical price range
29. Signal flip validity
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from src.backfiller.verify import CheckResult, VerificationReport
from src.common.config import get_active_tickers, load_config
from src.common.db import get_connection

logger = logging.getLogger(__name__)

_TELEGRAM_MAX_LENGTH = 4096
_MAX_DETAIL_ITEMS = 20
_STATUS_EMOJI = {"pass": "✅", "warn": "⚠️", "fail": "❌"}

# ---------------------------------------------------------------------------
# Indicator range definitions
# ---------------------------------------------------------------------------

INDICATOR_RANGES: dict[str, dict] = {
    "rsi_14":         {"min": 0,    "max": 100,  "critical": True},
    "adx":            {"min": 0,    "max": 100,  "critical": True},
    "stoch_k":        {"min": 0,    "max": 100,  "critical": True},
    "stoch_d":        {"min": 0,    "max": 100,  "critical": True},
    "williams_r":     {"min": -100, "max": 0,    "critical": True},
    "cmf_20":         {"min": -1,   "max": 1,    "critical": False},
    "bb_pctb":        {"min": -1.0, "max": 2.0,  "critical": False},
    "atr_14":         {"min": 0,    "max": None, "critical": True},
    "ema_9":          {"min": 0,    "max": None, "critical": True},
    "ema_21":         {"min": 0,    "max": None, "critical": True},
    "ema_50":         {"min": 0,    "max": None, "critical": True},
    "macd_line":      {"min": None, "max": None, "critical": False},
    "macd_signal":    {"min": None, "max": None, "critical": False},
    "macd_histogram": {"min": None, "max": None, "critical": False},
    "obv":            {"min": None, "max": None, "critical": False},
    "ad_line":        {"min": None, "max": None, "critical": False},
}

_VALID_DIVERGENCE_TYPES = frozenset({
    "regular_bullish", "regular_bearish", "hidden_bullish", "hidden_bearish"
})
_VALID_DIVERGENCE_INDICATORS = frozenset({"rsi", "macd_histogram", "obv", "stochastic"})
_VALID_CROSSOVER_TYPES = frozenset({"ema_9_21", "ema_21_50", "macd_signal"})
_VALID_CROSSOVER_DIRECTIONS = frozenset({"bullish", "bearish"})
_VALID_PATTERN_DIRECTIONS = frozenset({"bullish", "bearish", "neutral"})
_VALID_PATTERN_CATEGORIES = frozenset({"candlestick", "structural"})
_VALID_REGIMES = frozenset({"trending", "ranging", "volatile"})
_VALID_SIGNALS = frozenset({"BULLISH", "BEARISH", "NEUTRAL"})

# Days of same EMA value that indicate a computation error (stuck indicator)
_EMA_STUCK_THRESHOLD = 10

# Structural pattern count thresholds per ticker
_STRUCTURAL_PATTERN_WARN_HIGH = 2000

# Divergence count thresholds per ticker (over full history)
_DIVERGENCE_COUNT_WARN_LOW = 1

# Minimum ratio of weekly volume to (trading_days × avg_daily_volume) to pass.
# 0.30 means a week is only flagged if its volume is < 30% of expected — catches
# weeks where only a single day was summed instead of the full 5 days, while
# ignoring naturally quiet weeks (pre-holiday, summer, low-volatility periods).
_WEEKLY_VOLUME_MIN_RATIO = 0.30

# Weekly candle expected per year
_WEEKLY_CANDLES_PER_YEAR = 52
_WEEKLY_CANDLE_YEARS = 5
_WEEKLY_CANDLE_WARN_LOW_PCT = 0.60


# ---------------------------------------------------------------------------
# Indicator checks
# ---------------------------------------------------------------------------

def check_indicator_ranges(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
    warmup_rows: int = 50,
) -> CheckResult:
    """
    Check that indicator values fall within expected mathematical ranges.

    Queries indicators_daily for all active tickers and validates each bounded
    indicator against INDICATOR_RANGES. Bounds for individual indicators can be
    overridden via ``config/verify_pipeline.json`` under the ``indicator_ranges``
    key so thresholds are tunable without code changes. The first `warmup_rows`
    rows per ticker (ordered by date) are skipped because the `ta` library emits
    zeros or NaNs during the indicator warm-up period. A floating-point tolerance
    of 1e-9 is applied to all bounds. ATR and EMA values of exactly 0.0 are treated
    as violations after warm-up. EMA distance from price is NOT flagged as a warning
    — extreme divergence is expected during crashes (COIN 2022: -85%) and is logged
    at INFO level only. EMA stuck at the same value for ``ema_stuck_days_threshold``
    consecutive days IS flagged (warning) as that indicates a computation error.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols to check.
        warmup_rows: Number of initial rows per ticker to skip (default 50).

    Returns:
        CheckResult with status "fail", "warn", or "pass".
    """
    # Load verification config — thresholds are tunable without code changes.
    try:
        verify_cfg = load_config("verify_pipeline")
    except FileNotFoundError:
        verify_cfg = {}
    cfg_ranges: dict = verify_cfg.get("indicator_ranges", {})
    ema_stuck_threshold: int = int(
        verify_cfg.get("ema_stuck_days_threshold", _EMA_STUCK_THRESHOLD)
    )

    critical_issues: list[str] = []
    warning_issues: list[str] = []

    placeholders = ",".join("?" * len(active_tickers))

    for col, spec in INDICATOR_RANGES.items():
        # Config can override min/max per indicator; falls back to module defaults.
        col_override = cfg_ranges.get(col, {})
        low = col_override.get("min", spec["min"])
        high = col_override.get("max", spec["max"])
        is_critical = spec["critical"]

        if low is None and high is None:
            continue

        conditions: list[str] = []
        if low is not None:
            if col in ("atr_14", "ema_9", "ema_21", "ema_50"):
                # Zero is a real violation for these after warm-up; use tight tolerance
                conditions.append(f"{col} IS NOT NULL AND {col} < 1e-9")
            else:
                low_thresh = low - 1e-9
                conditions.append(f"{col} IS NOT NULL AND {col} < {low_thresh}")
        if high is not None:
            high_thresh = high + 1e-9
            conditions.append(f"{col} IS NOT NULL AND {col} > {high_thresh}")

        if not conditions:
            continue

        where_clause = " OR ".join(conditions)
        sql = (
            f"WITH ranked AS ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn"
            f"  FROM indicators_daily"
            f"  WHERE ticker IN ({placeholders})"
            f") "
            f"SELECT ticker, date, {col} FROM ranked "
            f"WHERE rn > {warmup_rows} AND ({where_clause}) "
            f"LIMIT 10"
        )
        rows = db_conn.execute(sql, active_tickers).fetchall()

        for row in rows:
            msg = (
                f"{row['ticker']} {row['date']}: "
                f"{col}={row[col]} out of expected range "
                f"[{low}, {high}]"
            )
            if is_critical:
                critical_issues.append(msg)
            else:
                warning_issues.append(msg)

    # EMA validity: stuck detection (warning) + INFO-level distance logging
    ema_issues = _check_ema_validity(db_conn, active_tickers, warmup_rows, ema_stuck_threshold)
    warning_issues.extend(ema_issues)

    if critical_issues:
        return CheckResult(
            name="indicator_ranges",
            status="fail",
            message=f"{len(critical_issues)} critical indicator range violation(s)",
            details=critical_issues + warning_issues,
        )
    if warning_issues:
        return CheckResult(
            name="indicator_ranges",
            status="warn",
            message=f"{len(warning_issues)} non-critical indicator range warning(s)",
            details=warning_issues,
        )
    return CheckResult(
        name="indicator_ranges",
        status="pass",
        message="All indicator values are within expected ranges",
    )


def _check_ema_validity(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
    warmup_rows: int = 50,
    stuck_threshold: int = _EMA_STUCK_THRESHOLD,
) -> list[str]:
    """
    Check EMAs for computation errors: stuck values and INFO-level distance logging.

    Two checks:
    1. **Stuck EMA (warning)**: if any EMA column has the same value for
       ``stuck_threshold`` consecutive trading days, that indicates the indicator
       was not recomputed correctly (e.g., a bug in the calculator or stale data).
       This is distinct from normal market behavior.
    2. **EMA distance (INFO only)**: large divergence between EMA and close price
       is expected during crashes (COIN 2022: -85% → EMA lagged 176%) and parabolic
       runs. It is logged at INFO level for awareness but never raises a warning.

    Negative/zero EMA values are detected by the main INDICATOR_RANGES loop
    (ema_9/21/50 are now marked critical=True there).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.
        warmup_rows: Number of initial rows per ticker to skip.
        stuck_threshold: Consecutive days with same EMA value to flag (default 10).

    Returns:
        List of warning strings for stuck EMAs only.
    """
    warning_issues: list[str] = []
    placeholders = ",".join("?" * len(active_tickers))

    for ema_col in ("ema_9", "ema_21", "ema_50"):
        stuck_rows = db_conn.execute(
            f"""
            WITH post_warmup AS (
              SELECT ticker, date, {ema_col},
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
              FROM indicators_daily
              WHERE ticker IN ({placeholders})
                AND {ema_col} IS NOT NULL AND {ema_col} > 0
            ),
            windowed AS (
              SELECT ticker, date, {ema_col},
                COUNT(*) OVER (
                  PARTITION BY ticker ORDER BY date
                  ROWS BETWEEN {stuck_threshold - 1} PRECEDING AND CURRENT ROW
                ) AS win_count,
                MAX({ema_col}) OVER (
                  PARTITION BY ticker ORDER BY date
                  ROWS BETWEEN {stuck_threshold - 1} PRECEDING AND CURRENT ROW
                ) - MIN({ema_col}) OVER (
                  PARTITION BY ticker ORDER BY date
                  ROWS BETWEEN {stuck_threshold - 1} PRECEDING AND CURRENT ROW
                ) AS win_range
              FROM post_warmup
              WHERE rn > {warmup_rows}
            )
            SELECT DISTINCT ticker, {ema_col} AS ema_val
            FROM windowed
            WHERE win_count = {stuck_threshold} AND win_range < 1e-6
            LIMIT 5
            """,
            active_tickers,
        ).fetchall()
        for row in stuck_rows:
            warning_issues.append(
                f"{row['ticker']}: {ema_col}={row['ema_val']:.4f} "
                f"unchanged for {stuck_threshold}+ consecutive trading days "
                f"— possible computation error"
            )

    # Log large EMA-to-price distance at INFO only (not actionable as a warning)
    distance_rows = db_conn.execute(
        f"""
        WITH ranked AS (
          SELECT i.ticker, i.date, i.ema_9, i.ema_21, i.ema_50, o.close,
                 ROW_NUMBER() OVER (PARTITION BY i.ticker ORDER BY i.date) AS rn
          FROM indicators_daily i
          JOIN ohlcv_daily o ON i.ticker = o.ticker AND i.date = o.date
          WHERE i.ticker IN ({placeholders})
            AND o.close IS NOT NULL AND o.close > 0
        )
        SELECT ticker, date, ema_9, ema_21, ema_50, close
        FROM ranked
        WHERE rn > {warmup_rows}
          AND (
            (ema_9  IS NOT NULL AND ema_9  > 0 AND ABS(ema_9  - close) / close > 0.5)
            OR (ema_21 IS NOT NULL AND ema_21 > 0 AND ABS(ema_21 - close) / close > 0.5)
            OR (ema_50 IS NOT NULL AND ema_50 > 0 AND ABS(ema_50 - close) / close > 0.5)
          )
        LIMIT 20
        """,
        active_tickers,
    ).fetchall()
    for row in distance_rows:
        close = row["close"]
        for col_name in ("ema_9", "ema_21", "ema_50"):
            val = row[col_name]
            if val is None or val <= 0:
                continue
            pct = abs(val - close) / close
            if pct > 0.5:
                logger.info(
                    "[EMA distance] %s %s: %s=%.2f is %.0f%% from close=%.2f "
                    "(informational — crash/rally behavior, not a computation error)",
                    row["ticker"], row["date"], col_name, val, pct * 100, close,
                )

    return warning_issues


def check_indicator_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that every active ticker has at least one row in indicators_daily.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of expected active ticker symbols.

    Returns:
        CheckResult with status "fail" if any ticker is missing, "pass" otherwise.
    """
    if not active_tickers:
        return CheckResult(
            name="indicator_coverage",
            status="warn",
            message="No active tickers provided",
        )

    placeholders = ",".join("?" * len(active_tickers))
    rows = db_conn.execute(
        f"SELECT DISTINCT ticker FROM indicators_daily WHERE ticker IN ({placeholders})",
        active_tickers,
    ).fetchall()
    present = {row["ticker"] for row in rows}
    missing = sorted(set(active_tickers) - present)

    if not missing:
        return CheckResult(
            name="indicator_coverage",
            status="pass",
            message=f"All {len(active_tickers)} tickers have indicator data",
        )

    return CheckResult(
        name="indicator_coverage",
        status="fail",
        message=f"{len(missing)} ticker(s) missing from indicators_daily",
        details=[f"{t} has no indicator rows" for t in missing],
    )


def check_indicator_date_alignment(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that indicators_daily dates match ohlcv_daily dates for each ticker.

    For each active ticker, finds OHLCV dates that have no corresponding
    indicators_daily row. Flags if > 5% of OHLCV dates are missing indicators.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if significant gaps exist, "pass" otherwise.
        data["missing_dates_count"] contains the total count of gaps.
    """
    total_missing = 0
    details: list[str] = []

    for ticker in active_tickers:
        ohlcv_dates = {
            row["date"]
            for row in db_conn.execute(
                "SELECT date FROM ohlcv_daily WHERE ticker = ?", (ticker,)
            ).fetchall()
        }
        indicator_dates = {
            row["date"]
            for row in db_conn.execute(
                "SELECT date FROM indicators_daily WHERE ticker = ?", (ticker,)
            ).fetchall()
        }
        missing = ohlcv_dates - indicator_dates
        if missing:
            total_missing += len(missing)
            pct = len(missing) / len(ohlcv_dates) * 100 if ohlcv_dates else 0
            details.append(
                f"{ticker}: {len(missing)} OHLCV dates missing from indicators_daily "
                f"({pct:.1f}%)"
            )

    if total_missing == 0:
        return CheckResult(
            name="indicator_date_alignment",
            status="pass",
            message="Indicator dates align with OHLCV dates for all tickers",
            data={"missing_dates_count": 0},
        )

    return CheckResult(
        name="indicator_date_alignment",
        status="warn",
        message=f"{total_missing} OHLCV dates missing from indicators_daily across all tickers",
        details=details,
        data={"missing_dates_count": total_missing},
    )


def check_indicator_null_percentage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
    max_null_pct: float = 20.0,
) -> CheckResult:
    """
    Check for excessive NULL values in indicators_daily and all-NULL rows.

    Flags any row where every indicator column is NULL (broken computation).
    Also warns if the NULL rate for any key indicator exceeds max_null_pct.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.
        max_null_pct: Maximum acceptable NULL percentage per indicator. Default 20.0.

    Returns:
        CheckResult with "fail" if all-NULL rows exist, "warn" if NULL rate
        exceeds threshold, "pass" otherwise.
    """
    key_indicators = ["rsi_14", "adx", "macd_line", "ema_9", "atr_14"]
    issues: list[str] = []
    null_data: dict[str, float] = {}
    has_all_null = False
    placeholders = ",".join("?" * len(active_tickers))

    # Check all-NULL rows
    all_null_check_cols = " AND ".join(
        f"{col} IS NULL"
        for col in ["rsi_14", "adx", "macd_line", "ema_9", "ema_21", "ema_50",
                    "stoch_k", "atr_14", "obv"]
    )
    all_null_count = db_conn.execute(
        f"SELECT COUNT(*) AS cnt FROM indicators_daily "
        f"WHERE ticker IN ({placeholders}) AND ({all_null_check_cols})",
        active_tickers,
    ).fetchone()["cnt"]

    if all_null_count and all_null_count > 0:
        has_all_null = True
        issues.append(
            f"{all_null_count} row(s) with all indicator columns NULL — "
            "possible computation failure"
        )

    # Check NULL percentage per key indicator
    for col in key_indicators:
        row = db_conn.execute(
            f"SELECT COUNT(*) AS total, "
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls "
            f"FROM indicators_daily WHERE ticker IN ({placeholders})",
            active_tickers,
        ).fetchone()
        total = row["total"] or 0
        nulls = row["nulls"] or 0
        null_pct = round(nulls / total * 100, 1) if total > 0 else 0.0
        null_data[col] = null_pct

        if null_pct > max_null_pct:
            issues.append(
                f"{col}: {null_pct}% NULL across all tickers "
                f"(threshold: {max_null_pct}%)"
            )

    if has_all_null:
        return CheckResult(
            name="indicator_null_percentage",
            status="fail",
            message=f"All-NULL indicator rows detected",
            details=issues,
            data=null_data,
        )
    if issues:
        return CheckResult(
            name="indicator_null_percentage",
            status="warn",
            message=f"{len(issues)} indicator NULL rate(s) exceed {max_null_pct}% threshold",
            details=issues,
            data=null_data,
        )
    return CheckResult(
        name="indicator_null_percentage",
        status="pass",
        message="Indicator NULL rates are within acceptable bounds",
        data=null_data,
    )


# ---------------------------------------------------------------------------
# Score checks
# ---------------------------------------------------------------------------

def check_score_ranges(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that final_score is within [-100, +100] for all tickers on the given date.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "fail" if any score is out of range, "pass" otherwise.
    """
    rows = db_conn.execute(
        "SELECT ticker, final_score FROM scores_daily "
        "WHERE date = ? AND final_score IS NOT NULL "
        "AND (final_score < -100 OR final_score > 100)",
        (scoring_date,),
    ).fetchall()

    if not rows:
        return CheckResult(
            name="score_ranges",
            status="pass",
            message=f"All final_score values are within [-100, +100] for {scoring_date}",
        )

    details = [
        f"{row['ticker']}: final_score={row['final_score']:.2f} out of range"
        for row in rows
    ]
    return CheckResult(
        name="score_ranges",
        status="fail",
        message=f"{len(rows)} ticker(s) have final_score outside [-100, +100]",
        details=details,
    )


def check_category_score_ranges(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that all 9 category scores are within [-100, +100].

    Checks: trend, momentum, volume, volatility, candlestick, structural,
    sentiment, fundamental, macro.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "fail" if any category score is out of range.
    """
    category_cols = [
        "trend_score", "momentum_score", "volume_score", "volatility_score",
        "candlestick_score", "structural_score", "sentiment_score",
        "fundamental_score", "macro_score",
    ]
    issues: list[str] = []

    for col in category_cols:
        rows = db_conn.execute(
            f"SELECT ticker, {col} FROM scores_daily "
            f"WHERE date = ? AND {col} IS NOT NULL "
            f"AND ({col} < -100 OR {col} > 100)",
            (scoring_date,),
        ).fetchall()
        for row in rows:
            issues.append(
                f"{row['ticker']}: {col}={row[col]:.2f} out of range"
            )

    if issues:
        return CheckResult(
            name="category_score_ranges",
            status="fail",
            message=f"{len(issues)} category score(s) outside [-100, +100]",
            details=issues,
        )
    return CheckResult(
        name="category_score_ranges",
        status="pass",
        message="All category scores are within [-100, +100]",
    )


def check_confidence_range(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that confidence is within [0, 100] for all tickers on the given date.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "fail" if any confidence is out of range.
    """
    rows = db_conn.execute(
        "SELECT ticker, confidence FROM scores_daily "
        "WHERE date = ? AND confidence IS NOT NULL "
        "AND (confidence < 0 OR confidence > 100)",
        (scoring_date,),
    ).fetchall()

    if not rows:
        return CheckResult(
            name="confidence_range",
            status="pass",
            message=f"All confidence values are within [0, 100] for {scoring_date}",
        )

    details = [
        f"{row['ticker']}: confidence={row['confidence']:.1f} out of [0, 100]"
        for row in rows
    ]
    return CheckResult(
        name="confidence_range",
        status="fail",
        message=f"{len(rows)} ticker(s) have confidence outside [0, 100]",
        details=details,
    )


def check_signal_score_consistency(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that BULLISH signals have positive final_score and BEARISH have negative.

    A BULLISH signal with final_score <= 0, or a BEARISH signal with
    final_score >= 0, indicates an inconsistency between signal and score.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "warn" if inconsistencies exist, "pass" otherwise.
    """
    rows = db_conn.execute(
        "SELECT ticker, signal, final_score FROM scores_daily "
        "WHERE date = ? AND final_score IS NOT NULL AND signal IS NOT NULL",
        (scoring_date,),
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        signal = row["signal"]
        score = row["final_score"]
        if signal == "BULLISH" and score <= 0:
            issues.append(
                f"{row['ticker']}: BULLISH signal but final_score={score:.2f} <= 0"
            )
        elif signal == "BEARISH" and score >= 0:
            issues.append(
                f"{row['ticker']}: BEARISH signal but final_score={score:.2f} >= 0"
            )

    if issues:
        return CheckResult(
            name="signal_score_consistency",
            status="warn",
            message=f"{len(issues)} ticker(s) have signal inconsistent with final_score",
            details=issues,
        )
    return CheckResult(
        name="signal_score_consistency",
        status="pass",
        message="All signal/score pairs are consistent",
    )


def check_signal_distribution(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Check that signals are not all identical, which would indicate a scoring bug.

    Warns if 100% of tickers have the same signal on the given date.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "warn" if distribution is degenerate, "pass" otherwise.
    """
    rows = db_conn.execute(
        "SELECT signal, COUNT(*) AS cnt FROM scores_daily "
        "WHERE date = ? AND signal IS NOT NULL GROUP BY signal",
        (scoring_date,),
    ).fetchall()

    if not rows:
        return CheckResult(
            name="signal_distribution",
            status="warn",
            message=f"No scores found for {scoring_date}",
        )

    total = sum(row["cnt"] for row in rows)
    distribution = {row["signal"]: row["cnt"] for row in rows}

    for signal, count in distribution.items():
        if count == total and total > 1:
            return CheckResult(
                name="signal_distribution",
                status="warn",
                message=(
                    f"100% of tickers ({total}) have signal={signal} — "
                    "possible scoring issue"
                ),
                data=distribution,
            )

    return CheckResult(
        name="signal_distribution",
        status="pass",
        message=f"Signal distribution is reasonable: {distribution}",
        data=distribution,
    )


def check_confidence_distribution(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Check that not all tickers have 0% confidence, which would indicate a bug.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "warn" if all confidences are 0%, "pass" otherwise.
    """
    rows = db_conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN confidence = 0 THEN 1 ELSE 0 END) AS zero_count "
        "FROM scores_daily WHERE date = ? AND confidence IS NOT NULL",
        (scoring_date,),
    ).fetchone()

    total = rows["total"] or 0
    zero_count = rows["zero_count"] or 0

    if total > 1 and zero_count == total:
        return CheckResult(
            name="confidence_distribution",
            status="warn",
            message=f"100% of tickers ({total}) have 0% confidence — possible scoring issue",
            data={"total": total, "zero_count": zero_count},
        )

    return CheckResult(
        name="confidence_distribution",
        status="pass",
        message=f"Confidence distribution is reasonable ({zero_count}/{total} at 0%)",
        data={"total": total, "zero_count": zero_count},
    )


def check_weighted_score_math(
    db_conn: sqlite3.Connection,
    scoring_date: str,
    tolerance: float = 2.0,
    daily_weight: Optional[float] = None,
    weekly_weight: Optional[float] = None,
) -> CheckResult:
    """
    Verify that final_score ≈ daily_weight × daily_score + weekly_weight × weekly_score.

    Weights default to the values in scorer.json timeframe_weights. Allows for a
    ±tolerance difference (default ±2.0) to account for sector adjustment and rounding.
    Flags tickers where the deviation exceeds tolerance.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.
        tolerance: Maximum allowed deviation from the weighted formula. Default 2.0.
        daily_weight: Override daily weight (defaults to scorer.json value).
        weekly_weight: Override weekly weight (defaults to scorer.json value).

    Returns:
        CheckResult with "warn" if any ticker exceeds the tolerance.
    """
    if daily_weight is None or weekly_weight is None:
        scorer_cfg = load_config("scorer")
        tw = scorer_cfg.get("timeframe_weights", {})
        daily_weight = daily_weight if daily_weight is not None else tw.get("daily", 0.2)
        weekly_weight = weekly_weight if weekly_weight is not None else tw.get("weekly", 0.8)

    rows = db_conn.execute(
        "SELECT ticker, final_score, daily_score, weekly_score "
        "FROM scores_daily "
        "WHERE date = ? "
        "AND final_score IS NOT NULL "
        "AND daily_score IS NOT NULL "
        "AND weekly_score IS NOT NULL",
        (scoring_date,),
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        expected = daily_weight * row["daily_score"] + weekly_weight * row["weekly_score"]
        deviation = abs(row["final_score"] - expected)
        if deviation > tolerance:
            issues.append(
                f"{row['ticker']}: final_score={row['final_score']:.2f}, "
                f"expected≈{expected:.2f} (deviation={deviation:.2f} > {tolerance})"
            )

    if issues:
        return CheckResult(
            name="weighted_score_math",
            status="warn",
            message=f"{len(issues)} ticker(s) have final_score far from {daily_weight}×daily+{weekly_weight}×weekly",
            details=issues,
        )
    return CheckResult(
        name="weighted_score_math",
        status="pass",
        message=f"Weighted score math checks out within ±{tolerance} tolerance",
    )


def check_regime_values(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that regime is one of: trending, ranging, volatile.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "fail" if any unknown regime value is found.
    """
    rows = db_conn.execute(
        "SELECT ticker, regime FROM scores_daily "
        "WHERE date = ? AND regime IS NOT NULL",
        (scoring_date,),
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        if row["regime"] not in _VALID_REGIMES:
            issues.append(
                f"{row['ticker']}: regime={row['regime']!r} is not valid "
                f"(expected one of: {sorted(_VALID_REGIMES)})"
            )

    if issues:
        return CheckResult(
            name="regime_values",
            status="fail",
            message=f"{len(issues)} ticker(s) have invalid regime values",
            details=issues,
        )
    return CheckResult(
        name="regime_values",
        status="pass",
        message=f"All regime values are valid for {scoring_date}",
    )


def check_json_fields(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that data_completeness and key_signals are valid JSON.

    data_completeness should parse as a JSON object (dict).
    key_signals should parse as a JSON array with at least one entry.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "warn" if any field fails JSON parsing.
    """
    rows = db_conn.execute(
        "SELECT ticker, data_completeness, key_signals FROM scores_daily "
        "WHERE date = ?",
        (scoring_date,),
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        ticker = row["ticker"]

        if row["data_completeness"] is not None:
            try:
                json.loads(row["data_completeness"])
            except (json.JSONDecodeError, ValueError):
                issues.append(
                    f"{ticker}: data_completeness is not valid JSON"
                )

        if row["key_signals"] is not None:
            try:
                parsed = json.loads(row["key_signals"])
                if not isinstance(parsed, list):
                    issues.append(
                        f"{ticker}: key_signals is not a JSON array"
                    )
            except (json.JSONDecodeError, ValueError):
                issues.append(
                    f"{ticker}: key_signals is not valid JSON"
                )

    if issues:
        return CheckResult(
            name="json_fields",
            status="warn",
            message=f"{len(issues)} JSON field(s) are invalid or malformed",
            details=issues,
        )
    return CheckResult(
        name="json_fields",
        status="pass",
        message=f"All JSON fields are valid for {scoring_date}",
    )


# ---------------------------------------------------------------------------
# Pattern checks
# ---------------------------------------------------------------------------

def check_pattern_counts(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check that pattern counts per ticker are within a reasonable range.

    Warns if a ticker has 0 patterns (detection may be broken) or if structural
    patterns exceed a high threshold (possible infinite loop or bug).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any ticker is suspicious.
    """
    issues: list[str] = []
    placeholders = ",".join("?" * len(active_tickers))

    count_rows = db_conn.execute(
        f"SELECT ticker, pattern_category, COUNT(*) AS cnt "
        f"FROM patterns_daily "
        f"WHERE ticker IN ({placeholders}) "
        f"GROUP BY ticker, pattern_category",
        active_tickers,
    ).fetchall()

    counts_by_ticker: dict[str, dict[str, int]] = {t: {} for t in active_tickers}
    for row in count_rows:
        counts_by_ticker[row["ticker"]][row["pattern_category"]] = row["cnt"]

    for ticker in active_tickers:
        ticker_counts = counts_by_ticker[ticker]
        total = sum(ticker_counts.values())

        if total == 0:
            issues.append(f"{ticker}: 0 patterns detected over full history")
            continue

        structural_count = ticker_counts.get("structural", 0)
        if structural_count > _STRUCTURAL_PATTERN_WARN_HIGH:
            issues.append(
                f"{ticker}: {structural_count} structural patterns "
                f"(excessive — threshold: {_STRUCTURAL_PATTERN_WARN_HIGH})"
            )

    if issues:
        return CheckResult(
            name="pattern_counts",
            status="warn",
            message=f"{len(issues)} ticker(s) have suspicious pattern counts",
            details=issues,
        )
    return CheckResult(
        name="pattern_counts",
        status="pass",
        message="Pattern counts are reasonable for all tickers",
    )


def check_pattern_duplicates(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check for duplicate patterns (same ticker, date, name, direction).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if duplicates exist.
    """
    placeholders = ",".join("?" * len(active_tickers))
    rows = db_conn.execute(
        f"SELECT ticker, date, pattern_name, direction, COUNT(*) AS cnt "
        f"FROM patterns_daily "
        f"WHERE ticker IN ({placeholders}) "
        f"GROUP BY ticker, date, pattern_name, direction "
        f"HAVING COUNT(*) > 1 "
        f"LIMIT 20",
        active_tickers,
    ).fetchall()

    if not rows:
        return CheckResult(
            name="pattern_duplicates",
            status="pass",
            message="No duplicate patterns found",
        )

    details = [
        f"{row['ticker']} {row['date']}: "
        f"{row['pattern_name']}/{row['direction']} appears {row['cnt']}x"
        for row in rows
    ]
    return CheckResult(
        name="pattern_duplicates",
        status="warn",
        message=f"{len(rows)} duplicate pattern(s) found",
        details=details,
    )


def check_pattern_field_validity(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Check that pattern fields have valid enum values.

    Validates:
    - direction in {bullish, bearish, neutral}
    - strength between 1 and 5
    - pattern_category in {candlestick, structural}

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with "fail" if any invalid values are found.
    """
    issues: list[str] = []

    # Check direction
    rows = db_conn.execute(
        "SELECT ticker, date, pattern_name, direction FROM patterns_daily "
        "WHERE direction IS NOT NULL AND direction NOT IN ('bullish', 'bearish', 'neutral') "
        "LIMIT 20"
    ).fetchall()
    for row in rows:
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"invalid direction={row['direction']!r} for {row['pattern_name']}"
        )

    # Check strength
    rows = db_conn.execute(
        "SELECT ticker, date, pattern_name, strength FROM patterns_daily "
        "WHERE strength IS NOT NULL AND (strength < 1 OR strength > 5) "
        "LIMIT 20"
    ).fetchall()
    for row in rows:
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"invalid strength={row['strength']} for {row['pattern_name']} "
            "(expected 1-5)"
        )

    # Check category
    rows = db_conn.execute(
        "SELECT ticker, date, pattern_name, pattern_category FROM patterns_daily "
        "WHERE pattern_category IS NOT NULL "
        "AND pattern_category NOT IN ('candlestick', 'structural') "
        "LIMIT 20"
    ).fetchall()
    for row in rows:
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"invalid pattern_category={row['pattern_category']!r} "
            f"for {row['pattern_name']}"
        )

    if issues:
        return CheckResult(
            name="pattern_field_validity",
            status="fail",
            message=f"{len(issues)} pattern field validity issue(s) found",
            details=issues,
        )
    return CheckResult(
        name="pattern_field_validity",
        status="pass",
        message="All pattern field values are valid",
    )


# ---------------------------------------------------------------------------
# Divergence checks
# ---------------------------------------------------------------------------

def check_divergence_counts(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check that divergence counts per ticker are within a reasonable range.

    Warns if a ticker has fewer than _DIVERGENCE_COUNT_WARN_LOW divergences
    over its entire history, which may indicate the calculator is not detecting
    divergences.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any ticker has suspiciously few divergences.
    """
    placeholders = ",".join("?" * len(active_tickers))
    rows = db_conn.execute(
        f"SELECT ticker, COUNT(*) AS cnt FROM divergences_daily "
        f"WHERE ticker IN ({placeholders}) GROUP BY ticker",
        active_tickers,
    ).fetchall()

    counts: dict[str, int] = {row["ticker"]: row["cnt"] for row in rows}
    issues: list[str] = []

    for ticker in active_tickers:
        count = counts.get(ticker, 0)
        if count < _DIVERGENCE_COUNT_WARN_LOW:
            issues.append(
                f"{ticker}: only {count} divergence(s) detected over full history"
            )

    if issues:
        return CheckResult(
            name="divergence_counts",
            status="warn",
            message=f"{len(issues)} ticker(s) have suspiciously few divergences",
            details=issues,
        )
    return CheckResult(
        name="divergence_counts",
        status="pass",
        message="Divergence counts are reasonable for all tickers",
    )


def check_divergence_consistency(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Check divergence_type, indicator validity, and swing value consistency.

    For each divergence row:
    1. divergence_type must be one of the four valid types.
    2. indicator must be one of the valid indicators.
    3. Swing values must be consistent with the divergence type:
       - regular_bullish: price_2 < price_1 AND ind_2 > ind_1
       - regular_bearish: price_2 > price_1 AND ind_2 < ind_1
       - hidden_bullish:  price_2 > price_1 AND ind_2 < ind_1
       - hidden_bearish:  price_2 < price_1 AND ind_2 > ind_1

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with "fail" if type/indicator issues or swing inconsistencies.
    """
    rows = db_conn.execute(
        "SELECT id, ticker, date, divergence_type, indicator, "
        "price_swing_1_value, price_swing_2_value, "
        "indicator_swing_1_value, indicator_swing_2_value "
        "FROM divergences_daily"
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        div_type = row["divergence_type"]
        ind = row["indicator"]

        if div_type not in _VALID_DIVERGENCE_TYPES:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"invalid divergence_type={div_type!r}"
            )
            continue  # skip swing check if type is unknown

        if ind not in _VALID_DIVERGENCE_INDICATORS:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"invalid indicator={ind!r} for divergence"
            )

        p1 = row["price_swing_1_value"]
        p2 = row["price_swing_2_value"]
        i1 = row["indicator_swing_1_value"]
        i2 = row["indicator_swing_2_value"]

        if None in (p1, p2, i1, i2):
            continue  # insufficient data to validate swing relationship

        consistent = _check_swing_consistency(div_type, p1, p2, i1, i2)
        if not consistent:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"{div_type} swing values inconsistent "
                f"(price: {p1}→{p2}, indicator: {i1}→{i2})"
            )

    if issues:
        return CheckResult(
            name="divergence_consistency",
            status="fail",
            message=f"{len(issues)} divergence consistency issue(s) found",
            details=issues,
        )
    return CheckResult(
        name="divergence_consistency",
        status="pass",
        message="All divergence types, indicators, and swing values are consistent",
    )


def _check_swing_consistency(
    div_type: str,
    price_1: float,
    price_2: float,
    ind_1: float,
    ind_2: float,
) -> bool:
    """
    Return True if swing values are consistent with the divergence type.

    Args:
        div_type: One of the four valid divergence types.
        price_1: First price swing value.
        price_2: Second price swing value.
        ind_1: First indicator swing value.
        ind_2: Second indicator swing value.

    Returns:
        True if consistent, False if contradictory.
    """
    if div_type == "regular_bullish":
        return price_2 < price_1 and ind_2 > ind_1
    if div_type == "regular_bearish":
        return price_2 > price_1 and ind_2 < ind_1
    if div_type == "hidden_bullish":
        return price_2 > price_1 and ind_2 < ind_1
    if div_type == "hidden_bearish":
        return price_2 < price_1 and ind_2 > ind_1
    return True  # unknown type already flagged elsewhere


# ---------------------------------------------------------------------------
# Crossover checks
# ---------------------------------------------------------------------------

def check_crossover_validity(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Check that crossover_type, direction, and days_ago are valid.

    crossover_type must be one of: ema_9_21, ema_21_50, macd_signal.
    direction must be one of: bullish, bearish.
    days_ago must be >= 0.

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with "fail" if any invalid values are found.
    """
    issues: list[str] = []

    rows = db_conn.execute(
        "SELECT ticker, date, crossover_type, direction, days_ago "
        "FROM crossovers_daily"
    ).fetchall()

    for row in rows:
        if row["crossover_type"] not in _VALID_CROSSOVER_TYPES:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"invalid crossover_type={row['crossover_type']!r}"
            )
        if row["direction"] not in _VALID_CROSSOVER_DIRECTIONS:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"invalid direction={row['direction']!r} for crossover"
            )
        if row["days_ago"] is not None and row["days_ago"] < 0:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"days_ago={row['days_ago']} is negative"
            )

    if issues:
        return CheckResult(
            name="crossover_validity",
            status="fail",
            message=f"{len(issues)} crossover validity issue(s) found",
            details=issues,
        )
    return CheckResult(
        name="crossover_validity",
        status="pass",
        message="All crossover field values are valid",
    )


# ---------------------------------------------------------------------------
# Profile checks
# ---------------------------------------------------------------------------

def check_profile_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that every active ticker has at least one indicator profile.

    Flags any ticker that has zero profiles, which indicates the profile
    computation step did not run for that ticker.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of expected active ticker symbols.

    Returns:
        CheckResult with "fail" if any ticker has no profiles.
    """
    if not active_tickers:
        return CheckResult(
            name="profile_coverage",
            status="warn",
            message="No active tickers provided",
        )

    placeholders = ",".join("?" * len(active_tickers))
    rows = db_conn.execute(
        f"SELECT DISTINCT ticker FROM indicator_profiles "
        f"WHERE ticker IN ({placeholders})",
        active_tickers,
    ).fetchall()
    present = {row["ticker"] for row in rows}
    missing = sorted(set(active_tickers) - present)

    if not missing:
        return CheckResult(
            name="profile_coverage",
            status="pass",
            message=f"All {len(active_tickers)} tickers have indicator profiles",
        )

    return CheckResult(
        name="profile_coverage",
        status="fail",
        message=f"{len(missing)} ticker(s) have no indicator profiles",
        details=[f"{t}: no indicator profiles" for t in missing],
    )


def check_profile_percentile_order(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Verify that p5 < p20 < p50 < p80 < p95 for every profile row.

    Also checks that std > 0 (zero std means no variance — broken computation).

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with "fail" if percentiles are out of order or std is zero.
    """
    rows = db_conn.execute(
        "SELECT ticker, indicator, p5, p20, p50, p80, p95, std "
        "FROM indicator_profiles"
    ).fetchall()

    issues: list[str] = []
    for row in rows:
        vals = [row["p5"], row["p20"], row["p50"], row["p80"], row["p95"]]
        # Only validate if all percentiles are non-null
        if all(v is not None for v in vals):
            p5, p20, p50, p80, p95 = vals
            if not (p5 <= p20 <= p50 <= p80 <= p95):
                issues.append(
                    f"{row['ticker']} {row['indicator']}: "
                    f"percentiles out of order: "
                    f"p5={p5}, p20={p20}, p50={p50}, p80={p80}, p95={p95}"
                )

        std = row["std"]
        if std is not None and std == 0:
            issues.append(
                f"{row['ticker']} {row['indicator']}: std=0 "
                "(no variance — broken profile)"
            )

    if issues:
        return CheckResult(
            name="profile_percentile_order",
            status="fail",
            message=f"{len(issues)} profile percentile issue(s) found",
            details=issues,
        )
    return CheckResult(
        name="profile_percentile_order",
        status="pass",
        message="All indicator profile percentiles are correctly ordered",
    )


def check_profile_freshness(
    db_conn: sqlite3.Connection,
    max_age_days: int = 30,
) -> CheckResult:
    """
    Verify that indicator profiles are not stale.

    Checks the maximum window_end date across all profiles. If the most
    recent profile is older than max_age_days, profiles need to be recomputed.

    Args:
        db_conn: Open SQLite connection.
        max_age_days: Maximum acceptable age in days. Default 30.

    Returns:
        CheckResult with "warn" if profiles are stale, "pass" otherwise.
    """
    row = db_conn.execute(
        "SELECT MAX(window_end) AS latest_end FROM indicator_profiles"
    ).fetchone()

    if not row or not row["latest_end"]:
        return CheckResult(
            name="profile_freshness",
            status="warn",
            message="No indicator profiles found",
        )

    latest_end = date.fromisoformat(row["latest_end"])
    age_days = (date.today() - latest_end).days

    if age_days > max_age_days:
        return CheckResult(
            name="profile_freshness",
            status="warn",
            message=(
                f"Indicator profiles are stale: most recent window_end="
                f"{row['latest_end']} ({age_days} days ago, threshold: {max_age_days})"
            ),
            details=[
                f"Most recent profile window_end: {row['latest_end']} "
                f"({age_days} days old)"
            ],
        )

    return CheckResult(
        name="profile_freshness",
        status="pass",
        message=(
            f"Indicator profiles are fresh (most recent: {row['latest_end']}, "
            f"{age_days} days ago)"
        ),
    )


# ---------------------------------------------------------------------------
# Weekly checks
# ---------------------------------------------------------------------------

def check_weekly_candle_validity(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check weekly candle counts, OHLC validity, and volume plausibility.

    Validates:
    - At least 60% of expected weekly candles exist per ticker. Expected count
      is computed dynamically from the ticker's first date in ohlcv_daily
      (so a ticker with 2.5 years of data is compared against ~130 weeks, not
      260). Falls back to a fixed 5-year assumption if no OHLCV data exists.
    - high >= low for every candle.
    - Weekly volume is logged at INFO severity only. Values below
      trading_days_in_week × ``weekly_volume_min_ratio`` × local_avg_daily_volume
      (default ratio 0.30, configurable) are noted but do not raise a warning —
      volatile small-caps (ASTS, GME, ARM) can have legitimately quiet weeks and
      real data problems (missing days, wrong OHLC) are caught by the other checks.
      The reference volume is a rolling average from the ~60 trading days (±42
      calendar days) surrounding that specific week so early low-volume periods
      are compared against their own era. The most recent week_start per ticker
      is always skipped (partial week in progress).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "fail" for OHLC violations, "warn" for count issues,
        "pass" (with info details) for volume-only observations.
    """
    critical_issues: list[str] = []
    warning_issues: list[str] = []
    info_issues: list[str] = []
    placeholders = ",".join("?" * len(active_tickers))

    try:
        verify_cfg = load_config("verify_pipeline")
    except FileNotFoundError:
        verify_cfg = {}
    volume_min_ratio: float = float(
        verify_cfg.get("weekly_volume_min_ratio", _WEEKLY_VOLUME_MIN_RATIO)
    )

    # OHLC validity: high < low
    bad_ohlc = db_conn.execute(
        f"SELECT ticker, week_start, open, high, low, close "
        f"FROM weekly_candles "
        f"WHERE ticker IN ({placeholders}) AND high < low "
        f"LIMIT 20",
        active_tickers,
    ).fetchall()
    for row in bad_ohlc:
        critical_issues.append(
            f"{row['ticker']} {row['week_start']}: "
            f"high={row['high']} < low={row['low']} (invalid candle)"
        )

    # Count per ticker — dynamic: derive expected from actual data start date
    first_date_rows = db_conn.execute(
        f"SELECT ticker, MIN(date) AS first_date FROM ohlcv_daily "
        f"WHERE ticker IN ({placeholders}) GROUP BY ticker",
        active_tickers,
    ).fetchall()
    first_dates = {row["ticker"]: row["first_date"] for row in first_date_rows}

    count_rows = db_conn.execute(
        f"SELECT ticker, COUNT(*) AS cnt FROM weekly_candles "
        f"WHERE ticker IN ({placeholders}) GROUP BY ticker",
        active_tickers,
    ).fetchall()
    counts = {row["ticker"]: row["cnt"] for row in count_rows}

    today = date.today()
    for ticker in active_tickers:
        cnt = counts.get(ticker, 0)
        first_date_str = first_dates.get(ticker)
        if first_date_str:
            first_date_obj = date.fromisoformat(first_date_str)
            weeks_elapsed = max(1, (today - first_date_obj).days // 7)
            min_expected = int(weeks_elapsed * _WEEKLY_CANDLE_WARN_LOW_PCT)
        else:
            min_expected = int(
                _WEEKLY_CANDLE_YEARS * _WEEKLY_CANDLES_PER_YEAR * _WEEKLY_CANDLE_WARN_LOW_PCT
            )
        if cnt < min_expected:
            warning_issues.append(
                f"{ticker}: only {cnt} weekly candles "
                f"(expected >= {min_expected} based on data history)"
            )

    # Volume plausibility: compare each week against a ROLLING local average.
    # Uses ~60 trading days (±42 calendar days) around the week so early
    # low-volume periods aren't unfairly compared to a high-volume all-time avg.
    # Most recent week_start is always skipped (partial week in progress).
    volume_rows = db_conn.execute(
        f"""
        WITH latest_week AS (
          SELECT ticker, MAX(week_start) AS max_ws
          FROM weekly_candles
          WHERE ticker IN ({placeholders})
          GROUP BY ticker
        ),
        week_stats AS (
          SELECT
            w.ticker,
            w.week_start,
            w.volume AS weekly_vol,
            COUNT(o.date) AS day_count,
            (
              SELECT AVG(o_ref.volume)
              FROM ohlcv_daily o_ref
              WHERE o_ref.ticker = w.ticker
                AND o_ref.date >= date(w.week_start, '-42 days')
                AND o_ref.date <  date(w.week_start, '+49 days')
            ) AS local_avg_vol
          FROM weekly_candles w
          JOIN latest_week lw
            ON lw.ticker = w.ticker AND w.week_start != lw.max_ws
          LEFT JOIN ohlcv_daily o
            ON o.ticker = w.ticker
            AND o.date >= w.week_start
            AND o.date < date(w.week_start, '+7 days')
          WHERE w.ticker IN ({placeholders})
            AND w.volume IS NOT NULL
          GROUP BY w.ticker, w.week_start, w.volume
        )
        SELECT ticker, week_start, weekly_vol, day_count, local_avg_vol
        FROM week_stats
        WHERE day_count > 0
          AND local_avg_vol IS NOT NULL AND local_avg_vol > 0
          AND weekly_vol < day_count * {volume_min_ratio} * local_avg_vol
        LIMIT 10
        """,
        active_tickers * 2,
    ).fetchall()
    for row in volume_rows:
        msg = (
            f"{row['ticker']} {row['week_start']}: "
            f"weekly volume={row['weekly_vol']:,.0f} < "
            f"{row['day_count']} trading days × {volume_min_ratio} × "
            f"local avg daily={row['local_avg_vol']:,.0f}"
        )
        logger.info("Weekly volume low (informational): %s", msg)
        info_issues.append(msg)

    if critical_issues:
        return CheckResult(
            name="weekly_candle_validity",
            status="fail",
            message=f"{len(critical_issues)} invalid weekly candle OHLC value(s)",
            details=critical_issues + warning_issues + info_issues,
        )
    if warning_issues:
        return CheckResult(
            name="weekly_candle_validity",
            status="warn",
            message=f"{len(warning_issues)} weekly candle warning(s)",
            details=warning_issues + info_issues,
        )
    return CheckResult(
        name="weekly_candle_validity",
        status="pass",
        message="Weekly candle counts, OHLC, and volumes are valid",
        details=info_issues if info_issues else [],
    )


def check_weekly_indicator_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that every ticker with weekly candles also has weekly indicators.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any ticker has candles but no weekly indicators.
    """
    placeholders = ",".join("?" * len(active_tickers))

    candle_tickers = {
        row["ticker"]
        for row in db_conn.execute(
            f"SELECT DISTINCT ticker FROM weekly_candles WHERE ticker IN ({placeholders})",
            active_tickers,
        ).fetchall()
    }
    indicator_tickers = {
        row["ticker"]
        for row in db_conn.execute(
            f"SELECT DISTINCT ticker FROM indicators_weekly WHERE ticker IN ({placeholders})",
            active_tickers,
        ).fetchall()
    }

    missing = sorted(candle_tickers - indicator_tickers)
    if missing:
        return CheckResult(
            name="weekly_indicator_coverage",
            status="warn",
            message=f"{len(missing)} ticker(s) have weekly candles but no weekly indicators",
            details=[f"{t}: no rows in indicators_weekly" for t in missing],
        )

    return CheckResult(
        name="weekly_indicator_coverage",
        status="pass",
        message="Weekly indicator coverage matches weekly candle coverage",
    )


# ---------------------------------------------------------------------------
# News summary checks
# ---------------------------------------------------------------------------

def check_news_summary_consistency(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check that news_daily_summary counts and sentiment are consistent.

    Validates:
    - article_count matches actual count in news_articles for that ticker+date
    - avg_sentiment_score is within [-1.0, +1.0]
    - positive_count + negative_count + neutral_count == article_count

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any inconsistencies are found.
    """
    issues: list[str] = []
    placeholders = ",".join("?" * len(active_tickers))

    # Sentiment range check
    bad_sentiment = db_conn.execute(
        f"SELECT ticker, date, avg_sentiment_score FROM news_daily_summary "
        f"WHERE ticker IN ({placeholders}) "
        f"AND avg_sentiment_score IS NOT NULL "
        f"AND (avg_sentiment_score < -1 OR avg_sentiment_score > 1) "
        f"LIMIT 20",
        active_tickers,
    ).fetchall()
    for row in bad_sentiment:
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"avg_sentiment_score={row['avg_sentiment_score']:.3f} "
            "outside [-1, +1]"
        )

    # Count mismatch: summary article_count vs actual article count
    summary_rows = db_conn.execute(
        f"SELECT ticker, date, article_count FROM news_daily_summary "
        f"WHERE ticker IN ({placeholders})",
        active_tickers,
    ).fetchall()
    for summary in summary_rows:
        actual_count_row = db_conn.execute(
            "SELECT COUNT(*) AS cnt FROM news_articles "
            "WHERE ticker = ? AND date = ?",
            (summary["ticker"], summary["date"]),
        ).fetchone()
        actual_count = actual_count_row["cnt"] if actual_count_row else 0
        if summary["article_count"] != actual_count:
            issues.append(
                f"{summary['ticker']} {summary['date']}: "
                f"summary article_count={summary['article_count']} "
                f"but actual article count={actual_count}"
            )

    # Counts add up check
    bad_counts = db_conn.execute(
        f"SELECT ticker, date, article_count, positive_count, "
        f"negative_count, neutral_count FROM news_daily_summary "
        f"WHERE ticker IN ({placeholders}) "
        f"AND positive_count IS NOT NULL "
        f"AND negative_count IS NOT NULL "
        f"AND neutral_count IS NOT NULL "
        f"AND (positive_count + negative_count + neutral_count) != article_count "
        f"LIMIT 20",
        active_tickers,
    ).fetchall()
    for row in bad_counts:
        subtotal = (
            (row["positive_count"] or 0)
            + (row["negative_count"] or 0)
            + (row["neutral_count"] or 0)
        )
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"positive+negative+neutral={subtotal} "
            f"!= article_count={row['article_count']}"
        )

    if issues:
        return CheckResult(
            name="news_summary_consistency",
            status="warn",
            message=f"{len(issues)} news summary consistency issue(s) found",
            details=issues,
        )
    return CheckResult(
        name="news_summary_consistency",
        status="pass",
        message="News summary counts and sentiment scores are consistent",
    )


# ---------------------------------------------------------------------------
# Cross-table consistency
# ---------------------------------------------------------------------------

def check_scores_have_indicators(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> CheckResult:
    """
    Verify that every ticker in scores_daily has a corresponding indicators_daily row.

    Args:
        db_conn: Open SQLite connection.
        scoring_date: Date string (YYYY-MM-DD) to check.

    Returns:
        CheckResult with "warn" if any scored ticker has no indicator data.
    """
    scored_tickers = {
        row["ticker"]
        for row in db_conn.execute(
            "SELECT DISTINCT ticker FROM scores_daily WHERE date = ?",
            (scoring_date,),
        ).fetchall()
    }
    indicator_tickers = {
        row["ticker"]
        for row in db_conn.execute(
            "SELECT DISTINCT ticker FROM indicators_daily WHERE date = ?",
            (scoring_date,),
        ).fetchall()
    }

    missing = sorted(scored_tickers - indicator_tickers)
    if missing:
        return CheckResult(
            name="scores_have_indicators",
            status="warn",
            message=(
                f"{len(missing)} ticker(s) in scores_daily have no "
                f"indicators_daily row for {scoring_date}"
            ),
            details=[f"{t}: scored but no indicator row" for t in missing],
        )

    return CheckResult(
        name="scores_have_indicators",
        status="pass",
        message=f"All scored tickers have indicator data for {scoring_date}",
    )


def check_indicators_have_ohlcv(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that every (ticker, date) in indicators_daily has a matching ohlcv_daily row.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any indicator row has no OHLCV counterpart.
    """
    placeholders = ",".join("?" * len(active_tickers))

    orphan_rows = db_conn.execute(
        f"""
        SELECT DISTINCT i.ticker, i.date
        FROM indicators_daily i
        LEFT JOIN ohlcv_daily o ON i.ticker = o.ticker AND i.date = o.date
        WHERE i.ticker IN ({placeholders}) AND o.ticker IS NULL
        LIMIT 20
        """,
        active_tickers,
    ).fetchall()

    if not orphan_rows:
        return CheckResult(
            name="indicators_have_ohlcv",
            status="pass",
            message="All indicator rows have corresponding OHLCV data",
        )

    details = [
        f"{row['ticker']} {row['date']}: indicator exists but no OHLCV row"
        for row in orphan_rows
    ]
    return CheckResult(
        name="indicators_have_ohlcv",
        status="warn",
        message=f"{len(orphan_rows)} indicator row(s) missing OHLCV counterpart",
        details=details,
    )


def check_sr_levels_within_range(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify that all S/R level prices are within the historical price range of each ticker.

    An S/R level far outside the observed price range is likely a computation error.
    Uses a 20% buffer beyond historical min/max to allow for slight extrapolation.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with "warn" if any S/R level is outside the expected range.
    """
    issues: list[str] = []
    placeholders = ",".join("?" * len(active_tickers))

    price_ranges = db_conn.execute(
        f"SELECT ticker, MIN(low) AS price_min, MAX(high) AS price_max "
        f"FROM ohlcv_daily WHERE ticker IN ({placeholders}) GROUP BY ticker",
        active_tickers,
    ).fetchall()
    ranges: dict[str, tuple[float, float]] = {
        row["ticker"]: (row["price_min"], row["price_max"])
        for row in price_ranges
        if row["price_min"] is not None and row["price_max"] is not None
    }

    sr_rows = db_conn.execute(
        f"SELECT ticker, level_price, date_computed FROM support_resistance "
        f"WHERE ticker IN ({placeholders}) AND level_price IS NOT NULL",
        active_tickers,
    ).fetchall()

    for row in sr_rows:
        ticker = row["ticker"]
        if ticker not in ranges:
            continue
        price_min, price_max = ranges[ticker]
        buffer = (price_max - price_min) * 0.20
        lower_bound = price_min - buffer
        upper_bound = price_max + buffer

        if row["level_price"] < lower_bound or row["level_price"] > upper_bound:
            issues.append(
                f"{ticker}: S/R level_price={row['level_price']:.2f} "
                f"outside historical range [{price_min:.2f}, {price_max:.2f}]"
            )

    if issues:
        return CheckResult(
            name="sr_levels_within_range",
            status="warn",
            message=f"{len(issues)} S/R level(s) outside historical price range",
            details=issues,
        )
    return CheckResult(
        name="sr_levels_within_range",
        status="pass",
        message="All S/R levels are within historical price ranges",
    )


# ---------------------------------------------------------------------------
# Signal flip checks
# ---------------------------------------------------------------------------

def check_signal_flip_validity(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Verify signal flip validity:
    1. previous_signal != new_signal (a flip to the same signal is meaningless).
    2. Every flip date has a corresponding row in scores_daily.

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with "warn" if any violations are found.
    """
    issues: list[str] = []

    # Check signals actually differ
    same_signal_rows = db_conn.execute(
        "SELECT ticker, date, previous_signal, new_signal FROM signal_flips "
        "WHERE previous_signal = new_signal LIMIT 20"
    ).fetchall()
    for row in same_signal_rows:
        issues.append(
            f"{row['ticker']} {row['date']}: "
            f"flip has previous_signal = new_signal = {row['previous_signal']!r}"
        )

    # Check each flip date has a score
    flip_rows = db_conn.execute(
        "SELECT DISTINCT ticker, date FROM signal_flips "
        "WHERE previous_signal != new_signal OR new_signal IS NULL"
    ).fetchall()
    for row in flip_rows:
        score_exists = db_conn.execute(
            "SELECT 1 FROM scores_daily WHERE ticker = ? AND date = ?",
            (row["ticker"], row["date"]),
        ).fetchone()
        if not score_exists:
            issues.append(
                f"{row['ticker']} {row['date']}: "
                f"flip exists but no corresponding scores_daily row"
            )

    if issues:
        return CheckResult(
            name="signal_flip_validity",
            status="warn",
            message=f"{len(issues)} signal flip validity issue(s)",
            details=issues,
        )
    return CheckResult(
        name="signal_flip_validity",
        status="pass",
        message="All signal flips are valid",
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_full_pipeline_verification(
    db_path: Optional[str] = None,
    scoring_date: Optional[str] = None,
) -> VerificationReport:
    """
    Run all pipeline data verification checks and return a VerificationReport.

    Loads active tickers from tickers.json config, opens the database, resolves
    the scoring_date (defaults to latest date in scores_daily), and runs every
    check in order.

    Args:
        db_path: Path to the SQLite database file. If None, loads from database.json.
        scoring_date: Date string (YYYY-MM-DD) for score-related checks. If None,
                      uses the latest date present in scores_daily.

    Returns:
        VerificationReport aggregating all check results.
    """
    if db_path is None:
        db_config = load_config("database")
        db_path = db_config["path"]

    logger.info(f"Running full pipeline verification against: {db_path}")

    active_ticker_dicts = get_active_tickers()
    active_tickers = [t["symbol"] for t in active_ticker_dicts]
    logger.info(f"Verifying {len(active_tickers)} active tickers")

    db_conn = get_connection(db_path)
    all_checks: list[CheckResult] = []

    try:
        # Resolve scoring_date
        if scoring_date is None:
            row = db_conn.execute(
                "SELECT MAX(date) AS latest FROM scores_daily"
            ).fetchone()
            scoring_date = row["latest"] if row and row["latest"] else date.today().isoformat()
            logger.info(f"Resolved scoring_date={scoring_date}")

        # ── Indicator checks ─────────────────────────────────────────────
        all_checks.append(check_indicator_ranges(db_conn, active_tickers))
        all_checks.append(check_indicator_coverage(db_conn, active_tickers))
        all_checks.append(check_indicator_date_alignment(db_conn, active_tickers))
        all_checks.append(check_indicator_null_percentage(db_conn, active_tickers))

        # ── Score checks ──────────────────────────────────────────────────
        all_checks.append(check_score_ranges(db_conn, scoring_date))
        all_checks.append(check_category_score_ranges(db_conn, scoring_date))
        all_checks.append(check_confidence_range(db_conn, scoring_date))
        all_checks.append(check_signal_score_consistency(db_conn, scoring_date))
        all_checks.append(check_signal_distribution(db_conn, scoring_date))
        all_checks.append(check_confidence_distribution(db_conn, scoring_date))
        all_checks.append(check_weighted_score_math(db_conn, scoring_date))
        all_checks.append(check_regime_values(db_conn, scoring_date))
        all_checks.append(check_json_fields(db_conn, scoring_date))

        # ── Pattern checks ────────────────────────────────────────────────
        all_checks.append(check_pattern_counts(db_conn, active_tickers))
        all_checks.append(check_pattern_duplicates(db_conn, active_tickers))
        all_checks.append(check_pattern_field_validity(db_conn))

        # ── Divergence checks ─────────────────────────────────────────────
        all_checks.append(check_divergence_counts(db_conn, active_tickers))
        all_checks.append(check_divergence_consistency(db_conn))

        # ── Crossover checks ──────────────────────────────────────────────
        all_checks.append(check_crossover_validity(db_conn))

        # ── Profile checks ────────────────────────────────────────────────
        all_checks.append(check_profile_coverage(db_conn, active_tickers))
        all_checks.append(check_profile_percentile_order(db_conn))
        all_checks.append(check_profile_freshness(db_conn))

        # ── Weekly checks ─────────────────────────────────────────────────
        all_checks.append(check_weekly_candle_validity(db_conn, active_tickers))
        all_checks.append(check_weekly_indicator_coverage(db_conn, active_tickers))

        # ── News checks ───────────────────────────────────────────────────
        all_checks.append(check_news_summary_consistency(db_conn, active_tickers))

        # ── Cross-table checks ────────────────────────────────────────────
        all_checks.append(check_scores_have_indicators(db_conn, scoring_date))
        all_checks.append(check_indicators_have_ohlcv(db_conn, active_tickers))
        all_checks.append(check_sr_levels_within_range(db_conn, active_tickers))

        # ── Signal flip checks ────────────────────────────────────────────
        all_checks.append(check_signal_flip_validity(db_conn))

    finally:
        db_conn.close()

    pass_count = sum(1 for c in all_checks if c.status == "pass")
    warn_count = sum(1 for c in all_checks if c.status == "warn")
    fail_count = sum(1 for c in all_checks if c.status == "fail")
    overall_status = "FAIL" if fail_count > 0 else "PASS"
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    logger.info(
        f"Pipeline verification complete — {overall_status}: "
        f"{pass_count} pass, {warn_count} warn, {fail_count} fail"
    )

    return VerificationReport(
        checks=all_checks,
        overall_status=overall_status,
        pass_count=pass_count,
        warn_count=warn_count,
        fail_count=fail_count,
        timestamp=timestamp,
    )


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------

def format_pipeline_verification_report(report: VerificationReport) -> str:
    """
    Format a VerificationReport as a human-readable string for console and Telegram.

    Uses emoji indicators: ✅ pass, ⚠️ warn, ❌ fail. Details lists longer than
    20 items are truncated with an "... and N more" suffix to keep the message
    under the Telegram 4096-character limit.

    Args:
        report: The VerificationReport to format.

    Returns:
        A formatted string suitable for both console output and Telegram messages.
    """
    timestamp_short = report.timestamp[:10]
    lines: list[str] = [
        f"📋 Pipeline Verification Report — {timestamp_short}",
        "",
    ]

    for check in report.checks:
        emoji = _STATUS_EMOJI.get(check.status, "❓")
        lines.append(f"{emoji} {check.name}: {check.message}")

        if check.details:
            displayed = check.details[:_MAX_DETAIL_ITEMS]
            for detail in displayed:
                lines.append(f"   • {detail}")
            remaining = len(check.details) - _MAX_DETAIL_ITEMS
            if remaining > 0:
                lines.append(f"   ... and {remaining} more")

    lines.append("")
    lines.append("─" * 33)
    overall_emoji = "✅" if report.overall_status == "PASS" else "❌"
    lines.append(f"{overall_emoji} Overall: {report.overall_status}")
    lines.append(
        f"✅ {report.pass_count} passed | "
        f"⚠️ {report.warn_count} warnings | "
        f"❌ {report.fail_count} failed"
    )

    message = "\n".join(lines)

    if len(message) > _TELEGRAM_MAX_LENGTH:
        message = message[: _TELEGRAM_MAX_LENGTH - 3] + "..."

    return message
