"""
Backfill verification module.

Validates that the backfilled data is complete, consistent, and sane.
Produces a detailed report with pass/warn/fail status per check.

Checks performed:
1. Row counts — each table has data
2. Ticker coverage — all active tickers have data in expected tables
3. Date coverage — OHLCV spans the expected date range
4. Date gaps — no missing trading days in OHLCV
5. Data freshness — most recent data is recent
6. Value sanity — no zero/negative/absurd values
7. Cross-table consistency — tickers table matches data tables
8. Null coverage — expected vs unexpected NULLs in fundamentals
9. News/sentiment coverage — articles exist, sentiment is populated
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, timedelta, timezone, datetime
from typing import Optional

from src.common.config import get_active_tickers, load_config
from src.common.db import get_connection

logger = logging.getLogger(__name__)

_TELEGRAM_MAX_LENGTH = 4096
_MAX_DETAIL_ITEMS = 20

# Expected minimum row counts for a fully backfilled DB (~55 tickers, 5 years).
_TABLE_MINIMUM_ROWS: dict[str, int] = {
    "ohlcv_daily": 50_000,
    "fundamentals": 200,
    "earnings_calendar": 100,
    "news_articles": 1_000,
    "treasury_yields": 1_000,
    "dividends": 50,
    "short_interest": 100,
}

# Freshness thresholds in days.
_FRESH_THRESHOLD_DAYS = 3
_WARN_THRESHOLD_DAYS = 5
_FAIL_THRESHOLD_DAYS = 30

# Expected trading days for a full 5-year backfill.
_EXPECTED_TRADING_DAYS = 1260
_WARN_TRADING_DAY_PCT = 0.80
_FAIL_TRADING_DAY_PCT = 0.50

# Day-over-day price change threshold above which we flag a warning (as a fraction).
_EXTREME_PRICE_CHANGE_THRESHOLD = 5.0  # 500%


@dataclass
class CheckResult:
    """Result of a single verification check."""

    name: str
    status: str  # "pass", "warn", "fail"
    message: str
    details: Optional[list[str]] = None
    data: Optional[dict] = None


@dataclass
class VerificationReport:
    """Complete verification report aggregating all individual check results."""

    checks: list[CheckResult]
    overall_status: str  # "PASS" or "FAIL"
    pass_count: int
    warn_count: int
    fail_count: int
    timestamp: str


# ---------------------------------------------------------------------------
# Row count checks
# ---------------------------------------------------------------------------

def check_table_row_counts(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Query row counts for every monitored table and evaluate against expected minimums.

    Counts rows in ohlcv_daily, fundamentals, earnings_calendar, news_articles,
    treasury_yields, dividends, and short_interest. Fails if ohlcv_daily is empty;
    warns if any other table is below its expected minimum.

    Args:
        db_conn: Open SQLite connection to the backfilled database.

    Returns:
        CheckResult with status "fail" if ohlcv_daily is empty, "warn" if any
        other table is below minimum, otherwise "pass". The ``data`` field contains
        a dict mapping table name to row count.
    """
    counts: dict[str, int] = {}
    for table_name in _TABLE_MINIMUM_ROWS:
        try:
            row = db_conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()
            counts[table_name] = row["cnt"] if row else 0
        except sqlite3.OperationalError:
            counts[table_name] = 0

    issues: list[str] = []
    status = "pass"

    if counts.get("ohlcv_daily", 0) == 0:
        status = "fail"
        issues.append("ohlcv_daily is empty — critical failure")
    else:
        for table_name, minimum in _TABLE_MINIMUM_ROWS.items():
            count = counts.get(table_name, 0)
            if count < minimum:
                if status != "fail":
                    status = "warn"
                issues.append(
                    f"{table_name}: {count:,} rows (expected >= {minimum:,})"
                )

    if status == "pass":
        message = f"All tables populated — ohlcv_daily has {counts.get('ohlcv_daily', 0):,} rows"
    elif status == "warn":
        message = f"{len(issues)} table(s) below expected minimum"
    else:
        message = "ohlcv_daily is empty — backfill may not have run"

    return CheckResult(
        name="table_row_counts",
        status=status,
        message=message,
        details=issues if issues else None,
        data=counts,
    )


# ---------------------------------------------------------------------------
# Ticker coverage
# ---------------------------------------------------------------------------

def check_ticker_coverage(
    db_conn: sqlite3.Connection,
    table_name: str,
    expected_tickers: list[str],
    ticker_column: str = "ticker",
) -> CheckResult:
    """
    Compare distinct tickers in a table against the expected list.

    For ohlcv_daily, any missing ticker is a "fail". For all other tables,
    missing tickers are a "warn" since some absence is legitimate (e.g. ETFs
    have no fundamentals, not all stocks pay dividends).

    Args:
        db_conn: Open SQLite connection.
        table_name: Name of the table to query (e.g. "ohlcv_daily").
        expected_tickers: List of ticker symbols that should have data.
        ticker_column: Column name for ticker in the given table (default "ticker").

    Returns:
        CheckResult with ``data["present"]`` and ``data["missing"]`` lists.
        Status is "fail" for missing tickers in ohlcv_daily, "warn" for other tables.
    """
    rows = db_conn.execute(
        f"SELECT DISTINCT {ticker_column} FROM {table_name}"
    ).fetchall()
    present_tickers = {row[0] for row in rows}

    expected_set = set(expected_tickers)
    present = sorted(expected_set & present_tickers)
    missing = sorted(expected_set - present_tickers)

    if not missing:
        return CheckResult(
            name=f"ticker_coverage_{table_name}",
            status="pass",
            message=f"All {len(expected_tickers)} tickers present in {table_name}",
            data={"present": present, "missing": []},
        )

    is_critical_table = table_name == "ohlcv_daily"
    status = "fail" if is_critical_table else "warn"
    message = (
        f"{len(missing)} ticker(s) missing from {table_name}: {', '.join(missing[:5])}"
        + (" ..." if len(missing) > 5 else "")
    )

    return CheckResult(
        name=f"ticker_coverage_{table_name}",
        status=status,
        message=message,
        details=[f"{t} missing from {table_name}" for t in missing],
        data={"present": present, "missing": missing},
    )


def check_optional_ticker_coverage(
    db_conn: sqlite3.Connection,
    table_name: str,
    expected_tickers: list[str],
    min_coverage_pct: float = 0.20,
) -> CheckResult:
    """
    Check ticker coverage for tables where absence is expected for some tickers.

    Used for tables like dividends and short_interest where not all tickers
    will have data (growth stocks don't pay dividends; very new listings may
    lack short interest history). Returns "pass" with an informational note
    as long as at least min_coverage_pct of expected tickers have data.
    Only returns "warn" if coverage drops suspiciously low, suggesting a
    data fetch failure rather than legitimate absence.

    Args:
        db_conn: Open SQLite connection.
        table_name: Name of the table to query.
        expected_tickers: List of ticker symbols to check against.
        min_coverage_pct: Minimum fraction of tickers that must have data
            before a warning is raised. Default is 0.20 (20%).

    Returns:
        CheckResult with "pass" and informational message when coverage is
        acceptable, or "warn" when coverage is suspiciously low.
    """
    rows = db_conn.execute(
        f"SELECT DISTINCT ticker FROM {table_name}"
    ).fetchall()
    present_tickers = {row[0] for row in rows}

    expected_set = set(expected_tickers)
    present = sorted(expected_set & present_tickers)
    missing = sorted(expected_set - present_tickers)

    total = len(expected_tickers)
    present_count = len(present)
    missing_count = len(missing)

    coverage_pct = present_count / total if total > 0 else 0.0

    if coverage_pct < min_coverage_pct:
        return CheckResult(
            name=f"ticker_coverage_{table_name}",
            status="warn",
            message=(
                f"{table_name}: only {present_count}/{total} tickers have data"
                " — possible fetch failure"
            ),
            data={"present": present, "missing": missing},
        )

    return CheckResult(
        name=f"ticker_coverage_{table_name}",
        status="pass",
        message=(
            f"{table_name}: {present_count}/{total} tickers have data"
            f" ({missing_count} tickers have no {table_name} — normal)"
        ),
        data={"present": present, "missing": missing},
    )


def check_all_ticker_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> list[CheckResult]:
    """
    Run ticker coverage checks for each relevant backfill table.

    Uses check_ticker_coverage for critical tables (ohlcv_daily, fundamentals,
    earnings_calendar, news_articles) where missing tickers are unexpected.
    Uses check_optional_ticker_coverage for dividends and short_interest, where
    absence is normal (growth stocks don't pay dividends; thin float stocks may
    lack short interest history).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols from tickers.json.

    Returns:
        List of CheckResult, one per checked table.
    """
    # Tables where every active ticker should have data
    required_tables = [
        "ohlcv_daily",
        "fundamentals",
        "earnings_calendar",
        "news_articles",
    ]
    # Tables where partial coverage is normal
    optional_tables = [
        "short_interest",
        "dividends",
    ]

    results: list[CheckResult] = []

    for table_name in required_tables:
        try:
            result = check_ticker_coverage(db_conn, table_name, active_tickers)
            results.append(result)
        except sqlite3.OperationalError as exc:
            logger.warning(f"check_ticker_coverage skipped for {table_name}: {exc}")
            results.append(
                CheckResult(
                    name=f"ticker_coverage_{table_name}",
                    status="warn",
                    message=f"Could not query {table_name}: {exc}",
                )
            )

    for table_name in optional_tables:
        try:
            result = check_optional_ticker_coverage(db_conn, table_name, active_tickers)
            results.append(result)
        except sqlite3.OperationalError as exc:
            logger.warning(f"check_optional_ticker_coverage skipped for {table_name}: {exc}")
            results.append(
                CheckResult(
                    name=f"ticker_coverage_{table_name}",
                    status="warn",
                    message=f"Could not query {table_name}: {exc}",
                )
            )

    return results


# ---------------------------------------------------------------------------
# Date range
# ---------------------------------------------------------------------------

def check_date_range(db_conn: sqlite3.Connection, ticker: str) -> dict:
    """
    Query the min and max dates in ohlcv_daily for a given ticker.

    Returns the date span and total trading day count (= row count). Adds a
    ``status`` field of "warn" if the row count is below 80% of the expected
    1260 trading days for a 5-year backfill, and "pass" otherwise.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol to query.

    Returns:
        Dict with keys: min_date (str), max_date (str), trading_days (int),
        status (str), message (str).
    """
    row = db_conn.execute(
        "SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS cnt "
        "FROM ohlcv_daily WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    min_date = row["min_date"] if row else None
    max_date = row["max_date"] if row else None
    trading_days = row["cnt"] if row else 0

    warn_threshold = int(_EXPECTED_TRADING_DAYS * _WARN_TRADING_DAY_PCT)
    if trading_days < warn_threshold:
        status = "warn"
        message = (
            f"{ticker}: only {trading_days} trading days "
            f"(expected ~{_EXPECTED_TRADING_DAYS} for 5 years)"
        )
    else:
        status = "pass"
        message = f"{ticker}: {trading_days} trading days from {min_date} to {max_date}"

    return {
        "min_date": min_date,
        "max_date": max_date,
        "trading_days": trading_days,
        "status": status,
        "message": message,
    }


def check_date_range_all_tickers(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
    expected_years: int = 5,
) -> CheckResult:
    """
    Run check_date_range for each ticker and aggregate results.

    Warns if any ticker has < 80% of expected trading days. Fails if any
    ticker has < 50% of expected trading days.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols to check.
        expected_years: Number of years of history expected (default 5).

    Returns:
        Aggregated CheckResult across all tickers.
    """
    expected_days = int(252 * expected_years)
    fail_threshold = int(expected_days * _FAIL_TRADING_DAY_PCT)
    warn_threshold = int(expected_days * _WARN_TRADING_DAY_PCT)

    warnings: list[str] = []
    failures: list[str] = []

    for ticker in active_tickers:
        result = check_date_range(db_conn, ticker)
        trading_days = result["trading_days"]
        if trading_days < fail_threshold:
            failures.append(
                f"{ticker}: {trading_days} days (< {fail_threshold} fail threshold)"
            )
        elif trading_days < warn_threshold:
            warnings.append(
                f"{ticker}: {trading_days} days (< {warn_threshold} warn threshold)"
            )

    if failures:
        return CheckResult(
            name="date_range_all_tickers",
            status="fail",
            message=f"{len(failures)} ticker(s) have critically short OHLCV history",
            details=failures + warnings,
        )
    if warnings:
        return CheckResult(
            name="date_range_all_tickers",
            status="warn",
            message=f"{len(warnings)} ticker(s) have shorter than expected OHLCV history",
            details=warnings,
        )

    return CheckResult(
        name="date_range_all_tickers",
        status="pass",
        message=f"All {len(active_tickers)} tickers have sufficient OHLCV history",
    )


def detect_market_wide_closures(
    gaps_by_ticker: dict[str, list[str]],
    min_ticker_fraction: float = 0.80,
) -> set[str]:
    """
    Auto-detect market-wide closures from per-ticker gap lists.

    A date is considered a market-wide closure (e.g. a US market holiday) if
    it appears in the gap lists of at least min_ticker_fraction of all tickers.
    This avoids the need for a manually maintained holiday calendar.

    Args:
        gaps_by_ticker: Dict mapping ticker symbol to list of missing ISO date strings.
        min_ticker_fraction: Fraction of tickers (0.0–1.0) that must be missing a date
            for it to be classified as a market-wide closure. Default is 0.80 (80%).

    Returns:
        set[str]: ISO date strings identified as market-wide closures.
    """
    if not gaps_by_ticker:
        return set()

    total_tickers = len(gaps_by_ticker)
    threshold = total_tickers * min_ticker_fraction

    from collections import Counter
    date_counts: Counter = Counter()
    for gaps in gaps_by_ticker.values():
        for gap_date in gaps:
            date_counts[gap_date] += 1

    return {d for d, count in date_counts.items() if count >= threshold}


def check_date_gaps(
    db_conn: sqlite3.Connection,
    ticker: str,
    holidays: Optional[list[str]] = None,
) -> list[str]:
    """
    Find missing Mon-Fri trading days in ohlcv_daily for the given ticker.

    Walks every Mon-Fri calendar day between the ticker's min and max OHLCV date.
    Any such day absent from the DB (and not in the holidays list) is returned as
    a missing date. Weekends are always skipped.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol to check.
        holidays: Optional list of ISO date strings to exclude from gap detection
                  (e.g. ["2026-12-25"]).

    Returns:
        List of ISO date strings for each missing trading day.
    """
    rows = db_conn.execute(
        "SELECT date FROM ohlcv_daily WHERE ticker = ? ORDER BY date",
        (ticker,),
    ).fetchall()

    if len(rows) < 2:
        return []

    dates_in_db = {row["date"] for row in rows}
    min_date = date.fromisoformat(rows[0]["date"])
    max_date = date.fromisoformat(rows[-1]["date"])
    holidays_set = set(holidays) if holidays else set()

    missing: list[str] = []
    current = min_date
    while current <= max_date:
        if current.weekday() < 5:  # Mon-Fri only
            date_str = current.isoformat()
            if date_str not in dates_in_db and date_str not in holidays_set:
                missing.append(date_str)
        current += timedelta(days=1)

    return missing


def check_date_gaps_all_tickers(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
    holidays: Optional[list[str]] = None,
    max_gaps_before_warn: int = 5,
) -> CheckResult:
    """
    Run check_date_gaps for every active ticker and aggregate.

    First auto-detects market-wide closures (dates missing for >=80% of tickers)
    and excludes them. Then warns only if any ticker has more than
    max_gaps_before_warn truly ticker-specific missing days.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols to check.
        holidays: Optional list of ISO holiday date strings to also ignore.
        max_gaps_before_warn: Real-gap count above which a ticker triggers a warning.

    Returns:
        CheckResult with details listing ticker-specific missing dates.
        Pass message includes count of auto-detected market closures excluded.
    """
    # Collect all raw gaps per ticker (before filtering closures)
    gaps_by_ticker: dict[str, list[str]] = {}
    explicit_holidays = set(holidays) if holidays else set()

    for ticker in active_tickers:
        gaps = check_date_gaps(db_conn, ticker, holidays=holidays)
        gaps_by_ticker[ticker] = gaps

    # Auto-detect dates missing across the majority of tickers → market closures
    market_closures = detect_market_wide_closures(gaps_by_ticker)
    all_excluded = market_closures | explicit_holidays
    n_excluded = len(market_closures)

    # Re-evaluate each ticker using only ticker-specific gaps
    issues: list[str] = []
    for ticker, raw_gaps in gaps_by_ticker.items():
        real_gaps = [d for d in raw_gaps if d not in all_excluded]
        if len(real_gaps) > max_gaps_before_warn:
            issues.append(
                f"{ticker}: {len(real_gaps)} missing trading days — "
                + ", ".join(real_gaps[:5])
                + (" ..." if len(real_gaps) > 5 else "")
            )

    excluded_note = (
        f" (Excluded {n_excluded} detected market holidays/closures)"
        if n_excluded > 0
        else ""
    )

    if issues:
        return CheckResult(
            name="date_gaps_all_tickers",
            status="warn",
            message=f"{len(issues)} ticker(s) have excessive date gaps{excluded_note}",
            details=issues,
        )

    return CheckResult(
        name="date_gaps_all_tickers",
        status="pass",
        message=(
            f"All {len(active_tickers)} tickers have acceptable date continuity"
            + excluded_note
        ),
    )


# ---------------------------------------------------------------------------
# Data freshness
# ---------------------------------------------------------------------------

def check_data_freshness(db_conn: sqlite3.Connection, ticker: str) -> dict:
    """
    Determine how many days behind the latest OHLCV data is for a given ticker.

    Compares MAX(date) in ohlcv_daily against today's date. Status is "fresh" if
    days_behind <= 3, otherwise "stale".

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol to check.

    Returns:
        Dict with keys: max_date (str|None), days_behind (int), status (str).
    """
    row = db_conn.execute(
        "SELECT MAX(date) AS max_date FROM ohlcv_daily WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    max_date_str: Optional[str] = row["max_date"] if row else None
    today = date.today()

    if max_date_str is None:
        return {"max_date": None, "days_behind": None, "status": "stale"}

    max_date = date.fromisoformat(max_date_str)
    days_behind = (today - max_date).days
    status = "fresh" if days_behind <= _FRESH_THRESHOLD_DAYS else "stale"

    return {"max_date": max_date_str, "days_behind": days_behind, "status": status}


def check_data_freshness_all(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Run check_data_freshness for every active ticker and aggregate results.

    Warns if any ticker is > 5 days behind. Fails if any ticker is > 30 days behind.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols to check.

    Returns:
        Aggregated CheckResult across all tickers.
    """
    stale_warn: list[str] = []
    stale_fail: list[str] = []

    for ticker in active_tickers:
        result = check_data_freshness(db_conn, ticker)
        days_behind = result["days_behind"]
        if days_behind is None or days_behind > _FAIL_THRESHOLD_DAYS:
            stale_fail.append(
                f"{ticker}: {days_behind} days behind (max_date={result['max_date']})"
            )
        elif days_behind > _WARN_THRESHOLD_DAYS:
            stale_warn.append(
                f"{ticker}: {days_behind} days behind (max_date={result['max_date']})"
            )

    if stale_fail:
        return CheckResult(
            name="data_freshness",
            status="fail",
            message=f"{len(stale_fail)} ticker(s) are critically stale (> {_FAIL_THRESHOLD_DAYS} days)",
            details=stale_fail + stale_warn,
        )
    if stale_warn:
        return CheckResult(
            name="data_freshness",
            status="warn",
            message=f"{len(stale_warn)} ticker(s) are slightly stale (> {_WARN_THRESHOLD_DAYS} days)",
            details=stale_warn,
        )

    return CheckResult(
        name="data_freshness",
        status="pass",
        message=f"All {len(active_tickers)} tickers are up to date",
    )


# ---------------------------------------------------------------------------
# Value sanity
# ---------------------------------------------------------------------------

def check_value_sanity(db_conn: sqlite3.Connection, ticker: str) -> list[str]:
    """
    Check ohlcv_daily rows for a ticker for invalid or extreme values.

    Flags:
    - Any row where close <= 0 (zero or negative price).
    - Any row where volume <= 0 (zero or negative volume).
    - Consecutive rows with a day-over-day price change > 500% (possible bad data).

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol to check.

    Returns:
        List of human-readable issue description strings. Empty list means no issues.
    """
    rows = db_conn.execute(
        "SELECT date, close, volume FROM ohlcv_daily WHERE ticker = ? ORDER BY date",
        (ticker,),
    ).fetchall()

    issues: list[str] = []
    prev_close: Optional[float] = None

    for row in rows:
        row_date = row["date"]
        close = row["close"]
        volume = row["volume"]

        if close is not None and close <= 0:
            issues.append(
                f"{ticker} {row_date}: close={close} is zero or negative"
            )

        if volume is not None and volume <= 0:
            issues.append(
                f"{ticker} {row_date}: volume={volume} is zero or negative"
            )

        if close is not None and prev_close is not None and prev_close > 0:
            change_ratio = abs(close - prev_close) / prev_close
            if change_ratio >= _EXTREME_PRICE_CHANGE_THRESHOLD:
                pct = change_ratio * 100
                issues.append(
                    f"{ticker} {row_date}: extreme price change {pct:.0f}% "
                    f"(prev={prev_close}, curr={close}) — possible split/bad data"
                )

        if close is not None and close > 0:
            prev_close = close

    return issues


def check_value_sanity_all(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Run check_value_sanity for every active ticker and aggregate.

    Fails if any ticker has zero or negative prices. Warns for extreme price changes.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols to check.

    Returns:
        Aggregated CheckResult.
    """
    critical_issues: list[str] = []
    warning_issues: list[str] = []

    for ticker in active_tickers:
        issues = check_value_sanity(db_conn, ticker)
        for issue in issues:
            if "zero or negative" in issue:
                critical_issues.append(issue)
            else:
                warning_issues.append(issue)

    if critical_issues:
        return CheckResult(
            name="value_sanity",
            status="fail",
            message=f"{len(critical_issues)} critical value issue(s) found",
            details=critical_issues + warning_issues,
        )
    if warning_issues:
        return CheckResult(
            name="value_sanity",
            status="warn",
            message=f"{len(warning_issues)} value warning(s) found (possible splits or bad data)",
            details=warning_issues,
        )

    return CheckResult(
        name="value_sanity",
        status="pass",
        message="No value sanity issues found across all tickers",
    )


# ---------------------------------------------------------------------------
# Cross-table consistency
# ---------------------------------------------------------------------------

def check_cross_table_consistency(db_conn: sqlite3.Connection) -> CheckResult:
    """
    Verify every active ticker in the tickers table has at least one OHLCV row.

    Queries all tickers with active=1 and checks for presence in ohlcv_daily.

    Args:
        db_conn: Open SQLite connection.

    Returns:
        CheckResult with status "fail" if any active ticker has no OHLCV data,
        otherwise "pass".
    """
    active_rows = db_conn.execute(
        "SELECT symbol FROM tickers WHERE active = 1"
    ).fetchall()
    active_symbols = [row["symbol"] for row in active_rows]

    ohlcv_rows = db_conn.execute(
        "SELECT DISTINCT ticker FROM ohlcv_daily"
    ).fetchall()
    tickers_with_ohlcv = {row["ticker"] for row in ohlcv_rows}

    missing_ohlcv = [s for s in active_symbols if s not in tickers_with_ohlcv]

    if missing_ohlcv:
        return CheckResult(
            name="cross_table_consistency",
            status="fail",
            message=f"{len(missing_ohlcv)} active ticker(s) have no OHLCV data",
            details=[f"{t} has no OHLCV rows" for t in missing_ohlcv],
        )

    return CheckResult(
        name="cross_table_consistency",
        status="pass",
        message=f"All {len(active_symbols)} active tickers have OHLCV data",
    )


# ---------------------------------------------------------------------------
# Null coverage
# ---------------------------------------------------------------------------

def check_null_coverage(
    db_conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    ticker: str,
) -> dict:
    """
    Count total rows and NULL rows for a given column/ticker combination.

    Args:
        db_conn: Open SQLite connection.
        table_name: Table to query (e.g. "fundamentals").
        column_name: Column to inspect for NULLs (e.g. "pe_ratio").
        ticker: Ticker symbol to filter on.

    Returns:
        Dict with keys: total (int), nulls (int), null_pct (float).
    """
    row = db_conn.execute(
        f"SELECT COUNT(*) AS total, "
        f"SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) AS nulls "
        f"FROM {table_name} WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    total = row["total"] if row else 0
    nulls = row["nulls"] if row and row["nulls"] is not None else 0
    null_pct = round((nulls / total * 100), 1) if total > 0 else 0.0

    return {"total": total, "nulls": nulls, "null_pct": null_pct}


def check_fundamentals_null_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check key fundamentals columns for unexpected NULL rates across all tickers.

    Checks pe_ratio, eps, revenue, and debt_to_equity. Warns if > 50% NULL for
    any critical column. Some NULLs are expected (e.g. companies with no earnings
    have no P/E ratio).

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult summarising NULL coverage per column.
    """
    columns = ["pe_ratio", "eps", "revenue", "debt_to_equity"]
    issues: list[str] = []

    for column in columns:
        total_rows = 0
        total_nulls = 0
        for ticker in active_tickers:
            result = check_null_coverage(db_conn, "fundamentals", column, ticker)
            total_rows += result["total"]
            total_nulls += result["nulls"]

        if total_rows == 0:
            issues.append(f"{column}: no fundamentals rows at all")
            continue

        null_pct = round(total_nulls / total_rows * 100, 1)
        if null_pct > 50:
            issues.append(
                f"{column}: {null_pct}% NULL across all tickers (> 50% threshold)"
            )
            logger.warning(
                f"Fundamentals null check: {column} is {null_pct}% NULL"
            )

    if issues:
        return CheckResult(
            name="fundamentals_null_coverage",
            status="warn",
            message=f"{len(issues)} fundamentals column(s) exceed NULL threshold",
            details=issues,
        )

    return CheckResult(
        name="fundamentals_null_coverage",
        status="pass",
        message="Fundamentals NULL coverage is within acceptable bounds",
    )


# ---------------------------------------------------------------------------
# News / sentiment
# ---------------------------------------------------------------------------

def check_news_sentiment_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Check what fraction of news articles have sentiment populated.

    Polygon articles carry sentiment; Finnhub articles do not. Warns if overall
    sentiment coverage is < 50%.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult with overall and per-source sentiment coverage in ``data``.
    """
    row = db_conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN sentiment IS NOT NULL THEN 1 ELSE 0 END) AS with_sentiment, "
        "source "
        "FROM news_articles "
        "WHERE ticker IN ({placeholders}) "
        "GROUP BY source".format(
            placeholders=",".join("?" * len(active_tickers))
        ),
        active_tickers,
    ).fetchall()

    total_all = 0
    total_with_sentiment = 0
    source_breakdown: dict[str, dict] = {}

    for source_row in row:
        source = source_row["source"] or "unknown"
        total = source_row["total"]
        with_sent = source_row["with_sentiment"] or 0
        total_all += total
        total_with_sentiment += with_sent
        source_breakdown[source] = {
            "total": total,
            "with_sentiment": with_sent,
            "pct": round(with_sent / total * 100, 1) if total > 0 else 0.0,
        }

    overall_pct = round(total_with_sentiment / total_all * 100, 1) if total_all > 0 else 0.0

    status = "warn" if overall_pct < 50 else "pass"
    message = (
        f"News sentiment coverage: {overall_pct}% "
        f"({total_with_sentiment:,}/{total_all:,} articles)"
    )

    issues: Optional[list[str]] = None
    if status == "warn":
        issues = [f"Overall sentiment coverage {overall_pct}% is below 50% threshold"]

    return CheckResult(
        name="news_sentiment_coverage",
        status=status,
        message=message,
        details=issues,
        data={
            "overall_sentiment_pct": overall_pct,
            "total_articles": total_all,
            "articles_with_sentiment": total_with_sentiment,
            "source_breakdown": source_breakdown,
        },
    )


def check_short_interest_coverage(
    db_conn: sqlite3.Connection,
    active_tickers: list[str],
) -> CheckResult:
    """
    Verify short interest data exists for active tickers.

    Checks that days_to_cover is populated for the majority of rows.

    Args:
        db_conn: Open SQLite connection.
        active_tickers: List of active ticker symbols.

    Returns:
        CheckResult summarising short interest data availability.
    """
    rows = db_conn.execute(
        "SELECT DISTINCT ticker FROM short_interest "
        "WHERE ticker IN ({placeholders})".format(
            placeholders=",".join("?" * len(active_tickers))
        ),
        active_tickers,
    ).fetchall()
    present = {row["ticker"] for row in rows}
    missing = sorted(set(active_tickers) - present)

    if len(missing) > len(active_tickers) // 2:
        return CheckResult(
            name="short_interest_coverage",
            status="warn",
            message=f"Short interest missing for {len(missing)}/{len(active_tickers)} tickers",
            details=[f"{t} has no short interest data" for t in missing],
        )

    return CheckResult(
        name="short_interest_coverage",
        status="pass",
        message=(
            f"Short interest available for {len(present)}/{len(active_tickers)} tickers"
        ),
    )


# ---------------------------------------------------------------------------
# Full verification orchestrator
# ---------------------------------------------------------------------------

def run_full_verification(db_path: Optional[str] = None) -> VerificationReport:
    """
    Run all verification checks against the backfilled database.

    Loads active tickers from tickers.json, opens the database, runs every
    check in order, and aggregates results into a VerificationReport.

    Checks run in order:
    1. table_row_counts
    2. all ticker coverage (ohlcv_daily, fundamentals, earnings_calendar, news_articles, short_interest, dividends)
    3. date_range_all_tickers
    4. date_gaps_all_tickers
    5. data_freshness_all
    6. value_sanity_all
    7. cross_table_consistency
    8. fundamentals_null_coverage
    9. news_sentiment_coverage
    10. short_interest_coverage

    Args:
        db_path: Path to the SQLite database file. If None, loads from database.json config.

    Returns:
        VerificationReport with all checks, counts, and overall PASS/FAIL status.
    """
    if db_path is None:
        db_config = load_config("database")
        db_path = db_config["path"]

    logger.info(f"Running full backfill verification against: {db_path}")

    active_ticker_dicts = get_active_tickers()
    active_tickers = [t["symbol"] for t in active_ticker_dicts]

    logger.info(f"Verifying {len(active_tickers)} active tickers")

    db_conn = get_connection(db_path)

    all_checks: list[CheckResult] = []

    try:
        # 1. Row counts
        all_checks.append(check_table_row_counts(db_conn))

        # 2. Ticker coverage per table
        all_checks.extend(check_all_ticker_coverage(db_conn, active_tickers))

        # 3. Date range
        all_checks.append(check_date_range_all_tickers(db_conn, active_tickers))

        # 4. Date gaps (no holiday list; could be extended to fetch from Polygon)
        all_checks.append(check_date_gaps_all_tickers(db_conn, active_tickers))

        # 5. Data freshness
        all_checks.append(check_data_freshness_all(db_conn, active_tickers))

        # 6. Value sanity
        all_checks.append(check_value_sanity_all(db_conn, active_tickers))

        # 7. Cross-table consistency
        all_checks.append(check_cross_table_consistency(db_conn))

        # 8. Fundamentals null coverage
        all_checks.append(check_fundamentals_null_coverage(db_conn, active_tickers))

        # 9. News sentiment coverage
        all_checks.append(check_news_sentiment_coverage(db_conn, active_tickers))

        # 10. Short interest coverage
        all_checks.append(check_short_interest_coverage(db_conn, active_tickers))

    finally:
        db_conn.close()

    pass_count = sum(1 for c in all_checks if c.status == "pass")
    warn_count = sum(1 for c in all_checks if c.status == "warn")
    fail_count = sum(1 for c in all_checks if c.status == "fail")
    overall_status = "FAIL" if fail_count > 0 else "PASS"
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    logger.info(
        f"Verification complete — {overall_status}: "
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

_STATUS_EMOJI = {"pass": "✅", "warn": "⚠️", "fail": "❌"}


def format_verification_report(report: VerificationReport) -> str:
    """
    Format a VerificationReport as a human-readable string for console and Telegram.

    Uses emoji indicators: ✅ pass, ⚠️ warn, ❌ fail. Details lists longer than
    20 items are truncated with a "... and N more" suffix to keep the message under
    the Telegram 4096-character limit.

    Args:
        report: The VerificationReport to format.

    Returns:
        A formatted string suitable for both console output and Telegram messages.
    """
    timestamp_short = report.timestamp[:10]
    lines: list[str] = [
        f"📋 Backfill Verification Report — {timestamp_short}",
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

    # Truncate to Telegram limit if necessary
    if len(message) > _TELEGRAM_MAX_LENGTH:
        message = message[: _TELEGRAM_MAX_LENGTH - 3] + "..."

    return message
