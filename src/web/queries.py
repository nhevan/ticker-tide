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
from datetime import date, datetime, timedelta
from typing import Any, Optional

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


def fetch_snapshot(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    config: dict,
) -> dict[str, Any]:
    """
    Build the full three-card snapshot dict for a ticker and picked date.

    Resolves daily, weekly, and monthly data independently. Each section
    includes data_available, categories (the UI rendering contract), scores,
    indicators, patterns, sparkline, and period metadata.

    For daily: exact match on scores_daily.date.
    For weekly: most-recent scores_weekly.week_start <= picked_date.
    For monthly: most-recent scores_monthly.month_start <= picked_date.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        picked_date: ISO date string (YYYY-MM-DD) selected by the user.
        config: Web config dict containing the 'sparkline' section.

    Returns:
        Dict with keys 'daily', 'weekly', 'monthly', each a section dict.
    """
    sparkline_cfg = config.get("sparkline", {})
    daily_days = sparkline_cfg.get("daily_days", 15)
    weekly_weeks = sparkline_cfg.get("weekly_weeks", 6)
    monthly_months = sparkline_cfg.get("monthly_months", 6)

    why_limit = config.get("why_bullets", {}).get("limit", 3)
    signal_flip_lookback = config.get("signal_flip_lookback_days", 14)

    return {
        "daily": _build_daily_section(
            conn, ticker, picked_date, daily_days,
            why_limit=why_limit,
            signal_flip_lookback_days=signal_flip_lookback,
        ),
        "weekly": _build_weekly_section(conn, ticker, picked_date, weekly_weeks),
        "monthly": _build_monthly_section(conn, ticker, picked_date, monthly_months),
    }


def _build_daily_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_days: int,
    why_limit: int = 3,
    signal_flip_lookback_days: int = 14,
) -> dict[str, Any]:
    """
    Build the daily card data for a ticker and exact date.

    Returns a dict with data_available=False if no row exists for that date.
    When available, includes all 9 categories, scores, indicators, patterns,
    sparkline, signal, confidence, calibrated_score, resolved_period,
    key_signals (top N why-bullets), earnings (next + last_surprise),
    and signal_flip (most recent flip within the lookback window).

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Exact date to look up (YYYY-MM-DD).
        sparkline_days: Number of trading days to include in the sparkline.
        why_limit: Maximum number of key_signals items to include (from config).
        signal_flip_lookback_days: Number of days to look back for a signal flip.

    Returns:
        Daily section dict.
    """
    score_row = conn.execute(
        "SELECT * FROM scores_daily WHERE ticker = ? AND date = ?",
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
    sparkline = _fetch_daily_sparkline(conn, ticker, picked_date, sparkline_days)
    key_signals = _extract_key_signals(score_dict, limit=why_limit)
    earnings = _fetch_earnings(conn, ticker, picked_date)
    signal_flip = _fetch_signal_flip(
        conn, ticker, picked_date, lookback_days=signal_flip_lookback_days
    )

    return {
        "data_available": True,
        "categories": _DAILY_CATEGORIES,
        "scores": _extract_daily_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "sparkline": sparkline,
        "signal": score_dict.get("signal"),
        "confidence": score_dict.get("confidence"),
        "calibrated_score": score_dict.get("calibrated_score"),
        "composite_score": score_dict.get("final_score"),
        "resolved_period": picked_date,
        "key_signals": key_signals,
        "earnings": earnings,
        "signal_flip": signal_flip,
    }


def _build_weekly_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_weeks: int,
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
    sparkline = _fetch_weekly_sparkline(conn, ticker, picked_date, sparkline_weeks)
    period_label = _format_weekly_period_label(week_start)

    return {
        "data_available": True,
        "categories": _WEEKLY_CATEGORIES,
        "scores": _extract_timeframe_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "sparkline": sparkline,
        "composite_score": score_dict.get("composite_score"),
        "resolved_period": week_start,
        "resolved_period_label": period_label,
        "is_fallback": is_fallback,
    }


def _build_monthly_section(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    sparkline_months: int,
) -> dict[str, Any]:
    """
    Build the monthly card data for a ticker as of the picked date.

    Resolves to the most recent month_start <= picked_date in scores_monthly.
    Sets is_fallback=True when resolved_period < picked_date.
    Candlestick is intentionally excluded from the categories array even though
    candlestick_score exists as a column (always NULL for monthly — decay mismatch).

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        sparkline_months: Number of monthly bars to include in the sparkline.

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
    sparkline = _fetch_monthly_sparkline(conn, ticker, picked_date, sparkline_months)
    period_label = _format_monthly_period_label(month_start)

    return {
        "data_available": True,
        "categories": _MONTHLY_CATEGORIES,
        "scores": _extract_timeframe_scores(score_dict),
        "indicators": indicators,
        "patterns": patterns,
        "sparkline": sparkline,
        "composite_score": score_dict.get("composite_score"),
        "resolved_period": month_start,
        "resolved_period_label": period_label,
        "is_fallback": is_fallback,
    }


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
