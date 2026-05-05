"""
LLM integration for the web UI.

Provides context builders and prompt generators for per-ticker analysis across
daily, weekly, and monthly timeframes. Reuses call_claude() from ai_reasoner.py
via a thin config adapter. Weekly/monthly analysis does not include news,
fundamentals, or macro data (daily-only scope).
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from src.notifier.ai_reasoner import build_ticker_context, call_claude

logger = logging.getLogger(__name__)

_TIMEFRAME_DISCLAIMER = (
    "Note: This {timeframe} analysis is based on {timeframe} indicators and patterns only. "
    "News sentiment, fundamentals, and macro context are not included."
)


def build_daily_context(
    conn: sqlite3.Connection,
    ticker: str,
    score_row: dict,
    scoring_date: str,
) -> str:
    """
    Build the full daily LLM context string for a ticker.

    Wraps build_ticker_context() from ai_reasoner.py, which includes indicators,
    patterns, divergences, crossovers, fundamentals, news, short interest, and more.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        score_row: Dict with daily score fields (signal, confidence, category scores, etc.).
        scoring_date: Date being analyzed (YYYY-MM-DD).

    Returns:
        Multi-section formatted context string for the LLM prompt.
    """
    return build_ticker_context(conn, ticker, score_row, scoring_date)


def build_timeframe_context(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    timeframe: str,
) -> str:
    """
    Build a weekly or monthly LLM context string for a ticker.

    Reads timeframe-specific indicators and patterns directly without including
    daily-only keys (signal, confidence, sentiment_score, news, fundamentals, macro).
    This avoids key-not-found errors that would occur if build_ticker_context()
    were called with a weekly/monthly score row.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        picked_date: Upper-bound date (YYYY-MM-DD) for resolving the period.
        timeframe: Either 'weekly' or 'monthly'.

    Returns:
        Formatted context string for the LLM prompt.
    """
    if timeframe == "weekly":
        return _build_weekly_context_string(conn, ticker, picked_date)
    elif timeframe == "monthly":
        return _build_monthly_context_string(conn, ticker, picked_date)
    else:
        logger.warning(f"Unknown timeframe {timeframe!r} passed to build_timeframe_context()")
        return f"Ticker: {ticker} | Timeframe: {timeframe} | Date: {picked_date}\nNo data available."


def _build_weekly_context_string(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
) -> str:
    """
    Build the LLM context string for the weekly timeframe.

    Queries indicators_weekly and patterns_weekly for the most recent row
    at or before picked_date. Does not include news, fundamentals, or macro data.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Formatted context string.
    """
    indicators_row = conn.execute(
        "SELECT * FROM indicators_weekly WHERE ticker = ? AND week_start <= ? "
        "ORDER BY week_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    score_row = conn.execute(
        "SELECT * FROM scores_weekly WHERE ticker = ? AND week_start <= ? "
        "ORDER BY week_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    patterns = _load_recent_patterns_for_timeframe(conn, ticker, picked_date, "weekly")

    week_start = indicators_row["week_start"] if indicators_row else picked_date
    score_dict = dict(score_row) if score_row else {}

    lines = [
        f"Ticker: {ticker} | Timeframe: Weekly | Period: week of {week_start} | Date: {picked_date}",
        f"Composite Score: {score_dict.get('composite_score', 'N/A')} | Regime: {score_dict.get('regime', 'N/A')}",
        "",
        "Weekly Category Scores (-100 to +100):",
        f"  Trend: {score_dict.get('trend_score', 'N/A')}",
        f"  Momentum: {score_dict.get('momentum_score', 'N/A')}",
        f"  Volume: {score_dict.get('volume_score', 'N/A')}",
        f"  Volatility: {score_dict.get('volatility_score', 'N/A')}",
        f"  Candlestick: {score_dict.get('candlestick_score', 'N/A')}",
        f"  Structural: {score_dict.get('structural_score', 'N/A')}",
        "",
    ]

    if indicators_row:
        ind = dict(indicators_row)
        lines += _format_indicator_lines(ind)

    lines += ["", "Weekly Patterns:"]
    if patterns:
        for pat in patterns:
            lines.append(
                f"  {pat.get('pattern_name', 'Unknown')} "
                f"({pat.get('direction', '')}, strength={pat.get('strength', '')})"
            )
    else:
        lines.append("  — none detected")

    return "\n".join(lines)


def _build_monthly_context_string(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
) -> str:
    """
    Build the LLM context string for the monthly timeframe.

    Queries indicators_monthly and patterns_monthly for the most recent row
    at or before picked_date. Does not include news, fundamentals, or macro data.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Formatted context string.
    """
    indicators_row = conn.execute(
        "SELECT * FROM indicators_monthly WHERE ticker = ? AND month_start <= ? "
        "ORDER BY month_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    score_row = conn.execute(
        "SELECT * FROM scores_monthly WHERE ticker = ? AND month_start <= ? "
        "ORDER BY month_start DESC LIMIT 1",
        (ticker, picked_date),
    ).fetchone()

    patterns = _load_recent_patterns_for_timeframe(conn, ticker, picked_date, "monthly")

    month_start = indicators_row["month_start"] if indicators_row else picked_date
    score_dict = dict(score_row) if score_row else {}

    lines = [
        f"Ticker: {ticker} | Timeframe: Monthly | Period: month of {month_start} | Date: {picked_date}",
        f"Composite Score: {score_dict.get('composite_score', 'N/A')} | Regime: {score_dict.get('regime', 'N/A')}",
        "",
        "Monthly Category Scores (-100 to +100):",
        f"  Trend: {score_dict.get('trend_score', 'N/A')}",
        f"  Momentum: {score_dict.get('momentum_score', 'N/A')}",
        f"  Volume: {score_dict.get('volume_score', 'N/A')}",
        f"  Volatility: {score_dict.get('volatility_score', 'N/A')}",
        f"  Structural: {score_dict.get('structural_score', 'N/A')}",
        "  Candlestick: N/A (not applicable to monthly bars)",
        "",
    ]

    if indicators_row:
        ind = dict(indicators_row)
        lines += _format_indicator_lines(ind)

    lines += ["", "Monthly Patterns:"]
    if patterns:
        for pat in patterns:
            lines.append(
                f"  {pat.get('pattern_name', 'Unknown')} "
                f"({pat.get('direction', '')}, strength={pat.get('strength', '')})"
            )
    else:
        lines.append("  — none detected")

    return "\n".join(lines)


def _load_recent_patterns_for_timeframe(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    timeframe: str,
) -> list[dict[str, Any]]:
    """
    Load patterns for a ticker from the timeframe-specific patterns table.

    For weekly, queries patterns_weekly for the most recent week_start <= picked_date.
    For monthly, queries patterns_monthly for the most recent month_start <= picked_date.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        picked_date: Upper-bound date (YYYY-MM-DD).
        timeframe: Either 'weekly' or 'monthly'.

    Returns:
        List of pattern dicts, or empty list if none found.
    """
    if timeframe == "weekly":
        resolved_row = conn.execute(
            "SELECT week_start FROM indicators_weekly "
            "WHERE ticker = ? AND week_start <= ? "
            "ORDER BY week_start DESC LIMIT 1",
            (ticker, picked_date),
        ).fetchone()
        if not resolved_row:
            return []
        period_key = resolved_row["week_start"]
        rows = conn.execute(
            "SELECT pattern_name, direction, strength FROM patterns_weekly "
            "WHERE ticker = ? AND week_start = ? ORDER BY strength DESC",
            (ticker, period_key),
        ).fetchall()
    else:
        resolved_row = conn.execute(
            "SELECT month_start FROM indicators_monthly "
            "WHERE ticker = ? AND month_start <= ? "
            "ORDER BY month_start DESC LIMIT 1",
            (ticker, picked_date),
        ).fetchone()
        if not resolved_row:
            return []
        period_key = resolved_row["month_start"]
        rows = conn.execute(
            "SELECT pattern_name, direction, strength FROM patterns_monthly "
            "WHERE ticker = ? AND month_start = ? ORDER BY strength DESC",
            (ticker, period_key),
        ).fetchall()

    return [dict(r) for r in rows]


def _format_indicator_lines(indicators: dict[str, Any]) -> list[str]:
    """
    Format a dict of indicator values into a list of display lines.

    Skips the ticker and date/period key columns. Formats each indicator
    as '  KEY: VALUE' with 2-decimal rounding for float values.

    Parameters:
        indicators: Dict of indicator key-value pairs from an indicators row.

    Returns:
        List of formatted string lines.
    """
    skip_keys = {"ticker", "date", "week_start", "month_start"}
    lines = ["Weekly/Monthly Indicators:"]
    for key, value in indicators.items():
        if key in skip_keys:
            continue
        if value is None:
            formatted = "N/A"
        elif isinstance(value, float):
            formatted = f"{value:.2f}"
        else:
            formatted = str(value)
        lines.append(f"  {key}: {formatted}")
    return lines


def analyze_daily(
    conn: sqlite3.Connection,
    ticker: str,
    score_row: dict,
    scoring_date: str,
    web_config: dict,
) -> str:
    """
    Generate Claude's analysis for a ticker on a specific daily date.

    Builds the full daily context (indicators, patterns, news, fundamentals, macro)
    and calls Claude via call_claude(). Uses the web config's ai_reasoner section
    which is structurally compatible with the notifier config shape.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        score_row: Dict with daily score fields required by build_ticker_context().
        scoring_date: Date being analyzed (YYYY-MM-DD).
        web_config: Web config dict containing the 'ai_reasoner' section.

    Returns:
        Claude's analysis text, or a fallback message on error.
    """
    context = build_daily_context(conn, ticker, score_row, scoring_date)
    prompt = _build_daily_prompt(ticker, context, scoring_date, web_config)
    adapted_config = {"ai_reasoner": web_config.get("ai_reasoner", {})}
    return call_claude(prompt, adapted_config)


def analyze_timeframe(
    conn: sqlite3.Connection,
    ticker: str,
    picked_date: str,
    timeframe: str,
    web_config: dict,
) -> str:
    """
    Generate Claude's analysis for a ticker on a weekly or monthly timeframe.

    Builds a timeframe-specific context (indicators + patterns only) and prepends
    a one-line disclaimer noting the limited input scope. Calls Claude via call_claude().

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        picked_date: Upper-bound date (YYYY-MM-DD) for resolving the period.
        timeframe: Either 'weekly' or 'monthly'.
        web_config: Web config dict containing the 'ai_reasoner' section.

    Returns:
        Claude's analysis text, or a fallback message on error.
    """
    context = build_timeframe_context(conn, ticker, picked_date, timeframe)
    prompt = _build_timeframe_prompt(ticker, context, picked_date, timeframe, web_config)
    adapted_config = {"ai_reasoner": web_config.get("ai_reasoner", {})}
    return call_claude(prompt, adapted_config)


def call_claude_for_web(
    conn: sqlite3.Connection,
    ticker: str,
    date_str: str,
    timeframe: str,
    score_row: dict,
    web_config: dict,
) -> str:
    """
    Dispatch to the appropriate analysis function based on timeframe.

    This is the single entry point called by the /api/llm route. Routes to
    analyze_daily() for 'daily' and analyze_timeframe() for 'weekly'/'monthly'.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        date_str: Picked date string (YYYY-MM-DD).
        timeframe: One of 'daily', 'weekly', 'monthly'.
        score_row: Score dict for daily timeframe (ignored for weekly/monthly).
        web_config: Web config dict containing the 'ai_reasoner' section.

    Returns:
        Claude's analysis text.
    """
    if timeframe == "daily":
        return analyze_daily(conn, ticker, score_row, date_str, web_config)
    elif timeframe in ("weekly", "monthly"):
        return analyze_timeframe(conn, ticker, date_str, timeframe, web_config)
    else:
        logger.warning(f"Unknown timeframe {timeframe!r} in call_claude_for_web()")
        return "Analysis unavailable — unknown timeframe."


# ── Prompt builders ───────────────────────────────────────────────────────────

def _build_daily_prompt(
    ticker: str,
    context: str,
    scoring_date: str,
    web_config: dict,
) -> str:
    """
    Build the Claude prompt for a daily ticker analysis.

    Parameters:
        ticker: Ticker symbol.
        context: Full daily context string from build_daily_context().
        scoring_date: Date being analyzed (YYYY-MM-DD).
        web_config: Web config dict with ai_reasoner.target_words.

    Returns:
        Complete prompt string to send to Claude.
    """
    target_words = web_config.get("ai_reasoner", {}).get("target_words", 150)
    return (
        f"Analyze the following daily technical data for {ticker} on {scoring_date}. "
        f"Provide a concise analysis of ~{target_words} words covering the most important "
        "signals, key support/resistance levels if visible, and any notable patterns. "
        "Focus on what matters most — skip boilerplate.\n\n"
        f"{context}"
    )


def _build_timeframe_prompt(
    ticker: str,
    context: str,
    picked_date: str,
    timeframe: str,
    web_config: dict,
) -> str:
    """
    Build the Claude prompt for a weekly or monthly ticker analysis.

    Prepends a disclaimer noting the limited input scope (indicators + patterns only).

    Parameters:
        ticker: Ticker symbol.
        context: Timeframe-specific context string from build_timeframe_context().
        picked_date: Upper-bound date (YYYY-MM-DD).
        timeframe: Either 'weekly' or 'monthly'.
        web_config: Web config dict with ai_reasoner.target_words.

    Returns:
        Complete prompt string to send to Claude.
    """
    target_words = web_config.get("ai_reasoner", {}).get("target_words", 150)
    disclaimer = _TIMEFRAME_DISCLAIMER.format(timeframe=timeframe)
    return (
        f"{disclaimer}\n\n"
        f"Analyze the following {timeframe} technical data for {ticker} as of {picked_date}. "
        f"Provide a concise analysis of ~{target_words} words covering the most important "
        "signals and trends visible in the indicators and patterns. "
        "Focus on what matters most — skip boilerplate.\n\n"
        f"{context}"
    )
