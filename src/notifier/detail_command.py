"""
/detail command handler for Telegram bot.

Usage: /detail AAPL [days]

Responds with 3 messages:
  1. Technical chart image (4-panel)
  2. AI analyst deep analysis (Claude)
  3. Raw data breakdown (all scores, indicators, levels, triggers, history, peers)

This is a read-only command — it does NOT modify any data or trigger any pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import date, timedelta
from typing import Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.config import load_config
from src.common.progress import edit_telegram_message, send_telegram_message
from src.notifier.chart_generator import cleanup_chart, generate_chart

logger = logging.getLogger(__name__)

_MAX_TELEGRAM_LENGTH = 4096

# Characters that must be escaped in MarkdownV2 (outside code blocks)
_MARKDOWN_V2_SPECIAL = r'_*[]()~`>#+-=|{}.!'

# Section markers for _split_message_at_section_markers — exact prefix matches only
_MSG2_SECTION_MARKERS = (
    "📍 VERDICT",
    "⏱️ TIMEFRAME SUMMARY",
    "🧠 REASONING",
    "📊 CONFIDENCE",
    "🎯 LEVELS & TRIGGERS",
)


# ---------------------------------------------------------------------------
# MarkdownV2 escaping
# ---------------------------------------------------------------------------

def escape_markdown_v2(text: str) -> str:
    """
    Escape Telegram MarkdownV2 special characters in text.

    Triple-backtick code blocks are passed through unchanged — their
    contents are not re-escaped.

    Parameters:
        text: Source text, possibly containing code blocks.

    Returns:
        Text with all required MarkdownV2 special characters escaped
        outside of code blocks.
    """
    # split on code-block delimiters; even-indexed segments are outside,
    # odd-indexed are inside code blocks (passed through verbatim)
    parts = text.split("```")
    for idx in range(0, len(parts), 2):
        for ch in _MARKDOWN_V2_SPECIAL:
            parts[idx] = parts[idx].replace(ch, f"\\{ch}")
    return "```".join(parts)


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------

def parse_detail_command(
    message_text: str, active_tickers: list[dict], config: dict
) -> dict:
    """
    Parse the /detail command text and validate inputs.

    Accepts:
      /detail AAPL          — use default_chart_days from config
      /detail AAPL 90       — use 90 days, clamped to max_chart_days

    Parameters:
        message_text: Raw Telegram message text.
        active_tickers: List of active ticker dicts (each has 'symbol' key).
        config: Notifier config dict containing config["detail_command"].

    Returns:
        {"ticker": str, "days": int} on success.
        {"error": str} on failure.
    """
    detail_cfg = config.get("detail_command", {})
    default_days = detail_cfg.get("default_chart_days", 30)
    max_days = detail_cfg.get("max_chart_days", 180)

    parts = message_text.strip().split()
    # parts[0] is "/detail"
    if len(parts) < 2:
        return {"error": "❌ Please provide a ticker symbol. Usage: /detail AAPL [days]"}

    ticker = parts[1].upper()
    active_symbols = [t["symbol"].upper() for t in active_tickers]

    if ticker not in active_symbols:
        return {"error": f"❌ Ticker {ticker} not found in active tickers."}

    days = default_days
    if len(parts) >= 3:
        try:
            days = int(parts[2])
        except ValueError:
            return {"error": f"❌ Invalid days value '{parts[2]}'. Please provide an integer."}

    days = min(days, max_days)

    return {"ticker": ticker, "days": days}


# ---------------------------------------------------------------------------
# Weekly / monthly score fetchers
# ---------------------------------------------------------------------------

def fetch_weekly_score(db_conn: sqlite3.Connection, ticker: str) -> dict | None:
    """
    Fetch the latest scores_weekly row for a ticker.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Dict of all columns for the latest week_start, or None if no row exists.
    """
    row = db_conn.execute(
        "SELECT * FROM scores_weekly WHERE ticker = ? ORDER BY week_start DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


def fetch_monthly_score(db_conn: sqlite3.Connection, ticker: str) -> dict | None:
    """
    Fetch the latest scores_monthly row for a ticker.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Dict of all columns for the latest month_start, or None if no row exists.
    """
    row = db_conn.execute(
        "SELECT * FROM scores_monthly WHERE ticker = ? ORDER BY month_start DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Msg #2 deterministic builders
# ---------------------------------------------------------------------------

def build_verdict_header(score: dict, earnings_row: dict | None, config: dict) -> str:
    """
    Build the verdict header line for msg #2.

    Prepends an earnings ⚠️ warning line when the next earnings date is within
    the configured window (inclusive boundary).

    Parameters:
        score: Score dict from scores_daily (must have 'ticker' and 'date').
        earnings_row: Upcoming earnings row dict (with 'earnings_date'), or None.
        config: Notifier config dict containing config["detail_command"].

    Returns:
        One or two lines: optional ⚠️ warning, then the header.
    """
    detail_cfg = config.get("detail_command", {})
    earnings_warning_days: int = detail_cfg.get("earnings_warning_days", 7)

    ticker = score.get("ticker", "")
    scoring_date = score.get("date", date.today().isoformat())

    lines = []
    if earnings_row is not None:
        earnings_dt = date.fromisoformat(earnings_row["earnings_date"])
        scoring_dt = date.fromisoformat(scoring_date)
        days_away = (earnings_dt - scoring_dt).days
        if days_away <= earnings_warning_days:
            lines.append(f"⚠️ Earnings in {days_away} days ({earnings_row['earnings_date']})")

    lines.append(f"📊 {ticker} — Detail Analysis ({scoring_date})")
    return "\n".join(lines)


def _direction_symbol(score_val: float, threshold: float) -> str:
    """
    Return ▲, ▼, or ▬ based on score vs threshold.

    Parameters:
        score_val: The numeric score to evaluate.
        threshold: The direction threshold from config.

    Returns:
        '▲' if score_val > threshold, '▼' if score_val < -threshold, '▬' otherwise.
    """
    if score_val > threshold:
        return "▲"
    if score_val < -threshold:
        return "▼"
    return "▬"


def build_timeframe_table(
    daily_row: dict,
    weekly_row: dict | None,
    monthly_row: dict | None,
    config: dict,
) -> str:
    """
    Build the timeframe summary table for msg #2, wrapped in a triple-backtick code block.

    Columns: Timeframe, Score, Trend, Momentum, Direction.
    Rows: Daily, Weekly, Monthly (N/A for missing weekly/monthly rows).
    Does not read sentiment_score from weekly/monthly rows (they don't have it).

    Parameters:
        daily_row: Score dict from scores_daily.
        weekly_row: Latest scores_weekly row dict, or None.
        monthly_row: Latest scores_monthly row dict, or None.
        config: Notifier config dict containing config["detail_command"].

    Returns:
        Triple-backtick-wrapped table string.
    """
    detail_cfg = config.get("detail_command", {})
    threshold: float = detail_cfg.get("timeframe_direction_threshold", 15.0)

    header = f"{'':8} {'Score':>7}  {'Trend':>7}  {'Mom':>7}  Dir"
    separator = "-" * len(header)

    rows_data = [
        (
            "Daily",
            daily_row.get("final_score") or 0.0,
            daily_row.get("trend_score") or 0.0,
            daily_row.get("momentum_score") or 0.0,
        ),
    ]

    if weekly_row is not None:
        rows_data.append((
            "Weekly",
            weekly_row.get("composite_score") or 0.0,
            weekly_row.get("trend_score") or 0.0,
            weekly_row.get("momentum_score") or 0.0,
        ))
    else:
        rows_data.append(("Weekly", None, None, None))

    if monthly_row is not None:
        rows_data.append((
            "Monthly",
            monthly_row.get("composite_score") or 0.0,
            monthly_row.get("trend_score") or 0.0,
            monthly_row.get("momentum_score") or 0.0,
        ))
    else:
        rows_data.append(("Monthly", None, None, None))

    lines = [header, separator]
    for label, comp, trend, mom in rows_data:
        if comp is None:
            lines.append(f"{label:<8}  N/A")
        else:
            direction = _direction_symbol(comp, threshold)
            lines.append(
                f"{label:<8} {comp:>+7.1f}  {trend:>+7.1f}  {mom:>+7.1f}  {direction}"
            )

    table = "\n".join(lines)
    return f"```\n{table}\n```"


def build_deterministic_confidence(
    score: dict,
    weekly_row: dict | None,
    monthly_row: dict | None,
    config: dict,
) -> str:
    """
    Build the deterministic confidence section for msg #2.

    Shows agreeing/disagreeing categories across daily/weekly/monthly timeframes
    and flags raw-vs-calibrated sign contradictions when applicable.
    When final_score == 0, returns a single neutral line and skips directional logic.

    Parameters:
        score: Score dict from scores_daily.
        weekly_row: Latest scores_weekly row dict, or None.
        monthly_row: Latest scores_monthly row dict, or None.
        config: Notifier config dict containing config["detail_command"].

    Returns:
        Formatted confidence section string.
    """
    detail_cfg = config.get("detail_command", {})
    min_score: float = detail_cfg.get("category_agreement_min_score", 10.0)
    min_abs_calib: float = detail_cfg.get("calibration_divergence_min_abs", 0.3)

    signal = score.get("signal", "NEUTRAL")
    final_score = score.get("final_score") or 0.0
    daily_score = score.get("daily_score") or 0.0
    weekly_score = score.get("weekly_score") or 0.0
    monthly_score = score.get("monthly_score") or 0.0
    confidence = score.get("confidence") or 0.0
    calibrated_score = score.get("calibrated_score")

    lines = []
    lines.append(f"  Daily:    {daily_score:+.1f} | Weekly:  {weekly_score:+.1f} | Monthly: {monthly_score:+.1f}")
    lines.append(f"  Final score: {final_score:+.1f}  |  Confidence: {confidence:.0f}%")
    if calibrated_score is not None:
        lines.append(f"  Calibrated score: {calibrated_score:+.2f}")

    if final_score == 0.0:
        lines.append("  Signal NEUTRAL: no directional sign analysis.")
        return "\n".join(lines)

    direction = 1 if final_score > 0 else -1

    # Daily categories (9 — includes sentiment)
    daily_categories = [
        ("trend (D)", score.get("trend_score") or 0.0),
        ("momentum (D)", score.get("momentum_score") or 0.0),
        ("volume (D)", score.get("volume_score") or 0.0),
        ("volatility (D)", score.get("volatility_score") or 0.0),
        ("candlestick (D)", score.get("candlestick_score") or 0.0),
        ("structural (D)", score.get("structural_score") or 0.0),
        ("sentiment (D)", score.get("sentiment_score") or 0.0),
        ("fundamental (D)", score.get("fundamental_score") or 0.0),
        ("macro (D)", score.get("macro_score") or 0.0),
    ]

    # Weekly categories (8 — no sentiment)
    weekly_cats: list[tuple[str, float]] = []
    if weekly_row is not None:
        weekly_cats = [
            ("trend (W)", weekly_row.get("trend_score") or 0.0),
            ("momentum (W)", weekly_row.get("momentum_score") or 0.0),
            ("volume (W)", weekly_row.get("volume_score") or 0.0),
            ("volatility (W)", weekly_row.get("volatility_score") or 0.0),
            ("candlestick (W)", weekly_row.get("candlestick_score") or 0.0),
            ("structural (W)", weekly_row.get("structural_score") or 0.0),
            ("fundamental (W)", weekly_row.get("fundamental_score") or 0.0),
            ("macro (W)", weekly_row.get("macro_score") or 0.0),
        ]

    # Monthly categories (8 — no sentiment)
    monthly_cats: list[tuple[str, float]] = []
    if monthly_row is not None:
        monthly_cats = [
            ("trend (M)", monthly_row.get("trend_score") or 0.0),
            ("momentum (M)", monthly_row.get("momentum_score") or 0.0),
            ("volume (M)", monthly_row.get("volume_score") or 0.0),
            ("volatility (M)", monthly_row.get("volatility_score") or 0.0),
            ("candlestick (M)", monthly_row.get("candlestick_score") or 0.0),
            ("structural (M)", monthly_row.get("structural_score") or 0.0),
            ("fundamental (M)", monthly_row.get("fundamental_score") or 0.0),
            ("macro (M)", monthly_row.get("macro_score") or 0.0),
        ]

    all_cats = daily_categories + weekly_cats + monthly_cats

    agreeing = [label for label, val in all_cats if abs(val) >= min_score and (1 if val > 0 else -1) == direction]
    disagreeing = [label for label, val in all_cats if abs(val) >= min_score and (1 if val > 0 else -1) != direction]

    if agreeing:
        lines.append(f"  Agreeing:   {', '.join(agreeing)}")
    else:
        lines.append("  Agreeing:   none above threshold")

    if disagreeing:
        lines.append(f"  Disagreeing: {', '.join(disagreeing)}")
    else:
        lines.append("  Disagreeing: none above threshold")

    # Calibration sign-flip check
    if calibrated_score is not None and abs(calibrated_score) >= min_abs_calib:
        sign_flip = (
            (signal == "BULLISH" and calibrated_score < 0)
            or (signal == "BEARISH" and calibrated_score > 0)
        )
        if sign_flip:
            lines.append(
                f"  ⚠️ Calibrated score ({calibrated_score:+.2f}) contradicts {signal} signal "
                f"(final_score {final_score:+.1f}) — model expects opposite direction"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def build_indicators_section(indicators: dict) -> str:
    """
    Format the key indicator values.

    Parameters:
        indicators: Dict of indicator values (from indicators_daily row).

    Returns:
        Formatted string section starting with '═══ INDICATORS ═══'.
    """
    if not indicators:
        return ""

    indicator_map = [
        ("RSI 14", "rsi_14"),
        ("MACD line", "macd_line"),
        ("MACD signal", "macd_signal"),
        ("MACD hist", "macd_histogram"),
        ("EMA 9", "ema_9"),
        ("EMA 21", "ema_21"),
        ("EMA 50", "ema_50"),
        ("ADX", "adx"),
        ("Stoch K", "stoch_k"),
        ("Stoch D", "stoch_d"),
        ("CCI 20", "cci_20"),
        ("Williams R", "williams_r"),
        ("OBV", "obv"),
        ("CMF 20", "cmf_20"),
        ("BB upper", "bb_upper"),
        ("BB lower", "bb_lower"),
        ("ATR 14", "atr_14"),
    ]

    lines = ["═══ INDICATORS ═══"]
    for label, key in indicator_map:
        val = indicators.get(key)
        if val is not None:
            lines.append(f"  {label:<12} {val:.2f}")

    return "\n".join(lines) if len(lines) > 1 else ""


def build_patterns_section(db_conn: sqlite3.Connection, ticker: str, days: int = 10) -> str:
    """
    Format recent candlestick and structural patterns.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        days: Number of days to look back.

    Returns:
        Formatted string section, or empty string if no patterns.
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = db_conn.execute(
        "SELECT date, pattern_name, pattern_category, direction, strength "
        "FROM patterns_daily WHERE ticker = ? AND date >= ? ORDER BY date DESC",
        (ticker, cutoff),
    ).fetchall()

    if not rows:
        return ""

    lines = ["═══ PATTERNS ═══"]
    for row in rows:
        direction_emoji = "🟢" if row["direction"] == "bullish" else "🔴" if row["direction"] == "bearish" else "🟡"
        lines.append(
            f"  {row['date']}  {direction_emoji} {row['pattern_name']} "
            f"({row['pattern_category']}, str={row['strength']})"
        )

    return "\n".join(lines)


def build_divergences_section(
    db_conn: sqlite3.Connection, ticker: str, days: int = 30
) -> str:
    """
    Format recent divergences.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        days: Number of days to look back.

    Returns:
        Formatted string section, or empty string if no divergences.
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = db_conn.execute(
        "SELECT date, indicator, divergence_type, strength FROM divergences_daily "
        "WHERE ticker = ? AND date >= ? ORDER BY date DESC",
        (ticker, cutoff),
    ).fetchall()

    if not rows:
        return ""

    lines = ["═══ DIVERGENCES ═══"]
    for row in rows:
        emoji = "🟢" if row["divergence_type"] == "bullish" else "🔴"
        lines.append(
            f"  {row['date']}  {emoji} {row['indicator']} {row['divergence_type']} (str={row['strength']})"
        )

    return "\n".join(lines)


def build_crossovers_section(
    db_conn: sqlite3.Connection, ticker: str, days: int = 10
) -> str:
    """
    Format recent crossover events.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        days: Number of days to look back.

    Returns:
        Formatted string section, or empty string if no crossovers.
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = db_conn.execute(
        "SELECT date, crossover_type, direction FROM crossovers_daily "
        "WHERE ticker = ? AND date >= ? ORDER BY date DESC",
        (ticker, cutoff),
    ).fetchall()

    if not rows:
        return ""

    lines = ["═══ CROSSOVERS ═══"]
    for row in rows:
        emoji = "🟢" if row["direction"] == "bullish" else "🔴"
        lines.append(f"  {row['date']}  {emoji} {row['crossover_type']} ({row['direction']})")

    return "\n".join(lines)


def build_fibonacci_section(fib_result: dict | None) -> str:
    """
    Format Fibonacci retracement levels with a 'PRICE HERE' marker.

    Parameters:
        fib_result: Output from compute_fibonacci_for_ticker, or None.

    Returns:
        Formatted string section, or empty string if no data.
    """
    if not fib_result:
        return ""

    levels = fib_result.get("levels", [])
    current_price = fib_result.get("current_price", 0.0)
    nearest = fib_result.get("nearest_level")

    lines = ["═══ FIBONACCI ═══"]
    swing_low = fib_result.get("swing_low", {})
    swing_high = fib_result.get("swing_high", {})
    if swing_low and swing_high:
        lines.append(
            f"  Swing: ${swing_low.get('price', 0):.2f} ({swing_low.get('date', '')}) "
            f"→ ${swing_high.get('price', 0):.2f} ({swing_high.get('date', '')})"
        )

    for lv in levels:
        pct = f"{lv['level_pct'] * 100:.1f}%"
        marker = ""
        if nearest and abs(lv["price"] - nearest["level_price"]) < 0.001:
            marker = "  ← PRICE HERE"
        lines.append(f"  {pct:<8} ${lv['price']:.2f}{marker}")

    lines.append(f"  Current: ${current_price:.2f}")
    return "\n".join(lines)


def build_sentiment_section(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> str:
    """
    Format news sentiment, short interest, and 8-K filing flag.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Reference date in YYYY-MM-DD format.

    Returns:
        Formatted string section, or empty string if no data.
    """
    cutoff = (date.fromisoformat(scoring_date) - timedelta(days=7)).isoformat()
    news_row = db_conn.execute(
        "SELECT avg_sentiment_score, article_count, positive_count, negative_count, "
        "neutral_count, top_headline, filing_flag "
        "FROM news_daily_summary WHERE ticker = ? AND date >= ? ORDER BY date DESC LIMIT 1",
        (ticker, cutoff),
    ).fetchone()

    si_row = db_conn.execute(
        "SELECT short_interest, avg_daily_volume, days_to_cover "
        "FROM short_interest WHERE ticker = ? ORDER BY settlement_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if not news_row and not si_row:
        return ""

    lines = ["═══ SENTIMENT ═══"]
    if news_row:
        score_val = news_row["avg_sentiment_score"]
        if score_val is not None:
            lines.append(f"  News score:  {score_val:.2f}")
        lines.append(
            f"  Articles:    {news_row['article_count'] or 0} "
            f"(+{news_row['positive_count'] or 0} -{news_row['negative_count'] or 0} "
            f"={news_row['neutral_count'] or 0})"
        )
        if news_row["top_headline"]:
            lines.append(f"  Top:         {news_row['top_headline'][:80]}")
        if news_row["filing_flag"]:
            lines.append("  ⚠️ Recent 8-K filing detected")

    if si_row and si_row["short_interest"]:
        lines.append(f"  Short int:   {si_row['short_interest']:,} shares")
        if si_row["days_to_cover"] is not None:
            lines.append(f"  Days cover:  {si_row['days_to_cover']:.1f}")

    return "\n".join(lines) if len(lines) > 1 else ""


def build_fundamentals_section(db_conn: sqlite3.Connection, ticker: str) -> str:
    """
    Format the latest fundamental metrics.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Formatted string section, or empty string if no data.
    """
    row = db_conn.execute(
        "SELECT pe_ratio, eps, eps_growth_yoy, revenue_growth_yoy, "
        "debt_to_equity, market_cap, dividend_yield "
        "FROM fundamentals WHERE ticker = ? ORDER BY report_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if not row:
        return ""

    lines = ["═══ FUNDAMENTALS ═══"]
    if row["pe_ratio"] is not None:
        lines.append(f"  P/E:         {row['pe_ratio']:.1f}")
    if row["eps"] is not None:
        growth = f" (YoY: {row['eps_growth_yoy']:.1%})" if row["eps_growth_yoy"] is not None else ""
        lines.append(f"  EPS:         ${row['eps']:.2f}{growth}")
    if row["revenue_growth_yoy"] is not None:
        lines.append(f"  Rev growth:  {row['revenue_growth_yoy']:.1%} YoY")
    if row["debt_to_equity"] is not None:
        lines.append(f"  D/E:         {row['debt_to_equity']:.2f}")
    if row["market_cap"] is not None:
        cap_t = row["market_cap"] / 1e12
        lines.append(f"  Mkt cap:     ${cap_t:.2f}T")
    if row["dividend_yield"] is not None and row["dividend_yield"] > 0:
        lines.append(f"  Div yield:   {row['dividend_yield']:.2%}")

    return "\n".join(lines) if len(lines) > 1 else ""


def build_macro_section(db_conn: sqlite3.Connection, scoring_date: str) -> str:
    """
    Format macro context: SPY, VIX, and treasury yield snapshot.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Reference date in YYYY-MM-DD format.

    Returns:
        Formatted string section, or empty string if no data.
    """
    spy_row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = 'SPY' AND date <= ? ORDER BY date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()

    vix_row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = '^VIX' AND date <= ? ORDER BY date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()

    yield_row = db_conn.execute(
        "SELECT yield_10_year FROM treasury_yields WHERE date <= ? ORDER BY date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()

    if not spy_row and not vix_row and not yield_row:
        return ""

    lines = ["═══ MACRO ═══"]
    if spy_row:
        lines.append(f"  SPY:         ${spy_row['close']:.2f}")
    if vix_row:
        vix = float(vix_row["close"])
        level = "elevated" if vix >= 20 else "low"
        lines.append(f"  VIX:         {vix:.1f} ({level})")
    if yield_row and yield_row["yield_10_year"] is not None:
        lines.append(f"  10Y yield:   {yield_row['yield_10_year']:.2f}%")

    return "\n".join(lines) if len(lines) > 1 else ""


def build_key_levels(
    db_conn: sqlite3.Connection,
    ticker: str,
    current_price: float,
    indicators: dict,
    fib_result: dict | None,
    sr_levels: list[dict],
    config: dict,
) -> str:
    """
    Format the key price levels section.

    Shows resistance and support levels (S/R and Fibonacci), EMA values with
    distance percentage from current price.

    Parameters:
        db_conn: Open SQLite connection (not used directly; kept for extensibility).
        ticker: Ticker symbol.
        current_price: Current close price.
        indicators: Dict of latest indicator values.
        fib_result: Output from compute_fibonacci_for_ticker, or None.
        sr_levels: List of S/R level dicts.
        config: Notifier config dict.

    Returns:
        Formatted string section starting with '═══ KEY LEVELS ═══'.
    """
    lines = ["═══ KEY LEVELS ═══"]

    resistance = sorted(
        [lv for lv in sr_levels if lv["level_price"] > current_price],
        key=lambda lv: lv["level_price"],
    )
    support = sorted(
        [lv for lv in sr_levels if lv["level_price"] <= current_price],
        key=lambda lv: lv["level_price"],
        reverse=True,
    )

    if resistance:
        first = True
        for lv in resistance[:3]:
            prefix = "  Resistance:" if first else "             "
            strength = f" ({lv['strength']}, {lv['touch_count']} touches)" if lv.get("strength") else ""
            lines.append(f"{prefix} ${lv['level_price']:.2f}{strength}")
            first = False

    if support:
        first = True
        for lv in support[:3]:
            prefix = "  Support:   " if first else "             "
            strength = f" ({lv['strength']}, {lv['touch_count']} touches)" if lv.get("strength") else ""
            lines.append(f"{prefix} ${lv['level_price']:.2f}{strength}")
            first = False

    if fib_result:
        nearest = fib_result.get("nearest_level")
        for lv in fib_result.get("levels", []):
            marker = "  ← PRICE HERE" if nearest and abs(lv["price"] - nearest["level_price"]) < 0.001 else ""
            lines.append(f"  Fib {lv['level_pct']*100:.1f}%:   ${lv['price']:.2f}{marker}")

    lines.append("")
    for label, key in [("EMA 9", "ema_9"), ("EMA 21", "ema_21"), ("EMA 50", "ema_50")]:
        val = indicators.get(key)
        if val is not None and current_price > 0:
            diff_pct = (val - current_price) / current_price * 100
            direction = "above" if diff_pct > 0 else "below"
            lines.append(f"  {label}:      ${val:.2f} ({abs(diff_pct):.1f}% {direction})")

    return "\n".join(lines)


def build_signal_change_triggers(
    indicators: dict, score: dict, config: dict
) -> str:
    """
    Generate concrete conditions that would flip the current signal.

    If currently BULLISH: shows what would flip to NEUTRAL/BEARISH.
    If currently BEARISH: shows what would flip to NEUTRAL/BULLISH.
    If currently NEUTRAL: shows both BULLISH and BEARISH trigger conditions.

    Parameters:
        indicators: Dict of current indicator values.
        score: Score dict from scores_daily.
        config: Notifier config dict.

    Returns:
        Formatted string section starting with '═══ SIGNAL CHANGE TRIGGERS ═══'.
    """
    signal = score.get("signal", "NEUTRAL")
    rsi = indicators.get("rsi_14", 50.0) or 50.0
    macd_hist = indicators.get("macd_histogram", 0.0) or 0.0
    ema_9 = indicators.get("ema_9")
    macd_line = indicators.get("macd_line", 0.0) or 0.0
    adx = indicators.get("adx", 0.0) or 0.0

    lines = ["═══ SIGNAL CHANGE TRIGGERS ═══"]

    show_bullish = signal in ("NEUTRAL", "BEARISH")
    show_bearish = signal in ("NEUTRAL", "BULLISH")

    if show_bullish:
        lines.append("  → BULLISH if:")
        if macd_hist < 0:
            lines.append(f"    • MACD histogram turns positive (currently {macd_hist:+.2f})")
        if rsi < 50:
            lines.append(f"    • RSI crosses above 50 (currently {rsi:.1f})")
        if ema_9 is not None:
            lines.append(f"    • Price closes above EMA 9 (${ema_9:.2f})")
        if adx < 25:
            lines.append(f"    • ADX trends above 25 to confirm direction (currently {adx:.1f})")

    if show_bearish:
        lines.append("  → BEARISH if:")
        if macd_hist > 0:
            lines.append(f"    • MACD histogram turns negative (currently {macd_hist:+.2f})")
        if rsi > 50:
            lines.append(f"    • RSI crosses below 50 (currently {rsi:.1f})")
        if ema_9 is not None:
            lines.append(f"    • Price closes below EMA 9 (${ema_9:.2f})")
        if score.get("macro_score", 0.0) is not None and (score.get("macro_score") or 0.0) > -10:
            lines.append("    • VIX breaks above 30 or SPY breaks key support")

    if len(lines) == 1:
        lines.append("  No specific triggers identified based on current data.")

    return "\n".join(lines)


def build_signal_history(
    db_conn: sqlite3.Connection,
    ticker: str,
    days: int = 30,
    reference_date: Optional[str] = None,
) -> str:
    """
    Format recent signal history for a ticker.

    Shows daily signal entries sorted chronologically, summary counts by signal
    type, and a trend description (improving/deteriorating/stable based on
    whether the score is generally rising or falling).

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        days: Number of days to look back.
        reference_date: YYYY-MM-DD string to compute cutoff from. Defaults to today.

    Returns:
        Formatted string section, or 'No signal history available.' if no data.
    """
    ref = date.fromisoformat(reference_date) if reference_date else date.today()
    cutoff = (ref - timedelta(days=days)).isoformat()
    rows = db_conn.execute(
        "SELECT date, signal, confidence, final_score FROM scores_daily "
        "WHERE ticker = ? AND date >= ? ORDER BY date ASC",
        (ticker, cutoff),
    ).fetchall()

    if not rows:
        return "No signal history available."

    signal_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}

    lines = [f"═══ SIGNAL HISTORY (last {days} days) ═══"]
    for row in rows:
        day_str = row["date"][5:]  # MM-DD
        emoji = signal_emoji.get(row["signal"], "🟡")
        conf = row["confidence"] or 0.0
        score_val = row["final_score"] or 0.0
        lines.append(
            f"  {day_str}: {emoji} {row['signal']:<8} {conf:.0f}% | {score_val:+.1f}"
        )

    bullish_count = sum(1 for r in rows if r["signal"] == "BULLISH")
    bearish_count = sum(1 for r in rows if r["signal"] == "BEARISH")
    neutral_count = sum(1 for r in rows if r["signal"] == "NEUTRAL")
    lines.append(
        f"  Last {days} days: {bullish_count} 🟢 | {bearish_count} 🔴 | {neutral_count} 🟡"
    )

    # Trend: compare first-half average score vs second-half
    scores_list = [r["final_score"] or 0.0 for r in rows]
    half = max(1, len(scores_list) // 2)
    avg_first = sum(scores_list[:half]) / half
    avg_second = sum(scores_list[half:]) / max(1, len(scores_list) - half)

    if avg_second - avg_first > 5:
        trend = "improving"
    elif avg_first - avg_second > 5:
        trend = "deteriorating"
    else:
        trend = "stable"

    lines.append(f"  Trend: {trend}")
    return "\n".join(lines)


def build_earnings_warning(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> str:
    """
    Format the upcoming earnings warning section.

    Includes the next earnings date, days until it, EPS estimate, and a
    ⚠️ reliability warning if earnings are within 7 days.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Reference date in YYYY-MM-DD format.

    Returns:
        Formatted string section, or empty string if no upcoming earnings.
    """
    row = db_conn.execute(
        "SELECT earnings_date, estimated_eps, actual_eps FROM earnings_calendar "
        "WHERE ticker = ? AND earnings_date >= ? ORDER BY earnings_date ASC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()

    if not row:
        return ""

    earnings_dt = date.fromisoformat(row["earnings_date"])
    scoring_dt = date.fromisoformat(scoring_date)
    days_away = (earnings_dt - scoring_dt).days

    month_name = earnings_dt.strftime("%B %d, %Y")
    lines = [
        "═══ ⚠️ EARNINGS ALERT ═══",
        f"  Next earnings: {month_name} ({days_away} days away)",
    ]

    if row["estimated_eps"] is not None:
        lines.append(f"  Expected EPS: ${row['estimated_eps']:.2f}")

    if row["actual_eps"] is not None and row["estimated_eps"] is not None:
        surprise_pct = (row["actual_eps"] - row["estimated_eps"]) / abs(row["estimated_eps"]) * 100
        direction = "beat" if surprise_pct > 0 else "missed"
        lines.append(f"  Last actual: ${row['actual_eps']:.2f} ({direction} by {abs(surprise_pct):.1f}%)")

    if days_away <= 7:
        lines.append("  ⚠️ Signal reliability decreases within 7 days of earnings")

    return "\n".join(lines)


def build_sector_peers(
    db_conn: sqlite3.Connection,
    ticker: str,
    sector: str,
    active_tickers: list[dict],
    scoring_date: str,
    config: dict,
) -> str:
    """
    Format sector peer comparison showing all same-sector tickers sorted by score.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: The queried ticker symbol.
        sector: Sector name for the queried ticker.
        active_tickers: Full list of active tickers (with sector info).
        scoring_date: Reference date in YYYY-MM-DD format.
        config: Notifier config dict containing config["detail_command"].

    Returns:
        Formatted string section, or empty string if no peer data.
    """
    peer_count = config.get("detail_command", {}).get("peer_count", 5)

    sector_symbols = [
        t["symbol"] for t in active_tickers
        if t.get("sector") == sector and t.get("symbol") != ticker
    ]
    all_symbols = sector_symbols + [ticker]

    placeholders = ",".join("?" * len(all_symbols))
    rows = db_conn.execute(
        f"SELECT ticker, signal, confidence, final_score FROM scores_daily "
        f"WHERE ticker IN ({placeholders}) AND date = ? ORDER BY final_score DESC",
        (*all_symbols, scoring_date),
    ).fetchall()

    if not rows:
        return ""

    lines = [f"═══ SECTOR PEERS ({sector}) ═══"]

    ticker_rank = None
    total = len(rows)
    for rank, row in enumerate(rows, start=1):
        if row["ticker"] == ticker:
            ticker_rank = rank
        marker = "▸" if row["ticker"] == ticker else " "
        you = "  ← you are here" if row["ticker"] == ticker else ""
        emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}.get(row["signal"], "🟡")
        lines.append(
            f"  {marker}{row['ticker']:<6} {emoji} {row['signal']:<8} {row['final_score']:+.1f}{you}"
        )

    if total > peer_count + 1:
        lines.append(f"  ... ({total} total in sector)")

    if ticker_rank is not None:
        lines.append(f"  {ticker} rank: {ticker_rank}/{total} in sector")

    return "\n".join(lines)


def build_confidence_modifiers_section(score: dict) -> str:
    """
    Show the base confidence and its final result for context.

    Parameters:
        score: Score dict from scores_daily.

    Returns:
        Formatted string section.
    """
    confidence = score.get("confidence", 0.0) or 0.0
    final_score = score.get("final_score", 0.0) or 0.0

    lines = [
        "═══ CONFIDENCE ═══",
        f"  Final confidence: {confidence:.0f}%",
        f"  Final score:      {final_score:+.1f}",
    ]

    data_completeness = score.get("data_completeness")
    if data_completeness and data_completeness != "complete":
        lines.append(f"  Data completeness: {data_completeness}")

    return "\n".join(lines)


def build_analyst_prompt(
    ticker: str,
    score: dict,
    weekly_row: dict | None,
    monthly_row: dict | None,
    market_context: str,
    key_levels: str,
    signal_triggers: str,
    signal_history: str,
    earnings_info: str,
    sector_peers: str,
    calibration_divergence_note: str,
) -> tuple[str, str]:
    """
    Build the (system_prompt, user_prompt) pair for /detail msg #2 AI analysis.

    The assistant turn is prefilled with '<verdict>' separately to guarantee
    well-formed XML output. The system prompt instructs Claude to emit exactly
    three XML-tagged sections: <verdict>, <timeframe_note>, <reasoning>.

    Parameters:
        ticker: Ticker symbol.
        score: Score dict from scores_daily.
        weekly_row: Latest scores_weekly row dict, or None.
        monthly_row: Latest scores_monthly row dict, or None.
        market_context: Formatted macro/market context string.
        key_levels: Formatted key levels section.
        signal_triggers: Formatted signal change triggers section.
        signal_history: Formatted signal history section.
        earnings_info: Formatted earnings warning section (may be empty).
        sector_peers: Formatted sector peers section.
        calibration_divergence_note: Non-empty string when raw/calibrated scores diverge.

    Returns:
        Tuple of (system_prompt, user_prompt).
    """
    calib_instruction = ""
    if calibration_divergence_note:
        calib_instruction = (
            "\n\nImportant: The calibrated model score contradicts the raw signal direction. "
            "Explain this divergence inside <reasoning> — discuss whether the raw score or "
            "calibrated score is more trustworthy given current conditions."
        )

    system_prompt = (
        "You are a senior technical analyst writing a structured internal research note. "
        "Emit exactly three XML-tagged sections in order:\n\n"
        "<verdict> — max 3 lines. First line: BUY / SELL / HOLD-WAIT. "
        "Next 2 lines: concrete justification with prices and levels.\n\n"
        "<timeframe_note> — 1 line. Do daily/weekly/monthly timeframes agree? "
        "Is momentum building or fading?\n\n"
        "<reasoning> — 1 paragraph. Explain the call with specific indicator values, "
        "price levels, and the key risk to the thesis. Write with conviction."
        + calib_instruction
    )

    signal = score.get("signal", "NEUTRAL")
    final_score = score.get("final_score") or 0.0
    confidence = score.get("confidence") or 0.0
    regime = score.get("regime", "unknown")
    daily_score = score.get("daily_score") or 0.0
    weekly_score = score.get("weekly_score") or 0.0
    monthly_score = score.get("monthly_score") or 0.0

    weekly_summary = "N/A"
    if weekly_row is not None:
        wc = weekly_row.get("composite_score") or 0.0
        wt = weekly_row.get("trend_score") or 0.0
        wm = weekly_row.get("momentum_score") or 0.0
        weekly_summary = f"composite={wc:+.1f}, trend={wt:+.1f}, momentum={wm:+.1f}"

    monthly_summary = "N/A"
    if monthly_row is not None:
        mc = monthly_row.get("composite_score") or 0.0
        mt = monthly_row.get("trend_score") or 0.0
        mm = monthly_row.get("momentum_score") or 0.0
        monthly_summary = f"composite={mc:+.1f}, trend={mt:+.1f}, momentum={mm:+.1f}"

    user_sections = [
        "--- TICKER CONTEXT ---",
        f"Ticker: {ticker}",
        f"Signal: {signal} | Final score: {final_score:+.1f} | Confidence: {confidence:.0f}% | Regime: {regime}",
        f"Daily score: {daily_score:+.1f} | Weekly score: {weekly_score:+.1f} | Monthly score: {monthly_score:+.1f}",
        f"Weekly breakdown: {weekly_summary}",
        f"Monthly breakdown: {monthly_summary}",
    ]

    if calibration_divergence_note:
        user_sections += ["--- CALIBRATION NOTE ---", calibration_divergence_note]

    user_sections += [
        "--- MARKET CONTEXT ---",
        market_context,
        "--- KEY LEVELS ---",
        key_levels,
        "--- SIGNAL CHANGE TRIGGERS ---",
        signal_triggers,
        "--- SIGNAL HISTORY ---",
        signal_history,
    ]

    if earnings_info:
        user_sections += ["--- EARNINGS ---", earnings_info]

    if sector_peers:
        user_sections += ["--- SECTOR PEERS ---", sector_peers]

    user_prompt = "\n".join(user_sections)
    return system_prompt, user_prompt


def build_full_breakdown(
    db_conn: sqlite3.Connection,
    ticker: str,
    score: dict,
    config: dict,
    indicators: dict | None = None,
    current_price: float | None = None,
    sr_levels: list[dict] | None = None,
    active_tickers: list[dict] | None = None,
) -> str:
    """
    Assemble all breakdown sections into the complete raw data message (msg #3).

    Sections are included in this order:
      Indicators, Patterns, Divergences, Crossovers, Fibonacci, Sentinel header,
      Sentiment, Fundamentals, Macro, Key levels, Signal change triggers,
      Signal history, Earnings warning, Sector peers, Confidence modifiers.

    Note: Scoring chain and category scores were removed in Plan B — they are
    now presented exclusively via the deterministic confidence and AI sections
    in msg #2. The underlying build_scoring_chain and build_category_scores
    functions have been deleted.

    Empty sections are omitted silently.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        score: Score dict from scores_daily.
        config: Notifier config dict.
        indicators: Pre-fetched indicator dict (avoids duplicate query if provided).
        current_price: Pre-fetched current close price (avoids duplicate query if provided).
        sr_levels: Pre-fetched S/R levels list (avoids duplicate query if provided).
        active_tickers: List of active ticker dicts for sector peer comparison.

    Returns:
        Complete formatted string (may be > 4096 chars).
    """
    scoring_date = score.get("date", date.today().isoformat())

    if indicators is None:
        indicators_row = db_conn.execute(
            "SELECT * FROM indicators_daily WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()
        indicators = dict(indicators_row) if indicators_row else {}

    if current_price is None:
        current_price_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()
        current_price = float(current_price_row["close"]) if current_price_row else 0.0

    if sr_levels is None:
        sr_rows = db_conn.execute(
            "SELECT level_price, level_type, touch_count, strength FROM support_resistance "
            "WHERE ticker = ? AND broken = 0",
            (ticker,),
        ).fetchall()
        sr_levels = [dict(r) for r in sr_rows]

    ticker_sector = ""
    if active_tickers:
        ticker_sector = next(
            (t.get("sector", "") for t in active_tickers if t["symbol"] == ticker), ""
        )

    sections = [
        build_indicators_section(indicators),
        build_patterns_section(db_conn, ticker),
        build_divergences_section(db_conn, ticker),
        build_crossovers_section(db_conn, ticker),
        build_sentinel_section_header(ticker, scoring_date),
        build_sentiment_section(db_conn, ticker, scoring_date),
        build_fundamentals_section(db_conn, ticker),
        build_macro_section(db_conn, scoring_date),
        build_key_levels(db_conn, ticker, current_price, indicators, None, sr_levels, config),
        build_signal_change_triggers(indicators, score, config),
        build_signal_history(
            db_conn, ticker, config.get("detail_command", {}).get("signal_history_days", 30),
            reference_date=scoring_date,
        ),
        build_earnings_warning(db_conn, ticker, scoring_date),
    ]

    if active_tickers and ticker_sector:
        sections.append(
            build_sector_peers(
                db_conn, ticker, ticker_sector, active_tickers, scoring_date, config
            )
        )

    sections.append(build_confidence_modifiers_section(score))

    filled = [s for s in sections if s and s.strip()]
    return "\n\n".join(filled)


def build_sentinel_section_header(ticker: str, scoring_date: str) -> str:
    """
    Return a small section header showing the ticker and scoring date.

    Parameters:
        ticker: Ticker symbol.
        scoring_date: Date in YYYY-MM-DD format.

    Returns:
        Single-line header string.
    """
    return f"📊 {ticker} — Detail Report ({scoring_date})"


# ---------------------------------------------------------------------------
# Claude integration
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_claude_for_analysis(
    user_prompt: str,
    system_prompt: str,
    prefill: str,
    config: dict,
) -> str:
    """
    Call the Claude API for structured technical analysis.

    Uses the system prompt for persona and format instructions, and optionally
    prefills the assistant turn to guarantee well-formed XML output.
    Prefilling requires Claude 3+ models.

    Parameters:
        user_prompt: User message containing ticker context and data.
        system_prompt: System prompt with persona and XML format instructions.
        prefill: String to prefill the assistant turn (e.g. '<verdict>').
            When non-empty, this content is prepended by parse_ai_response
            before parsing — it is NOT included in the API response text.
        config: Notifier config dict containing config["ai_reasoner"].

    Returns:
        Claude's raw response text (NOT including the prefill content).
    """
    import anthropic as _anthropic  # lazy import — not available in all environments

    reasoner_cfg = config.get("ai_reasoner", {})
    model = reasoner_cfg.get("model", "claude-sonnet-4-20250514")
    max_tokens = reasoner_cfg.get("max_tokens", 4096)
    temperature = reasoner_cfg.get("temperature", 0.3)

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    client = _anthropic.Anthropic(api_key=api_key)

    messages: list[dict] = [{"role": "user", "content": user_prompt}]
    if prefill:
        messages.append({"role": "assistant", "content": prefill})

    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system_prompt,
        messages=messages,
    )
    return message.content[0].text


_STRAY_TAG_PATTERN = re.compile(r"</?(?:verdict|timeframe_note|reasoning)\s*/?>", re.IGNORECASE)


def _strip_stray_tags(text: str) -> str:
    """Remove any stray <verdict>/<timeframe_note>/<reasoning> tags from extracted content."""
    return _STRAY_TAG_PATTERN.sub("", text).strip()


def parse_ai_response(text: str, prefill: str) -> dict:
    """
    Parse Claude's XML-tagged response into verdict/timeframe_note/reasoning.

    The prefill is prepended to text before parsing because prefilled content
    is not included in the API response.

    Each tag is extracted independently — missing tags yield empty strings for
    that section instead of dumping the raw response into verdict. As a safety
    net, any stray `<verdict>`, `<timeframe_note>`, or `<reasoning>` tags in
    extracted content (e.g. nested or duplicated) are stripped so they never
    appear in the rendered Telegram message.

    Only when ALL three tags are missing does the function fall back to the
    raw text in the verdict slot — logging WARNING. This protects users from
    seeing tag artifacts when Claude deviates from the format on a single tag.

    Parameters:
        text: Raw text from _call_claude_for_analysis.
        prefill: The prefill string (typically "<verdict>").

    Returns:
        {"verdict": str, "timeframe_note": str, "reasoning": str}
    """
    full_text = (prefill or "") + text

    verdict_match = re.search(r"<verdict>(.*?)</verdict>", full_text, re.DOTALL)
    timeframe_match = re.search(r"<timeframe_note>(.*?)</timeframe_note>", full_text, re.DOTALL)
    reasoning_match = re.search(r"<reasoning>(.*?)</reasoning>", full_text, re.DOTALL)

    missing_tags = [
        name for name, match in [
            ("verdict", verdict_match),
            ("timeframe_note", timeframe_match),
            ("reasoning", reasoning_match),
        ]
        if match is None
    ]
    if missing_tags:
        logger.warning(
            "phase=detail_command parse_ai_response missing tags %s in response",
            missing_tags,
        )

    # Last-resort fallback: NO tags found at all → put raw text into verdict slot
    # (with stray tags stripped so users never see <...> artifacts).
    if not verdict_match and not timeframe_match and not reasoning_match:
        return {"verdict": _strip_stray_tags(text), "timeframe_note": "", "reasoning": ""}

    return {
        "verdict": _strip_stray_tags(verdict_match.group(1)) if verdict_match else "",
        "timeframe_note": _strip_stray_tags(timeframe_match.group(1)) if timeframe_match else "",
        "reasoning": _strip_stray_tags(reasoning_match.group(1)) if reasoning_match else "",
    }


# ---------------------------------------------------------------------------
# Photo sending
# ---------------------------------------------------------------------------

def send_photo_to_chat(
    bot_token: str, chat_id: str, photo_path: str, caption: Optional[str] = None
) -> bool:
    """
    Send a photo file to a Telegram chat using the Bot API.

    Uses multipart/form-data upload via httpx.

    Parameters:
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.
        photo_path: Absolute path to the PNG file to send.
        caption: Optional text caption for the photo.

    Returns:
        True if the photo was sent successfully, False otherwise.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    try:
        with open(photo_path, "rb") as photo_file:
            data: dict = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption
            response = httpx.post(
                url,
                data=data,
                files={"photo": ("chart.png", photo_file, "image/png")},
                timeout=30.0,
            )
        response.raise_for_status()
        return True
    except (httpx.HTTPStatusError, httpx.RequestError, OSError) as exc:
        logger.error("phase=detail_command send_photo_to_chat failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Message splitting
# ---------------------------------------------------------------------------

def _split_breakdown_at_sections(text: str, max_len: int = _MAX_TELEGRAM_LENGTH) -> list[str]:
    """
    Split a long breakdown text into chunks at section boundaries.

    Splits at '═══' section headers when a chunk would exceed max_len characters.

    Parameters:
        text: Complete breakdown text.
        max_len: Maximum characters per chunk.

    Returns:
        List of text chunks, each ≤ max_len characters.
    """
    if len(text) <= max_len:
        return [text]

    lines = text.split("\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1  # +1 for newline
        if current_len + line_len > max_len and current and line.startswith("═══"):
            chunks.append("\n".join(current))
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks


def _split_message_at_section_markers(text: str, max_len: int = _MAX_TELEGRAM_LENGTH) -> list[str]:
    """
    Split msg #2 text into chunks at section marker boundaries.

    Splits ONLY on lines that start with one of the five msg #2 section markers:
      📍 VERDICT, ⏱️ TIMEFRAME SUMMARY, 🧠 REASONING, 📊 CONFIDENCE, 🎯 LEVELS & TRIGGERS

    The verdict header '📊 {ticker} — Detail Analysis' does NOT match '📊 CONFIDENCE'
    so it is never a split point.

    Parameters:
        text: Complete msg #2 text.
        max_len: Maximum characters per chunk.

    Returns:
        List of text chunks, each ≤ max_len characters.
    """
    if len(text) <= max_len:
        return [text]

    lines = text.split("\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1  # +1 for newline
        is_marker = any(line.startswith(marker) for marker in _MSG2_SECTION_MARKERS)
        if current and is_marker and current_len + line_len > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks


# ---------------------------------------------------------------------------
# Main command handler
# ---------------------------------------------------------------------------

def handle_detail_command(
    db_conn: sqlite3.Connection,
    chat_id: str,
    message_text: str,
    bot_token: str,
    config: dict,
    active_tickers: list[dict],
    calc_config: dict,
) -> None:
    """
    Handle an incoming /detail command end-to-end.

    Flow:
      1. Parse command; send error message and return on failure.
      2. Load latest score from scores_daily; send error if missing.
      3. Send '⏳ Generating analysis...' placeholder.
      4. Generate and send chart image.
      5. Build context sections, call Claude, send AI analysis.
      6. Build and send raw data breakdown (split if > 4096 chars).
      7. Clean up chart file.
      8. Edit placeholder to '✅ Analysis complete'.

    Parameters:
        db_conn: Open SQLite connection.
        chat_id: Telegram chat ID to reply to.
        message_text: Raw command text (e.g. '/detail AAPL 90').
        bot_token: Telegram Bot API token.
        config: Notifier config dict.
        active_tickers: List of active ticker dicts.
        calc_config: Calculator config dict for Fibonacci computation.

    Returns:
        None
    """
    parse_result = parse_detail_command(message_text, active_tickers, config)
    if "error" in parse_result:
        send_telegram_message(bot_token, chat_id, parse_result["error"])
        return

    ticker = parse_result["ticker"]
    days = parse_result["days"]

    score_row = db_conn.execute(
        "SELECT * FROM scores_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if not score_row:
        send_telegram_message(
            bot_token,
            chat_id,
            f"❌ No scoring data for {ticker}. Run the scorer first.",
        )
        return

    score = dict(score_row)
    scoring_date = score["date"]

    placeholder_id = send_telegram_message(
        bot_token, chat_id, f"⏳ Generating analysis for {ticker}..."
    )

    chart_path: Optional[str] = None
    try:
        # Step 1: Chart
        chart_path = generate_chart(db_conn, ticker, days, config, calc_config)
        if chart_path:
            send_photo_to_chat(
                bot_token,
                chat_id,
                chart_path,
                caption=f"{ticker} — {days}-Day Technical Chart",
            )

        # Step 2: Fetch weekly/monthly scores
        weekly_row = fetch_weekly_score(db_conn, ticker)
        monthly_row = fetch_monthly_score(db_conn, ticker)

        # Step 3: Fetch upcoming earnings row for verdict header
        earnings_calendar_row = db_conn.execute(
            "SELECT earnings_date, estimated_eps FROM earnings_calendar "
            "WHERE ticker = ? AND earnings_date >= ? ORDER BY earnings_date ASC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()
        upcoming_earnings_row = dict(earnings_calendar_row) if earnings_calendar_row else None

        # Step 4: Pre-fetch shared data once for both AI prompt and breakdown
        indicators_row = db_conn.execute(
            "SELECT * FROM indicators_daily WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()
        indicators = dict(indicators_row) if indicators_row else {}

        current_price_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()
        current_price = float(current_price_row["close"]) if current_price_row else 0.0

        sr_rows = db_conn.execute(
            "SELECT level_price, level_type, touch_count, strength FROM support_resistance "
            "WHERE ticker = ? AND broken = 0",
            (ticker,),
        ).fetchall()
        sr_levels = [dict(r) for r in sr_rows]

        from src.calculator.fibonacci import compute_fibonacci_for_ticker
        fib_result = compute_fibonacci_for_ticker(db_conn, ticker, calc_config)

        ticker_sector = next(
            (t.get("sector", "") for t in active_tickers if t["symbol"] == ticker), ""
        )

        key_levels_text = build_key_levels(
            db_conn, ticker, current_price, indicators, fib_result, sr_levels, config
        )
        signal_triggers_text = build_signal_change_triggers(indicators, score, config)
        history_days = config.get("detail_command", {}).get("signal_history_days", 30)
        signal_history_text = build_signal_history(db_conn, ticker, days=history_days, reference_date=scoring_date)
        earnings_text = build_earnings_warning(db_conn, ticker, scoring_date)
        peers_text = build_sector_peers(
            db_conn, ticker, ticker_sector, active_tickers, scoring_date, config
        )
        macro_text = build_macro_section(db_conn, scoring_date)

        # Step 5: Compute calibration divergence note (shared by AI prompt and confidence section)
        detail_cfg = config.get("detail_command", {})
        min_abs_calib: float = detail_cfg.get("calibration_divergence_min_abs", 0.3)
        calibrated_score = score.get("calibrated_score")
        signal_val = score.get("signal", "NEUTRAL")
        final_score_val = score.get("final_score") or 0.0
        calibration_divergence_note = ""
        if (
            calibrated_score is not None
            and abs(calibrated_score) >= min_abs_calib
            and final_score_val != 0
        ):
            sign_flip = (
                (signal_val == "BULLISH" and calibrated_score < 0)
                or (signal_val == "BEARISH" and calibrated_score > 0)
            )
            if sign_flip:
                calibration_divergence_note = (
                    f"⚠️ Calibrated score ({calibrated_score:+.2f}) contradicts {signal_val} "
                    f"raw signal ({final_score_val:+.1f})"
                )

        # Step 6: Build deterministic msg #2 sections
        verdict_header = build_verdict_header(score, upcoming_earnings_row, config)
        timeframe_table = build_timeframe_table(score, weekly_row, monthly_row, config)
        confidence_section = build_deterministic_confidence(score, weekly_row, monthly_row, config)

        # Step 7: Build AI prompt and call Claude
        system_prompt, user_prompt = build_analyst_prompt(
            ticker=ticker,
            score=score,
            weekly_row=weekly_row,
            monthly_row=monthly_row,
            market_context=macro_text,
            key_levels=key_levels_text,
            signal_triggers=signal_triggers_text,
            signal_history=signal_history_text,
            earnings_info=earnings_text,
            sector_peers=peers_text,
            calibration_divergence_note=calibration_divergence_note,
        )

        try:
            ai_raw = _call_claude_for_analysis(user_prompt, system_prompt, prefill="<verdict>", config=config)
        except Exception as exc:
            logger.warning("ticker=%s phase=detail_command Claude call failed: %s", ticker, exc)
            ai_raw = "BUY / SELL / HOLD-WAIT — AI unavailable</verdict><timeframe_note>N/A</timeframe_note><reasoning></reasoning>"

        # Step 8: Parse Claude response
        parsed = parse_ai_response(ai_raw, prefill="<verdict>")

        # Step 9: Apply MarkdownV2 escaping to all non-code-block sections.
        # The timeframe_table is wrapped in a triple-backtick code block, so
        # escape_markdown_v2 passes its content through unchanged. All other
        # sections (verdict header, AI free-text, deterministic confidence,
        # existing key_levels and signal_triggers builders) contain MarkdownV2
        # special characters (parens, dots, dashes, plus signs, pipes) that
        # must be escaped or Telegram's parser returns 400 Bad Request.
        verdict_header_esc = escape_markdown_v2(verdict_header)
        ai_verdict_esc = escape_markdown_v2(parsed["verdict"])
        ai_timeframe_note_esc = escape_markdown_v2(parsed["timeframe_note"])
        ai_reasoning_esc = escape_markdown_v2(parsed["reasoning"])
        confidence_section_esc = escape_markdown_v2(confidence_section)
        key_levels_text_esc = escape_markdown_v2(key_levels_text)
        signal_triggers_text_esc = escape_markdown_v2(signal_triggers_text)

        # Step 10: Assemble msg #2
        msg2_parts = [
            verdict_header_esc,
            "📍 VERDICT",
            ai_verdict_esc,
            "⏱️ TIMEFRAME SUMMARY",
            timeframe_table,  # code block — escape passes content through unchanged
            ai_timeframe_note_esc,
        ]
        if parsed["reasoning"].strip():
            msg2_parts += ["🧠 REASONING", ai_reasoning_esc]
        msg2_parts += [
            "📊 CONFIDENCE",
            confidence_section_esc,
            "🎯 LEVELS & TRIGGERS",
            key_levels_text_esc,
            signal_triggers_text_esc,
        ]
        msg2 = "\n".join(msg2_parts)

        # Step 11: Split and send msg #2 with MarkdownV2
        msg2_chunks = _split_message_at_section_markers(msg2, _MAX_TELEGRAM_LENGTH)
        for chunk in msg2_chunks:
            result_id = send_telegram_message(bot_token, chat_id, chunk, parse_mode="MarkdownV2")
            if result_id is None:
                logger.error(
                    "ticker=%s phase=detail_command send_telegram_message returned None for msg #2 chunk "
                    "(MarkdownV2 escaping may have failed — check for unescaped special characters)",
                    ticker,
                )

        # Step 12: Raw breakdown — reuse pre-fetched data to avoid duplicate queries
        breakdown = build_full_breakdown(
            db_conn, ticker, score, config,
            indicators=indicators,
            current_price=current_price,
            sr_levels=sr_levels,
            active_tickers=active_tickers,
        )
        breakdown_chunks = _split_breakdown_at_sections(breakdown)
        for chunk in breakdown_chunks:
            send_telegram_message(bot_token, chat_id, chunk)

        if placeholder_id:
            edit_telegram_message(
                bot_token, chat_id, placeholder_id, f"✅ Analysis complete for {ticker}"
            )

    finally:
        if chart_path:
            cleanup_chart(chart_path)
