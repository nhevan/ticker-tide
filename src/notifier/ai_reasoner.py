"""
AI reasoning layer using Claude (Anthropic API).

Takes structured scoring output and generates human-readable explanations.
Claude is asked to REASON about the signals — not just reformat the data.

Example output for a single ticker:
  "AAPL is showing bearish trend signals with a perfect bearish EMA stack,
   but momentum indicators (RSI 38.7, Stochastic oversold) suggest the
   selling is overdone. Price is sitting right on the 38.2% Fibonacci
   retracement at $252.88 — a key support level. In this ranging market
   (ADX 18.9), I'd watch for a bounce off this level rather than chasing
   the downside. Confidence is low because daily and weekly timeframes
   disagree."

The reasoner generates:
  1. Per-ticker analysis (2-4 sentences each) for qualifying tickers
  2. A daily summary covering the overall market + top signals + flips
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Optional

import anthropic
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_FALLBACK_RESPONSE = "AI analysis unavailable — see raw scores above."


# ---------------------------------------------------------------------------
# DB query helpers
# ---------------------------------------------------------------------------

def _load_latest_close(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> Optional[float]:
    """
    Query the most recent close price for a ticker on or before scoring_date.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Close price as float, or None if no data found.
    """
    row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    return float(row["close"]) if row and row["close"] is not None else None


def _load_latest_indicators(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> dict:
    """
    Query the most recent indicators_daily row for a ticker on or before scoring_date.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Dict of indicator values, or empty dict if no row found.
    """
    row = db_conn.execute(
        "SELECT * FROM indicators_daily WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    return dict(row) if row else {}


def _load_recent_patterns(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str, days: int = 10
) -> list[dict]:
    """
    Query patterns_daily for a ticker over the last `days` days.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Reference date (YYYY-MM-DD).
        days: Number of days to look back.

    Returns:
        List of pattern dicts ordered by date descending.
    """
    rows = db_conn.execute(
        "SELECT date, pattern_name, direction, strength FROM patterns_daily "
        "WHERE ticker = ? AND date <= ? AND date >= date(?, ?) "
        "ORDER BY date DESC",
        (ticker, scoring_date, scoring_date, f"-{days} days"),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_recent_divergences(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str, days: int = 30
) -> list[dict]:
    """
    Query divergences_daily for a ticker over the last `days` days.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Reference date (YYYY-MM-DD).
        days: Number of days to look back.

    Returns:
        List of divergence dicts ordered by date descending.
    """
    rows = db_conn.execute(
        "SELECT date, indicator, divergence_type FROM divergences_daily "
        "WHERE ticker = ? AND date <= ? AND date >= date(?, ?) "
        "ORDER BY date DESC",
        (ticker, scoring_date, scoring_date, f"-{days} days"),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_recent_crossovers(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str, days: int = 10
) -> list[dict]:
    """
    Query crossovers_daily for a ticker over the last `days` days.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Reference date (YYYY-MM-DD).
        days: Number of days to look back.

    Returns:
        List of crossover dicts ordered by date descending.
    """
    rows = db_conn.execute(
        "SELECT date, crossover_type, direction FROM crossovers_daily "
        "WHERE ticker = ? AND date <= ? AND date >= date(?, ?) "
        "ORDER BY date DESC",
        (ticker, scoring_date, scoring_date, f"-{days} days"),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_latest_fundamentals(
    db_conn: sqlite3.Connection, ticker: str
) -> Optional[dict]:
    """
    Query the most recent fundamentals row for a ticker.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Dict of fundamental values, or None if no data found.
    """
    row = db_conn.execute(
        "SELECT * FROM fundamentals WHERE ticker = ? ORDER BY report_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


def _load_news_summary_for_context(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> Optional[dict]:
    """
    Query news_daily_summary for a ticker on or before scoring_date.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Dict of news summary values, or None if no data found.
    """
    row = db_conn.execute(
        "SELECT * FROM news_daily_summary WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    return dict(row) if row else None


def _load_short_interest_for_context(
    db_conn: sqlite3.Connection, ticker: str
) -> Optional[dict]:
    """
    Query the most recent short_interest row for a ticker.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Dict of short interest values, or None if no data found.
    """
    row = db_conn.execute(
        "SELECT * FROM short_interest WHERE ticker = ? ORDER BY settlement_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


def _load_signal_flip_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> Optional[dict]:
    """
    Query signal_flips for a ticker on the scoring date.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        scoring_date: Date to query (YYYY-MM-DD).

    Returns:
        Dict of flip values (previous_signal, new_signal, etc.), or None.
    """
    row = db_conn.execute(
        "SELECT * FROM signal_flips WHERE ticker = ? AND date = ? "
        "ORDER BY id DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    return dict(row) if row else None


def _load_ticker_sector_etf(
    db_conn: sqlite3.Connection, ticker: str
) -> Optional[str]:
    """
    Query the sector ETF symbol for a ticker from the tickers table.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Sector ETF symbol (e.g., 'XLK'), or None if not found.
    """
    row = db_conn.execute(
        "SELECT sector_etf FROM tickers WHERE symbol = ?",
        (ticker,),
    ).fetchone()
    return row["sector_etf"] if row else None


def _load_vix_level(
    db_conn: sqlite3.Connection, scoring_date: str
) -> Optional[float]:
    """
    Query the most recent VIX close on or before scoring_date.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        VIX close as float, or None if not available.
    """
    row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = '^VIX' AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()
    return float(row["close"]) if row and row["close"] is not None else None


def _load_etf_summary(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> dict:
    """
    Load close price and key indicators for a market ETF (SPY, QQQ, etc.).

    Parameters:
        db_conn: Open SQLite connection.
        ticker: ETF ticker symbol.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Dict with optional keys: close, rsi, adx, ema_alignment.
    """
    result: dict = {}

    close_row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    if close_row and close_row["close"] is not None:
        result["close"] = float(close_row["close"])

    ind_row = db_conn.execute(
        "SELECT rsi_14, adx, ema_9, ema_21, ema_50 FROM indicators_daily "
        "WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    if ind_row:
        result["rsi"] = ind_row["rsi_14"]
        result["adx"] = ind_row["adx"]
        ema_9 = ind_row["ema_9"]
        ema_21 = ind_row["ema_21"]
        ema_50 = ind_row["ema_50"]
        if ema_9 and ema_21 and ema_50:
            if ema_9 > ema_21 > ema_50:
                result["ema_alignment"] = "bullish"
            elif ema_9 < ema_21 < ema_50:
                result["ema_alignment"] = "bearish"
            else:
                result["ema_alignment"] = "mixed"

    return result


def _load_treasury_10y(
    db_conn: sqlite3.Connection, scoring_date: str
) -> Optional[float]:
    """
    Query the most recent 10-year treasury yield on or before scoring_date.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Upper-bound date (YYYY-MM-DD).

    Returns:
        Yield as float (e.g., 4.25), or None if not available.
    """
    row = db_conn.execute(
        "SELECT yield_10_year FROM treasury_yields WHERE date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()
    return float(row["yield_10_year"]) if row and row["yield_10_year"] is not None else None


def _load_sector_performance(
    db_conn: sqlite3.Connection, scoring_date: str, lookback_days: int = 5
) -> list[dict]:
    """
    Compute recent percent-change performance for all active sector ETFs.

    Compares the latest close to the close `lookback_days` trading days ago.
    Returns sorted list (best to worst) with keys: etf, pct_change.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Reference date (YYYY-MM-DD).
        lookback_days: Number of days for the lookback window.

    Returns:
        List of dicts with keys 'etf' and 'pct_change', sorted descending.
    """
    etf_rows = db_conn.execute(
        "SELECT DISTINCT sector_etf FROM tickers "
        "WHERE sector_etf IS NOT NULL AND active = 1",
    ).fetchall()
    etf_list = [r["sector_etf"] for r in etf_rows if r["sector_etf"]]

    results: list[dict] = []
    for etf in etf_list:
        curr = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? "
            "ORDER BY date DESC LIMIT 1",
            (etf, scoring_date),
        ).fetchone()
        prev = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= date(?, ?) "
            "ORDER BY date DESC LIMIT 1",
            (etf, scoring_date, f"-{lookback_days} days"),
        ).fetchone()
        if curr and curr["close"] is not None and prev and prev["close"] is not None:
            pct_change = (float(curr["close"]) - float(prev["close"])) / float(prev["close"]) * 100
            results.append({"etf": etf, "pct_change": pct_change})

    return sorted(results, key=lambda x: x["pct_change"], reverse=True)


# ---------------------------------------------------------------------------
# Context interpretation helpers
# ---------------------------------------------------------------------------

def _interpret_vix(vix_value: float) -> str:
    """
    Return a human-readable interpretation of the VIX level.

    Thresholds: calm < 15, normal 15-20, elevated 20-25, high 25-30, extreme > 30.

    Parameters:
        vix_value: Current VIX level.

    Returns:
        String label for the VIX level.
    """
    if vix_value < 15:
        return "calm"
    if vix_value < 20:
        return "normal"
    if vix_value < 25:
        return "elevated"
    if vix_value < 30:
        return "high"
    return "extreme"


# ---------------------------------------------------------------------------
# Context formatting helpers
# ---------------------------------------------------------------------------

def _format_key_signals(key_signals_json: str) -> str:
    """
    Format the key_signals JSON string as a bulleted list.

    Parameters:
        key_signals_json: JSON-encoded list of signal strings.

    Returns:
        Bulleted multi-line string, or a placeholder if empty.
    """
    try:
        signals = json.loads(key_signals_json or "[]")
        if not signals:
            return "  (none recorded)"
        return "\n".join(f"  • {s}" for s in signals)
    except (json.JSONDecodeError, TypeError):
        return "  (none recorded)"


def _format_category_scores(score: dict) -> str:
    """
    Format all 9 category scores into a compact readable block.

    Parameters:
        score: Score dict containing the 9 category score fields.

    Returns:
        Multi-line string with three rows of category labels and values.
    """
    return (
        f"  Trend: {score.get('trend_score', 0):+.1f} | "
        f"Momentum: {score.get('momentum_score', 0):+.1f} | "
        f"Volume: {score.get('volume_score', 0):+.1f}\n"
        f"  Volatility: {score.get('volatility_score', 0):+.1f} | "
        f"Candlestick: {score.get('candlestick_score', 0):+.1f} | "
        f"Structural: {score.get('structural_score', 0):+.1f}\n"
        f"  Sentiment: {score.get('sentiment_score', 0):+.1f} | "
        f"Fundamental: {score.get('fundamental_score', 0):+.1f} | "
        f"Macro: {score.get('macro_score', 0):+.1f}"
    )


def _format_indicators(indicators: dict) -> str:
    """
    Format indicator values from indicators_daily into a readable block.

    Parameters:
        indicators: Dict of indicator column values.

    Returns:
        Multi-line string of indicator values, or a placeholder if empty.
    """
    if not indicators:
        return "  (no indicator data available)"

    obv_val = indicators.get("obv")
    if obv_val is not None:
        obv_dir = "rising" if float(obv_val) > 0 else "falling"
    else:
        obv_dir = "N/A"

    return (
        f"  RSI: {indicators.get('rsi_14', 'N/A')} | "
        f"MACD: {indicators.get('macd_line', 'N/A')} | "
        f"ADX: {indicators.get('adx', 'N/A')}\n"
        f"  EMA 9: {indicators.get('ema_9', 'N/A')} | "
        f"EMA 21: {indicators.get('ema_21', 'N/A')} | "
        f"EMA 50: {indicators.get('ema_50', 'N/A')}\n"
        f"  Stochastic K/D: {indicators.get('stoch_k', 'N/A')}/{indicators.get('stoch_d', 'N/A')}\n"
        f"  BB %B: {indicators.get('bb_pctb', 'N/A')} | ATR: {indicators.get('atr_14', 'N/A')}\n"
        f"  OBV trend: {obv_dir} | CMF: {indicators.get('cmf_20', 'N/A')}"
    )


def _format_patterns(patterns: list[dict]) -> str:
    """
    Format a list of recent candlestick/structural patterns.

    Parameters:
        patterns: List of pattern dicts with date, pattern_name, direction, strength.

    Returns:
        Multi-line string, or a placeholder if none.
    """
    if not patterns:
        return "  None in last 10 days"
    lines = [
        f"  {p.get('date', '?')} | {p.get('pattern_name', '?')} | "
        f"{p.get('direction', '?')} | strength={p.get('strength', '?')}"
        for p in patterns
    ]
    return "\n".join(lines)


def _format_divergences(divergences: list[dict]) -> str:
    """
    Format a list of recent divergences.

    Parameters:
        divergences: List of divergence dicts with date, indicator, divergence_type.

    Returns:
        Multi-line string, or a placeholder if none.
    """
    if not divergences:
        return "  None in last 30 days"
    lines = [
        f"  {d.get('date', '?')} | {d.get('indicator', '?')} | {d.get('divergence_type', '?')}"
        for d in divergences
    ]
    return "\n".join(lines)


def _format_crossovers(crossovers: list[dict]) -> str:
    """
    Format a list of recent crossovers.

    Parameters:
        crossovers: List of crossover dicts with date, crossover_type, direction.

    Returns:
        Multi-line string, or a placeholder if none.
    """
    if not crossovers:
        return "  None in last 10 days"
    lines = [
        f"  {c.get('date', '?')} | {c.get('crossover_type', '?')} | {c.get('direction', '?')}"
        for c in crossovers
    ]
    return "\n".join(lines)


def _format_fundamentals(fundamentals: Optional[dict]) -> str:
    """
    Format the fundamentals dict into a readable snapshot.

    Parameters:
        fundamentals: Dict from the fundamentals table, or None.

    Returns:
        Multi-line string with key ratios and metrics.
    """
    if not fundamentals:
        return "  (No fundamentals data available)"

    pe = fundamentals.get("pe_ratio")
    eps_growth = fundamentals.get("eps_growth_yoy")
    rev_growth = fundamentals.get("revenue_growth_yoy")
    debt_equity = fundamentals.get("debt_to_equity")
    market_cap = fundamentals.get("market_cap")

    pe_str = f"{pe:.1f}" if pe is not None else "N/A"
    eps_str = f"{eps_growth:.1%}" if eps_growth is not None else "N/A"
    rev_str = f"{rev_growth:.1%}" if rev_growth is not None else "N/A"
    de_str = f"{debt_equity:.2f}" if debt_equity is not None else "N/A"

    if market_cap and market_cap >= 1e12:
        cap_str = f"${market_cap / 1e12:.2f}T"
    elif market_cap and market_cap >= 1e9:
        cap_str = f"${market_cap / 1e9:.1f}B"
    else:
        cap_str = "N/A"

    return (
        f"  P/E: {pe_str} | EPS Growth YoY: {eps_str} | Revenue Growth: {rev_str}\n"
        f"  Debt/Equity: {de_str} | Market Cap: {cap_str}"
    )


def _format_news(news: Optional[dict]) -> str:
    """
    Format the news_daily_summary dict into a readable block.

    Parameters:
        news: Dict from the news_daily_summary table, or None.

    Returns:
        Multi-line string with sentiment score, count, and top headline.
    """
    if not news:
        return "  No news data available"

    avg_sentiment = news.get("avg_sentiment_score")
    article_count = news.get("article_count", 0)
    top_headline = news.get("top_headline") or "(none)"

    sentiment_str = f"{avg_sentiment:.2f}" if avg_sentiment is not None else "N/A"

    return (
        f"  Avg Sentiment: {sentiment_str} | Article Count: {article_count}\n"
        f"  Recent headline: {top_headline}"
    )


def _format_short_interest(short_data: Optional[dict]) -> str:
    """
    Format short interest data into a readable string.

    Parameters:
        short_data: Dict from the short_interest table, or None.

    Returns:
        String with days-to-cover value.
    """
    if not short_data:
        return "  (No short interest data)"
    dtc = short_data.get("days_to_cover")
    dtc_str = f"{dtc:.2f}" if dtc is not None else "N/A"
    return f"  Days to Cover: {dtc_str}"


def _format_signal_flip(flip: Optional[dict]) -> str:
    """
    Format signal flip information if present.

    Parameters:
        flip: Dict from signal_flips table, or None.

    Returns:
        Formatted SIGNAL CHANGE block, or empty string if no flip.
    """
    if not flip:
        return ""
    return (
        f"\nSIGNAL CHANGE: Previously {flip.get('previous_signal', '?')} "
        f"({flip.get('previous_confidence', '?')}%)\n"
        f"  → Now {flip.get('new_signal', '?')} ({flip.get('new_confidence', '?')}%)"
    )


def _compute_fibonacci_context(
    db_conn: sqlite3.Connection, ticker: str
) -> str:
    """
    Compute Fibonacci retracement levels for context via lazy import from calculator.

    Queries swing points from the DB and computes levels on-the-fly.
    Returns a formatted string describing the levels and whether price is near one.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.

    Returns:
        Formatted multi-line Fibonacci summary, or an N/A message on failure.
    """
    try:
        from src.calculator.fibonacci import compute_fibonacci_for_ticker  # lazy import
        from src.common.config import load_config

        calc_config = load_config("calculator")
        fib_result = compute_fibonacci_for_ticker(db_conn, ticker, calc_config)
        if fib_result is None:
            return "  N/A — no significant swing data"

        swing_low = fib_result["swing_low"]
        swing_high = fib_result["swing_high"]
        levels = fib_result.get("levels", [])
        nearest = fib_result.get("nearest_level")

        lines = [
            f"  Swing: ${swing_low['price']:.2f} → ${swing_high['price']:.2f}"
        ]
        for lvl in levels:
            pct = lvl.get("level_pct", 0)
            price = lvl.get("price", 0)
            is_nearest = (
                nearest is not None
                and abs(float(nearest.get("price", 0)) - float(price)) < 0.01
            )
            marker = " ← price nearby" if is_nearest else ""
            lines.append(f"  {pct:.1%} at ${price:.2f}{marker}")

        return "\n".join(lines)
    except Exception as exc:
        logger.debug(f"Fibonacci context unavailable for {ticker}: {exc}")
        return "  N/A — insufficient swing data"


def _compute_rs_context(
    db_conn: sqlite3.Connection, ticker: str, sector_etf: Optional[str]
) -> str:
    """
    Compute relative strength values for context via lazy import from calculator.

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        sector_etf: Sector ETF symbol (e.g., 'XLK'), or None.

    Returns:
        Formatted relative strength string, or N/A values on failure.
    """
    try:
        from src.calculator.relative_strength import compute_relative_strength_for_ticker
        from src.common.config import load_config

        calc_config = load_config("calculator")
        rs_result = compute_relative_strength_for_ticker(db_conn, ticker, calc_config)
        rs_market = rs_result.get("rs_market")
        rs_sector = rs_result.get("rs_sector")

        market_str = f"{rs_market:.4f}" if rs_market is not None else "N/A"
        sector_label = sector_etf or "sector"
        sector_str = f"{rs_sector:.4f}" if rs_sector is not None else "N/A"

        return f"  vs Market (SPY): {market_str} | vs Sector ({sector_label}): {sector_str}"
    except Exception as exc:
        logger.debug(f"Relative strength context unavailable for {ticker}: {exc}")
        return f"  vs Market (SPY): N/A | vs Sector ({sector_etf or 'sector'}): N/A"


# ---------------------------------------------------------------------------
# Public context builders
# ---------------------------------------------------------------------------

def build_ticker_context(
    db_conn: sqlite3.Connection,
    ticker: str,
    score: dict,
    scoring_date: str,
) -> str:
    """
    Build a comprehensive context string for Claude about this ticker.

    Queries all relevant data from the DB (indicators, patterns, divergences,
    crossovers, fundamentals, news, short interest, signal flips) and computes
    Fibonacci levels and relative strength on-the-fly. Formats everything into
    a structured, readable string that gives Claude maximum context to reason well.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol (e.g., 'AAPL').
        score: Dict from scores_daily containing signal, confidence, category scores, etc.
        scoring_date: Date being analyzed (YYYY-MM-DD).

    Returns:
        Multi-section formatted string with all ticker context.
    """
    close_price = _load_latest_close(db_conn, ticker, scoring_date)
    indicators = _load_latest_indicators(db_conn, ticker, scoring_date)
    patterns = _load_recent_patterns(db_conn, ticker, scoring_date, days=10)
    divergences = _load_recent_divergences(db_conn, ticker, scoring_date, days=30)
    crossovers = _load_recent_crossovers(db_conn, ticker, scoring_date, days=10)
    fundamentals = _load_latest_fundamentals(db_conn, ticker)
    news = _load_news_summary_for_context(db_conn, ticker, scoring_date)
    short_data = _load_short_interest_for_context(db_conn, ticker)
    flip = _load_signal_flip_for_ticker(db_conn, ticker, scoring_date)
    sector_etf = _load_ticker_sector_etf(db_conn, ticker)

    fibonacci_context = _compute_fibonacci_context(db_conn, ticker)
    rs_context = _compute_rs_context(db_conn, ticker, sector_etf)

    price_str = f"${close_price:.2f}" if close_price is not None else "N/A"
    signal = score.get("signal", "N/A")
    confidence = score.get("confidence", 0)
    final_score = score.get("final_score", 0) or 0
    regime = score.get("regime", "N/A")
    daily_score = score.get("daily_score", 0) or 0
    weekly_score = score.get("weekly_score", 0) or 0

    flip_section = _format_signal_flip(flip)

    parts = [
        f"Ticker: {ticker} | Price: {price_str} | Date: {scoring_date}",
        f"Signal: {signal} | Confidence: {confidence}% | Score: {final_score:+.1f}",
        f"Regime: {regime}",
        "",
        "Category Scores (-100 to +100):",
        _format_category_scores(score),
        "",
        "Key Signals:",
        _format_key_signals(score.get("key_signals", "[]")),
        "",
        "Recent Indicators:",
        _format_indicators(indicators),
        "",
        f"Daily Score: {daily_score:+.1f} | Weekly Score: {weekly_score:+.1f}",
        "",
        "Recent Patterns (last 10 days):",
        _format_patterns(patterns),
        "",
        "Recent Divergences (last 30 days):",
        _format_divergences(divergences),
        "",
        "Recent Crossovers (last 10 days):",
        _format_crossovers(crossovers),
        "",
        "Fibonacci:",
        fibonacci_context,
        "",
        "Fundamentals:",
        _format_fundamentals(fundamentals),
        "",
        "News Sentiment (last 7 days):",
        _format_news(news),
        "",
        "Short Interest:",
        _format_short_interest(short_data),
        "",
        "Relative Strength:",
        rs_context,
    ]

    if flip_section:
        parts.append(flip_section)

    return "\n".join(parts)


def build_market_context(
    db_conn: sqlite3.Connection, scoring_date: str
) -> str:
    """
    Build overall market context (VIX, SPY, QQQ, treasury yields, sector performance).

    Queries ohlcv_daily and indicators_daily for SPY and QQQ, the treasury_yields
    table for the 10Y yield, and sector ETF performance from ohlcv_daily.
    All missing data is handled gracefully with N/A placeholders.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date: Date being analyzed (YYYY-MM-DD).

    Returns:
        Multi-line formatted market context string.
    """
    vix = _load_vix_level(db_conn, scoring_date)
    vix_str = f"{vix:.1f} ({_interpret_vix(vix)})" if vix is not None else "N/A"

    spy = _load_etf_summary(db_conn, "SPY", scoring_date)
    spy_close = f"${spy['close']:.2f}" if "close" in spy else "N/A"
    spy_rsi = spy.get("rsi", "N/A")
    spy_adx = spy.get("adx", "N/A")
    spy_ema = spy.get("ema_alignment", "N/A")
    spy_str = f"close={spy_close} | RSI={spy_rsi} | ADX={spy_adx} | EMA alignment: {spy_ema}"

    qqq = _load_etf_summary(db_conn, "QQQ", scoring_date)
    qqq_close = f"${qqq['close']:.2f}" if "close" in qqq else "N/A"

    yield_10y = _load_treasury_10y(db_conn, scoring_date)
    yield_str = f"{yield_10y:.2f}%" if yield_10y is not None else "N/A"

    sector_perf = _load_sector_performance(db_conn, scoring_date)
    if sector_perf:
        leaders_str = ", ".join(
            f"{s['etf']} ({s['pct_change']:+.1f}%)" for s in sector_perf[:3]
        )
        laggards_str = ", ".join(
            f"{s['etf']} ({s['pct_change']:+.1f}%)" for s in sector_perf[-3:]
        ) if len(sector_perf) >= 3 else ", ".join(
            f"{s['etf']} ({s['pct_change']:+.1f}%)" for s in reversed(sector_perf)
        )
    else:
        leaders_str = "N/A"
        laggards_str = "N/A"

    lines = [
        f"Market Context — {scoring_date}",
        f"  VIX: {vix_str}",
        f"  SPY (S&P 500): {spy_str}",
        f"  QQQ (Nasdaq): close={qqq_close}",
        f"  10Y Treasury Yield: {yield_str}",
        f"  Sector Leaders: {leaders_str}",
        f"  Sector Laggards: {laggards_str}",
    ]
    return "\n".join(lines)


def build_prompt_for_ticker(
    ticker_context: str,
    market_context: str,
    is_flip: bool = False,
) -> str:
    """
    Build the full prompt to send to Claude for a single ticker analysis.

    Includes system role, format instructions, market context, ticker data,
    and an optional flip instruction when the signal has changed direction.

    Parameters:
        ticker_context: Formatted string from build_ticker_context().
        market_context: Formatted string from build_market_context().
        is_flip: If True, adds an instruction to focus on the direction change.

    Returns:
        Complete prompt string ready to send to Claude.
    """
    flip_instruction = (
        "\nIMPORTANT: This stock's signal just changed direction. Focus your analysis "
        "on what caused the change and whether it's likely to sustain.\n"
        if is_flip
        else ""
    )

    return (
        "You are an expert technical and fundamental analyst reviewing stock signals\n"
        "from a quantitative scoring engine. Your job is to INTERPRET the signals,\n"
        "not just summarize them.\n\n"
        "For each stock, provide a concise analysis (2-4 sentences) that:\n"
        "1. States the overall signal direction and why\n"
        "2. Identifies the most important technical factor driving the signal\n"
        "3. Notes any conflicting signals or risks\n"
        "4. Mentions any relevant fundamental or macro context\n\n"
        "Be direct and specific. Use actual numbers. Don't hedge excessively.\n"
        "Write as if briefing a portfolio manager who needs actionable insight.\n"
        f"{flip_instruction}\n"
        "MARKET CONTEXT:\n"
        f"{market_context}\n\n"
        "STOCK DATA:\n"
        f"{ticker_context}\n\n"
        "Provide your analysis in 2-4 sentences. Be concise and actionable."
    )


def _format_ticker_list_for_summary(tickers: list[dict]) -> str:
    """
    Format a list of qualifying ticker dicts for the daily summary prompt.

    Each ticker dict has keys: ticker, score (a scores_daily dict).

    Parameters:
        tickers: List of ticker dicts with 'ticker' and 'score' keys.

    Returns:
        Indented multi-line string, or '(none)' if empty.
    """
    if not tickers:
        return "  (none)"
    lines: list[str] = []
    for item in tickers:
        ticker = item.get("ticker", "?")
        score = item.get("score", {})
        confidence = score.get("confidence", 0)
        final_score = score.get("final_score", 0) or 0
        key_signals: list[str] = []
        try:
            key_signals = json.loads(score.get("key_signals", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            pass
        key_reason = key_signals[0] if key_signals else "N/A"
        lines.append(
            f"  {ticker}: score={final_score:+.1f}, confidence={confidence:.0f}%, "
            f"reason: {key_reason}"
        )
    return "\n".join(lines)


def _format_flips_list_for_summary(flips: list[dict]) -> str:
    """
    Format a list of signal flip dicts for the daily summary prompt.

    Each flip dict has keys: ticker, flip (signal_flips row), score.

    Parameters:
        flips: List of flip dicts.

    Returns:
        Indented multi-line string, or '(none)' if empty.
    """
    if not flips:
        return "  (none)"
    lines: list[str] = []
    for item in flips:
        ticker = item.get("ticker", "?")
        flip = item.get("flip", {})
        prev = flip.get("previous_signal", "?")
        new = flip.get("new_signal", "?")
        confidence = item.get("score", {}).get("confidence", 0)
        lines.append(f"  {ticker}: {prev} → {new} (confidence: {confidence:.0f}%)")
    return "\n".join(lines)


def build_prompt_for_daily_summary(
    bullish_tickers: list[dict],
    bearish_tickers: list[dict],
    flips: list[dict],
    market_context: str,
) -> str:
    """
    Build the prompt for the daily market summary.

    Compiles all qualifying tickers (bullish, bearish, flips) into a structured
    prompt asking Claude to write cohesive 3-5 sentence prose covering market
    tone, strongest signals, notable flips, and sector themes.

    Parameters:
        bullish_tickers: List of dicts with ticker, score keys.
        bearish_tickers: List of dicts with ticker, score keys.
        flips: List of dicts with ticker, flip, score keys.
        market_context: Formatted market context string.

    Returns:
        Complete prompt string for the daily summary.
    """
    bullish_str = _format_ticker_list_for_summary(bullish_tickers)
    bearish_str = _format_ticker_list_for_summary(bearish_tickers)
    flips_str = _format_flips_list_for_summary(flips)

    return (
        "You are an expert market analyst writing a daily signal briefing.\n"
        "Summarize today's signals in a concise, actionable format.\n\n"
        "MARKET CONTEXT:\n"
        f"{market_context}\n\n"
        f"TODAY'S BULLISH SIGNALS ({len(bullish_tickers)}):\n"
        f"{bullish_str}\n\n"
        f"TODAY'S BEARISH SIGNALS ({len(bearish_tickers)}):\n"
        f"{bearish_str}\n\n"
        f"SIGNAL CHANGES ({len(flips)}):\n"
        f"{flips_str}\n\n"
        "Write a 3-5 sentence market summary that:\n"
        "1. Notes the overall market tone\n"
        "2. Highlights the strongest conviction signals (highest confidence)\n"
        "3. Mentions any notable signal flips and what they suggest\n"
        "4. Identifies any sector themes (e.g., 'energy stocks dominating bullish signals')\n\n"
        "Be concise. No bullet points in your response — write flowing prose."
    )


# ---------------------------------------------------------------------------
# Claude API caller
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(anthropic.RateLimitError),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _invoke_claude_with_retry(
    client: anthropic.Anthropic,
    model: str,
    max_tokens: int,
    temperature: float,
    prompt: str,
) -> Any:
    """
    Make a single Claude API call. Wrapped by the retry decorator for rate limit handling.

    Parameters:
        client: Authenticated Anthropic client instance.
        model: Model identifier string.
        max_tokens: Maximum tokens in the response.
        temperature: Sampling temperature.
        prompt: The user prompt to send.

    Returns:
        Anthropic message object with .content[0].text.
    """
    return client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        messages=[{"role": "user", "content": prompt}],
    )


def call_claude(prompt: str, config: dict) -> str:
    """
    Call the Claude API and return the response text.

    Reads model, max_tokens, and temperature from config['ai_reasoner'].
    Retries on rate limit errors (max 3 attempts with exponential backoff).
    Returns a fallback string on any error — never crashes the pipeline.

    Parameters:
        prompt: The full prompt string to send to Claude.
        config: Notifier config dict containing the 'ai_reasoner' section.

    Returns:
        Claude's response text, or the fallback string if any error occurs.
    """
    ai_cfg = config.get("ai_reasoner", {})
    model: str = ai_cfg.get("model", "claude-sonnet-4-20250514")
    max_tokens: int = ai_cfg.get("max_tokens", 1024)
    temperature: float = ai_cfg.get("temperature", 0.3)

    try:
        client = anthropic.Anthropic()
        response = _invoke_claude_with_retry(client, model, max_tokens, temperature, prompt)
        return response.content[0].text
    except anthropic.APIError as exc:
        logger.error(f"Claude API error: phase=ai_reasoner, error={exc!r}")
        return _FALLBACK_RESPONSE
    except anthropic.APIConnectionError as exc:
        logger.error(f"Claude connection error: phase=ai_reasoner, error={exc!r}")
        return _FALLBACK_RESPONSE
    except Exception as exc:
        logger.error(f"Claude unexpected error: phase=ai_reasoner, error={exc!r}")
        return _FALLBACK_RESPONSE


# ---------------------------------------------------------------------------
# High-level reasoning functions
# ---------------------------------------------------------------------------

def generate_ticker_reasoning(
    db_conn: sqlite3.Connection,
    ticker: str,
    score: dict,
    market_context: str,
    config: dict,
    is_flip: bool = False,
) -> str:
    """
    Generate Claude's analysis for a single ticker.

    Builds the full ticker context from the DB, constructs the prompt,
    and calls Claude. Returns the raw Claude response text.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        score: Dict from scores_daily for this ticker and date.
        market_context: Pre-built market context string.
        config: Notifier config dict.
        is_flip: If True, includes flip-specific instructions in the prompt.

    Returns:
        Claude's analysis string (2-4 sentences), or the fallback string on error.
    """
    scoring_date = score.get("date", "")
    ticker_context = build_ticker_context(db_conn, ticker, score, scoring_date)
    prompt = build_prompt_for_ticker(ticker_context, market_context, is_flip=is_flip)
    return call_claude(prompt, config)


def generate_daily_summary(
    db_conn: sqlite3.Connection,
    bullish_tickers: list[dict],
    bearish_tickers: list[dict],
    flips: list[dict],
    market_context: str,
    config: dict,
) -> str:
    """
    Generate Claude's daily market summary covering all qualifying tickers.

    Returns a fixed 'No significant signals today.' message if there are no
    qualifying tickers and no flips, without calling the Claude API.

    Parameters:
        db_conn: Open SQLite connection (not used directly but kept for consistency).
        bullish_tickers: List of qualifying bullish ticker dicts.
        bearish_tickers: List of qualifying bearish ticker dicts.
        flips: List of signal flip dicts.
        market_context: Pre-built market context string.
        config: Notifier config dict.

    Returns:
        Claude's 3-5 sentence daily summary, or 'No significant signals today.'
    """
    if not bullish_tickers and not bearish_tickers and not flips:
        return "No significant signals today."

    prompt = build_prompt_for_daily_summary(
        bullish_tickers, bearish_tickers, flips, market_context
    )
    return call_claude(prompt, config)


def get_qualifying_tickers(
    db_conn: sqlite3.Connection, scoring_date: str, config: dict
) -> dict:
    """
    Query scores_daily and signal_flips to identify qualifying tickers for today.

    Qualifying:
      - bullish: signal='BULLISH' AND confidence >= threshold
      - bearish: signal='BEARISH' AND confidence >= threshold
      - flips: all entries from signal_flips for this date (always included)

    Each bucket is sorted by confidence descending and capped at max_tickers_per_section.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date: Date to query (YYYY-MM-DD).
        config: Notifier config dict; reads telegram.confidence_threshold and
            telegram.max_tickers_per_section.

    Returns:
        Dict with keys 'bullish', 'bearish', 'flips', each a list of row dicts.
    """
    telegram_cfg = config.get("telegram", {})
    threshold: float = telegram_cfg.get("confidence_threshold", 70)
    max_per_section: int = telegram_cfg.get("max_tickers_per_section", 10)

    bullish_rows = db_conn.execute(
        "SELECT * FROM scores_daily WHERE date = ? AND signal = 'BULLISH' AND confidence >= ? "
        "ORDER BY confidence DESC",
        (scoring_date, threshold),
    ).fetchall()

    bearish_rows = db_conn.execute(
        "SELECT * FROM scores_daily WHERE date = ? AND signal = 'BEARISH' AND confidence >= ? "
        "ORDER BY confidence DESC",
        (scoring_date, threshold),
    ).fetchall()

    flip_rows = db_conn.execute(
        "SELECT * FROM signal_flips WHERE date = ? ORDER BY id ASC",
        (scoring_date,),
    ).fetchall()

    return {
        "bullish": [dict(r) for r in bullish_rows[:max_per_section]],
        "bearish": [dict(r) for r in bearish_rows[:max_per_section]],
        "flips": [dict(r) for r in flip_rows],
    }


def _load_score_for_ticker(
    db_conn: sqlite3.Connection, ticker: str, scoring_date: str
) -> Optional[dict]:
    """
    Load a scores_daily row for a specific ticker and date.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        scoring_date: Date (YYYY-MM-DD).

    Returns:
        Dict of score values, or None if not found.
    """
    row = db_conn.execute(
        "SELECT * FROM scores_daily WHERE ticker = ? AND date = ?",
        (ticker, scoring_date),
    ).fetchone()
    return dict(row) if row else None


def reason_all_qualifying_tickers(
    db_conn: sqlite3.Connection, scoring_date: str, config: dict
) -> dict:
    """
    Run AI reasoning for all qualifying tickers and produce the daily summary.

    Steps:
      1. Identify qualifying tickers (bullish, bearish, flips).
      2. Build market context once (reused for all tickers).
      3. Generate per-ticker reasoning for bullish + bearish (with flip flag if applicable).
      4. Generate per-ticker reasoning for flip-only tickers (not already processed).
      5. Generate the daily summary.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date: Date being processed (YYYY-MM-DD).
        config: Notifier config dict.

    Returns:
        Dict with keys:
            'bullish': list of {ticker, score, reasoning}
            'bearish': list of {ticker, score, reasoning}
            'flips': list of {ticker, flip, score, reasoning}
            'daily_summary': str
            'market_context_summary': str
    """
    qualifying = get_qualifying_tickers(db_conn, scoring_date, config)
    bullish_rows: list[dict] = qualifying["bullish"]
    bearish_rows: list[dict] = qualifying["bearish"]
    flip_rows: list[dict] = qualifying["flips"]

    market_context = build_market_context(db_conn, scoring_date)

    # Build a set of flip ticker symbols for quick lookup
    flip_ticker_set = {f["ticker"] for f in flip_rows}

    # Track processed tickers to avoid double-processing flip tickers
    processed_tickers: set[str] = set()

    bullish_results: list[dict] = []
    for score_row in bullish_rows:
        ticker = score_row["ticker"]
        processed_tickers.add(ticker)
        is_flip = ticker in flip_ticker_set
        reasoning = generate_ticker_reasoning(
            db_conn, ticker, score_row, market_context, config, is_flip=is_flip
        )
        bullish_results.append({"ticker": ticker, "score": score_row, "reasoning": reasoning})
        logger.info(f"ticker={ticker} phase=ai_reasoner date={scoring_date} signal=BULLISH generated")

    bearish_results: list[dict] = []
    for score_row in bearish_rows:
        ticker = score_row["ticker"]
        processed_tickers.add(ticker)
        is_flip = ticker in flip_ticker_set
        reasoning = generate_ticker_reasoning(
            db_conn, ticker, score_row, market_context, config, is_flip=is_flip
        )
        bearish_results.append({"ticker": ticker, "score": score_row, "reasoning": reasoning})
        logger.info(f"ticker={ticker} phase=ai_reasoner date={scoring_date} signal=BEARISH generated")

    flip_results: list[dict] = []
    for flip_row in flip_rows:
        ticker = flip_row["ticker"]
        if ticker in processed_tickers:
            # Already reasoned above with is_flip=True; add to flips list without extra call
            existing = next(
                (r for r in bullish_results + bearish_results if r["ticker"] == ticker),
                None,
            )
            if existing:
                flip_results.append({
                    "ticker": ticker,
                    "flip": flip_row,
                    "score": existing["score"],
                    "reasoning": existing["reasoning"],
                })
            continue

        # Flip-only ticker: below threshold but has a signal change
        score_row = _load_score_for_ticker(db_conn, ticker, scoring_date)
        if score_row is None:
            logger.warning(f"ticker={ticker} phase=ai_reasoner date={scoring_date} no score row found for flip")
            continue

        reasoning = generate_ticker_reasoning(
            db_conn, ticker, score_row, market_context, config, is_flip=True
        )
        flip_results.append({
            "ticker": ticker,
            "flip": flip_row,
            "score": score_row,
            "reasoning": reasoning,
        })
        processed_tickers.add(ticker)
        logger.info(f"ticker={ticker} phase=ai_reasoner date={scoring_date} flip-only reasoning generated")

    daily_summary = generate_daily_summary(
        db_conn, bullish_results, bearish_results, flip_results, market_context, config
    )

    return {
        "bullish": bullish_results,
        "bearish": bearish_results,
        "flips": flip_results,
        "daily_summary": daily_summary,
        "market_context_summary": market_context,
    }
