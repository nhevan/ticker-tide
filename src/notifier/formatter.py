"""
Telegram message formatter for the Stock Signal Engine.

Formats the AI reasoner output into readable Telegram messages.
Handles Telegram's 4096-character limit by splitting long messages at
section boundaries. Displays times in the configured timezone
(default: Europe/Amsterdam).

Message structure:
  📊 Signal Report — March 16, 2026 • 01:23 CET
  ━━━━━━━━━━━━━━━━━━━━━━━━━
  🟢 11 | 🔴 5 | 🟡 43
  ━━━━━━━━━━━━━━━━━━━━━━━━━

  📋 Daily Summary
  {AI-generated summary}

  🟢 BULLISH (high confidence)
  ━━━━━━━━━━━━━━━━━━━━━━━━━
  WMT — 67% 📊 +41.8
  {AI reasoning}

  🔴 BEARISH (high confidence)
  ━━━━━━━━━━━━━━━━━━━━━━━━━
  PYPL — 46% 📊 -36.1
  {AI reasoning}

  🔄 SIGNAL FLIPS
  ━━━━━━━━━━━━━━━━━━━━━━━━━
  AAPL: NEUTRAL → BULLISH (72%)
  {AI reasoning}

  📉 Market Context
  ━━━━━━━━━━━━━━━━━━━━━━━━━
  VIX: 23.5 (elevated) | SPY: ranging

  ✅ Pipeline completed at 01:23 CET
  Fetcher: 2m 15s | Calculator: 12m 4s | Scorer: 2m 14s
  Tickers: 59/59 (0 failed) | Signals: 3 🟢 2 🔴 54 🟡
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━━━━"
MAX_TELEGRAM_LENGTH = 4096


def format_duration(seconds: float) -> str:
    """
    Format a number of seconds into a human-readable duration string.

    Parameters:
        seconds: Duration in seconds (may be fractional).

    Returns:
        String like "45s", "2m 15s", or "1h 2m 5s".
    """
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    if total < 3600:
        minutes, secs = divmod(total, 60)
        return f"{minutes}m {secs}s"
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes}m {secs}s"


def format_header(scoring_date: str, display_timezone: str) -> str:
    """
    Format the report header line with date and local time.

    Converts the current UTC time to the display timezone and formats it.

    Parameters:
        scoring_date: Trading date in YYYY-MM-DD format.
        display_timezone: IANA timezone name (e.g. "Europe/Amsterdam").

    Returns:
        Two-line string: header line + divider.
    """
    now_utc = datetime.now(tz=timezone.utc)
    local_dt = now_utc.astimezone(ZoneInfo(display_timezone))
    tz_abbr = local_dt.strftime("%Z")
    time_str = local_dt.strftime("%H:%M")

    parsed_date = datetime.strptime(scoring_date, "%Y-%m-%d")
    day = str(parsed_date.day)
    date_str = parsed_date.strftime(f"%B {day}, %Y")

    return f"📊 Signal Report — {date_str} • {time_str} {tz_abbr}\n{DIVIDER}"


def format_signal_distribution(bullish_count: int, bearish_count: int, neutral_count: int) -> str:
    """
    Format the signal distribution summary line.

    Parameters:
        bullish_count: Number of BULLISH signals.
        bearish_count: Number of BEARISH signals.
        neutral_count: Number of NEUTRAL signals.

    Returns:
        Single distribution line followed by a divider.
    """
    return f"🟢 {bullish_count} | 🔴 {bearish_count} | 🟡 {neutral_count}\n{DIVIDER}"


def format_daily_summary_section(daily_summary: str) -> str:
    """
    Format the daily AI summary section.

    Returns an empty string if the summary is the no-signals placeholder or empty,
    so the section is omitted entirely rather than shown as blank.

    Parameters:
        daily_summary: AI-generated summary text.

    Returns:
        Formatted section string, or empty string if no meaningful summary.
    """
    if not daily_summary or daily_summary.strip() == "No significant signals today.":
        return ""
    return f"\n📋 Daily Summary\n{DIVIDER}\n{daily_summary}"


def format_bullish_section(bullish_tickers: list[dict]) -> str:
    """
    Format the BULLISH signals section.

    Tickers are sorted by confidence descending (highest conviction first).
    Each ticker shows a concise summary line followed by AI reasoning.

    Parameters:
        bullish_tickers: List of dicts with keys: ticker, score (dict with
            confidence and final_score), reasoning.

    Returns:
        Formatted bullish section, or empty string if no tickers provided.
    """
    if not bullish_tickers:
        return ""

    sorted_tickers = sorted(
        bullish_tickers,
        key=lambda item: item["score"].get("confidence", 0),
        reverse=True,
    )

    lines = [f"\n🟢 BULLISH (high confidence)\n{DIVIDER}"]
    for item in sorted_tickers:
        ticker = item["ticker"]
        confidence = item["score"].get("confidence", 0)
        final_score = item["score"].get("final_score", 0) or 0
        reasoning = item.get("reasoning", "")
        lines.append(f"\n{ticker} — {confidence:.0f}% 📊 {final_score:+.1f}\n{reasoning}")

    return "\n".join(lines)


def format_bearish_section(bearish_tickers: list[dict]) -> str:
    """
    Format the BEARISH signals section.

    Tickers are sorted by confidence descending (highest conviction bearish first).
    Each ticker shows a concise summary line followed by AI reasoning.

    Parameters:
        bearish_tickers: List of dicts with keys: ticker, score (dict with
            confidence and final_score), reasoning.

    Returns:
        Formatted bearish section, or empty string if no tickers provided.
    """
    if not bearish_tickers:
        return ""

    sorted_tickers = sorted(
        bearish_tickers,
        key=lambda item: item["score"].get("confidence", 0),
        reverse=True,
    )

    lines = [f"\n🔴 BEARISH (high confidence)\n{DIVIDER}"]
    for item in sorted_tickers:
        ticker = item["ticker"]
        confidence = item["score"].get("confidence", 0)
        final_score = item["score"].get("final_score", 0) or 0
        reasoning = item.get("reasoning", "")
        lines.append(f"\n{ticker} — {confidence:.0f}% 📊 {final_score:+.1f}\n{reasoning}")

    return "\n".join(lines)


def format_flips_section(flips: list[dict]) -> str:
    """
    Format the signal flips section.

    Each flip shows the direction change and confidence, followed by reasoning.

    Parameters:
        flips: List of dicts with keys: ticker, flip (dict with previous_signal,
            new_signal, new_confidence), score (dict with confidence), reasoning.

    Returns:
        Formatted flips section, or empty string if no flips.
    """
    if not flips:
        return ""

    lines = [f"\n🔄 SIGNAL FLIPS\n{DIVIDER}"]
    for item in flips:
        ticker = item["ticker"]
        flip = item.get("flip", {})
        prev = flip.get("previous_signal", "?")
        new = flip.get("new_signal", "?")
        confidence = item.get("score", {}).get("confidence", flip.get("new_confidence", 0))
        reasoning = item.get("reasoning", "")
        lines.append(f"\n{ticker}: {prev} → {new} ({confidence:.0f}%)\n{reasoning}")

    return "\n".join(lines)


def format_market_context_section(market_context: str) -> str:
    """
    Format the market context section.

    Parameters:
        market_context: Market context summary text (VIX, SPY, sector leaders, etc.).

    Returns:
        Formatted market context section.
    """
    if not market_context:
        return ""
    return f"\n📉 Market Context\n{DIVIDER}\n{market_context}"


def format_heartbeat(pipeline_stats: dict) -> str:
    """
    Format the pipeline heartbeat message.

    Shows completion status, per-phase timing, ticker counts, and signal distribution.
    Uses 'display_timezone' from pipeline_stats for local time display.

    Parameters:
        pipeline_stats: Dict with keys: fetcher_duration, calculator_duration,
            scorer_duration, tickers_processed, tickers_total, tickers_failed,
            failed_tickers, scoring_date, bullish_count, bearish_count,
            neutral_count, display_timezone.

    Returns:
        Multi-line heartbeat string.
    """
    display_timezone = pipeline_stats.get("display_timezone", "Europe/Amsterdam")
    now_utc = datetime.now(tz=timezone.utc)
    local_dt = now_utc.astimezone(ZoneInfo(display_timezone))
    tz_abbr = local_dt.strftime("%Z")
    time_str = local_dt.strftime("%H:%M")

    fetcher_dur = pipeline_stats.get("fetcher_duration") or 0
    calc_dur = pipeline_stats.get("calculator_duration") or 0
    scorer_dur = pipeline_stats.get("scorer_duration") or 0

    tickers_processed = pipeline_stats.get("tickers_processed", 0)
    tickers_total = pipeline_stats.get("tickers_total", tickers_processed)
    tickers_failed = pipeline_stats.get("tickers_failed", 0)
    bullish_count = pipeline_stats.get("bullish_count", 0)
    bearish_count = pipeline_stats.get("bearish_count", 0)
    neutral_count = pipeline_stats.get("neutral_count", 0)

    status_emoji = "⚠️" if tickers_failed > 0 else "✅"

    lines = [
        f"{status_emoji} Pipeline completed at {time_str} {tz_abbr}",
        f"Fetcher: {format_duration(fetcher_dur)} | Calculator: {format_duration(calc_dur)} | Scorer: {format_duration(scorer_dur)}",
        f"Tickers: {tickers_processed}/{tickers_total} ({tickers_failed} failed) | Signals: {bullish_count} 🟢 {bearish_count} 🔴 {neutral_count} 🟡",
    ]

    failed_tickers = pipeline_stats.get("failed_tickers", [])
    if tickers_failed > 0 and failed_tickers:
        lines.append(f"Failed: {', '.join(failed_tickers)}")

    return "\n".join(lines)


def _split_sections_into_messages(sections: list[str]) -> list[str]:
    """
    Pack a list of section strings into Telegram messages respecting the 4096-char limit.

    Splits only at section boundaries, never mid-section. If a single section
    exceeds the limit it is placed in its own message (Telegram will truncate).

    Parameters:
        sections: Ordered list of non-empty section strings.

    Returns:
        List of message strings each at most MAX_TELEGRAM_LENGTH characters.
    """
    messages: list[str] = []
    current = ""

    for section in sections:
        if not section:
            continue
        joined = current + "\n" + section if current else section
        if len(joined) <= MAX_TELEGRAM_LENGTH:
            current = joined
        else:
            if current:
                messages.append(current)
            # Section alone exceeds limit — place it as-is (rare edge case)
            current = section

    if current:
        messages.append(current)

    return messages or [""]


def format_full_report(results: dict, pipeline_stats: dict, config: dict) -> list[str]:
    """
    Assemble and return the full daily signal report as a list of Telegram messages.

    Sections are assembled in order: header, signal distribution, daily summary,
    bullish, bearish, flips, market context, heartbeat. If the assembled text
    exceeds 4096 characters it is split at section boundaries.

    Parameters:
        results: Dict from reason_all_qualifying_tickers with keys: bullish,
            bearish, flips, daily_summary, market_context_summary.
        pipeline_stats: Dict with timing and ticker counts.
        config: Notifier config dict (reads telegram.display_timezone).

    Returns:
        List of message strings, each at most 4096 characters.
    """
    display_timezone = config.get("telegram", {}).get("display_timezone", "Europe/Amsterdam")
    scoring_date = pipeline_stats.get("scoring_date", "")

    bullish = results.get("bullish", [])
    bearish = results.get("bearish", [])
    flips = results.get("flips", [])
    daily_summary = results.get("daily_summary", "")
    market_context = results.get("market_context_summary", "")

    bullish_count = pipeline_stats.get("bullish_count", len(bullish))
    bearish_count = pipeline_stats.get("bearish_count", len(bearish))
    neutral_count = pipeline_stats.get("neutral_count", 0)

    stats_with_tz = {**pipeline_stats, "display_timezone": display_timezone,
                     "bullish_count": bullish_count, "bearish_count": bearish_count,
                     "neutral_count": neutral_count}

    sections: list[str] = []

    header = format_header(scoring_date, display_timezone)
    dist = format_signal_distribution(bullish_count, bearish_count, neutral_count)
    sections.append(f"{header}\n{dist}")

    summary_section = format_daily_summary_section(daily_summary)
    if summary_section:
        sections.append(summary_section)

    bullish_section = format_bullish_section(bullish)
    if bullish_section:
        sections.append(bullish_section)

    bearish_section = format_bearish_section(bearish)
    if bearish_section:
        sections.append(bearish_section)

    flips_section = format_flips_section(flips)
    if flips_section:
        sections.append(flips_section)

    context_section = format_market_context_section(market_context)
    if context_section:
        sections.append(context_section)

    heartbeat = format_heartbeat(stats_with_tz)
    sections.append(f"\n{heartbeat}")

    full_text = "\n".join(s for s in sections if s)
    if len(full_text) <= MAX_TELEGRAM_LENGTH:
        return [full_text]

    return _split_sections_into_messages(sections)


def format_no_signals_report(market_context: str, pipeline_stats: dict, config: dict) -> list[str]:
    """
    Format a minimal report for days with no qualifying signals or flips.

    Parameters:
        market_context: Market context summary text.
        pipeline_stats: Dict with timing and ticker counts.
        config: Notifier config dict.

    Returns:
        List of message strings (usually just one).
    """
    display_timezone = config.get("telegram", {}).get("display_timezone", "Europe/Amsterdam")
    scoring_date = pipeline_stats.get("scoring_date", "")

    bullish_count = pipeline_stats.get("bullish_count", 0)
    bearish_count = pipeline_stats.get("bearish_count", 0)
    neutral_count = pipeline_stats.get("neutral_count", 0)
    stats_with_tz = {**pipeline_stats, "display_timezone": display_timezone,
                     "bullish_count": bullish_count, "bearish_count": bearish_count,
                     "neutral_count": neutral_count}

    header = format_header(scoring_date, display_timezone)
    dist = format_signal_distribution(bullish_count, bearish_count, neutral_count)
    context_section = format_market_context_section(market_context)
    heartbeat = format_heartbeat(stats_with_tz)

    parts = [
        f"{header}\n{dist}",
        "\nNo significant signals today.",
    ]
    if context_section:
        parts.append(context_section)
    parts.append(f"\n{heartbeat}")

    full_text = "\n".join(parts)
    if len(full_text) <= MAX_TELEGRAM_LENGTH:
        return [full_text]

    return _split_sections_into_messages(parts)


def format_market_closed_message(date: str, config: dict) -> str:
    """
    Format the market-closed notification message.

    Parameters:
        date: Trading date in YYYY-MM-DD format.
        config: Notifier config dict (unused but kept for interface consistency).

    Returns:
        Single-line market closed message string.
    """
    return f"📅 Market closed today ({date}) — no signals generated."


def format_pipeline_error_message(phase: str, error: str, config: dict) -> str:
    """
    Format a pipeline failure alert message.

    Parameters:
        phase: Pipeline phase where the failure occurred (e.g. "fetcher").
        error: Error message or description.
        config: Notifier config dict (unused but kept for interface consistency).

    Returns:
        Multi-line error alert message string.
    """
    return f"❌ Pipeline failed at {phase}\n{error}\nCheck logs for details."
