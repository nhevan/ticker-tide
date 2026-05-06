"""Backend for the /why Telegram command — three modes (default, all, drill-down).

Three modes:
  default  — top-N drivers with truncation footer
  all      — ranked one-liner table for every signal in the payload
  drilldown — deep explanation of a single indicator or pattern

No Telegram wiring here — bot.py will call dispatch_why() in Sitting 2.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from typing import Optional

from src.common.progress import send_telegram_message
from src.scorer.indicator_scorer import FIXED_LADDER, PROFILE_FREE_INDICATORS
from src.scorer.pattern_scorer import PATTERN_RULE_DESCRIPTIONS

logger = logging.getLogger(__name__)

# Validates ticker tokens received via /why callback_data — mirrors the active-ticker
# shape (uppercase letters/digits + . and - for tickers like BRK.B, max 10 chars) and
# rejects empty/oversized/non-ticker callback payloads. Imported by bot.py for
# CallbackQueryHandler validation.
_WHY_TICKER_PATTERN: re.Pattern = re.compile(r'^[A-Z0-9.\-]{1,10}$')

_WHY_USAGE_HINT: str = "Usage: /why TICKER [all|<name>]\nExample: /why AAPL"

#: Returned by load_why_payload when the row exists but contains no usable data.
_NULL_DATA_SENTINEL: dict = {"error": "no_payload"}

_FALLBACK_PATTERN_RULE = "rule not available — see source"

_MAX_TELEGRAM_CHARS = 4096


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------

def load_why_payload(ticker: str, db: sqlite3.Connection) -> Optional[dict]:
    """Load and validate the key_signals_data JSON payload for the latest date.

    Parameters:
        ticker: Uppercase ticker symbol to look up.
        db: Open SQLite connection with row_factory set.

    Returns:
        Parsed payload dict if valid; _NULL_DATA_SENTINEL if data present but
        unusable; None if no row exists for this ticker.
    """
    row = db.execute(
        "SELECT key_signals_data, signal, confidence, final_score, regime, date "
        "FROM scores_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (ticker,),
    ).fetchone()

    if row is None:
        return None

    raw = row["key_signals_data"]
    if raw is None:
        return _NULL_DATA_SENTINEL

    return _parse_and_validate_payload(raw, row)


def _parse_and_validate_payload(raw: str, row: sqlite3.Row) -> dict:
    """Parse raw JSON string and validate version field.

    Parameters:
        raw: Raw JSON string from the database.
        row: The full scores_daily row for metadata enrichment.

    Returns:
        Enriched payload dict or _NULL_DATA_SENTINEL on any failure.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(f"load_why_payload: malformed JSON in key_signals_data — {exc}")
        return _NULL_DATA_SENTINEL

    if payload.get("v") != 1:
        logger.warning(
            f"load_why_payload: unsupported payload version {payload.get('v')!r} — expected 1"
        )
        return _NULL_DATA_SENTINEL

    # Enrich payload with row-level metadata for formatters
    payload.setdefault("signal", row["signal"])
    payload.setdefault("confidence", row["confidence"])
    payload.setdefault("final_score", row["final_score"])
    payload.setdefault("regime", row["regime"])
    payload.setdefault("date", row["date"])
    return payload


# ---------------------------------------------------------------------------
# Name resolution
# ---------------------------------------------------------------------------

def resolve_name_token(token: str, payload: dict) -> dict:
    """Resolve a user-supplied token to a canonical item name via substring match.

    Resolution is case-insensitive and uses substring matching.  'all' is NOT
    handled here — dispatch_why intercepts it before calling this function.

    Parameters:
        token: User-supplied string, e.g. "RSI", "ema", "macd_hist".
        payload: Validated payload dict with an 'items' list.

    Returns:
        {'match': name} for a unique hit;
        {'ambiguous': [name, ...]} for multiple hits;
        {'unknown': True} for no hits.
    """
    needle = token.lower()
    names = [item["name"] for item in payload.get("items", [])]
    matches = [n for n in names if needle in n]

    if len(matches) == 1:
        return {"match": matches[0]}
    if len(matches) > 1:
        return {"ambiguous": matches}
    return {"unknown": True}


# ---------------------------------------------------------------------------
# Default mode
# ---------------------------------------------------------------------------

def format_why_default(payload: dict, top_n: int) -> str:
    """Format the top-N signal drivers in a verbose math style.

    Parameters:
        payload: Validated payload dict.
        top_n: Maximum number of entries to show.

    Returns:
        Formatted string for Telegram (plain text, no code block).
    """
    items = payload.get("items", [])
    shown = items[:top_n]
    remaining = len(items) - len(shown)

    header = _build_default_header(payload)
    lines = [header, ""]

    for item in shown:
        lines.append(_format_default_entry(item, payload))
        lines.append("")

    if remaining > 0:
        lines.append(
            f"+{remaining} more — use `/why {payload.get('ticker', 'TICKER')} all` for full list"
        )

    return "\n".join(lines).rstrip() + "\n"


def _build_default_header(payload: dict) -> str:
    """Build the header line for default mode output."""
    ticker = payload.get("ticker", "?")
    signal = payload.get("signal", "?")
    confidence = payload.get("confidence", 0.0)
    regime = payload.get("regime", "?")
    score = payload.get("final_score", 0.0)
    date_str = payload.get("date", "?")
    return (
        f"Why {ticker} → {signal}  |  {confidence:.0f}% confidence\n"
        f"Regime: {regime}  |  Composite: {score:.1f}  |  Date: {date_str}"
    )


def _format_default_entry(item: dict, payload: dict) -> str:
    """Format one entry for default mode with the full sibling-share math.

    Patterns have ``raw_value = None`` by contract — the ``raw=`` line is
    suppressed for them so we never try to ``:.2f``-format ``None``.

    Parameters:
        item: A single signal item dict from the payload.
        payload: The full payload, used to look up sibling magnitudes
                 inside the focal item's category.

    Returns:
        Multi-line string showing raw → score → share → contribution.
    """
    name = item["name"]
    kind_tag = "[pattern]" if item["kind"] == "pattern" else "[indicator]"
    score = item["score"]
    raw = item.get("raw_value")
    if raw is None:
        head_line = f"    score: {score:+.2f}"
    else:
        head_line = f"    raw: {raw:.2f}, score: {score:+.2f}"
    return (
        f"  {name} {kind_tag}\n"
        f"{head_line}\n"
        f"{_build_math_breakdown(item, payload)}"
    )


# ---------------------------------------------------------------------------
# All mode
# ---------------------------------------------------------------------------

def format_why_all(payload: dict, max_entries: int) -> str:
    """Format a ranked one-liner table of all signal drivers.

    Parameters:
        payload: Validated payload dict.
        max_entries: Maximum rows to include before truncation.

    Returns:
        Telegram-formatted string with a monospace code block.
    """
    items = payload.get("items", [])
    total = len(items)
    shown = items[:max_entries]
    remaining = total - len(shown)

    header = _build_all_header(payload, total)
    table_lines = _build_all_table(shown)
    footer = _build_all_footer(remaining, total, payload.get("ticker", "TICKER"))

    inner = "\n".join(table_lines)
    body = f"```\n{inner}\n```"

    parts = [header, body]
    if footer:
        parts.append(footer)

    output = "\n".join(parts)
    # Safety truncate to stay within Telegram limit
    if len(output) > _MAX_TELEGRAM_CHARS:
        output = output[:_MAX_TELEGRAM_CHARS - 3] + "..."
    return output


def _build_all_header(payload: dict, total: int) -> str:
    """Build the header for all-mode output."""
    ticker = payload.get("ticker", "?")
    signal = payload.get("signal", "?")
    regime = payload.get("regime", "?")
    date_str = payload.get("date", "?")
    return f"All signals for {ticker} — {signal} | {regime} | {date_str} ({total} drivers)"


def _build_all_table(items: list[dict]) -> list[str]:
    """Build one-liner rows for the all-mode table.

    Parameters:
        items: List of signal item dicts (already sliced to max_entries).

    Returns:
        List of fixed-width row strings.
    """
    rows = ["#   Name                          Kind       Score  Contrib"]
    rows.append("-" * 58)
    for i, item in enumerate(items, start=1):
        name = item["name"][:30].ljust(30)
        kind = item["kind"][:10].ljust(10)
        score = f"{item['score']:+.1f}".rjust(6)
        contrib = f"{item['contribution']:+.2f}".rjust(7)
        rows.append(f"{i:<3} {name} {kind} {score} {contrib}")
    return rows


def _build_all_footer(remaining: int, total: int, ticker: str) -> str:
    """Build the overflow footer for all-mode, or empty string if none needed."""
    if remaining <= 0:
        return ""
    shown = total - remaining
    return f"Showing {shown} of {total} — use `/why {ticker} <name>` for drill-down"


# ---------------------------------------------------------------------------
# Drill-down mode
# ---------------------------------------------------------------------------

def format_why_drilldown(
    payload: dict, name: str, db: sqlite3.Connection, ticker: str
) -> str:
    """Format a detailed drill-down explanation for a single signal.

    Parameters:
        payload: Validated payload dict (enriched with ticker/date/regime).
        name: Canonical lowercase item name to drill into.
        db: Open SQLite connection for profile lookups.
        ticker: Uppercase ticker symbol.

    Returns:
        Formatted string with header, rule description, and math chain.
    """
    item = _find_item(payload, name)
    header = _build_drilldown_header(payload, ticker, name)
    rule_block = _build_rule_block(item, name, db, ticker)
    math_block = _build_math_chain(item, payload)
    return "\n\n".join([header, rule_block, math_block])


def _find_item(payload: dict, name: str) -> Optional[dict]:
    """Return the first item matching name, or None."""
    for item in payload.get("items", []):
        if item["name"] == name:
            return item
    return None


def _build_drilldown_header(payload: dict, ticker: str, name: str) -> str:
    """Build the top header block for drill-down mode."""
    date_str = payload.get("date", "?")
    regime = payload.get("regime", "?")
    signal = payload.get("signal", "?")
    confidence = payload.get("confidence", 0.0)
    return (
        f"Drill-down: {ticker} / {name}\n"
        f"Signal: {signal}  |  Confidence: {confidence:.0f}%\n"
        f"Regime: {regime}  |  Date: {date_str}"
    )


def _build_rule_block(
    item: Optional[dict], name: str, db: sqlite3.Connection, ticker: str
) -> str:
    """Build the rule/profile block depending on item kind.

    Parameters:
        item: The signal item dict, or None if not found in payload.
        name: Canonical item name.
        db: Open SQLite connection.
        ticker: Uppercase ticker symbol.

    Returns:
        Formatted string block describing the signal's scoring rule.
    """
    if item is None:
        return f"No data found for '{name}' in the current payload."

    if item["kind"] == "pattern":
        desc = PATTERN_RULE_DESCRIPTIONS.get(name, _FALLBACK_PATTERN_RULE)
        return f"Rule:\n  {desc}"

    # Indicator — check if profile-free or profile-driven
    if name in PROFILE_FREE_INDICATORS:
        return _format_fixed_ladder(name, item.get("raw_value"))

    return _format_profile_ladder(ticker, name, db, item.get("raw_value"))


def _format_fixed_ladder(name: str, raw_value: Optional[float]) -> str:
    """Render the FIXED_LADDER bands for a profile-free indicator."""
    bands = FIXED_LADDER.get(name, [])
    lines = ["Fixed scoring bands:"]
    for threshold, label in bands:
        lines.append(f"  < {threshold:.1f} → {label}")
    if raw_value is not None:
        lines.append(f"Current value: {raw_value:.2f}")
    return "\n".join(lines)


def _format_profile_ladder(
    ticker: str, name: str, db: sqlite3.Connection, raw_value: Optional[float]
) -> str:
    """Render the percentile ladder from indicator_profiles for a ticker/indicator.

    Parameters:
        ticker: Uppercase ticker symbol.
        name: Indicator key.
        db: Open SQLite connection.
        raw_value: The current indicator value for annotation.

    Returns:
        Formatted string with percentile bands or a graceful fallback.
    """
    try:
        row = db.execute(
            "SELECT p5, p20, p50, p80, p95, mean, std FROM indicator_profiles "
            "WHERE ticker = ? AND indicator = ?",
            (ticker, name),
        ).fetchone()
    except Exception as exc:
        logger.warning(f"_format_profile_ladder: DB error for {ticker}/{name} — {exc}")
        return "profile not built — see source"

    if row is None:
        return "Percentile profile not built for this ticker — see source data."

    lines = [
        "Percentile profile:",
        f"  p5={row['p5']:.2f}  p20={row['p20']:.2f}  p50={row['p50']:.2f}"
        f"  p80={row['p80']:.2f}  p95={row['p95']:.2f}",
        f"  mean={row['mean']:.2f}  std={row['std']:.2f}",
    ]
    if raw_value is not None:
        lines.append(f"Current value: {raw_value:.2f}")
    return "\n".join(lines)


def _build_math_chain(item: Optional[dict], payload: dict) -> str:
    """Render the full sibling-share math chain for a focal item.

    Parameters:
        item: The signal item dict or None.
        payload: The full payload, used to look up sibling magnitudes
                 inside the focal item's category.

    Returns:
        Formatted multi-line string, or empty string when no item was found.
    """
    if item is None:
        return ""
    return "Math chain:\n" + _build_math_breakdown(item, payload)


def _build_math_breakdown(item: dict, payload: dict) -> str:
    """Walk through score → category share → regime weight → contribution.

    Mirrors the formula in ``src.scorer.contribution`` exactly so that the
    displayed math reconciles with the persisted ``contribution``:

        contribution = (score × |score| ÷ Σ|sibling scores|)
                        × regime_weight × expansion_factor

    The expansion factor lives at the payload root and is shown explicitly
    in the math line so users can reproduce the number with a calculator.

    Parameters:
        item: The focal item.
        payload: The full payload used to find category siblings AND read
                 the top-level ``expansion_factor`` field.

    Returns:
        Multi-line indented block.
    """
    cat = item["category"]
    score = item["score"]
    contrib = item["contribution"]
    cat_w = item["category_weight"]
    expansion = payload.get("expansion_factor", 1.0)
    siblings = [it for it in payload.get("items", []) if it.get("category") == cat]
    sibling_strs = ", ".join(f"{s['name']} {s['score']:+.2f}" for s in siblings)
    total_mag = sum(abs(s["score"]) for s in siblings)
    if total_mag == 0:
        return (
            f"    {cat.capitalize()} siblings: {sibling_strs}\n"
            f"    Total magnitude: 0.00 — contribution forced to 0\n"
            f"    Contribution: {contrib:+.3f}"
        )
    share = (score * abs(score)) / total_mag
    return (
        f"    {cat.capitalize()} siblings: {sibling_strs}\n"
        f"    Total magnitude: {total_mag:.2f}\n"
        f"    Share: {score:+.2f} × |{score:.2f}| ÷ {total_mag:.2f} = {share:+.3f}\n"
        f"    Regime weight ({cat}): {cat_w:.2f}\n"
        f"    Expansion factor: {expansion:.2f}\n"
        f"    Contribution: {share:+.3f} × {cat_w:.2f} × {expansion:.2f} = {contrib:+.3f}"
    )


# ---------------------------------------------------------------------------
# Synchronous bot handler
# ---------------------------------------------------------------------------

def handle_why_command(
    db_conn: sqlite3.Connection,
    chat_id: str,
    message_text: str,
    bot_token: str,
    configs: dict,
) -> None:
    """Handle a /why Telegram command synchronously and send the response.

    Strips the leading /why or /why@<botname> prefix, tokenizes the remainder,
    and delegates to dispatch_why for the appropriate formatter. The response
    is sent via send_telegram_message without parse_mode (formatters emit plain
    text only).

    Parameters:
        db_conn: Open SQLite connection for payload and profile lookups.
        chat_id: Telegram chat ID to reply to.
        message_text: Full raw message text, e.g. "/why AAPL all".
        bot_token: Telegram Bot API token.
        configs: Multi-key configs dict; dispatch_why reads
                 configs['notifier']['why_top_n'] and
                 configs['notifier']['why_list_max_entries'].

    Returns:
        None
    """
    remainder = re.sub(r'^/why(@\w+)?\s*', '', message_text).strip()
    tokens = remainder.split() if remainder else []

    if not tokens:
        send_telegram_message(bot_token, chat_id, _WHY_USAGE_HINT)
        return

    ticker = tokens[0].upper()
    args = tokens[1:]

    logger.info("phase=bot command=/why ticker=%s args=%s", ticker, args)
    response = dispatch_why(ticker, args, db_conn, configs)
    send_telegram_message(bot_token, chat_id, response)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def dispatch_why(
    ticker: str, args: list[str], db: sqlite3.Connection, configs: dict
) -> str:
    """Route a /why command to the appropriate formatter.

    Parameters:
        ticker: Uppercase ticker symbol.
        args: Remaining arguments after the ticker (may be empty).
        db: Open SQLite connection.
        configs: Full configs dict; reads configs['notifier']['why_top_n'] and
                 configs['notifier']['why_list_max_entries'].

    Returns:
        Formatted string ready to send to Telegram.
    """
    if len(args) > 1:
        return f"Usage: /why {ticker} [all | <signal_name>]"

    payload = load_why_payload(ticker, db)
    if payload is None:
        return f"No signal data found for {ticker}."
    if payload == _NULL_DATA_SENTINEL:
        return f"Signal data for {ticker} is unavailable or malformed."

    notifier_cfg = configs.get("notifier", {})

    if len(args) == 0:
        top_n = int(notifier_cfg.get("why_top_n", 5))
        payload.setdefault("ticker", ticker)
        return format_why_default(payload, top_n=top_n)

    keyword = args[0].lower()

    if keyword == "all":
        max_entries = int(notifier_cfg.get("why_list_max_entries", 50))
        payload.setdefault("ticker", ticker)
        return format_why_all(payload, max_entries=max_entries)

    return _dispatch_drilldown(ticker, args[0], payload, db)


def _dispatch_drilldown(
    ticker: str, raw_token: str, payload: dict, db: sqlite3.Connection
) -> str:
    """Resolve the token and call format_why_drilldown or return an error.

    Parameters:
        ticker: Uppercase ticker symbol.
        raw_token: The user-supplied token string (pre-lowercasing by resolve_name_token).
        payload: Validated payload dict.
        db: Open SQLite connection.

    Returns:
        Formatted drill-down string or an error/disambiguation message.
    """
    payload.setdefault("ticker", ticker)
    resolution = resolve_name_token(raw_token, payload)

    if "match" in resolution:
        return format_why_drilldown(payload, resolution["match"], db, ticker)

    if "ambiguous" in resolution:
        matches = ", ".join(resolution["ambiguous"])
        return f"Ambiguous — '{raw_token}' matches multiple signals: {matches}\nBe more specific."

    return f"No signal named '{raw_token}' found for {ticker} — check `/why {ticker} all` for the full list."
