"""
Progress tracking with Telegram notifications for the Stock Signal Engine.

Provides two things:
1. ProgressTracker — tracks per-ticker progress during backfill/pipeline runs
   and formats progress messages for display.
2. Telegram helper functions — lightweight send/edit message functions using
   the Telegram Bot API directly via httpx (NOT the python-telegram-bot library).
   The full python-telegram-bot library is used in src/notifier/ for the daily
   AI-powered reports. This module is intentionally simple and dependency-light.

Usage during backfill:
  tracker = ProgressTracker(phase="backfill_ohlcv", tickers=["AAPL", "MSFT", ...])
  msg_id = send_telegram_message(token, chat_id, tracker.format_progress_message())
  for ticker in tickers:
      tracker.mark_processing(ticker)
      edit_telegram_message(token, chat_id, msg_id, tracker.format_progress_message())
      ... do work ...
      tracker.mark_completed(ticker, details="1,260 days")
      edit_telegram_message(token, chat_id, msg_id, tracker.format_progress_message())
  send_telegram_message(token, chat_id, tracker.format_final_summary(duration))
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

_TELEGRAM_MAX_LENGTH = 4096
_STATUS_PENDING = "pending"
_STATUS_PROCESSING = "processing"
_STATUS_COMPLETED = "completed"
_STATUS_SKIPPED = "skipped"
_STATUS_FAILED = "failed"

_STATUS_EMOJI = {
    _STATUS_COMPLETED: "✅",
    _STATUS_PROCESSING: "⏳",
    _STATUS_SKIPPED: "⚠️",
    _STATUS_FAILED: "❌",
    _STATUS_PENDING: "⬜",
}


class ProgressTracker:
    """
    Track per-ticker progress during a pipeline phase and format Telegram messages.

    Maintains a status and optional detail string for each ticker, plus running
    counters for completed, skipped, and failed tickers.
    """

    def __init__(self, phase: str, tickers: list[str]) -> None:
        """
        Initialise the tracker.

        Args:
            phase: Name of the pipeline phase (e.g. "backfill_ohlcv").
            tickers: Ordered list of ticker symbols to track.
        """
        self.phase = phase
        self.tickers = list(tickers)
        self.status: dict[str, str] = {ticker: _STATUS_PENDING for ticker in tickers}
        self.details: dict[str, str] = {ticker: "" for ticker in tickers}
        self.completed = 0
        self.skipped = 0
        self.failed = 0
        self.start_time = datetime.now(tz=timezone.utc)

    def mark_processing(self, ticker: str) -> None:
        """
        Mark a ticker as currently being processed.

        Args:
            ticker: The ticker symbol.
        """
        self.status[ticker] = _STATUS_PROCESSING

    def mark_completed(self, ticker: str, details: str = "") -> None:
        """
        Mark a ticker as completed and increment the completed counter.

        Args:
            ticker: The ticker symbol.
            details: Optional detail string (e.g. "1,260 days").
        """
        self.status[ticker] = _STATUS_COMPLETED
        self.details[ticker] = details
        self.completed += 1

    def mark_skipped(self, ticker: str, reason: str = "") -> None:
        """
        Mark a ticker as skipped and increment the skipped counter.

        Args:
            ticker: The ticker symbol.
            reason: Human-readable reason for skipping.
        """
        self.status[ticker] = _STATUS_SKIPPED
        self.details[ticker] = reason
        self.skipped += 1

    def mark_failed(self, ticker: str, reason: str = "") -> None:
        """
        Mark a ticker as failed and increment the failed counter.

        Args:
            ticker: The ticker symbol.
            reason: Human-readable reason for the failure.
        """
        self.status[ticker] = _STATUS_FAILED
        self.details[ticker] = reason
        self.failed += 1

    def _all_tracked_tickers(self) -> list[str]:
        """Return original tickers plus any dynamically added ones (in order)."""
        all_tickers = list(self.tickers)
        for ticker in self.status:
            if ticker not in all_tickers:
                all_tickers.append(ticker)
        return all_tickers

    def format_progress_message(self) -> str:
        """
        Build a Telegram-friendly progress message showing the current state.

        Line 1: "📊 {phase} — {completed}/{total} ✅"
        Line 2: (if skipped > 0) "⚠️ {skipped} skipped"
        Line 3: (if failed > 0) "❌ {failed} failed"
        Blank line, then one line per ticker showing its status, emoji, and details.

        If the full ticker list would exceed 4096 chars, shows first 20 completed
        tickers + all active/skipped/failed tickers + a count of remaining pending.

        Returns:
            A formatted string suitable for a Telegram message.
        """
        total = len(self.tickers)
        header_lines = [f"📊 {self.phase} — {self.completed}/{total} ✅"]
        if self.skipped > 0:
            header_lines.append(f"⚠️ {self.skipped} skipped")
        if self.failed > 0:
            header_lines.append(f"❌ {self.failed} failed")
        header_lines.append("")

        all_tickers = self._all_tracked_tickers()

        def _build_ticker_lines(tickers: list[str]) -> list[str]:
            lines = []
            for ticker in tickers:
                ticker_status = self.status[ticker]
                detail = self.details.get(ticker, "")
                emoji = _STATUS_EMOJI[ticker_status]
                line = f"{emoji} {ticker}"
                if detail:
                    line += f" — {detail}"
                lines.append(line)
            return lines

        ticker_lines = _build_ticker_lines(all_tickers)
        message = "\n".join(header_lines + ticker_lines)

        if len(message) <= _TELEGRAM_MAX_LENGTH:
            return message

        # Message too long — condense: show first 20 completed + non-pending + pending count
        condensed_lines = list(header_lines)
        completed_shown = 0
        pending_count = 0
        _MAX_COMPLETED_SHOWN = 20

        for ticker in all_tickers:
            ticker_status = self.status[ticker]
            detail = self.details.get(ticker, "")

            if ticker_status == _STATUS_PENDING:
                pending_count += 1
            elif ticker_status == _STATUS_COMPLETED:
                if completed_shown < _MAX_COMPLETED_SHOWN:
                    line = f"✅ {ticker}"
                    if detail:
                        line += f" — {detail}"
                    condensed_lines.append(line)
                    completed_shown += 1
            else:
                emoji = _STATUS_EMOJI[ticker_status]
                line = f"{emoji} {ticker}"
                if detail:
                    line += f" — {detail}"
                condensed_lines.append(line)

        if pending_count > 0:
            condensed_lines.append(f"⬜ ... and {pending_count} more pending")

        message = "\n".join(condensed_lines)
        if len(message) > _TELEGRAM_MAX_LENGTH:
            message = message[: _TELEGRAM_MAX_LENGTH - 3] + "..."

        return message

    def format_final_summary(
        self, duration_seconds: float, extra_stats: dict = None
    ) -> str:
        """
        Build a final summary message to be sent as a new Telegram message.

        Includes phase name, formatted duration, processed/skipped/failed counts,
        and optional extra statistics.

        Args:
            duration_seconds: Total elapsed time in seconds.
            extra_stats: Optional dict of additional key-value pairs to display
                         (e.g. {"OHLCV rows": "63,000"}).

        Returns:
            A formatted summary string.
        """
        total = len(self.tickers)
        duration_str = _format_duration(duration_seconds)

        lines = [
            f"✅ {self.phase} Complete!",
            f"Duration: {duration_str}",
            f"Processed: {self.completed}/{total}",
        ]
        if self.skipped > 0:
            lines.append(f"skipped: {self.skipped}")
        if self.failed > 0:
            lines.append(f"failed: {self.failed}")
        if extra_stats:
            for key, value in extra_stats.items():
                lines.append(f"{key}: {value}")

        return "\n".join(lines)


def _format_duration(duration_seconds: float) -> str:
    """
    Format a duration in seconds as a human-readable string.

    Args:
        duration_seconds: Elapsed time in seconds.

    Returns:
        A string like "3m 12s" or "1h 5m 30s".
    """
    total_seconds = int(duration_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    parse_mode: str = None,
) -> int | None:
    """
    Send a Telegram message via the Bot API and return the message_id.

    Truncates text to 4096 characters if necessary. Never raises — returns None
    on any error so pipeline failures are not caused by notification issues.

    Args:
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.
        text: Message text to send.
        parse_mode: Optional parse mode ("Markdown" or "HTML").

    Returns:
        The message_id integer from the API response, or None on failure.
    """
    if len(text) > _TELEGRAM_MAX_LENGTH:
        text = text[: _TELEGRAM_MAX_LENGTH - 3] + "..."

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload: dict = {"chat_id": chat_id, "text": text}
    if parse_mode is not None:
        payload["parse_mode"] = parse_mode

    try:
        response = httpx.post(url, json=payload, timeout=10.0)
        response.raise_for_status()
        data = response.json()
        return data["result"]["message_id"]
    except Exception as exc:
        logger.warning(f"send_telegram_message failed: {exc}")
        return None


def edit_telegram_message(
    bot_token: str,
    chat_id: str,
    message_id: int,
    new_text: str,
    parse_mode: str = None,
) -> bool:
    """
    Edit an existing Telegram message via the Bot API.

    Truncates new_text to 4096 characters if necessary. Never raises — returns
    False on any error (except "message is not modified" which returns True).

    Args:
        bot_token: Telegram Bot API token.
        chat_id: Target chat or channel ID.
        message_id: ID of the message to edit.
        new_text: Replacement message text.
        parse_mode: Optional parse mode ("Markdown" or "HTML").

    Returns:
        True on success (including "message is not modified"), False on failure.
    """
    if len(new_text) > _TELEGRAM_MAX_LENGTH:
        new_text = new_text[: _TELEGRAM_MAX_LENGTH - 3] + "..."

    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload: dict = {"chat_id": chat_id, "message_id": message_id, "text": new_text}
    if parse_mode is not None:
        payload["parse_mode"] = parse_mode

    try:
        response = httpx.post(url, json=payload, timeout=10.0)
        data = response.json()
        # Telegram returns HTTP 400 with this description when content is unchanged
        if not data.get("ok") and "message is not modified" in data.get("description", ""):
            return True
        response.raise_for_status()
        return True
    except Exception as exc:
        logger.warning(f"edit_telegram_message failed: {exc}")
        return False
