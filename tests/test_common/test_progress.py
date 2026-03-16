"""
Tests for src/common/progress.py — ProgressTracker and Telegram helpers.
"""

import pytest

from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

TICKERS = ["AAPL", "MSFT", "NVDA"]


# ---------------------------------------------------------------------------
# ProgressTracker — initialisation
# ---------------------------------------------------------------------------


def test_progress_tracker_init():
    """ProgressTracker should initialise with correct attributes."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    assert tracker.phase == "backfill_ohlcv"
    assert tracker.tickers == TICKERS
    assert len(tracker.tickers) == 3
    assert tracker.completed == 0
    assert tracker.skipped == 0
    assert tracker.failed == 0


# ---------------------------------------------------------------------------
# ProgressTracker — format_progress_message initial state
# ---------------------------------------------------------------------------


def test_progress_tracker_format_message_initial():
    """Initial message should show 0/3 and phase name, all tickers pending."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    msg = tracker.format_progress_message()
    assert "backfill_ohlcv" in msg
    assert "0/3" in msg
    for ticker in TICKERS:
        assert ticker in msg


# ---------------------------------------------------------------------------
# ProgressTracker — mark_* methods
# ---------------------------------------------------------------------------


def test_progress_tracker_mark_completed():
    """mark_completed increments completed and shows checkmark + details."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    tracker.mark_completed("AAPL", details="1,260 days")
    assert tracker.completed == 1
    msg = tracker.format_progress_message()
    assert "AAPL" in msg
    assert "✅" in msg
    assert "1,260 days" in msg


def test_progress_tracker_mark_skipped():
    """mark_skipped increments skipped and shows skip indicator + reason."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    tracker.mark_skipped("INTC", reason="API error")
    assert tracker.skipped == 1
    msg = tracker.format_progress_message()
    assert "INTC" in msg
    assert "⚠️" in msg


def test_progress_tracker_mark_failed():
    """mark_failed increments failed and shows failure indicator + reason."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    tracker.mark_failed("GE", reason="timeout")
    assert tracker.failed == 1
    msg = tracker.format_progress_message()
    assert "GE" in msg
    assert "❌" in msg


def test_progress_tracker_mark_processing():
    """mark_processing shows a processing indicator for the ticker."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    tracker.mark_processing("TSLA")
    msg = tracker.format_progress_message()
    assert "TSLA" in msg
    assert "⏳" in msg


# ---------------------------------------------------------------------------
# ProgressTracker — counts in header
# ---------------------------------------------------------------------------


def test_progress_tracker_format_shows_correct_counts():
    """Header should show correct counts after marking multiple tickers."""
    tickers = ["AAPL", "MSFT", "NVDA", "INTC"]
    tracker = ProgressTracker(phase="test_phase", tickers=tickers)
    tracker.mark_completed("AAPL")
    tracker.mark_completed("MSFT")
    tracker.mark_skipped("NVDA", reason="no data")
    tracker.mark_failed("INTC", reason="error")

    msg = tracker.format_progress_message()
    assert "2/4" in msg
    assert "✅" in msg
    assert "skipped" in msg
    assert "failed" in msg


# ---------------------------------------------------------------------------
# ProgressTracker — pending tickers shown
# ---------------------------------------------------------------------------


def test_progress_tracker_pending_tickers_shown():
    """Pending tickers should appear in message with pending indicator."""
    five_tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]
    tracker = ProgressTracker(phase="test_phase", tickers=five_tickers)
    tracker.mark_completed("AAPL")
    tracker.mark_completed("MSFT")

    msg = tracker.format_progress_message()
    # Remaining 3 should still appear
    for ticker in ["NVDA", "TSLA", "AMZN"]:
        assert ticker in msg


# ---------------------------------------------------------------------------
# ProgressTracker — final summary
# ---------------------------------------------------------------------------


def test_progress_tracker_format_final_summary():
    """Final summary should contain processed counts and formatted duration."""
    tracker = ProgressTracker(phase="backfill_ohlcv", tickers=TICKERS)
    tracker.mark_completed("AAPL")
    tracker.mark_completed("MSFT")
    tracker.mark_skipped("NVDA", reason="no data")

    summary = tracker.format_final_summary(duration_seconds=192.5)
    assert "backfill_ohlcv" in summary
    assert "2" in summary          # processed count
    assert "skipped" in summary
    # Duration formatted as Xm Ys
    assert "m" in summary or "h" in summary or "s" in summary


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------


def test_telegram_send_message(mocker):
    """send_telegram_message should POST to the correct URL with correct payload."""
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = {"ok": True, "result": {"message_id": 42}}
    mock_response.raise_for_status = mocker.MagicMock()
    mock_post = mocker.patch("httpx.post", return_value=mock_response)

    result = send_telegram_message("mytoken", "chat123", "hello")

    mock_post.assert_called_once()
    call_args = mock_post.call_args
    # url is the first positional arg; json payload is a keyword arg
    url_called = call_args.args[0]
    assert "mytoken" in url_called
    assert "sendMessage" in url_called

    payload = call_args.kwargs.get("json", {})
    assert payload.get("chat_id") == "chat123"
    assert payload.get("text") == "hello"
    assert result == 42


def test_telegram_edit_message(mocker):
    """edit_telegram_message should POST to editMessageText with correct payload."""
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = {"ok": True, "result": {"message_id": 12345}}
    mock_response.raise_for_status = mocker.MagicMock()
    mock_post = mocker.patch("httpx.post", return_value=mock_response)

    result = edit_telegram_message("mytoken", "chat123", message_id=12345, new_text="updated")

    mock_post.assert_called_once()
    call_args = mock_post.call_args
    url_called = call_args.args[0]
    assert "mytoken" in url_called
    assert "editMessageText" in url_called

    payload = call_args.kwargs.get("json", {})
    assert payload.get("chat_id") == "chat123"
    assert payload.get("message_id") == 12345
    assert payload.get("text") == "updated"
    assert result is True


def test_telegram_send_message_error_handling(mocker):
    """send_telegram_message should return None on exception, not crash."""
    mocker.patch("httpx.post", side_effect=Exception("connection error"))
    result = send_telegram_message("mytoken", "chat123", "hello")
    assert result is None


def test_telegram_edit_message_error_handling(mocker):
    """edit_telegram_message should return False on exception, not crash."""
    mocker.patch("httpx.post", side_effect=Exception("connection error"))
    result = edit_telegram_message("mytoken", "chat123", message_id=1, new_text="hi")
    assert result is False


def test_telegram_message_too_long_is_truncated(mocker):
    """Messages longer than 4096 chars should be truncated before sending."""
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = {"ok": True, "result": {"message_id": 1}}
    mock_response.raise_for_status = mocker.MagicMock()
    mock_post = mocker.patch("httpx.post", return_value=mock_response)

    long_text = "x" * 5000
    send_telegram_message("mytoken", "chat123", long_text)

    call_args = mock_post.call_args
    payload = call_args.kwargs.get("json", {})
    sent_text = payload.get("text", "")
    assert len(sent_text) <= 4096
    assert sent_text.endswith("...")
