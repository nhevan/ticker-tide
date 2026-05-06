"""
Tests for src/notifier/bot.py — Telegram bot command handlers and message logging.

Stubs for python-telegram-bot and mplfinance are installed by the directory-level
conftest.py before this module is imported.
"""

from __future__ import annotations

import asyncio
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.events import log_telegram_message
from src.notifier.bot import (
    _extract_command,
    handle_detail_command_wrapper,
    handle_help_command,
    handle_why_callback_wrapper,
    handle_why_command_wrapper,
)


# ---------------------------------------------------------------------------
# _extract_command
# ---------------------------------------------------------------------------


def test_extract_command_returns_slash_token():
    """Command token should be extracted from the start of the message."""
    assert _extract_command("/detail AAPL 30") == "/detail"


def test_extract_command_returns_help():
    """Help command token should be extracted correctly."""
    assert _extract_command("/help") == "/help"


def test_extract_command_returns_none_for_plain_text():
    """Non-command text should return None."""
    assert _extract_command("hello world") is None


def test_extract_command_returns_none_for_empty_string():
    """Empty string should return None without raising."""
    assert _extract_command("") is None


def test_extract_command_strips_leading_whitespace():
    """Leading whitespace should be stripped before extracting the command."""
    assert _extract_command("  /detail AAPL") == "/detail"


# ---------------------------------------------------------------------------
# log_telegram_message
# ---------------------------------------------------------------------------


def test_log_telegram_message_inserts_row(db_connection: sqlite3.Connection):
    """Inserting a message should produce one row with all fields stored correctly."""
    log_telegram_message(
        db_connection,
        chat_id="111222333",
        user_id="999",
        username="trader_joe",
        command="/detail",
        message_text="/detail AAPL 30",
        received_at="2026-03-19T10:00:00+00:00",
    )

    row = db_connection.execute(
        "SELECT * FROM telegram_message_log WHERE chat_id=?", ("111222333",)
    ).fetchone()

    assert row is not None
    assert row["chat_id"] == "111222333"
    assert row["user_id"] == "999"
    assert row["username"] == "trader_joe"
    assert row["command"] == "/detail"
    assert row["message_text"] == "/detail AAPL 30"
    assert row["received_at"] == "2026-03-19T10:00:00+00:00"


def test_log_telegram_message_allows_null_user_fields(db_connection: sqlite3.Connection):
    """user_id, username, and command can be None for anonymous or unrecognised messages."""
    log_telegram_message(
        db_connection,
        chat_id="444555666",
        user_id=None,
        username=None,
        command=None,
        message_text="some text",
        received_at="2026-03-19T10:01:00+00:00",
    )

    row = db_connection.execute(
        "SELECT * FROM telegram_message_log WHERE chat_id=?", ("444555666",)
    ).fetchone()

    assert row is not None
    assert row["user_id"] is None
    assert row["username"] is None
    assert row["command"] is None


def test_log_telegram_message_accumulates_multiple_rows(db_connection: sqlite3.Connection):
    """The same user can send multiple messages and each should be stored as a separate row."""
    for index in range(3):
        log_telegram_message(
            db_connection,
            chat_id="777888999",
            user_id="42",
            username="alice",
            command="/help",
            message_text="/help",
            received_at=f"2026-03-19T10:0{index}:00+00:00",
        )

    rows = db_connection.execute(
        "SELECT * FROM telegram_message_log WHERE chat_id=?", ("777888999",)
    ).fetchall()

    assert len(rows) == 3


# ---------------------------------------------------------------------------
# handle_detail_command_wrapper — logging integration
# ---------------------------------------------------------------------------


def test_handle_detail_command_wrapper_logs_message(tmp_path):
    """handle_detail_command_wrapper should log the incoming message to the DB before handling."""
    db_path = str(tmp_path / "test.db")

    # Pre-create the DB with the required table so get_connection can use it
    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    mock_user = MagicMock()
    mock_user.id = 123
    mock_user.username = "testuser"

    mock_message = MagicMock()
    mock_message.chat_id = 987654321
    mock_message.text = "/detail AAPL"
    mock_message.from_user = mock_user
    mock_message.reply_text = AsyncMock()

    mock_update = MagicMock()
    mock_update.message = mock_message

    mock_context = MagicMock()
    mock_context.args = ["AAPL"]

    # Let get_connection use the real DB file; only mock the heavy dependencies
    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.get_active_tickers", return_value=[]),
        patch("src.notifier.bot.handle_detail_command"),
    ):
        asyncio.run(handle_detail_command_wrapper(mock_update, mock_context))

    # Open a fresh connection to verify the logged row — handler has already closed its connections
    verify_conn = sqlite3.connect(db_path)
    verify_conn.row_factory = sqlite3.Row
    row = verify_conn.execute("SELECT * FROM telegram_message_log").fetchone()
    verify_conn.close()

    assert row is not None
    assert row["chat_id"] == "987654321"
    assert row["user_id"] == "123"
    assert row["username"] == "testuser"
    assert row["command"] == "/detail"


def test_handle_detail_command_wrapper_skips_logging_on_db_error():
    """A SQLite error during logging should be caught and not abort the command handling."""
    mock_user = MagicMock()
    mock_user.id = 1
    mock_user.username = "user"

    mock_message = MagicMock()
    mock_message.chat_id = 111
    mock_message.text = "/detail AAPL"
    mock_message.from_user = mock_user
    mock_message.reply_text = AsyncMock()

    mock_update = MagicMock()
    mock_update.message = mock_message

    mock_context = MagicMock()
    mock_context.args = ["AAPL"]

    bad_conn = MagicMock()
    bad_conn.execute.side_effect = sqlite3.Error("disk full")

    with (
        patch("src.notifier.bot.os.getenv", return_value="irrelevant"),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.get_active_tickers", return_value=[]),
        patch("src.notifier.bot.handle_detail_command"),
        patch("src.notifier.bot.get_connection", return_value=bad_conn),
    ):
        # Should complete without raising even though logging fails
        asyncio.run(handle_detail_command_wrapper(mock_update, mock_context))


# ---------------------------------------------------------------------------
# handle_help_command — logging integration
# ---------------------------------------------------------------------------


def test_handle_help_command_logs_message(tmp_path):
    """handle_help_command should log the /help command to the DB before replying."""
    db_path = str(tmp_path / "test_help.db")

    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    mock_user = MagicMock()
    mock_user.id = 55
    mock_user.username = "helper"

    mock_message = MagicMock()
    mock_message.chat_id = 112233
    mock_message.text = "/help"
    mock_message.from_user = mock_user
    mock_message.reply_text = AsyncMock()

    mock_update = MagicMock()
    mock_update.message = mock_message

    mock_context = MagicMock()

    with patch("src.notifier.bot.os.getenv", return_value=db_path):
        asyncio.run(handle_help_command(mock_update, mock_context))

    verify_conn = sqlite3.connect(db_path)
    verify_conn.row_factory = sqlite3.Row
    row = verify_conn.execute("SELECT * FROM telegram_message_log").fetchone()
    verify_conn.close()

    assert row is not None
    assert row["chat_id"] == "112233"
    assert row["command"] == "/help"
    mock_message.reply_text.assert_called_once()


# ---------------------------------------------------------------------------
# handle_why_command_wrapper — logging integration
# ---------------------------------------------------------------------------


def test_handle_why_command_wrapper_logs_command(tmp_path):
    """handle_why_command_wrapper should log command='/why' to telegram_message_log."""
    db_path = str(tmp_path / "test_why.db")

    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    mock_user = MagicMock()
    mock_user.id = 77
    mock_user.username = "why_user"

    mock_message = MagicMock()
    mock_message.chat_id = 555666777
    mock_message.text = "/why AAPL"
    mock_message.from_user = mock_user
    mock_message.reply_text = AsyncMock()

    mock_update = MagicMock()
    mock_update.message = mock_message

    mock_context = MagicMock()
    mock_context.args = ["AAPL"]

    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.handle_why_command"),
    ):
        asyncio.run(handle_why_command_wrapper(mock_update, mock_context))

    verify_conn = sqlite3.connect(db_path)
    verify_conn.row_factory = sqlite3.Row
    row = verify_conn.execute("SELECT * FROM telegram_message_log").fetchone()
    verify_conn.close()

    assert row is not None
    assert row["chat_id"] == "555666777"
    assert row["user_id"] == "77"
    assert row["username"] == "why_user"
    assert row["command"] == "/why"


def test_handle_why_command_wrapper_skips_when_no_message():
    """handle_why_command_wrapper returns None immediately when update.message is None."""
    mock_update = MagicMock()
    mock_update.message = None
    mock_context = MagicMock()

    # Should not raise or call any DB/handler
    result = asyncio.run(handle_why_command_wrapper(mock_update, mock_context))
    assert result is None


def test_handle_why_command_wrapper_passes_multi_key_configs(tmp_path):
    """Wrapper must build a multi-key configs dict and pass it to handle_why_command."""
    db_path = str(tmp_path / "test_why_cfg.db")

    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    mock_user = MagicMock()
    mock_user.id = 1
    mock_user.username = "u"

    mock_message = MagicMock()
    mock_message.chat_id = 111
    mock_message.text = "/why MSFT"
    mock_message.from_user = mock_user
    mock_message.reply_text = AsyncMock()

    mock_update = MagicMock()
    mock_update.message = mock_message
    mock_context = MagicMock()
    mock_context.args = ["MSFT"]

    captured_configs = {}

    def capture_handle_why(conn, chat_id, message_text, bot_token, configs):
        captured_configs.update(configs)

    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", side_effect=lambda name: {"_loaded": name}),
        patch("src.notifier.bot.handle_why_command", side_effect=capture_handle_why),
    ):
        asyncio.run(handle_why_command_wrapper(mock_update, mock_context))

    # configs must be a dict with both "notifier" and "calculator" keys
    assert "notifier" in captured_configs
    assert "calculator" in captured_configs


# ---------------------------------------------------------------------------
# handle_why_callback_wrapper — inline button handler tests
# ---------------------------------------------------------------------------


def _make_callback_query(data: str, chat_id: int = 12345, user_id: int = 42, username: str = "alice") -> AsyncMock:
    """Build a mock CallbackQuery with the given data and user/chat attributes."""
    query = AsyncMock()
    query.data = data
    query.answer = AsyncMock()
    query.from_user = MagicMock(id=user_id, username=username)
    query.message = MagicMock(chat_id=chat_id)
    return query


def _make_callback_update(query: AsyncMock) -> MagicMock:
    """Wrap a mock CallbackQuery inside a mock Update."""
    update = MagicMock()
    update.callback_query = query
    return update


def test_handle_why_callback_wrapper_valid_ticker_calls_handler(tmp_path):
    """Valid callback_data 'why:AAPL' must call handle_why_command with '/why AAPL'."""
    db_path = str(tmp_path / "cb.db")
    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    query = _make_callback_query("why:AAPL", chat_id=12345)
    update = _make_callback_update(query)
    context = MagicMock()

    captured = {}

    def capture(conn, chat_id, message_text, bot_token, configs):
        captured["chat_id"] = chat_id
        captured["message_text"] = message_text

    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.handle_why_command", side_effect=capture),
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    assert captured["chat_id"] == "12345"
    assert captured["message_text"] == "/why AAPL"
    query.answer.assert_awaited_once()


def test_handle_why_callback_wrapper_empty_ticker_sends_error_and_no_handler():
    """callback_data='why:' (empty ticker) must send an error reply and NOT call handler."""
    query = _make_callback_query("why:")
    update = _make_callback_update(query)
    context = MagicMock()

    with (
        patch("src.notifier.bot.os.getenv", return_value=""),
        patch("src.notifier.bot.handle_why_command") as mock_handler,
        patch("src.notifier.bot.send_telegram_message") as mock_send,
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    mock_handler.assert_not_called()
    assert mock_send.call_count == 1
    sent_text = mock_send.call_args[1].get("text") or mock_send.call_args[0][2]
    assert "recognize" in sent_text or "format" in sent_text
    query.answer.assert_awaited_once()


def test_handle_why_callback_wrapper_invalid_ticker_with_spaces_sends_error():
    """callback_data with spaces in ticker (e.g. 'why:DROP TABLE') must send error, not call handler."""
    query = _make_callback_query("why:DROP TABLE")
    update = _make_callback_update(query)
    context = MagicMock()

    with (
        patch("src.notifier.bot.os.getenv", return_value=""),
        patch("src.notifier.bot.handle_why_command") as mock_handler,
        patch("src.notifier.bot.send_telegram_message") as mock_send,
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    mock_handler.assert_not_called()
    assert mock_send.call_count == 1
    query.answer.assert_awaited_once()


def test_handle_why_callback_wrapper_handler_raises_still_answers(tmp_path):
    """If sync handler raises ValueError, query.answer() must still be awaited."""
    db_path = str(tmp_path / "cb_err.db")
    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    query = _make_callback_query("why:TSLA")
    update = _make_callback_update(query)
    context = MagicMock()

    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.handle_why_command", side_effect=ValueError("boom")),
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    query.answer.assert_awaited_once()


def test_handle_why_callback_wrapper_connections_closed_on_success(tmp_path):
    """Both log_conn and conn must be closed even on the happy path."""
    db_path = str(tmp_path / "cb_close.db")
    setup_conn = sqlite3.connect(db_path)
    setup_conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"""
    )
    setup_conn.commit()
    setup_conn.close()

    query = _make_callback_query("why:MSFT")
    update = _make_callback_update(query)
    context = MagicMock()

    mock_log_conn = MagicMock()
    mock_conn = MagicMock()
    conn_call_count = [0]

    def make_conn(path):
        conn_call_count[0] += 1
        return mock_log_conn if conn_call_count[0] == 1 else mock_conn

    with (
        patch("src.notifier.bot.os.getenv", side_effect=lambda key, default="": db_path if key == "DB_PATH" else default),
        patch("src.notifier.bot.load_config", return_value={}),
        patch("src.notifier.bot.handle_why_command"),
        patch("src.notifier.bot.get_connection", side_effect=make_conn),
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    mock_log_conn.close.assert_called()
    mock_conn.close.assert_called()


def test_handle_why_callback_wrapper_reply_uses_message_chat_id_not_user_id():
    """Error reply must use query.message.chat_id, not query.from_user.id."""
    query = _make_callback_query("why:", chat_id=99999, user_id=11111)
    update = _make_callback_update(query)
    context = MagicMock()

    with (
        patch("src.notifier.bot.os.getenv", return_value=""),
        patch("src.notifier.bot.handle_why_command"),
        patch("src.notifier.bot.send_telegram_message") as mock_send,
    ):
        asyncio.run(handle_why_callback_wrapper(update, context))

    called_chat_id = mock_send.call_args[1].get("chat_id") or mock_send.call_args[0][1]
    assert called_chat_id == "99999"
    assert called_chat_id != "11111"
