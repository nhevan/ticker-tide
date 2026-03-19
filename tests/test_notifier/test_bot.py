"""
Tests for src/notifier/bot.py — Telegram bot command handlers and message logging.

The python-telegram-bot library and other optional packages are stubbed out at
module level so bot.py can be imported without the real libraries installed.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Stub out optional packages before any src.notifier.bot import
# ---------------------------------------------------------------------------

def _stub_if_missing(module_name: str, attrs: dict) -> None:
    """Register a stub module under *module_name* unless it is already present."""
    if module_name in sys.modules:
        return
    stub = ModuleType(module_name)
    for attr_name, attr_value in attrs.items():
        setattr(stub, attr_name, attr_value)
    sys.modules[module_name] = stub


def _install_stubs() -> None:
    """
    Install minimal stubs for optional packages so bot.py and its transitive
    dependencies can be imported without the real libraries installed.
    """
    _stub_if_missing("telegram", {"Update": MagicMock()})
    _stub_if_missing("telegram.ext", {
        "Application": MagicMock(),
        "CommandHandler": MagicMock(),
        "ContextTypes": MagicMock(),
    })
    _stub_if_missing("mplfinance", {"make_addplot": MagicMock(), "plot": MagicMock()})
    _stub_if_missing("anthropic", {})
    _stub_if_missing("finnhub", {})
    _stub_if_missing("yfinance", {})


_install_stubs()

from src.common.events import log_telegram_message  # noqa: E402
from src.notifier.bot import _extract_command, handle_detail_command_wrapper, handle_help_command  # noqa: E402


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
