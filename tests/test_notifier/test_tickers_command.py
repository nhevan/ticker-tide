"""
Tests for src/notifier/tickers_command.py — /tickers Telegram bot command.

Covers format_tickers_message (sector grouping, inactive filtering, empty list)
and handle_tickers_command (async handler sends reply).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.notifier.tickers_command import format_tickers_message, handle_tickers_command

SAMPLE_TICKERS = [
    {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": True},
    {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": True},
    {"symbol": "BAC", "sector": "Financials", "sector_etf": "XLF", "active": True},
    {"symbol": "GOOGL", "sector": "Communication Services", "sector_etf": "XLC", "active": True},
    {"symbol": "OLD", "sector": "Technology", "sector_etf": "XLK", "active": False},
]


# ---------------------------------------------------------------------------
# format_tickers_message
# ---------------------------------------------------------------------------


def test_format_tickers_message_groups_by_sector() -> None:
    """Active tickers should be grouped under their sector heading."""
    result = format_tickers_message(SAMPLE_TICKERS)

    assert "Technology" in result
    assert "Financials" in result
    assert "Communication Services" in result

    assert "AAPL" in result
    assert "MSFT" in result
    assert "JPM" in result
    assert "BAC" in result
    assert "GOOGL" in result


def test_format_tickers_message_filters_inactive() -> None:
    """Inactive tickers must not appear in the output."""
    result = format_tickers_message(SAMPLE_TICKERS)
    assert "OLD" not in result


def test_format_tickers_message_shows_counts() -> None:
    """Header should show total active ticker count and sector count."""
    result = format_tickers_message(SAMPLE_TICKERS)

    assert "5" in result   # 5 active tickers
    assert "3" in result   # 3 sectors


def test_format_tickers_message_sectors_sorted_alphabetically() -> None:
    """Sectors should appear in alphabetical order."""
    result = format_tickers_message(SAMPLE_TICKERS)

    comm_pos = result.index("Communication Services")
    fin_pos = result.index("Financials")
    tech_pos = result.index("Technology")

    assert comm_pos < fin_pos < tech_pos


def test_format_tickers_message_symbols_sorted_within_sector() -> None:
    """Symbols within each sector should be sorted alphabetically."""
    result = format_tickers_message(SAMPLE_TICKERS)

    # Technology section ends at the next sector boundary (nothing after Technology here)
    tech_section_start = result.index("Technology")
    tech_section = result[tech_section_start:]
    aapl_pos = tech_section.index("AAPL")
    msft_pos = tech_section.index("MSFT")
    assert aapl_pos < msft_pos


def test_format_tickers_message_empty_list() -> None:
    """Empty ticker list should return a graceful no-tickers message."""
    result = format_tickers_message([])
    assert result
    assert "0" in result or "no" in result.lower()


def test_format_tickers_message_all_inactive() -> None:
    """List with all inactive tickers should behave like empty list."""
    tickers = [
        {"symbol": "DEAD", "sector": "Technology", "sector_etf": "XLK", "active": False},
    ]
    result = format_tickers_message(tickers)
    assert "DEAD" not in result


# ---------------------------------------------------------------------------
# handle_tickers_command
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_tickers_command_sends_reply() -> None:
    """Handler should reply with the formatted tickers message."""
    mock_update = MagicMock()
    mock_update.message = MagicMock()
    mock_update.message.reply_text = AsyncMock()

    mock_context = MagicMock()

    with patch(
        "src.notifier.tickers_command.get_active_tickers",
        return_value=SAMPLE_TICKERS,
    ):
        await handle_tickers_command(mock_update, mock_context)

    mock_update.message.reply_text.assert_called_once()
    sent_text = mock_update.message.reply_text.call_args[0][0]
    assert "Technology" in sent_text
    assert "AAPL" in sent_text


@pytest.mark.asyncio
async def test_handle_tickers_command_no_message() -> None:
    """Handler should exit gracefully when update.message is None."""
    mock_update = MagicMock()
    mock_update.message = None
    mock_context = MagicMock()

    with patch("src.notifier.tickers_command.get_active_tickers", return_value=SAMPLE_TICKERS):
        await handle_tickers_command(mock_update, mock_context)
    # No exception raised — test passes
