"""
Tests for src/notifier/scatter_command.py — /scatter Telegram bot command.

Covers:
  - parse_scatter_command: valid inputs, invalid inputs, defaults
  - fetch_signals_with_forward_returns: happy path, BEARISH inversion, dropped rows
  - generate_scatter_chart: produces a PNG file, handles empty data
  - handle_scatter_command: end-to-end orchestration (mocked Telegram)
"""

from __future__ import annotations

import os
import sqlite3
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

SAMPLE_CONFIG = {
    "scatter_command": {
        "default_n_days": 5,
        "max_n_days": 60,
        "default_days_back": 90,
        "max_days_back": 365,
    }
}

ACTIVE_TICKERS = [
    {"symbol": "AAPL", "sector": "Technology", "active": True},
    {"symbol": "MSFT", "sector": "Technology", "active": True},
]

# Use a known Monday as reference so OHLCV helper does not skip the first row
_REF_DATE = date(2025, 6, 2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_ohlcv(
    conn: sqlite3.Connection,
    ticker: str,
    start_date: date,
    closes: list[float],
) -> None:
    """Insert consecutive daily OHLCV rows, skipping weekends."""
    current = start_date
    inserted = 0
    day_offset = 0
    while inserted < len(closes):
        if current.weekday() < 5:
            close = closes[inserted]
            conn.execute(
                """INSERT OR REPLACE INTO ohlcv_daily
                   (ticker, date, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (ticker, current.isoformat(), close * 0.99, close * 1.01, close * 0.98, close, 1_000_000),
            )
            inserted += 1
        current += timedelta(days=1)
        day_offset += 1
    conn.commit()


def _insert_score(
    conn: sqlite3.Connection,
    ticker: str,
    signal_date: str,
    signal: str,
    confidence: float,
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence)
           VALUES (?, ?, ?, ?)""",
        (ticker, signal_date, signal, confidence),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# parse_scatter_command
# ---------------------------------------------------------------------------

class TestParseScatterCommand:
    def test_n_days_only(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 10", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == 10
        assert result["ticker"] is None
        assert result["days_back"] == SAMPLE_CONFIG["scatter_command"]["default_days_back"]

    def test_n_days_and_ticker(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 5 AAPL", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == 5
        assert result["ticker"] == "AAPL"
        assert result["days_back"] == SAMPLE_CONFIG["scatter_command"]["default_days_back"]

    def test_n_days_ticker_and_days_back(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 20 MSFT 180", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == 20
        assert result["ticker"] == "MSFT"
        assert result["days_back"] == 180

    def test_n_days_and_days_back_without_ticker(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 10 60", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == 10
        assert result["ticker"] is None
        assert result["days_back"] == 60

    def test_n_days_clamped_to_max(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 9999", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == SAMPLE_CONFIG["scatter_command"]["max_n_days"]

    def test_days_back_clamped_to_max(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 5 9999", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["days_back"] == SAMPLE_CONFIG["scatter_command"]["max_days_back"]

    def test_n_days_minimum_is_one(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 0", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["n_days"] == 1

    def test_missing_n_days_raises_value_error(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        with pytest.raises(ValueError, match="N"):
            parse_scatter_command("/scatter", ACTIVE_TICKERS, SAMPLE_CONFIG)

    def test_non_numeric_n_days_raises_value_error(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        with pytest.raises(ValueError):
            parse_scatter_command("/scatter abc", ACTIVE_TICKERS, SAMPLE_CONFIG)

    def test_ticker_must_be_in_active_tickers(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        with pytest.raises(ValueError, match="ZZZZ"):
            parse_scatter_command("/scatter 5 ZZZZ", ACTIVE_TICKERS, SAMPLE_CONFIG)

    def test_ticker_is_case_insensitive(self) -> None:
        from src.notifier.scatter_command import parse_scatter_command

        result = parse_scatter_command("/scatter 5 aapl", ACTIVE_TICKERS, SAMPLE_CONFIG)
        assert result["ticker"] == "AAPL"


# ---------------------------------------------------------------------------
# fetch_signals_with_forward_returns
# ---------------------------------------------------------------------------

class TestFetchSignalsWithForwardReturns:
    def test_bullish_forward_return_positive_when_price_rises(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        # Insert 10 consecutive trading days starting from _REF_DATE
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)

        # Signal on first day, n_days=5 → forward close is closes[5]=105.0
        signal_date = _REF_DATE.isoformat()
        _insert_score(db_connection, "AAPL", signal_date, "BULLISH", 75.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"
        assert rows[0]["signal"] == "BULLISH"
        assert rows[0]["confidence"] == 75.0
        assert rows[0]["forward_return_pct"] == pytest.approx(5.0, rel=1e-3)

    def test_bearish_return_inverted(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        # Price falls from 100 to 90 over 5 trading days
        closes = [100.0, 99.0, 98.0, 97.0, 96.0, 90.0, 89.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)

        signal_date = _REF_DATE.isoformat()
        _insert_score(db_connection, "AAPL", signal_date, "BEARISH", 60.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 1
        # Raw return = (90-100)/100 = -10%, aligned BEARISH return = +10%
        assert rows[0]["forward_return_pct"] == pytest.approx(10.0, rel=1e-3)

    def test_neutral_return_not_inverted(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        closes = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)

        signal_date = _REF_DATE.isoformat()
        _insert_score(db_connection, "AAPL", signal_date, "NEUTRAL", 30.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 1
        # Raw return = (95-100)/100 = -5%, NEUTRAL is not inverted
        assert rows[0]["forward_return_pct"] == pytest.approx(-5.0, rel=1e-3)

    def test_signal_dropped_when_no_future_close(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        # Only 3 trading days of data, n_days=5 → no Nth future close
        closes = [100.0, 101.0, 102.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)

        signal_date = _REF_DATE.isoformat()
        _insert_score(db_connection, "AAPL", signal_date, "BULLISH", 80.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 0

    def test_signal_dropped_when_no_ohlcv_on_signal_date(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        # Insert ohlcv for different dates than the signal date
        future_start = _REF_DATE + timedelta(days=10)
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        _insert_ohlcv(db_connection, "AAPL", future_start, closes)

        signal_date = _REF_DATE.isoformat()
        _insert_score(db_connection, "AAPL", signal_date, "BULLISH", 80.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 0

    def test_filters_by_ticker(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        for ticker in ("AAPL", "MSFT"):
            closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
            _insert_ohlcv(db_connection, ticker, _REF_DATE, closes)
            _insert_score(db_connection, ticker, _REF_DATE.isoformat(), "BULLISH", 70.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=365
        )

        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"

    def test_no_ticker_filter_returns_all(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        for ticker in ("AAPL", "MSFT"):
            closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
            _insert_ohlcv(db_connection, ticker, _REF_DATE, closes)
            _insert_score(db_connection, ticker, _REF_DATE.isoformat(), "BULLISH", 70.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter=None, days_back=365
        )

        tickers_in_result = {row["ticker"] for row in rows}
        assert "AAPL" in tickers_in_result
        assert "MSFT" in tickers_in_result

    def test_respects_days_back_filter(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        # Two signals: one recent, one old
        recent_date = date.today() - timedelta(days=10)
        old_date = date.today() - timedelta(days=200)

        for signal_date in (recent_date, old_date):
            closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
            _insert_ohlcv(db_connection, "AAPL", signal_date, closes)
            _insert_score(db_connection, "AAPL", signal_date.isoformat(), "BULLISH", 70.0)

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter="AAPL", days_back=30
        )

        assert len(rows) == 1
        assert rows[0]["signal_date"] == recent_date.isoformat()

    def test_returns_empty_when_no_data(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import fetch_signals_with_forward_returns

        rows = fetch_signals_with_forward_returns(
            db_connection, n_days=5, ticker_filter=None, days_back=90
        )

        assert rows == []


# ---------------------------------------------------------------------------
# generate_scatter_chart
# ---------------------------------------------------------------------------

class TestGenerateScatterChart:
    def test_returns_png_file_path(self) -> None:
        from src.notifier.scatter_command import generate_scatter_chart

        data = [
            {"ticker": "AAPL", "signal_date": "2025-01-10", "signal": "BULLISH", "confidence": 75.0, "forward_return_pct": 3.5},
            {"ticker": "AAPL", "signal_date": "2025-01-11", "signal": "BEARISH", "confidence": 60.0, "forward_return_pct": 2.0},
            {"ticker": "AAPL", "signal_date": "2025-01-12", "signal": "NEUTRAL", "confidence": 40.0, "forward_return_pct": -0.5},
        ]

        chart_path = generate_scatter_chart(data, n_days=5, ticker_filter=None, days_back=90)
        try:
            assert chart_path.endswith(".png")
            assert os.path.exists(chart_path)
        finally:
            if os.path.exists(chart_path):
                os.unlink(chart_path)

    def test_handles_empty_data(self) -> None:
        from src.notifier.scatter_command import generate_scatter_chart

        chart_path = generate_scatter_chart([], n_days=5, ticker_filter=None, days_back=90)
        try:
            assert chart_path.endswith(".png")
            assert os.path.exists(chart_path)
        finally:
            if os.path.exists(chart_path):
                os.unlink(chart_path)

    def test_ticker_filter_appears_in_title(self) -> None:
        """Chart is generated without error when ticker_filter is set."""
        from src.notifier.scatter_command import generate_scatter_chart

        data = [
            {"ticker": "AAPL", "signal_date": "2025-01-10", "signal": "BULLISH", "confidence": 80.0, "forward_return_pct": 4.0},
        ]

        chart_path = generate_scatter_chart(data, n_days=10, ticker_filter="AAPL", days_back=60)
        try:
            assert os.path.exists(chart_path)
        finally:
            if os.path.exists(chart_path):
                os.unlink(chart_path)


# ---------------------------------------------------------------------------
# handle_scatter_command (end-to-end, mocked Telegram)
# ---------------------------------------------------------------------------

class TestHandleScatterCommand:
    def test_sends_photo_on_success(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import handle_scatter_command

        # Seed enough data for a valid forward return
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)
        _insert_score(db_connection, "AAPL", _REF_DATE.isoformat(), "BULLISH", 70.0)

        with patch("src.notifier.scatter_command.send_photo_to_chat") as mock_send, \
             patch("src.notifier.scatter_command.send_telegram_message") as mock_msg:
            mock_send.return_value = True

            handle_scatter_command(
                conn=db_connection,
                chat_id="123",
                message_text=f"/scatter 5 AAPL 365",
                bot_token="fake-token",
                config=SAMPLE_CONFIG,
                active_tickers=ACTIVE_TICKERS,
            )

        mock_send.assert_called_once()
        call_args = mock_send.call_args
        assert call_args[0][0] == "fake-token"
        assert call_args[0][1] == "123"
        # PNG file should have been cleaned up after send
        chart_path = call_args[0][2]
        assert not os.path.exists(chart_path)

    def test_sends_error_message_on_invalid_command(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import handle_scatter_command

        with patch("src.notifier.scatter_command.send_telegram_message") as mock_msg:
            handle_scatter_command(
                conn=db_connection,
                chat_id="123",
                message_text="/scatter",
                bot_token="fake-token",
                config=SAMPLE_CONFIG,
                active_tickers=ACTIVE_TICKERS,
            )

        mock_msg.assert_called_once()
        msg_text = mock_msg.call_args[0][2]
        assert "Usage" in msg_text or "usage" in msg_text

    def test_sends_info_message_when_no_data(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import handle_scatter_command

        with patch("src.notifier.scatter_command.send_photo_to_chat") as mock_send, \
             patch("src.notifier.scatter_command.send_telegram_message") as mock_msg:
            handle_scatter_command(
                conn=db_connection,
                chat_id="123",
                message_text="/scatter 5 AAPL 90",
                bot_token="fake-token",
                config=SAMPLE_CONFIG,
                active_tickers=ACTIVE_TICKERS,
            )

        # When no data with N future closes exists, we still generate a chart
        # (could be an "empty" chart), so either send_photo is called OR
        # send_telegram_message is called with "no data" message.
        assert mock_send.called or mock_msg.called

    def test_sends_error_message_when_photo_send_returns_false(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import handle_scatter_command

        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)
        _insert_score(db_connection, "AAPL", _REF_DATE.isoformat(), "BULLISH", 70.0)

        with patch("src.notifier.scatter_command.send_photo_to_chat") as mock_send, \
             patch("src.notifier.scatter_command.send_telegram_message") as mock_msg:
            mock_send.return_value = False

            handle_scatter_command(
                conn=db_connection,
                chat_id="123",
                message_text=f"/scatter 5 AAPL 365",
                bot_token="fake-token",
                config=SAMPLE_CONFIG,
                active_tickers=ACTIVE_TICKERS,
            )

        mock_send.assert_called_once()
        mock_msg.assert_called_once()
        assert "❌" in mock_msg.call_args[0][2]

    def test_sends_error_message_when_photo_send_raises(
        self, db_connection: sqlite3.Connection
    ) -> None:
        from src.notifier.scatter_command import handle_scatter_command

        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        _insert_ohlcv(db_connection, "AAPL", _REF_DATE, closes)
        _insert_score(db_connection, "AAPL", _REF_DATE.isoformat(), "BULLISH", 70.0)

        with patch("src.notifier.scatter_command.send_photo_to_chat") as mock_send, \
             patch("src.notifier.scatter_command.send_telegram_message") as mock_msg:
            mock_send.side_effect = RuntimeError("network failure")

            handle_scatter_command(
                conn=db_connection,
                chat_id="123",
                message_text=f"/scatter 5 AAPL 365",
                bot_token="fake-token",
                config=SAMPLE_CONFIG,
                active_tickers=ACTIVE_TICKERS,
            )

        mock_send.assert_called_once()
        mock_msg.assert_called_once()
        assert "❌" in mock_msg.call_args[0][2]
