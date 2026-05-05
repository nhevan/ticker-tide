"""
Tests for src/web/queries.py — snapshot, sparkline, ticker list, and date range queries.

All tests use tmp_path + in-memory SQLite populated with minimal fixture data.
No external API calls are made.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Generator

import pytest

from src.common.db import create_all_tables
from src.web.queries import (
    fetch_active_tickers,
    fetch_date_range,
    fetch_snapshot,
    _extract_key_signals,
    _fetch_earnings,
    _fetch_signal_flip,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn(tmp_path) -> Generator[sqlite3.Connection, None, None]:
    """Open a temporary SQLite connection with the full schema created."""
    db_path = str(tmp_path / "test_signals.db")
    c = sqlite3.connect(db_path)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    create_all_tables(c)
    yield c
    c.close()


def _insert_tickers(conn: sqlite3.Connection) -> None:
    """Insert a set of test tickers including active, inactive, and ETFs."""
    rows = [
        ("AAPL", "Apple Inc", True),
        ("MSFT", "Microsoft Corp", True),
        ("INACT", "Inactive Corp", False),
        ("QQQ", "Invesco QQQ Trust", True),
        ("VOO", "Vanguard S&P 500 ETF", True),
        ("DIA", "SPDR Dow Jones ETF", True),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO tickers(symbol, name, active) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()


def _insert_daily_score(conn: sqlite3.Connection, ticker: str, date: str) -> None:
    """Insert a minimal scores_daily row."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ticker, date, "BULLISH", 72.5, 55.0, "trending",
         40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42),
    )
    conn.commit()


def _insert_weekly_score(conn: sqlite3.Connection, ticker: str, week_start: str) -> None:
    """Insert a minimal scores_weekly row."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_weekly(
            ticker, week_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (ticker, week_start, 48.0, "ranging",
         35.0, 20.0, 15.0, -5.0, 10.0, 12.0),
    )
    conn.commit()


def _insert_monthly_score(conn: sqlite3.Connection, ticker: str, month_start: str) -> None:
    """Insert a minimal scores_monthly row (candlestick_score intentionally NULL)."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_monthly(
            ticker, month_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (ticker, month_start, 38.0, "ranging",
         30.0, 15.0, 10.0, -8.0, None, 11.0),
    )
    conn.commit()


def _insert_daily_ohlcv(
    conn: sqlite3.Connection, ticker: str, dates_and_closes: list[tuple[str, float]]
) -> None:
    """Insert OHLCV rows for sparkline testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(ticker, d, c, c + 1, c - 1, c, 1_000_000) for d, c in dates_and_closes],
    )
    conn.commit()


def _insert_weekly_ohlcv(
    conn: sqlite3.Connection, ticker: str, weeks_and_closes: list[tuple[str, float]]
) -> None:
    """Insert weekly_candles rows for sparkline testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO weekly_candles(ticker, week_start, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(ticker, w, c, c + 1, c - 1, c, 5_000_000) for w, c in weeks_and_closes],
    )
    conn.commit()


def _insert_monthly_ohlcv(
    conn: sqlite3.Connection, ticker: str, months_and_closes: list[tuple[str, float]]
) -> None:
    """Insert monthly_candles rows for sparkline testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO monthly_candles(ticker, month_start, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(ticker, m, c, c + 1, c - 1, c, 20_000_000) for m, c in months_and_closes],
    )
    conn.commit()


def _insert_daily_patterns(conn: sqlite3.Connection, ticker: str, date: str) -> None:
    """Insert a pattern row for a ticker/date."""
    conn.execute(
        "INSERT INTO patterns_daily(ticker, date, pattern_name, direction, strength) "
        "VALUES (?, ?, 'Bullish Engulfing', 'bullish', 3)",
        (ticker, date),
    )
    conn.commit()


def _insert_weekly_patterns(conn: sqlite3.Connection, ticker: str, week_start: str) -> None:
    """Insert a weekly pattern row."""
    conn.execute(
        "INSERT INTO patterns_weekly(ticker, week_start, pattern_name, direction, strength) "
        "VALUES (?, ?, 'Morning Star', 'bullish', 4)",
        (ticker, week_start),
    )
    conn.commit()


def _insert_monthly_patterns(conn: sqlite3.Connection, ticker: str, month_start: str) -> None:
    """Insert a monthly pattern row."""
    conn.execute(
        "INSERT INTO patterns_monthly(ticker, month_start, pattern_name, direction, strength) "
        "VALUES (?, ?, 'Cup and Handle', 'bullish', 5)",
        (ticker, month_start),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# fetch_active_tickers tests
# ---------------------------------------------------------------------------

class TestFetchActiveTickers:
    """Tests for fetch_active_tickers()."""

    def test_returns_only_active_tickers(self, conn: sqlite3.Connection) -> None:
        """Inactive tickers must be excluded from the returned list."""
        _insert_tickers(conn)
        tickers = fetch_active_tickers(conn)
        assert "INACT" not in tickers

    def test_returns_alphabetized_list(self, conn: sqlite3.Connection) -> None:
        """Ticker list must be alphabetically sorted."""
        _insert_tickers(conn)
        tickers = fetch_active_tickers(conn)
        assert tickers == sorted(tickers)

    def test_includes_etfs_that_are_active(self, conn: sqlite3.Connection) -> None:
        """QQQ, VOO, DIA are active stocks and must appear in the list."""
        _insert_tickers(conn)
        tickers = fetch_active_tickers(conn)
        assert "QQQ" in tickers
        assert "VOO" in tickers
        assert "DIA" in tickers

    def test_empty_when_no_tickers(self, conn: sqlite3.Connection) -> None:
        """Empty table must return an empty list, not raise."""
        tickers = fetch_active_tickers(conn)
        assert tickers == []


# ---------------------------------------------------------------------------
# fetch_date_range tests
# ---------------------------------------------------------------------------

class TestFetchDateRange:
    """Tests for fetch_date_range()."""

    def test_returns_min_max_for_known_ticker(self, conn: sqlite3.Connection) -> None:
        """Min and max dates must reflect the actual scores_daily rows."""
        _insert_daily_score(conn, "AAPL", "2026-04-01")
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        result = fetch_date_range(conn, "AAPL")
        assert result["min"] == "2026-04-01"
        assert result["max"] == "2026-04-25"

    def test_returns_none_for_unknown_ticker(self, conn: sqlite3.Connection) -> None:
        """Unknown ticker must return min=None, max=None."""
        result = fetch_date_range(conn, "ZZZZ")
        assert result["min"] is None
        assert result["max"] is None


# ---------------------------------------------------------------------------
# fetch_snapshot — daily tests
# ---------------------------------------------------------------------------

class TestFetchSnapshotDaily:
    """Tests for daily section of fetch_snapshot()."""

    def test_daily_returns_all_9_categories(self, conn: sqlite3.Connection) -> None:
        """Daily categories array must contain all 9 category names."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        daily = snapshot["daily"]
        expected = {
            "trend", "momentum", "volume", "volatility",
            "candlestick", "structural", "sentiment", "fundamental", "macro",
        }
        assert set(daily["categories"]) == expected
        assert len(daily["categories"]) == 9

    def test_daily_data_available_true_for_known_date(self, conn: sqlite3.Connection) -> None:
        """data_available must be True when a scores_daily row exists."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["data_available"] is True

    def test_daily_data_available_false_for_unknown_date(self, conn: sqlite3.Connection) -> None:
        """data_available must be False when no scores_daily row exists for the date."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2099-01-01", config=_default_config())
        assert snapshot["daily"]["data_available"] is False

    def test_daily_includes_signal_and_confidence(self, conn: sqlite3.Connection) -> None:
        """Daily section must include signal and confidence fields."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        daily = snapshot["daily"]
        assert daily["signal"] == "BULLISH"
        assert abs(daily["confidence"] - 72.5) < 0.01

    def test_daily_includes_calibrated_score(self, conn: sqlite3.Connection) -> None:
        """Daily section must include calibrated_score."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert abs(snapshot["daily"]["calibrated_score"] - 1.42) < 0.01

    def test_daily_resolved_period_matches_date(self, conn: sqlite3.Connection) -> None:
        """Daily resolved_period must equal the picked date."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["resolved_period"] == "2026-04-25"


# ---------------------------------------------------------------------------
# fetch_snapshot — weekly tests
# ---------------------------------------------------------------------------

class TestFetchSnapshotWeekly:
    """Tests for weekly section of fetch_snapshot()."""

    def test_weekly_has_exactly_6_categories(self, conn: sqlite3.Connection) -> None:
        """Weekly categories array must contain exactly 6 entries."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        weekly = snapshot["weekly"]
        assert weekly["data_available"] is True
        expected = {"trend", "momentum", "volume", "volatility", "candlestick", "structural"}
        assert set(weekly["categories"]) == expected
        assert len(weekly["categories"]) == 6

    def test_weekly_returns_most_recent_le_date(self, conn: sqlite3.Connection) -> None:
        """Weekly must return the most recent week_start <= picked date."""
        _insert_weekly_score(conn, "AAPL", "2026-04-14")
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["weekly"]["resolved_period"] == "2026-04-21"

    def test_weekly_data_unavailable_when_no_row_le_date(self, conn: sqlite3.Connection) -> None:
        """data_available must be False when no week_start <= picked date exists."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-01-01", config=_default_config())
        assert snapshot["weekly"]["data_available"] is False

    def test_weekly_is_fallback_true_when_resolved_differs(self, conn: sqlite3.Connection) -> None:
        """is_fallback must be True when resolved_period < picked date."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        # Picked date 2026-04-25 but week_start is 2026-04-21 → fallback
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["weekly"]["is_fallback"] is True

    def test_weekly_is_fallback_false_when_resolved_matches(self, conn: sqlite3.Connection) -> None:
        """is_fallback must be False when resolved_period equals picked date."""
        _insert_weekly_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["weekly"]["is_fallback"] is False

    def test_weekly_resolved_period_label_format(self, conn: sqlite3.Connection) -> None:
        """Weekly resolved_period_label must say 'Week ending <date>'."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        label = snapshot["weekly"]["resolved_period_label"]
        assert "Week ending" in label

    def test_weekly_no_signal_or_confidence_fields(self, conn: sqlite3.Connection) -> None:
        """Weekly section must NOT expose signal or confidence fields (daily-only)."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        weekly = snapshot["weekly"]
        assert "signal" not in weekly
        assert "confidence" not in weekly


# ---------------------------------------------------------------------------
# fetch_snapshot — monthly tests
# ---------------------------------------------------------------------------

class TestFetchSnapshotMonthly:
    """Tests for monthly section of fetch_snapshot()."""

    def test_monthly_has_exactly_5_categories(self, conn: sqlite3.Connection) -> None:
        """Monthly categories must contain exactly 5 entries — candlestick excluded."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        monthly = snapshot["monthly"]
        assert monthly["data_available"] is True
        expected = {"trend", "momentum", "volume", "volatility", "structural"}
        assert set(monthly["categories"]) == expected
        assert len(monthly["categories"]) == 5

    def test_monthly_candlestick_excluded_from_categories_even_when_null(
        self, conn: sqlite3.Connection
    ) -> None:
        """candlestick must not appear in monthly categories array even when score is None."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert "candlestick" not in snapshot["monthly"]["categories"]

    def test_monthly_returns_most_recent_le_date(self, conn: sqlite3.Connection) -> None:
        """Monthly must return the most recent month_start <= picked date."""
        _insert_monthly_score(conn, "AAPL", "2026-03-01")
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["monthly"]["resolved_period"] == "2026-04-01"

    def test_monthly_data_unavailable_when_no_row_le_date(self, conn: sqlite3.Connection) -> None:
        """data_available must be False when no month_start <= picked date exists."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-01-01", config=_default_config())
        assert snapshot["monthly"]["data_available"] is False

    def test_monthly_is_fallback_true_when_resolved_differs(self, conn: sqlite3.Connection) -> None:
        """is_fallback must be True when resolved_period < picked date."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["monthly"]["is_fallback"] is True

    def test_monthly_is_fallback_false_when_resolved_matches(self, conn: sqlite3.Connection) -> None:
        """is_fallback must be False when resolved_period equals picked date."""
        _insert_monthly_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["monthly"]["is_fallback"] is False

    def test_monthly_no_signal_or_confidence_fields(self, conn: sqlite3.Connection) -> None:
        """Monthly section must NOT expose signal or confidence fields."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        monthly = snapshot["monthly"]
        assert "signal" not in monthly
        assert "confidence" not in monthly


# ---------------------------------------------------------------------------
# fetch_snapshot — sparkline tests
# ---------------------------------------------------------------------------

class TestFetchSnapshotSparkline:
    """Tests for sparkline data in fetch_snapshot()."""

    def test_daily_sparkline_respects_le_date_bound(self, conn: sqlite3.Connection) -> None:
        """Daily sparkline must NOT include dates after the picked date."""
        _insert_daily_score(conn, "AAPL", "2026-04-10")
        dates_and_closes = [
            ("2026-03-20", 150.0),
            ("2026-03-21", 151.0),
            ("2026-04-10", 155.0),
            ("2026-04-20", 160.0),  # after picked date — must NOT appear
        ]
        _insert_daily_ohlcv(conn, "AAPL", dates_and_closes)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-10", config=_default_config())
        sparkline_dates = [row["date"] for row in snapshot["daily"]["sparkline"]]
        assert "2026-04-20" not in sparkline_dates

    def test_daily_sparkline_chronological_order(self, conn: sqlite3.Connection) -> None:
        """Daily sparkline entries must be in chronological order."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        dates_and_closes = [
            ("2026-04-11", 148.0),
            ("2026-04-14", 150.0),
            ("2026-04-15", 151.0),
            ("2026-04-25", 160.0),
        ]
        _insert_daily_ohlcv(conn, "AAPL", dates_and_closes)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        sparkline = snapshot["daily"]["sparkline"]
        dates = [row["date"] for row in sparkline]
        assert dates == sorted(dates)

    def test_weekly_sparkline_respects_le_date_bound(self, conn: sqlite3.Connection) -> None:
        """Weekly sparkline must NOT include weeks after the picked date."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        weeks_and_closes = [
            ("2026-03-17", 140.0),
            ("2026-03-24", 142.0),
            ("2026-04-21", 155.0),
            ("2026-04-28", 158.0),  # future — must NOT appear
        ]
        _insert_weekly_ohlcv(conn, "AAPL", weeks_and_closes)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        weekly_sparkline_dates = [row["date"] for row in snapshot["weekly"]["sparkline"]]
        assert "2026-04-28" not in weekly_sparkline_dates

    def test_monthly_sparkline_respects_le_date_bound(self, conn: sqlite3.Connection) -> None:
        """Monthly sparkline must NOT include months after the picked date."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        months_and_closes = [
            ("2025-10-01", 120.0),
            ("2026-04-01", 155.0),
            ("2026-05-01", 160.0),  # future — must NOT appear
        ]
        _insert_monthly_ohlcv(conn, "AAPL", months_and_closes)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        monthly_sparkline_dates = [row["date"] for row in snapshot["monthly"]["sparkline"]]
        assert "2026-05-01" not in monthly_sparkline_dates


# ---------------------------------------------------------------------------
# fetch_snapshot — patterns tests
# ---------------------------------------------------------------------------

class TestFetchSnapshotPatterns:
    """Tests that patterns arrays are populated in the snapshot."""

    def test_daily_patterns_populated(self, conn: sqlite3.Connection) -> None:
        """Daily patterns list must contain the inserted pattern."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_daily_patterns(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        patterns = snapshot["daily"]["patterns"]
        assert len(patterns) >= 1
        assert any(p["pattern_name"] == "Bullish Engulfing" for p in patterns)

    def test_weekly_patterns_populated(self, conn: sqlite3.Connection) -> None:
        """Weekly patterns list must contain the inserted pattern."""
        _insert_weekly_score(conn, "AAPL", "2026-04-21")
        _insert_weekly_patterns(conn, "AAPL", "2026-04-21")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        patterns = snapshot["weekly"]["patterns"]
        assert len(patterns) >= 1
        assert any(p["pattern_name"] == "Morning Star" for p in patterns)

    def test_monthly_patterns_populated(self, conn: sqlite3.Connection) -> None:
        """Monthly patterns list must contain the inserted pattern."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        _insert_monthly_patterns(conn, "AAPL", "2026-04-01")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        patterns = snapshot["monthly"]["patterns"]
        assert len(patterns) >= 1
        assert any(p["pattern_name"] == "Cup and Handle" for p in patterns)

    def test_empty_patterns_returns_empty_list(self, conn: sqlite3.Connection) -> None:
        """Patterns list must be an empty list (not None) when no patterns exist."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["patterns"] == []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_config() -> dict:
    """Return a minimal web config dict for test use."""
    return {
        "sparkline": {"daily_days": 15, "weekly_weeks": 6, "monthly_months": 6},
        "why_bullets": {"limit": 3},
        "signal_flip_lookback_days": 14,
    }


def _insert_daily_score_with_key_signals(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    key_signals: str | None,
) -> None:
    """Insert a scores_daily row with a specific key_signals value."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score, key_signals
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ticker, date, "BULLISH", 72.5, 55.0, "trending",
         40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42, key_signals),
    )
    conn.commit()


def _insert_earnings(
    conn: sqlite3.Connection,
    ticker: str,
    earnings_date: str,
    estimated_eps: float | None,
    actual_eps: float | None,
    eps_surprise: float | None,
) -> None:
    """Insert an earnings_calendar row."""
    conn.execute(
        """INSERT OR REPLACE INTO earnings_calendar(
            ticker, earnings_date, estimated_eps, actual_eps, eps_surprise
        ) VALUES (?, ?, ?, ?, ?)""",
        (ticker, earnings_date, estimated_eps, actual_eps, eps_surprise),
    )
    conn.commit()


def _insert_signal_flip(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    previous_signal: str,
    new_signal: str,
) -> int:
    """Insert a signal_flips row and return the new row id."""
    cursor = conn.execute(
        """INSERT INTO signal_flips(ticker, date, previous_signal, new_signal)
           VALUES (?, ?, ?, ?)""",
        (ticker, date, previous_signal, new_signal),
    )
    conn.commit()
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# TestExtractKeySignals
# ---------------------------------------------------------------------------

class TestExtractKeySignals:
    """Tests for _extract_key_signals()."""

    def test_happy_path_seven_items_returns_top_three(self) -> None:
        """Seven items in key_signals → only first 3 returned."""
        signals = [f"Signal {i}" for i in range(7)]
        score_dict = {"key_signals": json.dumps(signals)}
        result = _extract_key_signals(score_dict, limit=3)
        assert result == ["Signal 0", "Signal 1", "Signal 2"]

    def test_fewer_than_limit_returns_all(self) -> None:
        """Two items with limit=3 → both items returned."""
        signals = ["Signal A", "Signal B"]
        score_dict = {"key_signals": json.dumps(signals)}
        result = _extract_key_signals(score_dict, limit=3)
        assert result == ["Signal A", "Signal B"]

    def test_missing_key_returns_empty_list(self) -> None:
        """score_dict with no key_signals key → empty list."""
        result = _extract_key_signals({}, limit=3)
        assert result == []

    def test_none_value_returns_empty_list(self) -> None:
        """key_signals=None → empty list."""
        result = _extract_key_signals({"key_signals": None}, limit=3)
        assert result == []

    def test_malformed_json_returns_empty_list(self) -> None:
        """Invalid JSON in key_signals → empty list, no exception."""
        result = _extract_key_signals({"key_signals": "not valid json {{{"}, limit=3)
        assert result == []

    def test_non_list_parsed_value_returns_empty_list(self) -> None:
        """Parsed JSON that is not a list (e.g. dict) → empty list."""
        result = _extract_key_signals({"key_signals": json.dumps({"key": "value"})}, limit=3)
        assert result == []

    def test_limit_zero_returns_empty_list(self) -> None:
        """limit=0 → empty list."""
        result = _extract_key_signals({"key_signals": json.dumps(["A", "B"])}, limit=0)
        assert result == []


# ---------------------------------------------------------------------------
# TestFetchEarnings
# ---------------------------------------------------------------------------

class TestFetchEarnings:
    """Tests for _fetch_earnings()."""

    def test_both_next_and_last_populated(self, conn: sqlite3.Connection) -> None:
        """When future and past earnings exist, both next and last_surprise are populated."""
        _insert_earnings(conn, "AAPL", "2026-01-29", 2.67, 2.84, 0.17)
        _insert_earnings(conn, "AAPL", "2026-04-30", 1.95, None, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is not None
        assert result["next"]["date"] == "2026-04-30"
        assert abs(result["next"]["estimated_eps"] - 1.95) < 0.001
        assert result["last_surprise"] is not None
        assert result["last_surprise"]["date"] == "2026-01-29"
        assert abs(result["last_surprise"]["actual_eps"] - 2.84) < 0.001
        assert abs(result["last_surprise"]["surprise"] - 0.17) < 0.001
        assert result["last_surprise"]["beat"] is True

    def test_only_next_earnings(self, conn: sqlite3.Connection) -> None:
        """Only future earnings row present → next populated, last_surprise=None."""
        _insert_earnings(conn, "AAPL", "2026-04-30", 1.95, None, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is not None
        assert result["last_surprise"] is None

    def test_only_last_surprise(self, conn: sqlite3.Connection) -> None:
        """Only past earnings with actual_eps → last_surprise populated, next=None."""
        _insert_earnings(conn, "AAPL", "2026-01-29", 2.67, 2.84, 0.17)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is None
        assert result["last_surprise"] is not None

    def test_neither_present_returns_both_none(self, conn: sqlite3.Connection) -> None:
        """No earnings rows → both next and last_surprise are None."""
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is None
        assert result["last_surprise"] is None

    def test_same_day_excluded_from_next_by_strict_gt(self, conn: sqlite3.Connection) -> None:
        """An earnings row on picked_date itself is NOT returned as next (strict >)."""
        _insert_earnings(conn, "AAPL", "2026-04-17", 1.95, None, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is None

    def test_past_row_with_null_actual_eps_excluded_from_next(
        self, conn: sqlite3.Connection
    ) -> None:
        """Past row with actual_eps=NULL is excluded from 'next' query (actual_eps IS NULL guard
        only applies to future rows; the stricter earnings_date > picked_date excludes past rows anyway,
        but a same-date row with actual_eps=NULL must also be excluded by strict > boundary)."""
        # Insert row in the past but with actual_eps=NULL (stale null)
        _insert_earnings(conn, "AAPL", "2026-04-10", 1.50, None, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        # Past date (< picked_date) should not appear as next
        assert result["next"] is None

    def test_future_row_with_actual_eps_excluded_from_next(
        self, conn: sqlite3.Connection
    ) -> None:
        """Future row already having actual_eps is excluded from next (already reported)."""
        _insert_earnings(conn, "AAPL", "2026-04-30", 1.95, 2.10, 0.15)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"] is None

    def test_days_until_math_correct(self, conn: sqlite3.Connection) -> None:
        """days_until == (earnings_date - picked_date).days."""
        _insert_earnings(conn, "AAPL", "2026-04-30", 1.95, None, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["next"]["days_until"] == 13

    def test_beat_true_for_positive_surprise(self, conn: sqlite3.Connection) -> None:
        """beat=True when eps_surprise > 0."""
        _insert_earnings(conn, "AAPL", "2026-01-29", 2.67, 2.84, 0.17)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["last_surprise"]["beat"] is True

    def test_beat_false_for_negative_surprise(self, conn: sqlite3.Connection) -> None:
        """beat=False when eps_surprise < 0."""
        _insert_earnings(conn, "AAPL", "2026-01-29", 2.67, 2.50, -0.17)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["last_surprise"]["beat"] is False

    def test_beat_none_when_eps_surprise_is_null(self, conn: sqlite3.Connection) -> None:
        """beat=None when eps_surprise is NULL."""
        _insert_earnings(conn, "AAPL", "2026-01-29", 2.67, 2.84, None)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["last_surprise"]["beat"] is None

    def test_last_surprise_date_uses_le_boundary(self, conn: sqlite3.Connection) -> None:
        """Past row on exactly picked_date with actual_eps is included in last_surprise."""
        _insert_earnings(conn, "AAPL", "2026-04-17", 2.67, 2.84, 0.17)
        result = _fetch_earnings(conn, "AAPL", "2026-04-17")
        assert result["last_surprise"] is not None
        assert result["last_surprise"]["date"] == "2026-04-17"


# ---------------------------------------------------------------------------
# TestFetchSignalFlip
# ---------------------------------------------------------------------------

class TestFetchSignalFlip:
    """Tests for _fetch_signal_flip()."""

    def test_single_row_in_window(self, conn: sqlite3.Connection) -> None:
        """Single flip within lookback window is returned."""
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "NEUTRAL", "BULLISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is not None
        assert result["date"] == "2026-04-27"
        assert result["previous_signal"] == "NEUTRAL"
        assert result["new_signal"] == "BULLISH"

    def test_no_rows_returns_none(self, conn: sqlite3.Connection) -> None:
        """No flip rows in window → None."""
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is None

    def test_row_outside_window_excluded(self, conn: sqlite3.Connection) -> None:
        """Flip older than lookback_days floor is excluded."""
        _insert_signal_flip(conn, "AAPL", "2026-04-01", "NEUTRAL", "BULLISH")
        # picked_date=2026-04-28, lookback=14 → floor = 2026-04-14; 2026-04-01 < floor
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is None

    def test_row_on_floor_date_included(self, conn: sqlite3.Connection) -> None:
        """Flip exactly on the floor date (picked_date - lookback_days) is included."""
        _insert_signal_flip(conn, "AAPL", "2026-04-14", "NEUTRAL", "BULLISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is not None

    def test_duplicate_same_date_resolved_by_id_desc(
        self, conn: sqlite3.Connection
    ) -> None:
        """Multiple rows on same date → highest id (last inserted) is returned."""
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "NEUTRAL", "BULLISH")
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "NEUTRAL", "BEARISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is not None
        # Second insert has higher id → BEARISH wins
        assert result["new_signal"] == "BEARISH"

    def test_contradictory_same_date_resolves_to_highest_id(
        self, conn: sqlite3.Connection
    ) -> None:
        """Contradictory rows on same date for different transitions resolve by id DESC."""
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "BULLISH", "NEUTRAL")
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "NEUTRAL", "BULLISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result is not None
        assert result["previous_signal"] == "NEUTRAL"
        assert result["new_signal"] == "BULLISH"

    def test_days_ago_math(self, conn: sqlite3.Connection) -> None:
        """days_ago == (picked_date - flip_date).days."""
        _insert_signal_flip(conn, "AAPL", "2026-04-27", "NEUTRAL", "BULLISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result["days_ago"] == 1

    def test_days_ago_zero_for_same_day(self, conn: sqlite3.Connection) -> None:
        """Flip on picked_date itself → days_ago=0."""
        _insert_signal_flip(conn, "AAPL", "2026-04-28", "NEUTRAL", "BULLISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result["days_ago"] == 0

    def test_most_recent_row_returned_when_multiple_dates(
        self, conn: sqlite3.Connection
    ) -> None:
        """When multiple flip dates exist in window, most recent date is returned."""
        _insert_signal_flip(conn, "AAPL", "2026-04-15", "NEUTRAL", "BULLISH")
        _insert_signal_flip(conn, "AAPL", "2026-04-25", "BULLISH", "BEARISH")
        result = _fetch_signal_flip(conn, "AAPL", "2026-04-28", lookback_days=14)
        assert result["date"] == "2026-04-25"


# ---------------------------------------------------------------------------
# TestBuildDailySection — extended for new keys
# ---------------------------------------------------------------------------

class TestBuildDailySectionNewKeys:
    """Extended tests ensuring key_signals, earnings, and signal_flip appear in daily section."""

    def test_key_signals_present_when_populated(self, conn: sqlite3.Connection) -> None:
        """key_signals in daily section returns first 3 items when scores_daily has them."""
        signals = [f"Signal {i}" for i in range(7)]
        _insert_daily_score_with_key_signals(conn, "AAPL", "2026-04-25", json.dumps(signals))
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["key_signals"] == ["Signal 0", "Signal 1", "Signal 2"]

    def test_key_signals_empty_when_column_null(self, conn: sqlite3.Connection) -> None:
        """key_signals is [] when the column value is None."""
        _insert_daily_score_with_key_signals(conn, "AAPL", "2026-04-25", None)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["key_signals"] == []

    def test_key_signals_empty_when_no_data(self, conn: sqlite3.Connection) -> None:
        """key_signals not present (or empty) when data_available=False."""
        snapshot = fetch_snapshot(conn, "AAPL", "2099-01-01", config=_default_config())
        assert snapshot["daily"]["data_available"] is False
        # key_signals should not be in the no-data dict
        assert "key_signals" not in snapshot["daily"]

    def test_earnings_next_populated(self, conn: sqlite3.Connection) -> None:
        """earnings.next populated when future earnings row exists."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_earnings(conn, "AAPL", "2026-04-30", 1.95, None, None)
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["earnings"]["next"] is not None
        assert snapshot["daily"]["earnings"]["next"]["date"] == "2026-04-30"

    def test_earnings_both_none_when_no_rows(self, conn: sqlite3.Connection) -> None:
        """earnings.next and earnings.last_surprise are None when no rows exist."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["earnings"]["next"] is None
        assert snapshot["daily"]["earnings"]["last_surprise"] is None

    def test_signal_flip_present_within_window(self, conn: sqlite3.Connection) -> None:
        """signal_flip populated when a flip exists within the lookback window."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        _insert_signal_flip(conn, "AAPL", "2026-04-24", "NEUTRAL", "BULLISH")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["signal_flip"] is not None
        assert snapshot["daily"]["signal_flip"]["new_signal"] == "BULLISH"

    def test_signal_flip_none_when_no_rows(self, conn: sqlite3.Connection) -> None:
        """signal_flip=None when no flip rows exist within the lookback window."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["signal_flip"] is None

    def test_uses_config_why_bullets_limit(self, conn: sqlite3.Connection) -> None:
        """why_bullets.limit from config controls how many signals are returned."""
        signals = [f"Signal {i}" for i in range(7)]
        _insert_daily_score_with_key_signals(conn, "AAPL", "2026-04-25", json.dumps(signals))
        config = _default_config()
        config["why_bullets"] = {"limit": 5}
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=config)
        assert len(snapshot["daily"]["key_signals"]) == 5

    def test_uses_config_signal_flip_lookback_days(self, conn: sqlite3.Connection) -> None:
        """signal_flip_lookback_days from config controls lookback window."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        # Insert a flip 20 days ago — outside default 14-day window
        _insert_signal_flip(conn, "AAPL", "2026-04-05", "NEUTRAL", "BULLISH")
        # With default lookback (14), should be None
        default_snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert default_snapshot["daily"]["signal_flip"] is None
        # With extended lookback (30), should be found
        extended_config = _default_config()
        extended_config["signal_flip_lookback_days"] = 30
        extended_snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=extended_config)
        assert extended_snapshot["daily"]["signal_flip"] is not None
