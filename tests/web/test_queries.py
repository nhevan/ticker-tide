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
    fetch_tickers_list,
    _extract_key_signals,
    _fetch_earnings,
    _fetch_signal_flip,
    _fetch_recent_patterns,
    _fetch_adx_sparkline,
    _fetch_stoch_sparkline,
    _fetch_stoch_k_profile,
    _fetch_cci_sparkline,
    _fetch_cci_profile,
    _build_daily_section,
    _build_weekly_section,
    _build_monthly_section,
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
    """Insert a minimal scores_daily row including daily_score (pre-blend)."""
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, daily_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ticker, date, "BULLISH", 72.5, 55.0, 42.0, "trending",
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

    def test_daily_includes_daily_score_pre_blend(self, conn: sqlite3.Connection) -> None:
        """daily_score (pre-blend) must be surfaced in the daily section dict."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        daily = snapshot["daily"]
        assert "daily_score" in daily
        assert abs(daily["daily_score"] - 42.0) < 0.01

    def test_daily_score_distinct_from_composite_score(self, conn: sqlite3.Connection) -> None:
        """daily_score (42.0) must differ from composite_score/final_score (55.0)."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        daily = snapshot["daily"]
        assert daily["daily_score"] != daily["composite_score"]


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

    def test_monthly_contributions_payload_parsed_when_column_populated(
        self, conn: sqlite3.Connection
    ) -> None:
        """
        When scores_monthly.key_signals_data contains valid JSON, _build_monthly_section
        must return contributions_payload as a parsed dict (not None, not a raw string).
        """
        import json
        sample_payload = {"v": 1, "expansion_factor": 1.0, "items": [
            {"name": "rsi_14", "kind": "indicator", "raw_value": 55.0,
             "score": 55.0, "category": "momentum", "category_weight": 0.25,
             "contribution": 2.0},
        ]}
        conn.execute(
            """INSERT OR REPLACE INTO scores_monthly(
                ticker, month_start, composite_score, regime,
                trend_score, momentum_score, volume_score, volatility_score,
                candlestick_score, structural_score, key_signals_data
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            ("AAPL", "2026-04-01", 38.0, "ranging",
             30.0, 15.0, 10.0, -8.0, None, 11.0,
             json.dumps(sample_payload)),
        )
        conn.commit()

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        monthly = snapshot["monthly"]
        assert monthly["data_available"] is True
        payload = monthly.get("contributions_payload")
        assert payload is not None, "contributions_payload must be present and non-None"
        assert isinstance(payload, dict), "contributions_payload must be a parsed dict"
        assert payload["v"] == 1
        assert len(payload["items"]) == 1
        assert payload["items"][0]["name"] == "rsi_14"

    def test_monthly_contributions_payload_is_none_when_column_null(
        self, conn: sqlite3.Connection
    ) -> None:
        """
        When scores_monthly.key_signals_data is NULL (legacy row), contributions_payload
        must be None in the returned section dict.
        """
        # _insert_monthly_score does not set key_signals_data — it stays NULL.
        _insert_monthly_score(conn, "AAPL", "2026-04-01")

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        monthly = snapshot["monthly"]
        assert monthly["data_available"] is True
        assert monthly.get("contributions_payload") is None, (
            "contributions_payload must be None for legacy rows with NULL key_signals_data"
        )


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


# ---------------------------------------------------------------------------
# indicator_scores in snapshot sections (Step 6)
# ---------------------------------------------------------------------------

def _insert_indicator_scores_daily(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    scores: dict,
) -> None:
    """Insert rows into indicator_scores_daily for testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicator_scores_daily(ticker, date, indicator_name, score) "
        "VALUES (?, ?, ?, ?)",
        [(ticker, date, name, val) for name, val in scores.items()],
    )
    conn.commit()


def _insert_indicator_scores_weekly(
    conn: sqlite3.Connection,
    ticker: str,
    week_start: str,
    scores: dict,
) -> None:
    """Insert rows into indicator_scores_weekly for testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicator_scores_weekly(ticker, week_start, indicator_name, score) "
        "VALUES (?, ?, ?, ?)",
        [(ticker, week_start, name, val) for name, val in scores.items()],
    )
    conn.commit()


def _insert_indicator_scores_monthly(
    conn: sqlite3.Connection,
    ticker: str,
    month_start: str,
    scores: dict,
) -> None:
    """Insert rows into indicator_scores_monthly for testing."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicator_scores_monthly(ticker, month_start, indicator_name, score) "
        "VALUES (?, ?, ?, ?)",
        [(ticker, month_start, name, val) for name, val in scores.items()],
    )
    conn.commit()


class TestSnapshotIndicatorScores:
    """Verify fetch_snapshot returns indicator_scores in each section."""

    def test_daily_indicator_scores_present_in_snapshot(
        self, conn: sqlite3.Connection
    ) -> None:
        """Daily section includes indicator_scores when rows exist."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")
        expected = {"rsi_14": 45.5, "macd_histogram": -20.0}
        _insert_indicator_scores_daily(conn, "AAPL", "2026-04-25", expected)

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert "indicator_scores" in snapshot["daily"], (
            "Daily section must contain 'indicator_scores'"
        )
        assert snapshot["daily"]["indicator_scores"] == expected

    def test_daily_indicator_scores_empty_when_no_rows(
        self, conn: sqlite3.Connection
    ) -> None:
        """Daily section returns empty dict for indicator_scores when no sidecar rows exist."""
        _insert_daily_score(conn, "AAPL", "2026-04-25")

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["indicator_scores"] == {}

    def test_daily_indicator_scores_empty_when_table_missing(
        self, tmp_path
    ) -> None:
        """indicator_scores returns {} when indicator_scores_daily table doesn't exist."""
        import sqlite3 as _sqlite3
        from src.web.queries import fetch_snapshot as _fetch_snapshot
        # Create a minimal DB without sidecar tables — use create_all_tables
        # but then drop the sidecar tables to simulate pre-migration state.
        from src.common.db import create_all_tables
        raw_conn = _sqlite3.connect(str(tmp_path / "bare.db"))
        raw_conn.row_factory = _sqlite3.Row
        create_all_tables(raw_conn)
        # Drop the sidecar table to simulate pre-migration state.
        raw_conn.execute("DROP TABLE IF EXISTS indicator_scores_daily")
        raw_conn.execute(
            "INSERT INTO scores_daily(ticker, date, signal, confidence, final_score, regime) "
            "VALUES ('AAPL', '2026-04-25', 'BULLISH', 60.0, 30.0, 'ranging')"
        )
        raw_conn.commit()

        snapshot = _fetch_snapshot(raw_conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["daily"]["indicator_scores"] == {}, (
            "indicator_scores must be empty dict when table does not exist"
        )
        raw_conn.close()

    def test_weekly_indicator_scores_present_in_snapshot(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly section includes indicator_scores when rows exist."""
        _insert_weekly_score(conn, "AAPL", "2026-04-20")
        expected = {"rsi_14": 60.0, "adx": None}
        _insert_indicator_scores_weekly(conn, "AAPL", "2026-04-20", expected)

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert "indicator_scores" in snapshot["weekly"]
        assert snapshot["weekly"]["indicator_scores"] == expected

    def test_weekly_indicator_scores_empty_when_no_rows(
        self, conn: sqlite3.Connection
    ) -> None:
        """Weekly section returns empty dict when no indicator_scores_weekly rows exist."""
        _insert_weekly_score(conn, "AAPL", "2026-04-20")

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["weekly"]["indicator_scores"] == {}

    def test_monthly_indicator_scores_present_in_snapshot(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly section includes indicator_scores when rows exist."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")
        expected = {"cmf_20": 15.0, "bb_pctb": -5.0}
        _insert_indicator_scores_monthly(conn, "AAPL", "2026-04-01", expected)

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert "indicator_scores" in snapshot["monthly"]
        assert snapshot["monthly"]["indicator_scores"] == expected

    def test_monthly_indicator_scores_empty_when_no_rows(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly section returns empty dict when no indicator_scores_monthly rows exist."""
        _insert_monthly_score(conn, "AAPL", "2026-04-01")

        snapshot = fetch_snapshot(conn, "AAPL", "2026-04-25", config=_default_config())
        assert snapshot["monthly"]["indicator_scores"] == {}


# ---------------------------------------------------------------------------
# _insert_pattern_with_category helper — used only by recent-patterns tests
# ---------------------------------------------------------------------------

def _insert_pattern_with_category(
    conn: sqlite3.Connection,
    table: str,
    ticker: str,
    period_col: str,
    period_value: str,
    pattern_name: str,
    pattern_category: str,
    direction: str,
    strength: float,
    confirmed: bool = True,
) -> None:
    """Insert a pattern row with explicit pattern_category. Used only by recent-patterns tests."""
    conn.execute(
        f"INSERT INTO {table} (ticker, {period_col}, pattern_name, pattern_category, "
        f"direction, strength, confirmed) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, period_value, pattern_name, pattern_category, direction, strength, int(confirmed)),
    )
    conn.commit()


def _default_config_with_pattern_limit() -> dict:
    """Return a web config dict that also includes pattern_row_limit."""
    cfg = _default_config()
    cfg["pattern_row_limit"] = 5
    return cfg


# ---------------------------------------------------------------------------
# TestFetchRecentPatterns — unit tests for _fetch_recent_patterns helper
# ---------------------------------------------------------------------------

class TestFetchRecentPatterns:
    """Tests for _fetch_recent_patterns()."""

    def test_fetch_recent_patterns_daily_within_window(
        self, conn: sqlite3.Connection
    ) -> None:
        """Insert candlestick at period_date, period_date-6d, period_date-8d; 7d window returns first two."""
        period_date = "2026-05-08"
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-08",
            "hammer", "candlestick", "bullish", 2.0,
        )
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-02",
            "doji", "candlestick", "neutral", 1.5,
        )
        # 8 days before period_date — outside 7-day window
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-04-30",
            "shooting_star", "candlestick", "bearish", 3.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", period_date,
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=False,
        )
        returned_names = {row["pattern_name"] for row in result}
        assert "hammer" in returned_names
        assert "doji" in returned_names
        assert "shooting_star" not in returned_names

    def test_fetch_recent_patterns_boundary(
        self, conn: sqlite3.Connection
    ) -> None:
        """Boundary: candlestick at period_date-7d is INCLUDED; at period_date-8d is EXCLUDED."""
        period_date = "2026-05-08"
        # Exactly 7 days before → INCLUDED (window is [period_date - 7d, period_date])
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-01",
            "engulfing_7d", "candlestick", "bullish", 2.0,
        )
        # 8 days before → EXCLUDED
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-04-30",
            "engulfing_8d", "candlestick", "bullish", 2.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", period_date,
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=False,
        )
        returned_names = {row["pattern_name"] for row in result}
        assert "engulfing_7d" in returned_names
        assert "engulfing_8d" not in returned_names

    def test_fetch_recent_patterns_caps_per_category_at_five(
        self, conn: sqlite3.Connection
    ) -> None:
        """7 candlestick + 6 structural all within window; each category capped at 5."""
        period_date = "2026-05-08"
        for i in range(7):
            _insert_pattern_with_category(
                conn, "patterns_daily", "AAPL", "date", period_date,
                f"cdl_{i}", "candlestick", "bullish", float(i + 1),
            )
        for i in range(6):
            _insert_pattern_with_category(
                conn, "patterns_daily", "AAPL", "date", period_date,
                f"struct_{i}", "structural", "bullish", float(i + 1),
            )
        result = _fetch_recent_patterns(
            conn, "AAPL", period_date,
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick", "structural"),
            top_n=5, compute_days_ago=False,
        )
        candlestick_rows = [row for row in result if row["pattern_category"] == "candlestick"]
        structural_rows = [row for row in result if row["pattern_category"] == "structural"]
        assert len(candlestick_rows) == 5
        assert len(structural_rows) == 5

    def test_fetch_recent_patterns_sort_order(
        self, conn: sqlite3.Connection
    ) -> None:
        """Sort order: date DESC then strength DESC within date."""
        period_date = "2026-05-08"
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", period_date,
            "high_strength", "candlestick", "bullish", 99.0,
        )
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", period_date,
            "low_strength", "candlestick", "bullish", 10.0,
        )
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-07",
            "older_pattern", "candlestick", "bullish", 99.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", period_date,
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=False,
        )
        assert len(result) == 3
        assert result[0]["pattern_name"] == "high_strength"
        assert result[1]["pattern_name"] == "low_strength"
        assert result[2]["pattern_name"] == "older_pattern"

    def test_fetch_recent_patterns_empty_returns_empty_list(
        self, conn: sqlite3.Connection
    ) -> None:
        """No rows seeded → empty list returned."""
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-08",
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick", "structural"),
            top_n=5, compute_days_ago=True,
        )
        assert result == []

    def test_fetch_recent_patterns_computes_days_ago(
        self, conn: sqlite3.Connection
    ) -> None:
        """Insert at 2026-05-04, call with period_date='2026-05-08'; days_ago == 4."""
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-04",
            "hammer", "candlestick", "bullish", 2.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-08",
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=True,
        )
        assert len(result) == 1
        assert result[0]["days_ago"] == 4

    def test_fetch_recent_patterns_clamps_future_days_ago(
        self, conn: sqlite3.Connection
    ) -> None:
        """Future-dated row (period_date in the past relative to pattern) → days_ago clamped to 0."""
        # Insert a pattern with date AFTER period_date (anomalous data)
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-10",
            "hammer", "candlestick", "bullish", 2.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-10",
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=True,
        )
        # The row AT period_date has days_ago=0
        assert result[0]["days_ago"] == 0

    def test_fetch_recent_patterns_excludes_null_category(
        self, conn: sqlite3.Connection
    ) -> None:
        """Rows with pattern_category=NULL must be excluded."""
        conn.execute(
            "INSERT INTO patterns_daily (ticker, date, pattern_name, direction, strength) "
            "VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2026-05-08", "null_cat_pattern", "bullish", 2.0),
        )
        conn.commit()
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-08",
            table_name="patterns_daily", period_column="date",
            allowed_categories=("candlestick",),
            top_n=5, compute_days_ago=True,
        )
        assert result == []

    def test_fetch_recent_patterns_unknown_category_skipped(
        self, conn: sqlite3.Connection
    ) -> None:
        """Unknown category (not in _WINDOW_BY_CATEGORY) → returns [], no exception."""
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-08",
            "some_pattern", "trend", "bullish", 2.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-08",
            table_name="patterns_daily", period_column="date",
            allowed_categories=("trend",),
            top_n=5, compute_days_ago=True,
        )
        assert result == []

    def test_fetch_recent_patterns_invalid_table_name_raises(
        self, conn: sqlite3.Connection
    ) -> None:
        """Invalid table_name raises ValueError."""
        with pytest.raises(ValueError, match="Invalid table_name"):
            _fetch_recent_patterns(
                conn, "AAPL", "2026-05-08",
                table_name="users", period_column="date",
                allowed_categories=("candlestick",),
                top_n=5, compute_days_ago=True,
            )

    def test_fetch_recent_patterns_invalid_period_column_raises(
        self, conn: sqlite3.Connection
    ) -> None:
        """Invalid period_column raises ValueError."""
        with pytest.raises(ValueError, match="Invalid period_column"):
            _fetch_recent_patterns(
                conn, "AAPL", "2026-05-08",
                table_name="patterns_daily", period_column="bad_col",
                allowed_categories=("candlestick",),
                top_n=5, compute_days_ago=True,
            )

    def test_fetch_recent_patterns_weekly_omits_days_ago(
        self, conn: sqlite3.Connection
    ) -> None:
        """compute_days_ago=False → 'days_ago' key absent from returned dicts."""
        _insert_pattern_with_category(
            conn, "patterns_weekly", "AAPL", "week_start", "2026-05-05",
            "breakout", "structural", "bullish", 2.0,
        )
        result = _fetch_recent_patterns(
            conn, "AAPL", "2026-05-08",
            table_name="patterns_weekly", period_column="week_start",
            allowed_categories=("structural",),
            top_n=5, compute_days_ago=False,
        )
        assert len(result) == 1
        assert "days_ago" not in result[0]

    def test_build_daily_section_includes_recent_patterns(
        self, conn: sqlite3.Connection
    ) -> None:
        """_build_daily_section result must include 'recent_patterns' as a list."""
        _insert_daily_score(conn, "AAPL", "2026-05-08")
        _insert_pattern_with_category(
            conn, "patterns_daily", "AAPL", "date", "2026-05-08",
            "hammer", "candlestick", "bullish", 2.0,
        )
        result = _build_daily_section(
            conn, "AAPL", "2026-05-08", sparkline_days=15,
            pattern_row_limit=5,
        )
        assert "recent_patterns" in result
        assert isinstance(result["recent_patterns"], list)
        pattern_names = [p["pattern_name"] for p in result["recent_patterns"]]
        assert "hammer" in pattern_names

    def test_build_weekly_section_includes_recent_patterns(
        self, conn: sqlite3.Connection
    ) -> None:
        """_build_weekly_section result must include 'recent_patterns' as a list."""
        _insert_weekly_score(conn, "AAPL", "2026-05-05")
        _insert_pattern_with_category(
            conn, "patterns_weekly", "AAPL", "week_start", "2026-05-05",
            "breakout", "structural", "bullish", 3.0,
        )
        result = _build_weekly_section(
            conn, "AAPL", "2026-05-08", sparkline_weeks=6,
            pattern_row_limit=5,
        )
        assert "recent_patterns" in result
        assert isinstance(result["recent_patterns"], list)
        pattern_names = [p["pattern_name"] for p in result["recent_patterns"]]
        assert "breakout" in pattern_names

    def test_build_monthly_section_includes_recent_patterns(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monthly allows structural only; candlestick in patterns_monthly must NOT appear."""
        _insert_monthly_score(conn, "AAPL", "2026-05-01")
        _insert_pattern_with_category(
            conn, "patterns_monthly", "AAPL", "month_start", "2026-05-01",
            "double_bottom", "structural", "bullish", 3.0,
        )
        # This candlestick row should NOT appear — monthly excludes candlestick
        _insert_pattern_with_category(
            conn, "patterns_monthly", "AAPL", "month_start", "2026-05-01",
            "hammer", "candlestick", "bullish", 2.0,
        )
        result = _build_monthly_section(
            conn, "AAPL", "2026-05-08", sparkline_months=6,
            pattern_row_limit=5,
        )
        assert "recent_patterns" in result
        pattern_names = [p["pattern_name"] for p in result["recent_patterns"]]
        assert "double_bottom" in pattern_names
        assert "hammer" not in pattern_names

    def test_existing_patterns_field_still_exact_date(
        self, conn: sqlite3.Connection
    ) -> None:
        """Regression: existing 'patterns' field still present and unchanged; no days_ago key."""
        _insert_daily_score(conn, "AAPL", "2026-05-08")
        _insert_daily_patterns(conn, "AAPL", "2026-05-08")
        result = _build_daily_section(
            conn, "AAPL", "2026-05-08", sparkline_days=15,
            pattern_row_limit=5,
        )
        assert "patterns" in result
        assert len(result["patterns"]) >= 1
        # The existing patterns field rows must not have a days_ago key
        for pattern in result["patterns"]:
            assert "days_ago" not in pattern


# ---------------------------------------------------------------------------
# _fetch_stoch_sparkline tests
# ---------------------------------------------------------------------------

def _insert_stoch_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float | None, float | None]],
) -> None:
    """Insert multiple (date, stoch_k, stoch_d) rows into indicators_daily for a ticker."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, stoch_k, stoch_d) VALUES (?, ?, ?, ?)",
        [(ticker, date, stoch_k, stoch_d) for date, stoch_k, stoch_d in rows],
    )
    conn.commit()


class TestFetchStochSparkline:
    """Tests for _fetch_stoch_sparkline()."""

    def test_happy_path_multiple_rows(self, conn: sqlite3.Connection) -> None:
        """Multiple rows with stoch_k are returned in ascending date order."""
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-23", 25.0, 22.0),
            ("2026-04-24", 35.0, 28.0),
            ("2026-04-25", 55.0, 38.0),
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert len(result) == 3
        assert result[0]["date"] == "2026-04-23"
        assert result[1]["date"] == "2026-04-24"
        assert result[2]["date"] == "2026-04-25"

    def test_null_stoch_k_row_excluded(self, conn: sqlite3.Connection) -> None:
        """Rows where stoch_k IS NULL are excluded from the sparkline."""
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-23", 25.0, 22.0),
            ("2026-04-24", None, None),  # NULL stoch_k — must be excluded
            ("2026-04-25", 55.0, 38.0),
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        dates = [r["date"] for r in result]
        assert "2026-04-24" not in dates
        assert len(result) == 2

    def test_null_stoch_d_row_kept_with_none_value(self, conn: sqlite3.Connection) -> None:
        """
        Rows where stoch_d IS NULL but stoch_k is present are kept with stoch_d=None.

        Why stoch_d can be null while stoch_k is not:
        stoch_d is a 3-period SMA of stoch_k. For the first 2 rows after stoch_k becomes
        available (warm-up period), there are not yet 3 stoch_k values to average, so
        stoch_d remains NULL while stoch_k is already defined.
        """
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-23", 25.0, None),  # stoch_k present, stoch_d null (warm-up)
            ("2026-04-24", 35.0, 28.0),
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert len(result) == 2
        row_23 = next(r for r in result if r["date"] == "2026-04-23")
        assert row_23["stoch_k"] == 25.0
        assert row_23["stoch_d"] is None

    def test_empty_result_when_no_rows(self, conn: sqlite3.Connection) -> None:
        """Returns empty list when no rows exist."""
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert result == []

    def test_le_picked_date_bound_respected(self, conn: sqlite3.Connection) -> None:
        """Rows after picked_date are excluded."""
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-24", 30.0, 25.0),
            ("2026-04-25", 50.0, 35.0),
            ("2026-04-26", 70.0, 55.0),  # after picked_date — must be excluded
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        dates = [r["date"] for r in result]
        assert "2026-04-26" not in dates
        assert len(result) == 2

    def test_limit_respected(self, conn: sqlite3.Connection) -> None:
        """LIMIT parameter caps the number of returned rows."""
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-21", 10.0, 8.0),
            ("2026-04-22", 20.0, 15.0),
            ("2026-04-23", 30.0, 22.0),
            ("2026-04-24", 40.0, 30.0),
            ("2026-04-25", 50.0, 38.0),
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=3)
        assert len(result) == 3

    def test_ascending_order_verified(self, conn: sqlite3.Connection) -> None:
        """Returned rows are in ascending date order (oldest first)."""
        _insert_stoch_rows(conn, "AAPL", [
            ("2026-04-25", 55.0, 45.0),
            ("2026-04-23", 25.0, 20.0),
            ("2026-04-24", 35.0, 30.0),
        ])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        dates = [r["date"] for r in result]
        assert dates == sorted(dates)

    def test_row_shape_has_stoch_k_and_stoch_d(self, conn: sqlite3.Connection) -> None:
        """Each returned row has keys: date (str), stoch_k (float), stoch_d (float or None)."""
        _insert_stoch_rows(conn, "AAPL", [("2026-04-25", 55.5, 45.3)])
        result = _fetch_stoch_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert len(result) == 1
        row = result[0]
        assert set(row.keys()) == {"date", "stoch_k", "stoch_d"}
        assert isinstance(row["date"], str)
        assert isinstance(row["stoch_k"], float)
        assert isinstance(row["stoch_d"], float)


# ---------------------------------------------------------------------------
# _fetch_stoch_k_profile tests
# ---------------------------------------------------------------------------

def _insert_stoch_profile(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert a stoch_k profile row into indicator_profiles."""
    conn.execute(
        """INSERT OR REPLACE INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std)
           VALUES (?, 'stoch_k', 5.0, 20.0, 50.0, 80.0, 95.0, 50.0, 25.0)""",
        (ticker,),
    )
    conn.commit()


class TestFetchStochKProfile:
    """Tests for _fetch_stoch_k_profile()."""

    def test_row_present(self, conn: sqlite3.Connection) -> None:
        """Returns profile dict when stoch_k row exists in indicator_profiles."""
        _insert_stoch_profile(conn, "AAPL")
        profile = _fetch_stoch_k_profile(conn, "AAPL")
        assert profile is not None
        assert profile["p5"] == 5.0
        assert profile["p20"] == 20.0
        assert profile["p50"] == 50.0
        assert profile["p80"] == 80.0
        assert profile["p95"] == 95.0
        assert profile["mean"] == 50.0
        assert profile["std"] == 25.0

    def test_row_absent(self, conn: sqlite3.Connection) -> None:
        """Returns None when no stoch_k row exists for the ticker."""
        profile = _fetch_stoch_k_profile(conn, "AAPL")
        assert profile is None

    def test_table_missing_returns_none(self, tmp_path) -> None:
        """Returns None (swallows OperationalError) when indicator_profiles table is missing."""
        import sqlite3 as _sqlite3
        raw_conn = _sqlite3.connect(str(tmp_path / "no_profiles.db"))
        raw_conn.row_factory = _sqlite3.Row
        # Do NOT create any tables — indicator_profiles does not exist.
        profile = _fetch_stoch_k_profile(raw_conn, "AAPL")
        assert profile is None
        raw_conn.close()


# ---------------------------------------------------------------------------
# _fetch_adx_sparkline tests
# ---------------------------------------------------------------------------

def _insert_adx_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float | None]],
) -> None:
    """Insert multiple (date, adx) rows into indicators_daily for a ticker."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, adx) VALUES (?, ?, ?)",
        [(ticker, date, adx) for date, adx in rows],
    )
    conn.commit()


class TestFetchAdxSparkline:
    """Tests for _fetch_adx_sparkline()."""

    def test_happy_path_multiple_rows(self, conn: sqlite3.Connection) -> None:
        """Multiple rows with adx are returned in ascending date order."""
        _insert_adx_rows(conn, "AAPL", [
            ("2026-04-23", 18.5),
            ("2026-04-24", 22.1),
            ("2026-04-25", 27.3),
        ])
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert len(result) == 3
        assert result[0]["date"] == "2026-04-23"
        assert result[1]["date"] == "2026-04-24"
        assert result[2]["date"] == "2026-04-25"

    def test_row_shape_has_date_and_adx_keys(self, conn: sqlite3.Connection) -> None:
        """Each returned row has exactly keys: date (str), adx (float)."""
        _insert_adx_rows(conn, "AAPL", [("2026-04-25", 30.0)])
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert len(result) == 1
        row = result[0]
        assert set(row.keys()) == {"date", "adx"}
        assert isinstance(row["date"], str)
        assert isinstance(row["adx"], float)

    def test_null_adx_rows_excluded(self, conn: sqlite3.Connection) -> None:
        """Rows where adx IS NULL are excluded from the sparkline."""
        _insert_adx_rows(conn, "AAPL", [
            ("2026-04-23", 18.5),
            ("2026-04-24", None),   # NULL adx — must be excluded
            ("2026-04-25", 27.3),
        ])
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        dates = [r["date"] for r in result]
        assert "2026-04-24" not in dates
        assert len(result) == 2

    def test_le_picked_date_bound_respected(self, conn: sqlite3.Connection) -> None:
        """Rows after picked_date are excluded."""
        _insert_adx_rows(conn, "AAPL", [
            ("2026-04-24", 20.0),
            ("2026-04-25", 25.0),
            ("2026-04-26", 30.0),  # after picked_date — must be excluded
        ])
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        dates = [r["date"] for r in result]
        assert "2026-04-26" not in dates
        assert len(result) == 2

    def test_limit_respected(self, conn: sqlite3.Connection) -> None:
        """LIMIT parameter caps the number of returned rows."""
        _insert_adx_rows(conn, "AAPL", [
            ("2026-04-21", 15.0),
            ("2026-04-22", 18.0),
            ("2026-04-23", 22.0),
            ("2026-04-24", 26.0),
            ("2026-04-25", 30.0),
        ])
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=2)
        assert len(result) == 2

    def test_empty_result_when_no_rows(self, conn: sqlite3.Connection) -> None:
        """Returns empty list when no rows exist for the ticker."""
        result = _fetch_adx_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert result == []


# ---------------------------------------------------------------------------
# CCI(20) sparkline and profile tests
# ---------------------------------------------------------------------------

def _insert_cci_rows(
    conn: sqlite3.Connection,
    ticker: str,
    rows: list[tuple[str, float]],
) -> None:
    """Insert multiple (date, cci_20) rows into indicators_daily for a ticker."""
    conn.executemany(
        "INSERT OR REPLACE INTO indicators_daily(ticker, date, cci_20) VALUES (?, ?, ?)",
        [(ticker, date, cci) for date, cci in rows],
    )
    conn.commit()


class TestFetchCciSparkline:
    """Tests for _fetch_cci_sparkline()."""

    def test_returns_expected_window(self, conn: sqlite3.Connection) -> None:
        """Requesting 100 days returns up to 100 rows ascending by date."""
        from datetime import date, timedelta

        start = date(2025, 10, 1)
        rows = [
            ((start + timedelta(days=i)).isoformat(), float(10 * (i % 30) - 150))
            for i in range(130)
        ]
        _insert_cci_rows(conn, "AAPL", rows)
        # Pick a date far enough in to have 100 rows available.
        result = _fetch_cci_sparkline(conn, "AAPL", "2026-04-25", num_days=100)
        assert len(result) == 100
        # Rows are in ascending date order.
        dates = [r["date"] for r in result]
        assert dates == sorted(dates)

    def test_returns_cci_key(self, conn: sqlite3.Connection) -> None:
        """Each returned dict has a 'cci' key (not 'value' or 'cci_20')."""
        _insert_cci_rows(conn, "AAPL", [("2026-04-24", 55.5)])
        result = _fetch_cci_sparkline(conn, "AAPL", "2026-04-24", num_days=10)
        assert len(result) == 1
        assert "cci" in result[0]
        assert abs(result[0]["cci"] - 55.5) < 0.001

    def test_excludes_null_cci_rows(self, conn: sqlite3.Connection) -> None:
        """Rows where cci_20 IS NULL are excluded from results."""
        conn.execute(
            "INSERT OR REPLACE INTO indicators_daily(ticker, date, cci_20) VALUES (?, ?, NULL)",
            ("AAPL", "2026-04-23"),
        )
        conn.commit()
        result = _fetch_cci_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert result == []

    def test_empty_result_when_no_rows(self, conn: sqlite3.Connection) -> None:
        """Returns empty list when no cci_20 rows exist."""
        result = _fetch_cci_sparkline(conn, "AAPL", "2026-04-25", num_days=10)
        assert result == []


class TestFetchCciProfile:
    """Tests for _fetch_cci_profile()."""

    def _insert_cci_profile(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        regime: str = "ranging",
    ) -> None:
        """Insert a cci_20 row in indicator_profiles."""
        conn.execute(
            "INSERT OR REPLACE INTO indicator_profiles("
            "  ticker, indicator, p5, p20, p50, p80, p95, mean, std"
            ") VALUES (?, 'cci_20', ?, ?, ?, ?, ?, ?, ?)",
            (ticker, -150.0, -60.0, 0.0, 60.0, 150.0, 0.0, 80.0),
        )
        conn.commit()

    def test_returns_persisted_percentiles(self, conn: sqlite3.Connection) -> None:
        """Returns dict with p5/p20/p50/p80/p95/mean/std when profile row exists."""
        self._insert_cci_profile(conn, "AAPL", "ranging")
        result = _fetch_cci_profile(conn, "AAPL")
        assert result is not None
        assert abs(result["p5"] - (-150.0)) < 0.001
        assert abs(result["p20"] - (-60.0)) < 0.001
        assert abs(result["p50"] - 0.0) < 0.001
        assert abs(result["p80"] - 60.0) < 0.001
        assert abs(result["p95"] - 150.0) < 0.001
        assert "mean" in result
        assert "std" in result

    def test_returns_none_when_missing(self, conn: sqlite3.Connection) -> None:
        """Returns None when no cci_20 profile row exists for the ticker."""
        result = _fetch_cci_profile(conn, "AAPL")
        assert result is None

    def test_returns_none_for_different_ticker(self, conn: sqlite3.Connection) -> None:
        """Returns None when profile exists for a different ticker, not AAPL."""
        self._insert_cci_profile(conn, "MSFT", "ranging")
        result = _fetch_cci_profile(conn, "AAPL")
        assert result is None


# ---------------------------------------------------------------------------
# DailySection new fields: raw_daily_score, sector_etf_score, sector_etf
# ---------------------------------------------------------------------------

def _insert_daily_score_with_etf_columns(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    raw_daily_score: float | None,
    sector_etf_score: float | None,
) -> None:
    """
    Insert a scores_daily row that includes the two new columns.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        date: Date string (YYYY-MM-DD).
        raw_daily_score: Pre-sector-adjustment daily score, or None.
        sector_etf_score: Sector ETF composite score, or None.
    """
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, daily_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score,
            raw_daily_score, sector_etf_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ticker, date, "BULLISH", 72.5, 55.0, 42.0, "trending",
            40.0, 30.0, 20.0, -10.0, 25.0, 15.0, 5.0, 8.0, -3.0, 1.42,
            raw_daily_score, sector_etf_score,
        ),
    )
    conn.commit()


def _insert_ticker_with_etf(
    conn: sqlite3.Connection,
    ticker: str,
    sector_etf: str | None,
) -> None:
    """
    Insert a tickers row with the given sector_etf.

    Parameters:
        conn: Open SQLite connection.
        ticker: Ticker symbol.
        sector_etf: Sector ETF symbol, or None.
    """
    conn.execute(
        "INSERT OR REPLACE INTO tickers(symbol, name, active, sector_etf) VALUES (?, ?, 1, ?)",
        (ticker, f"{ticker} Corp", sector_etf),
    )
    conn.commit()


class TestDailySectionNewEtfFields:
    """
    Tests that _build_daily_section surfaces raw_daily_score, sector_etf_score,
    and sector_etf (joined from tickers) on the daily snapshot dict.
    """

    def test_raw_daily_score_is_returned(self, conn: sqlite3.Connection) -> None:
        """
        raw_daily_score from scores_daily must appear on the daily section dict
        when the column is populated.
        """
        _insert_ticker_with_etf(conn, "AAPL", "XLK")
        _insert_daily_score_with_etf_columns(conn, "AAPL", "2026-05-01", 39.5, 58.0)

        result = _build_daily_section(conn, "AAPL", "2026-05-01", 15)

        assert result["data_available"] is True
        assert result.get("raw_daily_score") == 39.5, (
            f"Expected raw_daily_score=39.5, got {result.get('raw_daily_score')}"
        )

    def test_sector_etf_score_is_returned(self, conn: sqlite3.Connection) -> None:
        """
        sector_etf_score from scores_daily must appear on the daily section dict
        when the column is populated.
        """
        _insert_ticker_with_etf(conn, "AAPL", "XLK")
        _insert_daily_score_with_etf_columns(conn, "AAPL", "2026-05-01", 39.5, 58.0)

        result = _build_daily_section(conn, "AAPL", "2026-05-01", 15)

        assert result.get("sector_etf_score") == 58.0, (
            f"Expected sector_etf_score=58.0, got {result.get('sector_etf_score')}"
        )

    def test_sector_etf_is_joined_from_tickers(self, conn: sqlite3.Connection) -> None:
        """
        sector_etf must be joined from the tickers table and returned on the
        daily section dict.
        """
        _insert_ticker_with_etf(conn, "AAPL", "XLK")
        _insert_daily_score_with_etf_columns(conn, "AAPL", "2026-05-01", 39.5, 58.0)

        result = _build_daily_section(conn, "AAPL", "2026-05-01", 15)

        assert result.get("sector_etf") == "XLK", (
            f"Expected sector_etf='XLK', got {result.get('sector_etf')}"
        )

    def test_sector_etf_is_null_when_not_mapped(self, conn: sqlite3.Connection) -> None:
        """
        When the ticker row has sector_etf=None, the daily section must return
        sector_etf=None (not absent, not an error).
        """
        _insert_ticker_with_etf(conn, "AAPL", None)
        _insert_daily_score_with_etf_columns(conn, "AAPL", "2026-05-01", 40.0, None)

        result = _build_daily_section(conn, "AAPL", "2026-05-01", 15)

        assert result.get("sector_etf") is None
        assert result.get("sector_etf_score") is None

    def test_sector_etf_null_when_no_tickers_row(self, conn: sqlite3.Connection) -> None:
        """
        When there is no matching row in the tickers table (LEFT JOIN), sector_etf
        must be None rather than raising.
        """
        # Insert score row without inserting a tickers row.
        conn.execute(
            """INSERT OR REPLACE INTO scores_daily(
                ticker, date, signal, confidence, final_score, daily_score, regime,
                raw_daily_score, sector_etf_score
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            ("AAPL", "2026-05-01", "NEUTRAL", 50.0, 10.0, 12.0, "ranging", 11.0, None),
        )
        conn.commit()

        result = _build_daily_section(conn, "AAPL", "2026-05-01", 15)

        assert result["data_available"] is True
        assert result.get("sector_etf") is None


# ---------------------------------------------------------------------------
# fetch_tickers_list
# ---------------------------------------------------------------------------


class TestFetchTickersList:
    """Tests for fetch_tickers_list — the Tickers listing page query."""

    def _insert_ticker_with_meta(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        name: str,
        sector: Optional[str],
        market_cap: Optional[float],
        active: bool,
    ) -> None:
        """Insert a tickers row with sector + market_cap metadata."""
        conn.execute(
            "INSERT OR REPLACE INTO tickers(symbol, name, sector, market_cap, active) "
            "VALUES (?, ?, ?, ?, ?)",
            (symbol, name, sector, market_cap, active),
        )
        conn.commit()

    def _insert_score(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        date: str,
        signal: str,
        final_score: float,
    ) -> None:
        """Insert a scores_daily row with the fields fetch_tickers_list reads."""
        conn.execute(
            """INSERT OR REPLACE INTO scores_daily(
                ticker, date, signal, confidence, final_score, regime,
                daily_score, weekly_score, monthly_score
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (ticker, date, signal, 50.0, final_score, "trending",
             40.0, 30.0, 20.0),
        )
        conn.commit()

    def _insert_close(
        self, conn: sqlite3.Connection, ticker: str, date: str, close: float
    ) -> None:
        """Insert a single ohlcv_daily row used as the latest close source."""
        conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ticker, date, close, close, close, close, 1_000_000),
        )
        conn.commit()

    def _insert_fundamental(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        report_date: str,
        period: str,
        pe_ratio: Optional[float],
        fetched_at: Optional[str] = None,
    ) -> None:
        """Insert a fundamentals row with the fields fetch_tickers_list reads."""
        conn.execute(
            "INSERT OR REPLACE INTO fundamentals(ticker, report_date, period, pe_ratio, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (ticker, report_date, period, pe_ratio, fetched_at),
        )
        conn.commit()

    def test_returns_one_row_per_active_scored_ticker(
        self, conn: sqlite3.Connection
    ) -> None:
        """Two active tickers with one score row each return two rows."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple Inc", "Tech", 3e12, True)
        self._insert_ticker_with_meta(conn, "MSFT", "Microsoft", "Tech", 2e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)
        self._insert_score(conn, "MSFT", "2026-05-01", "NEUTRAL", 10.0)

        rows = fetch_tickers_list(conn)

        assert len(rows) == 2
        symbols = [row["symbol"] for row in rows]
        assert symbols == ["AAPL", "MSFT"]
        aapl = rows[0]
        assert aapl["name"] == "Apple Inc"
        assert aapl["sector"] == "Tech"
        assert aapl["market_cap"] == 3e12
        assert aapl["signal"] == "BULLISH"
        assert aapl["final_score"] == 55.0

    def test_skips_inactive_tickers(self, conn: sqlite3.Connection) -> None:
        """active = 0 tickers must not appear even if they have score rows."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_ticker_with_meta(conn, "DEAD", "Dead Corp", "Tech", 1e9, False)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)
        self._insert_score(conn, "DEAD", "2026-05-01", "BEARISH", -55.0)

        rows = fetch_tickers_list(conn)

        assert [row["symbol"] for row in rows] == ["AAPL"]

    def test_skips_tickers_with_no_scores(self, conn: sqlite3.Connection) -> None:
        """Active tickers with no scores_daily row are excluded by the INNER JOIN."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_ticker_with_meta(conn, "NEW", "Fresh Corp", "Tech", 1e9, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)
        # NEW has no scores_daily row.

        rows = fetch_tickers_list(conn)

        assert [row["symbol"] for row in rows] == ["AAPL"]

    def test_picks_latest_score_row_per_ticker(
        self, conn: sqlite3.Connection
    ) -> None:
        """When multiple scores_daily rows exist, the latest by date wins."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-04-01", "BEARISH", -30.0)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)

        rows = fetch_tickers_list(conn)

        assert len(rows) == 1
        assert rows[0]["signal"] == "BULLISH"
        assert rows[0]["final_score"] == 55.0

    def test_picks_latest_close_per_ticker(self, conn: sqlite3.Connection) -> None:
        """The latest ohlcv_daily.close is joined as price."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)
        self._insert_close(conn, "AAPL", "2026-04-30", 200.0)
        self._insert_close(conn, "AAPL", "2026-05-01", 248.61)

        rows = fetch_tickers_list(conn)

        assert rows[0]["price"] == 248.61

    def test_handles_missing_close(self, conn: sqlite3.Connection) -> None:
        """price is None when no ohlcv_daily row exists."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)

        rows = fetch_tickers_list(conn)

        assert rows[0]["price"] is None

    def test_handles_missing_fundamentals(self, conn: sqlite3.Connection) -> None:
        """pe_ratio is None when no fundamentals row exists for the ticker."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)

        rows = fetch_tickers_list(conn)

        assert rows[0]["pe_ratio"] is None

    def test_picks_deterministic_fundamentals_row(
        self, conn: sqlite3.Connection
    ) -> None:
        """When multiple fundamentals rows share a report_date with different
        periods, the choice is deterministic (latest report_date → latest
        fetched_at → latest period). Q4 sorts after Q1 lexicographically,
        so Q4's pe_ratio wins when report_date and fetched_at tie."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)
        self._insert_fundamental(conn, "AAPL", "2026-03-31", "Q1", 10.0,
                                 "2026-04-01T00:00:00Z")
        self._insert_fundamental(conn, "AAPL", "2026-03-31", "Q4", 20.0,
                                 "2026-04-01T00:00:00Z")
        # Latest report_date — should beat both above regardless of period.
        self._insert_fundamental(conn, "AAPL", "2026-06-30", "Q2", 30.0,
                                 "2026-07-01T00:00:00Z")

        rows = fetch_tickers_list(conn)

        assert rows[0]["pe_ratio"] == 30.0

    def test_includes_all_expected_keys(self, conn: sqlite3.Connection) -> None:
        """Each returned dict must carry the documented snake_case key set."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", 3e12, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)

        rows = fetch_tickers_list(conn)

        expected_keys = {
            "symbol", "name", "sector", "market_cap", "price",
            "signal", "confidence", "final_score", "regime",
            "daily_score", "weekly_score", "monthly_score",
            "pe_ratio",
        }
        assert set(rows[0].keys()) == expected_keys

    def test_handles_null_market_cap(self, conn: sqlite3.Connection) -> None:
        """tickers.market_cap may be NULL — surfaces as None, no errors."""
        self._insert_ticker_with_meta(conn, "AAPL", "Apple", "Tech", None, True)
        self._insert_score(conn, "AAPL", "2026-05-01", "BULLISH", 55.0)

        rows = fetch_tickers_list(conn)

        assert rows[0]["market_cap"] is None

    def test_returns_empty_list_when_no_active_tickers(
        self, conn: sqlite3.Connection
    ) -> None:
        """Empty schema returns an empty list rather than raising."""
        rows = fetch_tickers_list(conn)
        assert rows == []
