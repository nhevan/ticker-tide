"""
Tests for src/scorer/persistence.py — closed-period scores_weekly/monthly writers.

Covers:
  - Closed-period gate (week + month) — including T1's Sunday vs Monday boundary
  - Idempotency (INSERT OR REPLACE on composite PK)
  - Fundamental + macro inheritance from scores_daily (happy path, no row,
    upper-bound enforcement, T2 prior-week staleness tolerance)
  - T4: month_end calendar correctness for 28/30/31-day months
  - persist_indicator_scores_daily/weekly/monthly: happy path, idempotency,
    None stored as SQL NULL, mixed float+None round-trips correctly
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.persistence import (
    _inherit_fundamental_macro,
    _last_day_of_month,
    persist_indicator_scores_daily,
    persist_indicator_scores_monthly,
    persist_indicator_scores_weekly,
    persist_monthly_score_row,
    persist_weekly_score_row,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_indicators_weekly_row(
    conn: sqlite3.Connection, ticker: str, week_start: str
) -> None:
    """Insert a minimal indicators_weekly row so persist_weekly can resolve week_start."""
    conn.execute(
        "INSERT OR REPLACE INTO indicators_weekly "
        "(ticker, week_start, ema_9, ema_21, ema_50) VALUES (?, ?, ?, ?, ?)",
        (ticker, week_start, 100.0, 99.5, 99.0),
    )
    conn.commit()


def _insert_indicators_monthly_row(
    conn: sqlite3.Connection, ticker: str, month_start: str
) -> None:
    """Insert a minimal indicators_monthly row."""
    conn.execute(
        "INSERT OR REPLACE INTO indicators_monthly "
        "(ticker, month_start, ema_9, ema_21, ema_50) VALUES (?, ?, ?, ?, ?)",
        (ticker, month_start, 100.0, 99.5, 99.0),
    )
    conn.commit()


def _insert_scores_daily_row(
    conn: sqlite3.Connection,
    ticker: str,
    dt: str,
    fundamental: float | None = None,
    macro: float | None = None,
) -> None:
    """Insert a minimal scores_daily row with fundamental + macro values."""
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily "
        "(ticker, date, fundamental_score, macro_score) VALUES (?, ?, ?, ?)",
        (ticker, dt, fundamental, macro),
    )
    conn.commit()


def _make_breakdown(
    composite: float = 42.0,
    trend: float = 30.0,
    momentum: float = 25.0,
    volume: float = 10.0,
    volatility: float = 5.0,
    candlestick: float | None = None,
    structural: float | None = None,
) -> dict:
    """Return a breakdown dict shaped like compute_weekly_score_breakdown's output."""
    return {
        "composite_score": composite,
        "trend_score": trend,
        "momentum_score": momentum,
        "volume_score": volume,
        "volatility_score": volatility,
        "candlestick_score": candlestick,
        "structural_score": structural,
    }


# ---------------------------------------------------------------------------
# Closed-period gate (weekly) — T1
# ---------------------------------------------------------------------------

class TestWeeklyClosedPeriodGate:
    def test_sunday_of_to_close_week_skips_persist(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """T1a: Sunday is still in progress → persist_weekly_score_row returns False."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={"news": True}, key_signals=["x"],
            scoring_date="2026-04-26",
        )
        assert wrote is False
        rows = db_connection.execute(
            "SELECT COUNT(*) FROM scores_weekly WHERE ticker='AAPL'"
        ).fetchone()
        assert rows[0] == 0

    def test_following_monday_persists(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """T1b: scoring on the NEXT Monday → row written."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=58.4), regime="trending",
            data_completeness={"news": True}, key_signals=["x"],
            scoring_date="2026-04-27",
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT week_start, composite_score, regime FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row is not None
        assert row["week_start"] == "2026-04-20"
        assert row["composite_score"] == 58.4
        assert row["regime"] == "trending"

    def test_no_indicators_weekly_row_returns_false(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """When no indicators_weekly row exists for the ticker, persist no-ops."""
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-27",
        )
        assert wrote is False

    def test_in_progress_week_falls_back_to_prior_closed_week(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Regression: scoring mid-week with both current (open) and prior
        (closed) weekly indicator rows must persist the prior closed week,
        not skip silently. Without the fallback, live daily runs and
        historical weekly mode (where scoring_date == week_start) would
        never produce any scores_weekly rows."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-13")  # closed
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")  # in-progress
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=42.0), regime="ranging",
            data_completeness={"news": True}, key_signals=["x"],
            scoring_date="2026-04-22",  # Wednesday of the 2026-04-20 week
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT week_start, composite_score FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["week_start"] == "2026-04-13"
        assert row["composite_score"] == 42.0

    def test_historical_mode_scoring_date_equals_week_start(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Regression: historical weekly scoring iterates week_starts and
        calls score_ticker(scoring_date=week_start). The persist function
        must fall back to the prior closed week so the row gets written."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-13")
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=15.0), regime="ranging",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-20",  # equals week_start of in-progress week
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT week_start FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["week_start"] == "2026-04-13"


# ---------------------------------------------------------------------------
# Closed-period gate (monthly)
# ---------------------------------------------------------------------------

class TestMonthlyClosedPeriodGate:
    def test_same_month_in_progress_skips(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """scoring_date in the same month as month_start → no persist."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-04-01")
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-22",
        )
        assert wrote is False

    def test_later_month_closes_period(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """scoring in a later month → row persisted."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=72.0), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-22",
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT month_start, composite_score FROM scores_monthly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row is not None
        assert row["month_start"] == "2026-03-01"
        assert row["composite_score"] == 72.0

    def test_in_progress_month_falls_back_to_prior_closed_month(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Regression: scoring mid-month with both current (open) and prior
        (closed) monthly indicator rows must persist the prior closed
        month, not skip silently."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")  # closed
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-04-01")  # in-progress
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=33.3), regime="ranging",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-22",
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT month_start, composite_score FROM scores_monthly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["month_start"] == "2026-03-01"
        assert row["composite_score"] == 33.3

    def test_historical_mode_scoring_date_equals_month_start(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Regression: historical monthly mode iterates month_starts and
        calls score_ticker(scoring_date=month_start). Fallback persists
        the prior closed month."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-04-01")
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=10.0), regime="ranging",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-01",  # equals month_start of in-progress month
        )
        assert wrote is True
        row = db_connection.execute(
            "SELECT month_start FROM scores_monthly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["month_start"] == "2026-03-01"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_weekly_persist_is_idempotent(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Re-calling persist_weekly_score_row leaves a single row pinned at the latest values."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        for composite in (42.0, 88.0):
            persist_weekly_score_row(
                db_connection, "AAPL",
                breakdown=_make_breakdown(composite=composite), regime="trending",
                data_completeness={"news": True}, key_signals=["x"],
                scoring_date="2026-04-27",
            )
        rows = db_connection.execute(
            "SELECT composite_score FROM scores_weekly WHERE ticker=?", ("AAPL",)
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["composite_score"] == 88.0

    def test_monthly_persist_is_idempotent(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Same idempotency contract for the monthly persist."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        for composite in (10.0, 99.0):
            persist_monthly_score_row(
                db_connection, "AAPL",
                breakdown=_make_breakdown(composite=composite), regime="trending",
                data_completeness={}, key_signals=[],
                scoring_date="2026-04-22",
            )
        rows = db_connection.execute(
            "SELECT composite_score FROM scores_monthly WHERE ticker=?", ("AAPL",)
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["composite_score"] == 99.0


# ---------------------------------------------------------------------------
# Fundamental + macro inheritance
# ---------------------------------------------------------------------------

class TestInheritance:
    def test_inherit_happy_path_weekly(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Most recent in-period scores_daily row's fundamental + macro carried into scores_weekly."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        # Tuesday inside the week.
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-04-21", fundamental=42.0, macro=10.0
        )
        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-27",
        )
        row = db_connection.execute(
            "SELECT fundamental_score, macro_score FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["fundamental_score"] == 42.0
        assert row["macro_score"] == 10.0

    def test_inherit_no_row_persists_null(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """No scores_daily row → fundamental + macro NULL on the persisted row."""
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-27",
        )
        row = db_connection.execute(
            "SELECT fundamental_score, macro_score FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["fundamental_score"] is None
        assert row["macro_score"] is None

    def test_inherit_upper_bound_is_friday(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        Upper bound for weekly inheritance is week_start + 4 days (Friday).

        99.0 on Sat 2026-04-25 — wait, 2026-04-25 is the Saturday for week_start
        2026-04-20.  Use 2026-04-24 (Fri) as in-bound and 2026-04-26 (Sun) as
        out-of-bound to keep the assertion crisp.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        # In-bound (Friday) — should be selected.
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-04-24", fundamental=42.0, macro=10.0
        )
        # Out-of-bound (Sunday after the period_end Friday).
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-04-26", fundamental=99.0, macro=88.0
        )
        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-27",
        )
        row = db_connection.execute(
            "SELECT fundamental_score, macro_score FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["fundamental_score"] == 42.0
        assert row["macro_score"] == 10.0

    def test_t2_prior_week_inheritance_staleness_tolerance(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        T2: When no in-period scores_daily exists, the helper returns the most
        recent available row (prior week) — documented staleness tolerance.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-21")
        # Prior Friday — staleness-tolerated row.
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-04-18", fundamental=42.0, macro=10.0
        )
        # No rows in 2026-04-21..2026-04-25.
        period_end = "2026-04-25"
        fund, macro = _inherit_fundamental_macro(db_connection, "AAPL", period_end)
        assert fund == 42.0
        assert macro == 10.0


# ---------------------------------------------------------------------------
# T4: month_end calendar correctness
# ---------------------------------------------------------------------------

class TestMonthEndCalendar:
    @pytest.mark.parametrize(
        "month_start, expected_end",
        [
            ("2026-02-01", "2026-02-28"),  # 28 days
            ("2024-02-01", "2024-02-29"),  # 29 days (leap year sanity check)
            ("2026-04-01", "2026-04-30"),  # 30 days
            ("2026-12-01", "2026-12-31"),  # 31 days
        ],
    )
    def test_last_day_of_month(self, month_start: str, expected_end: str) -> None:
        """
        T4: ``_last_day_of_month`` must match ``calendar.monthrange`` for
        Feb (28/29), 30-day, and 31-day months.
        """
        assert _last_day_of_month(month_start) == expected_end

    def test_inherit_monthly_uses_last_day_of_month(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Monthly persist should pick up scores_daily up to the last day of month."""
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-02-01")
        # Last day of Feb 2026 is 2026-02-28.
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-02-28", fundamental=11.0, macro=22.0
        )
        # Out-of-bound (next day, March 1).
        _insert_scores_daily_row(
            db_connection, "AAPL", "2026-03-01", fundamental=99.0, macro=99.0
        )
        persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={}, key_signals=[],
            scoring_date="2026-04-22",  # April → March is closed.
        )
        # Recall: persist resolves the latest indicators_monthly.month_start <= scoring_date.
        # We only inserted month_start=2026-02-01, so that's what gets persisted.
        row = db_connection.execute(
            "SELECT fundamental_score, macro_score FROM scores_monthly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert row["fundamental_score"] == 11.0
        assert row["macro_score"] == 22.0


# ---------------------------------------------------------------------------
# Data shape persistence
# ---------------------------------------------------------------------------

class TestDataShapeRoundTrip:
    def test_data_completeness_dict_serialised_as_json_text(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Dict input round-trips as a JSON string in the TEXT column."""
        import json
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(), regime="trending",
            data_completeness={"news": True, "fundamentals": False},
            key_signals=["EMA crossover"],
            scoring_date="2026-04-27",
        )
        row = db_connection.execute(
            "SELECT data_completeness, key_signals FROM scores_weekly WHERE ticker=?",
            ("AAPL",),
        ).fetchone()
        assert isinstance(row["data_completeness"], str)
        parsed = json.loads(row["data_completeness"])
        assert parsed == {"news": True, "fundamentals": False}
        assert json.loads(row["key_signals"]) == ["EMA crossover"]


# ---------------------------------------------------------------------------
# Sidecar indicator score persistence helpers
# ---------------------------------------------------------------------------

def _create_sidecar_tables(conn: sqlite3.Connection) -> None:
    """Create the three sidecar tables needed by persist_indicator_scores_*."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS indicator_scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            score REAL,
            PRIMARY KEY (ticker, date, indicator_name)
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS indicator_scores_weekly (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            score REAL,
            PRIMARY KEY (ticker, week_start, indicator_name)
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS indicator_scores_monthly (
            ticker TEXT NOT NULL,
            month_start TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            score REAL,
            PRIMARY KEY (ticker, month_start, indicator_name)
        )"""
    )
    conn.commit()


class TestPersistIndicatorScoresDaily:
    """Tests for persist_indicator_scores_daily."""

    def test_happy_path_writes_rows(self, db_connection: sqlite3.Connection) -> None:
        """Writing a known dict inserts one row per indicator."""
        _create_sidecar_tables(db_connection)
        scores: dict[str, float | None] = {
            "rsi_14": 45.5,
            "macd_histogram": -20.0,
            "bb_pctb": 0.0,
        }
        persist_indicator_scores_daily(db_connection, "AAPL", "2026-04-22", scores)

        rows = db_connection.execute(
            "SELECT indicator_name, score FROM indicator_scores_daily "
            "WHERE ticker = ? AND date = ? ORDER BY indicator_name",
            ("AAPL", "2026-04-22"),
        ).fetchall()
        assert len(rows) == 3
        result = {row["indicator_name"]: row["score"] for row in rows}
        assert result["rsi_14"] == pytest.approx(45.5)
        assert result["macd_histogram"] == pytest.approx(-20.0)
        assert result["bb_pctb"] == pytest.approx(0.0)

    def test_none_value_stored_as_sql_null(self, db_connection: sqlite3.Connection) -> None:
        """A None score must be persisted as SQL NULL, not the string 'None'."""
        _create_sidecar_tables(db_connection)
        persist_indicator_scores_daily(
            db_connection, "AAPL", "2026-04-22", {"adx": None}
        )
        row = db_connection.execute(
            "SELECT score FROM indicator_scores_daily "
            "WHERE ticker = ? AND date = ? AND indicator_name = ?",
            ("AAPL", "2026-04-22", "adx"),
        ).fetchone()
        assert row is not None
        assert row["score"] is None, "None score must be stored as SQL NULL"

    def test_idempotency_same_row_count(self, db_connection: sqlite3.Connection) -> None:
        """Writing the same dict twice yields the same row count (INSERT OR REPLACE)."""
        _create_sidecar_tables(db_connection)
        scores = {"rsi_14": 50.0, "adx": None}
        persist_indicator_scores_daily(db_connection, "AAPL", "2026-04-22", scores)
        persist_indicator_scores_daily(db_connection, "AAPL", "2026-04-22", scores)
        count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_daily WHERE ticker = ? AND date = ?",
            ("AAPL", "2026-04-22"),
        ).fetchone()[0]
        assert count == 2

    def test_second_write_updates_value(self, db_connection: sqlite3.Connection) -> None:
        """A second write with a different score updates the existing row."""
        _create_sidecar_tables(db_connection)
        persist_indicator_scores_daily(db_connection, "AAPL", "2026-04-22", {"rsi_14": 30.0})
        persist_indicator_scores_daily(db_connection, "AAPL", "2026-04-22", {"rsi_14": 75.0})
        row = db_connection.execute(
            "SELECT score FROM indicator_scores_daily "
            "WHERE ticker = ? AND date = ? AND indicator_name = ?",
            ("AAPL", "2026-04-22", "rsi_14"),
        ).fetchone()
        assert row["score"] == pytest.approx(75.0)


class TestPersistIndicatorScoresWeekly:
    """Tests for persist_indicator_scores_weekly."""

    def test_happy_path_writes_rows(self, db_connection: sqlite3.Connection) -> None:
        """Writing a known dict inserts one row per indicator."""
        _create_sidecar_tables(db_connection)
        scores: dict[str, float | None] = {"rsi_14": 62.0, "ema_alignment": -30.0}
        persist_indicator_scores_weekly(db_connection, "AAPL", "2026-04-20", scores)

        rows = db_connection.execute(
            "SELECT indicator_name, score FROM indicator_scores_weekly "
            "WHERE ticker = ? AND week_start = ? ORDER BY indicator_name",
            ("AAPL", "2026-04-20"),
        ).fetchall()
        assert len(rows) == 2
        result = {row["indicator_name"]: row["score"] for row in rows}
        assert result["rsi_14"] == pytest.approx(62.0)
        assert result["ema_alignment"] == pytest.approx(-30.0)

    def test_none_stored_as_sql_null(self, db_connection: sqlite3.Connection) -> None:
        """None scores become SQL NULL in indicator_scores_weekly."""
        _create_sidecar_tables(db_connection)
        persist_indicator_scores_weekly(
            db_connection, "AAPL", "2026-04-20", {"obv": None}
        )
        row = db_connection.execute(
            "SELECT score FROM indicator_scores_weekly "
            "WHERE ticker = ? AND week_start = ? AND indicator_name = ?",
            ("AAPL", "2026-04-20", "obv"),
        ).fetchone()
        assert row is not None
        assert row["score"] is None

    def test_idempotency(self, db_connection: sqlite3.Connection) -> None:
        """Writing the same weekly dict twice yields the same row count."""
        _create_sidecar_tables(db_connection)
        scores = {"rsi_14": 55.0}
        persist_indicator_scores_weekly(db_connection, "AAPL", "2026-04-20", scores)
        persist_indicator_scores_weekly(db_connection, "AAPL", "2026-04-20", scores)
        count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_weekly "
            "WHERE ticker = ? AND week_start = ?",
            ("AAPL", "2026-04-20"),
        ).fetchone()[0]
        assert count == 1


class TestPersistIndicatorScoresMonthly:
    """Tests for persist_indicator_scores_monthly."""

    def test_happy_path_writes_rows(self, db_connection: sqlite3.Connection) -> None:
        """Writing a known dict inserts one row per indicator."""
        _create_sidecar_tables(db_connection)
        scores: dict[str, float | None] = {"cmf_20": 15.0, "bb_pctb": None}
        persist_indicator_scores_monthly(db_connection, "AAPL", "2026-04-01", scores)

        rows = db_connection.execute(
            "SELECT indicator_name, score FROM indicator_scores_monthly "
            "WHERE ticker = ? AND month_start = ? ORDER BY indicator_name",
            ("AAPL", "2026-04-01"),
        ).fetchall()
        assert len(rows) == 2
        result = {row["indicator_name"]: row["score"] for row in rows}
        assert result["cmf_20"] == pytest.approx(15.0)
        assert result["bb_pctb"] is None

    def test_idempotency(self, db_connection: sqlite3.Connection) -> None:
        """Writing the same monthly dict twice yields the same row count."""
        _create_sidecar_tables(db_connection)
        scores = {"adx": 40.0, "rsi_14": None}
        persist_indicator_scores_monthly(db_connection, "AAPL", "2026-04-01", scores)
        persist_indicator_scores_monthly(db_connection, "AAPL", "2026-04-01", scores)
        count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_monthly "
            "WHERE ticker = ? AND month_start = ?",
            ("AAPL", "2026-04-01"),
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# indicator_scores kwarg on persist_weekly_score_row / persist_monthly_score_row
# ---------------------------------------------------------------------------

class TestPersistWeeklyWithIndicatorScores:
    """
    Tests for the optional indicator_scores kwarg added to persist_weekly_score_row.

    When indicator_scores is provided:
      - Both scores_weekly and indicator_scores_weekly rows are written
        using the same resolved week_start (atomicity via a single function call).
    When indicator_scores is None:
      - Only scores_weekly is written; indicator_scores_weekly stays empty.
    """

    def test_indicator_scores_kwarg_writes_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        Passing indicator_scores to persist_weekly_score_row writes both
        scores_weekly and indicator_scores_weekly keyed to the same week_start.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        indicator_scores: dict[str, float | None] = {"rsi_14": 25.0, "macd_line": None}
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=55.0),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
            indicator_scores=indicator_scores,
        )
        assert wrote is True

        # scores_weekly row exists.
        sw_row = db_connection.execute(
            "SELECT week_start, composite_score FROM scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert sw_row is not None
        assert sw_row["week_start"] == "2026-04-20"
        assert sw_row["composite_score"] == pytest.approx(55.0)

        # indicator_scores_weekly rows exist, keyed to the same week_start.
        iw_rows = db_connection.execute(
            "SELECT indicator_name, score FROM indicator_scores_weekly "
            "WHERE ticker = ? AND week_start = ? ORDER BY indicator_name",
            ("AAPL", "2026-04-20"),
        ).fetchall()
        assert len(iw_rows) == 2
        result = {row["indicator_name"]: row["score"] for row in iw_rows}
        assert result["rsi_14"] == pytest.approx(25.0)
        assert result["macd_line"] is None

    def test_none_indicator_scores_kwarg_writes_no_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When indicator_scores=None (default), persist_weekly_score_row writes
        scores_weekly but leaves indicator_scores_weekly empty.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
            indicator_scores=None,
        )
        assert wrote is True

        sidecar_count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()[0]
        assert sidecar_count == 0

    def test_omitted_indicator_scores_kwarg_writes_no_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When indicator_scores is omitted entirely (kwarg optional, default None),
        indicator_scores_weekly stays empty — same result as explicit None.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        wrote = persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
        )
        assert wrote is True

        sidecar_count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()[0]
        assert sidecar_count == 0

    def test_sidecar_keyed_to_closed_fallback_week(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When the most recent week is in-progress, the fallback week_start is used
        for BOTH scores_weekly and indicator_scores_weekly (consistent keying).
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-13")  # closed
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")  # in-progress
        indicator_scores: dict[str, float | None] = {"rsi_14": 40.0}
        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="ranging",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-22",  # Wednesday of the 2026-04-20 week
            indicator_scores=indicator_scores,
        )
        iw_row = db_connection.execute(
            "SELECT week_start FROM indicator_scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert iw_row is not None
        assert iw_row["week_start"] == "2026-04-13"


class TestPersistMonthlyWithIndicatorScores:
    """
    Tests for the optional indicator_scores kwarg added to persist_monthly_score_row.

    Mirrors TestPersistWeeklyWithIndicatorScores for the monthly cadence.
    """

    def test_indicator_scores_kwarg_writes_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        Passing indicator_scores to persist_monthly_score_row writes both
        scores_monthly and indicator_scores_monthly keyed to the same month_start.
        """
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        indicator_scores: dict[str, float | None] = {"rsi_14": 25.0, "macd_line": None}
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=72.0),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-22",
            indicator_scores=indicator_scores,
        )
        assert wrote is True

        # scores_monthly row exists.
        sm_row = db_connection.execute(
            "SELECT month_start, composite_score FROM scores_monthly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert sm_row is not None
        assert sm_row["month_start"] == "2026-03-01"
        assert sm_row["composite_score"] == pytest.approx(72.0)

        # indicator_scores_monthly rows exist, keyed to the same month_start.
        im_rows = db_connection.execute(
            "SELECT indicator_name, score FROM indicator_scores_monthly "
            "WHERE ticker = ? AND month_start = ? ORDER BY indicator_name",
            ("AAPL", "2026-03-01"),
        ).fetchall()
        assert len(im_rows) == 2
        result = {row["indicator_name"]: row["score"] for row in im_rows}
        assert result["rsi_14"] == pytest.approx(25.0)
        assert result["macd_line"] is None

    def test_none_indicator_scores_kwarg_writes_no_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When indicator_scores=None (default), persist_monthly_score_row writes
        scores_monthly but leaves indicator_scores_monthly empty.
        """
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-22",
            indicator_scores=None,
        )
        assert wrote is True

        sidecar_count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_monthly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()[0]
        assert sidecar_count == 0

    def test_omitted_indicator_scores_kwarg_writes_no_sidecar_rows(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When indicator_scores is omitted entirely (kwarg optional, default None),
        indicator_scores_monthly stays empty.
        """
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")
        wrote = persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-22",
        )
        assert wrote is True

        sidecar_count = db_connection.execute(
            "SELECT COUNT(*) FROM indicator_scores_monthly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()[0]
        assert sidecar_count == 0

    def test_sidecar_keyed_to_closed_fallback_month(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When the most recent month is in-progress, the fallback month_start is used
        for BOTH scores_monthly and indicator_scores_monthly (consistent keying).
        """
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-03-01")  # closed
        _insert_indicators_monthly_row(db_connection, "AAPL", "2026-04-01")  # in-progress
        indicator_scores: dict[str, float | None] = {"rsi_14": 60.0}
        persist_monthly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(),
            regime="ranging",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-22",
            indicator_scores=indicator_scores,
        )
        im_row = db_connection.execute(
            "SELECT month_start FROM indicator_scores_monthly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert im_row is not None


# ---------------------------------------------------------------------------
# Weekly key_signals_data persistence
# ---------------------------------------------------------------------------

class TestWeeklyContributionsPayloadPersistence:
    """Tests for scores_weekly.key_signals_data written by persist_weekly_score_row."""

    def test_contributions_json_is_persisted_when_provided(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When contributions_json is provided, persist_weekly_score_row stores it
        in scores_weekly.key_signals_data.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        sample_payload = {"v": 1, "expansion_factor": 1.0, "items": []}
        import json
        contributions_json = json.dumps(sample_payload)

        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=48.0),
            regime="ranging",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
            contributions_json=contributions_json,
        )

        row = db_connection.execute(
            "SELECT key_signals_data FROM scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert row is not None
        assert row["key_signals_data"] is not None
        parsed = json.loads(row["key_signals_data"])
        assert parsed["v"] == 1
        assert parsed["expansion_factor"] == 1.0

    def test_contributions_json_is_null_when_not_provided(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        When contributions_json is omitted (default None), key_signals_data is
        stored as SQL NULL — backward-compatible with legacy rows.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")

        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=35.0),
            regime="ranging",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
            # contributions_json omitted
        )

        row = db_connection.execute(
            "SELECT key_signals_data FROM scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert row is not None
        assert row["key_signals_data"] is None

    def test_contributions_json_with_indicator_items_round_trips(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """
        A payload with real indicator items survives the write-read cycle intact.
        Asserts specific values, not just non-None.
        """
        _insert_indicators_weekly_row(db_connection, "AAPL", "2026-04-20")
        import json
        sample_payload = {
            "v": 1,
            "expansion_factor": 1.2,
            "items": [
                {
                    "name": "rsi_14",
                    "kind": "indicator",
                    "raw_value": 55.0,
                    "score": 55.0,
                    "category": "momentum",
                    "category_weight": 0.35,
                    "contribution": 2.5,
                }
            ],
        }
        contributions_json = json.dumps(sample_payload)

        persist_weekly_score_row(
            db_connection, "AAPL",
            breakdown=_make_breakdown(composite=55.0),
            regime="trending",
            data_completeness={},
            key_signals=[],
            scoring_date="2026-04-27",
            contributions_json=contributions_json,
        )

        row = db_connection.execute(
            "SELECT key_signals_data FROM scores_weekly WHERE ticker = ?",
            ("AAPL",),
        ).fetchone()
        assert row is not None
        parsed = json.loads(row["key_signals_data"])
        assert parsed["expansion_factor"] == 1.2
        assert len(parsed["items"]) == 1
        assert parsed["items"][0]["name"] == "rsi_14"
        assert parsed["items"][0]["contribution"] == 2.5
