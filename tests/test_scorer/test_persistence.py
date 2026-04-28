"""
Tests for src/scorer/persistence.py — closed-period scores_weekly/monthly writers.

Covers:
  - Closed-period gate (week + month) — including T1's Sunday vs Monday boundary
  - Idempotency (INSERT OR REPLACE on composite PK)
  - Fundamental + macro inheritance from scores_daily (happy path, no row,
    upper-bound enforcement, T2 prior-week staleness tolerance)
  - T4: month_end calendar correctness for 28/30/31-day months
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.persistence import (
    _inherit_fundamental_macro,
    _last_day_of_month,
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
