"""
Tests for src/scorer/period_gate.py — closed-period gate helpers.
"""

from __future__ import annotations

from src.scorer.period_gate import is_month_closed, is_week_closed


# ---------------------------------------------------------------------------
# is_week_closed (T1)
# ---------------------------------------------------------------------------

class TestIsWeekClosed:
    def test_sunday_of_to_close_week_is_in_progress(self) -> None:
        """
        T1a: Sunday is the LAST day of the week_start=Monday period; the week
        is NOT yet closed (we wait for the next Monday to begin).
        """
        # 2026-04-20 is a Monday; 2026-04-26 is the following Sunday.
        assert is_week_closed(week_start="2026-04-20", scoring_date="2026-04-26") is False

    def test_following_monday_closes_the_week(self) -> None:
        """T1b: scoring on the NEXT Monday after week_start → closed."""
        assert is_week_closed(week_start="2026-04-20", scoring_date="2026-04-27") is True

    def test_well_after_period_is_closed(self) -> None:
        """A scoring_date weeks later still reports closed."""
        assert is_week_closed(week_start="2026-04-20", scoring_date="2026-05-15") is True

    def test_same_day_as_week_start_is_in_progress(self) -> None:
        """Monday of the same week is NOT closed yet."""
        assert is_week_closed(week_start="2026-04-20", scoring_date="2026-04-20") is False

    def test_friday_of_week_is_in_progress(self) -> None:
        """Friday is mid-period — not closed."""
        assert is_week_closed(week_start="2026-04-20", scoring_date="2026-04-24") is False


# ---------------------------------------------------------------------------
# is_month_closed
# ---------------------------------------------------------------------------

class TestIsMonthClosed:
    def test_same_month_in_progress(self) -> None:
        """scoring_date inside the same month → in progress."""
        assert is_month_closed(month_start="2026-04-01", scoring_date="2026-04-22") is False

    def test_first_of_next_month_closes_period(self) -> None:
        """First day of next month → period closed."""
        assert is_month_closed(month_start="2026-03-01", scoring_date="2026-04-01") is True

    def test_later_month_closes_period(self) -> None:
        """scoring_date in a later month → closed."""
        assert is_month_closed(month_start="2026-03-01", scoring_date="2026-04-22") is True

    def test_year_rollover(self) -> None:
        """Crossing the year boundary still closes the prior month."""
        assert is_month_closed(month_start="2026-12-01", scoring_date="2027-01-05") is True

    def test_same_month_first_day_is_in_progress(self) -> None:
        """The 1st of the same month is NOT closed."""
        assert is_month_closed(month_start="2026-04-01", scoring_date="2026-04-01") is False
