"""
Closed-period gate helpers for weekly and monthly score persistence.

Determines whether a given (period_start, scoring_date) pair represents a
closed (persistable) period or an in-progress (skip) period. Persistence
of scores_weekly / scores_monthly rows is gated on these helpers so we
never write a snapshot of a period that is still accumulating data.

Convention:
  - A weekly bar is keyed by its Monday ``week_start`` and is "closed" the
    moment the next Monday begins (i.e. ``scoring_date >= week_start + 7``).
    Sunday is therefore considered part of the period that is still in
    progress — we wait until the calendar tips over to Monday before we
    treat the week as a finished snapshot.
  - A monthly bar is keyed by the first calendar day ``month_start`` and
    is "closed" once ``scoring_date`` falls in any later (year, month).
"""

from __future__ import annotations

from datetime import date, timedelta


def is_week_closed(week_start: str, scoring_date: str) -> bool:
    """
    Decide whether the week that begins on ``week_start`` is closed.

    A week-starting-Monday is considered closed when ``scoring_date`` is the
    following Monday or later (``scoring_date >= week_start + 7 days``).
    Sunday — the last day of the period — does NOT close the week.

    Parameters:
        week_start:   ISO date string (YYYY-MM-DD) for the Monday on which the
                      week begins. Caller is responsible for ensuring this is
                      a valid Monday; the helper does not validate the weekday.
        scoring_date: ISO date string (YYYY-MM-DD) representing the date the
                      score is being computed for.

    Returns:
        True if the week is closed (safe to persist), False if it is still
        in progress.
    """
    next_monday = date.fromisoformat(week_start) + timedelta(days=7)
    return date.fromisoformat(scoring_date) >= next_monday


def is_month_closed(month_start: str, scoring_date: str) -> bool:
    """
    Decide whether the month that begins on ``month_start`` is closed.

    A month is considered closed when ``scoring_date`` is in any later
    ``(year, month)`` than ``month_start`` — i.e. we have crossed into the
    next calendar month.

    Parameters:
        month_start:  ISO date string (YYYY-MM-DD) for the first day of the
                      month. Caller is responsible for ensuring this is a
                      valid month-start; the helper does not validate the day.
        scoring_date: ISO date string (YYYY-MM-DD).

    Returns:
        True if the month is closed (safe to persist), False if it is still
        in progress.
    """
    month_start_dt = date.fromisoformat(month_start)
    scoring_dt = date.fromisoformat(scoring_date)
    return (scoring_dt.year, scoring_dt.month) > (month_start_dt.year, month_start_dt.month)
