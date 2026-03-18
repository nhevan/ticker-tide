"""
Market calendar utilities for the Stock Signal Engine.

Provides simple market open/closed detection based on weekday and a hardcoded
set of US market holidays. Does not account for emergency closures or early
half-day sessions.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

_US_MARKET_HOLIDAYS: set[str] = {
    # 2025
    "2025-01-01",  # New Year's Day
    "2025-01-20",  # MLK Day
    "2025-02-17",  # Presidents Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-11-28",  # Black Friday (early close treated as closed)
    "2025-12-25",  # Christmas
    # 2026
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # MLK Day
    "2026-02-16",  # Presidents Day
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-07-03",  # Independence Day (observed)
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving
    "2026-11-27",  # Black Friday
    "2026-12-25",  # Christmas
}


def is_market_open_today(check_date: date | None = None) -> bool:
    """
    Return True if the US stock market is open on the given date.

    Checks weekday (Monday–Friday) and a hardcoded set of US market holidays.
    Does NOT account for early closes or emergency closures.

    Parameters:
        check_date: Date to check. Defaults to today in UTC.

    Returns:
        True if the market is open, False otherwise.
    """
    if check_date is None:
        check_date = datetime.now(tz=timezone.utc).date()

    if check_date.weekday() >= 5:
        logger.info(f"market_calendar: {check_date} is a weekend — market closed")
        return False

    if check_date.isoformat() in _US_MARKET_HOLIDAYS:
        logger.info(f"market_calendar: {check_date} is a market holiday — market closed")
        return False

    return True
