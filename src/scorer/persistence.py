"""
Persistence helpers for scores_weekly and scores_monthly snapshot tables.

The merged daily ``final_score`` is unaffected by these writes; this module
only adds *additional* per-timeframe rows so downstream consumers (verify
pipeline, UI dashboards, calibrator features) can see the underlying weekly
and monthly composites independently of how they were merged into the daily
signal.

Design notes:

  - Weekly rows are keyed on (ticker, week_start) — the most recent
    ``indicators_weekly.week_start <= scoring_date`` is queried on the fly so
    callers don't need to know the cadence. Monthly mirrors that contract.
  - The closed-period gate (``period_gate.is_week_closed`` /
    ``is_month_closed``) ensures we never persist a snapshot of a period
    that is still accumulating data.
  - Fundamental + macro scores are inherited from the most recent
    ``scores_daily`` row whose ``date`` falls inside the closed period
    (``<= week_start + 4 days`` for weekly, ``<= last_day_of_month`` for
    monthly). When no daily row exists, NULL is persisted.
  - Both helpers use ``INSERT OR REPLACE`` so re-running the persistence
    block for the same (ticker, period_start) is idempotent.
"""

from __future__ import annotations

import calendar
import json
import logging
import sqlite3
from datetime import date, timedelta
from typing import Optional

from src.scorer.period_gate import is_month_closed, is_week_closed

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inheritance helper
# ---------------------------------------------------------------------------

def _inherit_fundamental_macro(
    db_conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Look up fundamental + macro scores from the most recent in-period
    ``scores_daily`` row.

    The selection is bounded by ``period_end_date`` (inclusive) so a weekly
    persist with ``period_end_date = week_start + 4 days`` (Friday) cannot
    pick up a Monday-of-the-next-week fundamental update by accident. Same
    contract for monthly with ``period_end_date = last calendar day of the
    month``.

    Parameters:
        db_conn:         Open SQLite connection (row factory may be Row or default).
        ticker:          Ticker symbol.
        period_end_date: Inclusive upper bound on ``scores_daily.date``
                         (YYYY-MM-DD string).

    Returns:
        Tuple ``(fundamental_score, macro_score)``. Either or both may be
        None — None when no scores_daily row exists in range, or when the
        row exists but the column is NULL.
    """
    row = db_conn.execute(
        "SELECT fundamental_score, macro_score FROM scores_daily "
        "WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, period_end_date),
    ).fetchone()
    if row is None:
        return (None, None)
    # Tolerate both Row and tuple shapes.
    if hasattr(row, "keys"):
        return (row["fundamental_score"], row["macro_score"])
    return (row[0], row[1])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_latest_week_start(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[str]:
    """
    Return the most recent ``indicators_weekly.week_start`` <= scoring_date.

    Mirrors the query used by ``compute_weekly_score_breakdown`` so persistence
    keys stay aligned with the breakdown that produced the composite.

    Returns:
        ISO date string, or None when no weekly indicator data exists for the
        ticker.
    """
    row = db_conn.execute(
        "SELECT week_start FROM indicators_weekly "
        "WHERE ticker = ? AND week_start <= ? "
        "ORDER BY week_start DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    if row is None:
        return None
    return row["week_start"] if hasattr(row, "keys") else row[0]


def _resolve_latest_month_start(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[str]:
    """Return the most recent ``indicators_monthly.month_start`` <= scoring_date."""
    row = db_conn.execute(
        "SELECT month_start FROM indicators_monthly "
        "WHERE ticker = ? AND month_start <= ? "
        "ORDER BY month_start DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    if row is None:
        return None
    return row["month_start"] if hasattr(row, "keys") else row[0]


def _serialise_for_storage(value: object) -> Optional[str]:
    """
    Coerce a Python object to the TEXT shape used by ``scores_*.data_completeness``
    and ``scores_*.key_signals``.

    - dict / list  → ``json.dumps(value)``
    - str          → returned unchanged (caller already serialised)
    - None         → None (NULL in SQL)
    - other        → ``str(value)`` as a defensive fallback so we never blow up
                     on an unexpected shape during the persist hot path.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, str):
        return value
    return str(value)


def _last_day_of_month(month_start: str) -> str:
    """
    Return the calendar last day of the month identified by ``month_start``.

    Uses ``calendar.monthrange`` so February (28/29), 30-day, and 31-day
    months are all handled correctly.
    """
    parsed = date.fromisoformat(month_start)
    last_day = calendar.monthrange(parsed.year, parsed.month)[1]
    return date(parsed.year, parsed.month, last_day).isoformat()


# ---------------------------------------------------------------------------
# Weekly persistence
# ---------------------------------------------------------------------------

def persist_weekly_score_row(
    db_conn: sqlite3.Connection,
    ticker: str,
    breakdown: dict,
    regime: str,
    data_completeness: object,
    key_signals: object,
    scoring_date: str,
) -> bool:
    """
    Persist a closed-week score snapshot to ``scores_weekly``.

    Workflow:
      1. Resolve the most recent ``indicators_weekly.week_start <= scoring_date``.
         (Persistence keys stay aligned with the composite produced by
         ``compute_weekly_score_breakdown``.)
      2. Apply the closed-period gate (``is_week_closed``). In-progress weeks
         are skipped — the function returns False after a DEBUG log.
      3. Inherit fundamental + macro from the most recent ``scores_daily``
         row with ``date <= week_start + 4 days`` (Friday) — see
         ``_inherit_fundamental_macro``.
      4. ``INSERT OR REPLACE`` into scores_weekly on (ticker, week_start).

    Parameters:
        db_conn:           Open SQLite connection.
        ticker:            Ticker symbol.
        breakdown:         Dict produced by ``compute_weekly_score_breakdown``
                           — must contain ``composite_score`` plus the four
                           main category scores. ``candlestick_score`` and
                           ``structural_score`` may be None (v1 mode).
        regime:            Market regime string.
        data_completeness: Dict (preferred) or pre-serialised JSON string.
                           Stored as JSON text.
        key_signals:       List (preferred) or pre-serialised JSON string.
                           Stored as JSON text.
        scoring_date:      Reference date the scorer ran against.

    Returns:
        True if a row was written. False when the week is in-progress
        (gate skipped) or when no weekly indicator data exists for the ticker.
    """
    week_start = _resolve_latest_week_start(db_conn, ticker, scoring_date)
    if week_start is None:
        logger.debug(
            f"{ticker}: no indicators_weekly row <= {scoring_date} — skipping persist"
        )
        return False

    if not is_week_closed(week_start, scoring_date):
        logger.debug(
            f"{ticker}: week {week_start} still in progress on {scoring_date} — skip persist"
        )
        return False

    period_end = (date.fromisoformat(week_start) + timedelta(days=4)).isoformat()
    fundamental_score, macro_score = _inherit_fundamental_macro(
        db_conn, ticker, period_end
    )

    db_conn.execute(
        """
        INSERT OR REPLACE INTO scores_weekly
            (ticker, week_start, composite_score, regime,
             trend_score, momentum_score, volume_score, volatility_score,
             candlestick_score, structural_score,
             fundamental_score, macro_score,
             data_completeness, key_signals)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            week_start,
            breakdown["composite_score"],
            regime,
            breakdown.get("trend_score"),
            breakdown.get("momentum_score"),
            breakdown.get("volume_score"),
            breakdown.get("volatility_score"),
            breakdown.get("candlestick_score"),
            breakdown.get("structural_score"),
            fundamental_score,
            macro_score,
            _serialise_for_storage(data_completeness),
            _serialise_for_storage(key_signals),
        ),
    )
    db_conn.commit()
    return True


# ---------------------------------------------------------------------------
# Monthly persistence
# ---------------------------------------------------------------------------

def persist_monthly_score_row(
    db_conn: sqlite3.Connection,
    ticker: str,
    breakdown: dict,
    regime: str,
    data_completeness: object,
    key_signals: object,
    scoring_date: str,
) -> bool:
    """
    Persist a closed-month score snapshot to ``scores_monthly``.

    Same contract as ``persist_weekly_score_row``: queries the most recent
    ``indicators_monthly.month_start <= scoring_date``, applies
    ``is_month_closed``, inherits fundamental + macro from the most recent
    ``scores_daily`` row whose ``date <= last calendar day of the month``,
    and writes the row via ``INSERT OR REPLACE``.

    Parameters:
        db_conn:           Open SQLite connection.
        ticker:            Ticker symbol.
        breakdown:         Dict from ``compute_monthly_score_breakdown``.
        regime:            Market regime.
        data_completeness: Dict or pre-serialised JSON string.
        key_signals:       List or pre-serialised JSON string.
        scoring_date:      Reference date.

    Returns:
        True if a row was written; False when the month is in-progress
        or when no monthly indicator data exists for the ticker.
    """
    month_start = _resolve_latest_month_start(db_conn, ticker, scoring_date)
    if month_start is None:
        logger.debug(
            f"{ticker}: no indicators_monthly row <= {scoring_date} — skipping persist"
        )
        return False

    if not is_month_closed(month_start, scoring_date):
        logger.debug(
            f"{ticker}: month {month_start} still in progress on {scoring_date} — skip persist"
        )
        return False

    period_end = _last_day_of_month(month_start)
    fundamental_score, macro_score = _inherit_fundamental_macro(
        db_conn, ticker, period_end
    )

    db_conn.execute(
        """
        INSERT OR REPLACE INTO scores_monthly
            (ticker, month_start, composite_score, regime,
             trend_score, momentum_score, volume_score, volatility_score,
             candlestick_score, structural_score,
             fundamental_score, macro_score,
             data_completeness, key_signals)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            month_start,
            breakdown["composite_score"],
            regime,
            breakdown.get("trend_score"),
            breakdown.get("momentum_score"),
            breakdown.get("volume_score"),
            breakdown.get("volatility_score"),
            breakdown.get("candlestick_score"),
            breakdown.get("structural_score"),
            fundamental_score,
            macro_score,
            _serialise_for_storage(data_completeness),
            _serialise_for_storage(key_signals),
        ),
    )
    db_conn.commit()
    return True
