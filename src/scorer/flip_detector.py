"""
Signal flip detection.

Compares today's signal with yesterday's signal for each ticker.
A flip occurs when the signal direction changes (e.g., NEUTRAL → BULLISH).

Flips are always included in Telegram notifications regardless of
confidence threshold — they represent significant changes in the
stock's technical posture.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


def get_previous_score(
    db_conn: sqlite3.Connection,
    ticker: str,
    current_date: str,
) -> Optional[dict]:
    """
    Retrieve the most recent score for a ticker strictly before the given date.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        current_date: The current scoring date (YYYY-MM-DD). Rows on this date
                      are excluded; only rows strictly before this date are returned.

    Returns:
        Dict with keys signal, confidence, final_score, date, or None if no
        previous score exists.
    """
    row = db_conn.execute(
        "SELECT ticker, date, signal, confidence, final_score "
        "FROM scores_daily "
        "WHERE ticker = ? AND date < ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, current_date),
    ).fetchone()

    if row is None:
        return None

    return dict(row)


def detect_signal_flip(
    previous: Optional[dict],
    current: dict,
) -> Optional[dict]:
    """
    Detect whether a signal flip occurred between the previous and current scores.

    A flip is defined as a change in signal direction (e.g., NEUTRAL → BULLISH,
    BULLISH → BEARISH, etc.). On the first scoring day for a ticker, no flip is
    possible since there is no baseline.

    Parameters:
        previous: Dict with previous score data (signal, confidence, etc.),
                  or None if this is the first scoring day.
        current: Dict with the current score data (must include ticker, date,
                 signal, confidence).

    Returns:
        A flip record dict if the signal changed, or None otherwise.
        Flip dict keys: ticker, date, previous_signal, new_signal,
        previous_confidence, new_confidence.
    """
    if previous is None:
        return None

    if previous["signal"] == current["signal"]:
        return None

    return {
        "ticker": current["ticker"],
        "date": current["date"],
        "previous_signal": previous["signal"],
        "new_signal": current["signal"],
        "previous_confidence": previous["confidence"],
        "new_confidence": current["confidence"],
    }


def save_flip_to_db(db_conn: sqlite3.Connection, flip: dict) -> None:
    """
    Insert a signal flip record into the signal_flips table.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        flip: Dict containing ticker, date, previous_signal, new_signal,
              previous_confidence, new_confidence.
    """
    db_conn.execute(
        """
        INSERT INTO signal_flips
            (ticker, date, previous_signal, new_signal, previous_confidence, new_confidence)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            flip["ticker"],
            flip["date"],
            flip["previous_signal"],
            flip["new_signal"],
            flip["previous_confidence"],
            flip["new_confidence"],
        ),
    )
    db_conn.commit()
    logger.info(
        f"{flip['ticker']}: signal flip {flip['previous_signal']} → {flip['new_signal']} "
        f"on {flip['date']}"
    )


def get_flips_for_date(db_conn: sqlite3.Connection, date: str) -> list[dict]:
    """
    Retrieve all signal flips recorded for a given date.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        date: Scoring date in YYYY-MM-DD format.

    Returns:
        List of flip dicts (may be empty if no flips occurred).
    """
    rows = db_conn.execute(
        "SELECT * FROM signal_flips WHERE date = ? ORDER BY ticker ASC",
        (date,),
    ).fetchall()
    return [dict(row) for row in rows]


def detect_flips_for_all(
    db_conn: sqlite3.Connection,
    new_scores: list[dict],
    scoring_date: str,
) -> list[dict]:
    """
    Detect and save signal flips for all tickers in the provided scores list.

    For each score in new_scores, retrieves the previous score and checks for a
    flip. Any detected flips are saved to signal_flips and returned.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        new_scores: List of score dicts for the current scoring date, each
                    containing ticker, date, signal, confidence, final_score.
        scoring_date: The current scoring date (YYYY-MM-DD).

    Returns:
        List of flip dicts for all tickers that changed signal.
    """
    flips: list[dict] = []
    for score in new_scores:
        ticker = score["ticker"]
        try:
            previous = get_previous_score(db_conn, ticker, scoring_date)
            flip = detect_signal_flip(previous, score)
            if flip:
                save_flip_to_db(db_conn, flip)
                flips.append(flip)
        except Exception as exc:
            logger.error(f"{ticker}: error during flip detection — {exc}")
            continue
    return flips
