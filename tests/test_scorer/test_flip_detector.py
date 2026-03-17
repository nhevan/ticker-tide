"""
Tests for src/scorer/flip_detector.py — signal flip detection.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.flip_detector import (
    detect_signal_flip,
    get_flips_for_date,
    get_previous_score,
    save_flip_to_db,
)


# ---------------------------------------------------------------------------
# get_previous_score
# ---------------------------------------------------------------------------

class TestGetPreviousScore:
    def test_get_previous_score_returns_most_recent(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns the most recent score BEFORE current_date."""
        db_connection.executemany(
            "INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence, final_score) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                ("AAPL", "2025-01-10", "BULLISH", 70.0, 45.0),
                ("AAPL", "2025-01-12", "BULLISH", 75.0, 50.0),
            ],
        )
        db_connection.commit()

        result = get_previous_score(db_connection, "AAPL", "2025-01-15")
        assert result is not None
        assert result["date"] == "2025-01-12"
        assert result["signal"] == "BULLISH"

    def test_get_previous_score_excludes_current_date(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Excludes the current_date itself (only returns BEFORE)."""
        db_connection.execute(
            "INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence, final_score) "
            "VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2025-01-15", "BULLISH", 70.0, 45.0),
        )
        db_connection.commit()

        result = get_previous_score(db_connection, "AAPL", "2025-01-15")
        assert result is None

    def test_get_previous_score_returns_none_when_no_history(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns None if no previous scores exist for this ticker."""
        result = get_previous_score(db_connection, "AAPL", "2025-01-15")
        assert result is None


# ---------------------------------------------------------------------------
# detect_signal_flip
# ---------------------------------------------------------------------------

class TestDetectSignalFlip:
    def test_detect_flip_neutral_to_bullish(self) -> None:
        """NEUTRAL → BULLISH: returns flip record."""
        previous = {
            "ticker": "AAPL", "date": "2025-01-14",
            "signal": "NEUTRAL", "confidence": 25.0, "final_score": 15.0,
        }
        current = {
            "ticker": "AAPL", "date": "2025-01-15",
            "signal": "BULLISH", "confidence": 65.0, "final_score": 45.0,
        }
        flip = detect_signal_flip(previous, current)
        assert flip is not None
        assert flip["previous_signal"] == "NEUTRAL"
        assert flip["new_signal"] == "BULLISH"

    def test_detect_flip_bullish_to_bearish(self) -> None:
        """BULLISH → BEARISH: returns flip record."""
        previous = {
            "ticker": "AAPL", "date": "2025-01-14",
            "signal": "BULLISH", "confidence": 70.0, "final_score": 50.0,
        }
        current = {
            "ticker": "AAPL", "date": "2025-01-15",
            "signal": "BEARISH", "confidence": 55.0, "final_score": -35.0,
        }
        flip = detect_signal_flip(previous, current)
        assert flip is not None
        assert flip["previous_signal"] == "BULLISH"
        assert flip["new_signal"] == "BEARISH"

    def test_detect_no_flip(self) -> None:
        """BULLISH → BULLISH: returns None."""
        previous = {
            "ticker": "AAPL", "date": "2025-01-14",
            "signal": "BULLISH", "confidence": 70.0, "final_score": 50.0,
        }
        current = {
            "ticker": "AAPL", "date": "2025-01-15",
            "signal": "BULLISH", "confidence": 72.0, "final_score": 52.0,
        }
        flip = detect_signal_flip(previous, current)
        assert flip is None

    def test_detect_flip_first_day(self) -> None:
        """No previous score (first time scoring) → returns None."""
        current = {
            "ticker": "AAPL", "date": "2025-01-15",
            "signal": "BULLISH", "confidence": 65.0, "final_score": 45.0,
        }
        flip = detect_signal_flip(None, current)
        assert flip is None

    def test_detect_flip_record_contains_all_fields(self) -> None:
        """Flip record contains ticker, date, previous_signal, new_signal, confidences."""
        previous = {
            "ticker": "AAPL", "date": "2025-01-14",
            "signal": "NEUTRAL", "confidence": 20.0, "final_score": 10.0,
        }
        current = {
            "ticker": "AAPL", "date": "2025-01-15",
            "signal": "BULLISH", "confidence": 65.0, "final_score": 45.0,
        }
        flip = detect_signal_flip(previous, current)
        assert flip["ticker"] == "AAPL"
        assert flip["date"] == "2025-01-15"
        assert flip["previous_confidence"] == 20.0
        assert flip["new_confidence"] == 65.0


# ---------------------------------------------------------------------------
# save_flip_to_db
# ---------------------------------------------------------------------------

class TestSaveFlipToDb:
    def test_save_flip_to_db(self, db_connection: sqlite3.Connection) -> None:
        """Saves a flip dict to the signal_flips table; row exists with correct values."""
        flip = {
            "ticker": "AAPL",
            "date": "2025-01-15",
            "previous_signal": "NEUTRAL",
            "new_signal": "BULLISH",
            "previous_confidence": 20.0,
            "new_confidence": 65.0,
        }
        save_flip_to_db(db_connection, flip)

        row = db_connection.execute(
            "SELECT * FROM signal_flips WHERE ticker=? AND date=?",
            ("AAPL", "2025-01-15"),
        ).fetchone()

        assert row is not None
        assert row["previous_signal"] == "NEUTRAL"
        assert row["new_signal"] == "BULLISH"
        assert row["previous_confidence"] == 20.0
        assert row["new_confidence"] == 65.0


# ---------------------------------------------------------------------------
# get_flips_for_date
# ---------------------------------------------------------------------------

class TestGetFlipsForDate:
    def test_get_flips_for_date(self, db_connection: sqlite3.Connection) -> None:
        """Returns all 3 flips inserted for today's date."""
        flips = [
            ("AAPL", "2025-01-15", "NEUTRAL", "BULLISH", 20.0, 65.0),
            ("MSFT", "2025-01-15", "BULLISH", "BEARISH", 70.0, 55.0),
            ("JPM", "2025-01-15", "BEARISH", "NEUTRAL", 40.0, 22.0),
        ]
        db_connection.executemany(
            "INSERT INTO signal_flips "
            "(ticker, date, previous_signal, new_signal, previous_confidence, new_confidence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            flips,
        )
        db_connection.commit()

        result = get_flips_for_date(db_connection, "2025-01-15")
        assert len(result) == 3
        tickers = {r["ticker"] for r in result}
        assert tickers == {"AAPL", "MSFT", "JPM"}

    def test_get_flips_returns_empty(self, db_connection: sqlite3.Connection) -> None:
        """Returns empty list when no flips exist."""
        result = get_flips_for_date(db_connection, "2025-01-15")
        assert result == []


# ---------------------------------------------------------------------------
# detect_flips_for_all_tickers
# ---------------------------------------------------------------------------

class TestDetectFlipsForAllTickers:
    def test_detect_flips_for_all_tickers(self, db_connection: sqlite3.Connection) -> None:
        """3 tickers; only the one that changed signal gets a signal_flips row."""
        from src.scorer.flip_detector import detect_flips_for_all

        # Insert previous scores for 3 tickers
        prev_scores = [
            ("AAPL", "2025-01-14", "NEUTRAL", 20.0, 10.0),
            ("MSFT", "2025-01-14", "BULLISH", 70.0, 50.0),
            ("JPM", "2025-01-14", "BEARISH", 45.0, -35.0),
        ]
        db_connection.executemany(
            "INSERT OR REPLACE INTO scores_daily "
            "(ticker, date, signal, confidence, final_score) VALUES (?, ?, ?, ?, ?)",
            prev_scores,
        )
        db_connection.commit()

        # New scores: AAPL flipped to BULLISH, MSFT stayed BULLISH, JPM stayed BEARISH
        new_scores = [
            {"ticker": "AAPL", "date": "2025-01-15", "signal": "BULLISH", "confidence": 65.0, "final_score": 45.0},
            {"ticker": "MSFT", "date": "2025-01-15", "signal": "BULLISH", "confidence": 72.0, "final_score": 52.0},
            {"ticker": "JPM", "date": "2025-01-15", "signal": "BEARISH", "confidence": 50.0, "final_score": -40.0},
        ]

        detect_flips_for_all(db_connection, new_scores, "2025-01-15")

        flips = get_flips_for_date(db_connection, "2025-01-15")
        assert len(flips) == 1
        assert flips[0]["ticker"] == "AAPL"
        assert flips[0]["previous_signal"] == "NEUTRAL"
        assert flips[0]["new_signal"] == "BULLISH"
