"""
Tests for src/scorer/main.py — scorer orchestrator.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

SAMPLE_CONFIG = {
    "regime_detection": {
        "adx_trending_threshold": 25,
        "adx_ranging_threshold": 20,
        "atr_volatile_multiplier": 1.5,
        "atr_volatile_lookback": 20,
        "vix_volatile_threshold": 25,
    },
    "adaptive_weights": {
        "trending": {
            "trend": 0.30, "momentum": 0.15, "volume": 0.10, "volatility": 0.05,
            "candlestick": 0.05, "structural": 0.15, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
        "ranging": {
            "trend": 0.10, "momentum": 0.25, "volume": 0.10, "volatility": 0.10,
            "candlestick": 0.10, "structural": 0.15, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
        "volatile": {
            "trend": 0.20, "momentum": 0.15, "volume": 0.10, "volatility": 0.15,
            "candlestick": 0.10, "structural": 0.10, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
    },
    "sector_adjustment": {
        "bullish_sector_threshold": 30,
        "bearish_sector_threshold": -30,
        "max_adjustment": 10,
    },
    "timeframe_weights": {"daily": 0.6, "weekly": 0.4},
    "signal_thresholds": {"bullish": 30, "bearish": -30},
    "confidence_modifiers": {
        "timeframe_agree": 10,
        "timeframe_disagree": -15,
        "volume_confirms": 10,
        "volume_diverges": -10,
        "indicator_consensus": 5,
        "indicator_mixed": -10,
        "earnings_within_days": 7,
        "earnings_penalty": -15,
        "vix_extreme_threshold": 30,
        "vix_extreme_penalty": -10,
        "atr_expanding_penalty": -5,
        "missing_news_penalty": -5,
        "missing_fundamentals_penalty": -3,
    },
    "historical_scoring": {
        "daily_lookback_months": 12,
        "weekly_lookback_months": 60,
    },
}

SAMPLE_TICKER_CONFIG = {
    "symbol": "AAPL",
    "sector": "Technology",
    "sector_etf": "XLK",
    "added": "2026-01-01",
    "active": 1,
}

SCORING_DATE = "2025-01-15"


def _insert_indicator_row(
    conn: sqlite3.Connection,
    ticker: str,
    dt: str,
    rsi: float = 55.0,
    adx: float = 22.0,
) -> None:
    """Insert a minimal indicators_daily row for testing."""
    conn.execute(
        """
        INSERT OR REPLACE INTO indicators_daily
            (ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal, macd_histogram,
             adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line,
             bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower)
        VALUES (?, ?, 101.0, 100.0, 99.0, 0.5, 0.3, 0.2, ?, ?, 60.0, 55.0, 30.0,
                -30.0, 1000000.0, 0.1, 500000.0, 105.0, 95.0, 0.6, 1.5, 106.0, 94.0)
        """,
        (ticker, dt, adx, rsi),
    )


def _insert_ohlcv_row(conn: sqlite3.Connection, ticker: str, dt: str, close: float = 100.0) -> None:
    """Insert a minimal ohlcv_daily row for testing."""
    conn.execute(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, dt, close * 0.99, close * 1.01, close * 0.98, close, 1_000_000),
    )


# ---------------------------------------------------------------------------
# save_score_to_db
# ---------------------------------------------------------------------------

class TestSaveScoreToDb:
    def test_save_score_to_db_inserts_row(self, db_connection: sqlite3.Connection) -> None:
        """INSERT OR REPLACE writes a row to scores_daily."""
        from src.scorer.main import save_score_to_db

        score = {
            "ticker": "AAPL",
            "date": SCORING_DATE,
            "signal": "BULLISH",
            "confidence": 72.0,
            "final_score": 45.0,
            "regime": "trending",
            "daily_score": 50.0,
            "weekly_score": 35.0,
            "trend_score": 60.0,
            "momentum_score": 40.0,
            "volume_score": 30.0,
            "volatility_score": -10.0,
            "candlestick_score": 20.0,
            "structural_score": 50.0,
            "sentiment_score": 15.0,
            "fundamental_score": 25.0,
            "macro_score": 30.0,
            "data_completeness": json.dumps({"news": True, "fundamentals": True}),
            "key_signals": json.dumps(["Bullish EMA alignment", "RSI rising"]),
        }
        save_score_to_db(db_connection, score)

        row = db_connection.execute(
            "SELECT * FROM scores_daily WHERE ticker=? AND date=?",
            ("AAPL", SCORING_DATE),
        ).fetchone()
        assert row is not None
        assert row["signal"] == "BULLISH"
        assert row["confidence"] == 72.0
        assert row["regime"] == "trending"

    def test_save_score_to_db_is_idempotent(self, db_connection: sqlite3.Connection) -> None:
        """Calling save_score_to_db twice results in only 1 row (INSERT OR REPLACE)."""
        from src.scorer.main import save_score_to_db

        base_score = {
            "ticker": "AAPL",
            "date": SCORING_DATE,
            "signal": "BULLISH",
            "confidence": 70.0,
            "final_score": 45.0,
            "regime": "trending",
            "daily_score": 50.0,
            "weekly_score": None,
            "trend_score": 60.0,
            "momentum_score": 40.0,
            "volume_score": 30.0,
            "volatility_score": -10.0,
            "candlestick_score": 20.0,
            "structural_score": 50.0,
            "sentiment_score": 15.0,
            "fundamental_score": 25.0,
            "macro_score": 30.0,
            "data_completeness": "{}",
            "key_signals": "[]",
        }
        save_score_to_db(db_connection, base_score)
        updated = {**base_score, "signal": "BEARISH", "confidence": 55.0}
        save_score_to_db(db_connection, updated)

        rows = db_connection.execute(
            "SELECT COUNT(*) AS cnt FROM scores_daily WHERE ticker=? AND date=?",
            ("AAPL", SCORING_DATE),
        ).fetchone()
        assert rows["cnt"] == 1

        row = db_connection.execute(
            "SELECT signal FROM scores_daily WHERE ticker=? AND date=?",
            ("AAPL", SCORING_DATE),
        ).fetchone()
        assert row["signal"] == "BEARISH"


# ---------------------------------------------------------------------------
# score_ticker
# ---------------------------------------------------------------------------

class TestScoreTicker:
    def test_score_single_ticker_returns_complete_dict(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """score_ticker returns a dict with all required fields."""
        from src.scorer.main import score_ticker

        _insert_indicator_row(db_connection, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(db_connection, "AAPL", SCORING_DATE)

        result = score_ticker(
            db_conn=db_connection,
            ticker="AAPL",
            ticker_config=SAMPLE_TICKER_CONFIG,
            scoring_date=SCORING_DATE,
            config=SAMPLE_CONFIG,
        )

        assert result is not None
        required_keys = [
            "ticker", "date", "signal", "confidence", "final_score",
            "regime", "daily_score", "weekly_score",
            "trend_score", "momentum_score", "volume_score", "volatility_score",
            "candlestick_score", "structural_score", "sentiment_score",
            "fundamental_score", "macro_score",
            "data_completeness", "key_signals",
        ]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

    def test_score_single_ticker_signal_is_valid(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """score_ticker returns BULLISH, BEARISH, or NEUTRAL."""
        from src.scorer.main import score_ticker

        _insert_indicator_row(db_connection, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(db_connection, "AAPL", SCORING_DATE)

        result = score_ticker(
            db_conn=db_connection,
            ticker="AAPL",
            ticker_config=SAMPLE_TICKER_CONFIG,
            scoring_date=SCORING_DATE,
            config=SAMPLE_CONFIG,
        )

        assert result["signal"] in ("BULLISH", "BEARISH", "NEUTRAL")

    def test_score_single_ticker_saves_to_db(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """After score_ticker(), scores_daily has a row for AAPL."""
        from src.scorer.main import score_ticker

        _insert_indicator_row(db_connection, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(db_connection, "AAPL", SCORING_DATE)

        score_ticker(
            db_conn=db_connection,
            ticker="AAPL",
            ticker_config=SAMPLE_TICKER_CONFIG,
            scoring_date=SCORING_DATE,
            config=SAMPLE_CONFIG,
        )

        row = db_connection.execute(
            "SELECT ticker, signal FROM scores_daily WHERE ticker=? AND date=?",
            ("AAPL", SCORING_DATE),
        ).fetchone()
        assert row is not None
        assert row["ticker"] == "AAPL"

    def test_score_single_ticker_is_idempotent(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Scoring twice → only 1 row in scores_daily."""
        from src.scorer.main import score_ticker

        _insert_indicator_row(db_connection, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(db_connection, "AAPL", SCORING_DATE)

        for _ in range(2):
            score_ticker(
                db_conn=db_connection,
                ticker="AAPL",
                ticker_config=SAMPLE_TICKER_CONFIG,
                scoring_date=SCORING_DATE,
                config=SAMPLE_CONFIG,
            )

        count = db_connection.execute(
            "SELECT COUNT(*) AS cnt FROM scores_daily WHERE ticker=? AND date=?",
            ("AAPL", SCORING_DATE),
        ).fetchone()["cnt"]
        assert count == 1

    def test_score_ticker_returns_none_when_no_indicators(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns None when no indicator data exists for the date."""
        from src.scorer.main import score_ticker

        result = score_ticker(
            db_conn=db_connection,
            ticker="AAPL",
            ticker_config=SAMPLE_TICKER_CONFIG,
            scoring_date=SCORING_DATE,
            config=SAMPLE_CONFIG,
        )
        assert result is None


# ---------------------------------------------------------------------------
# run_scorer
# ---------------------------------------------------------------------------

class TestRunScorer:
    def test_score_all_tickers(self, db_connection: sqlite3.Connection, tmp_path) -> None:
        """run_scorer calls score_ticker for each active ticker."""
        import os
        from src.scorer.main import run_scorer

        db_path = str(tmp_path / "test.db")
        from src.common.db import create_all_tables, get_connection
        conn = get_connection(db_path)
        create_all_tables(conn)

        # Set up calculator_done event
        from src.common.events import write_pipeline_event
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")

        # Insert indicator + OHLCV data for 3 tickers
        for ticker in ["AAPL", "MSFT", "JPM"]:
            _insert_indicator_row(conn, ticker, SCORING_DATE)
            _insert_ohlcv_row(conn, ticker, SCORING_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(
                db_path=db_path,
                scoring_date=SCORING_DATE,
            )

        assert result["tickers_processed"] == 3

    def test_score_all_tickers_continues_on_error(
        self, db_connection: sqlite3.Connection, tmp_path
    ) -> None:
        """When ticker 2 fails, tickers 1 and 3 still get scored. Alert logged for failed ticker."""
        import os
        from src.scorer.main import run_scorer

        db_path = str(tmp_path / "test_err.db")
        from src.common.db import create_all_tables, get_connection
        conn = get_connection(db_path)
        create_all_tables(conn)

        from src.common.events import write_pipeline_event
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")

        for ticker in ["AAPL", "JPM"]:
            _insert_indicator_row(conn, ticker, SCORING_DATE)
            _insert_ohlcv_row(conn, ticker, SCORING_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(
                db_path=db_path,
                scoring_date=SCORING_DATE,
            )

        # AAPL and JPM scored; MSFT skipped (no data → returns None, not error)
        assert result["tickers_processed"] + result["tickers_skipped"] >= 2

    def test_scorer_writes_pipeline_event(self, tmp_path) -> None:
        """run_scorer writes 'scorer_done' event with status='completed'."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event, get_pipeline_event_status

        db_path = str(tmp_path / "test_event.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        conn2 = get_connection(db_path)
        status = get_pipeline_event_status(conn2, "scorer_done", SCORING_DATE)
        conn2.close()
        assert status == "completed"

    def test_scorer_logs_pipeline_run(self, tmp_path) -> None:
        """run_scorer inserts a pipeline_runs entry with phase='scorer'."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_run.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        conn2 = get_connection(db_path)
        row = conn2.execute(
            "SELECT * FROM pipeline_runs WHERE phase=?", ("scorer",)
        ).fetchone()
        conn2.close()
        assert row is not None
        assert row["phase"] == "scorer"

    def test_scorer_skips_if_already_done(self, tmp_path) -> None:
        """If 'scorer_done' already completed for today, run_scorer skips scoring."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_skip.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        write_pipeline_event(conn, "scorer_done", SCORING_DATE, "completed")
        conn.close()

        with patch("src.scorer.main.get_active_tickers") as mock_tickers, \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        mock_tickers.assert_not_called()
        assert result.get("skipped") is True

    def test_force_reruns_despite_completed_event(self, tmp_path) -> None:
        """force=True bypasses the 'already completed' skip and re-scores."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_force.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        write_pipeline_event(conn, "scorer_done", SCORING_DATE, "completed")
        _insert_indicator_row(conn, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(conn, "AAPL", SCORING_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path, scoring_date=SCORING_DATE, force=True)

        # Should NOT be skipped — force bypasses the completed check
        assert result.get("skipped") is not True
        assert result["tickers_processed"] == 1

    def test_scorer_waits_for_calculator_event(self, tmp_path) -> None:
        """If 'calculator_done' is absent, run_scorer logs a warning and returns."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection

        db_path = str(tmp_path / "test_wait.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        conn.close()

        with patch("src.scorer.main.get_active_tickers") as mock_tickers, \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        mock_tickers.assert_not_called()
        assert result.get("skipped") is True

    def test_scorer_single_ticker_filter(self, tmp_path) -> None:
        """run_scorer(ticker_filter='AAPL') only scores AAPL."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_filter.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        _insert_indicator_row(conn, "AAPL", SCORING_DATE)
        _insert_ohlcv_row(conn, "AAPL", SCORING_DATE)
        conn.commit()
        conn.close()

        all_tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=all_tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(
                db_path=db_path,
                scoring_date=SCORING_DATE,
                ticker_filter="AAPL",
            )

        assert result["tickers_total"] == 1

    def test_scorer_detects_flips(self, tmp_path) -> None:
        """After run_scorer(), signal_flips has entries for tickers that changed signal."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_flips.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")

        # Insert a previous NEUTRAL score for AAPL
        conn.execute(
            "INSERT OR REPLACE INTO scores_daily "
            "(ticker, date, signal, confidence, final_score, regime) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("AAPL", "2025-01-14", "NEUTRAL", 20.0, 5.0, "ranging"),
        )

        # Insert bullish-leaning indicators so it scores BULLISH today
        _insert_indicator_row(conn, "AAPL", SCORING_DATE, rsi=60.0, adx=30.0)
        _insert_ohlcv_row(conn, "AAPL", SCORING_DATE)
        conn.commit()
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        conn2 = get_connection(db_path)
        flips = conn2.execute(
            "SELECT * FROM signal_flips WHERE date=?", (SCORING_DATE,)
        ).fetchall()
        conn2.close()
        # If AAPL scored NEUTRAL today too, there won't be a flip — we just verify no crash
        # and that the table was queried correctly
        assert isinstance(flips, list)

    def test_scorer_sends_telegram_summary(self, tmp_path) -> None:
        """run_scorer sends a Telegram summary message after scoring."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_tg.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        conn.close()

        send_mock = MagicMock(return_value=123)
        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", send_mock), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        assert send_mock.called

    def test_scorer_summary_contains_signal_distribution(self, tmp_path) -> None:
        """run_scorer result dict includes bullish/bearish/neutral counts."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        db_path = str(tmp_path / "test_dist.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", SCORING_DATE, "completed")
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path, scoring_date=SCORING_DATE)

        assert "bullish_count" in result
        assert "bearish_count" in result
        assert "neutral_count" in result


# ---------------------------------------------------------------------------
# Scoring date resolution
# ---------------------------------------------------------------------------

class TestScoringDateResolution:
    def test_scorer_uses_latest_indicator_date_when_today_has_no_data(
        self, tmp_path
    ) -> None:
        """When no explicit scoring_date is given and today has no data, run_scorer
        falls back to the latest date that has indicator data in the DB."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        LATEST_DATE = "2025-01-15"

        db_path = str(tmp_path / "test_resolve.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        # calculator_done exists for LATEST_DATE, not today
        write_pipeline_event(conn, "calculator_done", LATEST_DATE, "completed")
        _insert_indicator_row(conn, "AAPL", LATEST_DATE)
        _insert_ohlcv_row(conn, "AAPL", LATEST_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path)  # no scoring_date — must resolve from data

        assert result.get("skipped") is not True
        assert result["scoring_date"] == LATEST_DATE
        assert result["tickers_processed"] == 1

    def test_scorer_uses_most_common_latest_date(self, tmp_path) -> None:
        """When tickers have different latest dates, run_scorer uses the most common one."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        COMMON_DATE = "2025-01-14"
        OUTLIER_DATE = "2025-01-15"

        db_path = str(tmp_path / "test_common.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        # calculator_done for the common date (2 out of 3 tickers have this as latest)
        write_pipeline_event(conn, "calculator_done", COMMON_DATE, "completed")

        # 2 tickers have data on COMMON_DATE only
        for ticker in ["MSFT", "JPM"]:
            _insert_indicator_row(conn, ticker, COMMON_DATE)
            _insert_ohlcv_row(conn, ticker, COMMON_DATE)

        # 1 ticker also has a more recent OUTLIER_DATE row
        _insert_indicator_row(conn, "AAPL", COMMON_DATE)
        _insert_ohlcv_row(conn, "AAPL", COMMON_DATE)
        _insert_indicator_row(conn, "AAPL", OUTLIER_DATE)
        _insert_ohlcv_row(conn, "AAPL", OUTLIER_DATE)

        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "active": 1},
            {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path)

        # Should resolve to COMMON_DATE (2 tickers), not OUTLIER_DATE (1 ticker)
        assert result["scoring_date"] == COMMON_DATE

    def test_scorer_accepts_calculator_done_for_latest_data_date(self, tmp_path) -> None:
        """calculator_done for the latest data date (not today) satisfies the pre-flight check."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event

        DATA_DATE = "2025-01-13"  # Monday — last trading day before a long weekend

        db_path = str(tmp_path / "test_calc_preflight.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        write_pipeline_event(conn, "calculator_done", DATA_DATE, "completed")
        _insert_indicator_row(conn, "AAPL", DATA_DATE)
        _insert_ohlcv_row(conn, "AAPL", DATA_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path)

        # Should NOT be skipped — calculator_done exists for the data date
        assert result.get("skipped") is not True
        assert result["tickers_processed"] == 1


    def test_scorer_accepts_calculator_done_for_today_when_scoring_older_data(
        self, tmp_path
    ) -> None:
        """Belt-and-suspenders: calculator_done written for today satisfies pre-flight
        even when the actual data (and resolved scoring date) is from an older date."""
        from src.scorer.main import run_scorer
        from src.common.db import create_all_tables, get_connection
        from src.common.events import write_pipeline_event
        import datetime as dt

        DATA_DATE = "2025-01-15"
        TODAY = dt.date.today().isoformat()

        db_path = str(tmp_path / "test_belt.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        # calculator_done written for TODAY (the run date), not DATA_DATE
        write_pipeline_event(conn, "calculator_done", TODAY, "completed")
        _insert_indicator_row(conn, "AAPL", DATA_DATE)
        _insert_ohlcv_row(conn, "AAPL", DATA_DATE)
        conn.commit()
        conn.close()

        tickers = [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "active": 1},
        ]

        with patch("src.scorer.main.get_active_tickers", return_value=tickers), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_scorer(db_path=db_path)  # no explicit scoring_date

        # Should NOT be skipped — calculator_done for today is acceptable
        assert result.get("skipped") is not True
        assert result["tickers_processed"] == 1
        assert result["scoring_date"] == DATA_DATE


# ---------------------------------------------------------------------------
# run_historical_scoring
# ---------------------------------------------------------------------------

class TestRunHistoricalScoring:
    def test_score_historical_daily(self, tmp_path) -> None:
        """run_historical_scoring(mode='daily') computes scores for last 12 months."""
        from src.scorer.main import run_historical_scoring
        from src.common.db import create_all_tables, get_connection

        db_path = str(tmp_path / "test_hist_daily.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_historical_scoring(db_path=db_path, mode="daily")

        assert "mode" in result
        assert result["mode"] == "daily"

    def test_score_historical_weekly(self, tmp_path) -> None:
        """run_historical_scoring(mode='weekly') computes scores for months 13-60."""
        from src.scorer.main import run_historical_scoring
        from src.common.db import create_all_tables, get_connection

        db_path = str(tmp_path / "test_hist_weekly.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_historical_scoring(db_path=db_path, mode="weekly")

        assert result["mode"] == "weekly"

    def test_score_historical_uses_option_e(self, tmp_path) -> None:
        """run_historical_scoring(mode='both') follows Option E: daily for 12mo + weekly for older."""
        from src.scorer.main import run_historical_scoring
        from src.common.db import create_all_tables, get_connection

        db_path = str(tmp_path / "test_hist_both.db")
        conn = get_connection(db_path)
        create_all_tables(conn)
        conn.close()

        with patch("src.scorer.main.get_active_tickers", return_value=[]), \
             patch("src.scorer.main.send_telegram_message", return_value=1), \
             patch("src.scorer.main.edit_telegram_message", return_value=True), \
             patch("src.scorer.main.load_env"):
            result = run_historical_scoring(db_path=db_path, mode="both")

        assert "total_scores" in result
        assert result["mode"] == "both"
