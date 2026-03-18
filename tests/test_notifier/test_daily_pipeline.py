"""
Tests for scripts/run_daily.py — Daily pipeline orchestrator.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, call, patch

import pytest

# Ensure project root is importable
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


MOCK_FETCH_RESULT = {"skipped": False, "tickers_processed": 59, "tickers_failed": 0, "duration_seconds": 135.0}
MOCK_CALC_RESULT = {"tickers_processed": 59, "tickers_failed": 0, "duration_seconds": 724.0}
MOCK_SCORER_RESULT = {
    "skipped": False,
    "scoring_date": "2026-03-16",
    "tickers_processed": 59,
    "tickers_total": 59,
    "tickers_failed": 0,
    "bullish_count": 3,
    "bearish_count": 2,
    "neutral_count": 54,
    "flips_detected": 1,
    "duration_seconds": 134.0,
}
MOCK_NOTIFIER_RESULT = {
    "scoring_date": "2026-03-16",
    "bullish_count": 1,
    "bearish_count": 1,
    "neutral_count": 57,
    "flips_count": 1,
    "tickers_reasoned": 3,
    "telegram_sent": True,
    "duration_seconds": 45.0,
}


@pytest.fixture(autouse=True)
def patch_load_env(mocker):
    mocker.patch("scripts.run_daily.load_env")


@pytest.fixture(autouse=True)
def patch_load_config(mocker):
    mocker.patch("scripts.run_daily.load_config", return_value={})


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_daily_pipeline_runs_all_phases(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mock_fetch = mocker.patch("scripts.run_daily.run_daily_fetch", return_value=MOCK_FETCH_RESULT)
    mock_calc = mocker.patch("scripts.run_daily.run_calculator", return_value=MOCK_CALC_RESULT)
    mock_scorer = mocker.patch("scripts.run_daily.run_scorer", return_value=MOCK_SCORER_RESULT)
    mock_notifier = mocker.patch("scripts.run_daily.run_notifier", return_value=MOCK_NOTIFIER_RESULT)

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 0
    assert mock_fetch.called
    assert mock_calc.called
    assert mock_scorer.called
    assert mock_notifier.called


# ---------------------------------------------------------------------------
# Market closed
# ---------------------------------------------------------------------------


def test_daily_pipeline_skips_on_holiday(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=False)
    mock_closed = mocker.patch("scripts.run_daily.send_market_closed_notification", return_value=True)
    mock_fetch = mocker.patch("scripts.run_daily.run_daily_fetch")
    mock_calc = mocker.patch("scripts.run_daily.run_calculator")
    mock_scorer = mocker.patch("scripts.run_daily.run_scorer")
    mock_notifier = mocker.patch("scripts.run_daily.run_notifier")

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 0
    assert not mock_fetch.called
    assert not mock_calc.called
    assert not mock_scorer.called
    assert not mock_notifier.called
    assert mock_closed.called


# ---------------------------------------------------------------------------
# Fetcher failure → stop
# ---------------------------------------------------------------------------


def test_daily_pipeline_continues_after_fetcher_failure(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", side_effect=Exception("Fetch error"))
    mock_calc = mocker.patch("scripts.run_daily.run_calculator")
    mock_scorer = mocker.patch("scripts.run_daily.run_scorer")
    mock_notifier = mocker.patch("scripts.run_daily.run_notifier")
    mock_alert = mocker.patch("scripts.run_daily.send_pipeline_error_alert", return_value=True)

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 1
    assert not mock_calc.called
    assert not mock_scorer.called
    assert not mock_notifier.called
    assert mock_alert.called


# ---------------------------------------------------------------------------
# Calculator failure → stop
# ---------------------------------------------------------------------------


def test_daily_pipeline_continues_after_calculator_failure(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", return_value=MOCK_FETCH_RESULT)
    mocker.patch("scripts.run_daily.run_calculator", side_effect=Exception("Calc error"))
    mock_scorer = mocker.patch("scripts.run_daily.run_scorer")
    mock_notifier = mocker.patch("scripts.run_daily.run_notifier")
    mock_alert = mocker.patch("scripts.run_daily.send_pipeline_error_alert", return_value=True)

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 1
    assert not mock_scorer.called
    assert not mock_notifier.called
    assert mock_alert.called


# ---------------------------------------------------------------------------
# Scorer failure → notifier still runs
# ---------------------------------------------------------------------------


def test_daily_pipeline_continues_after_scorer_failure(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", return_value=MOCK_FETCH_RESULT)
    mocker.patch("scripts.run_daily.run_calculator", return_value=MOCK_CALC_RESULT)
    mocker.patch("scripts.run_daily.run_scorer", side_effect=Exception("Scorer error"))
    mock_notifier = mocker.patch("scripts.run_daily.run_notifier", return_value=MOCK_NOTIFIER_RESULT)
    mock_alert = mocker.patch("scripts.run_daily.send_pipeline_error_alert", return_value=True)

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 1  # partial failure
    assert mock_notifier.called  # notifier still runs
    assert mock_alert.called


# ---------------------------------------------------------------------------
# Failure alert
# ---------------------------------------------------------------------------


def test_daily_pipeline_sends_failure_alert(mocker):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", side_effect=Exception("Network timeout"))
    mocker.patch("scripts.run_daily.run_calculator")
    mocker.patch("scripts.run_daily.run_scorer")
    mocker.patch("scripts.run_daily.run_notifier")
    mock_alert = mocker.patch("scripts.run_daily.send_pipeline_error_alert", return_value=True)

    from scripts.run_daily import run_daily_pipeline

    run_daily_pipeline(db_path="/tmp/test.db")

    assert mock_alert.called
    call_args = mock_alert.call_args[0]
    assert "fetcher" in call_args[0].lower() or "fetcher" in str(call_args)


# ---------------------------------------------------------------------------
# Idempotent
# ---------------------------------------------------------------------------


def test_daily_pipeline_idempotent(mocker):
    """Phases returning skipped=True should not cause failure."""
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", return_value={"skipped": True, "reason": "already completed"})
    mocker.patch("scripts.run_daily.run_calculator", return_value={"skipped": True, "reason": "already completed"})
    mocker.patch("scripts.run_daily.run_scorer", return_value={"skipped": True, "reason": "already completed"})
    mocker.patch("scripts.run_daily.run_notifier", return_value={"skipped": True, "reason": "already completed"})

    from scripts.run_daily import run_daily_pipeline

    exit_code = run_daily_pipeline(db_path="/tmp/test.db")

    assert exit_code == 0


# ---------------------------------------------------------------------------
# Total duration tracked
# ---------------------------------------------------------------------------


def test_daily_pipeline_total_duration(mocker, capsys):
    mocker.patch("scripts.run_daily.is_market_open_today", return_value=True)
    mocker.patch("scripts.run_daily.run_daily_fetch", return_value=MOCK_FETCH_RESULT)
    mocker.patch("scripts.run_daily.run_calculator", return_value=MOCK_CALC_RESULT)
    mocker.patch("scripts.run_daily.run_scorer", return_value=MOCK_SCORER_RESULT)
    mocker.patch("scripts.run_daily.run_notifier", return_value=MOCK_NOTIFIER_RESULT)

    from scripts.run_daily import run_daily_pipeline

    run_daily_pipeline(db_path="/tmp/test.db")

    captured = capsys.readouterr()
    assert "Duration" in captured.out or "duration" in captured.out.lower()
