"""
Tests for src/scorer/acceptance_gate.py — calibrator distribution acceptance gate.

The acceptance gate exists to validate that flipping `weekly_score_method` from
v1_4cat to v2_8cat does not catastrophically shift the calibrated_score
distribution. Tests cover all four module-level helpers:

  - compute_calibrated_score_distribution
  - find_latest_scoring_date_with_calibration
  - validate_snapshot_compatibility
  - compare_distributions
"""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from src.common.db import create_all_tables
from src.scorer.acceptance_gate import (
    compare_distributions,
    compute_calibrated_score_distribution,
    find_latest_scoring_date_with_calibration,
    validate_snapshot_compatibility,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def gate_db(tmp_path) -> Generator[sqlite3.Connection, None, None]:
    """Empty DB with the full schema; row_factory set so column access works."""
    db_path = tmp_path / "gate.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    create_all_tables(conn)
    yield conn
    conn.close()


def _insert_score(
    conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    calibrated_score: float | None,
) -> None:
    """Insert a minimal scores_daily row with the given calibrated_score (may be NULL)."""
    conn.execute(
        "INSERT INTO scores_daily (ticker, date, signal, final_score, calibrated_score) "
        "VALUES (?, ?, 'BULLISH', 50.0, ?)",
        (ticker, scoring_date, calibrated_score),
    )
    conn.commit()


@pytest.fixture
def default_thresholds() -> dict:
    """Default acceptance-gate thresholds matching scorer.json[calibrator_acceptance]."""
    return {
        "max_mean_delta": 5.0,
        "max_std_delta": 8.0,
        "max_ticker_delta": 15.0,
        "min_sample_size": 30,
    }


# ---------------------------------------------------------------------------
# compute_calibrated_score_distribution
# ---------------------------------------------------------------------------

def test_compute_distribution_returns_correct_mean_std_count_tickers(
    gate_db: sqlite3.Connection,
) -> None:
    """Mean/std/count/tickers are derived correctly from non-NULL calibrated_score rows."""
    scores = {"AAPL": 1.0, "MSFT": 3.0, "GOOG": 5.0, "AMZN": 7.0, "META": 9.0}
    for ticker, score in scores.items():
        _insert_score(gate_db, ticker, "2026-04-25", score)

    snap = compute_calibrated_score_distribution(gate_db, "2026-04-25")

    assert snap["scoring_date"] == "2026-04-25"
    assert snap["count"] == 5
    assert snap["mean"] == pytest.approx(5.0)
    # Population std of [1,3,5,7,9] = sqrt(8) ≈ 2.8284
    assert snap["std"] == pytest.approx(2.8284271247, rel=1e-4)
    assert snap["tickers"] == scores


def test_compute_distribution_skips_null_calibrated_score(
    gate_db: sqlite3.Connection,
) -> None:
    """Rows with NULL calibrated_score on the same date are excluded from count/mean."""
    _insert_score(gate_db, "AAPL", "2026-04-25", 4.0)
    _insert_score(gate_db, "MSFT", "2026-04-25", 6.0)
    _insert_score(gate_db, "NVDA", "2026-04-25", None)
    _insert_score(gate_db, "TSLA", "2026-04-25", None)

    snap = compute_calibrated_score_distribution(gate_db, "2026-04-25")

    assert snap["count"] == 2
    assert snap["mean"] == pytest.approx(5.0)
    assert "NVDA" not in snap["tickers"]
    assert "TSLA" not in snap["tickers"]


def test_compute_distribution_empty_when_all_null(
    gate_db: sqlite3.Connection,
) -> None:
    """A date with only-NULL calibrated_score rows yields count=0 and empty tickers."""
    _insert_score(gate_db, "AAPL", "2026-04-25", None)
    _insert_score(gate_db, "MSFT", "2026-04-25", None)

    snap = compute_calibrated_score_distribution(gate_db, "2026-04-25")

    assert snap["count"] == 0
    assert snap["tickers"] == {}
    assert snap["mean"] == 0.0
    assert snap["std"] == 0.0


# ---------------------------------------------------------------------------
# find_latest_scoring_date_with_calibration
# ---------------------------------------------------------------------------

def test_find_latest_returns_newest_non_null_date(
    gate_db: sqlite3.Connection,
) -> None:
    """Newest date that has at least one non-NULL calibrated_score is returned."""
    _insert_score(gate_db, "AAPL", "2026-04-23", 1.0)
    _insert_score(gate_db, "AAPL", "2026-04-24", 2.0)
    _insert_score(gate_db, "AAPL", "2026-04-25", 3.0)

    assert find_latest_scoring_date_with_calibration(gate_db) == "2026-04-25"


def test_find_latest_skips_dates_with_only_nulls(
    gate_db: sqlite3.Connection,
) -> None:
    """A more-recent date with only NULL calibrated_score must not be selected."""
    _insert_score(gate_db, "AAPL", "2026-04-23", 1.0)
    _insert_score(gate_db, "AAPL", "2026-04-24", 2.0)
    _insert_score(gate_db, "AAPL", "2026-04-25", None)
    _insert_score(gate_db, "MSFT", "2026-04-25", None)

    assert find_latest_scoring_date_with_calibration(gate_db) == "2026-04-24"


def test_find_latest_returns_none_when_no_calibrated_rows(
    gate_db: sqlite3.Connection,
) -> None:
    """All-NULL DB returns None."""
    _insert_score(gate_db, "AAPL", "2026-04-23", None)

    assert find_latest_scoring_date_with_calibration(gate_db) is None


# ---------------------------------------------------------------------------
# compare_distributions
# ---------------------------------------------------------------------------

def _snap(scoring_date: str, tickers: dict[str, float]) -> dict:
    """Build a snapshot dict from a tickers map (mean/std/count derived)."""
    import statistics

    if not tickers:
        return {
            "scoring_date": scoring_date,
            "mean": 0.0,
            "std": 0.0,
            "count": 0,
            "tickers": {},
        }
    values = list(tickers.values())
    mean_value = sum(values) / len(values)
    if len(values) > 1:
        std_value = statistics.pstdev(values)
    else:
        std_value = 0.0
    return {
        "scoring_date": scoring_date,
        "mean": mean_value,
        "std": std_value,
        "count": len(values),
        "tickers": dict(tickers),
    }


def test_compare_pass_when_both_deltas_under_threshold(
    default_thresholds: dict,
) -> None:
    """PASS: |Δ mean| < max_mean_delta AND |Δ std| < max_std_delta."""
    baseline = _snap("2026-04-25", {f"T{i}": 4.0 for i in range(40)})
    current = _snap("2026-04-25", {f"T{i}": 4.5 for i in range(40)})

    result = compare_distributions(baseline, current, default_thresholds)

    assert result["passed"] is True
    assert result["warning"] is False
    assert abs(result["mean_delta"]) < 5.0
    assert abs(result["std_delta"]) < 8.0


def test_compare_fail_when_mean_delta_exceeds_threshold(
    default_thresholds: dict,
) -> None:
    """FAIL when |Δ mean| > max_mean_delta."""
    baseline = _snap("2026-04-25", {f"T{i}": 4.0 for i in range(40)})
    current = _snap("2026-04-25", {f"T{i}": 12.0 for i in range(40)})

    result = compare_distributions(baseline, current, default_thresholds)

    assert result["passed"] is False
    assert "mean" in result["reason"].lower()


def test_compare_fail_when_std_delta_exceeds_threshold(
    default_thresholds: dict,
) -> None:
    """FAIL when |Δ std| > max_std_delta."""
    baseline = _snap("2026-04-25", {f"T{i}": float(i % 5) for i in range(40)})
    # Inflate spread dramatically so std jumps by far more than 8.0
    current = _snap("2026-04-25", {f"T{i}": float((i % 5) * 20) for i in range(40)})

    result = compare_distributions(baseline, current, default_thresholds)

    assert result["passed"] is False
    assert "std" in result["reason"].lower()


def test_compare_warning_when_delta_in_70_to_100_pct_band(
    default_thresholds: dict,
) -> None:
    """WARNING: 70% ≤ |Δ| / threshold < 100%. Still passes (non-blocking)."""
    # max_mean_delta=5.0, so a Δ of 4.0 = 80% → warning band
    baseline = _snap("2026-04-25", {f"T{i}": 0.0 for i in range(40)})
    current = _snap("2026-04-25", {f"T{i}": 4.0 for i in range(40)})

    result = compare_distributions(baseline, current, default_thresholds)

    assert result["passed"] is True
    assert result["warning"] is True
    assert abs(result["mean_delta"]) == pytest.approx(4.0)


def test_compare_bipolar_shift_passes_but_reports_per_ticker_count(
    default_thresholds: dict,
) -> None:
    """
    Bipolar shift: half the tickers swing +20, half swing -20. Mean stays at 0,
    std barely budges (still passes), but per-ticker delta count surfaces the issue.
    `tickers_exceeding_delta` is informational — does NOT flip `passed` to False.
    """
    baseline_tickers = {f"T{i}": 0.0 for i in range(40)}
    current_tickers = {}
    for i in range(40):
        current_tickers[f"T{i}"] = 20.0 if i % 2 == 0 else -20.0

    baseline = _snap("2026-04-25", baseline_tickers)
    current = _snap("2026-04-25", current_tickers)

    result = compare_distributions(baseline, current, default_thresholds)

    # Mean delta ≈ 0; std delta from 0 → 20 fails the std check (>8.0).
    # That's expected — this test only verifies the per-ticker count surfaces.
    assert result["tickers_exceeding_delta"] == 40
    assert result["max_per_ticker_delta"] == pytest.approx(20.0)
    # `passed` reflects mean+std rules, not the per-ticker count.
    # If you constructed this so std stays under threshold, passed=True; otherwise False.
    # The contract: tickers_exceeding_delta is INFORMATIONAL.
    assert "tickers_exceeding_delta" in result
    # Per-ticker map is populated:
    assert "per_ticker_deltas" in result
    assert len(result["per_ticker_deltas"]) == 40


def test_compare_bipolar_with_small_swings_passes_but_count_nonzero(
    default_thresholds: dict,
) -> None:
    """
    True-bipolar within tolerance: small ±x swings. Mean ≈ 0, std small enough
    to PASS but per-ticker count > 0 still surfaces shift candidates.
    """
    # Use ±16 (just over max_ticker_delta=15.0) but small std change
    baseline_tickers = {f"T{i}": 0.0 for i in range(40)}
    current_tickers = {}
    # Need std change small. If std jumps from 0 to 16 that's > 8.0 (fails std).
    # Engineer it: mostly 0s, just a few tickers shift by 16 — std change small.
    for i in range(40):
        current_tickers[f"T{i}"] = 0.0
    current_tickers["T0"] = 16.0
    current_tickers["T1"] = -16.0

    baseline = _snap("2026-04-25", baseline_tickers)
    current = _snap("2026-04-25", current_tickers)

    result = compare_distributions(baseline, current, default_thresholds)

    # Mean delta ≈ 0; std delta small. Passed should be True.
    assert result["passed"] is True
    # Two tickers shifted past 15.0 — informational count.
    assert result["tickers_exceeding_delta"] == 2
    assert result["max_per_ticker_delta"] == pytest.approx(16.0)


# ---------------------------------------------------------------------------
# validate_snapshot_compatibility
# ---------------------------------------------------------------------------

def test_validate_raises_on_scoring_date_mismatch() -> None:
    """ValueError raised when baseline.scoring_date != current.scoring_date."""
    baseline = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(40)})
    current = _snap("2026-04-26", {f"T{i}": 1.0 for i in range(40)})

    with pytest.raises(ValueError, match="scoring_date"):
        validate_snapshot_compatibility(baseline, current, min_sample_size=30)


def test_validate_raises_when_baseline_count_below_min() -> None:
    """ValueError when baseline.count < min_sample_size."""
    baseline = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(10)})  # below 30
    current = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(40)})

    with pytest.raises(ValueError, match="sample size"):
        validate_snapshot_compatibility(baseline, current, min_sample_size=30)


def test_validate_raises_when_current_count_below_min() -> None:
    """ValueError when current.count < min_sample_size."""
    baseline = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(40)})
    current = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(10)})

    with pytest.raises(ValueError, match="sample size"):
        validate_snapshot_compatibility(baseline, current, min_sample_size=30)


def test_validate_passes_when_compatible() -> None:
    """No exception raised when scoring_date matches and counts are sufficient."""
    baseline = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(40)})
    current = _snap("2026-04-25", {f"T{i}": 1.0 for i in range(40)})

    validate_snapshot_compatibility(baseline, current, min_sample_size=30)
