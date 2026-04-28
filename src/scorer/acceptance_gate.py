"""
Calibrator distribution acceptance gate.

This module exists to validate that flipping `weekly_score_method` (or
`monthly_score_method`) from `v1_4cat` to `v2_8cat` does not catastrophically
shift the calibrated_score distribution. The gate compares two snapshots of
`calibrated_score` taken on the same scoring_date — one before the flip, one
after — and reports PASS, WARNING, or FAIL based on configured thresholds.

The actual flip lives in `config/scorer.json` (`weekly_score_method`).
This module provides only the distribution math + snapshot helpers; the CLI
that drives operators through the snapshot/check workflow is
`scripts/check_calibrator_acceptance.py`.

Key design decisions:
  - Snapshot stores per-ticker values (not just summary stats). This is what
    surfaces "bipolar" shifts — cases where mean stays at 0 because some
    tickers swing +X and others swing -X — that summary stats alone would miss.
  - `tickers_exceeding_delta` is INFORMATIONAL: it reports how many individual
    tickers shifted by more than `max_ticker_delta`, but does NOT by itself
    cause `passed=False`. The summary-stat checks (mean, std) are the gating
    rules.
  - WARNING fires when |Δ| / threshold ∈ [70%, 100%) for either mean or std.
    This is non-blocking but operators should investigate.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Optional


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def compute_calibrated_score_distribution(
    db_conn: sqlite3.Connection,
    scoring_date: str,
) -> dict:
    """
    Compute the calibrated_score distribution snapshot for a given scoring_date.

    Selects rows from `scores_daily` where `date = scoring_date` and
    `calibrated_score IS NOT NULL`. NULL rows are excluded entirely from count,
    mean, std, and the per-ticker map.

    Parameters:
        db_conn:      Open SQLite connection. row_factory does not need to be
                      sqlite3.Row — values are read positionally.
        scoring_date: The date (YYYY-MM-DD) to snapshot.

    Returns:
        Snapshot dict with keys:
          scoring_date (str)
          mean         (float, 0.0 when count == 0)
          std          (float, population stdev; 0.0 when count < 2)
          count        (int)
          tickers      (dict[str, float] — ticker → calibrated_score)
    """
    rows = db_conn.execute(
        "SELECT ticker, calibrated_score FROM scores_daily "
        "WHERE date = ? AND calibrated_score IS NOT NULL",
        (scoring_date,),
    ).fetchall()

    tickers_map: dict[str, float] = {}
    for row in rows:
        # row may be a tuple or a sqlite3.Row — both support index access
        ticker_value = row[0]
        score_value = row[1]
        tickers_map[str(ticker_value)] = float(score_value)

    count_value = len(tickers_map)
    if count_value == 0:
        return {
            "scoring_date": scoring_date,
            "mean": 0.0,
            "std": 0.0,
            "count": 0,
            "tickers": {},
        }

    values = list(tickers_map.values())
    mean_value = sum(values) / count_value
    if count_value > 1:
        variance = sum((value - mean_value) ** 2 for value in values) / count_value
        std_value = math.sqrt(variance)
    else:
        std_value = 0.0

    return {
        "scoring_date": scoring_date,
        "mean": mean_value,
        "std": std_value,
        "count": count_value,
        "tickers": tickers_map,
    }


def find_latest_scoring_date_with_calibration(
    db_conn: sqlite3.Connection,
) -> Optional[str]:
    """
    Find the most recent date in `scores_daily` that has at least one non-NULL
    `calibrated_score` row.

    Used by the snapshot CLI for convenience auto-discovery when the operator
    omits `--scoring-date`. Operators are nonetheless instructed (in
    OPERATIONS.md) to always pass `--scoring-date` explicitly so that pre/post
    snapshots use the same anchor date.

    Parameters:
        db_conn: Open SQLite connection.

    Returns:
        The latest scoring_date as YYYY-MM-DD, or None if no qualifying rows
        exist anywhere in the table.
    """
    row = db_conn.execute(
        "SELECT MAX(date) FROM scores_daily WHERE calibrated_score IS NOT NULL"
    ).fetchone()
    if row is None:
        return None
    value = row[0]
    if value is None:
        return None
    return str(value)


# ---------------------------------------------------------------------------
# Snapshot compatibility
# ---------------------------------------------------------------------------

def validate_snapshot_compatibility(
    baseline: dict,
    current: dict,
    min_sample_size: int,
) -> None:
    """
    Validate that two snapshots can be compared.

    Raises ValueError if:
      - baseline.scoring_date != current.scoring_date (different anchor dates)
      - either snapshot's `count` is below `min_sample_size`

    Parameters:
        baseline:        The pre-flip snapshot dict (from snapshot CLI).
        current:         The post-flip snapshot dict (typically computed live).
        min_sample_size: Minimum row count required in both snapshots.

    Raises:
        ValueError: When the snapshots are incompatible. The message names
                    the specific problem so the CLI can surface it cleanly.
    """
    baseline_date = baseline.get("scoring_date")
    current_date = current.get("scoring_date")
    if baseline_date != current_date:
        raise ValueError(
            f"Snapshot scoring_date mismatch: baseline={baseline_date!r} "
            f"current={current_date!r}. Both snapshots must anchor on the "
            f"same date for the comparison to be meaningful."
        )

    baseline_count = int(baseline.get("count", 0))
    current_count = int(current.get("count", 0))
    if baseline_count < min_sample_size or current_count < min_sample_size:
        raise ValueError(
            f"Insufficient sample size: baseline_count={baseline_count}, "
            f"current_count={current_count}, min_sample_size={min_sample_size}. "
            f"Smaller samples produce noisy mean/std; gate refuses to compare."
        )


# ---------------------------------------------------------------------------
# Distribution comparison
# ---------------------------------------------------------------------------

# WARNING fires when |Δ| / threshold ∈ [_WARNING_RATIO, 1.0)
_WARNING_RATIO = 0.70


def compare_distributions(
    baseline: dict,
    current: dict,
    thresholds: dict,
) -> dict:
    """
    Compare a baseline snapshot against a current snapshot and return a
    pass/warning/fail verdict plus per-ticker delta metadata.

    Decision rules:
      - FAIL when |Δ mean| > max_mean_delta OR |Δ std| > max_std_delta.
      - WARNING (passed=True, warning=True) when either |Δ| / threshold falls
        in [0.70, 1.00) for mean or std.
      - PASS otherwise.

    `tickers_exceeding_delta` is INFORMATIONAL — it counts how many tickers
    moved by more than `max_ticker_delta` between the two snapshots. It does
    NOT, by itself, flip `passed` to False. Bipolar shifts (mean stays 0
    because half the tickers swing +X and half -X) surface here even when
    the mean-delta check passes.

    Parameters:
        baseline:   Pre-flip snapshot dict (must contain scoring_date, mean,
                    std, count, tickers).
        current:    Post-flip snapshot dict (same shape).
        thresholds: Dict with keys max_mean_delta, max_std_delta,
                    max_ticker_delta, min_sample_size.

    Returns:
        Dict with keys:
          passed                   (bool) — False on FAIL, True on PASS or WARNING
          warning                  (bool) — True only on WARNING band
          mean_delta               (float) — current.mean - baseline.mean
          std_delta                (float) — current.std - baseline.std
          tickers_exceeding_delta  (int)  — count > max_ticker_delta (informational)
          max_per_ticker_delta     (float) — largest |delta| across all tickers
          per_ticker_deltas        (dict[str, float]) — ticker → (current - baseline)
          reason                   (str)  — human-readable verdict explanation
    """
    max_mean_delta = float(thresholds["max_mean_delta"])
    max_std_delta = float(thresholds["max_std_delta"])
    max_ticker_delta = float(thresholds["max_ticker_delta"])

    mean_delta = float(current.get("mean", 0.0)) - float(baseline.get("mean", 0.0))
    std_delta = float(current.get("std", 0.0)) - float(baseline.get("std", 0.0))

    per_ticker_deltas, tickers_over, max_abs_ticker_delta = _compute_per_ticker_deltas(
        baseline.get("tickers", {}),
        current.get("tickers", {}),
        max_ticker_delta,
    )

    abs_mean_delta = abs(mean_delta)
    abs_std_delta = abs(std_delta)

    mean_failed = abs_mean_delta > max_mean_delta
    std_failed = abs_std_delta > max_std_delta

    passed = not (mean_failed or std_failed)

    # Warning band: 70% ≤ |Δ| / threshold < 100% (only when not failed)
    warning = False
    if passed:
        mean_ratio = abs_mean_delta / max_mean_delta if max_mean_delta > 0 else 0.0
        std_ratio = abs_std_delta / max_std_delta if max_std_delta > 0 else 0.0
        if mean_ratio >= _WARNING_RATIO or std_ratio >= _WARNING_RATIO:
            warning = True

    reason = _build_reason(
        mean_failed=mean_failed,
        std_failed=std_failed,
        warning=warning,
        mean_delta=mean_delta,
        std_delta=std_delta,
        max_mean_delta=max_mean_delta,
        max_std_delta=max_std_delta,
        tickers_over=tickers_over,
        max_abs_ticker_delta=max_abs_ticker_delta,
        max_ticker_delta=max_ticker_delta,
    )

    return {
        "passed": passed,
        "warning": warning,
        "mean_delta": mean_delta,
        "std_delta": std_delta,
        "tickers_exceeding_delta": tickers_over,
        "max_per_ticker_delta": max_abs_ticker_delta,
        "per_ticker_deltas": per_ticker_deltas,
        "reason": reason,
    }


def _compute_per_ticker_deltas(
    baseline_tickers: dict,
    current_tickers: dict,
    max_ticker_delta: float,
) -> tuple[dict[str, float], int, float]:
    """
    Compute the per-ticker (current - baseline) delta map plus summary counts.

    Tickers present in only one snapshot are skipped — comparing them would
    require pretending the missing side was 0, which would manufacture a
    spurious huge delta. The intersection captures the meaningful comparison.

    Parameters:
        baseline_tickers: ticker → calibrated_score map from baseline.
        current_tickers:  ticker → calibrated_score map from current.
        max_ticker_delta: Threshold above which a ticker is counted as
                          "exceeding" (for the informational tally).

    Returns:
        (per_ticker_deltas, tickers_over, max_abs_ticker_delta) where:
          per_ticker_deltas    — dict[str, float] of intersection deltas
          tickers_over         — count of tickers with |delta| > max_ticker_delta
          max_abs_ticker_delta — largest absolute delta seen (0.0 if empty)
    """
    per_ticker_deltas: dict[str, float] = {}
    common_tickers = set(baseline_tickers.keys()) & set(current_tickers.keys())
    for ticker_symbol in common_tickers:
        delta_value = float(current_tickers[ticker_symbol]) - float(
            baseline_tickers[ticker_symbol]
        )
        per_ticker_deltas[ticker_symbol] = delta_value

    tickers_over = sum(
        1 for value in per_ticker_deltas.values() if abs(value) > max_ticker_delta
    )
    max_abs_ticker_delta = (
        max((abs(value) for value in per_ticker_deltas.values()), default=0.0)
    )
    return per_ticker_deltas, tickers_over, max_abs_ticker_delta


def _build_reason(
    *,
    mean_failed: bool,
    std_failed: bool,
    warning: bool,
    mean_delta: float,
    std_delta: float,
    max_mean_delta: float,
    max_std_delta: float,
    tickers_over: int,
    max_abs_ticker_delta: float,
    max_ticker_delta: float,
) -> str:
    """Build a single-line human-readable verdict string."""
    parts: list[str] = []
    if mean_failed:
        parts.append(
            f"FAIL: mean shift |{mean_delta:.2f}| exceeds max_mean_delta={max_mean_delta:.2f}"
        )
    if std_failed:
        parts.append(
            f"FAIL: std shift |{std_delta:.2f}| exceeds max_std_delta={max_std_delta:.2f}"
        )
    if not parts:
        if warning:
            parts.append(
                f"WARNING: mean_delta={mean_delta:+.2f} std_delta={std_delta:+.2f} "
                f"approaching thresholds (>=70%)"
            )
        else:
            parts.append(
                f"PASS: mean_delta={mean_delta:+.2f} std_delta={std_delta:+.2f}"
            )
    if tickers_over > 0:
        parts.append(
            f"info: {tickers_over} tickers shifted by more than "
            f"{max_ticker_delta:.1f} (max |Δ|={max_abs_ticker_delta:.2f})"
        )
    return " | ".join(parts)
