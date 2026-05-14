"""
Realized-return persistence for scores_daily rows.

Computes the actual 10-trading-day forward return (and its components) for
each historical prediction row and writes the result back to scores_daily.
This enables direct SQL accuracy queries against persisted predictions without
recomputing anything from the raw price history at query time.

Columns written (all nullable, added by Migration 7):
  realized_trading_days INTEGER  — number of forward trading days found
  realized_ticker_return REAL    — (close_forward - close_signal) / close_signal * 100
  benchmark_return REAL          — same formula for the benchmark (SPY); NULL when absent
  realized_excess REAL           — ticker_return - benchmark_return (or raw when SPY absent)
  realized_computed_at TEXT      — UTC ISO-8601 timestamp of last population run

Design decisions (locked):
  - Columns live on scores_daily (not a sidecar table). If a second return horizon
    is ever required, migrate to a sidecar table at that time.
  - benchmark_return IS NULL → realized_excess falls back to raw ticker return.
    This matches how compute_excess_return() in calibrator.py computes training
    labels (src/scorer/calibrator.py:127-129). The fallback is intentional.
  - fetch_training_data in calibrator.py is NOT changed to read these columns.
    The calibrator must remain self-contained so that re-scoring any historical
    date always produces the same result regardless of whether backfill has run.
  - analytics.forward_days is decoupled from calibration.forward_days. A WARNING
    is logged per invocation when the two keys diverge; historical realized_excess
    values reflect whichever horizon was active when populated.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from src.common.config import load_config
from src.scorer.calibrator import compute_excess_return

logger = logging.getLogger(__name__)

_PHASE = "realized_returns"


# ---------------------------------------------------------------------------
# Row-level computation
# ---------------------------------------------------------------------------

def compute_realized_return_for_row(
    conn: sqlite3.Connection,
    ticker: str,
    signal_date: str,
    forward_days: int,
    benchmark: str,
) -> Optional[dict]:
    """
    Compute the realized forward return for a single (ticker, signal_date) pair.

    Mirrors the excess-return computation in src/scorer/calibrator.py:112-129.
    Counts the actual number of forward OHLCV rows available (which may be less
    than forward_days if the ticker was delisted mid-window), and uses the last
    available close as the forward price. When SPY data is absent, benchmark_return
    is set to None and realized_excess falls back to the raw ticker return — this
    matches how compute_excess_return() works in the calibrator's training loop.

    Parameters:
        conn:         Open SQLite connection with row_factory=sqlite3.Row.
        ticker:       Ticker symbol to look up.
        signal_date:  The prediction date (YYYY-MM-DD). OHLCV close on this date
                      is used as the starting price.
        forward_days: Number of forward trading days to look ahead.
        benchmark:    Benchmark ticker symbol (e.g. "SPY").

    Returns:
        Dict with keys:
            realized_trading_days (int)        — forward bars found (≤ forward_days)
            realized_ticker_return (float)     — percent return
            benchmark_return (Optional[float]) — benchmark percent return, or None
            realized_excess (float)            — excess return (or raw if SPY absent)
            realized_computed_at (str)         — UTC ISO-8601 timestamp
        Returns None when:
            - No OHLCV row exists for (ticker, signal_date)
            - Signal-date close is zero or negative
            - No forward OHLCV rows exist after signal_date
    """
    # Get signal-date close for ticker
    sig_row = conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
        (ticker, signal_date),
    ).fetchone()

    if sig_row is None or sig_row["close"] is None:
        logger.debug(
            "phase=%s ticker=%s date=%s no signal-date OHLCV row — skipping",
            _PHASE, ticker, signal_date,
        )
        return None

    signal_close = sig_row["close"]

    if not (signal_close > 0):
        logger.warning(
            "phase=%s ticker=%s date=%s signal-date close is zero or negative (%.4f) — skipping",
            _PHASE, ticker, signal_date, signal_close,
        )
        return None

    # Fetch all forward rows (up to forward_days, ordered by date)
    forward_rows = conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
        "ORDER BY date LIMIT ?",
        (ticker, signal_date, forward_days),
    ).fetchall()

    if not forward_rows:
        logger.debug(
            "phase=%s ticker=%s date=%s no forward OHLCV rows — window not yet closed",
            _PHASE, ticker, signal_date,
        )
        return None

    realized_trading_days = len(forward_rows)
    forward_close = forward_rows[-1]["close"]
    ticker_return = (forward_close - signal_close) / signal_close * 100.0

    # Get benchmark (SPY) signal-date close and forward close
    spy_sig_row = conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
        (benchmark, signal_date),
    ).fetchone()

    benchmark_return: Optional[float] = None
    if spy_sig_row and spy_sig_row["close"] and spy_sig_row["close"] > 0:
        spy_fwd_rows = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
            "ORDER BY date LIMIT ?",
            (benchmark, signal_date, realized_trading_days),
        ).fetchall()
        if spy_fwd_rows:
            spy_fwd_close = spy_fwd_rows[-1]["close"]
            spy_sig_close = spy_sig_row["close"]
            benchmark_return = (spy_fwd_close - spy_sig_close) / spy_sig_close * 100.0

    realized_excess = compute_excess_return(ticker_return, benchmark_return)
    computed_at = datetime.now(tz=timezone.utc).isoformat()

    return {
        "realized_trading_days": realized_trading_days,
        "realized_ticker_return": ticker_return,
        "benchmark_return": benchmark_return,
        "realized_excess": realized_excess,
        "realized_computed_at": computed_at,
    }


# ---------------------------------------------------------------------------
# Batch populator
# ---------------------------------------------------------------------------

def populate_realized_returns(
    conn: sqlite3.Connection,
    *,
    scoring_date: Optional[str] = None,
    force: bool = False,
    dry_run: bool = False,
    batch_size: int = 500,
    ticker: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict:
    """
    Iterate eligible scores_daily rows and populate the 5 realized-return columns.

    Reads forward_days from config/scorer.json["analytics"]["forward_days"].
    Reads benchmark_ticker from config/scorer.json["calibration"]["benchmark_ticker"].

    Eligible rows:
      - date <= scoring_date (or all rows when scoring_date is None)
      - ticker = ticker (or all tickers when ticker is None)
      - realized_computed_at IS NULL (skipped when force=True — all rows eligible)

    When force=False, rows with realized_computed_at already set are counted in
    rows_skipped_already_populated and not recomputed. This makes the populator
    safe to call daily on the incremental path.

    When a row has no forward OHLCV data (open window or delisted with zero bars),
    it is counted in rows_skipped_no_forward and the 5 columns stay NULL.

    Commits every batch_size rows to bound memory and allow partial resume if the
    process is interrupted (realized_computed_at IS NULL guard ensures safe resume).

    The limit parameter bounds the number of rows UPDATED (not scanned). Already-
    populated rows that are skipped (force=False) do not count toward the limit.
    This means --limit 1000 will update at most 1000 previously-unpopulated rows.

    Parameters:
        conn:         Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date: Optional upper bound on scores_daily.date to process.
                      Pass today's date for daily pipeline runs.
                      None processes all rows.
        force:        When True, recompute and overwrite already-populated rows.
        dry_run:      When True, run all computation but do not write to DB.
        batch_size:   Rows to commit per transaction.
        ticker:       Optional ticker symbol. When provided, only rows for that
                      ticker are processed. Uses a parameterised query — no
                      string concatenation.
        limit:        Optional cap on the number of rows UPDATED. The populator
                      stops as soon as rows_updated reaches this value. Rows
                      that are skipped (already populated or no forward data)
                      do not count toward the limit.

    Returns:
        Dict with keys:
            rows_scanned (int)
            rows_updated (int)
            rows_skipped_no_forward (int)
            rows_skipped_already_populated (int)
            spy_missing_fallbacks (int)
    """
    scorer_cfg = load_config("scorer")
    analytics_cfg = scorer_cfg.get("analytics", {})
    calibration_cfg = scorer_cfg.get("calibration", {})

    forward_days: int = int(analytics_cfg.get("forward_days", 10))
    benchmark: str = calibration_cfg.get("benchmark_ticker", "SPY")

    # Warn when analytics and calibration horizons diverge
    calib_forward_days = int(calibration_cfg.get("forward_days", 10))
    if forward_days != calib_forward_days:
        logger.warning(
            "phase=%s analytics.forward_days=%d differs from calibration.forward_days=%d; "
            "realized_excess reflects the analytics horizon (%d days)",
            _PHASE, forward_days, calib_forward_days, forward_days,
        )

    # Build query for eligible rows
    # Note: we do NOT pre-filter by realized_computed_at IS NULL in the SQL so that
    # rows_skipped_already_populated is correctly counted. The inner loop does the
    # per-row check when force=False.
    where_parts: list[str] = []
    params: list = []

    if scoring_date is not None:
        where_parts.append("date <= ?")
        params.append(scoring_date)

    if ticker is not None:
        where_parts.append("ticker = ?")
        params.append(ticker)

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    sql = f"SELECT ticker, date, realized_computed_at FROM scores_daily WHERE {where_clause} ORDER BY date DESC"
    rows = conn.execute(sql, params).fetchall()

    rows_scanned = 0
    rows_updated = 0
    rows_skipped_no_forward = 0
    rows_skipped_already_populated = 0
    spy_missing_fallbacks = 0

    pending_updates: list[tuple] = []

    for row in rows:
        rows_scanned += 1
        row_ticker = row["ticker"]
        row_date = row["date"]

        # When force=False, skip rows that already have realized_computed_at set.
        if not force and row["realized_computed_at"] is not None:
            rows_skipped_already_populated += 1
            continue

        result = compute_realized_return_for_row(conn, row_ticker, row_date, forward_days, benchmark)

        if result is None:
            rows_skipped_no_forward += 1
            continue

        if result["benchmark_return"] is None:
            spy_missing_fallbacks += 1

        if not dry_run:
            pending_updates.append((
                result["realized_trading_days"],
                result["realized_ticker_return"],
                result["benchmark_return"],
                result["realized_excess"],
                result["realized_computed_at"],
                row_ticker,
                row_date,
            ))

        rows_updated += 1

        # Stop early when the update limit has been reached
        if limit is not None and rows_updated >= limit:
            if not dry_run and pending_updates:
                _flush_updates(conn, pending_updates)
                pending_updates.clear()
            break

        # Commit in batches to bound memory and allow partial resume
        if len(pending_updates) >= batch_size:
            _flush_updates(conn, pending_updates)
            pending_updates.clear()

    if pending_updates:
        _flush_updates(conn, pending_updates)
        pending_updates.clear()

    logger.info(
        "phase=%s rows_scanned=%d rows_updated=%d rows_skipped_no_forward=%d "
        "rows_skipped_already_populated=%d spy_missing_fallbacks=%d dry_run=%s",
        _PHASE,
        rows_scanned,
        rows_updated,
        rows_skipped_no_forward,
        rows_skipped_already_populated,
        spy_missing_fallbacks,
        dry_run,
    )

    return {
        "rows_scanned": rows_scanned,
        "rows_updated": rows_updated,
        "rows_skipped_no_forward": rows_skipped_no_forward,
        "rows_skipped_already_populated": rows_skipped_already_populated,
        "spy_missing_fallbacks": spy_missing_fallbacks,
    }


def _flush_updates(
    conn: sqlite3.Connection,
    updates: list[tuple],
) -> None:
    """
    Write a batch of realized-return updates to scores_daily and commit.

    Each tuple in updates must be:
      (realized_trading_days, realized_ticker_return, benchmark_return,
       realized_excess, realized_computed_at, ticker, date)

    Parameters:
        conn:    Open SQLite connection.
        updates: List of update tuples to apply.

    Returns:
        None
    """
    conn.executemany(
        """UPDATE scores_daily
           SET realized_trading_days = ?,
               realized_ticker_return = ?,
               benchmark_return = ?,
               realized_excess = ?,
               realized_computed_at = ?
           WHERE ticker = ? AND date = ?""",
        updates,
    )
    conn.commit()
