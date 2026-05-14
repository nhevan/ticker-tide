"""
Tests for src/scorer/realized_returns.py.

Written first (TDD). All tests use pytest's tmp_path fixture so no real
database files are created. Each test is fully isolated.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from src.common.db import create_all_tables, get_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_ohlcv_row(
    conn: sqlite3.Connection,
    ticker: str,
    trade_date: str,
    close: float,
    open_: float = 100.0,
    high: float = 105.0,
    low: float = 95.0,
    volume: float = 1_000_000,
) -> None:
    """Insert a single OHLCV row into ohlcv_daily."""
    conn.execute(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, trade_date, open_, high, low, close, volume),
    )
    conn.commit()


def _insert_scores_daily_row(
    conn: sqlite3.Connection,
    ticker: str,
    trade_date: str,
    signal: str = "BULLISH",
) -> None:
    """Insert a minimal scores_daily row."""
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence, final_score) "
        "VALUES (?, ?, ?, ?, ?)",
        (ticker, trade_date, signal, 60.0, 10.0),
    )
    conn.commit()


def _make_ohlcv_sequence(
    conn: sqlite3.Connection,
    ticker: str,
    start_date: str,
    closes: list[float],
) -> None:
    """Insert a sequence of OHLCV rows on consecutive trading days (no weekends)."""
    current = date.fromisoformat(start_date)
    inserted = 0
    day_offset = 0
    while inserted < len(closes):
        # Skip weekends
        candidate = current + timedelta(days=day_offset)
        if candidate.weekday() < 5:
            _insert_ohlcv_row(conn, ticker, candidate.isoformat(), closes[inserted])
            inserted += 1
        day_offset += 1


def _make_db(tmp_path: Path) -> sqlite3.Connection:
    """Return an open, fully-schemed test DB connection."""
    db_file = str(tmp_path / "signals.db")
    conn = get_connection(db_file)
    create_all_tables(conn)
    return conn


def _load_scorer_config_with_analytics(forward_days: int = 10) -> dict:
    """Return a minimal scorer config dict with analytics.forward_days set."""
    return {
        "calibration": {
            "enabled": True,
            "benchmark_ticker": "SPY",
            "forward_days": forward_days,
        },
        "analytics": {
            "forward_days": forward_days,
        },
    }


# ---------------------------------------------------------------------------
# Test 1: full window
# ---------------------------------------------------------------------------

def test_compute_realized_return_full_window(tmp_path: Path) -> None:
    """
    When 11 OHLCV rows exist after the signal date (forward_days=10), compute
    the realized return from signal close to the 10th future trading-day close.

    Hand-checked values:
      signal close = 100.0
      10th forward close = 115.0 (index OFFSET 9 in 0-based future rows)
      ticker_return = (115 - 100) / 100 * 100 = 15.0%
      SPY signal close = 200.0, SPY 10th forward close = 210.0
      benchmark_return = (210 - 200) / 200 * 100 = 5.0%
      realized_excess = 15.0 - 5.0 = 10.0%
    """
    from src.scorer.realized_returns import compute_realized_return_for_row

    conn = _make_db(tmp_path)

    # Signal date: 2025-01-02 (Thursday), close = 100
    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=100.0)
    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)

    # Insert 11 forward rows for AAPL and SPY (Mon-Fri only starting 2025-01-03)
    # 2025-01-03: Fri, 2025-01-06: Mon, ..., up to 11 trading days
    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 11:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    aapl_closes = [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 110.0, 115.0, 120.0]
    spy_closes = [201.0, 202.0, 203.0, 204.0, 205.0, 206.0, 207.0, 208.0, 209.0, 210.0, 211.0]

    for fdate, ac, sc in zip(forward_dates, aapl_closes, spy_closes):
        _insert_ohlcv_row(conn, "AAPL", fdate, close=ac)
        _insert_ohlcv_row(conn, "SPY", fdate, close=sc)

    result = compute_realized_return_for_row(conn, "AAPL", signal_date, forward_days=10, benchmark="SPY")

    assert result is not None
    assert result["realized_trading_days"] == 10
    assert abs(result["realized_ticker_return"] - 15.0) < 0.01
    assert abs(result["benchmark_return"] - 5.0) < 0.01
    assert abs(result["realized_excess"] - 10.0) < 0.01
    assert result["realized_computed_at"] is not None

    conn.close()


# ---------------------------------------------------------------------------
# Test 2: delisted mid-window
# ---------------------------------------------------------------------------

def test_compute_realized_return_delisted_mid_window(tmp_path: Path) -> None:
    """
    When only 4 forward rows exist (< forward_days=10), the function populates
    a partial result with realized_trading_days=4.
    """
    from src.scorer.realized_returns import compute_realized_return_for_row

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "DLST", signal_date, close=100.0)
    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)

    # Only 4 forward days for DLST, 10 for SPY
    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates[:4]):
        _insert_ohlcv_row(conn, "DLST", fdate, close=100.0 + (i + 1) * 2)

    for i, fdate in enumerate(forward_dates):
        _insert_ohlcv_row(conn, "SPY", fdate, close=200.0 + i + 1)

    result = compute_realized_return_for_row(conn, "DLST", signal_date, forward_days=10, benchmark="SPY")

    assert result is not None
    assert result["realized_trading_days"] == 4
    # Last DLST close = 108.0, signal close = 100.0 → 8.0%
    assert abs(result["realized_ticker_return"] - 8.0) < 0.01

    conn.close()


# ---------------------------------------------------------------------------
# Test 3: no forward data
# ---------------------------------------------------------------------------

def test_compute_realized_return_no_forward_data(tmp_path: Path) -> None:
    """
    When no OHLCV rows exist after the signal date, the function returns None.
    """
    from src.scorer.realized_returns import compute_realized_return_for_row

    conn = _make_db(tmp_path)

    signal_date = "2025-06-01"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=150.0)
    # No forward rows inserted for AAPL

    result = compute_realized_return_for_row(conn, "AAPL", signal_date, forward_days=10, benchmark="SPY")

    assert result is None

    conn.close()


# ---------------------------------------------------------------------------
# Test 4: missing SPY → falls back to raw ticker return
# ---------------------------------------------------------------------------

def test_compute_realized_return_missing_spy_falls_back_to_raw(tmp_path: Path) -> None:
    """
    When SPY data is absent, benchmark_return should be None and
    realized_excess should equal realized_ticker_return (raw fallback, per design decision #4).
    """
    from src.scorer.realized_returns import compute_realized_return_for_row

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=100.0)
    # No SPY rows at all

    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates):
        _insert_ohlcv_row(conn, "AAPL", fdate, close=100.0 + i + 1)

    result = compute_realized_return_for_row(conn, "AAPL", signal_date, forward_days=10, benchmark="SPY")

    assert result is not None
    assert result["benchmark_return"] is None
    assert abs(result["realized_excess"] - result["realized_ticker_return"]) < 1e-9

    conn.close()


# ---------------------------------------------------------------------------
# Test 5: zero signal close → returns None + WARNING
# ---------------------------------------------------------------------------

def test_compute_realized_return_zero_signal_close_handled(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    When the signal-date close is 0 (or negative), the function must return None
    and log a WARNING-level message.
    """
    import logging
    from src.scorer.realized_returns import compute_realized_return_for_row

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=0.0)

    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 5:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for fdate in forward_dates:
        _insert_ohlcv_row(conn, "AAPL", fdate, close=105.0)

    with caplog.at_level(logging.WARNING):
        result = compute_realized_return_for_row(conn, "AAPL", signal_date, forward_days=10, benchmark="SPY")

    assert result is None
    assert any("zero" in rec.message.lower() or "signal" in rec.message.lower() for rec in caplog.records)

    conn.close()


# ---------------------------------------------------------------------------
# Test 6: populate_realized_returns — idempotency
# ---------------------------------------------------------------------------

def test_populate_realized_returns_idempotent(tmp_path: Path) -> None:
    """
    Calling populate_realized_returns twice with the same data should produce
    rows_updated=0 on the second call (already populated rows are skipped).
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=100.0)
    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)
    _insert_scores_daily_row(conn, "AAPL", signal_date)

    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates):
        _insert_ohlcv_row(conn, "AAPL", fdate, close=100.0 + i + 1)
        _insert_ohlcv_row(conn, "SPY", fdate, close=200.0 + i + 1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result1 = populate_realized_returns(conn)
        result2 = populate_realized_returns(conn)

    assert result1["rows_updated"] == 1
    assert result2["rows_updated"] == 0
    assert result2["rows_skipped_already_populated"] == 1

    conn.close()


# ---------------------------------------------------------------------------
# Test 7: populate_realized_returns — force overwrite
# ---------------------------------------------------------------------------

def test_populate_realized_returns_force_overwrites(tmp_path: Path) -> None:
    """
    When force=True, populate_realized_returns must recompute and overwrite
    already-populated rows (rows_updated > 0 on second call).
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=100.0)
    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)
    _insert_scores_daily_row(conn, "AAPL", signal_date)

    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates):
        _insert_ohlcv_row(conn, "AAPL", fdate, close=100.0 + i + 1)
        _insert_ohlcv_row(conn, "SPY", fdate, close=200.0 + i + 1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result1 = populate_realized_returns(conn)
        result2 = populate_realized_returns(conn, force=True)

    assert result1["rows_updated"] == 1
    assert result2["rows_updated"] == 1

    conn.close()


# ---------------------------------------------------------------------------
# Test 8: populate_realized_returns — open window rows stay NULL
# ---------------------------------------------------------------------------

def test_populate_realized_returns_skips_rows_with_open_window(tmp_path: Path) -> None:
    """
    Rows where not enough forward OHLCV data exists should stay NULL.
    rows_skipped_no_forward is incremented; the DB row is not updated.
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    # Use a very recent signal date with no forward data
    signal_date = "2026-05-13"
    _insert_ohlcv_row(conn, "AAPL", signal_date, close=150.0)
    _insert_scores_daily_row(conn, "AAPL", signal_date)
    # No forward rows at all

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result = populate_realized_returns(conn)

    assert result["rows_updated"] == 0
    assert result["rows_skipped_no_forward"] == 1

    # Verify the DB row was not mutated
    row = conn.execute(
        "SELECT realized_computed_at FROM scores_daily WHERE ticker='AAPL' AND date=?",
        (signal_date,),
    ).fetchone()
    assert row is not None
    assert row["realized_computed_at"] is None

    conn.close()


# ---------------------------------------------------------------------------
# Test 9: partial window populated (realized_trading_days < forward_days)
# ---------------------------------------------------------------------------

def test_populate_realized_returns_partial_window_is_populated(tmp_path: Path) -> None:
    """
    When a ticker was delisted mid-window (6 forward rows), populate_realized_returns
    must still write the row with realized_trading_days=6.
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    _insert_ohlcv_row(conn, "DLST", signal_date, close=100.0)
    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)
    _insert_scores_daily_row(conn, "DLST", signal_date)

    forward_dates = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates[:6]):
        _insert_ohlcv_row(conn, "DLST", fdate, close=100.0 + i + 1)
    for i, fdate in enumerate(forward_dates):
        _insert_ohlcv_row(conn, "SPY", fdate, close=200.0 + i + 1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result = populate_realized_returns(conn)

    assert result["rows_updated"] == 1

    row = conn.execute(
        "SELECT realized_trading_days, realized_computed_at FROM scores_daily "
        "WHERE ticker='DLST' AND date=?",
        (signal_date,),
    ).fetchone()
    assert row is not None
    assert row["realized_trading_days"] == 6
    assert row["realized_computed_at"] is not None

    conn.close()


# ---------------------------------------------------------------------------
# Test 10: property test — realized_excess matches compute_excess_return helper
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ticker_ret,benchmark_ret,expected_excess",
    [
        (10.0, 5.0, 5.0),
        (10.0, None, 10.0),
        (-5.0, 3.0, -8.0),
        (-5.0, None, -5.0),
        (0.0, 0.0, 0.0),
        (7.5, 7.5, 0.0),
        (15.0, 10.0, 5.0),
        (-2.5, -5.0, 2.5),
        (20.0, 1.0, 19.0),
        (0.1, 0.1, 0.0),
        (-0.5, 0.5, -1.0),
        (100.0, 50.0, 50.0),
        (-100.0, -50.0, -50.0),
        (3.14159, 1.41421, 1.72738),
        (8.0, 8.0, 0.0),
        (12.0, 3.0, 9.0),
        (-7.0, 7.0, -14.0),
        (0.0, -0.1, 0.1),
        (5.5, None, 5.5),
        (-3.3, -3.3, 0.0),
    ],
)
def test_realized_excess_matches_compute_excess_return_helper(
    ticker_ret: float,
    benchmark_ret: float | None,
    expected_excess: float,
) -> None:
    """
    Parametrised property test: compute_realized_return_for_row uses
    compute_excess_return internally. Verify the 20 cases directly against
    the imported helper to confirm alignment.
    """
    from src.scorer.calibrator import compute_excess_return

    result = compute_excess_return(ticker_ret, benchmark_ret)
    assert abs(result - expected_excess) < 1e-6


# ---------------------------------------------------------------------------
# Test 11: populate_realized_returns — ticker filter
# ---------------------------------------------------------------------------

def test_populate_realized_returns_filters_by_ticker(tmp_path: Path) -> None:
    """
    When ticker='MSFT' is passed, only MSFT rows are updated; AAPL and GOOG stay NULL.
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    signal_date = "2025-01-02"
    for sym in ("AAPL", "MSFT", "GOOG"):
        _insert_ohlcv_row(conn, sym, signal_date, close=100.0)
        _insert_scores_daily_row(conn, sym, signal_date)

    _insert_ohlcv_row(conn, "SPY", signal_date, close=200.0)

    forward_dates: list[str] = []
    current = date(2025, 1, 3)
    while len(forward_dates) < 10:
        if current.weekday() < 5:
            forward_dates.append(current.isoformat())
        current += timedelta(days=1)

    for i, fdate in enumerate(forward_dates):
        for sym in ("AAPL", "MSFT", "GOOG", "SPY"):
            _insert_ohlcv_row(conn, sym, fdate, close=100.0 + i + 1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result = populate_realized_returns(conn, ticker="MSFT")

    assert result["rows_updated"] == 1
    assert result["rows_scanned"] == 1

    # MSFT should be populated
    msft_row = conn.execute(
        "SELECT realized_computed_at FROM scores_daily WHERE ticker='MSFT' AND date=?",
        (signal_date,),
    ).fetchone()
    assert msft_row is not None
    assert msft_row["realized_computed_at"] is not None

    # AAPL and GOOG should remain NULL
    for sym in ("AAPL", "GOOG"):
        other_row = conn.execute(
            "SELECT realized_computed_at FROM scores_daily WHERE ticker=? AND date=?",
            (sym, signal_date),
        ).fetchone()
        assert other_row is not None
        assert other_row["realized_computed_at"] is None, f"{sym} should not have been updated"

    conn.close()


# ---------------------------------------------------------------------------
# Test 12: populate_realized_returns — limit caps updates
# ---------------------------------------------------------------------------

def test_populate_realized_returns_respects_limit(tmp_path: Path) -> None:
    """
    When limit=2 is passed and 5 rows all have closed forward windows,
    exactly 2 rows are updated and the remaining 3 stay NULL.
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    # Create 5 signal dates with closed windows
    base_signal = date(2025, 1, 2)
    signal_dates: list[str] = []
    current = base_signal
    while len(signal_dates) < 5:
        if current.weekday() < 5:
            signal_dates.append(current.isoformat())
        current += timedelta(days=1)

    for sig_date in signal_dates:
        _insert_ohlcv_row(conn, "AAPL", sig_date, close=100.0)
        _insert_ohlcv_row(conn, "SPY", sig_date, close=200.0)
        _insert_scores_daily_row(conn, "AAPL", sig_date)

        # Insert 10 forward rows after each signal date
        fwd_current = date.fromisoformat(sig_date) + timedelta(days=1)
        inserted = 0
        while inserted < 10:
            if fwd_current.weekday() < 5:
                _insert_ohlcv_row(conn, "AAPL", fwd_current.isoformat(), close=110.0)
                _insert_ohlcv_row(conn, "SPY", fwd_current.isoformat(), close=205.0)
                inserted += 1
            fwd_current += timedelta(days=1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result = populate_realized_returns(conn, limit=2)

    assert result["rows_updated"] == 2

    # Count NULL vs non-NULL rows in scores_daily for AAPL
    populated = conn.execute(
        "SELECT COUNT(*) AS cnt FROM scores_daily WHERE ticker='AAPL' AND realized_computed_at IS NOT NULL"
    ).fetchone()["cnt"]
    null_remaining = conn.execute(
        "SELECT COUNT(*) AS cnt FROM scores_daily WHERE ticker='AAPL' AND realized_computed_at IS NULL"
    ).fetchone()["cnt"]

    assert populated == 2
    assert null_remaining == 3

    conn.close()


# ---------------------------------------------------------------------------
# Test 13: populate_realized_returns — limit counts updates, not scans
# ---------------------------------------------------------------------------

def test_populate_realized_returns_limit_counts_updates_not_scans(tmp_path: Path) -> None:
    """
    When 3 rows are already populated and 2 are not, calling with force=False,
    limit=1 should update exactly 1 unpopulated row. The 3 already-populated rows
    must NOT count toward the limit.
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    signal_date_base = date(2025, 1, 2)
    signal_dates: list[str] = []
    current = signal_date_base
    while len(signal_dates) < 5:
        if current.weekday() < 5:
            signal_dates.append(current.isoformat())
        current += timedelta(days=1)

    for sig_date in signal_dates:
        _insert_ohlcv_row(conn, "AAPL", sig_date, close=100.0)
        _insert_ohlcv_row(conn, "SPY", sig_date, close=200.0)
        _insert_scores_daily_row(conn, "AAPL", sig_date)

        fwd_current = date.fromisoformat(sig_date) + timedelta(days=1)
        inserted = 0
        while inserted < 10:
            if fwd_current.weekday() < 5:
                _insert_ohlcv_row(conn, "AAPL", fwd_current.isoformat(), close=110.0)
                _insert_ohlcv_row(conn, "SPY", fwd_current.isoformat(), close=205.0)
                inserted += 1
            fwd_current += timedelta(days=1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    # First pass: populate 3 rows (limit=3)
    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        first_result = populate_realized_returns(conn, limit=3)

    assert first_result["rows_updated"] == 3

    # Second pass: force=False, limit=1 — should update exactly 1 of the 2 remaining rows
    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        second_result = populate_realized_returns(conn, force=False, limit=1)

    assert second_result["rows_updated"] == 1
    assert second_result["rows_skipped_already_populated"] == 3

    # Total populated rows should now be 4
    total_populated = conn.execute(
        "SELECT COUNT(*) AS cnt FROM scores_daily WHERE ticker='AAPL' AND realized_computed_at IS NOT NULL"
    ).fetchone()["cnt"]
    assert total_populated == 4

    conn.close()


# ---------------------------------------------------------------------------
# Test 14: populate_realized_returns — scoring_date scoping excludes future rows
# ---------------------------------------------------------------------------

def test_populate_realized_returns_scoping_by_date_does_not_scan_future_rows(
    tmp_path: Path,
) -> None:
    """
    When scoring_date=D2 is passed, only rows on or before D2 are processed.
    The row at D3 (> D2) must remain NULL even though its forward window is closed.

    Setup:
      D1 = 2025-01-02, D2 = 2025-01-03, D3 = 2025-01-06 (all weekdays)
      All three have 10 forward OHLCV rows available (windows fully closed).
    Expected:
      D1 and D2 get populated (realized_computed_at IS NOT NULL).
      D3 stays NULL (excluded by the scoring_date filter).
    """
    from src.scorer.realized_returns import populate_realized_returns

    conn = _make_db(tmp_path)

    d1 = "2025-01-02"
    d2 = "2025-01-03"
    d3 = "2025-01-06"
    signal_dates = [d1, d2, d3]

    # Insert signal-date OHLCV and scores_daily rows for each date
    for sig_date in signal_dates:
        _insert_ohlcv_row(conn, "AAPL", sig_date, close=100.0)
        _insert_ohlcv_row(conn, "SPY", sig_date, close=200.0)
        _insert_scores_daily_row(conn, "AAPL", sig_date)

    # Insert 10 closed forward OHLCV rows after each signal date
    for sig_date in signal_dates:
        fwd_current = date.fromisoformat(sig_date) + timedelta(days=1)
        inserted = 0
        while inserted < 10:
            if fwd_current.weekday() < 5:
                _insert_ohlcv_row(conn, "AAPL", fwd_current.isoformat(), close=110.0)
                _insert_ohlcv_row(conn, "SPY", fwd_current.isoformat(), close=205.0)
                inserted += 1
            fwd_current += timedelta(days=1)

    scorer_config = _load_scorer_config_with_analytics(forward_days=10)

    with patch("src.scorer.realized_returns.load_config", return_value=scorer_config):
        result = populate_realized_returns(conn, scoring_date=d2)

    # D1 and D2 must be updated; D3 must be excluded
    assert result["rows_scanned"] == 2
    assert result["rows_updated"] == 2

    for sig_date in (d1, d2):
        row = conn.execute(
            "SELECT realized_computed_at FROM scores_daily WHERE ticker='AAPL' AND date=?",
            (sig_date,),
        ).fetchone()
        assert row is not None
        assert row["realized_computed_at"] is not None, f"Expected {sig_date} to be populated"

    d3_row = conn.execute(
        "SELECT realized_computed_at FROM scores_daily WHERE ticker='AAPL' AND date=?",
        (d3,),
    ).fetchone()
    assert d3_row is not None
    assert d3_row["realized_computed_at"] is None, "D3 must remain NULL (excluded by scoring_date)"

    conn.close()
