"""
Tests for scripts/select_volatile_universe.py

All tests run fully offline — no real DB access, no HTTP calls.
Uses tmp_path for SQLite fixture databases and in-memory config files.
"""

from __future__ import annotations

import datetime
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from statistics import pstdev
from typing import Any

import pytest

import scripts.select_volatile_universe as sel_mod
from scripts.select_volatile_universe import (
    SelectionResult,
    TickerMetric,
    UniverseConfig,
    compute_avg_dollar_volume,
    compute_realized_vol,
    select_universe,
    sort_tickers,
    update_active_flags,
    write_config,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_UNIVERSE_CONFIG_DEFAULTS = UniverseConfig(
    target_count=275,
    liquidity_floor_usd=100_000_000.0,
    window_trading_days=90,
    window_calendar_days=130,
    min_history_returns=60,
)


def _make_ticker_entry(
    symbol: str,
    sector: str = "Technology",
    sector_etf: str | None = "XLK",
    added: str = "2026-01-01",
    active: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    """Build a minimal ticker entry dict."""
    entry: dict[str, Any] = {
        "symbol": symbol,
        "sector": sector,
        "sector_etf": sector_etf,
        "added": added,
        "active": active,
    }
    entry.update(extra)
    return entry


def _make_config(tickers: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    """Build a minimal tickers.json config dict."""
    config: dict[str, Any] = {"tickers": tickers}
    config.update(extra)
    return config


def _write_json(path: Path, obj: Any) -> None:
    """Write a JSON file with trailing newline."""
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _setup_ohlcv_db(db_path: Path, rows: list[tuple[str, str, float, float]]) -> None:
    """
    Create a minimal ohlcv_daily table and insert test rows.

    Args:
        db_path: Path to the SQLite file to create.
        rows: List of (ticker, date, close, volume) tuples.
    """
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_daily (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (ticker, date)
            )
        """)
        con.executemany(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, close, volume) VALUES (?, ?, ?, ?)",
            rows,
        )
        con.commit()
    finally:
        con.close()


def _build_constant_closes(value: float, count: int) -> list[float]:
    """Build a list of count identical close prices."""
    return [value] * count


def _build_geometric_closes(start: float, daily_return: float, count: int) -> list[float]:
    """
    Build a list of closes with a constant daily return.

    Args:
        start: Starting price.
        daily_return: Multiplicative factor per day (e.g. 1.01 for +1%/day).
        count: Number of bars.
    """
    closes = [start]
    for _ in range(count - 1):
        closes.append(closes[-1] * daily_return)
    return closes


# ---------------------------------------------------------------------------
# Test 1: compute_realized_vol — known inputs
# ---------------------------------------------------------------------------


def test_compute_realized_vol_known_inputs() -> None:
    """
    compute_realized_vol with a hand-built series should match hand-computed
    pstdev(log_returns) × sqrt(252) × 100 within 1e-9.
    """
    # 5 closes → 4 log returns
    closes = [100.0, 101.0, 99.0, 102.0, 100.5]
    log_returns = [
        math.log(101.0 / 100.0),
        math.log(99.0 / 101.0),
        math.log(102.0 / 99.0),
        math.log(100.5 / 102.0),
    ]
    expected = pstdev(log_returns) * math.sqrt(252) * 100

    result = compute_realized_vol(closes, min_history_returns=4)

    assert result is not None
    assert abs(result - expected) < 1e-9


# ---------------------------------------------------------------------------
# Test 2: compute_realized_vol — constant price is zero
# ---------------------------------------------------------------------------


def test_compute_realized_vol_constant_price_is_zero() -> None:
    """
    A constant-price series has zero log returns → pstdev = 0 → vol = 0.0.
    This is a valid result, not None.
    """
    closes = _build_constant_closes(50.0, 65)
    result = compute_realized_vol(closes, min_history_returns=60)
    assert result is not None
    assert result == 0.0


# ---------------------------------------------------------------------------
# Test 3: compute_realized_vol — insufficient data returns None
# ---------------------------------------------------------------------------


def test_compute_realized_vol_insufficient_data_returns_none() -> None:
    """
    Fewer closes than min_history_returns + 1 should return None, not 0.0.
    """
    closes = [100.0, 101.0, 99.0]  # 2 log returns
    result = compute_realized_vol(closes, min_history_returns=60)
    assert result is None


# ---------------------------------------------------------------------------
# Test 4: compute_avg_dollar_volume — simple
# ---------------------------------------------------------------------------


def test_compute_avg_dollar_volume_simple() -> None:
    """
    compute_avg_dollar_volume should return the arithmetic mean of close × volume.
    """
    bars = [
        (100.0, 1_000_000.0),   # $100M
        (200.0, 500_000.0),     # $100M
        (50.0, 4_000_000.0),    # $200M
    ]
    # mean of [100M, 100M, 200M] = 133.33...M
    expected = (100_000_000.0 + 100_000_000.0 + 200_000_000.0) / 3
    result = compute_avg_dollar_volume(bars)
    assert abs(result - expected) < 1.0


# ---------------------------------------------------------------------------
# Test 5: select_universe — basic (top 3 by vol)
# ---------------------------------------------------------------------------


def test_select_universe_basic() -> None:
    """
    With 10 tickers, target=3, floor=0: the top 3 by vol should be selected.
    """
    metrics = [
        TickerMetric(ticker="AAPL", vol=10.0, adv=500_000_000.0),
        TickerMetric(ticker="MSFT", vol=50.0, adv=500_000_000.0),
        TickerMetric(ticker="NVDA", vol=80.0, adv=500_000_000.0),
        TickerMetric(ticker="META", vol=30.0, adv=500_000_000.0),
        TickerMetric(ticker="AMZN", vol=25.0, adv=500_000_000.0),
        TickerMetric(ticker="TSLA", vol=90.0, adv=500_000_000.0),
        TickerMetric(ticker="JPM", vol=15.0, adv=500_000_000.0),
        TickerMetric(ticker="GOOG", vol=20.0, adv=500_000_000.0),
        TickerMetric(ticker="XOM", vol=12.0, adv=500_000_000.0),
        TickerMetric(ticker="JNJ", vol=8.0, adv=500_000_000.0),
    ]

    result = select_universe(metrics, target_count=3, liquidity_floor=0.0)

    assert result.selected == ["TSLA", "NVDA", "MSFT"]
    assert result.cutoff_vol == 50.0
    assert len(result.filtered_low_history) == 0
    assert len(result.filtered_low_liquidity) == 0


# ---------------------------------------------------------------------------
# Test 6: select_universe — liquidity filter drops below floor
# ---------------------------------------------------------------------------


def test_select_universe_liquidity_filter_drops_below_floor() -> None:
    """
    A high-vol ticker with ADV below the floor must be excluded from selection.
    """
    metrics = [
        TickerMetric(ticker="HIGH_VOL_LOW_LIQ", vol=150.0, adv=50_000_000.0),  # below $100M floor
        TickerMetric(ticker="AAPL", vol=30.0, adv=500_000_000.0),
        TickerMetric(ticker="MSFT", vol=25.0, adv=400_000_000.0),
    ]

    result = select_universe(metrics, target_count=2, liquidity_floor=100_000_000.0)

    assert "HIGH_VOL_LOW_LIQ" not in result.selected
    assert "HIGH_VOL_LOW_LIQ" in result.filtered_low_liquidity
    assert result.selected == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# Test 7: select_universe — history filter drops short series
# ---------------------------------------------------------------------------


def test_select_universe_history_filter_drops_short_series() -> None:
    """
    A ticker with vol=None (insufficient history) must be excluded and appear
    in filtered_low_history.
    """
    metrics = [
        TickerMetric(ticker="SHORT_HIST", vol=None, adv=500_000_000.0),
        TickerMetric(ticker="AAPL", vol=30.0, adv=500_000_000.0),
        TickerMetric(ticker="MSFT", vol=25.0, adv=500_000_000.0),
    ]

    result = select_universe(metrics, target_count=2, liquidity_floor=0.0)

    assert "SHORT_HIST" not in result.selected
    assert "SHORT_HIST" in result.filtered_low_history
    assert result.selected == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# Test 8: select_universe — short pool selects all
# ---------------------------------------------------------------------------


def test_select_universe_short_pool_selects_all() -> None:
    """
    When the eligible pool is smaller than target_count, all eligible tickers
    are selected. cutoff_vol is the lowest vol in the pool.
    """
    metrics = [
        TickerMetric(ticker="AAPL", vol=30.0, adv=500_000_000.0),
        TickerMetric(ticker="MSFT", vol=25.0, adv=500_000_000.0),
        TickerMetric(ticker="JNJ", vol=10.0, adv=500_000_000.0),
        TickerMetric(ticker="XOM", vol=15.0, adv=500_000_000.0),
        TickerMetric(ticker="JPM", vol=20.0, adv=500_000_000.0),
    ]

    result = select_universe(metrics, target_count=10, liquidity_floor=0.0)

    assert len(result.selected) == 5
    assert result.cutoff_vol == 10.0  # lowest vol in pool
    assert set(result.selected) == {"AAPL", "MSFT", "JNJ", "XOM", "JPM"}


# ---------------------------------------------------------------------------
# Test 9: select_universe — deterministic tiebreak
# ---------------------------------------------------------------------------


def test_select_universe_deterministic_tiebreak() -> None:
    """
    Two tickers with identical vol must be ordered by ticker symbol asc.
    Two consecutive runs must produce identical order.
    """
    metrics = [
        TickerMetric(ticker="ZEBRA", vol=50.0, adv=500_000_000.0),
        TickerMetric(ticker="ALPHA", vol=50.0, adv=500_000_000.0),
        TickerMetric(ticker="BETA", vol=30.0, adv=500_000_000.0),
    ]

    result1 = select_universe(metrics, target_count=2, liquidity_floor=0.0)
    result2 = select_universe(metrics, target_count=2, liquidity_floor=0.0)

    # ALPHA sorts before ZEBRA when vol is equal
    assert result1.selected[0] == "ALPHA"
    assert result1.selected[1] == "ZEBRA"
    assert result1.selected == result2.selected


# ---------------------------------------------------------------------------
# Test 10: update_active_flags — preserves extra fields
# ---------------------------------------------------------------------------


def test_update_active_flags_preserves_extra_fields() -> None:
    """
    Toggling the active flag must not strip any other fields from the entry dict.
    A dict containing former_symbol, symbol_since, and a non-standard __test_extra
    key must be preserved verbatim.
    """
    entry = {
        "symbol": "META",
        "sector": "Communication Services",
        "sector_etf": "XLC",
        "added": "2026-01-01",
        "active": False,
        "former_symbol": "FB",
        "symbol_since": "2022-06-09",
        "__test_extra": "preserved",
    }
    tickers = [entry]

    update_active_flags(tickers, selected_symbols={"META"})

    assert entry["active"] is True
    assert entry["former_symbol"] == "FB"
    assert entry["symbol_since"] == "2022-06-09"
    assert entry["__test_extra"] == "preserved"
    # No fields added or removed
    expected_keys = {
        "symbol", "sector", "sector_etf", "added", "active",
        "former_symbol", "symbol_since", "__test_extra",
    }
    assert set(entry.keys()) == expected_keys


# ---------------------------------------------------------------------------
# Test 11: update_active_flags — Index entries always active
# ---------------------------------------------------------------------------


def test_update_active_flags_index_entries_always_active() -> None:
    """
    Index entries must be set to active=True even if their symbol is NOT in
    selected_symbols and even if their current active flag is False.
    """
    spy_entry = _make_ticker_entry(
        "SPY", sector="Index", sector_etf=None, active=False
    )
    aapl_entry = _make_ticker_entry("AAPL", active=True)
    msft_entry = _make_ticker_entry("MSFT", active=True)

    tickers = [spy_entry, aapl_entry, msft_entry]
    # SPY is NOT in selected_symbols
    update_active_flags(tickers, selected_symbols={"AAPL"})

    assert spy_entry["active"] is True   # Index always active
    assert aapl_entry["active"] is True  # in selection
    assert msft_entry["active"] is False  # not in selection


# ---------------------------------------------------------------------------
# Test 12: sort_tickers — stocks then Index
# ---------------------------------------------------------------------------


def test_sort_stocks_then_index() -> None:
    """
    sort_tickers must place non-Index stocks first in alpha order,
    then Index entries in alpha order.
    This is a fresh test of the local sort_tickers implementation.
    """
    tickers = [
        _make_ticker_entry("TSLA", sector="Consumer Discretionary"),
        _make_ticker_entry("QQQ", sector="Index", sector_etf=None),
        _make_ticker_entry("AAPL", sector="Technology"),
        _make_ticker_entry("DIA", sector="Index", sector_etf=None),
        _make_ticker_entry("MSFT", sector="Technology"),
    ]

    sorted_result = sort_tickers(tickers)
    symbols = [entry["symbol"] for entry in sorted_result]

    assert symbols[:3] == ["AAPL", "MSFT", "TSLA"]
    assert symbols[3:] == ["DIA", "QQQ"]


# ---------------------------------------------------------------------------
# Test 13: full run against fixture ohlcv DB
# ---------------------------------------------------------------------------


def test_full_run_against_fixture_ohlcv(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """
    End-to-end test with a small in-memory SQLite DB.

    10 tickers including 1 Index entry. target=5 (not 275). Seeds deterministic
    closes so vols are computable. Verifies:
    - written config has correct active flags
    - deactivated tickers' extra fields are preserved
    - write is skipped on dry-run
    - report is printed to stdout
    """
    # --- Setup DB ---
    db_path = tmp_path / "signals.db"

    # Build rows: 92 bars per ticker (91 returns) so we exceed min_history_returns=60
    # Constant return per ticker → deterministic pstdev
    tickers_and_returns = [
        ("AAVV", 1.015),   # highest vol
        ("BBWW", 1.012),
        ("CCXX", 1.010),
        ("DDYY", 1.008),
        ("EEZZ", 1.006),   # 5th
        ("FFAA", 1.004),   # 6th — deactivated
        ("GGBB", 1.002),
        ("HHCC", 1.001),
        ("IIDD", 1.0005),
    ]

    # Build 92 sequential real calendar dates starting from a fixed base.
    base = datetime.date(2026, 1, 1)
    date_strs = [(base + datetime.timedelta(days=i)).isoformat() for i in range(92)]

    rows: list[tuple[str, str, float, float]] = []
    for sym, daily_ret in tickers_and_returns:
        closes = _build_geometric_closes(100.0, daily_ret, 92)
        for idx, close_val in enumerate(closes):
            rows.append((sym, date_strs[idx], close_val, 2_000_000.0))

    _setup_ohlcv_db(db_path, rows)

    # --- Setup configs ---
    tickers_config_path = tmp_path / "tickers.json"
    universe_config_path = tmp_path / "universe_selection.json"

    tickers = [
        _make_ticker_entry("AAVV"),
        _make_ticker_entry("BBWW"),
        _make_ticker_entry("CCXX"),
        _make_ticker_entry("DDYY"),
        _make_ticker_entry("EEZZ"),
        _make_ticker_entry("FFAA", former_symbol="OLDFF", active=True),
        _make_ticker_entry("GGBB"),
        _make_ticker_entry("HHCC"),
        _make_ticker_entry("IIDD"),
        _make_ticker_entry("SPY", sector="Index", sector_etf=None),
    ]

    _write_json(tickers_config_path, _make_config(tickers))
    _write_json(universe_config_path, {
        "target_count": 5,
        "liquidity_floor_usd": 0,
        "window_trading_days": 90,
        "window_calendar_days": 130,
        "min_history_returns": 60,
    })

    # --- Dry-run: file should not change ---
    original_bytes = tickers_config_path.read_bytes()
    mtime_before = tickers_config_path.stat().st_mtime

    exit_code = sel_mod.main([
        "--dry-run",
        "--config-path", str(tickers_config_path),
        "--universe-config-path", str(universe_config_path),
        "--db-path", str(db_path),
        "--as-of", "2026-05-17",
    ])
    assert exit_code == 0
    assert tickers_config_path.read_bytes() == original_bytes

    # Report was printed
    captured = capsys.readouterr()
    assert "Universe selection" in captured.out

    # --- Real run ---
    exit_code = sel_mod.main([
        "--config-path", str(tickers_config_path),
        "--universe-config-path", str(universe_config_path),
        "--db-path", str(db_path),
        "--as-of", "2026-05-17",
    ])
    assert exit_code == 0

    result_config = json.loads(tickers_config_path.read_text(encoding="utf-8"))
    result_tickers = result_config["tickers"]

    active_symbols = {entry["symbol"] for entry in result_tickers if entry.get("active")}
    inactive_symbols = {entry["symbol"] for entry in result_tickers if not entry.get("active")}

    # SPY (Index) must always be active
    assert "SPY" in active_symbols

    # FFAA should be deactivated (rank 6, target=5)
    assert "FFAA" in inactive_symbols

    # former_symbol preserved verbatim on FFAA
    ffaa = next(entry for entry in result_tickers if entry["symbol"] == "FFAA")
    assert ffaa.get("former_symbol") == "OLDFF"


# ---------------------------------------------------------------------------
# Test 14: as-of excludes future rows
# ---------------------------------------------------------------------------


def test_as_of_excludes_future_rows(tmp_path: Path) -> None:
    """
    Rows with date > as_of must NOT be used in vol/ADV computation.
    Regression test for the adversarial reviewer's #1 finding.
    """
    db_path = tmp_path / "signals.db"

    # Seed 65 bars for AAPL before as_of, plus 10 extra bars AFTER as_of
    past_rows = []
    for day_idx in range(65):
        date_str = f"2025-{(day_idx // 30) + 1:02d}-{(day_idx % 30) + 1:02d}"
        past_rows.append(("AAPL", date_str, 100.0, 2_000_000.0))

    # Future rows with a dramatically different price — if included, vol would differ
    future_rows = [
        ("AAPL", "2026-06-01", 500.0, 50_000_000.0),
        ("AAPL", "2026-06-02", 200.0, 50_000_000.0),
        ("AAPL", "2026-06-03", 800.0, 50_000_000.0),
    ]

    _setup_ohlcv_db(db_path, past_rows + future_rows)

    # Compute vol using only past bars (no future rows)
    past_closes = [row[2] for row in past_rows]
    expected_vol = compute_realized_vol(past_closes, min_history_returns=60)

    # Now fetch via the module using as_of = "2026-05-17" (before the future rows)
    bars = sel_mod._fetch_ohlcv_bars(
        db_path=db_path,
        candidate_symbols=["AAPL"],
        lower_bound="2025-01-01",
        as_of="2026-05-17",
    )
    aapl_bars = bars["AAPL"]
    fetched_closes = [bar[1] for bar in aapl_bars]
    fetched_vol = compute_realized_vol(fetched_closes, min_history_returns=60)

    # Future rows excluded → vols match
    assert fetched_vol == expected_vol
    # Verify future dates are not in the fetched bars
    fetched_dates = [bar[0] for bar in aapl_bars]
    for future_row in future_rows:
        assert future_row[1] not in fetched_dates


# ---------------------------------------------------------------------------
# Test 15: dry-run does not modify config
# ---------------------------------------------------------------------------


def test_dry_run_does_not_modify_config(tmp_path: Path) -> None:
    """
    --dry-run must leave config/tickers.json byte-identical (mtime unchanged).
    """
    db_path = tmp_path / "signals.db"
    _setup_ohlcv_db(db_path, [])  # empty DB — all tickers will have no history

    tickers_config_path = tmp_path / "tickers.json"
    universe_config_path = tmp_path / "universe_selection.json"

    tickers = [
        _make_ticker_entry("AAPL"),
        _make_ticker_entry("MSFT"),
    ]
    _write_json(tickers_config_path, _make_config(tickers))
    _write_json(universe_config_path, {
        "target_count": 1,
        "liquidity_floor_usd": 0,
        "window_trading_days": 90,
        "window_calendar_days": 130,
        "min_history_returns": 60,
    })

    original_bytes = tickers_config_path.read_bytes()
    # Sleep a tiny amount so any write would change mtime
    time.sleep(0.05)

    sel_mod.main([
        "--dry-run",
        "--config-path", str(tickers_config_path),
        "--universe-config-path", str(universe_config_path),
        "--db-path", str(db_path),
        "--as-of", "2026-05-17",
    ])

    assert tickers_config_path.read_bytes() == original_bytes


# ---------------------------------------------------------------------------
# Test 16: idempotent two runs
# ---------------------------------------------------------------------------


def test_idempotent_two_runs(tmp_path: Path) -> None:
    """
    Running the script twice with identical data must produce bytes-equal output.
    The second invocation must be a no-op (write_config bytes-equal short-circuit).
    """
    db_path = tmp_path / "signals.db"

    # Seed 65 bars per ticker so vol is computable
    rows: list[tuple[str, str, float, float]] = []
    for sym, ret in [("AAPL", 1.01), ("MSFT", 1.008)]:
        for day_idx in range(65):
            date_str = f"2026-{(day_idx // 31) + 1:02d}-{(day_idx % 31) + 1:02d}"
            rows.append((sym, date_str, 100.0 * (ret ** day_idx), 2_000_000.0))
    _setup_ohlcv_db(db_path, rows)

    tickers_config_path = tmp_path / "tickers.json"
    universe_config_path = tmp_path / "universe_selection.json"

    _write_json(tickers_config_path, _make_config([
        _make_ticker_entry("AAPL"),
        _make_ticker_entry("MSFT"),
    ]))
    _write_json(universe_config_path, {
        "target_count": 1,
        "liquidity_floor_usd": 0,
        "window_trading_days": 90,
        "window_calendar_days": 130,
        "min_history_returns": 60,
    })

    args = [
        "--config-path", str(tickers_config_path),
        "--universe-config-path", str(universe_config_path),
        "--db-path", str(db_path),
        "--as-of", "2026-05-17",
    ]

    sel_mod.main(args)
    bytes_after_run1 = tickers_config_path.read_bytes()

    sel_mod.main(args)
    bytes_after_run2 = tickers_config_path.read_bytes()

    assert bytes_after_run1 == bytes_after_run2


# ---------------------------------------------------------------------------
# Test 17: existing entry dicts preserved verbatim
# ---------------------------------------------------------------------------


def test_existing_entry_dicts_preserved_verbatim() -> None:
    """
    update_active_flags must mutate in place — only the active field changes.
    A dict containing former_symbol and a non-standard __test_extra key must
    survive with no field stripping.
    """
    entry = {
        "symbol": "META",
        "sector": "Communication Services",
        "sector_etf": "XLC",
        "added": "2026-01-01",
        "active": True,
        "former_symbol": "FB",
        "symbol_since": "2022-06-09",
        "__test_extra": "should survive",
    }
    original_id = id(entry)
    tickers = [entry]

    # Deactivate META
    update_active_flags(tickers, selected_symbols=set())

    # Same dict object (mutated in place, not reconstructed)
    assert id(tickers[0]) == original_id
    assert entry["active"] is False
    assert entry["former_symbol"] == "FB"
    assert entry["symbol_since"] == "2022-06-09"
    assert entry["__test_extra"] == "should survive"
    # No new keys added, no keys removed
    assert set(entry.keys()) == {
        "symbol", "sector", "sector_etf", "added", "active",
        "former_symbol", "symbol_since", "__test_extra",
    }

    # Reactivate META — same dict still, only active changes
    update_active_flags(tickers, selected_symbols={"META"})
    assert entry["active"] is True
    assert entry["former_symbol"] == "FB"
    assert entry["__test_extra"] == "should survive"
