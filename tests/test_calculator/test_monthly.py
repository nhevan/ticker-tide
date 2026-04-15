"""
Tests for src/calculator/monthly.py

Covers:
- build_monthly_candles: single month aggregation, multiple months, month_start
  is always YYYY-MM-01, first/last trading day open/close, correct high/low/volume
- save_monthly_candles_to_db: values correct, idempotency (INSERT OR REPLACE)
- compute_monthly_indicators: same indicator columns as weekly, uses same config
- save_monthly_indicators_to_db: correct columns written
- compute_monthly_for_ticker: end-to-end DB round-trip (full mode)
- compute_monthly_for_ticker incremental mode: only new month added
- Empty input edge cases return empty DataFrames / 0 rows
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.monthly import (
    build_monthly_candles,
    compute_monthly_for_ticker,
    compute_monthly_indicators,
    save_monthly_candles_to_db,
    save_monthly_indicators_to_db,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


_BASE_CONFIG = {
    "indicators": {
        "ema_periods": [9, 21, 50],
        "macd": {"fast": 12, "slow": 26, "signal": 9},
        "adx_period": 14,
        "rsi_period": 14,
        "stochastic": {"k": 14, "d": 3, "smooth_k": 3},
        "cci_period": 20,
        "williams_r_period": 14,
        "bollinger": {"period": 20, "std_dev": 2},
        "atr_period": 14,
        "keltner_period": 20,
        "cmf_period": 20,
    },
    "monthly": {},
}


def _make_ohlcv_row(
    day: date,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> dict:
    return {
        "date": day.isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _trading_days_for_month(year: int, month: int) -> list[date]:
    """Return Mon–Fri dates for the given month (approx trading days)."""
    result = []
    d = date(year, month, 1)
    next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    while d < next_month:
        if d.weekday() < 5:
            result.append(d)
        d += timedelta(days=1)
    return result


def _make_month_rows(year: int, month: int, base_price: float = 100.0) -> list[dict]:
    """Generate daily OHLCV rows for all trading days in a calendar month."""
    trading_days = _trading_days_for_month(year, month)
    rows = []
    for i, day in enumerate(trading_days):
        close = base_price + i * 0.5
        rows.append(_make_ohlcv_row(day, close - 1, close + 2, close - 2, close, 1_000_000.0))
    return rows


def _make_ohlcv_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _make_monthly_db(
    tmp_path,
    ticker: str = "AAPL",
    ohlcv_rows: list[dict] | None = None,
) -> sqlite3.Connection:
    """Create an in-memory-style SQLite DB with the full production schema."""
    from src.common.db import create_all_tables, get_connection
    db_path = tmp_path / "monthly_test.db"
    conn = get_connection(str(db_path))
    create_all_tables(conn)
    if ohlcv_rows:
        for row in ohlcv_rows:
            conn.execute(
                "INSERT OR REPLACE INTO ohlcv_daily "
                "(ticker, date, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ticker, row["date"], row["open"], row["high"],
                 row["low"], row["close"], row["volume"]),
            )
        conn.commit()
    return conn


# ── build_monthly_candles ─────────────────────────────────────────────────────


class TestBuildMonthlyCandles:

    def test_single_full_month_aggregates_correctly(self) -> None:
        """A full calendar month of daily bars produces exactly one monthly candle."""
        rows = _make_month_rows(2025, 1, base_price=100.0)
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["month_start"] == "2025-01-01"
        assert row["open"] == pytest.approx(rows[0]["open"], abs=0.01)
        assert row["close"] == pytest.approx(rows[-1]["close"], abs=0.01)
        assert row["high"] == pytest.approx(max(r["high"] for r in rows), abs=0.01)
        assert row["low"] == pytest.approx(min(r["low"] for r in rows), abs=0.01)
        assert row["volume"] == pytest.approx(sum(r["volume"] for r in rows), rel=1e-6)

    def test_month_start_is_first_of_month(self) -> None:
        """month_start key must always be YYYY-MM-01 regardless of what day of week it falls on."""
        rows = _make_month_rows(2025, 3)  # March 1 is a Saturday — first trading day is Mon 3rd
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert len(result) == 1
        assert result.iloc[0]["month_start"] == "2025-03-01"

    def test_two_months_produce_two_candles(self) -> None:
        """Two calendar months of daily bars produce exactly two monthly candles."""
        rows = _make_month_rows(2025, 1) + _make_month_rows(2025, 2)
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert len(result) == 2
        assert result.iloc[0]["month_start"] == "2025-01-01"
        assert result.iloc[1]["month_start"] == "2025-02-01"

    def test_partial_month_still_produces_candle(self) -> None:
        """A partial month (only a few trading days) still produces one candle."""
        rows = [
            _make_ohlcv_row(date(2025, 4, 1), 200.0, 202.0, 199.0, 201.0, 500_000),
            _make_ohlcv_row(date(2025, 4, 2), 201.0, 203.0, 200.0, 202.0, 600_000),
        ]
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert len(result) == 1
        assert result.iloc[0]["month_start"] == "2025-04-01"

    def test_open_is_first_trading_day_open(self) -> None:
        """Monthly open = open of the first trading day in the month."""
        rows = [
            _make_ohlcv_row(date(2025, 5, 1), 150.0, 155.0, 149.0, 152.0, 1_000_000),
            _make_ohlcv_row(date(2025, 5, 2), 152.0, 158.0, 151.0, 156.0, 1_100_000),
        ]
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert result.iloc[0]["open"] == pytest.approx(150.0)

    def test_close_is_last_trading_day_close(self) -> None:
        """Monthly close = close of the last trading day in the month."""
        rows = [
            _make_ohlcv_row(date(2025, 5, 1), 150.0, 155.0, 149.0, 152.0, 1_000_000),
            _make_ohlcv_row(date(2025, 5, 2), 152.0, 158.0, 151.0, 157.5, 1_100_000),
        ]
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        assert result.iloc[0]["close"] == pytest.approx(157.5)

    def test_empty_dataframe_returns_empty_result(self) -> None:
        """Empty input returns an empty DataFrame with correct columns."""
        df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        result = build_monthly_candles(df)
        assert result.empty
        assert "month_start" in result.columns

    def test_result_sorted_by_month_start_ascending(self) -> None:
        """Output rows are sorted by month_start ascending."""
        rows = _make_month_rows(2025, 3) + _make_month_rows(2025, 1) + _make_month_rows(2025, 2)
        df = _make_ohlcv_df(rows)
        result = build_monthly_candles(df)

        month_starts = result["month_start"].tolist()
        assert month_starts == sorted(month_starts)

    def test_string_dates_accepted(self) -> None:
        """Input DataFrame with string date column (not datetime) is handled correctly."""
        rows = _make_month_rows(2025, 6)
        df = pd.DataFrame(rows)  # date is already a string from isoformat()
        result = build_monthly_candles(df)
        assert len(result) == 1
        assert result.iloc[0]["month_start"] == "2025-06-01"


# ── save_monthly_candles_to_db ────────────────────────────────────────────────


class TestSaveMonthlyCandles:

    def test_saves_correct_values(self, tmp_path) -> None:
        """Saved rows should round-trip correctly from the DB."""
        rows = _make_month_rows(2025, 1)
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        conn = _make_monthly_db(tmp_path)
        count = save_monthly_candles_to_db(conn, "AAPL", monthly_df)

        assert count == 1
        row = conn.execute(
            "SELECT * FROM monthly_candles WHERE ticker='AAPL' AND month_start='2025-01-01'"
        ).fetchone()
        assert row is not None
        assert row["close"] == pytest.approx(monthly_df.iloc[0]["close"], abs=0.01)

    def test_idempotent_insert_or_replace(self, tmp_path) -> None:
        """Saving the same month twice should not create duplicate rows."""
        rows = _make_month_rows(2025, 2)
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        conn = _make_monthly_db(tmp_path)
        save_monthly_candles_to_db(conn, "AAPL", monthly_df)
        save_monthly_candles_to_db(conn, "AAPL", monthly_df)

        count = conn.execute(
            "SELECT COUNT(*) as n FROM monthly_candles WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert count == 1

    def test_empty_dataframe_saves_zero_rows(self, tmp_path) -> None:
        """Saving an empty DataFrame should save 0 rows and not error."""
        conn = _make_monthly_db(tmp_path)
        monthly_df = build_monthly_candles(pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"]))
        count = save_monthly_candles_to_db(conn, "AAPL", monthly_df)
        assert count == 0


# ── compute_monthly_indicators ────────────────────────────────────────────────


class TestComputeMonthlyIndicators:

    def test_returns_indicator_columns(self) -> None:
        """compute_monthly_indicators should add all 21 indicator columns to the DataFrame."""
        rows: list[dict] = []
        for month in range(1, 7):  # 6 months for some warm-up
            rows.extend(_make_month_rows(2024, month))
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        result = compute_monthly_indicators(monthly_df, _BASE_CONFIG)

        assert "month_start" in result.columns
        for col in ["ema_9", "rsi_14", "macd_line", "adx", "bb_pctb", "atr_14"]:
            assert col in result.columns, f"Expected indicator column '{col}' not found"

    def test_month_start_column_preserved(self) -> None:
        """month_start column must be preserved after indicator computation."""
        rows = []
        for month in range(1, 4):
            rows.extend(_make_month_rows(2024, month))
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        result = compute_monthly_indicators(monthly_df, _BASE_CONFIG)
        assert "month_start" in result.columns
        assert "date" not in result.columns

    def test_empty_input_returns_empty(self) -> None:
        """Empty DataFrame input returns an empty DataFrame."""
        empty_df = pd.DataFrame(columns=["month_start", "open", "high", "low", "close", "volume"])
        result = compute_monthly_indicators(empty_df, _BASE_CONFIG)
        assert result.empty


# ── save_monthly_indicators_to_db ─────────────────────────────────────────────


class TestSaveMonthlyIndicators:

    def test_saves_indicator_rows(self, tmp_path) -> None:
        """Computed indicators should be persisted to indicators_monthly."""
        rows = []
        for month in range(1, 7):
            rows.extend(_make_month_rows(2024, month))
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        indicators_df = compute_monthly_indicators(monthly_df, _BASE_CONFIG)

        conn = _make_monthly_db(tmp_path)
        count = save_monthly_indicators_to_db(conn, "AAPL", indicators_df)

        assert count > 0
        db_count = conn.execute(
            "SELECT COUNT(*) as n FROM indicators_monthly WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert db_count == count

    def test_idempotent_save(self, tmp_path) -> None:
        """Saving indicators twice should not create duplicate rows."""
        rows = []
        for month in range(1, 4):
            rows.extend(_make_month_rows(2024, month))
        df = _make_ohlcv_df(rows)
        monthly_df = build_monthly_candles(df)
        indicators_df = compute_monthly_indicators(monthly_df, _BASE_CONFIG)

        conn = _make_monthly_db(tmp_path)
        save_monthly_indicators_to_db(conn, "AAPL", indicators_df)
        save_monthly_indicators_to_db(conn, "AAPL", indicators_df)

        db_count = conn.execute(
            "SELECT COUNT(*) as n FROM indicators_monthly WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert db_count == len(indicators_df)


# ── compute_monthly_for_ticker ────────────────────────────────────────────────


class TestComputeMonthlyForTicker:

    def test_full_mode_end_to_end(self, tmp_path) -> None:
        """Full mode: reads ohlcv_daily, builds candles and indicators, saves both."""
        ohlcv_rows: list[dict] = []
        for month in range(1, 7):
            ohlcv_rows.extend(_make_month_rows(2024, month))

        conn = _make_monthly_db(tmp_path, "AAPL", ohlcv_rows)
        count = compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="full")

        assert count == 6
        candle_count = conn.execute(
            "SELECT COUNT(*) as n FROM monthly_candles WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert candle_count == 6

        indicator_count = conn.execute(
            "SELECT COUNT(*) as n FROM indicators_monthly WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert indicator_count == 6

    def test_no_ohlcv_returns_zero(self, tmp_path) -> None:
        """If no OHLCV data exists for the ticker, return 0 with no error."""
        conn = _make_monthly_db(tmp_path)
        count = compute_monthly_for_ticker(conn, "MISSING", _BASE_CONFIG, mode="full")
        assert count == 0

    def test_incremental_mode_only_adds_new_month(self, tmp_path) -> None:
        """Incremental mode: only new months (not already in monthly_candles) are added."""
        # Seed 3 months of OHLCV
        ohlcv_rows: list[dict] = []
        for month in range(1, 4):
            ohlcv_rows.extend(_make_month_rows(2024, month))
        conn = _make_monthly_db(tmp_path, "AAPL", ohlcv_rows)

        # First run in full mode
        compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="full")

        # Add a 4th month to ohlcv_daily
        for row in _make_month_rows(2024, 4):
            conn.execute(
                "INSERT OR REPLACE INTO ohlcv_daily "
                "(ticker, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("AAPL", row["date"], row["open"], row["high"],
                 row["low"], row["close"], row["volume"]),
            )
        conn.commit()

        # Incremental run should add only month 4
        new_count = compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="incremental")
        assert new_count == 1

        total = conn.execute(
            "SELECT COUNT(*) as n FROM monthly_candles WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert total == 4

    def test_incremental_falls_back_to_full_when_no_existing_data(self, tmp_path) -> None:
        """Incremental mode falls back to full mode when no monthly data exists yet."""
        ohlcv_rows: list[dict] = []
        for month in range(1, 3):
            ohlcv_rows.extend(_make_month_rows(2024, month))
        conn = _make_monthly_db(tmp_path, "AAPL", ohlcv_rows)

        count = compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="incremental")
        assert count == 2

    def test_full_mode_is_idempotent(self, tmp_path) -> None:
        """Running full mode twice produces no duplicate rows."""
        ohlcv_rows: list[dict] = []
        for month in range(1, 4):
            ohlcv_rows.extend(_make_month_rows(2024, month))
        conn = _make_monthly_db(tmp_path, "AAPL", ohlcv_rows)

        compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="full")
        compute_monthly_for_ticker(conn, "AAPL", _BASE_CONFIG, mode="full")

        count = conn.execute(
            "SELECT COUNT(*) as n FROM monthly_candles WHERE ticker='AAPL'"
        ).fetchone()["n"]
        assert count == 3
