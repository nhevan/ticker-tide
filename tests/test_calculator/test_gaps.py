"""
Tests for src/calculator/gaps.py

Covers:
- detect_gaps: gap up, gap down, no gap, gap_size_pct, volume_ratio
- classify_gap: breakaway, common, continuation, exhaustion
- detect_and_classify_gaps: full pipeline
- save_gaps_to_db: delete + replace semantics
- detect_gaps_for_ticker: end-to-end
"""

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest

from src.calculator.gaps import (
    detect_and_classify_gaps,
    detect_gaps,
    detect_gaps_for_ticker,
    save_gaps_to_db,
)


# ── Local fixtures ──────────────────────────────────────────────────────────────


def _make_date(offset: int, base: str = "2024-01-02") -> str:
    return (date.fromisoformat(base) + timedelta(days=offset)).isoformat()


def _make_flat_ohlcv(n: int, price: float = 100.0, volume: float = 200_000.0) -> pd.DataFrame:
    """Generate flat OHLCV data with no gaps and consistent volume."""
    records = []
    for i in range(n):
        records.append({
            "date": _make_date(i),
            "open": price,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price,
            "volume": volume,
        })
    return pd.DataFrame(records)


def _make_trending_ohlcv(n: int, start: float = 100.0, daily_gain: float = 1.0) -> pd.DataFrame:
    """Generate OHLCV data with a steady uptrend (no gaps)."""
    records = []
    price = start
    for i in range(n):
        records.append({
            "date": _make_date(i),
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price + daily_gain,
            "volume": 200_000.0,
        })
        price += daily_gain
    return pd.DataFrame(records)


@pytest.fixture
def default_config() -> dict:
    """Return calculator config with gap settings."""
    return {
        "gaps": {
            "volume_breakaway_threshold": 2.0,
            "volume_average_period": 20,
        }
    }


# ── detect_gaps ─────────────────────────────────────────────────────────────────


def test_detect_gap_up(default_config: dict) -> None:
    """Day 2's low (105) > day 1's high (100) → gap up detected with ~5% size."""
    records = [
        {"date": _make_date(0), "open": 98.0, "high": 100.0, "low": 96.0, "close": 99.0, "volume": 200_000.0},
        {"date": _make_date(1), "open": 106.0, "high": 108.0, "low": 105.0, "close": 107.0, "volume": 300_000.0},
    ]
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["direction"] == "up"
    assert gaps[0]["gap_size_pct"] == pytest.approx(
        (105.0 - 100.0) / 99.0 * 100, rel=0.01
    )


def test_detect_gap_down(default_config: dict) -> None:
    """Day 2's high (95) < day 1's low (100) → gap down detected with negative size."""
    records = [
        {"date": _make_date(0), "open": 102.0, "high": 104.0, "low": 100.0, "close": 101.0, "volume": 200_000.0},
        {"date": _make_date(1), "open": 94.0, "high": 95.0, "low": 92.0, "close": 93.0, "volume": 300_000.0},
    ]
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["direction"] == "down"
    assert gaps[0]["gap_size_pct"] < 0


def test_detect_no_gap(default_config: dict) -> None:
    """Overlapping price ranges → no gap detected."""
    records = [
        {"date": _make_date(0), "open": 100.0, "high": 103.0, "low": 99.0, "close": 101.0, "volume": 200_000.0},
        {"date": _make_date(1), "open": 101.0, "high": 104.0, "low": 100.0, "close": 102.0, "volume": 200_000.0},
    ]
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 0


def test_gap_size_pct_calculation(default_config: dict) -> None:
    """Gap from high=100 to low=103 → gap_size_pct = 3.0%."""
    records = [
        {"date": _make_date(0), "open": 98.0, "high": 100.0, "low": 96.0, "close": 100.0, "volume": 200_000.0},
        {"date": _make_date(1), "open": 104.0, "high": 106.0, "low": 103.0, "close": 105.0, "volume": 300_000.0},
    ]
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["gap_size_pct"] == pytest.approx(3.0, rel=0.01)


def test_gap_volume_ratio(default_config: dict) -> None:
    """volume=500_000, 20-day avg=200_000 → volume_ratio ≈ 2.5."""
    # Build 21 rows: first 20 with avg vol=200_000, then a gap row with vol=500_000
    records = []
    for i in range(20):
        records.append({
            "date": _make_date(i),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 200_000.0,
        })
    # Gap up row
    records.append({
        "date": _make_date(20),
        "open": 106.0,
        "high": 108.0,
        "low": 105.0,
        "close": 107.0,
        "volume": 500_000.0,
    })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["volume_ratio"] == pytest.approx(2.5, rel=0.05)


def test_detect_multiple_gaps(default_config: dict) -> None:
    """60 days of data with 3 gaps → all 3 detected with correct dates."""
    records = []
    price = 100.0
    gap_indices = {10, 30, 50}
    for i in range(60):
        if i in gap_indices:
            # Create a gap up: previous close was price, today's low is price+5
            records.append({
                "date": _make_date(i),
                "open": price + 6.0,
                "high": price + 8.0,
                "low": price + 5.0,
                "close": price + 7.0,
                "volume": 200_000.0,
            })
            price += 7.0
        else:
            records.append({
                "date": _make_date(i),
                "open": price,
                "high": price + 0.5,
                "low": price - 0.5,
                "close": price,
                "volume": 200_000.0,
            })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_gaps(ohlcv_df, default_config)

    assert len(gaps) == 3


# ── classify_gap ─────────────────────────────────────────────────────────────────


def test_gap_classification_breakaway(default_config: dict) -> None:
    """Gap with volume > 2x average → gap_type='breakaway'."""
    # 25 rows: 20 flat consolidation (low ADX context), then a gap with high volume
    records = []
    for i in range(20):
        records.append({
            "date": _make_date(i),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 200_000.0,
        })
    # Gap row with 3x average volume
    records.append({
        "date": _make_date(20),
        "open": 106.0,
        "high": 108.0,
        "low": 105.0,
        "close": 107.0,
        "volume": 600_000.0,
    })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_and_classify_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["gap_type"] == "breakaway"


def test_gap_classification_common(default_config: dict) -> None:
    """Gap with normal volume → gap_type='common'."""
    records = []
    for i in range(20):
        records.append({
            "date": _make_date(i),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 200_000.0,
        })
    # Gap row with normal volume (< 1.5x average)
    records.append({
        "date": _make_date(20),
        "open": 103.5,
        "high": 105.0,
        "low": 102.5,
        "close": 104.0,
        "volume": 220_000.0,  # ~1.1x average
    })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_and_classify_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["gap_type"] == "common"


def test_gap_classification_continuation(default_config: dict) -> None:
    """Gap in the middle of an existing uptrend → gap_type='continuation'."""
    # Build 15 days of uptrend, then a gap in the middle of the trend
    records = _make_trending_ohlcv(15).to_dict("records")
    # Gap at row 15 (continuing the uptrend): price was at ~115 after 15 steps of +1
    last_price = 100.0 + 15 * 1.0
    records.append({
        "date": _make_date(15),
        "open": last_price + 4.0,
        "high": last_price + 6.0,
        "low": last_price + 3.0,
        "close": last_price + 5.0,
        "volume": 250_000.0,  # moderate volume (< 2x threshold)
    })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_and_classify_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["gap_type"] == "continuation"


def test_gap_classification_exhaustion(default_config: dict) -> None:
    """Gap after extended trend (30+ days) with declining volume → gap_type='exhaustion'."""
    # 30 days of uptrend with declining volume, then a gap
    records = []
    price = 100.0
    for i in range(30):
        # Declining volume over time
        vol = 400_000.0 - i * 10_000.0
        records.append({
            "date": _make_date(i),
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price + 1.0,
            "volume": max(vol, 50_000.0),
        })
        price += 1.0
    # Gap row: moderate volume (< 2x avg, so not breakaway)
    records.append({
        "date": _make_date(30),
        "open": price + 4.0,
        "high": price + 6.0,
        "low": price + 3.0,
        "close": price + 5.0,
        "volume": 250_000.0,
    })
    ohlcv_df = pd.DataFrame(records)

    gaps = detect_and_classify_gaps(ohlcv_df, default_config)

    assert len(gaps) == 1
    assert gaps[0]["gap_type"] == "exhaustion"


# ── save_gaps_to_db ──────────────────────────────────────────────────────────────


def test_save_gaps_to_db(db_connection: sqlite3.Connection, default_config: dict) -> None:
    """save_gaps_to_db writes gap records to gaps_daily."""
    gaps = [
        {
            "date": "2024-01-05",
            "direction": "up",
            "gap_type": "breakaway",
            "gap_size_pct": 3.5,
            "volume_ratio": 2.5,
            "filled": False,
        },
        {
            "date": "2024-01-10",
            "direction": "down",
            "gap_type": "common",
            "gap_size_pct": -2.0,
            "volume_ratio": 1.1,
            "filled": False,
        },
    ]

    count = save_gaps_to_db(db_connection, "AAPL", gaps)

    assert count == 2
    cursor = db_connection.execute("SELECT COUNT(*) FROM gaps_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] == 2


def test_save_gaps_clears_old_for_ticker(db_connection: sqlite3.Connection) -> None:
    """Saving new gaps replaces all existing ones for the ticker."""
    old_gaps = [
        {"date": "2024-01-03", "direction": "up", "gap_type": "common",
         "gap_size_pct": 2.0, "volume_ratio": 1.2, "filled": False},
        {"date": "2024-01-04", "direction": "down", "gap_type": "common",
         "gap_size_pct": -1.5, "volume_ratio": 1.1, "filled": False},
    ]
    new_gaps = [
        {"date": "2024-01-10", "direction": "up", "gap_type": "breakaway",
         "gap_size_pct": 4.0, "volume_ratio": 2.8, "filled": False},
    ]

    save_gaps_to_db(db_connection, "AAPL", old_gaps)
    save_gaps_to_db(db_connection, "AAPL", new_gaps)

    cursor = db_connection.execute("SELECT COUNT(*) FROM gaps_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] == 1  # Old ones replaced


# ── detect_gaps_for_ticker ───────────────────────────────────────────────────────


def test_detect_gaps_for_ticker_end_to_end(
    db_connection: sqlite3.Connection, default_config: dict
) -> None:
    """Insert OHLCV into DB with gaps, call detect_gaps_for_ticker, verify gaps_daily populated."""
    records = []
    for i in range(20):
        records.append((
            "AAPL", _make_date(i),
            100.0, 101.0, 99.0, 100.0, 200_000.0,
        ))
    # Gap at row 20
    records.append((
        "AAPL", _make_date(20),
        106.0, 108.0, 105.0, 107.0, 600_000.0,
    ))

    db_connection.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
        records,
    )
    db_connection.commit()

    count = detect_gaps_for_ticker(db_connection, "AAPL", default_config)

    assert count > 0
    cursor = db_connection.execute("SELECT COUNT(*) FROM gaps_daily WHERE ticker='AAPL'")
    assert cursor.fetchone()[0] > 0
