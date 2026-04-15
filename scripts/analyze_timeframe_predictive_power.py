"""
Timeframe Predictive Power Analysis

For each active ticker, computes the Pearson R between:
  - daily_score (from scores_daily) and actual 10-day excess return vs SPY
  - weekly_score (from scores_daily) and actual 10-day excess return vs SPY

Outputs a per-ticker table (ticker | r_daily | r_weekly | winner) and an
aggregate summary showing which timeframe has stronger predictive power overall.

Usage:
    python scripts/analyze_timeframe_predictive_power.py

No existing files are modified. This is a read-only analysis script.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
from scipy.stats import pearsonr

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "config"
TICKERS_JSON = CONFIG_DIR / "tickers.json"
DATABASE_JSON = CONFIG_DIR / "database.json"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_db_path() -> str:
    """
    Load the database file path from config/database.json.

    Returns:
        Absolute path string to the SQLite database file.
    """
    with open(DATABASE_JSON) as fh:
        db_cfg = json.load(fh)
    relative_path = db_cfg["path"]
    return str(REPO_ROOT / relative_path)


def load_active_tickers() -> list[str]:
    """
    Load the list of active ticker symbols from config/tickers.json.

    Returns:
        List of ticker symbol strings.
    """
    with open(TICKERS_JSON) as fh:
        raw = json.load(fh)
    ticker_entries: list[dict] = raw["tickers"]
    return [entry["symbol"] for entry in ticker_entries if entry.get("active", True)]


# ---------------------------------------------------------------------------
# Excess return computation
# ---------------------------------------------------------------------------

def fetch_ticker_data(
    conn: sqlite3.Connection,
    ticker: str,
    forward_days: int = 10,
    min_samples: int = 30,
) -> tuple[list[float], list[float], list[float]]:
    """
    Fetch daily_score, weekly_score, and 10-day forward excess return vs SPY
    for all scored rows of a ticker that have sufficient future OHLCV data.

    Parameters:
        conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol to query.
        forward_days: Number of trading days ahead to measure the return.
        min_samples: Minimum number of valid (score, return) pairs required.
                     Returns empty lists if not met.

    Returns:
        Tuple of three equal-length lists:
            daily_scores, weekly_scores, excess_returns
        All lists are empty if fewer than min_samples valid pairs are found.
    """
    rows = conn.execute(
        """
        SELECT s.date, s.daily_score, s.weekly_score, o.close AS signal_close
        FROM scores_daily s
        JOIN ohlcv_daily o ON s.ticker = o.ticker AND s.date = o.date
        WHERE s.ticker = ?
          AND s.daily_score IS NOT NULL
          AND s.weekly_score IS NOT NULL
          AND o.close IS NOT NULL
          AND o.close > 0
        ORDER BY s.date
        """,
        (ticker,),
    ).fetchall()

    daily_scores: list[float] = []
    weekly_scores: list[float] = []
    excess_returns: list[float] = []

    for row in rows:
        signal_date: str = row["date"]
        daily_score: float = float(row["daily_score"])
        weekly_score: float = float(row["weekly_score"])
        signal_close: float = float(row["signal_close"])

        # Ticker's forward close (Nth trading day after signal_date)
        future_row = conn.execute(
            "SELECT close FROM ohlcv_daily "
            "WHERE ticker = ? AND date > ? "
            "ORDER BY date "
            "LIMIT 1 OFFSET ?",
            (ticker, signal_date, forward_days - 1),
        ).fetchone()
        if not future_row or future_row["close"] is None:
            continue
        ticker_return = (float(future_row["close"]) - signal_close) / signal_close * 100.0

        # SPY signal close
        spy_sig = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = 'SPY' AND date = ?",
            (signal_date,),
        ).fetchone()
        # SPY forward close (same Nth-day offset)
        spy_fwd = conn.execute(
            "SELECT close FROM ohlcv_daily "
            "WHERE ticker = 'SPY' AND date > ? "
            "ORDER BY date "
            "LIMIT 1 OFFSET ?",
            (signal_date, forward_days - 1),
        ).fetchone()

        if spy_sig and spy_fwd and spy_sig["close"] and float(spy_sig["close"]) > 0:
            spy_return = (float(spy_fwd["close"]) - float(spy_sig["close"])) / float(spy_sig["close"]) * 100.0
            excess_return = ticker_return - spy_return
        else:
            excess_return = ticker_return

        daily_scores.append(daily_score)
        weekly_scores.append(weekly_score)
        excess_returns.append(excess_return)

    if len(daily_scores) < min_samples:
        return [], [], []

    return daily_scores, weekly_scores, excess_returns


# ---------------------------------------------------------------------------
# Pearson R helper
# ---------------------------------------------------------------------------

def safe_pearsonr(xs: list[float], ys: list[float]) -> float:
    """
    Compute Pearson R between xs and ys, returning 0.0 on zero-variance inputs.

    Parameters:
        xs: First numeric series.
        ys: Second numeric series.

    Returns:
        Pearson correlation coefficient, or 0.0 if computation is not possible.
    """
    x_arr = np.array(xs, dtype=float)
    y_arr = np.array(ys, dtype=float)
    if np.std(x_arr) == 0.0 or np.std(y_arr) == 0.0:
        return 0.0
    r, _ = pearsonr(x_arr, y_arr)
    return float(r)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def run_analysis(forward_days: int = 10, min_samples: int = 30) -> None:
    """
    Run the full timeframe predictive power analysis.

    For each active ticker, computes Pearson R between daily_score and
    10-day excess return, and Pearson R between weekly_score and 10-day
    excess return. Prints a per-ticker table with a winner column, then
    an aggregate summary.

    Parameters:
        forward_days: Trading-day horizon for forward return measurement.
        min_samples: Minimum valid pairs required per ticker to include it.
    """
    db_path = load_db_path()
    tickers = load_active_tickers()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    print(f"\nTimeframe Predictive Power — Pearson R (10-day excess return vs SPY)")
    print(f"Tickers in watchlist: {len(tickers)}  |  Forward days: {forward_days}  |  Min samples: {min_samples}\n")

    col_widths = (8, 9, 9, 8)
    header = (
        f"{'Ticker':<{col_widths[0]}} "
        f"{'r_daily':>{col_widths[1]}} "
        f"{'r_weekly':>{col_widths[2]}} "
        f"{'winner':<{col_widths[3]}}"
    )
    separator = "-" * len(header)
    print(header)
    print(separator)

    r_daily_list: list[float] = []
    r_weekly_list: list[float] = []
    skipped: list[str] = []

    for ticker in sorted(tickers):
        daily_scores, weekly_scores, excess_returns = fetch_ticker_data(
            conn, ticker, forward_days=forward_days, min_samples=min_samples
        )

        if not daily_scores:
            skipped.append(ticker)
            continue

        r_daily = safe_pearsonr(daily_scores, excess_returns)
        r_weekly = safe_pearsonr(weekly_scores, excess_returns)

        if abs(r_daily) >= abs(r_weekly):
            winner = "daily"
        else:
            winner = "weekly"

        r_daily_list.append(r_daily)
        r_weekly_list.append(r_weekly)

        print(
            f"{ticker:<{col_widths[0]}} "
            f"{r_daily:>{col_widths[1]}.4f} "
            f"{r_weekly:>{col_widths[2]}.4f} "
            f"{winner:<{col_widths[3]}}"
        )

    conn.close()

    # Aggregate summary
    print(separator)

    if not r_daily_list:
        print("No tickers had sufficient data.")
        return

    avg_r_daily = float(np.mean(r_daily_list))
    avg_r_weekly = float(np.mean(r_weekly_list))
    overall_winner = "daily" if abs(avg_r_daily) >= abs(avg_r_weekly) else "weekly"

    print(
        f"{'AVERAGE':<{col_widths[0]}} "
        f"{avg_r_daily:>{col_widths[1]}.4f} "
        f"{avg_r_weekly:>{col_widths[2]}.4f} "
        f"{overall_winner:<{col_widths[3]}}"
    )

    print(f"\n{'─'*45}")
    print(f"  Tickers analysed : {len(r_daily_list)}")
    print(f"  Tickers skipped  : {len(skipped)} (< {min_samples} valid samples)")
    if skipped:
        print(f"  Skipped list     : {', '.join(skipped)}")
    print(f"  avg r_daily      : {avg_r_daily:+.4f}")
    print(f"  avg r_weekly     : {avg_r_weekly:+.4f}")
    print(f"  Overall winner   : {overall_winner.upper()} → stronger predictive power")
    print(f"{'─'*45}\n")


if __name__ == "__main__":
    run_analysis()
