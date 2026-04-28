"""
Weekly candle construction and weekly indicator computation.

Constructs weekly OHLCV candles from daily data:
  - open   = first trading day's open for the week
  - high   = maximum high across all trading days in the week
  - low    = minimum low across all trading days in the week
  - close  = last trading day's close for the week
  - volume = sum of all trading days' volume

The week is keyed by its Monday calendar date (regardless of whether Monday
was a trading day). Uses pd.Grouper(freq='W-MON') for grouping.

Then computes the same 15 technical indicators on the weekly candles by
reusing compute_all_indicators() from indicators.py.

Weekly indicators are used for dual timeframe confirmation in the scorer:
    Final Score = (Daily Score × 0.6) + (Weekly Score × 0.4)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from src.calculator.crossovers import detect_crossovers_for_ticker
from src.calculator.divergences import detect_divergences_for_ticker
from src.calculator.indicators import compute_all_indicators
from src.calculator.patterns import detect_all_patterns_for_ticker
from src.calculator.profiles import compute_profile_for_ticker
from src.calculator.support_resistance import detect_support_resistance_for_ticker
from src.calculator.swing_points import detect_swing_points_for_ticker
from src.common.events import log_alert

logger = logging.getLogger(__name__)

_PHASE = "calculator-weekly"


def build_weekly_candles(
    ohlcv_df: pd.DataFrame,
    week_start_day: str = "Monday",
) -> pd.DataFrame:
    """
    Aggregate daily OHLCV into weekly candles.

    Each week is identified by its Monday calendar date. The candle is built as:
      - week_start: Monday's date (ISO string, even if Monday was a market holiday)
      - open:   first trading day's open in the week
      - high:   max(all highs in the week)
      - low:    min(all lows in the week)
      - close:  last trading day's close in the week
      - volume: sum(all volumes in the week)

    Args:
        ohlcv_df: DataFrame with columns: date (datetime or string), open, high, low,
                  close, volume. Must be sorted by date ascending.
        week_start_day: Day that defines the start of a week. Defaults to 'Monday'.
                        Only 'Monday' is currently supported.

    Returns:
        DataFrame with columns: week_start (ISO string), open, high, low, close, volume.
        Sorted by week_start ascending.
    """
    if ohlcv_df.empty:
        return pd.DataFrame(columns=["week_start", "open", "high", "low", "close", "volume"])

    df = ohlcv_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Compute the Monday of each date's ISO week:
    #   weekday() = 0 for Monday, ..., 4 for Friday
    #   Subtracting weekday() days always gives the preceding (or same-day) Monday.
    df["_week_start"] = df["date"].apply(
        lambda dt: (dt - pd.Timedelta(days=dt.weekday())).date().isoformat()
    )

    records = []
    for week_start_key, group in df.groupby("_week_start", sort=True):
        if group.empty:
            continue
        records.append(
            {
                "week_start": week_start_key,
                "open": float(group["open"].iloc[0]),
                "high": float(group["high"].max()),
                "low": float(group["low"].min()),
                "close": float(group["close"].iloc[-1]),
                "volume": float(group["volume"].sum()),
            }
        )

    result = pd.DataFrame(records)
    return result.sort_values("week_start").reset_index(drop=True)


def save_weekly_candles_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    weekly_df: pd.DataFrame,
) -> int:
    """
    Persist weekly candles to the weekly_candles table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the weekly_candles table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        weekly_df: DataFrame returned by build_weekly_candles().

    Returns:
        Number of rows saved.
    """
    count = 0
    for _, row in weekly_df.iterrows():
        db_conn.execute(
            """
            INSERT OR REPLACE INTO weekly_candles
                (ticker, week_start, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                row["week_start"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            ),
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} weekly candles for ticker={ticker}")
    return count


def compute_weekly_indicators(
    weekly_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Compute technical indicators on weekly candle data.

    Reuses compute_all_indicators() from indicators.py with the same config
    parameters as the daily computation. The weekly DataFrame uses week_start
    as the date column (renamed internally for compatibility).

    Args:
        weekly_df: DataFrame with columns: week_start, open, high, low, close, volume.
        config: Calculator config dict (same as used for daily indicators).

    Returns:
        Copy of weekly_df with all indicator columns added. week_start is preserved.
    """
    if weekly_df.empty:
        return weekly_df.copy()

    df = weekly_df.copy()
    # compute_all_indicators expects a 'date' column; rename for compatibility
    df = df.rename(columns={"week_start": "date"})
    df_with_indicators = compute_all_indicators(df, config)
    # Rename back to week_start
    df_with_indicators = df_with_indicators.rename(columns={"date": "week_start"})
    return df_with_indicators


def save_weekly_indicators_to_db(
    db_conn: sqlite3.Connection,
    ticker: str,
    indicators_df: pd.DataFrame,
) -> int:
    """
    Persist weekly indicator values to the indicators_weekly table.

    Uses INSERT OR REPLACE for idempotency. Commits after all rows are inserted.

    Args:
        db_conn: Open SQLite connection with the indicators_weekly table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        indicators_df: DataFrame returned by compute_weekly_indicators().

    Returns:
        Number of rows saved.
    """
    indicator_cols = [
        "ema_9", "ema_21", "ema_50",
        "macd_line", "macd_signal", "macd_histogram",
        "adx", "rsi_14", "stoch_k", "stoch_d",
        "cci_20", "williams_r", "obv", "cmf_20", "ad_line",
        "bb_upper", "bb_lower", "bb_pctb",
        "atr_14", "keltner_upper", "keltner_lower",
    ]

    def _nan_to_none(val: object) -> object:
        if isinstance(val, float) and (val != val):  # NaN check
            return None
        try:
            import math
            if math.isnan(float(val)):  # type: ignore[arg-type]
                return None
        except (TypeError, ValueError):
            pass
        return val

    count = 0
    for _, row in indicators_df.iterrows():
        values = [_nan_to_none(row.get(col)) for col in indicator_cols]
        db_conn.execute(
            f"""
            INSERT OR REPLACE INTO indicators_weekly
                (ticker, week_start, {', '.join(indicator_cols)})
            VALUES (?, ?, {', '.join(['?'] * len(indicator_cols))})
            """,
            [ticker, row["week_start"]] + values,
        )
        count += 1
    db_conn.commit()
    logger.info(f"Saved {count} weekly indicator rows for ticker={ticker}")
    return count


def compute_weekly_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    mode: str = "full",
    *,
    skip_event_detection: bool = False,
) -> int:
    """
    End-to-end weekly computation for one ticker.

    Pipeline (per ticker, per call):
      1. Build weekly candles from daily OHLCV and persist them.
      2. Compute weekly indicators on those candles and persist them.

    When ``skip_event_detection`` is False (default — regular tickers), the
    following six sub-steps then run against the weekly mirror tables:

      3. ``swing_points_weekly``        (depends on weekly candles)
      4. ``support_resistance_weekly``  (depends on swing_points_weekly + candles)
      5. ``patterns_weekly``            (candlestick + structural; needs S/R)
      6. ``divergences_weekly``         (needs swing_points + indicators)
      7. ``crossovers_weekly``          (needs indicators)
      8. ``indicator_profiles_weekly``  (needs indicators)

    Each of the six is wrapped in its own try/except so a single detector
    failure does not abort the rest. Failures are logged and recorded in
    ``alerts_log`` under ``phase='calculator-weekly'``.

    When ``skip_event_detection`` is True (sector ETFs and market benchmarks),
    all six sub-steps are skipped — matching the daily ETF policy in
    ``run_calculator_for_etfs_and_benchmarks`` (only indicators + candles run
    for ETFs/benchmarks).

    Modes:
      'full':        Load ALL daily OHLCV, rebuild all weekly candles from scratch.
      'incremental': Find the latest week_start in weekly_candles, load daily OHLCV
                     from 2 weeks before that date onward (to recompute the last
                     partial week), build candles for the new period. For indicator
                     computation, loads existing weekly candles + new ones so the
                     indicator warm-up window is satisfied. The six event/profile
                     sub-steps re-run on the FULL ticker history each call —
                     none of them operate on a date window. The cost is acceptable
                     because weekly bar counts are 5x fewer than daily.

    Args:
        db_conn: Open SQLite connection with ohlcv_daily, weekly_candles,
                 indicators_weekly and the six weekly mirror tables.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict.
        mode: 'full' or 'incremental'. Defaults to 'full'.
        skip_event_detection: When True, skip the six event/profile sub-steps
            (swing_points, S/R, patterns, divergences, crossovers, profiles).
            Used for ETF/benchmark tickers to mirror the daily ETF policy.

    Returns:
        Number of weekly candles created/updated. (The six sub-steps may
        succeed, partially fail, or be skipped — see ``alerts_log`` for any
        per-step failures.)
    """
    week_start_day = config.get("weekly", {}).get("week_start_day", "Monday")

    if mode == "incremental":
        return _compute_weekly_incremental(
            db_conn,
            ticker,
            config,
            week_start_day,
            skip_event_detection=skip_event_detection,
        )

    # --- full mode ---
    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        logger.warning(f"No OHLCV data found for ticker={ticker}")
        return 0

    ohlcv_df = pd.DataFrame([dict(row) for row in rows])
    ohlcv_df["date"] = pd.to_datetime(ohlcv_df["date"])

    weekly_df = build_weekly_candles(ohlcv_df, week_start_day=week_start_day)
    if weekly_df.empty:
        return 0

    save_weekly_candles_to_db(db_conn, ticker, weekly_df)
    indicators_df = compute_weekly_indicators(weekly_df, config)
    save_weekly_indicators_to_db(db_conn, ticker, indicators_df)

    if not skip_event_detection:
        _run_weekly_subpipeline(db_conn, ticker, config)

    return len(weekly_df)


def _run_weekly_subpipeline(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
) -> None:
    """
    Run the six event/profile detectors against the weekly mirror tables.

    Each detector is wrapped in its own try/except so a single failure does
    not block the rest. Failures are logged and recorded in ``alerts_log``
    under ``phase='calculator-weekly'``. This mirrors the per-step error
    handling of the daily orchestrator in ``src/calculator/main.py``.

    Note on profiles: ``config['profiles']['rolling_window_days']`` is tuned
    for daily data (default 504 trading days ≈ 2 years). On weekly bars this
    window will exceed available history; ``compute_profile_for_ticker``
    falls back to using all available data with a warning, which is
    acceptable here.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    swing_ok = False
    try:
        detect_swing_points_for_ticker(
            db_conn,
            ticker,
            config,
            source_candles_table="weekly_candles",
            source_date_column="week_start",
            dest_table="swing_points_weekly",
            date_column_name="week_start",
        )
        swing_ok = True
    except Exception as exc:
        logger.error(
            f"ticker={ticker} phase={_PHASE} step=swing_points error={exc}",
            exc_info=True,
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", f"swing_points failed: {exc}")

    sr_ok = False
    if swing_ok:
        try:
            detect_support_resistance_for_ticker(
                db_conn,
                ticker,
                config,
                source_swing_table="swing_points_weekly",
                source_swing_date_column="week_start",
                source_candles_table="weekly_candles",
                source_candles_date_column="week_start",
                dest_table="support_resistance_weekly",
                dest_date_column="week_start",
            )
            sr_ok = True
        except Exception as exc:
            logger.error(
                f"ticker={ticker} phase={_PHASE} step=support_resistance error={exc}",
                exc_info=True,
            )
            log_alert(
                db_conn, ticker, today, _PHASE, "error",
                f"support_resistance failed: {exc}",
            )

    if swing_ok and sr_ok:
        try:
            detect_all_patterns_for_ticker(
                db_conn,
                ticker,
                config,
                source_candles_table="weekly_candles",
                source_candles_date_column="week_start",
                source_indicators_table="indicators_weekly",
                source_indicators_date_column="week_start",
                source_swing_table="swing_points_weekly",
                source_swing_date_column="week_start",
                source_sr_table="support_resistance_weekly",
                dest_table="patterns_weekly",
                dest_date_column="week_start",
            )
        except Exception as exc:
            logger.error(
                f"ticker={ticker} phase={_PHASE} step=patterns error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", f"patterns failed: {exc}")

    if swing_ok:
        try:
            detect_divergences_for_ticker(
                db_conn,
                ticker,
                config,
                source_swing_table="swing_points_weekly",
                source_swing_date_column="week_start",
                source_indicators_table="indicators_weekly",
                source_indicators_date_column="week_start",
                dest_table="divergences_weekly",
                dest_date_column="week_start",
            )
        except Exception as exc:
            logger.error(
                f"ticker={ticker} phase={_PHASE} step=divergences error={exc}",
                exc_info=True,
            )
            log_alert(
                db_conn, ticker, today, _PHASE, "error", f"divergences failed: {exc}",
            )

    try:
        detect_crossovers_for_ticker(
            db_conn,
            ticker,
            config,
            source_indicators_table="indicators_weekly",
            source_indicators_date_column="week_start",
            dest_table="crossovers_weekly",
            dest_date_column="week_start",
        )
    except Exception as exc:
        logger.error(
            f"ticker={ticker} phase={_PHASE} step=crossovers error={exc}",
            exc_info=True,
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", f"crossovers failed: {exc}")

    try:
        compute_profile_for_ticker(
            db_conn,
            ticker,
            config,
            source_indicators_table="indicators_weekly",
            source_indicators_date_column="week_start",
            dest_table="indicator_profiles_weekly",
        )
    except Exception as exc:
        logger.error(
            f"ticker={ticker} phase={_PHASE} step=profiles error={exc}",
            exc_info=True,
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", f"profiles failed: {exc}")


def _compute_weekly_incremental(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    week_start_day: str,
    *,
    skip_event_detection: bool = False,
) -> int:
    """
    Incremental weekly computation: only process new/updated weeks.

    Finds the latest week_start already in weekly_candles, loads daily OHLCV
    from 14 days before that date (to recompute the last potentially partial week),
    and merges with existing weekly candles for indicator computation.

    The six event/profile sub-steps re-run on the FULL ticker history every
    incremental call. This is intentional: patterns / divergences / S/R
    depend on the global swing-point series, so a date-windowed run could
    miss patterns whose anchor falls outside the window. The cost is bounded
    because weekly bar counts are 5x fewer than daily.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
        week_start_day: Week start day string.
        skip_event_detection: When True, skip the six sub-steps (ETF policy).

    Returns:
        Number of new weekly candle rows saved.
    """
    latest_row = db_conn.execute(
        "SELECT MAX(week_start) AS latest FROM weekly_candles WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    if latest_row is None or latest_row["latest"] is None:
        # No existing data — fall back to full mode
        logger.info(f"No existing weekly data for ticker={ticker}, running full computation")
        return compute_weekly_for_ticker(
            db_conn, ticker, config, mode="full",
            skip_event_detection=skip_event_detection,
        )

    latest_week_start = latest_row["latest"]
    # Load daily OHLCV from 2 weeks before the latest week_start
    latest_dt = datetime.fromisoformat(latest_week_start)
    cutoff_date = (latest_dt - timedelta(days=14)).date().isoformat()

    rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? AND date >= ? ORDER BY date ASC",
        (ticker, cutoff_date),
    ).fetchall()

    if not rows:
        logger.info(f"No new OHLCV data for ticker={ticker} since {cutoff_date}")
        return 0

    new_ohlcv_df = pd.DataFrame([dict(row) for row in rows])
    new_ohlcv_df["date"] = pd.to_datetime(new_ohlcv_df["date"])
    new_weekly = build_weekly_candles(new_ohlcv_df, week_start_day=week_start_day)

    if new_weekly.empty:
        return 0

    # Save only genuinely new weeks (week_start > previous latest)
    new_weeks = new_weekly[new_weekly["week_start"] > latest_week_start]
    if new_weeks.empty:
        logger.info(f"No new weeks for ticker={ticker}")
        return 0

    save_weekly_candles_to_db(db_conn, ticker, new_weeks)

    # For indicator computation, load ALL existing weekly candles + new ones
    # so that warm-up periods are satisfied
    all_candle_rows = db_conn.execute(
        "SELECT week_start, open, high, low, close, volume FROM weekly_candles "
        "WHERE ticker = ? ORDER BY week_start ASC",
        (ticker,),
    ).fetchall()
    all_weekly_df = pd.DataFrame([dict(row) for row in all_candle_rows])
    indicators_df = compute_weekly_indicators(all_weekly_df, config)

    # Save only the new weeks' indicators
    new_week_starts = set(new_weeks["week_start"].tolist())
    new_indicators = indicators_df[indicators_df["week_start"].isin(new_week_starts)]
    save_weekly_indicators_to_db(db_conn, ticker, new_indicators)

    # Re-detect events / profiles on the full ticker history. Each detector
    # reads everything from its source table (no date window), so re-running
    # is the only way to keep the mirror tables consistent after new bars.
    if not skip_event_detection:
        _run_weekly_subpipeline(db_conn, ticker, config)

    return len(new_weeks)
