"""
Technical indicator computation using the `ta` library.

Computes all 15 indicators from OHLCV data and stores them in the
indicators_daily table. All parameters are read from config/calculator.json.

Indicators computed:
    Trend:    EMA (9, 21, 50), MACD (12, 26, 9), ADX (14)
    Momentum: RSI (14), Stochastic (14, 3, 3), CCI (20), Williams %R (14)
    Volume:   OBV, CMF (20), A/D Line
    Volatility: Bollinger Bands (20, 2), ATR (14), Keltner Channels (20)

Uses the ta library class-based API:
    ta.trend.EMAIndicator(close, window=9).ema_indicator()
    ta.momentum.RSIIndicator(close, window=14).rsi()
    etc.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import ta.momentum
import ta.trend
import ta.volatility
import ta.volume

logger = logging.getLogger(__name__)

_INDICATOR_COLUMNS = [
    "ema_9", "ema_21", "ema_50",
    "macd_line", "macd_signal", "macd_histogram",
    "adx",
    "rsi_14",
    "stoch_k", "stoch_d",
    "cci_20",
    "williams_r",
    "obv",
    "cmf_20",
    "ad_line",
    "bb_upper", "bb_lower", "bb_pctb",
    "atr_14",
    "keltner_upper", "keltner_lower",
]


def load_ohlcv_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Query ohlcv_daily for the given ticker, optionally filtered by date range.

    Args:
        db_conn: Open SQLite connection with row_factory = sqlite3.Row.
        ticker: Ticker symbol (e.g. 'AAPL').
        start_date: Optional ISO date string for the lower bound (inclusive).
        end_date: Optional ISO date string for the upper bound (inclusive).

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, sorted by date ascending.
        Returns an empty DataFrame if no data is found.
    """
    query = "SELECT date, open, high, low, close, volume FROM ohlcv_daily WHERE ticker = ?"
    params: list = [ticker]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date ASC"

    cursor = db_conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    return pd.DataFrame([dict(row) for row in rows])


def compute_all_indicators(ohlcv_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Compute all 15 technical indicators using the ta library and add them as columns.

    Reads all parameters from config["indicators"]. Returns the original DataFrame
    with new indicator columns appended. Handles empty DataFrames and insufficient
    data gracefully (indicator columns will contain NaN for warm-up rows).

    Args:
        ohlcv_df: DataFrame with columns: open, high, low, close, volume.
        config: Calculator config dict containing an "indicators" key.

    Returns:
        The input DataFrame with all indicator columns added.
    """
    if ohlcv_df.empty:
        return ohlcv_df.copy()

    df = ohlcv_df.copy()
    ind_cfg = config["indicators"]

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    _compute_trend_indicators(df, close, high, low, ind_cfg)
    _compute_momentum_indicators(df, close, high, low, ind_cfg)
    _compute_volume_indicators(df, close, high, low, volume, ind_cfg)
    _compute_volatility_indicators(df, close, high, low, ind_cfg)

    return df


def _compute_trend_indicators(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ind_cfg: dict,
) -> None:
    """Add EMA, MACD, and ADX columns to df in place."""
    for period in ind_cfg["ema_periods"]:
        df[f"ema_{period}"] = ta.trend.EMAIndicator(close=close, window=period).ema_indicator()

    macd_cfg = ind_cfg["macd"]
    macd = ta.trend.MACD(
        close=close,
        window_slow=macd_cfg["slow"],
        window_fast=macd_cfg["fast"],
        window_sign=macd_cfg["signal"],
    )
    df["macd_line"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_histogram"] = macd.macd_diff()

    try:
        df["adx"] = ta.trend.ADXIndicator(
            high=high, low=low, close=close, window=ind_cfg["adx_period"]
        ).adx()
    except (ValueError, IndexError):
        # Insufficient data for ADX (needs at least 2 × window rows)
        df["adx"] = float("nan")


def _compute_momentum_indicators(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ind_cfg: dict,
) -> None:
    """Add RSI, Stochastic, CCI, and Williams %R columns to df in place."""
    df["rsi_14"] = ta.momentum.RSIIndicator(
        close=close, window=ind_cfg["rsi_period"]
    ).rsi()

    stoch_cfg = ind_cfg["stochastic"]
    stoch = ta.momentum.StochasticOscillator(
        high=high,
        low=low,
        close=close,
        window=stoch_cfg["k"],
        smooth_window=stoch_cfg["smooth_k"],
    )
    df["stoch_k"] = stoch.stoch()
    df["stoch_d"] = stoch.stoch_signal()

    df["cci_20"] = ta.trend.CCIIndicator(
        high=high, low=low, close=close, window=ind_cfg["cci_period"]
    ).cci()

    df["williams_r"] = ta.momentum.WilliamsRIndicator(
        high=high, low=low, close=close, lbp=ind_cfg["williams_r_period"]
    ).williams_r()


def _compute_volume_indicators(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    ind_cfg: dict,
) -> None:
    """Add OBV, CMF, and A/D Line columns to df in place."""
    df["obv"] = ta.volume.OnBalanceVolumeIndicator(
        close=close, volume=volume
    ).on_balance_volume()

    df["cmf_20"] = ta.volume.ChaikinMoneyFlowIndicator(
        high=high, low=low, close=close, volume=volume, window=ind_cfg["cmf_period"]
    ).chaikin_money_flow()

    df["ad_line"] = ta.volume.AccDistIndexIndicator(
        high=high, low=low, close=close, volume=volume
    ).acc_dist_index()


def _compute_volatility_indicators(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ind_cfg: dict,
) -> None:
    """Add Bollinger Bands, ATR, and Keltner Channel columns to df in place."""
    bb_cfg = ind_cfg["bollinger"]
    bb = ta.volatility.BollingerBands(
        close=close, window=bb_cfg["period"], window_dev=bb_cfg["std_dev"]
    )
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_pctb"] = bb.bollinger_pband()

    try:
        df["atr_14"] = ta.volatility.AverageTrueRange(
            high=high, low=low, close=close, window=ind_cfg["atr_period"]
        ).average_true_range()
    except (ValueError, IndexError):
        df["atr_14"] = float("nan")

    try:
        kc = ta.volatility.KeltnerChannel(
            high=high, low=low, close=close, window=ind_cfg["keltner_period"]
        )
        df["keltner_upper"] = kc.keltner_channel_hband()
        df["keltner_lower"] = kc.keltner_channel_lband()
    except (ValueError, IndexError):
        df["keltner_upper"] = float("nan")
        df["keltner_lower"] = float("nan")


def save_indicators_to_db(
    db_conn: sqlite3.Connection, ticker: str, indicators_df: pd.DataFrame
) -> int:
    """
    Save computed indicator rows to the indicators_daily table.

    Skips rows where ALL indicator values are NaN (warm-up period rows).
    Uses INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        indicators_df: DataFrame containing a 'date' column plus all indicator columns.

    Returns:
        Number of rows saved.
    """
    saved_count = 0

    for _, row in indicators_df.iterrows():
        indicator_values = {col: row.get(col) for col in _INDICATOR_COLUMNS if col in indicators_df.columns}

        # Skip rows where every indicator is NaN
        if all(
            v is None or (isinstance(v, float) and pd.isna(v))
            for v in indicator_values.values()
        ):
            continue

        db_conn.execute(
            """INSERT OR REPLACE INTO indicators_daily(
                ticker, date,
                ema_9, ema_21, ema_50,
                macd_line, macd_signal, macd_histogram,
                adx, rsi_14,
                stoch_k, stoch_d, cci_20, williams_r,
                obv, cmf_20, ad_line,
                bb_upper, bb_lower, bb_pctb,
                atr_14, keltner_upper, keltner_lower
            ) VALUES (
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?
            )""",
            (
                ticker,
                row["date"],
                _nan_to_none(indicator_values.get("ema_9")),
                _nan_to_none(indicator_values.get("ema_21")),
                _nan_to_none(indicator_values.get("ema_50")),
                _nan_to_none(indicator_values.get("macd_line")),
                _nan_to_none(indicator_values.get("macd_signal")),
                _nan_to_none(indicator_values.get("macd_histogram")),
                _nan_to_none(indicator_values.get("adx")),
                _nan_to_none(indicator_values.get("rsi_14")),
                _nan_to_none(indicator_values.get("stoch_k")),
                _nan_to_none(indicator_values.get("stoch_d")),
                _nan_to_none(indicator_values.get("cci_20")),
                _nan_to_none(indicator_values.get("williams_r")),
                _nan_to_none(indicator_values.get("obv")),
                _nan_to_none(indicator_values.get("cmf_20")),
                _nan_to_none(indicator_values.get("ad_line")),
                _nan_to_none(indicator_values.get("bb_upper")),
                _nan_to_none(indicator_values.get("bb_lower")),
                _nan_to_none(indicator_values.get("bb_pctb")),
                _nan_to_none(indicator_values.get("atr_14")),
                _nan_to_none(indicator_values.get("keltner_upper")),
                _nan_to_none(indicator_values.get("keltner_lower")),
            ),
        )
        saved_count += 1

    db_conn.commit()
    return saved_count


def _nan_to_none(value: object) -> object:
    """Convert float NaN to None for SQLite storage; pass all other values through."""
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def compute_indicators_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    mode: str = "full",
) -> int:
    """
    Load OHLCV, compute all indicators, and save to indicators_daily.

    Two modes:
        full: Load ALL OHLCV history. Compute and save all rows. Used after backfill.
        incremental: Load last 200 days of OHLCV (warm-up buffer for EMA 50 + ADX).
            Compute indicators. Save only rows not already in indicators_daily.
            Used in the daily pipeline.

    Args:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        config: Calculator config dict.
        mode: Either 'full' or 'incremental'.

    Returns:
        Number of NEW rows saved.
    """
    try:
        ohlcv_df = _load_ohlcv_for_mode(db_conn, ticker, mode)

        if ohlcv_df.empty:
            logger.warning(f"No OHLCV data found for {ticker} ({mode} mode)")
            return 0

        indicators_df = compute_all_indicators(ohlcv_df, config)
        indicators_df["date"] = ohlcv_df["date"].values

        if mode == "incremental":
            indicators_df = _filter_new_dates(db_conn, ticker, indicators_df)

        count = save_indicators_to_db(db_conn, ticker, indicators_df)
        logger.info(f"Computed indicators for {ticker}: {count} rows ({mode} mode)")
        return count

    except Exception as exc:
        logger.error(
            f"Failed to compute indicators for {ticker} ({mode} mode): {exc}",
            exc_info=True,
        )
        _log_alert(db_conn, ticker, "calculator", str(exc))
        return 0


def _load_ohlcv_for_mode(
    db_conn: sqlite3.Connection, ticker: str, mode: str
) -> pd.DataFrame:
    """Load OHLCV rows for the given mode (full vs incremental 200-day lookback)."""
    if mode == "incremental":
        cursor = db_conn.execute(
            """SELECT date, open, high, low, close, volume
               FROM ohlcv_daily
               WHERE ticker = ?
               ORDER BY date DESC
               LIMIT 200""",
            (ticker,),
        )
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame([dict(row) for row in rows])
        return df.sort_values("date").reset_index(drop=True)

    return load_ohlcv_for_ticker(db_conn, ticker)


def _filter_new_dates(
    db_conn: sqlite3.Connection, ticker: str, indicators_df: pd.DataFrame
) -> pd.DataFrame:
    """Return only rows whose date is not already in indicators_daily for this ticker."""
    cursor = db_conn.execute(
        "SELECT date FROM indicators_daily WHERE ticker = ?", (ticker,)
    )
    existing_dates = {row["date"] for row in cursor.fetchall()}
    return indicators_df[~indicators_df["date"].isin(existing_dates)].reset_index(drop=True)


def _log_alert(
    db_conn: sqlite3.Connection, ticker: str, phase: str, message: str
) -> None:
    """Write a failure record to alerts_log."""
    now = datetime.now(tz=timezone.utc).isoformat()
    try:
        db_conn.execute(
            """INSERT INTO alerts_log(ticker, date, phase, severity, message, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ticker, now[:10], phase, "ERROR", message, now),
        )
        db_conn.commit()
    except Exception:
        pass
