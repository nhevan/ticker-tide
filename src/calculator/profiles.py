"""
Per-stock indicator profile computation.

Computes percentile distributions (p5, p20, p50, p80, p95) and z-score
parameters (mean, std) for each indicator for each ticker using a rolling
window (default 504 trading days ≈ 2 years).

Profiles are blended with sector-level profiles so that per-stock context
is calibrated against the stock's own history AND its sector peers:

    Effective = (α × stock_profile) + ((1 - α) × sector_profile)
    α = min(blend_alpha_max, days_of_data / blend_alpha_denominator)
      = min(0.85, days / 756)  (defaults)

This allows per-stock calibrated thresholds instead of fixed ones:
  - NVDA's RSI 78 might be its 80th percentile (overbought for NVDA)
  - INTC's RSI 61 might be its 80th percentile (overbought for INTC)
  - A fixed RSI threshold of 70 would miss INTC and be too early for NVDA

Profiles are recomputed weekly (not daily) since the distributions don't
change meaningfully day-to-day.

Timeframe parametrization
-------------------------
``compute_profile_for_ticker``, ``compute_sector_profile`` and
``compute_all_profiles`` accept keyword-only arguments selecting the source
indicators table + date-column and the destination profiles table. Defaults
preserve the original daily behaviour. Identifiers are validated against an
explicit whitelist before being interpolated into SQL — SQLite parameter
binding does not cover identifiers. Note: the indicator_profiles* tables
have no per-row date column (only ``window_start`` / ``window_end`` text
fields), so no destination date-column parameter is exposed.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd

from src.common.events import log_alert

logger = logging.getLogger(__name__)


# ── Whitelists ──────────────────────────────────────────────────────────────────
_ALLOWED_SOURCE_INDICATORS_TABLES: frozenset[str] = frozenset(
    {"indicators_daily", "indicators_weekly", "indicators_monthly"}
)
_ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS: frozenset[str] = frozenset(
    {"date", "week_start", "month_start"}
)
_ALLOWED_DEST_TABLES: frozenset[str] = frozenset(
    {"indicator_profiles", "indicator_profiles_weekly", "indicator_profiles_monthly"}
)


def _validate_identifier(name: str, allowed: frozenset[str], field: str) -> str:
    """
    Validate that an identifier (table or column name) is in the allow-list.

    SQLite parameter binding does not cover identifiers, so any table or
    column name interpolated into SQL must be checked against an explicit
    set of permitted values.

    Args:
        name: Candidate identifier.
        allowed: Frozenset of permitted identifier strings.
        field: Human-readable label used in the error message.

    Returns:
        The validated identifier verbatim.

    Raises:
        ValueError: If ``name`` is not in ``allowed``.
    """
    if name not in allowed:
        raise ValueError(
            f"Invalid {field}={name!r}; expected one of {sorted(allowed)}"
        )
    return name

# Only normalized/bounded indicators are profiled. Price-level absolute values
# (bb_upper, bb_lower, keltner_upper, keltner_lower) are excluded because their
# raw magnitudes scale with the stock price and are not meaningful across tickers.
# We still profile EMA, MACD, OBV, and A/D Line because we use z-scores
# (how far from the stock's own mean), not cross-stock absolute comparisons.
PROFILED_INDICATORS = [
    "rsi_14",
    "stoch_k",
    "stoch_d",
    "cci_20",
    "williams_r",
    "cmf_20",
    "bb_pctb",
    "adx",
    "macd_histogram",
    "atr_14",
    "obv",
    "ad_line",
    "macd_line",
    "macd_signal",
    "ema_9",
    "ema_21",
    "ema_50",
]

_MIN_VALID_VALUES = 30


def compute_percentiles(series: pd.Series) -> Optional[dict]:
    """
    Compute percentile distribution and z-score parameters for a Series.

    Drops NaN values before computation. Returns None if fewer than
    _MIN_VALID_VALUES (30) valid values remain.

    Args:
        series: Pandas Series of numeric indicator values.

    Returns:
        Dict with keys: p5, p20, p50, p80, p95, mean, std.
        Returns None if insufficient valid data.
    """
    clean = series.dropna()
    if len(clean) < _MIN_VALID_VALUES:
        return None

    values = clean.to_numpy(dtype=float)
    return {
        "p5": float(np.percentile(values, 5)),
        "p20": float(np.percentile(values, 20)),
        "p50": float(np.percentile(values, 50)),
        "p80": float(np.percentile(values, 80)),
        "p95": float(np.percentile(values, 95)),
        "mean": float(np.mean(values)),
        "std": float(np.std(values, ddof=1)),
    }


def compute_profile_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    *,
    source_indicators_table: str = "indicators_daily",
    source_indicators_date_column: str = "date",
    dest_table: str = "indicator_profiles",
) -> int:
    """
    Compute and persist percentile profiles for all profiled indicators for one ticker.

    Loads indicator data from the configured source table, applies the rolling
    window from config, computes percentiles for each indicator in
    PROFILED_INDICATORS, and upserts results into the configured destination
    profiles table.

    If fewer rows exist than the rolling window, all available data is used and
    a warning is logged.

    Defaults preserve the original daily behaviour: read from
    ``indicators_daily`` keyed by ``date`` and write to ``indicator_profiles``.
    Identifiers are validated against an explicit whitelist.

    Args:
        db_conn: Open SQLite connection with profiles and indicators tables.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict. Reads config['profiles']['rolling_window_days'].
        source_indicators_table: Indicators source table. Must be one of
            ``indicators_daily``, ``indicators_weekly`` or
            ``indicators_monthly``.
        source_indicators_date_column: Date-column name in
            ``source_indicators_table``. Must be one of ``date``,
            ``week_start`` or ``month_start``.
        dest_table: Destination profiles table. Must be one of
            ``indicator_profiles``, ``indicator_profiles_weekly`` or
            ``indicator_profiles_monthly``.

    Returns:
        Number of indicator profile rows saved.

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    safe_source_table = _validate_identifier(
        source_indicators_table,
        _ALLOWED_SOURCE_INDICATORS_TABLES,
        "source_indicators_table",
    )
    safe_source_date_col = _validate_identifier(
        source_indicators_date_column,
        _ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS,
        "source_indicators_date_column",
    )
    safe_dest_table = _validate_identifier(
        dest_table, _ALLOWED_DEST_TABLES, "dest_table"
    )

    rolling_window = config.get("profiles", {}).get("rolling_window_days", 504)

    rows = db_conn.execute(
        f"SELECT * FROM {safe_source_table} WHERE ticker = ? "
        f"ORDER BY {safe_source_date_col} ASC",
        (ticker,),
    ).fetchall()

    if not rows:
        logger.warning(
            f"No indicator data found for ticker={ticker} in {safe_source_table}, "
            f"skipping profile computation"
        )
        return 0

    df = pd.DataFrame([dict(row) for row in rows])

    if len(df) < rolling_window:
        logger.warning(
            f"ticker={ticker} source={safe_source_table} has {len(df)} indicator rows, "
            f"less than rolling_window_days={rolling_window}. "
            f"Computing profile from all available data."
        )
        window_df = df
    else:
        window_df = df.tail(rolling_window)

    # The source date column is named ``date``, ``week_start`` or
    # ``month_start`` depending on timeframe; pick whichever exists.
    date_col = safe_source_date_col if safe_source_date_col in window_df.columns else "date"
    window_start = window_df.iloc[0][date_col]
    window_end = window_df.iloc[-1][date_col]
    computed_at = datetime.now(tz=timezone.utc).isoformat()

    saved_count = 0
    for indicator in PROFILED_INDICATORS:
        if indicator not in window_df.columns:
            continue

        series = window_df[indicator].astype(float)
        percentile_dict = compute_percentiles(series)
        if percentile_dict is None:
            logger.debug(f"Skipping profile for ticker={ticker} indicator={indicator}: insufficient data")
            continue

        db_conn.execute(
            f"""
            INSERT OR REPLACE INTO {safe_dest_table}
                (ticker, indicator, p5, p20, p50, p80, p95, mean, std,
                 window_start, window_end, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker, indicator,
                percentile_dict["p5"], percentile_dict["p20"], percentile_dict["p50"],
                percentile_dict["p80"], percentile_dict["p95"],
                percentile_dict["mean"], percentile_dict["std"],
                window_start, window_end, computed_at,
            ),
        )
        saved_count += 1

    db_conn.commit()
    logger.info(
        f"Saved {saved_count} indicator profiles for ticker={ticker} dest={safe_dest_table}"
    )
    return saved_count


def compute_sector_profile(
    db_conn: sqlite3.Connection,
    sector: str,
    config: dict,
    *,
    source_indicators_table: str = "indicators_daily",
    source_indicators_date_column: str = "date",
) -> dict:
    """
    Compute percentile profiles for all profiled indicators across a full sector.

    Loads all active tickers in the sector, fetches their indicator data from
    the configured source table, and computes percentiles from the combined
    dataset (pool all rows from all tickers).

    Defaults preserve the original daily behaviour. Identifiers are validated
    against an explicit whitelist.

    Args:
        db_conn: Open SQLite connection.
        sector: Sector name to query from the tickers table, e.g. 'Technology'.
        config: Calculator config dict.
        source_indicators_table: Indicators source table. Must be one of
            ``indicators_daily``, ``indicators_weekly`` or
            ``indicators_monthly``.
        source_indicators_date_column: Date-column name in
            ``source_indicators_table``.

    Returns:
        Dict mapping indicator_name → percentile dict (or None if insufficient data).
        Returns empty dict if no tickers are in the sector.

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    safe_source_table = _validate_identifier(
        source_indicators_table,
        _ALLOWED_SOURCE_INDICATORS_TABLES,
        "source_indicators_table",
    )
    safe_source_date_col = _validate_identifier(
        source_indicators_date_column,
        _ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS,
        "source_indicators_date_column",
    )

    rolling_window = config.get("profiles", {}).get("rolling_window_days", 504)

    ticker_rows = db_conn.execute(
        "SELECT symbol FROM tickers WHERE sector = ? AND active = 1",
        (sector,),
    ).fetchall()

    if not ticker_rows:
        logger.warning(f"No active tickers found for sector={sector}")
        return {}

    symbols = [row["symbol"] for row in ticker_rows]
    all_dfs = []
    for symbol in symbols:
        rows = db_conn.execute(
            f"SELECT * FROM {safe_source_table} WHERE ticker = ? "
            f"ORDER BY {safe_source_date_col} ASC",
            (symbol,),
        ).fetchall()
        if not rows:
            continue
        ticker_df = pd.DataFrame([dict(row) for row in rows])
        if len(ticker_df) > rolling_window:
            ticker_df = ticker_df.tail(rolling_window)
        all_dfs.append(ticker_df)

    if not all_dfs:
        logger.warning(
            f"No indicator data found for any ticker in sector={sector} "
            f"source={safe_source_table}"
        )
        return {}

    combined = pd.concat(all_dfs, ignore_index=True)
    result: dict = {}
    for indicator in PROFILED_INDICATORS:
        if indicator not in combined.columns:
            result[indicator] = None
            continue
        series = combined[indicator].astype(float)
        result[indicator] = compute_percentiles(series)

    return result


def blend_profiles(
    stock_profile: Optional[dict],
    sector_profile: Optional[dict],
    alpha: float,
) -> dict:
    """
    Blend a stock-level indicator profile with a sector-level profile.

    Effective = (alpha × stock_value) + ((1 - alpha) × sector_value)

    If stock_profile is None, the sector profile is used entirely (alpha forced to 0).
    If sector_profile is None, the stock profile is used entirely (alpha forced to 1).
    If both are None, returns a dict of all-None values.

    Args:
        stock_profile: Percentile dict for the individual stock, or None.
        sector_profile: Percentile dict for the sector, or None.
        alpha: Blend weight for the stock profile (0–1). Typically min(0.85, days/756).

    Returns:
        Blended percentile dict with same keys as the input profiles.
    """
    keys = ["p5", "p20", "p50", "p80", "p95", "mean", "std"]

    if stock_profile is None and sector_profile is None:
        return {k: None for k in keys}

    if stock_profile is None:
        return dict(sector_profile)  # type: ignore[arg-type]

    if sector_profile is None:
        return dict(stock_profile)

    blended = {}
    for key in keys:
        stock_val = stock_profile.get(key)
        sector_val = sector_profile.get(key)
        if stock_val is None or sector_val is None:
            blended[key] = stock_val if stock_val is not None else sector_val
        else:
            blended[key] = alpha * stock_val + (1.0 - alpha) * sector_val
    return blended


def calculate_alpha(days_of_data: int, config: dict) -> float:
    """
    Calculate the blend weight (alpha) for stock vs sector profile blending.

    alpha = min(blend_alpha_max, days_of_data / blend_alpha_denominator)

    With defaults: alpha = min(0.85, days / 756)
    - Fewer data days → more weight on sector profile
    - After 756 days (≈3 years) of data, alpha caps at 0.85

    Args:
        days_of_data: Number of rows of indicator data available for the ticker.
        config: Calculator config dict.

    Returns:
        Float alpha in [0, blend_alpha_max].
    """
    profiles_cfg = config.get("profiles", {})
    alpha_max = profiles_cfg.get("blend_alpha_max", 0.85)
    denominator = profiles_cfg.get("blend_alpha_denominator", 756)
    if denominator <= 0:
        return alpha_max
    raw = days_of_data / denominator
    return min(alpha_max, raw)


def compute_all_profiles(
    db_conn: sqlite3.Connection,
    tickers: list[dict],
    config: dict,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    *,
    source_indicators_table: str = "indicators_daily",
    source_indicators_date_column: str = "date",
    dest_table: str = "indicator_profiles",
) -> dict:
    """
    Compute and persist blended indicator profiles for all tickers.

    Workflow:
    1. Determine all unique sectors and compute sector-level profiles.
    2. For each ticker, compute the stock-level profile, calculate alpha based
       on available data length, blend with sector profile, and save.

    Per-ticker failures are caught and logged without aborting the run.

    Defaults preserve the original daily behaviour. Override the keyword-only
    parameters to run against the weekly or monthly mirrors.

    Args:
        db_conn: Open SQLite connection.
        tickers: List of ticker dicts, each with 'symbol' and 'sector' keys.
        config: Calculator config dict.
        bot_token: Optional Telegram bot token for progress updates.
        chat_id: Optional Telegram chat ID for progress updates.
        source_indicators_table: Indicators source table. Must be one of
            ``indicators_daily``, ``indicators_weekly`` or
            ``indicators_monthly``.
        source_indicators_date_column: Date-column name in
            ``source_indicators_table``.
        dest_table: Destination profiles table. Must be one of
            ``indicator_profiles``, ``indicator_profiles_weekly`` or
            ``indicator_profiles_monthly``.

    Returns:
        Dict with keys: processed (int), failed (int), total_profiles (int).

    Raises:
        ValueError: If any identifier argument is not in the allow-list.
    """
    # Validate eagerly so misuse fails fast before any work begins.
    _validate_identifier(
        source_indicators_table,
        _ALLOWED_SOURCE_INDICATORS_TABLES,
        "source_indicators_table",
    )
    _validate_identifier(
        source_indicators_date_column,
        _ALLOWED_SOURCE_INDICATORS_DATE_COLUMNS,
        "source_indicators_date_column",
    )
    _validate_identifier(dest_table, _ALLOWED_DEST_TABLES, "dest_table")

    # Compute sector profiles for all unique sectors first
    sectors = {t.get("sector") for t in tickers if t.get("sector")}
    sector_profiles: dict[str, dict] = {}
    for sector in sectors:
        logger.info(f"Computing sector profile for sector={sector}")
        sector_profiles[sector] = compute_sector_profile(
            db_conn,
            sector,
            config,
            source_indicators_table=source_indicators_table,
            source_indicators_date_column=source_indicators_date_column,
        )

    processed = 0
    failed = 0
    total_profiles = 0
    today = datetime.now(tz=timezone.utc).date().isoformat()

    for ticker_config in tickers:
        ticker = ticker_config["symbol"]
        sector = ticker_config.get("sector")
        try:
            count = compute_profile_for_ticker(
                db_conn,
                ticker,
                config,
                source_indicators_table=source_indicators_table,
                source_indicators_date_column=source_indicators_date_column,
                dest_table=dest_table,
            )
            total_profiles += count
            processed += 1
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, today, "calculator",
                "error", f"Profile computation failed for ticker={ticker}: {exc}",
            )
            logger.error(f"Profile computation failed for ticker={ticker}: {exc!r}")

    logger.info(
        f"compute_all_profiles complete: processed={processed} "
        f"failed={failed} total_profiles={total_profiles}"
    )
    return {"processed": processed, "failed": failed, "total_profiles": total_profiles}
