#!/usr/bin/env python3
"""
Shadow v2 calibrator comparison.

Copies the production DB to a temp location, then re-runs the last 12 months
of daily historical scoring with ``weekly_score_method`` and
``monthly_score_method`` both set to ``v2_8cat`` — purely in-memory config
mutation.  config/scorer.json and the production DB are never touched.

After this script completes, run the acceptance gate:

    python scripts/check_calibrator_acceptance.py check \\
        --baseline baselines/pre_v2_flip.json \\
        --db-path /tmp/ticker_tide_shadow_v2.db

Workflow in context:
  1. Snapshot the current (v1) calibrated_score distribution:
       python scripts/check_calibrator_acceptance.py snapshot \\
           --output baselines/pre_v2_flip.json --scoring-date YYYY-MM-DD
  2. Run this script  (shadow v2 calibrator — 20-40 min).
  3. Run the acceptance gate check (see above).

Design notes:
  - Calls score_ticker() directly — avoids run_historical_scoring()'s
    Telegram send, load_env() re-read, and pipeline_events writes.
  - Uses sqlite3.Connection.backup() (WAL-safe) to copy the production DB.
  - Only re-scores mode="daily" (last daily_lookback_months*31 calendar days)
    because the calibrator's rolling window_size must fit within that window.
    An assertion guards this at startup.
  - Early dates in the shadow DB have mixed v1/v2 calibration semantics:
    the calibrator trains on its 365-day lookback, so dates at the start of
    the re-run window still see mostly v1 training rows.  Only the final date
    (the date the acceptance gate reads) has a fully v2-trained calibrator.
    This is intentional and matches the semantics of the real flip.
  - scores_weekly and scores_monthly in the shadow DB are overwritten as a
    side-effect of score_ticker's closed-period persistence helpers.  The
    acceptance gate reads only scores_daily.calibrated_score, so this does
    not affect the gate result.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from datetime import date, timedelta
from typing import Optional

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv  # noqa: E402

from src.common.config import (  # noqa: E402
    get_active_tickers,
    get_training_excluded_tickers,
    load_config,
)
from src.common.db import get_connection  # noqa: E402
from src.scorer.main import score_ticker  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

SHADOW_DB_PATH: str = "/tmp/ticker_tide_shadow_v2.db"
_PROD_DB_RELATIVE: str = "data/signals.db"


def _resolve_prod_db() -> str:
    """Return the absolute path to the production database."""
    return os.path.join(_REPO_ROOT, _PROD_DB_RELATIVE)


def _backup_db(src: str, dst: str) -> None:
    """
    Copy src → dst using the SQLite backup API (WAL-safe).

    Always overwrites dst when it already exists.

    Parameters:
        src: Path to the source SQLite database.
        dst: Destination path for the shadow copy.
    """
    if os.path.exists(dst):
        logger.info("Shadow DB already exists — overwriting: %s", dst)
        os.remove(dst)

    logger.info("Copying %s → %s (SQLite backup API)", src, dst)
    src_conn = sqlite3.connect(src)
    dst_conn = sqlite3.connect(dst)
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

    size_mb = os.path.getsize(dst) / 1_000_000
    logger.info("DB copy complete — %.1f MB at %s", size_mb, dst)


def _assert_calibration_window_fits(scorer_config: dict) -> None:
    """
    Assert that the calibrator's rolling window_size fits within the daily re-run window.

    mode="daily" re-scores the last daily_lookback_months * 31 calendar days.
    If window_size exceeds that span, early calibration training rows in the
    shadow DB will still be v1, making the gate comparison misleading.

    Parameters:
        scorer_config: Loaded scorer config dict.

    Raises:
        SystemExit: If window_size > daily_lookback_months * 31.
    """
    window_days: int = scorer_config.get("calibration", {}).get("window_size", 365)
    daily_months: int = scorer_config.get("historical_scoring", {}).get(
        "daily_lookback_months", 12
    )
    daily_days: int = daily_months * 31
    logger.info(
        "Calibration window check: window_size=%dd  daily_lookback=%dd*31=%dd",
        window_days,
        daily_months,
        daily_days,
    )
    if window_days > daily_days:
        logger.error(
            "calibration.window_size (%dd) exceeds daily_lookback_months*31 (%dd). "
            "The shadow run will have insufficient v2 training rows for the calibrator. "
            "Either increase historical_scoring.daily_lookback_months or run with mode='all'.",
            window_days,
            daily_days,
        )
        sys.exit(1)
    logger.info("Calibration window check: OK")


def _fetch_trading_dates(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Return sorted unique trading dates from ohlcv_daily in [start_date, end_date].

    Parameters:
        conn:       Open SQLite connection.
        start_date: Start date (YYYY-MM-DD), inclusive.
        end_date:   End date (YYYY-MM-DD), inclusive.

    Returns:
        Sorted list of date strings present in ohlcv_daily.
    """
    rows = conn.execute(
        "SELECT DISTINCT date FROM ohlcv_daily "
        "WHERE date >= ? AND date <= ? ORDER BY date ASC",
        (start_date, end_date),
    ).fetchall()
    return [r[0] for r in rows]


def _run_shadow_scoring(
    shadow_db_path: str,
    scorer_config: dict,
) -> dict:
    """
    Re-score the daily window on the shadow DB using the in-memory v2 config.

    Does NOT call run_historical_scoring() — calls score_ticker() directly to
    avoid Telegram sends, load_env() re-reads, and pipeline_events writes.

    Parameters:
        shadow_db_path: Path to the shadow DB copy.
        scorer_config:  Scorer config with weekly/monthly_score_method = v2_8cat.

    Returns:
        Dict with keys: completed, errors, n_dates, n_tickers.
    """
    all_tickers = get_active_tickers()
    excluded: set[str] = get_training_excluded_tickers()

    conn = get_connection(shadow_db_path)

    today = date.today()
    daily_months: int = scorer_config.get("historical_scoring", {}).get(
        "daily_lookback_months", 12
    )
    daily_start = (today - timedelta(days=daily_months * 31)).isoformat()

    trading_dates = _fetch_trading_dates(conn, daily_start, today.isoformat())
    n_tickers = len(all_tickers)
    total_calls = len(trading_dates) * n_tickers

    logger.info(
        "Shadow scoring: %d dates × %d tickers = %d calls  (mode=daily, v2_8cat)",
        len(trading_dates),
        n_tickers,
        total_calls,
    )
    logger.info("Estimated time: 20–40 minutes.  Progress logged every 20 dates.")

    completed = 0
    errors = 0

    for date_idx, dt in enumerate(trading_dates):
        for tc in all_tickers:
            ticker: str = tc["symbol"]
            try:
                result = score_ticker(
                    db_conn=conn,
                    ticker=ticker,
                    ticker_config=tc,
                    scoring_date=dt,
                    config=scorer_config,
                    excluded_tickers=excluded,
                )
                if result is not None:
                    completed += 1
            except Exception as exc:  # noqa: BLE001
                logger.error("%s on %s: score_ticker failed — %s", ticker, dt, exc)
                errors += 1

        if (date_idx + 1) % 20 == 0 or date_idx == len(trading_dates) - 1:
            pct = (date_idx + 1) / len(trading_dates) * 100
            logger.info(
                "Progress: %d/%d dates (%.0f%%)  completed=%d  errors=%d",
                date_idx + 1,
                len(trading_dates),
                pct,
                completed,
                errors,
            )

    conn.close()
    return {
        "completed": completed,
        "errors": errors,
        "n_dates": len(trading_dates),
        "n_tickers": n_tickers,
    }


def _sanity_check_shadow_db(shadow_db_path: str, scoring_date: str, min_rows: int) -> None:
    """
    Verify the shadow DB has enough calibrated rows on the target scoring_date.

    Logs a warning (does not exit) if the count falls below min_rows, because
    the acceptance gate will raise INSUFFICIENT_DATA in that case anyway.

    Parameters:
        shadow_db_path: Path to the shadow DB.
        scoring_date:   The date the acceptance gate will read (YYYY-MM-DD).
        min_rows:       Minimum expected calibrated rows (from calibrator_acceptance config).
    """
    conn = sqlite3.connect(shadow_db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM scores_daily "
            "WHERE calibrated_score IS NOT NULL AND date = ?",
            (scoring_date,),
        ).fetchone()[0]
    finally:
        conn.close()

    if count < min_rows:
        logger.warning(
            "Sanity check WARN: shadow DB has %d calibrated rows on %s — "
            "acceptance gate needs >= %d (min_sample_size).  The gate will "
            "exit with INSUFFICIENT_DATA.",
            count,
            scoring_date,
            min_rows,
        )
    else:
        logger.info(
            "Sanity check OK: shadow DB has %d calibrated rows on %s (>= %d)",
            count,
            scoring_date,
            min_rows,
        )


def main() -> None:
    """
    Entry point for the shadow v2 calibrator run.

    1. Loads .env and configs (no Telegram — we never read the token).
    2. Mutates the scorer config in-memory to v2_8cat.
    3. Copies production DB to the shadow path.
    4. Re-scores the daily window on the shadow DB.
    5. Emits sanity check and next-step instructions.
    """
    load_dotenv()  # loads .env so DB path config resolves, but we never read TELEGRAM vars

    prod_db = _resolve_prod_db()
    if not os.path.exists(prod_db):
        logger.error("Production DB not found: %s", prod_db)
        sys.exit(1)

    # Load and mutate config in-memory — config/scorer.json is never written
    scorer_config = load_config("scorer")
    scorer_config["weekly_score_method"] = "v2_8cat"
    scorer_config["monthly_score_method"] = "v2_8cat"
    logger.info(
        "In-memory config override: weekly_score_method=v2_8cat, "
        "monthly_score_method=v2_8cat  (config/scorer.json is unchanged)"
    )

    _assert_calibration_window_fits(scorer_config)

    _backup_db(prod_db, SHADOW_DB_PATH)

    stats = _run_shadow_scoring(SHADOW_DB_PATH, scorer_config)

    # Determine the latest scoring date for sanity check
    conn_tmp = get_connection(SHADOW_DB_PATH)
    latest_date_row = conn_tmp.execute(
        "SELECT MAX(date) FROM scores_daily WHERE calibrated_score IS NOT NULL"
    ).fetchone()
    conn_tmp.close()
    latest_date: Optional[str] = latest_date_row[0] if latest_date_row else None

    min_sample: int = scorer_config.get("calibrator_acceptance", {}).get("min_sample_size", 30)
    if latest_date:
        _sanity_check_shadow_db(SHADOW_DB_PATH, latest_date, min_sample)

    logger.info("")
    logger.info("=== Shadow v2 calibrator run complete ===")
    logger.info("  Shadow DB:       %s", SHADOW_DB_PATH)
    logger.info("  Dates rescored:  %d", stats["n_dates"])
    logger.info("  Tickers:         %d", stats["n_tickers"])
    logger.info("  Rows written:    %d", stats["completed"])
    logger.info("  Errors:          %d", stats["errors"])
    logger.info("  Latest date:     %s", latest_date or "unknown")
    logger.info("")
    logger.info(
        "NOTE: Early dates in the shadow DB carry mixed v1/v2 calibration "
        "semantics.  Only the final scoring date has a fully v2-trained "
        "calibrator.  The acceptance gate reads only that final date."
    )
    logger.info("")
    logger.info("Next steps:")
    logger.info(
        "  1. python scripts/check_calibrator_acceptance.py check "
        "--baseline baselines/pre_v2_flip.json --db-path %s",
        SHADOW_DB_PATH,
    )
    logger.info("  2. Report the gate output (PASS/WARNING/FAIL) before touching production.")


if __name__ == "__main__":
    main()
