"""
Calculator orchestrator — Phase 2b of the pipeline.

Runs all computation modules in dependency order for all active tickers.

Supports two modes:
  - "full":        Compute everything from scratch (after backfill).
  - "incremental": Compute only new data (daily pipeline — triggered by "fetcher_done" event).

Dependency order per ticker:
  1. Indicators (from OHLCV)
  2. Crossovers (from indicators)          — depends on step 1
  3. Gaps (from OHLCV)                     — independent
  4. Swing points (from OHLCV)             — independent
  5. Support/Resistance (from swing pts)   — depends on step 4
  6. Patterns (OHLCV + indicators + S/R)   — depends on steps 1, 4, 5
  7. Divergences (indicators + swing pts)  — depends on steps 1, 4
  8. Profiles (from indicators)            — depends on step 1 (weekly recompute)
  9. Weekly candles + weekly indicators    — independent
 10. News aggregation (from news_articles) — independent

Also processes sector ETFs and market benchmarks (SPY, QQQ, XLK, etc.) for
indicators and weekly candles — needed for sector scoring and relative strength.

Writes "calculator_done" pipeline event when complete.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.calculator.crossovers import detect_crossovers_for_ticker
from src.calculator.divergences import detect_divergences_for_ticker
from src.calculator.gaps import detect_gaps_for_ticker
from src.calculator.indicators import compute_indicators_for_ticker
from src.calculator.news_aggregator import aggregate_news_for_ticker
from src.calculator.patterns import detect_all_patterns_for_ticker
from src.calculator.profiles import compute_all_profiles, compute_profile_for_ticker
from src.calculator.support_resistance import detect_support_resistance_for_ticker
from src.calculator.swing_points import detect_swing_points_for_ticker
from src.calculator.weekly import compute_weekly_for_ticker
from src.common.config import (
    get_active_tickers,
    get_market_benchmarks,
    get_sector_etfs,
    load_config,
    load_env,
)
from src.common.db import create_all_tables, get_connection
from src.common.events import (
    get_pipeline_event_status,
    log_alert,
    log_pipeline_run,
    write_pipeline_event,
)
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

logger = logging.getLogger(__name__)

_PHASE = "calculator"
_EVENT_NAME = "calculator_done"
_PROFILE_RECOMPUTE_DAYS = 7


def should_recompute_profiles(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
) -> bool:
    """
    Determine whether indicator profiles should be recomputed for a ticker.

    Returns True if no profiles exist for this ticker, or if the most recent
    computed_at timestamp is older than _PROFILE_RECOMPUTE_DAYS (7 days).

    Args:
        db_conn: Open SQLite connection with the indicator_profiles table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict (unused currently, reserved for future thresholds).

    Returns:
        True if profiles should be recomputed, False otherwise.
    """
    row = db_conn.execute(
        "SELECT MAX(computed_at) AS latest FROM indicator_profiles WHERE ticker = ?",
        (ticker,),
    ).fetchone()

    if row is None or row["latest"] is None:
        return True

    threshold = datetime.now(tz=timezone.utc) - timedelta(days=_PROFILE_RECOMPUTE_DAYS)
    try:
        latest_dt = datetime.fromisoformat(row["latest"])
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=timezone.utc)
        return latest_dt <= threshold
    except (ValueError, TypeError):
        return True


def run_calculator_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    config: dict,
    mode: str = "full",
) -> dict:
    """
    Run all computation modules for a single ticker in dependency order.

    Handles module-level failures with fine-grained error recovery:
      - If indicators fails (critical): skip crossovers, patterns, divergences, profiles.
      - If swing_points fails: skip support_resistance, divergences; patterns still runs.
      - All other modules fail independently: log error and continue with remaining modules.

    Profile recomputation is skipped if mode is "incremental" and profiles were
    computed within the last 7 days (see should_recompute_profiles).

    Args:
        db_conn: Open SQLite connection with all calculator tables.
        ticker: Ticker symbol, e.g. 'AAPL'.
        config: Calculator config dict.
        mode: 'full' (recompute everything) or 'incremental' (new data only).

    Returns:
        A dict with keys: ticker, status ('success'/'partial'/'failed'),
        indicators_rows, crossovers_found, gaps_found, swing_points_found,
        sr_levels_found, patterns (dict with candlestick/structural counts),
        divergences_found, profiles_computed, weekly_candles, news_summaries,
        errors (list of error message strings).
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    errors: list[str] = []
    result: dict = {
        "ticker": ticker,
        "status": "success",
        "indicators_rows": 0,
        "crossovers_found": 0,
        "gaps_found": 0,
        "swing_points_found": 0,
        "sr_levels_found": 0,
        "patterns": {"candlestick": 0, "structural": 0},
        "divergences_found": 0,
        "profiles_computed": 0,
        "weekly_candles": 0,
        "news_summaries": 0,
        "errors": errors,
    }

    # ── Step 1: Indicators (CRITICAL) ───────────────────────────────────────
    indicators_ok = False
    try:
        result["indicators_rows"] = compute_indicators_for_ticker(
            db_conn, ticker, config, mode=mode
        )
        indicators_ok = True
    except Exception as exc:
        msg = f"indicators failed: {exc}"
        logger.error(
            f"ticker={ticker} phase=calculator module=indicators error={exc}",
            exc_info=True,
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", msg)
        errors.append(msg)
        result["status"] = "failed"
        # Indicators is critical — all downstream indicator-dependent modules skipped.
        # Independent modules (gaps, weekly, news) still run below.

    # ── Step 2: Crossovers (depends on indicators) ──────────────────────────
    if indicators_ok:
        try:
            result["crossovers_found"] = detect_crossovers_for_ticker(
                db_conn, ticker, config
            )
        except Exception as exc:
            msg = f"crossovers failed: {exc}"
            logger.error(
                f"ticker={ticker} phase=calculator module=crossovers error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", msg)
            errors.append(msg)

    # ── Step 3: Gaps (independent) ──────────────────────────────────────────
    try:
        result["gaps_found"] = detect_gaps_for_ticker(db_conn, ticker, config)
    except Exception as exc:
        msg = f"gaps failed: {exc}"
        logger.error(
            f"ticker={ticker} phase=calculator module=gaps error={exc}", exc_info=True
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", msg)
        errors.append(msg)

    # ── Step 4: Swing points (blocks S/R and divergences) ───────────────────
    swing_ok = False
    try:
        result["swing_points_found"] = detect_swing_points_for_ticker(
            db_conn, ticker, config
        )
        swing_ok = True
    except Exception as exc:
        msg = f"swing_points failed: {exc}"
        logger.error(
            f"ticker={ticker} phase=calculator module=swing_points error={exc}",
            exc_info=True,
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", msg)
        errors.append(msg)

    # ── Step 5: Support/Resistance (depends on swing points) ────────────────
    sr_ok = False
    if swing_ok:
        try:
            result["sr_levels_found"] = detect_support_resistance_for_ticker(
                db_conn, ticker, config
            )
            sr_ok = True
        except Exception as exc:
            msg = f"support_resistance failed: {exc}"
            logger.error(
                f"ticker={ticker} phase=calculator module=support_resistance error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", msg)
            errors.append(msg)

    # ── Step 6: Patterns (depends on indicators + swing points) ─────────────
    # Candlestick patterns only need OHLCV+indicators; structural need swing+S/R.
    # detect_all_patterns_for_ticker reads whatever is available from DB — run it
    # as long as indicators succeeded (it handles missing swing/S/R gracefully).
    if indicators_ok and swing_ok:
        try:
            patterns_result = detect_all_patterns_for_ticker(db_conn, ticker, config)
            result["patterns"]["candlestick"] = patterns_result.get("candlestick_count", 0)
            result["patterns"]["structural"] = patterns_result.get("structural_count", 0)
        except Exception as exc:
            msg = f"patterns failed: {exc}"
            logger.error(
                f"ticker={ticker} phase=calculator module=patterns error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", msg)
            errors.append(msg)

    # ── Step 7: Divergences (depends on indicators + swing points) ───────────
    if indicators_ok and swing_ok:
        try:
            result["divergences_found"] = detect_divergences_for_ticker(
                db_conn, ticker, config
            )
        except Exception as exc:
            msg = f"divergences failed: {exc}"
            logger.error(
                f"ticker={ticker} phase=calculator module=divergences error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", msg)
            errors.append(msg)

    # ── Step 8: Profiles (depends on indicators; weekly recompute) ───────────
    if indicators_ok and (mode == "full" or should_recompute_profiles(db_conn, ticker, config)):
        try:
            result["profiles_computed"] = compute_profile_for_ticker(
                db_conn, ticker, config
            )
        except Exception as exc:
            msg = f"profiles failed: {exc}"
            logger.error(
                f"ticker={ticker} phase=calculator module=profiles error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, ticker, today, _PHASE, "error", msg)
            errors.append(msg)

    # ── Step 9: Weekly candles + indicators (independent) ───────────────────
    try:
        result["weekly_candles"] = compute_weekly_for_ticker(
            db_conn, ticker, config, mode=mode
        )
    except Exception as exc:
        msg = f"weekly failed: {exc}"
        logger.error(
            f"ticker={ticker} phase=calculator module=weekly error={exc}", exc_info=True
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", msg)
        errors.append(msg)

    # ── Step 10: News aggregation (independent) ──────────────────────────────
    try:
        result["news_summaries"] = aggregate_news_for_ticker(db_conn, ticker)
    except Exception as exc:
        msg = f"news_aggregation failed: {exc}"
        logger.error(
            f"ticker={ticker} phase=calculator module=news error={exc}", exc_info=True
        )
        log_alert(db_conn, ticker, today, _PHASE, "error", msg)
        errors.append(msg)

    # ── Determine final status ────────────────────────────────────────────────
    if result["status"] != "failed" and errors:
        result["status"] = "partial"

    return result


def run_calculator_for_etfs_and_benchmarks(
    db_conn: sqlite3.Connection,
    config: dict,
    mode: str = "full",
) -> dict:
    """
    Run indicators and weekly computation for all sector ETFs and market benchmarks.

    Only indicators and weekly candles/indicators are computed — patterns, divergences,
    swing points, S/R, profiles, and news are skipped for ETF/benchmark tickers.
    These are needed for sector scoring and relative-strength computation by the scorer.

    Args:
        db_conn: Open SQLite connection with all calculator tables.
        config: Calculator config dict.
        mode: 'full' or 'incremental'.

    Returns:
        A summary dict with keys: tickers_processed, tickers_failed,
        indicators_rows, weekly_candles.
    """
    sector_etfs = get_sector_etfs()
    benchmarks = get_market_benchmarks()

    benchmark_symbols = list(benchmarks.values()) if isinstance(benchmarks, dict) else list(benchmarks)
    etf_set: set[str] = set(sector_etfs) | set(benchmark_symbols)

    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    tickers_processed = 0
    tickers_failed = 0
    total_indicators = 0
    total_weekly = 0

    for etf_ticker in sorted(etf_set):
        indicators_ok = False
        try:
            rows = compute_indicators_for_ticker(
                db_conn, etf_ticker, config, mode=mode
            )
            total_indicators += rows
            indicators_ok = True
            logger.info(
                f"ticker={etf_ticker} phase=calculator module=indicators "
                f"etf=True rows={rows}"
            )
        except Exception as exc:
            tickers_failed += 1
            logger.error(
                f"ticker={etf_ticker} phase=calculator module=indicators "
                f"etf=True error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, etf_ticker, today, _PHASE, "error", str(exc))
            continue

        try:
            weekly_rows = compute_weekly_for_ticker(
                db_conn, etf_ticker, config, mode=mode
            )
            total_weekly += weekly_rows
            logger.info(
                f"ticker={etf_ticker} phase=calculator module=weekly "
                f"etf=True rows={weekly_rows}"
            )
        except Exception as exc:
            logger.error(
                f"ticker={etf_ticker} phase=calculator module=weekly "
                f"etf=True error={exc}",
                exc_info=True,
            )
            log_alert(db_conn, etf_ticker, today, _PHASE, "error", str(exc))

        if indicators_ok:
            tickers_processed += 1

    return {
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
        "indicators_rows": total_indicators,
        "weekly_candles": total_weekly,
    }


def run_calculator(
    db_path: str = None,
    mode: str = "full",
    ticker_filter: str = None,
    force: bool = False,
) -> dict:
    """
    Run the full Calculator pipeline (Phase 2b).

    Loads configs, opens DB, runs all computation modules for every active ticker
    in dependency order, then writes a "calculator_done" pipeline event.

    Pre-flight checks:
      - Incremental mode: verifies "fetcher_done" event exists for today.
        If not, logs a warning and returns early without processing.
      - Checks "calculator_done" status for today. If "completed", skips (already done).
        If "failed" or missing, proceeds (initial run or retry).
      - When force=True, the "already completed" check is bypassed and the run
        always proceeds regardless of existing pipeline event status.

    Processing order:
      1. ETFs and market benchmarks (indicators + weekly only).
      2. All active stock tickers (all modules in dependency order).
      3. Sector profiles (compute_all_profiles) — full mode only, or if stale.

    Args:
        db_path: Optional path to the SQLite database file. Defaults to the path
            from config/database.json.
        mode: 'full' (recompute everything) or 'incremental' (new data only).
        ticker_filter: Optional ticker symbol to restrict processing to a single ticker.
        force: When True, bypass the "already completed today" check and re-run
            regardless of the existing pipeline event status.

    Returns:
        A summary dict with keys: tickers_processed, tickers_failed, duration_seconds,
        indicators_rows, patterns_found, divergences_found, weekly_candles,
        profiles_computed, news_summaries.
    """
    load_env()
    calc_config = load_config("calculator")
    db_config = load_config("database")

    resolved_db_path = db_path or db_config["path"]
    Path(resolved_db_path).parent.mkdir(parents=True, exist_ok=True)

    db_conn = get_connection(resolved_db_path)
    create_all_tables(db_conn)

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    started_at = datetime.now(tz=timezone.utc).isoformat()

    empty_summary: dict = {
        "tickers_processed": 0,
        "tickers_failed": 0,
        "duration_seconds": 0.0,
        "indicators_rows": 0,
        "patterns_found": 0,
        "divergences_found": 0,
        "weekly_candles": 0,
        "profiles_computed": 0,
        "news_summaries": 0,
    }

    # ── Pre-flight: incremental mode requires fetcher_done event ─────────────
    if mode == "incremental":
        fetcher_status = get_pipeline_event_status(db_conn, "fetcher_done", today)
        if fetcher_status != "completed":
            logger.warning(
                f"phase={_PHASE} mode=incremental date={today} "
                f"fetcher_done_status={fetcher_status!r} — skipping calculator run"
            )
            return empty_summary

    # ── Pre-flight: skip if already completed today (unless forced) ──────────
    calc_status = get_pipeline_event_status(db_conn, _EVENT_NAME, today)
    if calc_status == "completed" and not force:
        logger.info(
            f"phase={_PHASE} date={today} — already completed today, skipping "
            f"(use force=True to override)"
        )
        return empty_summary
    if force and calc_status == "completed":
        logger.info(
            f"phase={_PHASE} date={today} — force=True, re-running despite completed status"
        )

    # ── Mark as processing ───────────────────────────────────────────────────
    write_pipeline_event(db_conn, _EVENT_NAME, today, "processing")

    # ── Load tickers ─────────────────────────────────────────────────────────
    all_tickers = get_active_tickers()
    if ticker_filter:
        stock_tickers = [t for t in all_tickers if t["symbol"] == ticker_filter]
    else:
        stock_tickers = all_tickers

    ticker_symbols = [t["symbol"] for t in stock_tickers]
    tracker = ProgressTracker(phase="Calculator", tickers=ticker_symbols)
    progress_msg_id: int | None = None

    if bot_token and chat_id:
        progress_msg_id = send_telegram_message(
            bot_token, chat_id, tracker.format_progress_message()
        )

    # ── Step 1: ETFs and benchmarks ──────────────────────────────────────────
    etf_summary = run_calculator_for_etfs_and_benchmarks(db_conn, calc_config, mode)
    logger.info(
        f"phase={_PHASE} etfs_processed={etf_summary['tickers_processed']} "
        f"etfs_failed={etf_summary['tickers_failed']}"
    )

    # ── Step 2: Stock tickers ─────────────────────────────────────────────────
    total_indicators = etf_summary["indicators_rows"]
    total_patterns = 0
    total_divergences = 0
    total_weekly = etf_summary["weekly_candles"]
    total_profiles = 0
    total_news = 0
    tickers_processed = 0
    tickers_failed = 0

    for ticker_config in stock_tickers:
        ticker = ticker_config["symbol"]
        tracker.mark_processing(ticker)
        if bot_token and chat_id and progress_msg_id:
            edit_telegram_message(
                bot_token, chat_id, progress_msg_id, tracker.format_progress_message()
            )

        ticker_result = run_calculator_for_ticker(
            db_conn, ticker, calc_config, mode=mode
        )

        if ticker_result["status"] == "failed":
            tickers_failed += 1
            tracker.mark_failed(ticker, reason=ticker_result["errors"][0] if ticker_result["errors"] else "")
        else:
            tickers_processed += 1
            tracker.mark_completed(
                ticker,
                details=(
                    f"ind={ticker_result['indicators_rows']} "
                    f"pat={ticker_result['patterns']['candlestick']} "
                    f"div={ticker_result['divergences_found']}"
                ),
            )

        total_indicators += ticker_result["indicators_rows"]
        total_patterns += (
            ticker_result["patterns"]["candlestick"]
            + ticker_result["patterns"]["structural"]
        )
        total_divergences += ticker_result["divergences_found"]
        total_weekly += ticker_result["weekly_candles"]
        total_profiles += ticker_result["profiles_computed"]
        total_news += ticker_result["news_summaries"]

        if bot_token and chat_id and progress_msg_id:
            edit_telegram_message(
                bot_token, chat_id, progress_msg_id, tracker.format_progress_message()
            )

    # ── Step 3: Sector profiles ───────────────────────────────────────────────
    if not ticker_filter and (
        mode == "full"
        or any(
            should_recompute_profiles(db_conn, t["symbol"], calc_config)
            for t in stock_tickers[:3]  # spot-check a few tickers
        )
    ):
        try:
            compute_all_profiles(db_conn, ticker_symbols, calc_config)
            logger.info(f"phase={_PHASE} sector_profiles computed for {len(ticker_symbols)} tickers")
        except Exception as exc:
            logger.error(
                f"phase={_PHASE} compute_all_profiles failed: {exc}", exc_info=True
            )
            log_alert(db_conn, None, today, _PHASE, "error", f"compute_all_profiles failed: {exc}")

    # ── Post-flight ───────────────────────────────────────────────────────────
    completed_at = datetime.now(tz=timezone.utc).isoformat()
    duration_seconds = (
        datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
    ).total_seconds()

    write_pipeline_event(db_conn, _EVENT_NAME, today, "completed")

    run_status = (
        "success" if tickers_failed == 0
        else "partial" if tickers_processed > 0
        else "failed"
    )

    log_pipeline_run(
        db_conn=db_conn,
        date=today,
        phase=_PHASE,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        tickers_processed=tickers_processed,
        tickers_skipped=0,
        tickers_failed=tickers_failed,
        api_calls_made=0,
        status=run_status,
        error_summary=None,
    )

    summary = {
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
        "duration_seconds": duration_seconds,
        "indicators_rows": total_indicators,
        "patterns_found": total_patterns,
        "divergences_found": total_divergences,
        "weekly_candles": total_weekly,
        "profiles_computed": total_profiles,
        "news_summaries": total_news,
    }

    summary_text = (
        f"📊 Calculator Complete — {today}\n"
        f"Tickers: {tickers_processed}/{len(stock_tickers)} ({tickers_failed} failed)\n"
        f"Indicators: {total_indicators} rows | Divergences: {total_divergences}\n"
        f"Patterns: {total_patterns} | Profiles: {total_profiles}\n"
        f"Weekly Candles: {total_weekly} | News Summaries: {total_news}\n"
        f"Duration: {_format_duration(duration_seconds)}"
    )

    if bot_token and chat_id:
        send_telegram_message(bot_token, chat_id, summary_text)

    logger.info(
        f"phase={_PHASE} completed date={today} mode={mode} "
        f"tickers_processed={tickers_processed} tickers_failed={tickers_failed} "
        f"duration={duration_seconds:.1f}s"
    )

    return summary


def _format_duration(duration_seconds: float) -> str:
    """
    Format a duration in seconds as a human-readable string.

    Args:
        duration_seconds: Elapsed time in seconds.

    Returns:
        A string like '3m 12s' or '1h 5m 30s'.
    """
    total_seconds = int(duration_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"
