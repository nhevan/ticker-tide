"""
Notifier orchestrator — Phase 4 of the daily pipeline.

Runs the AI reasoner for qualifying tickers and sends the formatted daily
signal report via Telegram. This is the final phase of the pipeline.

Flow:
  1. Check 'scorer_done' event exists for the scoring date.
  2. Check 'notifier_done' — skip if already completed.
  3. Write 'notifier_done' with status='processing'.
  4. Call reason_all_qualifying_tickers() for AI-generated analysis.
  5. Query signal distribution from scores_daily.
  6. Build pipeline_stats (query pipeline_runs for prior phase durations).
  7. Format the report (full or no-signals variant).
  8. Send via Telegram.
  9. Update 'notifier_done' to status='completed'.
  10. Log pipeline_run for phase='notifier'.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

from src.common.config import get_active_tickers, load_config, load_env
from src.common.db import get_connection
from src.common.events import (
    get_latest_pipeline_run,
    get_pipeline_event_status,
    log_pipeline_run,
    write_pipeline_event,
)
from src.notifier.ai_reasoner import reason_all_qualifying_tickers
from src.notifier.formatter import (
    format_full_report,
    format_heartbeat,
    format_no_signals_report,
)
from src.notifier.telegram import get_telegram_config, send_daily_report, send_heartbeat

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _resolve_scoring_date(db_conn, explicit_date: Optional[str]) -> str:
    """
    Determine the date to use for scoring.

    Uses the most common latest date in indicators_daily across all tickers
    (i.e. the most recent trading day in the DB). Falls back to today's UTC
    date if the table is empty.

    Parameters:
        db_conn: Open SQLite connection.
        explicit_date: Caller-supplied date override, or None.

    Returns:
        Resolved scoring date as a YYYY-MM-DD string.
    """
    if explicit_date:
        return explicit_date
    row = db_conn.execute(
        """
        SELECT date, COUNT(*) AS cnt
        FROM (
            SELECT ticker, MAX(date) AS date
            FROM indicators_daily
            GROUP BY ticker
        )
        GROUP BY date
        ORDER BY cnt DESC, date DESC
        LIMIT 1
        """
    ).fetchone()
    return row["date"] if row else date.today().isoformat()


def _get_signal_distribution(db_conn, scoring_date: str) -> dict:
    """
    Query the signal distribution (bullish/bearish/neutral counts) for a date.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Date to query (YYYY-MM-DD).

    Returns:
        Dict with keys: bullish_count, bearish_count, neutral_count, total.
    """
    rows = db_conn.execute(
        "SELECT signal, COUNT(*) AS cnt FROM scores_daily WHERE date = ? GROUP BY signal",
        (scoring_date,),
    ).fetchall()
    counts = {row["signal"]: row["cnt"] for row in rows}
    bullish = counts.get("BULLISH", 0)
    bearish = counts.get("BEARISH", 0)
    neutral = counts.get("NEUTRAL", 0)
    return {
        "bullish_count": bullish,
        "bearish_count": bearish,
        "neutral_count": neutral,
        "total": bullish + bearish + neutral,
    }


def _build_pipeline_stats(
    db_conn,
    scoring_date: str,
    notifier_duration: float,
    signal_dist: dict,
    config: dict,
) -> dict:
    """
    Build the pipeline_stats dict for the formatter by querying prior phase durations.

    Parameters:
        db_conn: Open SQLite connection.
        scoring_date: Scoring date (YYYY-MM-DD).
        notifier_duration: Duration of the notifier phase in seconds.
        signal_dist: Signal distribution dict from _get_signal_distribution.
        config: Notifier config dict.

    Returns:
        pipeline_stats dict ready for the formatter.
    """
    display_timezone = config.get("telegram", {}).get("display_timezone", "Europe/Amsterdam")

    def _get_duration(phase: str) -> Optional[float]:
        run = get_latest_pipeline_run(db_conn, phase)
        return run["duration_seconds"] if run else None

    tickers_total = len(get_active_tickers())

    return {
        "scoring_date": scoring_date,
        "fetcher_duration": _get_duration("fetcher"),
        "calculator_duration": _get_duration("calculator"),
        "scorer_duration": _get_duration("scorer"),
        "notifier_duration": notifier_duration,
        "tickers_processed": signal_dist["total"],
        "tickers_total": tickers_total,
        "tickers_failed": 0,
        "failed_tickers": [],
        "bullish_count": signal_dist["bullish_count"],
        "bearish_count": signal_dist["bearish_count"],
        "neutral_count": signal_dist["neutral_count"],
        "display_timezone": display_timezone,
    }


def run_notifier(
    db_path: Optional[str] = None,
    pipeline_stats: Optional[dict] = None,
) -> dict:
    """
    Orchestrate Phase 4: AI reasoning + Telegram delivery.

    Parameters:
        db_path: Optional override for the database file path.
        pipeline_stats: Optional pre-built stats dict from the daily pipeline.
            When omitted, stats are queried from pipeline_runs.

    Returns:
        Summary dict with keys: scoring_date, bullish_count, bearish_count,
        neutral_count, flips_count, tickers_reasoned, telegram_sent,
        subscribers_notified, duration_seconds. Returns {"skipped": True,
        "reason": str} when pre-flight checks prevent execution.
    """
    load_env()
    config = load_config("notifier")
    db_config = load_config("database")

    tg_config = get_telegram_config(config)
    bot_token = tg_config["bot_token"]
    admin_chat_id = tg_config["admin_chat_id"]
    subscriber_chat_ids = tg_config["subscriber_chat_ids"]

    resolved_db_path = db_path or db_config["path"]
    db_conn = get_connection(resolved_db_path)

    scoring_date = _resolve_scoring_date(db_conn, None)
    today_date = date.today().isoformat()

    # Pre-flight: require scorer_done on the scoring date or today
    scorer_status = get_pipeline_event_status(db_conn, "scorer_done", scoring_date)
    scorer_status_today = get_pipeline_event_status(db_conn, "scorer_done", today_date)
    if scorer_status != "completed" and scorer_status_today != "completed":
        logger.warning(
            f"notifier: scorer_done not found for {scoring_date} (or today {today_date}) — cannot run"
        )
        db_conn.close()
        return {"skipped": True, "reason": "scorer_done not found"}

    # Pre-flight: skip if already done
    notifier_status = get_pipeline_event_status(db_conn, "notifier_done", scoring_date)
    if notifier_status == "completed":
        logger.info(f"notifier: already completed for {scoring_date} — skipping")
        db_conn.close()
        return {"skipped": True, "reason": "already completed"}

    write_pipeline_event(db_conn, "notifier_done", scoring_date, "processing")
    started_at = _utc_now_iso()
    start_ts = datetime.now(tz=timezone.utc)

    logger.info(f"phase=notifier date={scoring_date} Starting notifier")

    # Run AI reasoning — wrapped so any unexpected failure returns a fallback
    results: dict = {
        "bullish": [],
        "bearish": [],
        "flips": [],
        "daily_summary": "No significant signals today.",
        "market_context_summary": "",
    }
    try:
        results = reason_all_qualifying_tickers(db_conn, scoring_date, config)
    except Exception as exc:
        logger.error(f"phase=notifier date={scoring_date} AI reasoning failed: {exc} — using fallback")

    # Signal distribution
    signal_dist = _get_signal_distribution(db_conn, scoring_date)

    # Compute notifier duration (approximate — refined below after send)
    duration_seconds = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()

    # Build pipeline_stats
    if pipeline_stats is None:
        pipeline_stats = _build_pipeline_stats(
            db_conn, scoring_date, duration_seconds, signal_dist, config
        )
    else:
        # Merge signal distribution into caller-supplied stats
        pipeline_stats = {
            **pipeline_stats,
            "bullish_count": signal_dist["bullish_count"],
            "bearish_count": signal_dist["bearish_count"],
            "neutral_count": signal_dist["neutral_count"],
            "display_timezone": config.get("telegram", {}).get("display_timezone", "Europe/Amsterdam"),
        }

    # Format report (without heartbeat — heartbeat goes separately to admin)
    has_signals = bool(results.get("bullish") or results.get("bearish") or results.get("flips"))
    if has_signals:
        messages = format_full_report(results, pipeline_stats, config, include_heartbeat=False)
    else:
        messages = format_no_signals_report(
            results.get("market_context_summary", ""), pipeline_stats, config, include_heartbeat=False
        )

    # Heartbeat (use final duration)
    final_duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
    heartbeat_stats = {**pipeline_stats, "notifier_duration": final_duration}
    heartbeat_text = format_heartbeat(heartbeat_stats)

    # Send signal report to all subscribers (without heartbeat)
    send_result: dict = {"sent": 0, "failed": 0, "total_subscribers": 0}
    telegram_sent = False
    try:
        if not subscriber_chat_ids:
            logger.warning("phase=notifier run_notifier: No subscribers configured — skipping signal report")
        else:
            send_result = send_daily_report(messages, bot_token, subscriber_chat_ids)
            telegram_sent = send_result["sent"] > 0
    except Exception as exc:
        logger.error(f"phase=notifier date={scoring_date} Telegram send failed: {exc}")

    # Send heartbeat to admin only
    try:
        include_heartbeat = config.get("telegram", {}).get("include_heartbeat", True)
        if include_heartbeat and admin_chat_id:
            send_heartbeat(heartbeat_text, bot_token, admin_chat_id)
    except Exception as exc:
        logger.error(f"phase=notifier date={scoring_date} Heartbeat send failed: {exc}")

    # Finalise
    completed_at = _utc_now_iso()
    total_duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()

    write_pipeline_event(db_conn, "notifier_done", scoring_date, "completed")

    log_pipeline_run(
        db_conn,
        date=scoring_date,
        phase="notifier",
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=total_duration,
        tickers_processed=len(results.get("bullish", [])) + len(results.get("bearish", [])),
        tickers_skipped=0,
        tickers_failed=0,
        api_calls_made=0,
        status="success",
    )

    db_conn.close()

    logger.info(
        f"phase=notifier date={scoring_date} Completed in {total_duration:.1f}s "
        f"telegram_sent={telegram_sent} subscribers_notified={send_result['sent']}"
    )

    return {
        "scoring_date": scoring_date,
        "bullish_count": signal_dist["bullish_count"],
        "bearish_count": signal_dist["bearish_count"],
        "neutral_count": signal_dist["neutral_count"],
        "flips_count": len(results.get("flips", [])),
        "tickers_reasoned": len(results.get("bullish", [])) + len(results.get("bearish", [])),
        "telegram_sent": telegram_sent,
        "subscribers_notified": send_result["sent"],
        "duration_seconds": total_duration,
    }
