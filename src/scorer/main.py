"""
Scorer orchestrator — Phase 3 of the pipeline.

Scores all active tickers and produces BULLISH/BEARISH/NEUTRAL signals
with confidence scores. Writes results to scores_daily table and
writes "scorer_done" pipeline event to trigger the Notifier (Phase 4).

Supports:
  - Daily scoring (current day)
  - Single ticker scoring
  - Historical scoring (Option E: last 12mo daily + older weekly)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from src.common.config import get_active_tickers, load_config, load_env
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
from src.scorer.category_scorer import apply_adaptive_weights, compute_all_category_scores
from src.scorer.confidence import (
    build_data_completeness,
    build_key_signals,
    classify_signal,
    compute_full_confidence,
    get_next_earnings_date,
)
from src.scorer.flip_detector import (
    detect_flips_for_all,
    get_flips_for_date,
)
from src.scorer.indicator_scorer import load_profile_for_ticker, score_all_indicators
from src.scorer.pattern_scorer import (
    score_candlestick_patterns,
    score_crossovers,
    score_divergences,
    score_fibonacci,
    score_fundamentals as pattern_score_fundamentals,
    score_gaps,
    score_macro as pattern_score_macro,
    score_news_sentiment,
    score_short_interest,
    score_structural_patterns,
)
from src.scorer.calibrator import build_feature_vector, calibrate_score
from src.scorer.regime import detect_regime, get_atr_sma, get_current_vix, get_regime_weights
from src.scorer.sector_adjuster import apply_sector_adjustment, compute_sector_etf_score
from src.scorer.timeframe_merger import compute_weekly_score, merge_timeframes

logger = logging.getLogger(__name__)

_PHASE = "scorer"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _today_date() -> str:
    """Return today's date as YYYY-MM-DD string."""
    return date.today().isoformat()


def _get_db_path(db_path: Optional[str]) -> str:
    """Resolve the database path from argument or config."""
    if db_path:
        return db_path
    db_config = load_config("database")
    return db_config["path"]


def _resolve_scoring_date(
    db_conn: sqlite3.Connection,
    explicit_date: Optional[str],
) -> str:
    """Determine the date to use for scoring.

    If an explicit date was provided, return it unchanged. Otherwise, find the
    most common latest date that has indicator data across all tickers (i.e.
    the latest trading day in the DB). Falls back to today if the DB is empty.

    This handles the common case where today is a weekend or holiday and the
    most recent indicator data is from Friday.

    Parameters:
        db_conn: Open SQLite connection.
        explicit_date: Caller-supplied date override (YYYY-MM-DD), or None.

    Returns:
        The resolved scoring date as a YYYY-MM-DD string.
    """
    if explicit_date:
        return explicit_date

    today = _today_date()

    # Find the most common "latest date" across all tickers. For each ticker
    # we look at its MAX(date) in indicators_daily, then pick the date that
    # appears most often — this is the most recent trading day in the DB.
    # Ties are broken by date DESC so the more recent date wins.
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

    if row is None or row["date"] is None:
        return today

    latest_date: str = row["date"]
    if latest_date != today:
        logger.info(
            f"No indicator data for {today}, scoring latest available date: {latest_date}"
        )

    return latest_date


def _load_indicators(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[dict]:
    """Load the indicators_daily row for a ticker on the given date."""
    row = db_conn.execute(
        "SELECT * FROM indicators_daily WHERE ticker = ? AND date = ?",
        (ticker, scoring_date),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def _load_close_price(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[float]:
    """Load the closing price from ohlcv_daily for a ticker on the given date."""
    row = db_conn.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
        (ticker, scoring_date),
    ).fetchone()
    if row is None:
        return None
    return float(row["close"])


def _load_patterns(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    lookback_days: int = 30,
) -> list[dict]:
    """Load pattern rows from patterns_daily for the last lookback_days."""
    cutoff = (date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat()
    rows = db_conn.execute(
        "SELECT * FROM patterns_daily WHERE ticker = ? AND date >= ? AND date <= ? "
        "ORDER BY date DESC",
        (ticker, cutoff, scoring_date),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_divergences(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    lookback_days: int = 42,
) -> list[dict]:
    """Load divergences from divergences_daily for the last lookback_days."""
    cutoff = (date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat()
    rows = db_conn.execute(
        "SELECT * FROM divergences_daily WHERE ticker = ? AND date >= ? AND date <= ? "
        "ORDER BY date DESC",
        (ticker, cutoff, scoring_date),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_crossovers(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    lookback_days: int = 14,
) -> list[dict]:
    """Load crossovers from crossovers_daily for the last lookback_days."""
    cutoff = (date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat()
    rows = db_conn.execute(
        "SELECT * FROM crossovers_daily WHERE ticker = ? AND date >= ? AND date <= ? "
        "ORDER BY date DESC",
        (ticker, cutoff, scoring_date),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_gaps(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    lookback_days: int = 14,
) -> list[dict]:
    """Load gaps from gaps_daily for the last lookback_days."""
    cutoff = (date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat()
    rows = db_conn.execute(
        "SELECT * FROM gaps_daily WHERE ticker = ? AND date >= ? AND date <= ? "
        "ORDER BY date DESC",
        (ticker, cutoff, scoring_date),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_news_summary(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
) -> Optional[dict]:
    """Load the most recent news_daily_summary for a ticker on or before scoring_date."""
    row = db_conn.execute(
        "SELECT * FROM news_daily_summary WHERE ticker = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, scoring_date),
    ).fetchone()
    return dict(row) if row else None


def _load_short_interest(
    db_conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """Load the most recent short interest record for a ticker."""
    row = db_conn.execute(
        "SELECT * FROM short_interest WHERE ticker = ? ORDER BY settlement_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


def _load_fundamentals(
    db_conn: sqlite3.Connection,
    ticker: str,
) -> Optional[dict]:
    """Load the most recent fundamentals record for a ticker."""
    row = db_conn.execute(
        "SELECT * FROM fundamentals WHERE ticker = ? ORDER BY report_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None




def _score_macro(
    db_conn: sqlite3.Connection,
    vix_close: Optional[float],
    sector_etf_score: Optional[float],
    scoring_date: str,
    ticker: str = "",
) -> float:
    """
    Compute a macro score from SPY trend, VIX level, sector trend, and relative strength.

    Uses pattern_scorer.score_macro() after computing the component values.
    """
    # SPY trend
    spy_trend = 0.0
    spy_row = db_conn.execute(
        "SELECT i.ema_9, i.ema_21, i.ema_50, o.close "
        "FROM indicators_daily i "
        "JOIN ohlcv_daily o ON i.ticker = o.ticker AND i.date = o.date "
        "WHERE i.ticker = 'SPY' AND i.date <= ? "
        "ORDER BY i.date DESC LIMIT 1",
        (scoring_date,),
    ).fetchone()

    if spy_row:
        close = spy_row["close"]
        ema_9 = spy_row["ema_9"]
        ema_21 = spy_row["ema_21"]
        ema_50 = spy_row["ema_50"]
        if all(v is not None for v in (close, ema_9, ema_21, ema_50)):
            from src.scorer.indicator_scorer import score_ema_alignment
            spy_trend = score_ema_alignment(close, ema_9, ema_21, ema_50)

    # VIX score
    vix_score = 0.0
    if vix_close is not None:
        if vix_close > 30:
            vix_score = -80.0
        elif vix_close > 20:
            t = (vix_close - 20) / 10
            vix_score = -(t * 80.0)
        else:
            vix_score = 30.0

    # Relative strength vs market
    rs_market = _score_relative_strength(db_conn, ticker, scoring_date) if ticker else None

    # Treasury trend (simplified — positive 10yr yield change = headwind)
    treasury_trend = 0.0
    treasury_row = db_conn.execute(
        "SELECT yield_10_year FROM treasury_yields ORDER BY date DESC LIMIT 2",
    ).fetchall()
    if len(treasury_row) >= 2:
        latest = treasury_row[0]["yield_10_year"]
        prev = treasury_row[1]["yield_10_year"]
        if latest is not None and prev is not None and prev > 0:
            treasury_trend = (latest - prev) / prev * 1000  # small changes → small score

    return pattern_score_macro(
        spy_trend=spy_trend,
        vix_score=vix_score,
        sector_etf_trend=sector_etf_score if sector_etf_score is not None else 0.0,
        treasury_trend=treasury_trend,
        rs_market=rs_market,
        rs_sector=None,
    )


def _score_relative_strength(
    db_conn: sqlite3.Connection,
    ticker: str,
    scoring_date: str,
    lookback_days: int = 20,
) -> float:
    """
    Compute relative strength vs SPY over the last lookback_days.

    Returns a score from -100 to +100 based on the price performance ratio.
    """
    try:
        ticker_start_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? "
            "ORDER BY date ASC LIMIT 1",
            (ticker, (date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat()),
        ).fetchone()

        ticker_end_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date <= ? "
            "ORDER BY date DESC LIMIT 1",
            (ticker, scoring_date),
        ).fetchone()

        spy_start_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = 'SPY' AND date <= ? "
            "ORDER BY date ASC LIMIT 1",
            ((date.fromisoformat(scoring_date) - timedelta(days=lookback_days)).isoformat(),),
        ).fetchone()

        spy_end_row = db_conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = 'SPY' AND date <= ? "
            "ORDER BY date DESC LIMIT 1",
            (scoring_date,),
        ).fetchone()

        if not all([ticker_start_row, ticker_end_row, spy_start_row, spy_end_row]):
            return 0.0

        ticker_return = (ticker_end_row["close"] / ticker_start_row["close"]) - 1
        spy_return = (spy_end_row["close"] / spy_start_row["close"]) - 1
        relative = ticker_return - spy_return
        return max(-100.0, min(100.0, relative * 500.0))
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# save_score_to_db
# ---------------------------------------------------------------------------

def save_score_to_db(db_conn: sqlite3.Connection, score: dict) -> None:
    """
    Insert or replace a score record in the scores_daily table.

    Uses INSERT OR REPLACE to make re-runs idempotent. Serialises
    data_completeness and key_signals as JSON strings if they are dicts/lists.

    Parameters:
        db_conn: Open SQLite connection with WAL mode enabled.
        score: Score dict with all required fields.
    """
    data_completeness = score.get("data_completeness")
    if isinstance(data_completeness, dict):
        data_completeness = json.dumps(data_completeness)

    key_signals = score.get("key_signals")
    if isinstance(key_signals, list):
        key_signals = json.dumps(key_signals)

    db_conn.execute(
        """
        INSERT OR REPLACE INTO scores_daily
            (ticker, date, signal, confidence, final_score, regime,
             daily_score, weekly_score, trend_score, momentum_score,
             volume_score, volatility_score, candlestick_score, structural_score,
             sentiment_score, fundamental_score, macro_score,
             calibrated_score, model_r2,
             data_completeness, key_signals)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            score["ticker"],
            score["date"],
            score["signal"],
            score["confidence"],
            score["final_score"],
            score["regime"],
            score["daily_score"],
            score.get("weekly_score"),
            score.get("trend_score"),
            score.get("momentum_score"),
            score.get("volume_score"),
            score.get("volatility_score"),
            score.get("candlestick_score"),
            score.get("structural_score"),
            score.get("sentiment_score"),
            score.get("fundamental_score"),
            score.get("macro_score"),
            score.get("calibrated_score"),
            score.get("model_r2"),
            data_completeness,
            key_signals,
        ),
    )
    db_conn.commit()


# ---------------------------------------------------------------------------
# score_ticker
# ---------------------------------------------------------------------------

def score_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    ticker_config: dict,
    scoring_date: str,
    config: dict,
) -> Optional[dict]:
    """
    Compute a complete score for a single ticker on a single date.

    Runs the full scoring pipeline:
    1. Load indicators → score all indicators
    2. Detect regime → get adaptive weights
    3. Load patterns, divergences, crossovers, gaps → score
    4. Compute news sentiment and short interest scores
    5. Score fundamentals and macro
    6. Compute all 9 category scores → apply adaptive weights → daily_score
    7. Apply sector adjustment
    8. Compute weekly score → merge with regime-adaptive timeframe weights
    9. Calibrate score via rolling ridge regression (predicted excess return)
    10. Classify signal using calibrated score; compute confidence
    11. Save to scores_daily; detect and save any signal flip

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        ticker_config: Ticker dict with at least symbol, sector_etf.
        scoring_date: Date to score in YYYY-MM-DD format.
        config: Full scorer config dict.

    Returns:
        Complete score dict, or None if no indicator data is available for the date.
    """
    # 1. Load indicators
    indicators = _load_indicators(db_conn, ticker, scoring_date)
    if indicators is None:
        logger.debug(f"{ticker}: no indicator data for {scoring_date} — skipping")
        return None

    # 2. Load close price
    close = _load_close_price(db_conn, ticker, scoring_date)
    if close is None:
        logger.debug(f"{ticker}: no close price for {scoring_date} — skipping")
        return None

    # 3. VIX and ATR SMA
    vix_close = get_current_vix(db_conn)
    atr_sma = get_atr_sma(db_conn, ticker)
    atr = indicators.get("atr_14")

    # 4. Detect regime
    regime = detect_regime(
        adx=indicators.get("adx"),
        atr=atr,
        atr_sma_20=atr_sma,
        vix_close=vix_close,
        config=config,
        close=close,
        ema_9=indicators.get("ema_9"),
        ema_21=indicators.get("ema_21"),
        ema_50=indicators.get("ema_50"),
    )
    regime_weights = get_regime_weights(regime, config)

    # 5. Score indicators
    profiles = load_profile_for_ticker(db_conn, ticker)
    indicator_scores = score_all_indicators(indicators, close, profiles, config, regime=regime)

    # 6. Load and score patterns
    patterns = _load_patterns(db_conn, ticker, scoring_date)
    divergences = _load_divergences(db_conn, ticker, scoring_date)
    crossovers = _load_crossovers(db_conn, ticker, scoring_date)
    gaps = _load_gaps(db_conn, ticker, scoring_date)

    candlestick_score = score_candlestick_patterns(patterns, scoring_date)
    structural_score = score_structural_patterns(patterns, scoring_date)

    # Score divergences per indicator type
    div_rsi = score_divergences(
        [d for d in divergences if d.get("indicator") == "rsi_14"], scoring_date
    )
    div_macd = score_divergences(
        [d for d in divergences if d.get("indicator") == "macd_histogram"], scoring_date
    )
    div_stoch = score_divergences(
        [d for d in divergences if d.get("indicator") == "stoch_k"], scoring_date
    )
    div_obv = score_divergences(
        [d for d in divergences if d.get("indicator") == "obv"], scoring_date
    )

    # Score crossovers per type
    crossover_ema_9_21 = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "ema_9_21"], scoring_date
    )
    crossover_ema_21_50 = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "ema_21_50"], scoring_date
    )
    crossover_macd = score_crossovers(
        [c for c in crossovers if c.get("crossover_type") == "macd_signal"], scoring_date
    )

    # Score gaps
    gap_score = score_gaps(gaps, scoring_date)

    # Score Fibonacci (compute on-the-fly using calculator module)
    try:
        from src.calculator.fibonacci import compute_fibonacci_for_ticker
        calc_config = load_config("calculator")
        fib_result = compute_fibonacci_for_ticker(db_conn, ticker, calc_config)
        fibonacci_score = score_fibonacci(fib_result)
    except Exception:
        fibonacci_score = 0.0

    pattern_scores = {
        "candlestick_pattern_score": candlestick_score,
        "structural_pattern_score": structural_score,
        "divergence_rsi": div_rsi,
        "divergence_macd": div_macd,
        "divergence_stoch": div_stoch,
        "divergence_obv": div_obv,
        "crossover_ema_9_21": crossover_ema_9_21,
        "crossover_ema_21_50": crossover_ema_21_50,
        "crossover_macd_signal": crossover_macd,
        "gap_score": gap_score,
        "fibonacci_score": fibonacci_score,
    }

    # 7. Load and score news, short interest
    news = _load_news_summary(db_conn, ticker, scoring_date)
    news_sentiment_score = score_news_sentiment(
        avg_sentiment=news.get("avg_sentiment_score") if news else None,
        article_count=news.get("article_count", 0) if news else 0,
        filing_flag=bool(news.get("filing_flag", False)) if news else False,
    )
    short_data = _load_short_interest(db_conn, ticker)
    short_score = score_short_interest(
        days_to_cover=short_data.get("days_to_cover") if short_data else None
    )

    sentiment_scores = {
        "news_sentiment_score": news_sentiment_score,
        "short_interest_score": short_score,
    }

    # 8. Score fundamentals
    fundamentals_data = _load_fundamentals(db_conn, ticker)
    fundamental_score_val = pattern_score_fundamentals(fundamentals_data)

    # 9. Score sector ETF and macro
    sector_etf = ticker_config.get("sector_etf")
    sector_etf_score = compute_sector_etf_score(db_conn, sector_etf) if sector_etf else None
    macro_score_val = _score_macro(db_conn, vix_close, sector_etf_score, scoring_date, ticker)

    # 10. Compute all 9 category scores
    category_scores = compute_all_category_scores(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        sentiment_scores=sentiment_scores,
        fundamental_score=fundamental_score_val,
        macro_score=macro_score_val,
    )

    # 11. Apply adaptive weights → raw daily score
    expansion_factor = config.get("scoring", {}).get("score_expansion_factor", 1.0)
    raw_daily = apply_adaptive_weights(category_scores, regime_weights, expansion_factor)

    # 12. Apply sector adjustment
    daily_score = apply_sector_adjustment(raw_daily, sector_etf_score, config)

    # 13. Compute weekly score
    weekly_score = compute_weekly_score(db_conn, ticker, config, scoring_date=scoring_date, regime=regime)
    weekly_available = weekly_score is not None

    # 14. Merge timeframes → final score (regime-adaptive weights)
    final_score = merge_timeframes(daily_score, weekly_score, config, regime=regime)

    # 15. Calibrate score using rolling ridge regression
    ema_9 = indicators.get("ema_9")
    ema_21 = indicators.get("ema_21")
    ema_50 = indicators.get("ema_50")
    ema_positions = {
        "price_ema9_spread": ((close - ema_9) / ema_9 * 100.0) if ema_9 and ema_9 != 0 else 0.0,
        "ema9_ema21_spread": ((ema_9 - ema_21) / ema_21 * 100.0) if ema_9 and ema_21 and ema_21 != 0 else 0.0,
        "ema21_ema50_spread": ((ema_21 - ema_50) / ema_50 * 100.0) if ema_21 and ema_50 and ema_50 != 0 else 0.0,
    }
    raw_indicators_for_calibrator = {
        "rsi_14": indicators.get("rsi_14"),
        "adx": indicators.get("adx"),
        "macd_histogram": indicators.get("macd_histogram"),
        "stoch_k": indicators.get("stoch_k"),
        "bb_pctb": indicators.get("bb_pctb"),
        "cmf_20": indicators.get("cmf_20"),
    }
    calibration_config = config.get("calibration", {})
    calibration_result = calibrate_score(
        conn=db_conn,
        scoring_date=scoring_date,
        category_scores=category_scores,
        raw_indicators=raw_indicators_for_calibrator,
        ema_positions=ema_positions,
        config=calibration_config,
    )
    calibrated_score = calibration_result["calibrated_score"]
    model_r2 = calibration_result["model_r2"]

    # Use calibrated_score for signal classification when available;
    # fall back to final_score (static composite) during cold start.
    effective_score = calibrated_score if calibrated_score is not None else final_score

    # 16. Classify signal
    signal = classify_signal(effective_score, config)

    # 17. Compute confidence
    next_earnings = get_next_earnings_date(db_conn, ticker, scoring_date)
    filings_row = db_conn.execute(
        "SELECT 1 FROM filings_8k WHERE ticker = ? LIMIT 1", (ticker,)
    ).fetchone()

    # Derive confidence base from calibrated_score when available.
    # abs(calibrated_score) correlates with prediction accuracy up to ~8; above
    # that the calibrator overfits and accuracy drops (|cal| 8-12 → 57.6%,
    # |cal| > 12 → 47.7% — worse than the baseline).  Cap at 8.0 to prevent
    # extreme, low-reliability predictions from inflating confidence to 80-100%.
    # Scale: abs(cal) of 2 → base 20, abs(cal) of 5 → base 50, abs(cal) of 8+→ base 80.
    #
    # Cold start (calibrated_score is None): final_score has near-zero correlation
    # with returns (R ≈ -0.006).  Discount by 0.3 so confidence derives mainly
    # from the quality modifiers (data completeness, VIX, volatility, etc.).
    if calibrated_score is not None:
        confidence_base_score = min(abs(calibrated_score), 8.0) * 10.0
    else:
        confidence_base_score = abs(final_score) * 0.3

    confidence_result = compute_full_confidence(
        final_score=confidence_base_score,
        daily_score=daily_score,
        weekly_score=weekly_score,
        category_scores=category_scores,
        indicator_scores=indicator_scores,
        earnings_date=next_earnings,
        scoring_date=scoring_date,
        vix=vix_close,
        atr=atr,
        atr_sma=atr_sma,
        news_available=news is not None,
        fundamentals_available=fundamentals_data is not None,
        config=config,
    )

    # 18. Build data completeness
    data_completeness = build_data_completeness(
        news_available=news is not None,
        fundamentals_available=fundamentals_data is not None,
        weekly_available=weekly_available,
        filings_available=filings_row is not None,
        short_interest_available=short_data is not None,
        earnings_available=next_earnings is not None,
    )

    # 19. Build key signals
    key_signals = build_key_signals(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime=regime,
        category_scores=category_scores,
        final_score=final_score,
        signal=signal,
    )

    score = {
        "ticker": ticker,
        "date": scoring_date,
        "signal": signal,
        "confidence": confidence_result["confidence"],
        "final_score": final_score,
        "regime": regime,
        "daily_score": daily_score,
        "weekly_score": weekly_score,
        "trend_score": category_scores.get("trend"),
        "momentum_score": category_scores.get("momentum"),
        "volume_score": category_scores.get("volume"),
        "volatility_score": category_scores.get("volatility"),
        "candlestick_score": category_scores.get("candlestick"),
        "structural_score": category_scores.get("structural"),
        "sentiment_score": category_scores.get("sentiment"),
        "fundamental_score": category_scores.get("fundamental"),
        "macro_score": category_scores.get("macro"),
        "calibrated_score": calibrated_score,
        "model_r2": model_r2,
        "data_completeness": json.dumps(data_completeness),
        "key_signals": json.dumps(key_signals),
    }

    # 20. Save to DB
    save_score_to_db(db_conn, score)

    logger.info(
        f"{ticker}: {signal} (confidence={confidence_result['confidence']:.0f}%, "
        f"final_score={final_score:.1f}, calibrated={calibrated_score}, "
        f"effective={effective_score:.1f}) on {scoring_date}"
    )
    return score


# ---------------------------------------------------------------------------
# run_scorer
# ---------------------------------------------------------------------------

def run_scorer(
    db_path: Optional[str] = None,
    mode: str = "daily",
    ticker_filter: Optional[str] = None,
    scoring_date: Optional[str] = None,
    force: bool = False,
) -> dict:
    """
    Orchestrate the scoring phase for all active tickers.

    Pre-flight checks: verifies calculator_done event exists and scorer_done is not
    already completed. Writes pipeline events, scores each ticker, detects flips,
    and sends Telegram summary.

    Parameters:
        db_path: Optional override for the database file path.
        mode: Scoring mode — currently only "daily" is used here.
        ticker_filter: Optional single ticker symbol to restrict scoring.
        scoring_date: Optional date override (YYYY-MM-DD). When omitted the
            scoring date is resolved from the latest indicator data in the DB,
            so weekends and holidays are handled automatically.
        force: When True, bypass the "already completed" check and re-score
            even if scorer_done is already marked completed for the date.

    Returns:
        Summary dict with tickers_processed, tickers_skipped, tickers_failed,
        bullish_count, bearish_count, neutral_count, flips_detected, duration_seconds,
        and a skipped flag if scoring was skipped.
    """
    load_env()
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")

    resolved_db_path = _get_db_path(db_path)
    config = load_config("scorer")

    db_conn = get_connection(resolved_db_path)

    # Resolve scoring date: use the latest date with indicator data if no
    # explicit date was provided (handles weekends / holidays automatically).
    scoring_date = _resolve_scoring_date(db_conn, scoring_date)

    # Pre-flight: require calculator_done
    # Belt-and-suspenders: accept the event on EITHER the resolved scoring date
    # (e.g. 2026-03-16) OR today's wall-clock date (e.g. 2026-03-18).  This
    # handles the case where the calculator ran today but the data it processed
    # is from the last trading day.
    today_date = _today_date()
    calc_status_data = get_pipeline_event_status(db_conn, "calculator_done", scoring_date)
    calc_status_today = get_pipeline_event_status(db_conn, "calculator_done", today_date)
    if calc_status_data != "completed" and calc_status_today != "completed":
        logger.warning(
            f"scorer: calculator_done event not found for {scoring_date} "
            f"(or today {today_date}) — cannot score"
        )
        db_conn.close()
        return {"skipped": True, "reason": "calculator_done not found"}

    # Pre-flight: skip if already done (unless forced)
    scorer_status = get_pipeline_event_status(db_conn, "scorer_done", scoring_date)
    if scorer_status == "completed" and not force:
        logger.info(f"scorer: already completed for {scoring_date} — skipping (use --force to override)")
        db_conn.close()
        return {"skipped": True, "reason": "already completed"}
    if force and scorer_status == "completed":
        logger.info(f"scorer: force=True, re-scoring {scoring_date} despite completed status")

    # Mark as processing
    write_pipeline_event(db_conn, "scorer_done", scoring_date, "processing")

    started_at = _utc_now_iso()
    start_ts = datetime.now(tz=timezone.utc)

    # Get tickers
    all_tickers = get_active_tickers()
    if ticker_filter:
        all_tickers = [t for t in all_tickers if t["symbol"] == ticker_filter]

    ticker_symbols = [t["symbol"] for t in all_tickers]
    ticker_map = {t["symbol"]: t for t in all_tickers}

    tracker = ProgressTracker(phase="Scorer", tickers=ticker_symbols)
    msg_id = send_telegram_message(
        telegram_token, telegram_chat_id, tracker.format_progress_message()
    )

    scored_results: list[dict] = []
    failed_count = 0
    skipped_count = 0

    for ticker in ticker_symbols:
        ticker_config = ticker_map[ticker]
        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(telegram_token, telegram_chat_id, msg_id, tracker.format_progress_message())

        try:
            result = score_ticker(
                db_conn=db_conn,
                ticker=ticker,
                ticker_config=ticker_config,
                scoring_date=scoring_date,
                config=config,
            )
            if result is None:
                tracker.mark_skipped(ticker, reason="no indicator data")
                skipped_count += 1
            else:
                scored_results.append(result)
                tracker.mark_completed(ticker, details=result["signal"])
        except Exception as exc:
            logger.error(f"{ticker}: scoring failed — {exc}", exc_info=True)
            log_alert(db_conn, ticker, scoring_date, _PHASE, "error", str(exc))
            tracker.mark_failed(ticker, reason=str(exc))
            failed_count += 1
            continue

    # Detect flips
    if scored_results:
        detect_flips_for_all(db_conn, scored_results, scoring_date)

    flips = get_flips_for_date(db_conn, scoring_date)

    # Signal distribution
    bullish_count = sum(1 for r in scored_results if r["signal"] == "BULLISH")
    bearish_count = sum(1 for r in scored_results if r["signal"] == "BEARISH")
    neutral_count = sum(1 for r in scored_results if r["signal"] == "NEUTRAL")
    tickers_processed = len(scored_results)

    # Timing
    completed_at = _utc_now_iso()
    duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()

    # Update pipeline event
    write_pipeline_event(db_conn, "scorer_done", scoring_date, "completed")

    # Log pipeline run
    log_pipeline_run(
        db_conn=db_conn,
        date=scoring_date,
        phase=_PHASE,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration,
        tickers_processed=tickers_processed,
        tickers_skipped=skipped_count,
        tickers_failed=failed_count,
        api_calls_made=0,
        status="success" if failed_count == 0 else "partial",
    )

    # Telegram summary
    mins = int(duration // 60)
    secs = int(duration % 60)
    summary_text = (
        f"📊 Scorer Complete — {scoring_date}\n"
        f"Tickers: {tickers_processed}/{len(ticker_symbols)}\n"
        f"🟢 Bullish: {bullish_count} | 🔴 Bearish: {bearish_count} | 🟡 Neutral: {neutral_count}\n"
        f"🔄 Signal Flips: {len(flips)}\n"
        f"Duration: {mins}m {secs}s"
    )
    send_telegram_message(telegram_token, telegram_chat_id, summary_text)

    db_conn.close()

    return {
        "tickers_total": len(ticker_symbols),
        "tickers_processed": tickers_processed,
        "tickers_skipped": skipped_count,
        "tickers_failed": failed_count,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "neutral_count": neutral_count,
        "flips_detected": len(flips),
        "duration_seconds": duration,
        "scoring_date": scoring_date,
    }


# ---------------------------------------------------------------------------
# run_historical_scoring
# ---------------------------------------------------------------------------

def run_historical_scoring(
    db_path: Optional[str] = None,
    ticker_filter: Optional[str] = None,
    mode: str = "both",
) -> dict:
    """
    Run historical scoring following Option E.

    Option E strategy:
    - Last 12 months: compute daily scores for each trading day.
    - Months 13-60: compute weekly scores only (using weekly indicators).

    Parameters:
        db_path: Optional database path override.
        ticker_filter: Optional ticker symbol to restrict scoring.
        mode: One of "daily", "weekly", or "both" (Option E default).

    Returns:
        Summary dict with mode, total_scores, duration_seconds.
    """
    load_env()
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")

    resolved_db_path = _get_db_path(db_path)
    config = load_config("scorer")
    hist_cfg = config.get("historical_scoring", {})
    daily_lookback_months: int = hist_cfg.get("daily_lookback_months", 12)
    weekly_lookback_months: int = hist_cfg.get("weekly_lookback_months", 60)

    db_conn = get_connection(resolved_db_path)

    all_tickers = get_active_tickers()
    if ticker_filter:
        all_tickers = [t for t in all_tickers if t["symbol"] == ticker_filter]

    started_at = _utc_now_iso()
    start_ts = datetime.now(tz=timezone.utc)
    total_scores = 0

    today = date.today()

    if mode in ("daily", "both"):
        # Build list of trading dates for the last 12 months
        daily_start = today - timedelta(days=daily_lookback_months * 31)
        daily_dates = _get_trading_dates(db_conn, daily_start.isoformat(), today.isoformat())

        logger.info(
            f"Historical daily scoring: {len(daily_dates)} dates × {len(all_tickers)} tickers"
        )

        for dt in daily_dates:
            for tc in all_tickers:
                ticker = tc["symbol"]
                try:
                    result = score_ticker(
                        db_conn=db_conn,
                        ticker=ticker,
                        ticker_config=tc,
                        scoring_date=dt,
                        config=config,
                    )
                    if result is not None:
                        total_scores += 1
                except Exception as exc:
                    logger.error(f"{ticker} on {dt}: historical scoring failed — {exc}")

    if mode in ("weekly", "both"):
        # Build list of week starts for months 13-60
        weekly_end = today - timedelta(days=daily_lookback_months * 31)
        weekly_start = today - timedelta(days=weekly_lookback_months * 31)
        weekly_dates = _get_weekly_dates(db_conn, weekly_start.isoformat(), weekly_end.isoformat())

        logger.info(
            f"Historical weekly scoring: {len(weekly_dates)} weeks × {len(all_tickers)} tickers"
        )

        for week_start in weekly_dates:
            for tc in all_tickers:
                ticker = tc["symbol"]
                try:
                    result = score_ticker(
                        db_conn=db_conn,
                        ticker=ticker,
                        ticker_config=tc,
                        scoring_date=week_start,
                        config=config,
                    )
                    if result is not None:
                        total_scores += 1
                except Exception as exc:
                    logger.error(f"{ticker} on {week_start}: weekly historical scoring failed — {exc}")

    duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()

    summary_text = (
        f"📊 Historical Scoring Complete\n"
        f"Mode: {mode}\n"
        f"Total scores: {total_scores}\n"
        f"Duration: {int(duration // 60)}m {int(duration % 60)}s"
    )
    if telegram_token and telegram_chat_id:
        send_telegram_message(telegram_token, telegram_chat_id, summary_text)

    db_conn.close()

    return {
        "mode": mode,
        "total_scores": total_scores,
        "duration_seconds": duration,
        "tickers": len(all_tickers),
    }


def _get_trading_dates(
    db_conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Return a sorted list of unique trading dates from ohlcv_daily between start and end.

    Dates are sourced from actual data in the database (weekends/holidays absent naturally).

    Parameters:
        db_conn: Open SQLite connection.
        start_date: Start date (YYYY-MM-DD), inclusive.
        end_date: End date (YYYY-MM-DD), inclusive.

    Returns:
        Sorted list of date strings.
    """
    rows = db_conn.execute(
        "SELECT DISTINCT date FROM ohlcv_daily WHERE date >= ? AND date <= ? ORDER BY date ASC",
        (start_date, end_date),
    ).fetchall()
    return [r["date"] for r in rows]


def _get_weekly_dates(
    db_conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Return a sorted list of week_start dates from weekly_candles between start and end.

    Parameters:
        db_conn: Open SQLite connection.
        start_date: Start date (YYYY-MM-DD), inclusive.
        end_date: End date (YYYY-MM-DD), inclusive.

    Returns:
        Sorted list of week_start date strings.
    """
    rows = db_conn.execute(
        "SELECT DISTINCT week_start FROM weekly_candles "
        "WHERE week_start >= ? AND week_start <= ? ORDER BY week_start ASC",
        (start_date, end_date),
    ).fetchall()
    return [r["week_start"] for r in rows]
