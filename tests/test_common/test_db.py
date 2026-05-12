"""
Tests for src/common/db.py — database layer for the Stock Signal Engine.

Tests are written first (TDD). All tests use pytest's tmp_path fixture so no
real database files are left behind after the test session.
"""

import sqlite3

import pytest

from src.common.db import create_all_tables, get_connection


# ── Constants ──────────────────────────────────────────────────────────────────

ALL_TABLES = [
    "alerts_log",
    "crossovers_daily",
    "crossovers_monthly",
    "crossovers_weekly",
    "dashboard_verdicts",
    "divergences_daily",
    "divergences_monthly",
    "divergences_weekly",
    "dividends",
    "earnings_calendar",
    "filings_8k",
    "fundamentals",
    "gaps_daily",
    "indicator_profiles",
    "indicator_profiles_monthly",
    "indicator_profiles_weekly",
    "indicator_scores_daily",
    "indicator_scores_monthly",
    "indicator_scores_weekly",
    "indicators_daily",
    "indicators_monthly",
    "indicators_weekly",
    "monthly_candles",
    "news_articles",
    "news_daily_summary",
    "ohlcv_daily",
    "patterns_daily",
    "patterns_monthly",
    "patterns_weekly",
    "pipeline_events",
    "pipeline_runs",
    "scores_daily",
    "scores_monthly",
    "scores_weekly",
    "short_interest",
    "signal_flips",
    "splits",
    "support_resistance",
    "support_resistance_monthly",
    "support_resistance_weekly",
    "swing_points",
    "swing_points_monthly",
    "swing_points_weekly",
    "tickers",
    "treasury_yields",
    "weekly_candles",
]

EXPECTED_INDEXES = [
    "idx_alerts_log_date",
    "idx_crossovers_ticker_date",
    "idx_crossovers_monthly_ticker_month_start",
    "idx_crossovers_weekly_ticker_week_start",
    "idx_dashboard_verdicts_ticker_date",
    "idx_divergences_ticker_date",
    "idx_divergences_monthly_ticker_month_start",
    "idx_divergences_weekly_ticker_week_start",
    "idx_gaps_ticker_date",
    "idx_indicator_profiles_monthly_ticker_indicator",
    "idx_indicator_profiles_weekly_ticker_indicator",
    "idx_indicators_ticker_date",
    "idx_indicators_weekly_ticker",
    "idx_news_summary_ticker_date",
    "idx_news_ticker_date",
    "idx_ohlcv_ticker_date",
    "idx_patterns_ticker_date",
    "idx_patterns_monthly_ticker_month_start",
    "idx_patterns_weekly_ticker_week_start",
    "idx_pipeline_events",
    "idx_scores_ticker_date",
    "idx_scores_monthly_ticker_month_start",
    "idx_scores_weekly_ticker_week_start",
    "idx_short_interest_ticker",
    "idx_support_resistance_monthly_ticker_month_start",
    "idx_support_resistance_weekly_ticker_week_start",
    "idx_swing_points_ticker_date",
    "idx_swing_points_monthly_ticker_month_start",
    "idx_swing_points_weekly_ticker_week_start",
    "idx_weekly_candles_ticker",
]


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def fresh_db(tmp_path) -> sqlite3.Connection:
    """Open a fresh connection, create all tables, yield the connection, then close."""
    db_path = str(tmp_path / "test_signals.db")
    conn = get_connection(db_path)
    create_all_tables(conn)
    yield conn
    conn.close()


# ── Schema Tests ───────────────────────────────────────────────────────────────

def test_create_all_tables(tmp_path: pytest.TempPathFactory) -> None:
    """
    create_all_tables() must create every table listed in DESIGN.md section 4.

    Queries sqlite_master after setup and asserts each expected table name is present.
    """
    db_path = str(tmp_path / "test_signals.db")
    conn = get_connection(db_path)
    create_all_tables(conn)

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    existing_tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    for table_name in ALL_TABLES:
        assert table_name in existing_tables, f"Table '{table_name}' was not created"


def test_wal_mode_enabled(tmp_path: pytest.TempPathFactory) -> None:
    """
    get_connection() must enable WAL journal mode.

    Queries PRAGMA journal_mode and asserts the returned value is 'wal'.
    """
    db_path = str(tmp_path / "test_signals.db")
    conn = get_connection(db_path)
    row = conn.execute("PRAGMA journal_mode").fetchone()
    conn.close()

    assert row[0] == "wal"


def test_indexes_exist(fresh_db: sqlite3.Connection) -> None:
    """
    create_all_tables() must create all expected indexes on frequently queried columns.

    Queries sqlite_master for index objects and asserts each expected index name is present.
    """
    cursor = fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
    )
    existing_indexes = {row[0] for row in cursor.fetchall()}

    for index_name in EXPECTED_INDEXES:
        assert index_name in existing_indexes, f"Index '{index_name}' was not created"


def test_create_tables_is_idempotent(tmp_path: pytest.TempPathFactory) -> None:
    """
    Calling create_all_tables() twice on the same database must not raise errors.

    After both calls all expected tables must still exist with the correct structure.
    """
    db_path = str(tmp_path / "test_signals.db")
    conn = get_connection(db_path)

    create_all_tables(conn)
    create_all_tables(conn)  # must not raise

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    existing_tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    for table_name in ALL_TABLES:
        assert table_name in existing_tables, f"Table '{table_name}' missing after second call"


# ── Unique Constraint Tests ────────────────────────────────────────────────────

def test_unique_constraint_ohlcv(fresh_db: sqlite3.Connection) -> None:
    """
    ohlcv_daily has a UNIQUE(ticker, date) constraint.

    INSERT OR REPLACE with the same (ticker, date) must overwrite the row so only
    one row exists and it carries the updated close price.
    """
    fresh_db.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, vwap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", 170.0, 175.0, 169.0, 172.0, 50_000_000, 171.5),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO ohlcv_daily "
        "(ticker, date, open, high, low, close, volume, vwap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", 170.0, 175.0, 169.0, 174.5, 50_000_000, 171.5),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == 174.5


def test_unique_constraint_indicators(fresh_db: sqlite3.Connection) -> None:
    """
    indicators_daily has a UNIQUE(ticker, date) constraint.

    INSERT OR REPLACE with the same (ticker, date) must overwrite the row so only
    one row exists and it carries the updated rsi_14 value.
    """
    fresh_db.execute(
        "INSERT INTO indicators_daily (ticker, date, rsi_14, ema_9) VALUES (?, ?, ?, ?)",
        ("AAPL", "2026-03-16", 55.0, 170.0),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO indicators_daily (ticker, date, rsi_14, ema_9) "
        "VALUES (?, ?, ?, ?)",
        ("AAPL", "2026-03-16", 62.5, 171.0),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT rsi_14 FROM indicators_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == 62.5


def test_unique_constraint_scores(fresh_db: sqlite3.Connection) -> None:
    """
    scores_daily has a UNIQUE(ticker, date) constraint.

    INSERT OR REPLACE with the same (ticker, date) must overwrite the row so only
    one row exists and it carries the updated signal and confidence.
    """
    fresh_db.execute(
        "INSERT INTO scores_daily (ticker, date, signal, confidence, final_score) "
        "VALUES (?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", "BULLISH", 65.0, 45.0),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence, final_score) "
        "VALUES (?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", "BEARISH", 70.0, -40.0),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT signal, confidence FROM scores_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "BEARISH"
    assert rows[0][1] == 70.0


def test_unique_constraint_pipeline_events(fresh_db: sqlite3.Connection) -> None:
    """
    pipeline_events has a UNIQUE(event, date) constraint.

    INSERT OR REPLACE with the same (event, date) must overwrite the row so only
    one row exists and it carries the updated status.
    """
    fresh_db.execute(
        "INSERT INTO pipeline_events (event, date, status, timestamp) VALUES (?, ?, ?, ?)",
        ("fetcher_done", "2026-03-16", "ready", "2026-03-16T00:00:00Z"),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO pipeline_events (event, date, status, timestamp) "
        "VALUES (?, ?, ?, ?)",
        ("fetcher_done", "2026-03-16", "completed", "2026-03-16T01:00:00Z"),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT status FROM pipeline_events WHERE event = ? AND date = ?",
        ("fetcher_done", "2026-03-16"),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "completed"


# ── Insert / Query Round-trip Tests ───────────────────────────────────────────

def test_insert_and_query_ohlcv(fresh_db: sqlite3.Connection) -> None:
    """
    Insert 3 OHLCV rows for AAPL on three different dates.

    Queries them back and asserts every field (open, high, low, close, volume, vwap,
    num_transactions) matches the inserted values exactly.
    """
    rows_to_insert = [
        ("AAPL", "2026-03-14", 169.0, 171.5, 168.0, 170.5, 48_000_000.0, 170.1, 390_000),
        ("AAPL", "2026-03-15", 170.5, 173.0, 170.0, 172.0, 52_000_000.0, 171.5, 410_000),
        ("AAPL", "2026-03-16", 172.0, 174.5, 171.0, 173.5, 55_000_000.0, 172.8, 425_000),
    ]
    for row in rows_to_insert:
        fresh_db.execute(
            "INSERT INTO ohlcv_daily "
            "(ticker, date, open, high, low, close, volume, vwap, num_transactions) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    fresh_db.commit()

    results = fresh_db.execute(
        "SELECT ticker, date, open, high, low, close, volume, vwap, num_transactions "
        "FROM ohlcv_daily WHERE ticker = ? ORDER BY date",
        ("AAPL",),
    ).fetchall()

    assert len(results) == 3
    for result, expected in zip(results, rows_to_insert):
        assert result[0] == expected[0]  # ticker
        assert result[1] == expected[1]  # date
        assert result[2] == expected[2]  # open
        assert result[3] == expected[3]  # high
        assert result[4] == expected[4]  # low
        assert result[5] == expected[5]  # close
        assert result[6] == expected[6]  # volume
        assert result[7] == expected[7]  # vwap
        assert result[8] == expected[8]  # num_transactions


def test_insert_and_query_fundamentals(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a fundamentals row with revenue, eps, pe_ratio, debt_to_equity.

    Queries it back and asserts all four values match exactly.
    """
    fresh_db.execute(
        "INSERT INTO fundamentals "
        "(ticker, report_date, period, revenue, eps, pe_ratio, debt_to_equity) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-01-01", "Q1", 94_930_000_000.0, 2.18, 28.5, 1.73),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT ticker, report_date, period, revenue, eps, pe_ratio, debt_to_equity "
        "FROM fundamentals WHERE ticker = ? AND report_date = ?",
        ("AAPL", "2026-01-01"),
    ).fetchone()

    assert row is not None
    assert row[0] == "AAPL"
    assert row[1] == "2026-01-01"
    assert row[2] == "Q1"
    assert row[3] == 94_930_000_000.0
    assert row[4] == 2.18
    assert row[5] == 28.5
    assert row[6] == 1.73


def test_insert_and_query_news_article(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a news article row with sentiment data.

    Queries it back and asserts headline, sentiment, and sentiment_reasoning match exactly.
    """
    fresh_db.execute(
        "INSERT INTO news_articles "
        "(id, ticker, date, source, headline, summary, url, sentiment, "
        "sentiment_reasoning, published_utc, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "article-001",
            "AAPL",
            "2026-03-16",
            "polygon",
            "Apple Hits All-Time High",
            "AAPL shares reached a new all-time high on Monday.",
            "https://example.com/aapl-ath",
            "positive",
            "Record revenue and strong guidance indicate robust business performance.",
            "2026-03-16T14:30:00Z",
            "2026-03-16T15:00:00Z",
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT headline, sentiment, sentiment_reasoning "
        "FROM news_articles WHERE id = ?",
        ("article-001",),
    ).fetchone()

    assert row is not None
    assert row[0] == "Apple Hits All-Time High"
    assert row[1] == "positive"
    assert row[2] == "Record revenue and strong guidance indicate robust business performance."


def test_insert_and_query_indicators(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a full indicators_daily row with all 15 indicator columns populated.

    Queries it back and asserts every column value matches the inserted data exactly.
    """
    fresh_db.execute(
        "INSERT INTO indicators_daily "
        "(ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal, macd_histogram, "
        "adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line, "
        "bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AAPL", "2026-03-16",
            172.5, 171.0, 168.0,       # ema_9, ema_21, ema_50
            1.25, 0.95, 0.30,          # macd_line, macd_signal, macd_histogram
            28.5,                      # adx
            62.3,                      # rsi_14
            75.0, 68.0,                # stoch_k, stoch_d
            85.4,                      # cci_20
            -22.5,                     # williams_r
            1_250_000_000.0,           # obv
            0.15,                      # cmf_20
            950_000_000.0,             # ad_line
            175.0, 165.0, 0.72,        # bb_upper, bb_lower, bb_pctb
            2.30,                      # atr_14
            176.5, 167.5,              # keltner_upper, keltner_lower
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT ema_9, ema_21, ema_50, macd_line, macd_signal, macd_histogram, "
        "adx, rsi_14, stoch_k, stoch_d, cci_20, williams_r, obv, cmf_20, ad_line, "
        "bb_upper, bb_lower, bb_pctb, atr_14, keltner_upper, keltner_lower "
        "FROM indicators_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == 172.5            # ema_9
    assert row[1] == 171.0            # ema_21
    assert row[2] == 168.0            # ema_50
    assert row[3] == 1.25             # macd_line
    assert row[4] == 0.95             # macd_signal
    assert row[5] == 0.30             # macd_histogram
    assert row[6] == 28.5             # adx
    assert row[7] == 62.3             # rsi_14
    assert row[8] == 75.0             # stoch_k
    assert row[9] == 68.0             # stoch_d
    assert row[10] == 85.4            # cci_20
    assert row[11] == -22.5           # williams_r
    assert row[12] == 1_250_000_000.0 # obv
    assert row[13] == 0.15            # cmf_20
    assert row[14] == 950_000_000.0   # ad_line
    assert row[15] == 175.0           # bb_upper
    assert row[16] == 165.0           # bb_lower
    assert row[17] == 0.72            # bb_pctb
    assert row[18] == 2.30            # atr_14
    assert row[19] == 176.5           # keltner_upper
    assert row[20] == 167.5           # keltner_lower


def test_insert_and_query_scores(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a scores_daily row with signal=BULLISH, confidence=72.5, final_score=58.4,
    and all nine category scores.

    Queries it back and asserts every field matches the inserted values.
    """
    fresh_db.execute(
        "INSERT INTO scores_daily "
        "(ticker, date, signal, confidence, final_score, regime, daily_score, weekly_score, "
        "trend_score, momentum_score, volume_score, volatility_score, candlestick_score, "
        "structural_score, sentiment_score, fundamental_score, macro_score, "
        "data_completeness, key_signals) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AAPL", "2026-03-16",
            "BULLISH", 72.5, 58.4,
            "trending", 62.0, 52.0,
            35.0, 25.0, 18.0, 8.0, 12.0, 20.0,
            15.0, 10.0, 5.0,
            '{"ohlcv": true, "indicators": true}',
            '["EMA crossover", "RSI above 60"]',
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT signal, confidence, final_score, trend_score, momentum_score, volume_score, "
        "volatility_score, candlestick_score, structural_score, sentiment_score, "
        "fundamental_score, macro_score "
        "FROM scores_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "BULLISH"
    assert row[1] == 72.5
    assert row[2] == 58.4
    assert row[3] == 35.0   # trend_score
    assert row[4] == 25.0   # momentum_score
    assert row[5] == 18.0   # volume_score
    assert row[6] == 8.0    # volatility_score
    assert row[7] == 12.0   # candlestick_score
    assert row[8] == 20.0   # structural_score
    assert row[9] == 15.0   # sentiment_score
    assert row[10] == 10.0  # fundamental_score
    assert row[11] == 5.0   # macro_score


def test_insert_and_query_patterns(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a patterns_daily row for a bullish_engulfing candlestick reversal pattern.

    Queries it back and asserts pattern_name, pattern_category, pattern_type, direction,
    and strength all match the inserted values.
    """
    fresh_db.execute(
        "INSERT INTO patterns_daily "
        "(ticker, date, pattern_name, pattern_category, pattern_type, direction, "
        "strength, confirmed) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", "bullish_engulfing", "candlestick", "reversal", "bullish", 3, 1),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT pattern_name, pattern_category, pattern_type, direction, strength "
        "FROM patterns_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "bullish_engulfing"
    assert row[1] == "candlestick"
    assert row[2] == "reversal"
    assert row[3] == "bullish"
    assert row[4] == 3


def test_insert_and_query_divergences(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a divergences_daily row for an RSI regular bullish divergence.

    Queries it back and asserts all fields (indicator, divergence_type, both price swings,
    both indicator swing values, and strength) match the inserted data.
    """
    fresh_db.execute(
        "INSERT INTO divergences_daily "
        "(ticker, date, indicator, divergence_type, "
        "price_swing_1_date, price_swing_1_value, "
        "price_swing_2_date, price_swing_2_value, "
        "indicator_swing_1_value, indicator_swing_2_value, strength) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AAPL", "2026-03-16",
            "rsi", "regular_bullish",
            "2026-03-01", 165.0,
            "2026-03-16", 162.0,
            45.0, 52.0,
            3,
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT indicator, divergence_type, price_swing_1_date, price_swing_1_value, "
        "price_swing_2_date, price_swing_2_value, indicator_swing_1_value, "
        "indicator_swing_2_value, strength "
        "FROM divergences_daily WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "rsi"
    assert row[1] == "regular_bullish"
    assert row[2] == "2026-03-01"
    assert row[3] == 165.0
    assert row[4] == "2026-03-16"
    assert row[5] == 162.0
    assert row[6] == 45.0
    assert row[7] == 52.0
    assert row[8] == 3


def test_insert_and_query_signal_flips(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a signal_flips row recording a NEUTRAL → BULLISH transition.

    Queries it back and asserts previous_signal, new_signal, previous_confidence,
    and new_confidence all match.
    """
    fresh_db.execute(
        "INSERT INTO signal_flips "
        "(ticker, date, previous_signal, new_signal, previous_confidence, new_confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", "NEUTRAL", "BULLISH", 28.0, 72.5),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT previous_signal, new_signal, previous_confidence, new_confidence "
        "FROM signal_flips WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "NEUTRAL"
    assert row[1] == "BULLISH"
    assert row[2] == 28.0
    assert row[3] == 72.5


def test_insert_pipeline_event(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a pipeline_events row for the fetcher_done event on 2026-03-16.

    Queries it back and asserts event, date, status, timestamp, and details all match.
    """
    fresh_db.execute(
        "INSERT INTO pipeline_events (event, date, status, timestamp, details) "
        "VALUES (?, ?, ?, ?, ?)",
        ("fetcher_done", "2026-03-16", "completed", "2026-03-16T01:30:00Z", '{"tickers": 50}'),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT event, date, status, timestamp, details "
        "FROM pipeline_events WHERE event = ? AND date = ?",
        ("fetcher_done", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "fetcher_done"
    assert row[1] == "2026-03-16"
    assert row[2] == "completed"
    assert row[3] == "2026-03-16T01:30:00Z"
    assert row[4] == '{"tickers": 50}'


def test_insert_pipeline_run(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a pipeline_runs row with timing and ticker counts.

    Queries it back and asserts date, phase, duration_seconds, all three ticker counts,
    api_calls_made, status, and error_summary match.
    """
    fresh_db.execute(
        "INSERT INTO pipeline_runs "
        "(date, phase, started_at, completed_at, duration_seconds, "
        "tickers_processed, tickers_skipped, tickers_failed, "
        "api_calls_made, status, error_summary) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "2026-03-16", "fetcher",
            "2026-03-16T00:00:00Z", "2026-03-16T01:30:00Z",
            5400.0,
            48, 1, 1,
            250,
            "partial",
            "TSLA: API timeout after 3 retries",
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT date, phase, duration_seconds, tickers_processed, tickers_skipped, "
        "tickers_failed, api_calls_made, status, error_summary "
        "FROM pipeline_runs WHERE date = ? AND phase = ?",
        ("2026-03-16", "fetcher"),
    ).fetchone()

    assert row is not None
    assert row[0] == "2026-03-16"
    assert row[1] == "fetcher"
    assert row[2] == 5400.0
    assert row[3] == 48
    assert row[4] == 1
    assert row[5] == 1
    assert row[6] == 250
    assert row[7] == "partial"
    assert row[8] == "TSLA: API timeout after 3 retries"


def test_insert_alert_log(fresh_db: sqlite3.Connection) -> None:
    """
    Insert an alerts_log row for an AAPL fetcher API timeout error.

    Queries it back and asserts ticker, date, phase, severity, and message all match.
    """
    fresh_db.execute(
        "INSERT INTO alerts_log (ticker, date, phase, severity, message, notified, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("AAPL", "2026-03-16", "fetcher", "error", "API timeout", 0, "2026-03-16T00:30:00Z"),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT ticker, date, phase, severity, message "
        "FROM alerts_log WHERE ticker = ? AND date = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == "AAPL"
    assert row[1] == "2026-03-16"
    assert row[2] == "fetcher"
    assert row[3] == "error"
    assert row[4] == "API timeout"


# ── Connection Tests ───────────────────────────────────────────────────────────

# ── Timeframe Parity Tables (weekly + monthly) ────────────────────────────────

PARITY_NEW_TABLES = [
    "swing_points_weekly",
    "swing_points_monthly",
    "support_resistance_weekly",
    "support_resistance_monthly",
    "patterns_weekly",
    "patterns_monthly",
    "divergences_weekly",
    "divergences_monthly",
    "crossovers_weekly",
    "crossovers_monthly",
    "indicator_profiles_weekly",
    "indicator_profiles_monthly",
    "scores_weekly",
    "scores_monthly",
]


def test_parity_tables_created(fresh_db: sqlite3.Connection) -> None:
    """
    Every weekly/monthly parity mirror table must exist after create_all_tables().

    These mirror their daily counterparts and replace `date` with `week_start`
    or `month_start` as appropriate.
    """
    cursor = fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    existing = {row[0] for row in cursor.fetchall()}
    for table_name in PARITY_NEW_TABLES:
        assert table_name in existing, f"Parity table '{table_name}' was not created"


def test_swing_points_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """swing_points_weekly mirrors swing_points but with week_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(swing_points_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    assert columns == {"ticker", "week_start", "type", "price", "strength"}


def test_swing_points_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """swing_points_monthly mirrors swing_points but with month_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(swing_points_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    assert columns == {"ticker", "month_start", "type", "price", "strength"}


def test_support_resistance_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """support_resistance_weekly mirrors support_resistance with week_start."""
    cursor = fresh_db.execute("PRAGMA table_info(support_resistance_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "week_start", "level_price", "level_type",
        "touch_count", "first_touch", "last_touch", "strength",
        "broken", "broken_date",
    }
    assert columns == expected


def test_support_resistance_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """support_resistance_monthly mirrors support_resistance with month_start."""
    cursor = fresh_db.execute("PRAGMA table_info(support_resistance_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "month_start", "level_price", "level_type",
        "touch_count", "first_touch", "last_touch", "strength",
        "broken", "broken_date",
    }
    assert columns == expected


def test_patterns_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """patterns_weekly mirrors patterns_daily with week_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(patterns_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "week_start", "pattern_name", "pattern_category",
        "pattern_type", "direction", "strength", "confirmed", "details",
    }
    assert columns == expected


def test_patterns_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """patterns_monthly mirrors patterns_daily with month_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(patterns_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "month_start", "pattern_name", "pattern_category",
        "pattern_type", "direction", "strength", "confirmed", "details",
    }
    assert columns == expected


def test_divergences_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """divergences_weekly mirrors divergences_daily with week_start columns."""
    cursor = fresh_db.execute("PRAGMA table_info(divergences_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "week_start", "indicator", "divergence_type",
        "price_swing_1_date", "price_swing_1_value",
        "price_swing_2_date", "price_swing_2_value",
        "indicator_swing_1_value", "indicator_swing_2_value", "strength",
    }
    assert columns == expected


def test_divergences_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """divergences_monthly mirrors divergences_daily with month_start columns."""
    cursor = fresh_db.execute("PRAGMA table_info(divergences_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "id", "ticker", "month_start", "indicator", "divergence_type",
        "price_swing_1_date", "price_swing_1_value",
        "price_swing_2_date", "price_swing_2_value",
        "indicator_swing_1_value", "indicator_swing_2_value", "strength",
    }
    assert columns == expected


def test_crossovers_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """crossovers_weekly mirrors crossovers_daily with week_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(crossovers_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {"id", "ticker", "week_start", "crossover_type", "direction", "days_ago"}
    assert columns == expected


def test_crossovers_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """crossovers_monthly mirrors crossovers_daily with month_start instead of date."""
    cursor = fresh_db.execute("PRAGMA table_info(crossovers_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {"id", "ticker", "month_start", "crossover_type", "direction", "days_ago"}
    assert columns == expected


def test_indicator_profiles_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """indicator_profiles_weekly mirrors indicator_profiles."""
    cursor = fresh_db.execute("PRAGMA table_info(indicator_profiles_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "ticker", "indicator", "p5", "p20", "p50", "p80", "p95",
        "mean", "std", "window_start", "window_end", "computed_at",
    }
    assert columns == expected


def test_indicator_profiles_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """indicator_profiles_monthly mirrors indicator_profiles."""
    cursor = fresh_db.execute("PRAGMA table_info(indicator_profiles_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "ticker", "indicator", "p5", "p20", "p50", "p80", "p95",
        "mean", "std", "window_start", "window_end", "computed_at",
    }
    assert columns == expected


def test_scores_weekly_columns(fresh_db: sqlite3.Connection) -> None:
    """scores_weekly contains composite, regime, 8 category scores, key_signals, and key_signals_data."""
    cursor = fresh_db.execute("PRAGMA table_info(scores_weekly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "ticker", "week_start", "composite_score", "regime",
        "trend_score", "momentum_score", "volume_score", "volatility_score",
        "candlestick_score", "structural_score", "fundamental_score", "macro_score",
        "data_completeness", "key_signals", "key_signals_data",
    }
    assert columns == expected


def test_scores_monthly_columns(fresh_db: sqlite3.Connection) -> None:
    """scores_monthly contains composite, regime, 8 category scores, and key_signals."""
    cursor = fresh_db.execute("PRAGMA table_info(scores_monthly)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "ticker", "month_start", "composite_score", "regime",
        "trend_score", "momentum_score", "volume_score", "volatility_score",
        "candlestick_score", "structural_score", "fundamental_score", "macro_score",
        "data_completeness", "key_signals",
    }
    assert columns == expected


def test_scores_weekly_data_completeness_is_text(fresh_db: sqlite3.Connection) -> None:
    """
    scores_weekly.data_completeness must be TEXT (not REAL).

    Daily stores ``json.dumps(data_completeness)`` (a JSON object string), and the
    weekly/monthly tables receive the same shape via persist_weekly_score_row /
    persist_monthly_score_row. A REAL column would silently fail / coerce that
    string to NULL.
    """
    cursor = fresh_db.execute("PRAGMA table_info(scores_weekly)")
    types_by_name = {row[1]: row[2].upper() for row in cursor.fetchall()}
    assert types_by_name["data_completeness"] == "TEXT"


def test_scores_monthly_data_completeness_is_text(fresh_db: sqlite3.Connection) -> None:
    """scores_monthly.data_completeness must be TEXT (parity with weekly + daily)."""
    cursor = fresh_db.execute("PRAGMA table_info(scores_monthly)")
    types_by_name = {row[1]: row[2].upper() for row in cursor.fetchall()}
    assert types_by_name["data_completeness"] == "TEXT"


def test_scores_weekly_pk_enforcement_plain_insert(fresh_db: sqlite3.Connection) -> None:
    """
    Inserting two rows with the same (ticker, week_start) into scores_weekly via
    plain INSERT must raise IntegrityError because of the composite PK.
    """
    fresh_db.execute(
        "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-16", 42.0),
    )
    fresh_db.commit()

    with pytest.raises(sqlite3.IntegrityError):
        fresh_db.execute(
            "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
            "VALUES (?, ?, ?)",
            ("AAPL", "2026-03-16", 50.0),
        )


def test_scores_weekly_pk_enforcement_replace(fresh_db: sqlite3.Connection) -> None:
    """
    INSERT OR REPLACE on scores_weekly with a duplicate (ticker, week_start) overwrites
    the existing row so only one remains and its composite_score reflects the new value.
    """
    fresh_db.execute(
        "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-16", 42.0),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO scores_weekly (ticker, week_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-16", 80.0),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT composite_score FROM scores_weekly WHERE ticker = ? AND week_start = ?",
        ("AAPL", "2026-03-16"),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 80.0


def test_scores_monthly_pk_enforcement_plain_insert(fresh_db: sqlite3.Connection) -> None:
    """
    Inserting two rows with the same (ticker, month_start) into scores_monthly via
    plain INSERT must raise IntegrityError because of the composite PK.
    """
    fresh_db.execute(
        "INSERT INTO scores_monthly (ticker, month_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-01", 30.0),
    )
    fresh_db.commit()

    with pytest.raises(sqlite3.IntegrityError):
        fresh_db.execute(
            "INSERT INTO scores_monthly (ticker, month_start, composite_score) "
            "VALUES (?, ?, ?)",
            ("AAPL", "2026-03-01", 35.0),
        )


def test_scores_monthly_pk_enforcement_replace(fresh_db: sqlite3.Connection) -> None:
    """
    INSERT OR REPLACE on scores_monthly with a duplicate (ticker, month_start)
    overwrites the existing row.
    """
    fresh_db.execute(
        "INSERT INTO scores_monthly (ticker, month_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-01", 30.0),
    )
    fresh_db.commit()

    fresh_db.execute(
        "INSERT OR REPLACE INTO scores_monthly (ticker, month_start, composite_score) "
        "VALUES (?, ?, ?)",
        ("AAPL", "2026-03-01", 90.0),
    )
    fresh_db.commit()

    rows = fresh_db.execute(
        "SELECT composite_score FROM scores_monthly WHERE ticker = ? AND month_start = ?",
        ("AAPL", "2026-03-01"),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 90.0


def test_insert_and_query_scores_weekly(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a scores_weekly row with all fields populated and query it back.

    Asserts every category score, regime, composite_score, data_completeness, and
    key_signals match the inserted values.
    """
    fresh_db.execute(
        "INSERT INTO scores_weekly "
        "(ticker, week_start, composite_score, regime, "
        "trend_score, momentum_score, volume_score, volatility_score, "
        "candlestick_score, structural_score, fundamental_score, macro_score, "
        "data_completeness, key_signals) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AAPL", "2026-03-16",
            58.4, "trending",
            35.0, 25.0, 18.0, 8.0,
            12.0, 20.0, 10.0, 5.0,
            '{"news": true, "fundamentals": true}',
            '["EMA crossover", "RSI above 60"]',
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT composite_score, regime, trend_score, momentum_score, volume_score, "
        "volatility_score, candlestick_score, structural_score, "
        "fundamental_score, macro_score, data_completeness, key_signals "
        "FROM scores_weekly WHERE ticker = ? AND week_start = ?",
        ("AAPL", "2026-03-16"),
    ).fetchone()

    assert row is not None
    assert row[0] == 58.4
    assert row[1] == "trending"
    assert row[2] == 35.0
    assert row[3] == 25.0
    assert row[4] == 18.0
    assert row[5] == 8.0
    assert row[6] == 12.0
    assert row[7] == 20.0
    assert row[8] == 10.0
    assert row[9] == 5.0
    assert row[10] == '{"news": true, "fundamentals": true}'
    assert row[11] == '["EMA crossover", "RSI above 60"]'


def test_insert_and_query_scores_monthly(fresh_db: sqlite3.Connection) -> None:
    """
    Insert a scores_monthly row, query it back, and verify all values match.
    """
    fresh_db.execute(
        "INSERT INTO scores_monthly "
        "(ticker, month_start, composite_score, regime, "
        "trend_score, momentum_score, volume_score, volatility_score, "
        "candlestick_score, structural_score, fundamental_score, macro_score, "
        "data_completeness, key_signals) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AAPL", "2026-03-01",
            72.0, "trending",
            40.0, 30.0, 15.0, 10.0,
            8.0, 18.0, 12.0, 4.0,
            '{"news": false, "fundamentals": true}',
            '["monthly MACD bullish"]',
        ),
    )
    fresh_db.commit()

    row = fresh_db.execute(
        "SELECT composite_score, regime, trend_score, momentum_score, volume_score, "
        "volatility_score, candlestick_score, structural_score, "
        "fundamental_score, macro_score, data_completeness, key_signals "
        "FROM scores_monthly WHERE ticker = ? AND month_start = ?",
        ("AAPL", "2026-03-01"),
    ).fetchone()

    assert row is not None
    assert row[0] == 72.0
    assert row[1] == "trending"
    assert row[2] == 40.0
    assert row[3] == 30.0
    assert row[4] == 15.0
    assert row[5] == 10.0
    assert row[6] == 8.0
    assert row[7] == 18.0
    assert row[8] == 12.0
    assert row[9] == 4.0
    assert row[10] == '{"news": false, "fundamentals": true}'
    assert row[11] == '["monthly MACD bullish"]'


def test_scores_weekly_composite_score_not_null(fresh_db: sqlite3.Connection) -> None:
    """scores_weekly.composite_score is NOT NULL — inserting NULL must raise."""
    with pytest.raises(sqlite3.IntegrityError):
        fresh_db.execute(
            "INSERT INTO scores_weekly (ticker, week_start, composite_score) "
            "VALUES (?, ?, ?)",
            ("AAPL", "2026-03-16", None),
        )


def test_scores_monthly_composite_score_not_null(fresh_db: sqlite3.Connection) -> None:
    """scores_monthly.composite_score is NOT NULL — inserting NULL must raise."""
    with pytest.raises(sqlite3.IntegrityError):
        fresh_db.execute(
            "INSERT INTO scores_monthly (ticker, month_start, composite_score) "
            "VALUES (?, ?, ?)",
            ("AAPL", "2026-03-01", None),
        )


# ── Indicator scores sidecar table tests ──────────────────────────────────────

def _get_column_names(conn: sqlite3.Connection, table: str) -> list:
    """Return column names for the given table using PRAGMA table_info."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def _get_pk_columns(conn: sqlite3.Connection, table: str) -> list:
    """Return the PRIMARY KEY column names for the given table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows if row[5] > 0]


def test_indicator_scores_daily_columns_and_pk(fresh_db: sqlite3.Connection) -> None:
    """
    indicator_scores_daily must have ticker, date, indicator_name, score columns
    with PRIMARY KEY (ticker, date, indicator_name).
    """
    cols = _get_column_names(fresh_db, "indicator_scores_daily")
    assert "ticker" in cols
    assert "date" in cols
    assert "indicator_name" in cols
    assert "score" in cols

    pk_cols = _get_pk_columns(fresh_db, "indicator_scores_daily")
    assert set(pk_cols) == {"ticker", "date", "indicator_name"}, (
        f"Expected PK (ticker, date, indicator_name), got {pk_cols}"
    )


def test_indicator_scores_weekly_columns_and_pk(fresh_db: sqlite3.Connection) -> None:
    """
    indicator_scores_weekly must have ticker, week_start, indicator_name, score
    with PRIMARY KEY (ticker, week_start, indicator_name).
    """
    cols = _get_column_names(fresh_db, "indicator_scores_weekly")
    assert "ticker" in cols
    assert "week_start" in cols
    assert "indicator_name" in cols
    assert "score" in cols

    pk_cols = _get_pk_columns(fresh_db, "indicator_scores_weekly")
    assert set(pk_cols) == {"ticker", "week_start", "indicator_name"}, (
        f"Expected PK (ticker, week_start, indicator_name), got {pk_cols}"
    )


def test_indicator_scores_monthly_columns_and_pk(fresh_db: sqlite3.Connection) -> None:
    """
    indicator_scores_monthly must have ticker, month_start, indicator_name, score
    with PRIMARY KEY (ticker, month_start, indicator_name).
    """
    cols = _get_column_names(fresh_db, "indicator_scores_monthly")
    assert "ticker" in cols
    assert "month_start" in cols
    assert "indicator_name" in cols
    assert "score" in cols

    pk_cols = _get_pk_columns(fresh_db, "indicator_scores_monthly")
    assert set(pk_cols) == {"ticker", "month_start", "indicator_name"}, (
        f"Expected PK (ticker, month_start, indicator_name), got {pk_cols}"
    )


# ── Connection Tests ───────────────────────────────────────────────────────────

def test_connection_context_manager(tmp_path: pytest.TempPathFactory) -> None:
    """
    get_connection() must work as a context manager.

    Uses the 'with get_connection(path) as conn:' pattern and verifies the connection
    is usable (can execute a query) inside the block.
    """
    db_path = str(tmp_path / "test_signals.db")
    with get_connection(db_path) as conn:
        assert conn is not None
        conn.execute("CREATE TABLE IF NOT EXISTS ctx_test (val INTEGER)")
        row = conn.execute("SELECT 42").fetchone()
        assert row[0] == 42
