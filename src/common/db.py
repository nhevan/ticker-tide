"""
Database layer for the Stock Signal Engine.

Provides get_connection() for opening SQLite connections with WAL mode enabled,
and create_all_tables() for initialising the full schema defined in DESIGN.md section 4.
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Open a SQLite connection to the given path with project-standard settings applied.

    Enables WAL journal mode for better concurrent read performance, turns on foreign-key
    enforcement, and sets row_factory to sqlite3.Row so rows can be accessed by column name.
    The returned connection supports the context manager protocol — 'with conn:' commits on
    success and rolls back on exception, but does NOT close the connection automatically.

    Parameters:
        db_path: Absolute or relative path to the SQLite database file.
                 The file is created if it does not already exist.

    Returns:
        An open sqlite3.Connection configured with WAL mode, foreign keys, and
        dict-like row access via sqlite3.Row.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    logger.info(f"Opened SQLite connection to {db_path!r} (WAL mode enabled)")
    return conn


def create_all_tables(connection: sqlite3.Connection) -> None:
    """
    Create all tables and indexes defined in DESIGN.md section 4.

    Uses CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS throughout, so this
    function is safe to call multiple times on the same database — it is fully idempotent.
    Creates UNIQUE constraints as specified in the schema and indexes on (ticker, date) for
    every table that is queried frequently by both ticker and date.

    Parameters:
        connection: An open sqlite3.Connection to the target database.

    Returns:
        None
    """
    for statement in _build_schema_statements():
        connection.execute(statement)
    connection.commit()
    logger.info("All schema tables and indexes created (or already existed)")


def _build_schema_statements() -> list[str]:
    """
    Return the ordered list of CREATE TABLE and CREATE INDEX SQL statements for the full schema.

    Tables are created before the indexes that reference them. Every statement uses
    IF NOT EXISTS so re-running is safe.

    Returns:
        A list of SQL strings ready to be executed sequentially against a sqlite3.Connection.
    """
    return [
        # ── Core Tables ────────────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS tickers (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            sector TEXT,
            sector_etf TEXT,
            sic_code TEXT,
            sic_description TEXT,
            market_cap REAL,
            active BOOLEAN DEFAULT 1,
            added_date TEXT,
            updated_at TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS ohlcv_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            vwap REAL,
            num_transactions INTEGER,
            UNIQUE(ticker, date)
        )""",

        # ── Fundamental Tables ─────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS fundamentals (
            ticker TEXT NOT NULL,
            report_date TEXT NOT NULL,
            period TEXT,
            revenue REAL,
            revenue_growth_yoy REAL,
            net_income REAL,
            eps REAL,
            eps_growth_yoy REAL,
            pe_ratio REAL,
            pb_ratio REAL,
            ps_ratio REAL,
            debt_to_equity REAL,
            return_on_assets REAL,
            return_on_equity REAL,
            free_cash_flow REAL,
            market_cap REAL,
            dividend_yield REAL,
            fetched_at TEXT,
            UNIQUE(ticker, report_date, period)
        )""",

        """CREATE TABLE IF NOT EXISTS earnings_calendar (
            ticker TEXT NOT NULL,
            earnings_date TEXT NOT NULL,
            fiscal_quarter TEXT,
            fiscal_year INTEGER,
            estimated_eps REAL,
            actual_eps REAL,
            eps_surprise REAL,
            revenue_estimated REAL,
            revenue_actual REAL,
            fetched_at TEXT,
            UNIQUE(ticker, earnings_date)
        )""",

        # ── News & Filings Tables ──────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS news_articles (
            id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            source TEXT,
            headline TEXT,
            summary TEXT,
            url TEXT,
            sentiment TEXT,
            sentiment_reasoning TEXT,
            published_utc TEXT,
            fetched_at TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS news_daily_summary (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            avg_sentiment_score REAL,
            article_count INTEGER,
            positive_count INTEGER,
            negative_count INTEGER,
            neutral_count INTEGER,
            top_headline TEXT,
            filing_flag BOOLEAN DEFAULT 0,
            UNIQUE(ticker, date)
        )""",

        """CREATE TABLE IF NOT EXISTS filings_8k (
            accession_number TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            filing_date TEXT NOT NULL,
            form_type TEXT,
            items_text TEXT,
            filing_url TEXT,
            fetched_at TEXT
        )""",

        # ── Corporate Actions Tables ───────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS dividends (
            id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            ex_dividend_date TEXT,
            pay_date TEXT,
            cash_amount REAL,
            frequency INTEGER,
            fetched_at TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS splits (
            id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            execution_date TEXT,
            split_from REAL,
            split_to REAL,
            fetched_at TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS short_interest (
            ticker TEXT NOT NULL,
            settlement_date TEXT NOT NULL,
            short_interest INTEGER,
            avg_daily_volume INTEGER,
            days_to_cover REAL,
            fetched_at TEXT,
            UNIQUE(ticker, settlement_date)
        )""",

        # ── Macro Tables ───────────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS treasury_yields (
            date TEXT PRIMARY KEY,
            yield_1_month REAL,
            yield_3_month REAL,
            yield_6_month REAL,
            yield_1_year REAL,
            yield_2_year REAL,
            yield_3_year REAL,
            yield_5_year REAL,
            yield_7_year REAL,
            yield_10_year REAL,
            yield_20_year REAL,
            yield_30_year REAL
        )""",

        # ── Indicator Tables ───────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS indicators_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            ema_9 REAL,
            ema_21 REAL,
            ema_50 REAL,
            macd_line REAL,
            macd_signal REAL,
            macd_histogram REAL,
            adx REAL,
            rsi_14 REAL,
            stoch_k REAL,
            stoch_d REAL,
            cci_20 REAL,
            williams_r REAL,
            obv REAL,
            cmf_20 REAL,
            ad_line REAL,
            bb_upper REAL,
            bb_lower REAL,
            bb_pctb REAL,
            atr_14 REAL,
            keltner_upper REAL,
            keltner_lower REAL,
            UNIQUE(ticker, date)
        )""",

        """CREATE TABLE IF NOT EXISTS indicator_profiles (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL,
            p20 REAL,
            p50 REAL,
            p80 REAL,
            p95 REAL,
            mean REAL,
            std REAL,
            window_start TEXT,
            window_end TEXT,
            computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",

        """CREATE TABLE IF NOT EXISTS weekly_candles (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(ticker, week_start)
        )""",

        """CREATE TABLE IF NOT EXISTS indicators_weekly (
            ticker TEXT NOT NULL,
            week_start TEXT NOT NULL,
            ema_9 REAL,
            ema_21 REAL,
            ema_50 REAL,
            macd_line REAL,
            macd_signal REAL,
            macd_histogram REAL,
            adx REAL,
            rsi_14 REAL,
            stoch_k REAL,
            stoch_d REAL,
            cci_20 REAL,
            williams_r REAL,
            obv REAL,
            cmf_20 REAL,
            ad_line REAL,
            bb_upper REAL,
            bb_lower REAL,
            bb_pctb REAL,
            atr_14 REAL,
            keltner_upper REAL,
            keltner_lower REAL,
            UNIQUE(ticker, week_start)
        )""",

        # ── Pattern & Signal Tables ────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS swing_points (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            type TEXT,
            price REAL,
            strength INTEGER,
            UNIQUE(ticker, date, type)
        )""",

        """CREATE TABLE IF NOT EXISTS support_resistance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date_computed TEXT NOT NULL,
            level_price REAL,
            level_type TEXT,
            touch_count INTEGER,
            first_touch TEXT,
            last_touch TEXT,
            strength TEXT,
            broken BOOLEAN DEFAULT 0,
            broken_date TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS patterns_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            pattern_name TEXT,
            pattern_category TEXT,
            pattern_type TEXT,
            direction TEXT,
            strength INTEGER,
            confirmed BOOLEAN DEFAULT 0,
            details TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS divergences_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            indicator TEXT,
            divergence_type TEXT,
            price_swing_1_date TEXT,
            price_swing_1_value REAL,
            price_swing_2_date TEXT,
            price_swing_2_value REAL,
            indicator_swing_1_value REAL,
            indicator_swing_2_value REAL,
            strength INTEGER
        )""",

        """CREATE TABLE IF NOT EXISTS crossovers_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            crossover_type TEXT,
            direction TEXT,
            days_ago INTEGER
        )""",

        """CREATE TABLE IF NOT EXISTS gaps_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            gap_type TEXT,
            direction TEXT,
            gap_size_pct REAL,
            volume_ratio REAL,
            filled BOOLEAN DEFAULT 0
        )""",

        # ── Scoring Tables ─────────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            regime TEXT,
            daily_score REAL,
            weekly_score REAL,
            trend_score REAL,
            momentum_score REAL,
            volume_score REAL,
            volatility_score REAL,
            candlestick_score REAL,
            structural_score REAL,
            sentiment_score REAL,
            fundamental_score REAL,
            macro_score REAL,
            data_completeness TEXT,
            key_signals TEXT,
            UNIQUE(ticker, date)
        )""",

        """CREATE TABLE IF NOT EXISTS signal_flips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            previous_signal TEXT,
            new_signal TEXT,
            previous_confidence REAL,
            new_confidence REAL
        )""",

        # ── Pipeline Tables ────────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS pipeline_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            date TEXT NOT NULL,
            status TEXT,
            timestamp TEXT NOT NULL,
            details TEXT,
            UNIQUE(event, date)
        )""",

        """CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            phase TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            duration_seconds REAL,
            tickers_processed INTEGER,
            tickers_skipped INTEGER,
            tickers_failed INTEGER,
            api_calls_made INTEGER,
            status TEXT,
            error_summary TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS alerts_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            date TEXT,
            phase TEXT,
            severity TEXT,
            message TEXT,
            notified BOOLEAN DEFAULT 0,
            created_at TEXT
        )""",

        # ── Telegram Tables ────────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )""",

        # ── Indexes ────────────────────────────────────────────────────────────────
        "CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON ohlcv_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_indicators_ticker_date ON indicators_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_scores_ticker_date ON scores_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_news_ticker_date ON news_articles(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_patterns_ticker_date ON patterns_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_divergences_ticker_date ON divergences_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_crossovers_ticker_date ON crossovers_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_gaps_ticker_date ON gaps_daily(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_swing_points_ticker_date ON swing_points(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_weekly_candles_ticker ON weekly_candles(ticker, week_start)",
        "CREATE INDEX IF NOT EXISTS idx_indicators_weekly_ticker ON indicators_weekly(ticker, week_start)",
        "CREATE INDEX IF NOT EXISTS idx_news_summary_ticker_date ON news_daily_summary(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_short_interest_ticker ON short_interest(ticker, settlement_date)",
        "CREATE INDEX IF NOT EXISTS idx_pipeline_events ON pipeline_events(event, date)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_log_date ON alerts_log(date)",
        "CREATE INDEX IF NOT EXISTS idx_telegram_message_log_received_at ON telegram_message_log(received_at)",
    ]
