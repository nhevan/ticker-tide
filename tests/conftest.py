"""
Shared pytest fixtures for the Stock Signal Engine test suite.

Provides reusable test data and database infrastructure used across all test modules.
"""

import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Generator

import pandas as pd
import pytest


@pytest.fixture
def sample_ohlcv_dataframe() -> pd.DataFrame:
    """
    Generate 30 days of realistic OHLCV data for AAPL.

    Returns a DataFrame with columns: date, open, high, low, close, volume, vwap, num_transactions.
    Prices start around $170 and drift slightly upward with realistic intraday ranges.
    """
    base_date = date(2025, 1, 2)
    rows = []
    close_price = 170.00

    for day_index in range(30):
        current_date = base_date + timedelta(days=day_index)
        # Skip weekends
        if current_date.weekday() >= 5:
            continue

        open_price = close_price * (1 + (day_index % 3 - 1) * 0.002)
        high_price = open_price * 1.015
        low_price = open_price * 0.985
        close_price = open_price * (1 + (day_index % 5 - 2) * 0.003)
        volume = 50_000_000 + (day_index * 1_000_000)
        vwap = (open_price + high_price + low_price + close_price) / 4
        num_transactions = 400_000 + (day_index * 5_000)

        rows.append({
            "date": current_date.isoformat(),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
            "vwap": round(vwap, 4),
            "num_transactions": num_transactions,
        })

    return pd.DataFrame(rows)


@pytest.fixture
def sample_ticker_config() -> dict:
    """
    Return a single ticker configuration dict matching the tickers.json format.

    Returns a dict with keys: symbol, sector, sector_etf, added, active.
    """
    return {
        "symbol": "AAPL",
        "sector": "Technology",
        "sector_etf": "XLK",
        "added": "2026-03-16",
        "active": 1,
    }


@pytest.fixture
def sample_tickers_list() -> list[dict]:
    """
    Return a list of 3 ticker configuration dicts matching the tickers.json format.

    Returns a list of dicts each with keys: symbol, sector, sector_etf, added, active.
    """
    return [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-03-16", "active": 1},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-03-16", "active": 1},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF", "added": "2026-03-16", "active": 1},
    ]


@pytest.fixture
def db_connection(tmp_path) -> Generator[sqlite3.Connection, None, None]:
    """
    Create a temporary SQLite database with all project tables, yield the connection, then clean up.

    Uses WAL mode. Creates all tables defined in the schema. The database file is placed in
    pytest's tmp_path directory so it is automatically removed after the test session.

    Yields:
        sqlite3.Connection: An open connection to the temporary test database.
    """
    db_path = tmp_path / "test_signals.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    schema_statements = [
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
        """CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            username TEXT,
            command TEXT,
            message_text TEXT NOT NULL,
            received_at TEXT NOT NULL
        )""",
    ]

    for statement in schema_statements:
        try:
            conn.execute(statement)
        except sqlite3.OperationalError as exc:
            first_line = statement.strip().splitlines()[0]
            raise RuntimeError(f"Failed to create schema — statement: {first_line!r}") from exc
    conn.commit()

    yield conn

    conn.close()


@pytest.fixture
def mock_polygon_ohlcv_response() -> dict:
    """
    Return a sample Polygon OHLCV API response dict for AAPL.

    Mimics the structure returned by GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}.
    Contains 5 days of OHLCV data with realistic values. Timestamps are generated
    dynamically as the 5 most recent Mon-Fri trading days to avoid stale test data.
    """
    def _last_n_trading_day_timestamps_ms(n: int) -> list[int]:
        """Return Unix millisecond timestamps for the last n Mon-Fri trading days."""
        timestamps = []
        candidate = datetime.now(tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        while len(timestamps) < n:
            if candidate.weekday() < 5:
                timestamps.append(int(candidate.timestamp() * 1000))
            candidate -= timedelta(days=1)
        return list(reversed(timestamps))

    day_ts = _last_n_trading_day_timestamps_ms(5)

    return {
        "ticker": "AAPL",
        "status": "OK",
        "queryCount": 5,
        "resultsCount": 5,
        "adjusted": True,
        "results": [
            {
                "v": 52_341_200,
                "vw": 170.4523,
                "o": 169.50,
                "c": 171.20,
                "h": 172.30,
                "l": 168.90,
                "t": day_ts[0],
                "n": 412_345,
            },
            {
                "v": 48_123_400,
                "vw": 172.1234,
                "o": 171.50,
                "c": 173.00,
                "h": 174.10,
                "l": 170.80,
                "t": day_ts[1],
                "n": 398_210,
            },
            {
                "v": 55_678_900,
                "vw": 171.8901,
                "o": 172.80,
                "c": 170.50,
                "h": 173.50,
                "l": 169.70,
                "t": day_ts[2],
                "n": 445_678,
            },
            {
                "v": 47_890_100,
                "vw": 169.7654,
                "o": 170.20,
                "c": 168.90,
                "h": 171.00,
                "l": 168.00,
                "t": day_ts[3],
                "n": 389_012,
            },
            {
                "v": 61_234_500,
                "vw": 170.9876,
                "o": 169.00,
                "c": 172.50,
                "h": 173.80,
                "l": 168.50,
                "t": day_ts[4],
                "n": 498_765,
            },
        ],
        "next_url": None,
        "request_id": "test-request-id-12345",
    }


@pytest.fixture
def mock_polygon_news_response() -> dict:
    """
    Return a sample Polygon news API response dict with sentiment insights for AAPL.

    Mimics the structure returned by GET /v2/reference/news.
    Contains 3 news articles with sentiment data in the insights field.
    """
    return {
        "status": "OK",
        "count": 3,
        "results": [
            {
                "id": "news-article-001",
                "publisher": {
                    "name": "Reuters",
                    "homepage_url": "https://www.reuters.com",
                    "logo_url": "https://www.reuters.com/logo.png",
                    "favicon_url": "https://www.reuters.com/favicon.ico",
                },
                "title": "Apple Reports Record Revenue in Q4",
                "author": "Jane Smith",
                "published_utc": "2025-01-10T14:30:00Z",
                "article_url": "https://www.reuters.com/article/apple-q4-revenue",
                "tickers": ["AAPL"],
                "description": "Apple Inc reported record quarterly revenue driven by strong iPhone sales.",
                "keywords": ["earnings", "revenue", "iPhone"],
                "insights": [
                    {
                        "ticker": "AAPL",
                        "sentiment": "positive",
                        "sentiment_reasoning": "Record revenue and strong guidance indicate robust business performance.",
                    }
                ],
            },
            {
                "id": "news-article-002",
                "publisher": {
                    "name": "Bloomberg",
                    "homepage_url": "https://www.bloomberg.com",
                    "logo_url": "https://www.bloomberg.com/logo.png",
                    "favicon_url": "https://www.bloomberg.com/favicon.ico",
                },
                "title": "Apple Faces Supply Chain Challenges in Asia",
                "author": "John Doe",
                "published_utc": "2025-01-09T09:15:00Z",
                "article_url": "https://www.bloomberg.com/article/apple-supply-chain",
                "tickers": ["AAPL"],
                "description": "Apple is experiencing supply chain disruptions affecting production timelines.",
                "keywords": ["supply chain", "production", "Asia"],
                "insights": [
                    {
                        "ticker": "AAPL",
                        "sentiment": "negative",
                        "sentiment_reasoning": "Supply chain disruptions may impact production capacity and margins.",
                    }
                ],
            },
            {
                "id": "news-article-003",
                "publisher": {
                    "name": "CNBC",
                    "homepage_url": "https://www.cnbc.com",
                    "logo_url": "https://www.cnbc.com/logo.png",
                    "favicon_url": "https://www.cnbc.com/favicon.ico",
                },
                "title": "Apple Announces New Product Line",
                "author": "Sarah Johnson",
                "published_utc": "2025-01-08T16:45:00Z",
                "article_url": "https://www.cnbc.com/article/apple-new-products",
                "tickers": ["AAPL"],
                "description": "Apple unveiled several new products at its annual spring event.",
                "keywords": ["products", "launch", "innovation"],
                "insights": [
                    {
                        "ticker": "AAPL",
                        "sentiment": "neutral",
                        "sentiment_reasoning": "New product announcements are in line with market expectations.",
                    }
                ],
            },
        ],
        "next_url": None,
        "request_id": "test-news-request-id-67890",
    }
