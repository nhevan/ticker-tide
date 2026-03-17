"""
News backfiller using Polygon.io and Finnhub.

Fetches news articles for each ticker and stores them in the news_articles table.
Polygon provides AI-generated sentiment insights; Finnhub is used as a supplement.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

from src.common.events import log_alert
from src.common.progress import (
    ProgressTracker,
    edit_telegram_message,
    send_telegram_message,
)

logger = logging.getLogger(__name__)


def extract_sentiment_for_ticker(
    insights: list[dict],
    ticker: str,
) -> tuple[str | None, str | None]:
    """
    Search the insights list for the given ticker and return its sentiment fields.

    Iterates over the insights array from a Polygon news article and returns
    the sentiment and sentiment_reasoning for the first matching ticker entry.

    Args:
        insights: List of insight dicts, each with 'ticker', 'sentiment',
            and 'sentiment_reasoning' keys.
        ticker: Stock ticker symbol to search for, e.g. 'AAPL'.

    Returns:
        tuple: (sentiment, sentiment_reasoning) strings, or (None, None) if the
            ticker is not found in the insights list.
    """
    for insight in insights:
        if insight.get("ticker") == ticker:
            return insight.get("sentiment"), insight.get("sentiment_reasoning")
    return None, None


def extract_date_from_published_utc(published_utc: str) -> str:
    """
    Extract the date portion from an ISO 8601 UTC timestamp string.

    Splits on 'T' and returns the first part. For example,
    '2024-06-24T18:33:53Z' returns '2024-06-24'.

    Args:
        published_utc: ISO 8601 timestamp string in the format 'YYYY-MM-DDTHH:MM:SSZ'.

    Returns:
        str: Date portion in 'YYYY-MM-DD' format.
    """
    return published_utc.split("T")[0]


def convert_polygon_news_to_row(article: dict, ticker: str) -> dict:
    """
    Map a Polygon news article dict to the news_articles table schema.

    Extracts the per-ticker sentiment from the insights array (not just the first
    insight) to correctly handle multi-ticker articles.

    Args:
        article: Polygon news article dict with keys: id, title, description,
            article_url, published_utc, insights.
        ticker: Stock ticker symbol for which to extract sentiment, e.g. 'AAPL'.

    Returns:
        dict: Row dict matching the news_articles table schema, ready for INSERT.
    """
    sentiment, sentiment_reasoning = extract_sentiment_for_ticker(
        article.get("insights", []), ticker
    )
    return {
        "id": article["id"],
        "ticker": ticker,
        "date": extract_date_from_published_utc(article["published_utc"]),
        "source": "polygon",
        "headline": article.get("title"),
        "summary": article.get("description"),
        "url": article.get("article_url"),
        "sentiment": sentiment,
        "sentiment_reasoning": sentiment_reasoning,
        "published_utc": article["published_utc"],
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def generate_finnhub_article_id(ticker: str, article: dict) -> str:
    """
    Generate a unique ID for a Finnhub news article.

    Uses a hash of the headline to avoid collisions. Format:
    'finnhub_{ticker}_{datetime}_{sha256[:8]}'

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        article: Finnhub article dict with 'headline' and 'datetime' keys.

    Returns:
        str: Unique article ID string.
    """
    headline_hash = hashlib.sha256(
        article.get("headline", "").encode()
    ).hexdigest()[:8]
    return f"finnhub_{ticker}_{article['datetime']}_{headline_hash}"


def convert_finnhub_news_to_row(article: dict, ticker: str) -> dict:
    """
    Map a Finnhub news article dict to the news_articles table schema.

    Finnhub provides Unix timestamps; this function converts them to ISO 8601
    UTC strings. Sentiment is not available from Finnhub.

    Args:
        article: Finnhub article dict with keys: headline, summary, url, datetime.
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        dict: Row dict matching the news_articles table schema, ready for INSERT.
    """
    published_utc = datetime.fromtimestamp(
        article["datetime"], tz=timezone.utc
    ).isoformat()
    return {
        "id": generate_finnhub_article_id(ticker, article),
        "ticker": ticker,
        "date": published_utc[:10],
        "source": "finnhub",
        "headline": article.get("headline"),
        "summary": article.get("summary"),
        "url": article.get("url"),
        "sentiment": None,
        "sentiment_reasoning": None,
        "published_utc": published_utc,
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def backfill_news_polygon(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    ticker: str,
    from_date: str,
    to_date: str,
    limit: int = 1000,
) -> int:
    """
    Fetch and store Polygon news articles for a single ticker.

    Calls polygon_client.fetch_news(ticker, from_date, to_date, limit=limit), converts each
    article to DB format using the correct per-ticker sentiment extraction, and
    inserts using INSERT OR REPLACE for idempotency.

    Args:
        db_conn: Open SQLite connection with the news_articles table.
        polygon_client: PolygonClient instance with a fetch_news method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format (inclusive).
        to_date: End date in 'YYYY-MM-DD' format (inclusive).
        limit: Maximum articles per page request. Defaults to 1000.

    Returns:
        int: Number of rows inserted. Returns 0 if no data.
    """
    logger.info(
        f"Starting Polygon news backfill for ticker={ticker} from={from_date} to={to_date}"
    )
    articles = polygon_client.fetch_news(ticker, from_date, to_date, limit=limit)

    rows = [convert_polygon_news_to_row(article, ticker) for article in articles]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO news_articles
                (id, ticker, date, source, headline, summary, url,
                 sentiment, sentiment_reasoning, published_utc, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (row["id"], row["ticker"], row["date"], row["source"],
                 row["headline"], row["summary"], row["url"],
                 row["sentiment"], row["sentiment_reasoning"],
                 row["published_utc"], row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(
        f"Backfilled {len(rows)} Polygon news articles for ticker={ticker}"
    )
    return len(rows)


def backfill_news_finnhub(
    db_conn: sqlite3.Connection,
    finnhub_client: object,
    ticker: str,
    from_date: str,
    to_date: str,
) -> int:
    """
    Fetch and store Finnhub news articles for a single ticker.

    Calls finnhub_client.fetch_company_news(ticker, from_date, to_date), converts
    each article to DB format, and inserts using INSERT OR REPLACE for idempotency.
    Exceptions are propagated to the caller.

    Args:
        db_conn: Open SQLite connection with the news_articles table.
        finnhub_client: FinnhubClient instance with a fetch_company_news method.
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format (inclusive).
        to_date: End date in 'YYYY-MM-DD' format (inclusive).

    Returns:
        int: Number of rows inserted. Returns 0 if no data.
    """
    logger.info(
        f"Starting Finnhub news backfill for ticker={ticker} from={from_date} to={to_date}"
    )
    articles = finnhub_client.fetch_company_news(ticker, from_date, to_date)

    rows = [convert_finnhub_news_to_row(article, ticker) for article in articles]
    if rows:
        db_conn.executemany(
            """
            INSERT OR REPLACE INTO news_articles
                (id, ticker, date, source, headline, summary, url,
                 sentiment, sentiment_reasoning, published_utc, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (row["id"], row["ticker"], row["date"], row["source"],
                 row["headline"], row["summary"], row["url"],
                 row["sentiment"], row["sentiment_reasoning"],
                 row["published_utc"], row["fetched_at"])
                for row in rows
            ],
        )

    db_conn.commit()
    logger.info(
        f"Backfilled {len(rows)} Finnhub news articles for ticker={ticker}"
    )
    return len(rows)


def backfill_all_news(
    db_conn: sqlite3.Connection,
    polygon_client: object,
    finnhub_client: object,
    tickers: list[dict],
    config: dict,
    bot_token: str = None,
    chat_id: str = None,
) -> dict:
    """
    Backfill news from both Polygon and Finnhub for all tickers.

    For each ticker, attempts Polygon and Finnhub independently — a Finnhub failure
    does not prevent Polygon data from being stored. Progress is tracked via
    ProgressTracker and Telegram updates sent if credentials are provided.

    Date ranges:
    - Polygon: today minus config["news"]["lookback_months"] months
    - Finnhub: today minus config["news"]["finnhub_lookback_months"] months

    Args:
        db_conn: Open SQLite connection with the news_articles and alerts_log tables.
        polygon_client: PolygonClient instance with a fetch_news method.
        finnhub_client: FinnhubClient instance with a fetch_company_news method.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        config: Backfiller config dict containing the news section.
        bot_token: Optional Telegram bot token for progress notifications.
        chat_id: Optional Telegram chat/channel ID for progress notifications.

    Returns:
        dict with keys: polygon_articles (int), finnhub_articles (int),
            tickers_processed (int), tickers_failed (int).
    """
    ticker_symbols = [ticker["symbol"] for ticker in tickers]
    today = date.today()
    today_str = today.isoformat()

    polygon_lookback = config["news"]["lookback_months"]
    finnhub_lookback = config["news"]["finnhub_lookback_months"]
    polygon_limit = config["news"]["polygon_limit_per_request"]

    polygon_from = (today - relativedelta(months=polygon_lookback)).isoformat()
    finnhub_from = (today - relativedelta(months=finnhub_lookback)).isoformat()

    tracker = ProgressTracker(phase="Backfill News", tickers=ticker_symbols)
    msg_id = None

    if bot_token and chat_id:
        msg_id = send_telegram_message(bot_token, chat_id, tracker.format_progress_message())

    polygon_articles = 0
    finnhub_articles = 0
    tickers_processed = 0
    tickers_failed = 0

    for ticker in ticker_symbols:
        tracker.mark_processing(ticker)
        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

        ticker_had_error = False

        try:
            polygon_count = backfill_news_polygon(
                db_conn, polygon_client, ticker, polygon_from, today_str,
                limit=polygon_limit,
            )
            polygon_articles += polygon_count
        except Exception as exc:
            ticker_had_error = True
            log_alert(
                db_conn, ticker, today_str, "backfiller", "error",
                f"Polygon news backfill failed for ticker={ticker}: {exc}",
            )
            logger.error(
                f"Polygon news backfill failed for ticker={ticker}: {exc!r}"
            )

        try:
            finnhub_count = backfill_news_finnhub(
                db_conn, finnhub_client, ticker, finnhub_from, today_str
            )
            finnhub_articles += finnhub_count
        except Exception as exc:
            ticker_had_error = True
            log_alert(
                db_conn, ticker, today_str, "backfiller", "error",
                f"Finnhub news backfill failed for ticker={ticker}: {exc}",
            )
            logger.error(
                f"Finnhub news backfill failed for ticker={ticker}: {exc!r}"
            )

        if ticker_had_error:
            tickers_failed += 1
            tracker.mark_failed(ticker)
        else:
            tickers_processed += 1
            tracker.mark_completed(ticker)

        if msg_id:
            edit_telegram_message(bot_token, chat_id, msg_id, tracker.format_progress_message())

    duration = (datetime.now(timezone.utc) - tracker.start_time).total_seconds()

    if bot_token and chat_id:
        send_telegram_message(
            bot_token, chat_id,
            tracker.format_final_summary(
                duration,
                extra_stats={
                    "Polygon articles": f"{polygon_articles:,}",
                    "Finnhub articles": f"{finnhub_articles:,}",
                },
            ),
        )

    logger.info(
        f"Backfill News complete: tickers_processed={tickers_processed} "
        f"tickers_failed={tickers_failed} polygon_articles={polygon_articles} "
        f"finnhub_articles={finnhub_articles}"
    )
    return {
        "polygon_articles": polygon_articles,
        "finnhub_articles": finnhub_articles,
        "tickers_processed": tickers_processed,
        "tickers_failed": tickers_failed,
    }
