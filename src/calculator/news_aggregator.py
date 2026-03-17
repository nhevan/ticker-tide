"""
News sentiment aggregation.

Aggregates individual news articles into a daily per-ticker summary.
The news_daily_summary table is what the Scorer reads — it does not process
individual articles directly.

Summary fields computed per (ticker, date):
  - article_count:       Total articles on that date
  - positive_count:      Articles with sentiment='positive'
  - negative_count:      Articles with sentiment='negative'
  - neutral_count:       Articles with sentiment='neutral' or NULL
  - avg_sentiment_score: Mean score (positive=+1, negative=-1, neutral/NULL=0)
  - top_headline:        Headline of the most recently published article
  - filing_flag:         1 if an 8-K filing exists for the ticker on that date

NULL sentiment (common from Finnhub) is treated as neutral: it counts toward
article_count and neutral_count but contributes 0 to avg_sentiment_score.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from src.common.events import log_alert

logger = logging.getLogger(__name__)


def map_sentiment_to_score(sentiment: Optional[str]) -> float:
    """
    Convert a sentiment label to a numeric score.

    Args:
        sentiment: One of 'positive', 'negative', 'neutral', or None.

    Returns:
        1.0 for positive, -1.0 for negative, 0.0 for neutral or None or unknown.
    """
    if sentiment == "positive":
        return 1.0
    if sentiment == "negative":
        return -1.0
    return 0.0


def aggregate_news_for_date(articles: list[dict]) -> dict:
    """
    Aggregate a list of article dicts into a single daily summary dict.

    Sentiment mapping: positive→+1, negative→-1, neutral/NULL→0.
    top_headline is taken from the article with the latest published_utc.

    Args:
        articles: List of article dicts, each with keys:
                  'sentiment' (str|None), 'published_utc' (str), 'headline' (str).

    Returns:
        Dict with keys: avg_sentiment_score, article_count, positive_count,
        negative_count, neutral_count, top_headline.
    """
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    score_sum = 0.0

    top_headline = None
    top_published_utc: Optional[str] = None

    for article in articles:
        sentiment = article.get("sentiment")
        score = map_sentiment_to_score(sentiment)
        score_sum += score

        if sentiment == "positive":
            positive_count += 1
        elif sentiment == "negative":
            negative_count += 1
        else:
            neutral_count += 1

        published_utc = article.get("published_utc") or ""
        if top_published_utc is None or published_utc > top_published_utc:
            top_published_utc = published_utc
            top_headline = article.get("headline")

    article_count = len(articles)
    avg_sentiment_score = score_sum / article_count if article_count > 0 else 0.0

    return {
        "avg_sentiment_score": avg_sentiment_score,
        "article_count": article_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "top_headline": top_headline,
    }


def check_filing_on_date(
    db_conn: sqlite3.Connection,
    ticker: str,
    date: str,
) -> bool:
    """
    Check whether an 8-K filing exists for a ticker on a specific date.

    Args:
        db_conn: Open SQLite connection with the filings_8k table.
        ticker: Ticker symbol, e.g. 'AAPL'.
        date: ISO date string, e.g. '2026-03-16'.

    Returns:
        True if at least one 8-K filing row exists for that (ticker, date).
    """
    row = db_conn.execute(
        "SELECT 1 FROM filings_8k WHERE ticker = ? AND filing_date = ? LIMIT 1",
        (ticker, date),
    ).fetchone()
    return row is not None


def aggregate_news_for_ticker(
    db_conn: sqlite3.Connection,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """
    Aggregate all news articles for a ticker into daily summary rows.

    Loads articles from news_articles, groups by date, aggregates each day,
    checks for 8-K filings, and upserts into news_daily_summary.

    If no articles exist for a given date, no row is written for that date.

    Args:
        db_conn: Open SQLite connection with news_articles, filings_8k,
                 and news_daily_summary tables.
        ticker: Ticker symbol to aggregate, e.g. 'AAPL'.
        start_date: Optional ISO date to filter from (inclusive).
        end_date: Optional ISO date to filter to (inclusive).

    Returns:
        Number of daily summary rows upserted.
    """
    query = "SELECT id, date, headline, sentiment, published_utc FROM news_articles WHERE ticker = ?"
    params: list = [ticker]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date ASC, published_utc ASC"

    rows = db_conn.execute(query, params).fetchall()

    if not rows:
        logger.debug(f"No news articles found for ticker={ticker}")
        return 0

    # Group articles by date
    articles_by_date: dict[str, list[dict]] = {}
    for row in rows:
        date_key = row["date"]
        if date_key not in articles_by_date:
            articles_by_date[date_key] = []
        articles_by_date[date_key].append(dict(row))

    saved_count = 0
    for date_key, daily_articles in articles_by_date.items():
        summary = aggregate_news_for_date(daily_articles)
        filing_flag = 1 if check_filing_on_date(db_conn, ticker, date_key) else 0

        db_conn.execute(
            """
            INSERT OR REPLACE INTO news_daily_summary
                (ticker, date, avg_sentiment_score, article_count,
                 positive_count, negative_count, neutral_count,
                 top_headline, filing_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker, date_key,
                summary["avg_sentiment_score"],
                summary["article_count"],
                summary["positive_count"],
                summary["negative_count"],
                summary["neutral_count"],
                summary["top_headline"],
                filing_flag,
            ),
        )
        saved_count += 1

    db_conn.commit()
    logger.info(f"Aggregated {saved_count} daily news summaries for ticker={ticker}")
    return saved_count


def aggregate_all_news(
    db_conn: sqlite3.Connection,
    tickers: list[dict],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> dict:
    """
    Aggregate news sentiment for all tickers.

    Per-ticker failures are caught and logged without aborting the run.

    Args:
        db_conn: Open SQLite connection.
        tickers: List of ticker config dicts, each with at least a 'symbol' key.
        start_date: Optional ISO date to filter articles from (inclusive).
        end_date: Optional ISO date to filter articles to (inclusive).
        bot_token: Optional Telegram bot token for progress updates.
        chat_id: Optional Telegram chat ID for progress updates.

    Returns:
        Dict with keys: processed (int), failed (int), total_summaries (int).
    """
    from datetime import datetime, timezone

    processed = 0
    failed = 0
    total_summaries = 0
    today = datetime.now(tz=timezone.utc).date().isoformat()

    for ticker_config in tickers:
        ticker = ticker_config["symbol"]
        try:
            count = aggregate_news_for_ticker(db_conn, ticker, start_date, end_date)
            total_summaries += count
            processed += 1
        except Exception as exc:
            failed += 1
            log_alert(
                db_conn, ticker, today, "calculator",
                "error", f"News aggregation failed for ticker={ticker}: {exc}",
            )
            logger.error(f"News aggregation failed for ticker={ticker}: {exc!r}")

    logger.info(
        f"aggregate_all_news complete: processed={processed} "
        f"failed={failed} total_summaries={total_summaries}"
    )
    return {"processed": processed, "failed": failed, "total_summaries": total_summaries}
