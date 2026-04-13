"""
Tests for src/calculator/news_aggregator.py

Covers:
- map_sentiment_to_score: positive, negative, neutral, NULL
- aggregate_news_for_date: counts, avg_score, top_headline
- aggregate_news_for_ticker: no articles, multiple dates, idempotency,
  Finnhub NULL sentiment, filing_flag, end-to-end DB round-trip
- aggregate_all_news: multiple tickers
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from src.calculator.news_aggregator import (
    aggregate_all_news,
    aggregate_news_for_date,
    aggregate_news_for_ticker,
    check_filing_on_date,
    map_sentiment_to_score,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _insert_article(
    db_conn: sqlite3.Connection,
    article_id: str,
    ticker: str,
    date_str: str,
    sentiment: str | None,
    headline: str = "Test headline",
    published_utc: str | None = None,
    source: str = "polygon",
) -> None:
    if published_utc is None:
        published_utc = f"{date_str}T12:00:00Z"
    db_conn.execute(
        """
        INSERT OR REPLACE INTO news_articles
            (id, ticker, date, source, headline, summary, url, sentiment, published_utc, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (article_id, ticker, date_str, source, headline, "summary", "http://example.com",
         sentiment, published_utc, "2026-03-16T00:00:00Z"),
    )
    db_conn.commit()


def _insert_filing(db_conn: sqlite3.Connection, ticker: str, filing_date: str) -> None:
    db_conn.execute(
        "INSERT OR REPLACE INTO filings_8k (accession_number, ticker, filing_date, form_type) VALUES (?, ?, ?, '8-K')",
        (f"{ticker}-{filing_date}", ticker, filing_date),
    )
    db_conn.commit()


# ── map_sentiment_to_score ────────────────────────────────────────────────────


def test_map_sentiment_positive() -> None:
    assert map_sentiment_to_score("positive") == pytest.approx(1.0)


def test_map_sentiment_negative() -> None:
    assert map_sentiment_to_score("negative") == pytest.approx(-1.0)


def test_map_sentiment_neutral() -> None:
    assert map_sentiment_to_score("neutral") == pytest.approx(0.0)


def test_map_sentiment_none() -> None:
    """NULL sentiment is treated as neutral (0.0)."""
    assert map_sentiment_to_score(None) == pytest.approx(0.0)


def test_map_sentiment_unexpected_value() -> None:
    """Unknown sentiment strings are treated as neutral (0.0)."""
    assert map_sentiment_to_score("unknown") == pytest.approx(0.0)


# ── aggregate_news_for_date ───────────────────────────────────────────────────


def test_aggregate_news_for_date_counts_and_score() -> None:
    """Counts and avg_sentiment_score are computed correctly from a list of articles."""
    articles = [
        {"sentiment": "positive", "published_utc": "2026-03-16T10:00:00Z", "headline": "Good news"},
        {"sentiment": "positive", "published_utc": "2026-03-16T09:00:00Z", "headline": "More good"},
        {"sentiment": "positive", "published_utc": "2026-03-16T08:00:00Z", "headline": "Also good"},
        {"sentiment": "negative", "published_utc": "2026-03-16T07:00:00Z", "headline": "Bad news"},
        {"sentiment": "neutral",  "published_utc": "2026-03-16T06:00:00Z", "headline": "Neutral"},
    ]

    result = aggregate_news_for_date(articles)

    assert result["article_count"] == 5
    assert result["positive_count"] == 3
    assert result["negative_count"] == 1
    assert result["neutral_count"] == 1
    # avg = (1 + 1 + 1 - 1 + 0) / 5 = 0.4
    assert result["avg_sentiment_score"] == pytest.approx(0.4)


def test_aggregate_news_for_date_top_headline_is_most_recent() -> None:
    """top_headline is the headline of the most recently published article."""
    articles = [
        {"sentiment": "positive", "published_utc": "2026-03-16T08:00:00Z", "headline": "Old news"},
        {"sentiment": "positive", "published_utc": "2026-03-16T14:00:00Z", "headline": "Latest news"},
        {"sentiment": "neutral",  "published_utc": "2026-03-16T10:00:00Z", "headline": "Middle news"},
    ]

    result = aggregate_news_for_date(articles)

    assert result["top_headline"] == "Latest news"


def test_aggregate_news_for_date_all_positive() -> None:
    """All positive articles give avg_sentiment_score = 1.0."""
    articles = [{"sentiment": "positive", "published_utc": f"2026-03-16T{h:02d}:00:00Z", "headline": "Good"} for h in range(4)]

    result = aggregate_news_for_date(articles)

    assert result["avg_sentiment_score"] == pytest.approx(1.0)


def test_aggregate_news_for_date_all_negative() -> None:
    """All negative articles give avg_sentiment_score = -1.0."""
    articles = [{"sentiment": "negative", "published_utc": f"2026-03-16T{h:02d}:00:00Z", "headline": "Bad"} for h in range(3)]

    result = aggregate_news_for_date(articles)

    assert result["avg_sentiment_score"] == pytest.approx(-1.0)


def test_aggregate_news_for_date_null_sentiment_treated_as_neutral() -> None:
    """NULL sentiment counts toward article_count but contributes 0 to avg score."""
    articles = [
        {"sentiment": "positive", "published_utc": "2026-03-16T10:00:00Z", "headline": "Good"},
        {"sentiment": None, "published_utc": "2026-03-16T09:00:00Z", "headline": "Unknown"},
    ]

    result = aggregate_news_for_date(articles)

    assert result["article_count"] == 2
    assert result["neutral_count"] == 1  # NULL counts as neutral
    # avg = (1 + 0) / 2 = 0.5
    assert result["avg_sentiment_score"] == pytest.approx(0.5)


# ── check_filing_on_date ──────────────────────────────────────────────────────


def test_check_filing_on_date_true(db_connection: sqlite3.Connection) -> None:
    """Returns True when an 8-K filing exists for the ticker on that date."""
    _insert_filing(db_connection, "AAPL", "2026-03-16")

    assert check_filing_on_date(db_connection, "AAPL", "2026-03-16") is True


def test_check_filing_on_date_false(db_connection: sqlite3.Connection) -> None:
    """Returns False when no filing exists for the ticker on that date."""
    assert check_filing_on_date(db_connection, "AAPL", "2026-03-16") is False


def test_check_filing_different_ticker(db_connection: sqlite3.Connection) -> None:
    """Filing for MSFT does not count for AAPL."""
    _insert_filing(db_connection, "MSFT", "2026-03-16")

    assert check_filing_on_date(db_connection, "AAPL", "2026-03-16") is False


# ── aggregate_news_for_ticker ─────────────────────────────────────────────────


def test_aggregate_news_for_ticker_single_day(db_connection: sqlite3.Connection) -> None:
    """Aggregates 5 articles on one date into one summary row with correct values."""
    _insert_article(db_connection, "a1", "AAPL", "2026-03-16", "positive", "Good news 1", "2026-03-16T14:00:00Z")
    _insert_article(db_connection, "a2", "AAPL", "2026-03-16", "positive", "Good news 2", "2026-03-16T13:00:00Z")
    _insert_article(db_connection, "a3", "AAPL", "2026-03-16", "positive", "Good news 3", "2026-03-16T12:00:00Z")
    _insert_article(db_connection, "a4", "AAPL", "2026-03-16", "negative", "Bad news",   "2026-03-16T11:00:00Z")
    _insert_article(db_connection, "a5", "AAPL", "2026-03-16", "neutral",  "Neutral",     "2026-03-16T10:00:00Z")

    count = aggregate_news_for_ticker(db_connection, "AAPL")

    assert count == 1
    row = db_connection.execute(
        "SELECT * FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-03-16'"
    ).fetchone()
    assert row is not None
    assert row["article_count"] == 5
    assert row["positive_count"] == 3
    assert row["negative_count"] == 1
    assert row["neutral_count"] == 1
    assert row["avg_sentiment_score"] == pytest.approx(0.4)
    assert row["top_headline"] == "Good news 1"  # most recent published_utc


def test_aggregate_news_no_articles(db_connection: sqlite3.Connection) -> None:
    """No articles for a ticker → no rows in news_daily_summary."""
    count = aggregate_news_for_ticker(db_connection, "AAPL")

    assert count == 0
    row = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert row == 0


def test_aggregate_news_filing_flag_set(db_connection: sqlite3.Connection) -> None:
    """filing_flag=1 when an 8-K exists for that ticker and date."""
    _insert_article(db_connection, "a1", "AAPL", "2026-03-16", "positive")
    _insert_filing(db_connection, "AAPL", "2026-03-16")

    aggregate_news_for_ticker(db_connection, "AAPL")

    row = db_connection.execute(
        "SELECT filing_flag FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-03-16'"
    ).fetchone()
    assert row["filing_flag"] == 1


def test_aggregate_news_no_filing_flag(db_connection: sqlite3.Connection) -> None:
    """filing_flag=0 when no 8-K exists for that ticker and date."""
    _insert_article(db_connection, "a1", "AAPL", "2026-03-16", "positive")

    aggregate_news_for_ticker(db_connection, "AAPL")

    row = db_connection.execute(
        "SELECT filing_flag FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-03-16'"
    ).fetchone()
    assert row["filing_flag"] == 0


def test_aggregate_news_multiple_days(db_connection: sqlite3.Connection) -> None:
    """Articles across 5 different dates produce 5 separate summary rows."""
    for day_offset in range(5):
        day = (date(2026, 3, 10) + timedelta(days=day_offset)).isoformat()
        _insert_article(db_connection, f"a{day_offset}", "AAPL", day, "positive")

    count = aggregate_news_for_ticker(db_connection, "AAPL")

    assert count == 5
    total = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert total == 5


def test_aggregate_news_is_idempotent(db_connection: sqlite3.Connection) -> None:
    """Running aggregation twice does not duplicate rows."""
    _insert_article(db_connection, "a1", "AAPL", "2026-03-16", "positive")
    _insert_article(db_connection, "a2", "AAPL", "2026-03-16", "negative")

    aggregate_news_for_ticker(db_connection, "AAPL")
    aggregate_news_for_ticker(db_connection, "AAPL")

    count = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL'"
    ).fetchone()["cnt"]
    assert count == 1


def test_aggregate_news_finnhub_null_sentiment(db_connection: sqlite3.Connection) -> None:
    """Finnhub articles with NULL sentiment contribute 0 to score but count toward article_count."""
    _insert_article(db_connection, "f1", "AAPL", "2026-03-16", None, source="finnhub",
                    published_utc="2026-03-16T09:00:00Z")
    _insert_article(db_connection, "p1", "AAPL", "2026-03-16", "positive", source="polygon",
                    published_utc="2026-03-16T10:00:00Z")

    aggregate_news_for_ticker(db_connection, "AAPL")

    row = db_connection.execute(
        "SELECT * FROM news_daily_summary WHERE ticker='AAPL'"
    ).fetchone()
    assert row["article_count"] == 2
    assert row["neutral_count"] == 1   # NULL treated as neutral
    assert row["positive_count"] == 1
    # avg = (0 + 1) / 2 = 0.5
    assert row["avg_sentiment_score"] == pytest.approx(0.5)


def test_aggregate_news_date_range_filter(db_connection: sqlite3.Connection) -> None:
    """start_date and end_date filter which articles are aggregated."""
    for day_offset in range(10):
        day = (date(2026, 3, 1) + timedelta(days=day_offset)).isoformat()
        _insert_article(db_connection, f"a{day_offset}", "AAPL", day, "positive")

    count = aggregate_news_for_ticker(db_connection, "AAPL", start_date="2026-03-05", end_date="2026-03-07")

    assert count == 3


def test_aggregate_news_removes_stale_summaries(db_connection: sqlite3.Connection) -> None:
    """
    Stale summary rows for dates where articles no longer exist are deleted
    when the aggregator runs without a date filter.

    Scenario: a summary was previously written for 2026-01-05 with article_count=2,
    but those articles were later lost (e.g. fell outside the fetch lookback window).
    Re-running the aggregator should delete the orphaned summary row.
    """
    # Seed a stale summary row — article_count=2 but no corresponding articles.
    db_connection.execute(
        """
        INSERT INTO news_daily_summary
            (ticker, date, avg_sentiment_score, article_count,
             positive_count, negative_count, neutral_count, top_headline, filing_flag)
        VALUES ('AAPL', '2026-01-05', 1.0, 2, 2, 0, 0, 'Old headline', 0)
        """
    )
    db_connection.commit()

    # Only a more recent article exists.
    _insert_article(db_connection, "a1", "AAPL", "2026-03-16", "positive")

    aggregate_news_for_ticker(db_connection, "AAPL")

    stale = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-01-05'"
    ).fetchone()["cnt"]
    fresh = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-03-16'"
    ).fetchone()["cnt"]
    assert stale == 0, "Stale summary row should have been deleted"
    assert fresh == 1, "Current summary row should still exist"


def test_aggregate_news_stale_cleanup_respects_date_range(db_connection: sqlite3.Connection) -> None:
    """
    When a date range is provided, only stale summaries within that range are deleted.
    Summaries outside the range are left untouched.
    """
    # Stale summary inside the query range (no articles for this date).
    db_connection.execute(
        """
        INSERT INTO news_daily_summary
            (ticker, date, avg_sentiment_score, article_count,
             positive_count, negative_count, neutral_count, top_headline, filing_flag)
        VALUES ('AAPL', '2026-03-06', 1.0, 1, 1, 0, 0, 'Inside range', 0)
        """
    )
    # Stale summary outside the query range — should be untouched.
    db_connection.execute(
        """
        INSERT INTO news_daily_summary
            (ticker, date, avg_sentiment_score, article_count,
             positive_count, negative_count, neutral_count, top_headline, filing_flag)
        VALUES ('AAPL', '2026-01-05', 1.0, 1, 1, 0, 0, 'Outside range', 0)
        """
    )
    db_connection.commit()

    # One real article within the query range.
    _insert_article(db_connection, "a1", "AAPL", "2026-03-10", "positive")

    aggregate_news_for_ticker(db_connection, "AAPL", start_date="2026-03-01", end_date="2026-03-31")

    inside_stale = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-03-06'"
    ).fetchone()["cnt"]
    outside_stale = db_connection.execute(
        "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker='AAPL' AND date='2026-01-05'"
    ).fetchone()["cnt"]
    assert inside_stale == 0, "Stale summary inside date range should be deleted"
    assert outside_stale == 1, "Summary outside date range should be preserved"


# ── aggregate_all_news ────────────────────────────────────────────────────────


def test_aggregate_all_news(db_connection: sqlite3.Connection) -> None:
    """Aggregates news for all tickers in the list."""
    tickers = [{"symbol": "AAPL"}, {"symbol": "MSFT"}, {"symbol": "NVDA"}]
    for ticker_config in tickers:
        sym = ticker_config["symbol"]
        _insert_article(db_connection, f"{sym}-a1", sym, "2026-03-16", "positive")
        _insert_article(db_connection, f"{sym}-a2", sym, "2026-03-16", "negative")

    result = aggregate_all_news(db_connection, tickers)

    assert result["processed"] == 3
    assert result["total_summaries"] >= 3
    for ticker_config in tickers:
        count = db_connection.execute(
            "SELECT COUNT(*) AS cnt FROM news_daily_summary WHERE ticker=?",
            (ticker_config["symbol"],),
        ).fetchone()["cnt"]
        assert count >= 1, f"No summary for {ticker_config['symbol']}"
