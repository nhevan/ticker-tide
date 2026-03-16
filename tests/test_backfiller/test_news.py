"""Tests for src/backfiller/news.py.

All tests are written first (TDD). All external API calls are mocked.
"""

import hashlib
import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from src.backfiller.news import (
    backfill_all_news,
    backfill_news_finnhub,
    backfill_news_polygon,
    convert_finnhub_news_to_row,
    convert_polygon_news_to_row,
    extract_date_from_published_utc,
    extract_sentiment_for_ticker,
    generate_finnhub_article_id,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_polygon_articles() -> list[dict]:
    """Return 3 Polygon news articles for AAPL with sentiment insights."""
    return [
        {
            "id": "news-article-001",
            "title": "Apple Reports Record Revenue in Q4",
            "description": "Apple Inc reported record quarterly revenue.",
            "article_url": "https://www.reuters.com/article/apple-q4",
            "published_utc": "2024-06-24T18:33:53Z",
            "insights": [
                {
                    "ticker": "AAPL",
                    "sentiment": "positive",
                    "sentiment_reasoning": "Record revenue indicates strong performance.",
                }
            ],
        },
        {
            "id": "news-article-002",
            "title": "Apple Faces Supply Chain Challenges",
            "description": "Apple is experiencing supply chain disruptions.",
            "article_url": "https://www.bloomberg.com/article/apple-supply",
            "published_utc": "2024-06-23T09:15:00Z",
            "insights": [
                {
                    "ticker": "AAPL",
                    "sentiment": "negative",
                    "sentiment_reasoning": "Supply chain disruptions may impact margins.",
                }
            ],
        },
        {
            "id": "news-article-003",
            "title": "Apple Announces New Product Line",
            "description": "Apple unveiled several new products.",
            "article_url": "https://www.cnbc.com/article/apple-products",
            "published_utc": "2024-06-22T16:45:00Z",
            "insights": [
                {
                    "ticker": "AAPL",
                    "sentiment": "neutral",
                    "sentiment_reasoning": "New products are in line with expectations.",
                }
            ],
        },
    ]


@pytest.fixture
def sample_finnhub_articles() -> list[dict]:
    """Return 2 Finnhub news articles for AAPL."""
    return [
        {
            "headline": "Apple beats earnings expectations",
            "summary": "Apple posted stronger than expected results.",
            "url": "https://example.com/apple-earnings",
            "datetime": 1719244433,  # 2024-06-24T18:33:53Z
        },
        {
            "headline": "Apple launches new services",
            "summary": "Apple announced expanded services lineup.",
            "url": "https://example.com/apple-services",
            "datetime": 1719158033,  # 2024-06-23T18:33:53Z
        },
    ]


@pytest.fixture
def sample_config() -> dict:
    """Return a minimal backfiller config dict for news."""
    return {
        "news": {
            "lookback_months": 3,
            "finnhub_lookback_months": 1,
            "polygon_limit_per_request": 1000,
        }
    }


# ---------------------------------------------------------------------------
# Tests for extract_sentiment_for_ticker
# ---------------------------------------------------------------------------

def test_extract_sentiment_for_ticker_finds_matching_insight() -> None:
    """Returns sentiment and reasoning for the matched ticker."""
    insights = [
        {"ticker": "AAPL", "sentiment": "positive", "sentiment_reasoning": "Strong growth."},
        {"ticker": "MSFT", "sentiment": "negative", "sentiment_reasoning": "Weak outlook."},
    ]
    sentiment, reasoning = extract_sentiment_for_ticker(insights, "AAPL")
    assert sentiment == "positive"
    assert reasoning == "Strong growth."


def test_extract_sentiment_for_ticker_returns_none_when_no_match() -> None:
    """Returns (None, None) when insights list has no matching ticker."""
    insights = [
        {"ticker": "MSFT", "sentiment": "positive", "sentiment_reasoning": "Good quarter."},
    ]
    sentiment, reasoning = extract_sentiment_for_ticker(insights, "AAPL")
    assert sentiment is None
    assert reasoning is None


def test_extract_sentiment_for_ticker_returns_none_for_empty_list() -> None:
    """Returns (None, None) when insights list is empty."""
    sentiment, reasoning = extract_sentiment_for_ticker([], "AAPL")
    assert sentiment is None
    assert reasoning is None


# ---------------------------------------------------------------------------
# Tests for extract_date_from_published_utc
# ---------------------------------------------------------------------------

def test_extract_date_from_published_utc_extracts_date_portion() -> None:
    """Splits on T and returns first part."""
    date_str = extract_date_from_published_utc("2024-06-24T18:33:53Z")
    assert date_str == "2024-06-24"


# ---------------------------------------------------------------------------
# Tests for convert_polygon_news_to_row
# ---------------------------------------------------------------------------

def test_convert_polygon_news_to_row_maps_fields() -> None:
    """All fields are mapped correctly from a Polygon article dict."""
    article = {
        "id": "news-article-001",
        "title": "Apple Reports Record Revenue in Q4",
        "description": "Apple Inc reported record quarterly revenue.",
        "article_url": "https://www.reuters.com/article/apple-q4",
        "published_utc": "2024-06-24T18:33:53Z",
        "insights": [
            {
                "ticker": "AAPL",
                "sentiment": "positive",
                "sentiment_reasoning": "Record revenue indicates strong performance.",
            }
        ],
    }
    row = convert_polygon_news_to_row(article, "AAPL")
    assert row["id"] == "news-article-001"
    assert row["ticker"] == "AAPL"
    assert row["date"] == "2024-06-24"
    assert row["source"] == "polygon"
    assert row["headline"] == "Apple Reports Record Revenue in Q4"
    assert row["summary"] == "Apple Inc reported record quarterly revenue."
    assert row["url"] == "https://www.reuters.com/article/apple-q4"
    assert row["sentiment"] == "positive"
    assert row["sentiment_reasoning"] == "Record revenue indicates strong performance."
    assert row["published_utc"] == "2024-06-24T18:33:53Z"
    assert row["fetched_at"] is not None


def test_convert_polygon_news_to_row_extracts_sentiment_for_correct_ticker() -> None:
    """When article has 2 insights, backfilling for AAPL gets AAPL sentiment."""
    article = {
        "id": "multi-ticker-001",
        "title": "Tech Stocks Rally",
        "description": "Both Apple and Microsoft gained.",
        "article_url": "https://example.com/tech-rally",
        "published_utc": "2024-06-24T10:00:00Z",
        "insights": [
            {"ticker": "AAPL", "sentiment": "positive", "sentiment_reasoning": "AAPL up 5%."},
            {"ticker": "MSFT", "sentiment": "negative", "sentiment_reasoning": "MSFT down 2%."},
        ],
    }
    row_aapl = convert_polygon_news_to_row(article, "AAPL")
    row_msft = convert_polygon_news_to_row(article, "MSFT")
    assert row_aapl["sentiment"] == "positive"
    assert row_aapl["sentiment_reasoning"] == "AAPL up 5%."
    assert row_msft["sentiment"] == "negative"
    assert row_msft["sentiment_reasoning"] == "MSFT down 2%."


def test_convert_polygon_news_to_row_no_matching_sentiment() -> None:
    """When insights don't contain the requested ticker, sentiment fields are None."""
    article = {
        "id": "no-match-001",
        "title": "Generic Market News",
        "description": "Markets moved today.",
        "article_url": "https://example.com/market",
        "published_utc": "2024-06-24T10:00:00Z",
        "insights": [
            {"ticker": "GOOGL", "sentiment": "positive", "sentiment_reasoning": "Search beats."},
        ],
    }
    row = convert_polygon_news_to_row(article, "AAPL")
    assert row["sentiment"] is None
    assert row["sentiment_reasoning"] is None


# ---------------------------------------------------------------------------
# Tests for generate_finnhub_article_id
# ---------------------------------------------------------------------------

def test_generate_finnhub_article_id_format() -> None:
    """ID follows the pattern finnhub_{ticker}_{datetime}_{sha256[:8]}."""
    article = {"headline": "Apple earnings beat", "datetime": 1719244433}
    article_id = generate_finnhub_article_id("AAPL", article)
    headline_hash = hashlib.sha256("Apple earnings beat".encode()).hexdigest()[:8]
    assert article_id == f"finnhub_AAPL_1719244433_{headline_hash}"


# ---------------------------------------------------------------------------
# Tests for convert_finnhub_news_to_row
# ---------------------------------------------------------------------------

def test_convert_finnhub_news_to_row_maps_fields() -> None:
    """Finnhub article fields are mapped correctly, including Unix datetime conversion."""
    article = {
        "headline": "Apple beats earnings expectations",
        "summary": "Apple posted stronger than expected results.",
        "url": "https://example.com/apple-earnings",
        "datetime": 1719244433,
    }
    row = convert_finnhub_news_to_row(article, "AAPL")
    expected_published_utc = datetime.fromtimestamp(1719244433, tz=timezone.utc).isoformat()
    assert row["headline"] == "Apple beats earnings expectations"
    assert row["summary"] == "Apple posted stronger than expected results."
    assert row["url"] == "https://example.com/apple-earnings"
    assert row["ticker"] == "AAPL"
    assert row["source"] == "finnhub"
    assert row["published_utc"] == expected_published_utc
    assert row["date"] == expected_published_utc[:10]
    assert row["sentiment"] is None
    assert row["sentiment_reasoning"] is None
    assert row["fetched_at"] is not None


def test_convert_finnhub_news_to_row_generates_unique_id() -> None:
    """ID is in format finnhub_{ticker}_{datetime}_{sha256[:8]}."""
    article = {
        "headline": "Apple beats earnings expectations",
        "summary": "Summary.",
        "url": "https://example.com/article",
        "datetime": 1719244433,
    }
    row = convert_finnhub_news_to_row(article, "AAPL")
    expected_hash = hashlib.sha256("Apple beats earnings expectations".encode()).hexdigest()[:8]
    assert row["id"] == f"finnhub_AAPL_1719244433_{expected_hash}"


# ---------------------------------------------------------------------------
# Tests for backfill_news_polygon
# ---------------------------------------------------------------------------

def test_backfill_news_polygon_stores_articles(
    db_connection, sample_polygon_articles
) -> None:
    """Mock fetch_news returning 3 articles → 3 rows with source='polygon'."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = sample_polygon_articles

    count = backfill_news_polygon(
        db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24"
    )

    assert count == 3
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM news_articles WHERE ticker='AAPL' AND source='polygon'"
    ).fetchone()[0]
    assert row_count == 3


def test_backfill_news_polygon_maps_fields(db_connection) -> None:
    """All fields are correctly stored in news_articles table."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = [
        {
            "id": "news-article-001",
            "title": "Apple Reports Record Revenue in Q4",
            "description": "Apple Inc reported record quarterly revenue.",
            "article_url": "https://www.reuters.com/article/apple-q4",
            "published_utc": "2024-06-24T18:33:53Z",
            "insights": [
                {
                    "ticker": "AAPL",
                    "sentiment": "positive",
                    "sentiment_reasoning": "Record revenue indicates strong performance.",
                }
            ],
        }
    ]

    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    row = db_connection.execute(
        "SELECT * FROM news_articles WHERE id='news-article-001'"
    ).fetchone()
    assert row["ticker"] == "AAPL"
    assert row["headline"] == "Apple Reports Record Revenue in Q4"
    assert row["summary"] == "Apple Inc reported record quarterly revenue."
    assert row["url"] == "https://www.reuters.com/article/apple-q4"
    assert row["sentiment"] == "positive"
    assert row["sentiment_reasoning"] == "Record revenue indicates strong performance."
    assert row["published_utc"] == "2024-06-24T18:33:53Z"
    assert row["date"] == "2024-06-24"
    assert row["source"] == "polygon"


def test_backfill_news_polygon_extracts_sentiment_for_correct_ticker(
    db_connection,
) -> None:
    """Multi-ticker article: backfill for AAPL stores AAPL sentiment, not MSFT's."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = [
        {
            "id": "multi-ticker-001",
            "title": "Tech Stocks Rally",
            "description": "Both Apple and Microsoft gained.",
            "article_url": "https://example.com/tech-rally",
            "published_utc": "2024-06-24T10:00:00Z",
            "insights": [
                {"ticker": "AAPL", "sentiment": "positive", "sentiment_reasoning": "AAPL up 5%."},
                {"ticker": "MSFT", "sentiment": "negative", "sentiment_reasoning": "MSFT down 2%."},
            ],
        }
    ]

    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    row = db_connection.execute(
        "SELECT sentiment, sentiment_reasoning FROM news_articles WHERE id='multi-ticker-001'"
    ).fetchone()
    assert row["sentiment"] == "positive"
    assert row["sentiment_reasoning"] == "AAPL up 5%."


def test_backfill_news_polygon_no_matching_sentiment(db_connection) -> None:
    """When insights don't contain the requested ticker, sentiment fields are NULL."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = [
        {
            "id": "no-match-001",
            "title": "Generic News",
            "description": "Markets moved.",
            "article_url": "https://example.com/market",
            "published_utc": "2024-06-24T10:00:00Z",
            "insights": [
                {"ticker": "GOOGL", "sentiment": "positive", "sentiment_reasoning": "Search beats."},
            ],
        }
    ]

    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    row = db_connection.execute(
        "SELECT sentiment, sentiment_reasoning FROM news_articles WHERE id='no-match-001'"
    ).fetchone()
    assert row["sentiment"] is None
    assert row["sentiment_reasoning"] is None


def test_backfill_news_polygon_extracts_date_from_published_utc(db_connection) -> None:
    """'2024-06-24T18:33:53Z' is stored as date='2024-06-24'."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = [
        {
            "id": "date-test-001",
            "title": "Test Article",
            "description": "Test description.",
            "article_url": "https://example.com/test",
            "published_utc": "2024-06-24T18:33:53Z",
            "insights": [],
        }
    ]

    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    row = db_connection.execute(
        "SELECT date FROM news_articles WHERE id='date-test-001'"
    ).fetchone()
    assert row["date"] == "2024-06-24"


def test_backfill_news_polygon_deduplicates(db_connection, sample_polygon_articles) -> None:
    """Inserting the same articles twice yields 3 rows (id is PRIMARY KEY)."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = sample_polygon_articles

    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")
    backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    count = db_connection.execute(
        "SELECT COUNT(*) FROM news_articles WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert count == 3


def test_backfill_news_polygon_handles_api_error(db_connection) -> None:
    """When fetch_news returns [], no crash occurs and 0 is returned."""
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = []

    count = backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    assert count == 0


def test_backfill_news_polygon_follows_pagination(db_connection) -> None:
    """Articles from multiple pages are all stored."""
    page1 = [
        {
            "id": f"page1-article-{i}",
            "title": f"Page 1 Article {i}",
            "description": "Description.",
            "article_url": f"https://example.com/page1/{i}",
            "published_utc": "2024-06-24T10:00:00Z",
            "insights": [],
        }
        for i in range(3)
    ]
    page2 = [
        {
            "id": f"page2-article-{i}",
            "title": f"Page 2 Article {i}",
            "description": "Description.",
            "article_url": f"https://example.com/page2/{i}",
            "published_utc": "2024-06-23T10:00:00Z",
            "insights": [],
        }
        for i in range(2)
    ]
    mock_client = MagicMock()
    mock_client.fetch_news.return_value = page1 + page2

    count = backfill_news_polygon(db_connection, mock_client, "AAPL", "2024-03-24", "2024-06-24")

    assert count == 5
    total_rows = db_connection.execute(
        "SELECT COUNT(*) FROM news_articles WHERE ticker='AAPL'"
    ).fetchone()[0]
    assert total_rows == 5


# ---------------------------------------------------------------------------
# Tests for backfill_news_finnhub
# ---------------------------------------------------------------------------

def test_backfill_news_finnhub_stores_articles(
    db_connection, sample_finnhub_articles
) -> None:
    """Mock fetch_company_news returning 2 articles → 2 rows with source='finnhub'."""
    mock_client = MagicMock()
    mock_client.fetch_company_news.return_value = sample_finnhub_articles

    count = backfill_news_finnhub(
        db_connection, mock_client, "AAPL", "2024-05-24", "2024-06-24"
    )

    assert count == 2
    row_count = db_connection.execute(
        "SELECT COUNT(*) FROM news_articles WHERE ticker='AAPL' AND source='finnhub'"
    ).fetchone()[0]
    assert row_count == 2


def test_backfill_news_finnhub_maps_fields(db_connection) -> None:
    """Finnhub fields stored correctly, Unix datetime → published_utc and date."""
    mock_client = MagicMock()
    article = {
        "headline": "Apple beats earnings",
        "summary": "Strong quarter.",
        "url": "https://example.com/aapl-earnings",
        "datetime": 1719244433,
    }
    mock_client.fetch_company_news.return_value = [article]

    backfill_news_finnhub(db_connection, mock_client, "AAPL", "2024-05-24", "2024-06-24")

    expected_published_utc = datetime.fromtimestamp(1719244433, tz=timezone.utc).isoformat()
    row = db_connection.execute(
        "SELECT * FROM news_articles WHERE ticker='AAPL' AND source='finnhub'"
    ).fetchone()
    assert row["headline"] == "Apple beats earnings"
    assert row["summary"] == "Strong quarter."
    assert row["url"] == "https://example.com/aapl-earnings"
    assert row["published_utc"] == expected_published_utc
    assert row["date"] == expected_published_utc[:10]


def test_backfill_news_finnhub_generates_unique_id(db_connection) -> None:
    """Stored id is 'finnhub_{ticker}_{datetime}_{sha256[:8]}'."""
    mock_client = MagicMock()
    article = {
        "headline": "Apple beats earnings",
        "summary": "Strong quarter.",
        "url": "https://example.com/aapl-earnings",
        "datetime": 1719244433,
    }
    mock_client.fetch_company_news.return_value = [article]

    backfill_news_finnhub(db_connection, mock_client, "AAPL", "2024-05-24", "2024-06-24")

    expected_hash = hashlib.sha256("Apple beats earnings".encode()).hexdigest()[:8]
    expected_id = f"finnhub_AAPL_1719244433_{expected_hash}"
    row = db_connection.execute(
        f"SELECT id FROM news_articles WHERE id=?", (expected_id,)
    ).fetchone()
    assert row is not None


def test_backfill_news_finnhub_no_sentiment(db_connection) -> None:
    """Finnhub articles always have NULL sentiment and sentiment_reasoning."""
    mock_client = MagicMock()
    mock_client.fetch_company_news.return_value = [
        {
            "headline": "Apple news",
            "summary": "Summary.",
            "url": "https://example.com/news",
            "datetime": 1719244433,
        }
    ]

    backfill_news_finnhub(db_connection, mock_client, "AAPL", "2024-05-24", "2024-06-24")

    row = db_connection.execute(
        "SELECT sentiment, sentiment_reasoning FROM news_articles WHERE source='finnhub'"
    ).fetchone()
    assert row["sentiment"] is None
    assert row["sentiment_reasoning"] is None


def test_backfill_news_finnhub_handles_error(db_connection) -> None:
    """When fetch_company_news raises an exception, it propagates to the caller."""
    mock_client = MagicMock()
    mock_client.fetch_company_news.side_effect = RuntimeError("Finnhub API error")

    import pytest
    with pytest.raises(RuntimeError, match="Finnhub API error"):
        backfill_news_finnhub(
            db_connection, mock_client, "AAPL", "2024-05-24", "2024-06-24"
        )


# ---------------------------------------------------------------------------
# Tests for backfill_all_news
# ---------------------------------------------------------------------------

def test_backfill_all_news_calls_both_sources(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """With 2 tickers, fetch_news called twice and fetch_company_news called twice."""
    tickers = sample_tickers_list[:2]
    mock_polygon = MagicMock()
    mock_polygon.fetch_news.return_value = []
    mock_finnhub = MagicMock()
    mock_finnhub.fetch_company_news.return_value = []

    backfill_all_news(
        db_connection, mock_polygon, mock_finnhub, tickers, sample_config
    )

    assert mock_polygon.fetch_news.call_count == 2
    assert mock_finnhub.fetch_company_news.call_count == 2


def test_backfill_all_news_continues_on_partial_failure(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """Finnhub fails for one ticker, Polygon data is still stored for all tickers."""
    # Use ticker-specific article IDs so each ticker's articles are unique rows
    def polygon_articles_for_ticker(ticker, from_date, to_date, **kwargs):
        return [
            {
                "id": f"polygon-{ticker}-001",
                "title": f"{ticker} Article 1",
                "description": f"{ticker} news.",
                "article_url": f"https://example.com/{ticker.lower()}-1",
                "published_utc": "2024-06-24T10:00:00Z",
                "insights": [{"ticker": ticker, "sentiment": "positive", "sentiment_reasoning": "Good."}],
            }
        ]

    mock_polygon = MagicMock()
    mock_polygon.fetch_news.side_effect = polygon_articles_for_ticker

    mock_finnhub = MagicMock()
    mock_finnhub.fetch_company_news.side_effect = [
        RuntimeError("Finnhub down"),
        [],
        [],
    ]

    result = backfill_all_news(
        db_connection, mock_polygon, mock_finnhub, sample_tickers_list, sample_config
    )

    # Polygon articles should still be stored for all 3 tickers (1 unique article each)
    polygon_count = db_connection.execute(
        "SELECT COUNT(*) FROM news_articles WHERE source='polygon'"
    ).fetchone()[0]
    assert polygon_count == 3

    # Result dict has required keys
    assert "polygon_articles" in result
    assert "finnhub_articles" in result
    assert "tickers_processed" in result
    assert "tickers_failed" in result


def test_backfill_all_news_returns_summary(
    db_connection, sample_tickers_list, sample_config
) -> None:
    """Return dict contains polygon_articles, finnhub_articles, tickers_processed, tickers_failed."""
    mock_polygon = MagicMock()
    mock_polygon.fetch_news.return_value = []
    mock_finnhub = MagicMock()
    mock_finnhub.fetch_company_news.return_value = []

    result = backfill_all_news(
        db_connection, mock_polygon, mock_finnhub, sample_tickers_list, sample_config
    )

    assert "polygon_articles" in result
    assert "finnhub_articles" in result
    assert "tickers_processed" in result
    assert "tickers_failed" in result
    assert result["tickers_processed"] == 3
    assert result["tickers_failed"] == 0
