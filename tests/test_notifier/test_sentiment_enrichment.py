"""
Tests for src/notifier/sentiment_enrichment.py — Finnhub news sentiment enrichment.

All Claude API calls are mocked — no real API calls are made.
Uses the shared db_connection fixture from conftest.py for database tests.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Sample config used across tests
# ---------------------------------------------------------------------------

SAMPLE_CONFIG = {
    "sentiment_enrichment": {
        "enabled": True,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 50,
        "temperature": 0.0,
        "batch_size": 10,
        "max_articles_per_run": 500,
        "retry_failed": True,
    }
}

SAMPLE_CONFIG_DISABLED = {
    "sentiment_enrichment": {
        "enabled": False,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 50,
        "temperature": 0.0,
        "batch_size": 10,
        "max_articles_per_run": 500,
        "retry_failed": True,
    }
}

# ---------------------------------------------------------------------------
# Helpers for inserting test articles
# ---------------------------------------------------------------------------


def _insert_article(
    conn: sqlite3.Connection,
    article_id: str,
    ticker: str = "AAPL",
    date: str = "2025-01-15",
    source: str = "finnhub",
    headline: str = "Test headline",
    summary: str = "Test summary.",
    sentiment: str | None = None,
    sentiment_reasoning: str | None = None,
) -> None:
    """Insert a single news article into the test database."""
    conn.execute(
        """
        INSERT OR IGNORE INTO news_articles
            (id, ticker, date, source, headline, summary, url,
             sentiment, sentiment_reasoning, published_utc, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article_id, ticker, date, source, headline, summary,
            f"https://example.com/{article_id}", sentiment, sentiment_reasoning,
            f"{date}T12:00:00Z", "2025-01-15T00:00:00Z",
        ),
    )
    conn.commit()


def _insert_news_daily_summary(
    conn: sqlite3.Connection,
    ticker: str,
    date: str,
    avg_sentiment_score: float = 0.0,
    article_count: int = 1,
    positive_count: int = 0,
    negative_count: int = 0,
    neutral_count: int = 1,
) -> None:
    """Insert a news_daily_summary row for testing."""
    conn.execute(
        """
        INSERT OR REPLACE INTO news_daily_summary
            (ticker, date, avg_sentiment_score, article_count,
             positive_count, negative_count, neutral_count, top_headline, filing_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ticker, date, avg_sentiment_score, article_count,
         positive_count, negative_count, neutral_count, "Test headline", 0),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests: build_sentiment_prompt
# ---------------------------------------------------------------------------


class TestBuildSentimentPrompt:
    """Tests for build_sentiment_prompt()."""

    def test_build_sentiment_prompt_single_article(self) -> None:
        """Prompt for one article asks for exactly one of positive/negative/neutral."""
        from src.notifier.sentiment_enrichment import build_sentiment_prompt

        article = {
            "id": "finnhub_aapl_1",
            "ticker": "AAPL",
            "headline": "Apple beats Q4 earnings expectations",
            "summary": "Revenue grew 12% YoY with strong iPhone sales driving growth.",
        }
        prompt = build_sentiment_prompt([article])

        assert "positive" in prompt
        assert "negative" in prompt
        assert "neutral" in prompt
        assert "AAPL" in prompt
        assert "Apple beats Q4 earnings expectations" in prompt
        assert "Revenue grew 12% YoY" in prompt

    def test_build_sentiment_prompt_batch(self) -> None:
        """Prompt for 5 articles numbers them 1-5 and asks for one classification each."""
        from src.notifier.sentiment_enrichment import build_sentiment_prompt

        articles = [
            {"id": f"finnhub_{t}_1", "ticker": t, "headline": f"{t} headline", "summary": f"{t} summary"}
            for t in ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]
        ]
        prompt = build_sentiment_prompt(articles)

        for idx in range(1, 6):
            assert f"{idx}." in prompt
        for ticker in ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]:
            assert ticker in prompt

    def test_build_sentiment_prompt_truncates_long_summary(self) -> None:
        """Summaries longer than 500 chars are truncated in the prompt."""
        from src.notifier.sentiment_enrichment import build_sentiment_prompt

        long_summary = "X" * 2000
        article = {
            "id": "finnhub_aapl_1",
            "ticker": "AAPL",
            "headline": "Short headline",
            "summary": long_summary,
        }
        prompt = build_sentiment_prompt([article])

        # The 2000-char summary must not appear verbatim; only first 500 chars
        assert long_summary not in prompt
        assert "X" * 500 in prompt
        assert "X" * 501 not in prompt

    def test_build_sentiment_prompt_handles_none_summary(self) -> None:
        """When summary is None, the prompt falls back to headline text only."""
        from src.notifier.sentiment_enrichment import build_sentiment_prompt

        article = {
            "id": "finnhub_aapl_1",
            "ticker": "AAPL",
            "headline": "Apple launches new product",
            "summary": None,
        }
        prompt = build_sentiment_prompt([article])

        assert "Apple launches new product" in prompt

    def test_build_sentiment_prompt_includes_ticker_context(self) -> None:
        """Prompt includes the ticker so Claude can apply financial context."""
        from src.notifier.sentiment_enrichment import build_sentiment_prompt

        article = {
            "id": "finnhub_tsla_1",
            "ticker": "TSLA",
            "headline": "Quarterly delivery numbers released",
            "summary": "Tesla delivered 460,000 vehicles this quarter.",
        }
        prompt = build_sentiment_prompt([article])

        assert "TSLA" in prompt


# ---------------------------------------------------------------------------
# Tests: parse_sentiment_response
# ---------------------------------------------------------------------------


class TestParseSentimentResponse:
    """Tests for parse_sentiment_response()."""

    def test_parse_sentiment_response_single(self) -> None:
        """Single-word 'positive' response for 1 article."""
        from src.notifier.sentiment_enrichment import parse_sentiment_response

        result = parse_sentiment_response("positive", 1)

        assert len(result) == 1
        assert result[0]["index"] == 0
        assert result[0]["sentiment"] == "positive"
        assert result[0]["sentiment_reasoning"] is None

    def test_parse_sentiment_response_batch(self) -> None:
        """Numbered multi-line response parses all 5 with reasoning."""
        from src.notifier.sentiment_enrichment import parse_sentiment_response

        response = (
            "1. positive - strong earnings beat\n"
            "2. negative - revenue decline noted\n"
            "3. neutral - routine product update\n"
            "4. positive - market share gains\n"
            "5. negative - CEO departure announced"
        )
        result = parse_sentiment_response(response, 5)

        assert len(result) == 5

        assert result[0]["index"] == 0
        assert result[0]["sentiment"] == "positive"
        assert result[0]["sentiment_reasoning"] == "strong earnings beat"

        assert result[1]["index"] == 1
        assert result[1]["sentiment"] == "negative"
        assert result[1]["sentiment_reasoning"] == "revenue decline noted"

        assert result[2]["index"] == 2
        assert result[2]["sentiment"] == "neutral"
        assert result[2]["sentiment_reasoning"] == "routine product update"

        assert result[3]["index"] == 3
        assert result[3]["sentiment"] == "positive"

        assert result[4]["index"] == 4
        assert result[4]["sentiment"] == "negative"
        assert result[4]["sentiment_reasoning"] == "CEO departure announced"

    def test_parse_sentiment_response_handles_variations(self) -> None:
        """Capitalized and UPPERCASE sentiments are normalized to lowercase."""
        from src.notifier.sentiment_enrichment import parse_sentiment_response

        result_cap = parse_sentiment_response("1. Positive", 1)
        assert result_cap[0]["sentiment"] == "positive"

        result_upper = parse_sentiment_response("1. POSITIVE", 1)
        assert result_upper[0]["sentiment"] == "positive"

        result_neg = parse_sentiment_response("1. Negative - bad news", 1)
        assert result_neg[0]["sentiment"] == "negative"

    def test_parse_sentiment_response_handles_invalid(self) -> None:
        """Garbage lines get sentiment=None; valid lines are still parsed."""
        from src.notifier.sentiment_enrichment import parse_sentiment_response

        response = (
            "1. positive - good news\n"
            "2. THIS IS NOT VALID AT ALL BLAH BLAH\n"
            "3. negative - bad news"
        )
        result = parse_sentiment_response(response, 3)

        assert len(result) == 3
        assert result[0]["sentiment"] == "positive"
        assert result[1]["sentiment"] is None
        assert result[2]["sentiment"] == "negative"

    def test_parse_sentiment_response_wrong_count(self) -> None:
        """When Claude returns fewer lines than expected, missing entries get None."""
        from src.notifier.sentiment_enrichment import parse_sentiment_response

        response = (
            "1. positive - earnings beat\n"
            "2. negative - revenue miss\n"
            "3. neutral - no change"
        )
        result = parse_sentiment_response(response, 5)

        assert len(result) == 5
        assert result[0]["sentiment"] == "positive"
        assert result[1]["sentiment"] == "negative"
        assert result[2]["sentiment"] == "neutral"
        assert result[3]["sentiment"] is None
        assert result[4]["sentiment"] is None


# ---------------------------------------------------------------------------
# Tests: get_articles_needing_sentiment
# ---------------------------------------------------------------------------


class TestGetArticlesNeedingSentiment:
    """Tests for get_articles_needing_sentiment()."""

    def test_get_articles_needing_sentiment(self, db_connection: sqlite3.Connection) -> None:
        """Returns only NULL-sentiment articles, ignoring those already scored."""
        from src.notifier.sentiment_enrichment import get_articles_needing_sentiment

        # 6 Finnhub articles with NULL sentiment
        for idx in range(6):
            _insert_article(
                db_connection,
                f"finnhub_aapl_{idx}",
                source="finnhub",
                date=f"2025-01-{idx + 1:02d}",
                sentiment=None,
            )
        # 4 Polygon articles with sentiment already set
        for idx in range(4):
            _insert_article(
                db_connection,
                f"polygon_aapl_{idx}",
                source="polygon",
                date=f"2025-01-{idx + 1:02d}",
                sentiment="positive",
            )

        articles = get_articles_needing_sentiment(db_connection, limit=500)

        assert len(articles) == 6
        ids = {a["id"] for a in articles}
        for idx in range(6):
            assert f"finnhub_aapl_{idx}" in ids
        for idx in range(4):
            assert f"polygon_aapl_{idx}" not in ids

    def test_get_articles_needing_sentiment_respects_limit(self, db_connection: sqlite3.Connection) -> None:
        """Respects the limit parameter — does not return more than limit articles."""
        from src.notifier.sentiment_enrichment import get_articles_needing_sentiment

        for idx in range(100):
            _insert_article(
                db_connection,
                f"finnhub_aapl_{idx}",
                source="finnhub",
                date=f"2025-01-{(idx % 28) + 1:02d}",
                sentiment=None,
            )

        articles = get_articles_needing_sentiment(db_connection, limit=50)

        assert len(articles) == 50

    def test_get_articles_needing_sentiment_orders_by_date(self, db_connection: sqlite3.Connection) -> None:
        """Articles are returned ordered by date ASC (oldest first)."""
        from src.notifier.sentiment_enrichment import get_articles_needing_sentiment

        dates = ["2025-03-01", "2025-01-15", "2025-02-10", "2024-12-01"]
        for idx, date in enumerate(dates):
            _insert_article(
                db_connection,
                f"finnhub_aapl_{idx}",
                source="finnhub",
                date=date,
                sentiment=None,
            )

        articles = get_articles_needing_sentiment(db_connection, limit=500)

        returned_dates = [a["date"] for a in articles]
        assert returned_dates == sorted(returned_dates)

    def test_get_articles_needing_sentiment_returns_required_fields(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Each returned article has id, ticker, date, headline, summary, source."""
        from src.notifier.sentiment_enrichment import get_articles_needing_sentiment

        _insert_article(
            db_connection,
            "finnhub_test_1",
            ticker="TSLA",
            source="finnhub",
            headline="Tesla Q4 results",
            summary="Strong delivery numbers.",
            sentiment=None,
        )

        articles = get_articles_needing_sentiment(db_connection, limit=10)

        assert len(articles) == 1
        article = articles[0]
        assert article["id"] == "finnhub_test_1"
        assert article["ticker"] == "TSLA"
        assert article["headline"] == "Tesla Q4 results"
        assert article["summary"] == "Strong delivery numbers."
        assert article["source"] == "finnhub"


# ---------------------------------------------------------------------------
# Tests: update_article_sentiment
# ---------------------------------------------------------------------------


class TestUpdateArticleSentiment:
    """Tests for update_article_sentiment()."""

    def test_update_article_sentiment(self, db_connection: sqlite3.Connection) -> None:
        """Updates sentiment and reasoning for a NULL-sentiment article."""
        from src.notifier.sentiment_enrichment import update_article_sentiment

        _insert_article(db_connection, "finnhub_123", sentiment=None)

        updated = update_article_sentiment(
            db_connection, "finnhub_123", "positive", "Strong earnings beat"
        )

        assert updated is True
        row = db_connection.execute(
            "SELECT sentiment, sentiment_reasoning FROM news_articles WHERE id = ?",
            ("finnhub_123",),
        ).fetchone()
        assert row["sentiment"] == "positive"
        assert row["sentiment_reasoning"] == "Strong earnings beat"

    def test_update_article_sentiment_does_not_overwrite_existing(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Does NOT overwrite an article that already has a sentiment value."""
        from src.notifier.sentiment_enrichment import update_article_sentiment

        _insert_article(db_connection, "polygon_456", source="polygon", sentiment="negative")

        updated = update_article_sentiment(
            db_connection, "polygon_456", "positive", "Would overwrite"
        )

        assert updated is False
        row = db_connection.execute(
            "SELECT sentiment FROM news_articles WHERE id = ?",
            ("polygon_456",),
        ).fetchone()
        assert row["sentiment"] == "negative"

    def test_update_article_sentiment_returns_false_for_missing_id(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns False when the article ID does not exist."""
        from src.notifier.sentiment_enrichment import update_article_sentiment

        updated = update_article_sentiment(
            db_connection, "nonexistent_999", "positive", "Irrelevant"
        )

        assert updated is False


# ---------------------------------------------------------------------------
# Tests: enrich_batch
# ---------------------------------------------------------------------------


class TestEnrichBatch:
    """Tests for enrich_batch()."""

    def test_enrich_batch(self, db_connection: sqlite3.Connection) -> None:
        """Mock Claude API call enriches 5 articles; returns correct stats."""
        from src.notifier.sentiment_enrichment import enrich_batch

        articles = []
        for idx in range(5):
            article_id = f"finnhub_batch_{idx}"
            _insert_article(db_connection, article_id, ticker="AAPL", date="2025-01-15", sentiment=None)
            articles.append({
                "id": article_id,
                "ticker": "AAPL",
                "date": "2025-01-15",
                "headline": f"Headline {idx}",
                "summary": f"Summary {idx}",
                "source": "finnhub",
            })

        mock_response_text = (
            "1. positive - earnings beat\n"
            "2. negative - revenue miss\n"
            "3. neutral - routine update\n"
            "4. positive - market gains\n"
            "5. negative - CEO resign"
        )

        mock_message = MagicMock()
        mock_message.content = [MagicMock(text=mock_response_text)]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_message

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = enrich_batch(db_connection, articles, SAMPLE_CONFIG)

        assert result["enriched"] == 5
        assert result["failed"] == 0
        assert mock_client.messages.create.call_count == 1

        # Verify DB was updated
        rows = db_connection.execute(
            "SELECT id, sentiment FROM news_articles WHERE id LIKE 'finnhub_batch_%'"
        ).fetchall()
        sentiments = {row["id"]: row["sentiment"] for row in rows}
        assert sentiments["finnhub_batch_0"] == "positive"
        assert sentiments["finnhub_batch_1"] == "negative"
        assert sentiments["finnhub_batch_2"] == "neutral"

    def test_enrich_batch_partial_failure(self, db_connection: sqlite3.Connection) -> None:
        """When 2 of 5 lines are unparseable, 3 are enriched and 2 left as NULL."""
        from src.notifier.sentiment_enrichment import enrich_batch

        articles = []
        for idx in range(5):
            article_id = f"finnhub_partial_{idx}"
            _insert_article(db_connection, article_id, ticker="AAPL", date="2025-01-15", sentiment=None)
            articles.append({
                "id": article_id,
                "ticker": "AAPL",
                "date": "2025-01-15",
                "headline": f"Headline {idx}",
                "summary": f"Summary {idx}",
                "source": "finnhub",
            })

        # Only 3 of 5 valid; 2 are garbage
        mock_response_text = (
            "1. positive - ok\n"
            "2. GARBAGE LINE NOT PARSEABLE\n"
            "3. neutral - flat\n"
            "4. ANOTHER BAD LINE\n"
            "5. negative - bad"
        )

        mock_message = MagicMock()
        mock_message.content = [MagicMock(text=mock_response_text)]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_message

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = enrich_batch(db_connection, articles, SAMPLE_CONFIG)

        assert result["enriched"] == 3
        assert result["failed"] == 2

        rows = db_connection.execute(
            "SELECT id, sentiment FROM news_articles WHERE id LIKE 'finnhub_partial_%'"
        ).fetchall()
        sentiments = {row["id"]: row["sentiment"] for row in rows}
        assert sentiments["finnhub_partial_0"] == "positive"
        assert sentiments["finnhub_partial_1"] is None  # left untouched
        assert sentiments["finnhub_partial_2"] == "neutral"
        assert sentiments["finnhub_partial_3"] is None  # left untouched
        assert sentiments["finnhub_partial_4"] == "negative"

    def test_enrich_batch_api_failure(self, db_connection: sqlite3.Connection) -> None:
        """When Claude raises an exception, no articles are updated; returns 0 enriched."""
        from src.notifier.sentiment_enrichment import enrich_batch

        articles = []
        for idx in range(5):
            article_id = f"finnhub_apifail_{idx}"
            _insert_article(db_connection, article_id, ticker="AAPL", date="2025-01-15", sentiment=None)
            articles.append({
                "id": article_id,
                "ticker": "AAPL",
                "date": "2025-01-15",
                "headline": f"Headline {idx}",
                "summary": f"Summary {idx}",
                "source": "finnhub",
            })

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API timeout")

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = enrich_batch(db_connection, articles, SAMPLE_CONFIG)

        assert result["enriched"] == 0
        assert result["failed"] == 5

        # All articles remain NULL
        rows = db_connection.execute(
            "SELECT sentiment FROM news_articles WHERE id LIKE 'finnhub_apifail_%'"
        ).fetchall()
        for row in rows:
            assert row["sentiment"] is None


# ---------------------------------------------------------------------------
# Tests: run_sentiment_enrichment
# ---------------------------------------------------------------------------


class TestRunSentimentEnrichment:
    """Tests for run_sentiment_enrichment()."""

    def _make_mock_claude_response(self, count: int) -> MagicMock:
        """Build a mock Claude response for count articles (all positive)."""
        lines = "\n".join(f"{i + 1}. positive - good news" for i in range(count))
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text=lines)]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_message
        return mock_client

    def test_run_sentiment_enrichment_disabled(self, db_connection: sqlite3.Connection) -> None:
        """When enabled=false, Claude is not called and function returns immediately."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        # Insert some NULL articles
        for idx in range(5):
            _insert_article(db_connection, f"finnhub_disabled_{idx}", sentiment=None)

        mock_client = MagicMock()
        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = run_sentiment_enrichment(db_connection, SAMPLE_CONFIG_DISABLED)

        mock_client.messages.create.assert_not_called()
        assert result.get("skipped") is True or result == {} or result.get("enriched", 0) == 0

    def test_run_sentiment_enrichment_full(self, db_connection: sqlite3.Connection) -> None:
        """25 NULL articles with batch_size=10 triggers 3 Claude calls (10+10+5)."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        for idx in range(25):
            _insert_article(
                db_connection,
                f"finnhub_full_{idx}",
                ticker="AAPL",
                date=f"2025-01-{(idx % 28) + 1:02d}",
                sentiment=None,
            )

        mock_client = self._make_mock_claude_response(10)  # first two batches of 10
        # We need to handle variable batch sizes: just return valid responses
        def side_effect(**kwargs):
            prompt_text = kwargs.get("messages", [{}])[0].get("content", "")
            # Count articles in prompt by counting numbered lines
            import re
            count = len(re.findall(r"^\d+\.", prompt_text, re.MULTILINE))
            if count == 0:
                count = 10
            lines = "\n".join(f"{i + 1}. positive - good" for i in range(count))
            mock_msg = MagicMock()
            mock_msg.content = [MagicMock(text=lines)]
            return mock_msg

        mock_client.messages.create.side_effect = side_effect

        config = {**SAMPLE_CONFIG, "sentiment_enrichment": {**SAMPLE_CONFIG["sentiment_enrichment"], "batch_size": 10}}

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = run_sentiment_enrichment(db_connection, config)

        assert mock_client.messages.create.call_count == 3
        assert result["enriched"] == 25
        assert result["total"] == 25

    def test_run_sentiment_enrichment_respects_max(self, db_connection: sqlite3.Connection) -> None:
        """Config max_articles_per_run=50 limits processing to 50 of 100 articles."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        for idx in range(100):
            _insert_article(
                db_connection,
                f"finnhub_max_{idx}",
                ticker="AAPL",
                date=f"2025-01-{(idx % 28) + 1:02d}",
                sentiment=None,
            )

        def side_effect(**kwargs):
            prompt_text = kwargs.get("messages", [{}])[0].get("content", "")
            import re
            count = len(re.findall(r"^\d+\.", prompt_text, re.MULTILINE))
            if count == 0:
                count = 10
            lines = "\n".join(f"{i + 1}. neutral - ok" for i in range(count))
            mock_msg = MagicMock()
            mock_msg.content = [MagicMock(text=lines)]
            return mock_msg

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = side_effect

        config = {
            "sentiment_enrichment": {
                "enabled": True,
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 50,
                "temperature": 0.0,
                "batch_size": 10,
                "max_articles_per_run": 50,
                "retry_failed": True,
            }
        }

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = run_sentiment_enrichment(db_connection, config)

        assert result["total"] == 50
        assert result["enriched"] == 50

        # Only 50 of 100 should now have sentiment
        enriched_count = db_connection.execute(
            "SELECT COUNT(*) AS cnt FROM news_articles WHERE sentiment IS NOT NULL AND id LIKE 'finnhub_max_%'"
        ).fetchone()["cnt"]
        assert enriched_count == 50

    def test_run_sentiment_enrichment_updates_news_summary(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """After enrichment, news_daily_summary avg_sentiment_score is recomputed."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        # Insert a NULL-sentiment article and a summary that says 0.0 avg
        _insert_article(
            db_connection,
            "finnhub_recompute_1",
            ticker="AAPL",
            date="2025-01-15",
            sentiment=None,
        )
        _insert_news_daily_summary(
            db_connection,
            ticker="AAPL",
            date="2025-01-15",
            avg_sentiment_score=0.0,
            article_count=1,
            neutral_count=1,
        )

        # Claude returns "positive" — after enrichment summary should change
        mock_client = MagicMock()
        mock_msg = MagicMock()
        mock_msg.content = [MagicMock(text="1. positive - good news")]
        mock_client.messages.create.return_value = mock_msg

        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = run_sentiment_enrichment(db_connection, SAMPLE_CONFIG)

        assert result["enriched"] == 1
        assert result["summaries_recomputed"] >= 1

        # The summary should now reflect positive sentiment
        row = db_connection.execute(
            "SELECT avg_sentiment_score, positive_count FROM news_daily_summary "
            "WHERE ticker = 'AAPL' AND date = '2025-01-15'"
        ).fetchone()
        assert row is not None
        assert row["avg_sentiment_score"] == 1.0
        assert row["positive_count"] == 1

    def test_run_sentiment_enrichment_no_articles(self, db_connection: sqlite3.Connection) -> None:
        """When no NULL articles exist, Claude is not called and stats are zero."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        mock_client = MagicMock()
        with patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client):
            result = run_sentiment_enrichment(db_connection, SAMPLE_CONFIG)

        mock_client.messages.create.assert_not_called()
        assert result.get("enriched", 0) == 0

    def test_run_sentiment_enrichment_sends_progress(self, db_connection: sqlite3.Connection) -> None:
        """Progress updates are sent to admin_chat_id when bot_token and chat_id given."""
        from src.notifier.sentiment_enrichment import run_sentiment_enrichment

        for idx in range(5):
            _insert_article(db_connection, f"finnhub_prog_{idx}", sentiment=None)

        mock_client = MagicMock()
        mock_msg = MagicMock()
        mock_msg.content = [MagicMock(text="\n".join(f"{i + 1}. positive - ok" for i in range(5)))]
        mock_client.messages.create.return_value = mock_msg

        with (
            patch("src.notifier.sentiment_enrichment.anthropic.Anthropic", return_value=mock_client),
            patch("src.notifier.sentiment_enrichment.send_telegram_message") as mock_send,
        ):
            run_sentiment_enrichment(
                db_connection,
                SAMPLE_CONFIG,
                bot_token="fake-token",
                admin_chat_id="12345",
            )

        # At least one Telegram message should have been sent
        assert mock_send.call_count >= 1


# ---------------------------------------------------------------------------
# Tests: recompute_affected_news_summaries
# ---------------------------------------------------------------------------


class TestRecomputeAffectedNewsSummaries:
    """Tests for recompute_affected_news_summaries()."""

    def test_recompute_updates_avg_sentiment_score(self, db_connection: sqlite3.Connection) -> None:
        """After enriching articles, recompute updates the daily summary avg_sentiment_score."""
        from src.notifier.sentiment_enrichment import recompute_affected_news_summaries

        # Insert positive-sentiment article (already enriched)
        _insert_article(
            db_connection,
            "finnhub_recomp_1",
            ticker="AAPL",
            date="2025-02-10",
            sentiment="positive",
        )
        # Stale summary
        _insert_news_daily_summary(
            db_connection,
            ticker="AAPL",
            date="2025-02-10",
            avg_sentiment_score=0.0,
            neutral_count=1,
            article_count=1,
        )

        affected = {"AAPL": {"2025-02-10"}}
        count = recompute_affected_news_summaries(db_connection, affected)

        assert count == 1
        row = db_connection.execute(
            "SELECT avg_sentiment_score FROM news_daily_summary WHERE ticker='AAPL' AND date='2025-02-10'"
        ).fetchone()
        assert row["avg_sentiment_score"] == 1.0

    def test_recompute_returns_zero_for_empty_affected(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """When affected dict is empty, recompute returns 0 and makes no DB changes."""
        from src.notifier.sentiment_enrichment import recompute_affected_news_summaries

        count = recompute_affected_news_summaries(db_connection, {})

        assert count == 0
