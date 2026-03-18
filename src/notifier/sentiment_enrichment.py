"""
Finnhub news sentiment enrichment using Claude API.

Finnhub articles don't include sentiment scores. This module uses Claude
(Haiku model — cheapest) to classify each article as positive/negative/neutral.

Two modes:
  - Backfill: process all historical NULL-sentiment articles
  - Daily: process new NULL-sentiment articles after the daily fetch

Uses batched prompts (20 articles per Claude call) for efficiency.
Estimated cost: ~$0.001 per article.

After enrichment, news_daily_summary is recomputed for affected dates
so the scorer uses the updated sentiment data.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import anthropic

from src.calculator.news_aggregator import aggregate_news_for_ticker
from src.common.progress import send_telegram_message

logger = logging.getLogger(__name__)

# Valid sentiment labels Claude may return.
_VALID_SENTIMENTS = {"positive", "negative", "neutral"}

# Cost estimate per article (USD) — Haiku model.
_COST_PER_ARTICLE_USD = 0.001


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_articles_needing_sentiment(
    db_conn: sqlite3.Connection, limit: int = 500
) -> list[dict]:
    """
    Query news articles that have NULL sentiment, ordered oldest first.

    Only returns articles where sentiment IS NULL. Articles already scored
    by Polygon (or a previous enrichment run) are excluded.

    Args:
        db_conn: Open SQLite connection with the news_articles table.
        limit: Maximum number of articles to return.

    Returns:
        List of dicts with keys: id, ticker, date, headline, summary, source.
        Ordered by date ASC (oldest first).
    """
    rows = db_conn.execute(
        """
        SELECT id, ticker, date, headline, summary, source
        FROM news_articles
        WHERE sentiment IS NULL
        ORDER BY date ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    return [dict(row) for row in rows]


def build_sentiment_prompt(articles: list[dict]) -> str:
    """
    Build a batched Claude prompt to classify sentiment for a list of articles.

    Each article summary is truncated to 500 characters to keep prompt size
    manageable. When summary is None or empty, the headline is used alone.

    The prompt instructs Claude to respond in a specific numbered format that
    parse_sentiment_response() can reliably parse.

    Args:
        articles: List of article dicts, each with keys:
                  ticker (str), headline (str), summary (str|None).

    Returns:
        Prompt string ready to be sent to the Claude API.
    """
    header = (
        "Classify the sentiment of each financial news article below as exactly\n"
        "one of: positive, negative, or neutral.\n\n"
        "For each article, respond with ONLY the number followed by the classification\n"
        "and a brief reason (max 10 words).\n\n"
        "Format your response exactly like this:\n"
        "1. positive - strong earnings growth reported\n"
        "2. negative - revenue missed expectations\n"
        "3. neutral - routine product announcement\n\n"
        "Articles:\n"
    )

    article_lines = []
    for idx, article in enumerate(articles, start=1):
        ticker = article.get("ticker", "")
        headline = article.get("headline", "")
        raw_summary = article.get("summary") or ""
        summary = raw_summary[:500] if raw_summary else ""

        if summary:
            article_lines.append(
                f'{idx}. [{ticker}] "{headline}"\n   {summary}'
            )
        else:
            article_lines.append(f'{idx}. [{ticker}] "{headline}"')

    return header + "\n\n".join(article_lines)


def parse_sentiment_response(response: str, expected_count: int) -> list[dict]:
    """
    Parse Claude's numbered response into a list of sentiment classifications.

    Handles:
    - Single-word responses for single-article batches (e.g. "positive")
    - Numbered multi-line responses (e.g. "1. positive - reasoning")
    - Capitalized or UPPERCASE sentiment labels (normalised to lowercase)
    - Garbage lines (set to sentiment=None)
    - Fewer lines than expected (fill with None for missing indices)

    Args:
        response: Claude's raw response text.
        expected_count: Number of articles in the batch.

    Returns:
        List of dicts with keys: index (int), sentiment (str|None),
        sentiment_reasoning (str|None). Length always equals expected_count.
    """
    results: list[dict] = [
        {"index": idx, "sentiment": None, "sentiment_reasoning": None}
        for idx in range(expected_count)
    ]

    # Special case: single article with just a label (no number prefix)
    stripped = response.strip()
    if expected_count == 1 and not re.match(r"^\d+\.", stripped):
        lower = stripped.lower()
        # May be "positive" or "positive - some reasoning"
        parts = re.split(r"\s*-\s*", lower, maxsplit=1)
        label = parts[0].strip()
        reasoning = parts[1].strip() if len(parts) > 1 else None
        if label in _VALID_SENTIMENTS:
            results[0]["sentiment"] = label
            results[0]["sentiment_reasoning"] = reasoning if reasoning else None
        return results

    # Parse numbered lines
    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    for line in lines:
        match = re.match(r"^(\d+)\.\s+(\w+)(?:\s*-\s*(.+))?$", line)
        if not match:
            # Could not parse — skip (result already has sentinel None)
            continue

        line_num = int(match.group(1))
        label_raw = match.group(2).lower()
        reasoning_raw = match.group(3)

        if label_raw not in _VALID_SENTIMENTS:
            continue

        idx = line_num - 1
        if 0 <= idx < expected_count:
            results[idx]["sentiment"] = label_raw
            results[idx]["sentiment_reasoning"] = reasoning_raw.strip() if reasoning_raw else None

    return results


def update_article_sentiment(
    db_conn: sqlite3.Connection,
    article_id: str,
    sentiment: str,
    sentiment_reasoning: Optional[str] = None,
) -> bool:
    """
    Update the sentiment and reasoning for a single article.

    The UPDATE uses AND sentiment IS NULL as a safety guard so that existing
    Polygon sentiments (which are higher quality) are never overwritten.

    Args:
        db_conn: Open SQLite connection with the news_articles table.
        article_id: Primary key of the article to update.
        sentiment: One of 'positive', 'negative', 'neutral'.
        sentiment_reasoning: Optional brief reasoning string from Claude.

    Returns:
        True if the row was updated, False if the article already had a
        sentiment value or the article_id was not found.
    """
    cursor = db_conn.execute(
        """
        UPDATE news_articles
        SET sentiment = ?, sentiment_reasoning = ?
        WHERE id = ? AND sentiment IS NULL
        """,
        (sentiment, sentiment_reasoning, article_id),
    )
    db_conn.commit()
    return cursor.rowcount > 0


def enrich_batch(
    db_conn: sqlite3.Connection,
    articles: list[dict],
    config: dict,
) -> dict:
    """
    Classify sentiment for a batch of articles using a single Claude API call.

    Builds a single batched prompt, calls Claude, parses the response, and
    updates the DB for each successfully classified article.

    Args:
        db_conn: Open SQLite connection.
        articles: List of article dicts from get_articles_needing_sentiment().
        config: Full notifier config dict (reads config["sentiment_enrichment"]).

    Returns:
        Dict with keys: enriched (int), failed (int), cost_estimate (float).
    """
    se_config = config.get("sentiment_enrichment", {})
    model = se_config.get("model", "claude-haiku-4-20250514")
    max_tokens = se_config.get("max_tokens", 50)
    temperature = se_config.get("temperature", 0.0)

    enriched = 0
    failed = len(articles)

    prompt = build_sentiment_prompt(articles)

    try:
        client = anthropic.Anthropic()
        message = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        response_text = message.content[0].text
        logger.debug(
            f"phase=sentiment_enrichment Claude response for {len(articles)} articles: {response_text[:200]}"
        )
    except Exception as exc:
        logger.error(
            f"phase=sentiment_enrichment Claude API call failed for batch of {len(articles)} articles: {exc}"
        )
        return {"enriched": 0, "failed": len(articles), "cost_estimate": 0.0}

    parsed = parse_sentiment_response(response_text, len(articles))

    enriched = 0
    failed = 0
    for classification in parsed:
        idx = classification["index"]
        sentiment = classification["sentiment"]

        if sentiment is None:
            failed += 1
            continue

        article = articles[idx]
        was_updated = update_article_sentiment(
            db_conn,
            article["id"],
            sentiment,
            classification["sentiment_reasoning"],
        )
        if was_updated:
            enriched += 1
        else:
            failed += 1

    cost_estimate = len(articles) * _COST_PER_ARTICLE_USD
    logger.info(
        f"phase=sentiment_enrichment batch complete: enriched={enriched} "
        f"failed={failed} cost_estimate=${cost_estimate:.4f}"
    )
    return {"enriched": enriched, "failed": failed, "cost_estimate": cost_estimate}


def recompute_affected_news_summaries(
    db_conn: sqlite3.Connection,
    affected_dates: dict[str, set],
) -> int:
    """
    Recompute news_daily_summary rows for ticker/date pairs that were just enriched.

    After enrichment, previously NULL sentiments now have real values, so the
    avg_sentiment_score in news_daily_summary needs to be updated.

    Args:
        db_conn: Open SQLite connection.
        affected_dates: Dict mapping ticker → set of date strings that had
                        articles enriched (e.g. {"AAPL": {"2025-01-15", ...}}).

    Returns:
        Total number of news_daily_summary rows recomputed.
    """
    if not affected_dates:
        return 0

    recomputed = 0
    for ticker, dates in affected_dates.items():
        for date_str in dates:
            try:
                aggregate_news_for_ticker(db_conn, ticker, start_date=date_str, end_date=date_str)
                recomputed += 1
            except Exception as exc:
                logger.warning(
                    f"phase=sentiment_enrichment Failed to recompute summary for "
                    f"ticker={ticker} date={date_str}: {exc}"
                )

    logger.info(
        f"phase=sentiment_enrichment Recomputed {recomputed} news_daily_summary rows"
    )
    return recomputed


def run_sentiment_enrichment(
    db_conn: sqlite3.Connection,
    config: dict,
    bot_token: Optional[str] = None,
    admin_chat_id: Optional[str] = None,
) -> dict:
    """
    Run the full Finnhub sentiment enrichment pipeline.

    Checks if enrichment is enabled, fetches NULL-sentiment articles up to
    max_articles_per_run, processes them in batches via Claude, updates the
    DB, and recomputes affected news_daily_summary rows.

    Args:
        db_conn: Open SQLite connection.
        config: Full notifier config dict. Must contain 'sentiment_enrichment'
                section with enabled, batch_size, max_articles_per_run keys.
        bot_token: Optional Telegram bot token for admin progress updates.
        admin_chat_id: Optional Telegram admin chat ID for progress updates.

    Returns:
        Dict with keys: total (int), enriched (int), failed (int),
        summaries_recomputed (int), duration_seconds (float).
        Returns {"skipped": True, "reason": str} when enrichment is disabled.
    """
    se_config = config.get("sentiment_enrichment", {})

    if not se_config.get("enabled", False):
        logger.info("phase=sentiment_enrichment Disabled in config — skipping")
        return {"skipped": True, "reason": "disabled"}

    batch_size = se_config.get("batch_size", 20)
    max_articles = se_config.get("max_articles_per_run", 500)

    start_ts = datetime.now(tz=timezone.utc)
    logger.info(f"phase=sentiment_enrichment Starting (max_articles={max_articles} batch_size={batch_size})")

    articles = get_articles_needing_sentiment(db_conn, limit=max_articles)

    if not articles:
        logger.info("phase=sentiment_enrichment No articles need sentiment enrichment")
        return {
            "total": 0,
            "enriched": 0,
            "failed": 0,
            "summaries_recomputed": 0,
            "duration_seconds": 0.0,
        }

    total = len(articles)
    logger.info(f"phase=sentiment_enrichment Found {total} articles needing enrichment")

    if bot_token and admin_chat_id:
        send_telegram_message(
            bot_token,
            admin_chat_id,
            f"🧠 Sentiment Enrichment started — {total} articles to process",
        )

    total_enriched = 0
    total_failed = 0
    total_cost = 0.0
    affected_dates: dict[str, set] = {}

    # Track (ticker, date) pairs before enrichment so we can recompute summaries
    for article in articles:
        ticker = article["ticker"]
        date_str = article["date"]
        if ticker not in affected_dates:
            affected_dates[ticker] = set()
        affected_dates[ticker].add(date_str)

    # Process in batches
    for batch_start in range(0, total, batch_size):
        batch = articles[batch_start : batch_start + batch_size]
        batch_result = enrich_batch(db_conn, batch, config)
        total_enriched += batch_result["enriched"]
        total_failed += batch_result["failed"]
        total_cost += batch_result["cost_estimate"]

        processed_so_far = min(batch_start + batch_size, total)
        if bot_token and admin_chat_id:
            send_telegram_message(
                bot_token,
                admin_chat_id,
                f"🧠 Sentiment Enrichment: {processed_so_far}/{total} articles processed",
            )

    # Recompute summaries for all affected (ticker, date) pairs
    summaries_recomputed = recompute_affected_news_summaries(db_conn, affected_dates)

    duration_seconds = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)

    summary_message = (
        f"🧠 Sentiment Enrichment Complete\n"
        f"Articles: {total_enriched}/{total} enriched ({total_failed} failed)\n"
        f"Summaries recomputed: {summaries_recomputed}\n"
        f"Estimated cost: ${total_cost:.3f}\n"
        f"Duration: {minutes}m {seconds}s"
    )
    logger.info(f"phase=sentiment_enrichment {summary_message}")

    if bot_token and admin_chat_id:
        send_telegram_message(bot_token, admin_chat_id, summary_message)

    return {
        "total": total,
        "enriched": total_enriched,
        "failed": total_failed,
        "summaries_recomputed": summaries_recomputed,
        "duration_seconds": duration_seconds,
    }
