"""
Data validation functions for the Stock Signal Engine.

Every data row is validated before being inserted into the database.
Validation functions return a tuple of (is_valid: bool, reasons: list[str]).
If valid, reasons is an empty list. If invalid, reasons contains human-readable
descriptions of all violations found.
"""

from datetime import datetime


_OHLCV_REQUIRED_FIELDS = ["ticker", "date", "open", "high", "low", "close", "volume"]
_NEWS_REQUIRED_FIELDS = ["id", "ticker", "date", "headline", "source"]
_VALID_NEWS_SOURCES = {"polygon", "finnhub"}


def validate_ohlcv_row(row: dict) -> tuple[bool, list[str]]:
    """
    Validate a single OHLCV data row.

    Required fields: ticker, date, open, high, low, close, volume.
    Optional fields (may be None): vwap, num_transactions.

    Validation rules:
    - All required fields must be present and not None.
    - close > 0
    - volume > 0
    - high >= low
    - close >= low and close <= high
    - open >= low and open <= high

    All violations are collected before returning (not short-circuited).

    Args:
        row: A dict representing one OHLCV data point.

    Returns:
        A tuple (is_valid, reasons) where is_valid is True when the row passes
        all checks, and reasons is a list of human-readable violation strings.
    """
    reasons: list[str] = []

    for field in _OHLCV_REQUIRED_FIELDS:
        if field not in row:
            reasons.append(f"missing required field: {field}")
        elif row[field] is None:
            reasons.append(f"missing required field: {field}")

    if reasons:
        return False, reasons

    close = row["close"]
    volume = row["volume"]
    high = row["high"]
    low = row["low"]
    open_price = row["open"]

    if close <= 0:
        reasons.append("close must be > 0")

    if volume <= 0:
        reasons.append("volume must be > 0")

    if high < low:
        reasons.append("high must be >= low")

    if close > high:
        reasons.append("close must be <= high")

    if close < low:
        reasons.append("close must be >= low")

    if open_price > high:
        reasons.append("open must be <= high")

    if open_price < low:
        reasons.append("open must be >= low")

    return (len(reasons) == 0, reasons)


def validate_news_article(article: dict) -> tuple[bool, list[str]]:
    """
    Validate a news article dict.

    Required fields: id, ticker, date, headline, source.
    headline must not be an empty string.
    source must be one of: "polygon", "finnhub".

    Args:
        article: A dict representing one news article.

    Returns:
        A tuple (is_valid, reasons) where is_valid is True when the article
        passes all checks, and reasons is a list of human-readable violation strings.
    """
    reasons: list[str] = []

    for field in _NEWS_REQUIRED_FIELDS:
        if field not in article:
            reasons.append(f"missing required field: {field}")

    if reasons:
        return False, reasons

    if article["headline"] == "":
        reasons.append("headline must not be empty")

    if article["source"] not in _VALID_NEWS_SOURCES:
        reasons.append(f"source must be one of: {', '.join(sorted(_VALID_NEWS_SOURCES))}")

    return (len(reasons) == 0, reasons)


def validate_date_format(date_str: object) -> bool:
    """
    Return True if date_str is a valid YYYY-MM-DD calendar date, False otherwise.

    Rejects None, empty strings, wrong formats, and impossible dates (e.g., Feb 30).
    Rejects full ISO 8601 datetime strings (only YYYY-MM-DD is accepted).

    Args:
        date_str: The string to validate. May be None.

    Returns:
        True if date_str is a valid YYYY-MM-DD date string, False otherwise.
    """
    if not isinstance(date_str, str) or not date_str:
        return False

    # Reject anything that looks like a datetime (contains 'T', ' ', or timezone)
    if len(date_str) != 10:
        return False

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
