"""
Tests for src/common/validators.py — OHLCV, news, and date format validation.
"""

import pytest

from src.common.validators import (
    validate_date_format,
    validate_news_article,
    validate_ohlcv_row,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_OHLCV = {
    "ticker": "AAPL",
    "date": "2024-01-02",
    "open": 187.15,
    "high": 188.44,
    "low": 183.88,
    "close": 185.64,
    "volume": 81_964_874,
    "vwap": 185.95,
}

VALID_ARTICLE = {
    "id": "abc123",
    "ticker": "AAPL",
    "date": "2024-01-02",
    "headline": "Apple reports earnings",
    "source": "polygon",
}


# ---------------------------------------------------------------------------
# validate_ohlcv_row — happy path
# ---------------------------------------------------------------------------


def test_validate_ohlcv_valid_row():
    """A fully valid OHLCV row should pass with no reasons."""
    is_valid, reasons = validate_ohlcv_row(VALID_OHLCV)
    assert is_valid is True
    assert reasons == []


# ---------------------------------------------------------------------------
# validate_ohlcv_row — individual field violations
# ---------------------------------------------------------------------------


def test_validate_ohlcv_zero_close():
    """close=0 should fail with a reason about close."""
    row = {**VALID_OHLCV, "close": 0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("close" in r and "0" in r or "close must be > 0" in r for r in reasons)


def test_validate_ohlcv_negative_close():
    """close < 0 should fail."""
    row = {**VALID_OHLCV, "close": -5.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert len(reasons) >= 1


def test_validate_ohlcv_zero_volume():
    """volume=0 should fail with a reason about volume."""
    row = {**VALID_OHLCV, "volume": 0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("volume must be > 0" in r for r in reasons)


def test_validate_ohlcv_negative_volume():
    """volume < 0 should fail."""
    row = {**VALID_OHLCV, "volume": -100}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert len(reasons) >= 1


def test_validate_ohlcv_high_less_than_low():
    """high < low is always invalid."""
    row = {**VALID_OHLCV, "high": 180.0, "low": 185.0, "close": 182.0, "open": 183.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("high must be >= low" in r for r in reasons)


def test_validate_ohlcv_close_above_high():
    """close > high is invalid."""
    row = {**VALID_OHLCV, "close": 200.0, "high": 190.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("close must be <= high" in r for r in reasons)


def test_validate_ohlcv_close_below_low():
    """close < low is invalid."""
    row = {**VALID_OHLCV, "close": 170.0, "low": 180.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("close must be >= low" in r for r in reasons)


def test_validate_ohlcv_open_above_high():
    """open > high is invalid."""
    row = {**VALID_OHLCV, "open": 200.0, "high": 190.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("open must be <= high" in r for r in reasons)


def test_validate_ohlcv_open_below_low():
    """open < low is invalid."""
    row = {**VALID_OHLCV, "open": 170.0, "low": 180.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("open must be >= low" in r for r in reasons)


# ---------------------------------------------------------------------------
# validate_ohlcv_row — multiple violations collected
# ---------------------------------------------------------------------------


def test_validate_ohlcv_multiple_violations():
    """All violations should be collected — not just the first one."""
    row = {**VALID_OHLCV, "close": 0, "high": 180.0, "low": 185.0, "open": 183.0}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert len(reasons) >= 2


# ---------------------------------------------------------------------------
# validate_ohlcv_row — missing / None fields
# ---------------------------------------------------------------------------


def test_validate_ohlcv_missing_required_field():
    """Absence of a required field should produce a 'missing required field' reason."""
    row = {k: v for k, v in VALID_OHLCV.items() if k != "close"}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert any("missing required field: close" in r for r in reasons)


def test_validate_ohlcv_none_close():
    """close=None should fail."""
    row = {**VALID_OHLCV, "close": None}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is False
    assert len(reasons) >= 1


def test_validate_ohlcv_allows_none_vwap():
    """vwap=None is acceptable — it is optional."""
    row = {**VALID_OHLCV, "vwap": None}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is True
    assert reasons == []


def test_validate_ohlcv_allows_none_num_transactions():
    """num_transactions=None is acceptable — it is optional."""
    row = {**VALID_OHLCV, "num_transactions": None}
    is_valid, reasons = validate_ohlcv_row(row)
    assert is_valid is True
    assert reasons == []


# ---------------------------------------------------------------------------
# validate_news_article
# ---------------------------------------------------------------------------


def test_validate_news_article_valid():
    """A fully valid article should pass with no reasons."""
    is_valid, reasons = validate_news_article(VALID_ARTICLE)
    assert is_valid is True
    assert reasons == []


def test_validate_news_article_missing_id():
    """Article without 'id' should fail with a missing-field reason."""
    article = {k: v for k, v in VALID_ARTICLE.items() if k != "id"}
    is_valid, reasons = validate_news_article(article)
    assert is_valid is False
    assert any("missing required field: id" in r for r in reasons)


def test_validate_news_article_missing_headline():
    """Article without 'headline' should fail with a missing-field reason."""
    article = {k: v for k, v in VALID_ARTICLE.items() if k != "headline"}
    is_valid, reasons = validate_news_article(article)
    assert is_valid is False
    assert any("missing required field: headline" in r for r in reasons)


def test_validate_news_article_missing_ticker():
    """Article without 'ticker' should fail."""
    article = {k: v for k, v in VALID_ARTICLE.items() if k != "ticker"}
    is_valid, reasons = validate_news_article(article)
    assert is_valid is False
    assert len(reasons) >= 1


def test_validate_news_article_empty_headline():
    """headline='' should fail with a non-empty headline reason."""
    article = {**VALID_ARTICLE, "headline": ""}
    is_valid, reasons = validate_news_article(article)
    assert is_valid is False
    assert any("headline must not be empty" in r for r in reasons)


# ---------------------------------------------------------------------------
# validate_date_format
# ---------------------------------------------------------------------------


def test_validate_date_format_valid():
    """A correctly formatted calendar date should return True."""
    assert validate_date_format("2024-01-15") is True


def test_validate_date_format_invalid_format():
    """MM-DD-YYYY format should return False."""
    assert validate_date_format("01-15-2024") is False


def test_validate_date_format_invalid_date():
    """A non-existent calendar date should return False."""
    assert validate_date_format("2024-13-45") is False


def test_validate_date_format_empty_string():
    """Empty string should return False."""
    assert validate_date_format("") is False


def test_validate_date_format_none():
    """None should return False."""
    assert validate_date_format(None) is False


def test_validate_date_format_iso_datetime():
    """A full ISO 8601 datetime string should return False (only YYYY-MM-DD accepted)."""
    assert validate_date_format("2024-01-15T10:30:00Z") is False
