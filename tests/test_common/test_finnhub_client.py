"""
Tests for FinnhubClient in src/common/api_client.py.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.common.api_client import FinnhubClient


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def test_finnhub_client_init() -> None:
    """FinnhubClient should store api_key and delay_seconds, and create a finnhub.Client instance."""
    with patch("finnhub.Client") as mock_finnhub_class:
        client = FinnhubClient(api_key="test_key", delay_seconds=1.0)

    assert client.api_key == "test_key"
    assert client.delay_seconds == 1.0
    mock_finnhub_class.assert_called_once_with(api_key="test_key")


# ---------------------------------------------------------------------------
# fetch_earnings_calendar
# ---------------------------------------------------------------------------

def test_finnhub_fetch_earnings_calendar() -> None:
    """fetch_earnings_calendar should call the finnhub earnings_calendar method and return its list."""
    sample_earnings = [
        {
            "symbol": "AAPL",
            "date": "2024-02-01",
            "epsActual": 2.18,
            "epsEstimate": 2.10,
            "revenueActual": 119580000000,
            "revenueEstimate": 117910000000,
        }
    ]

    with patch("finnhub.Client") as mock_finnhub_class:
        mock_fh_instance = MagicMock()
        mock_fh_instance.earnings_calendar.return_value = {
            "earningsCalendar": sample_earnings
        }
        mock_finnhub_class.return_value = mock_fh_instance

        client = FinnhubClient(api_key="test_key", delay_seconds=0.0)
        result = client.fetch_earnings_calendar("AAPL", "2024-01-01", "2024-12-31")

    assert result == sample_earnings
    mock_fh_instance.earnings_calendar.assert_called_once_with(
        _from="2024-01-01", to="2024-12-31", symbol="AAPL"
    )


# ---------------------------------------------------------------------------
# fetch_company_news
# ---------------------------------------------------------------------------

def test_finnhub_fetch_company_news() -> None:
    """fetch_company_news should call finnhub company_news and return the list directly."""
    sample_news = [
        {
            "headline": "Apple Reports Record Revenue",
            "summary": "Apple Inc reported record quarterly revenue.",
            "url": "https://example.com/article",
            "datetime": 1704844800,
        }
    ]

    with patch("finnhub.Client") as mock_finnhub_class:
        mock_fh_instance = MagicMock()
        mock_fh_instance.company_news.return_value = sample_news
        mock_finnhub_class.return_value = mock_fh_instance

        client = FinnhubClient(api_key="test_key", delay_seconds=0.0)
        result = client.fetch_company_news("AAPL", "2024-01-01", "2024-03-01")

    assert result == sample_news
    mock_fh_instance.company_news.assert_called_once_with(
        "AAPL", _from="2024-01-01", to="2024-03-01"
    )


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

def test_finnhub_rate_limiting() -> None:
    """fetch_company_news should call time.sleep when calls are made faster than delay_seconds."""
    sample_news = [{"headline": "Test", "summary": "Test", "url": "http://x.com", "datetime": 1}]

    with patch("finnhub.Client") as mock_finnhub_class:
        mock_fh_instance = MagicMock()
        mock_fh_instance.company_news.return_value = sample_news
        mock_finnhub_class.return_value = mock_fh_instance

        client = FinnhubClient(api_key="test_key", delay_seconds=1.0)

        # Patch time.time to return the same value twice in a row (no time has passed),
        # so the rate limiter determines sleep IS needed.
        with patch("time.time", side_effect=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]):
            with patch("time.sleep") as mock_sleep:
                client.fetch_company_news("AAPL", "2024-01-01", "2024-03-01")
                client.fetch_company_news("MSFT", "2024-01-01", "2024-03-01")

    mock_sleep.assert_called()
    sleep_duration = mock_sleep.call_args[0][0]
    assert sleep_duration >= 1.0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_finnhub_error_handling() -> None:
    """fetch_company_news should return empty list when finnhub raises an exception."""
    with patch("finnhub.Client") as mock_finnhub_class:
        mock_fh_instance = MagicMock()
        mock_fh_instance.company_news.side_effect = Exception("API error")
        mock_finnhub_class.return_value = mock_fh_instance

        client = FinnhubClient(api_key="test_key", delay_seconds=0.0)
        result = client.fetch_company_news("AAPL", "2024-01-01", "2024-03-01")

    assert result == []
