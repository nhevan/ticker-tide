"""
Tests for src/common/api_client.py — PolygonClient HTTP client.
"""

from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from src.common.api_client import PolygonClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response that returns the given json_data."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_data
    mock_resp.status_code = status_code
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _make_http_status_error(status_code: int) -> httpx.HTTPStatusError:
    """Build an httpx.HTTPStatusError with the given HTTP status code."""
    mock_request = MagicMock(spec=httpx.Request)
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = status_code
    return httpx.HTTPStatusError("HTTP error", request=mock_request, response=mock_response)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def test_polygon_client_init() -> None:
    """PolygonClient should store api_key and base_url as attributes, and pre-build _retrying_execute."""
    client = PolygonClient(api_key="test_key", base_url="https://api.polygon.io")

    assert client.api_key == "test_key"
    assert client.base_url == "https://api.polygon.io"
    assert callable(client._retrying_execute), "_retrying_execute must be a callable built at init time"


def test_polygon_retry_wrapper_is_same_object_across_requests(
    mock_polygon_ohlcv_response: dict,
) -> None:
    """_retrying_execute should be the same object for every request — not recreated per call."""
    mock_polygon_ohlcv_response["next_url"] = None
    client = PolygonClient(api_key="test_key", rate_limited=False)
    wrapper_id = id(client._retrying_execute)

    mock_resp = _make_mock_response(mock_polygon_ohlcv_response)
    with patch.object(client.client, "get", return_value=mock_resp):
        client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")
        client.fetch_ohlcv("AAPL", "2024-01-06", "2024-01-10")

    assert id(client._retrying_execute) == wrapper_id, (
        "_retrying_execute must not be recreated between requests"
    )


# ---------------------------------------------------------------------------
# fetch_ohlcv
# ---------------------------------------------------------------------------

def test_polygon_fetch_ohlcv_url_construction(mock_polygon_ohlcv_response: dict) -> None:
    """fetch_ohlcv should call the correct URL with required params including apiKey and adjusted."""
    client = PolygonClient(api_key="test_key", base_url="https://api.polygon.io", rate_limited=False)

    mock_resp = _make_mock_response(mock_polygon_ohlcv_response)
    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", call_args[0][1] if len(call_args[0]) > 1 else {})

    assert url == "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-05"
    assert params.get("apiKey") == "test_key"
    assert params.get("adjusted") is True
    assert params.get("limit") == 50000
    assert params.get("sort") == "asc"


def test_polygon_fetch_ohlcv_parses_response(mock_polygon_ohlcv_response: dict) -> None:
    """fetch_ohlcv should return a list of dicts with the raw polygon fields (o, h, l, c, v, vw, t, n)."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    mock_resp = _make_mock_response(mock_polygon_ohlcv_response)
    with patch.object(client.client, "get", return_value=mock_resp):
        results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert isinstance(results, list)
    assert len(results) == 5
    for row in results:
        for key in ("o", "h", "l", "c", "v", "vw", "t", "n"):
            assert key in row, f"Missing key '{key}' in OHLCV result row"


def test_polygon_fetch_ohlcv_empty_response() -> None:
    """fetch_ohlcv should return an empty list when the API returns no results."""
    client = PolygonClient(api_key="test_key", rate_limited=False)
    empty_response = {"results": [], "status": "OK"}

    mock_resp = _make_mock_response(empty_response)
    with patch.object(client.client, "get", return_value=mock_resp):
        results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert results == []


# ---------------------------------------------------------------------------
# fetch_news
# ---------------------------------------------------------------------------

def test_polygon_fetch_news_url_construction(mock_polygon_news_response: dict) -> None:
    """fetch_news should call /v2/reference/news with correct params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    mock_resp = _make_mock_response(mock_polygon_news_response)
    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_news("AAPL", "2024-01-01", "2024-03-01")

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", call_args[0][1] if len(call_args[0]) > 1 else {})

    assert "/v2/reference/news" in url
    assert params.get("ticker") == "AAPL"
    assert params.get("published_utc.gte") == "2024-01-01"
    assert params.get("limit") == 1000


def test_polygon_fetch_news_parses_sentiment(mock_polygon_news_response: dict) -> None:
    """fetch_news should return articles that include sentiment and sentiment_reasoning fields."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    mock_resp = _make_mock_response(mock_polygon_news_response)
    with patch.object(client.client, "get", return_value=mock_resp):
        articles = client.fetch_news("AAPL", "2024-01-01", "2024-03-01")

    assert isinstance(articles, list)
    assert len(articles) == 3
    for article in articles:
        assert "sentiment" in article, "Missing 'sentiment' field in news article"
        assert "sentiment_reasoning" in article, "Missing 'sentiment_reasoning' field in news article"


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def test_polygon_pagination_follows_next_url() -> None:
    """_follow_pagination should follow next_url and combine results from both pages."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    page1_response = {
        "results": [{"o": 100, "c": 101, "h": 102, "l": 99, "v": 1000, "vw": 100.5, "t": 1, "n": 50}],
        "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/...?cursor=abc&apiKey=test_key",
        "status": "OK",
    }
    page2_response = {
        "results": [{"o": 102, "c": 103, "h": 104, "l": 101, "v": 1100, "vw": 102.5, "t": 2, "n": 55}],
        "next_url": None,
        "status": "OK",
    }

    mock_resp1 = _make_mock_response(page1_response)
    mock_resp2 = _make_mock_response(page2_response)

    with patch.object(client.client, "get", side_effect=[mock_resp1, mock_resp2]) as mock_get:
        results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-02")

    assert mock_get.call_count == 2
    assert len(results) == 2


def test_polygon_pagination_stops_without_next_url(mock_polygon_ohlcv_response: dict) -> None:
    """_follow_pagination should make only one request if the response has no next_url."""
    # Ensure next_url is None
    mock_polygon_ohlcv_response["next_url"] = None

    client = PolygonClient(api_key="test_key", rate_limited=False)
    mock_resp = _make_mock_response(mock_polygon_ohlcv_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

def test_polygon_retry_on_server_error() -> None:
    """fetch_ohlcv should retry on 500 and succeed on second call."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    success_response = {
        "results": [{"o": 100, "c": 101, "h": 102, "l": 99, "v": 1000, "vw": 100.5, "t": 1, "n": 50}],
        "next_url": None,
        "status": "OK",
    }
    mock_success = _make_mock_response(success_response)

    error = _make_http_status_error(500)

    with patch.object(client.client, "get", side_effect=[error, mock_success]) as mock_get:
        with patch("time.sleep"):
            results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert mock_get.call_count == 2
    assert len(results) == 1


def test_polygon_retry_on_rate_limit() -> None:
    """fetch_ohlcv should retry on 429 and succeed on second call."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    success_response = {
        "results": [{"o": 100, "c": 101, "h": 102, "l": 99, "v": 1000, "vw": 100.5, "t": 1, "n": 50}],
        "next_url": None,
        "status": "OK",
    }
    mock_success = _make_mock_response(success_response)

    error = _make_http_status_error(429)

    with patch.object(client.client, "get", side_effect=[error, mock_success]) as mock_get:
        with patch("time.sleep"):
            results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert mock_get.call_count == 2
    assert len(results) == 1


def test_polygon_gives_up_after_max_retries() -> None:
    """fetch_ohlcv should give up after 3 attempts and return empty list."""
    client = PolygonClient(api_key="test_key", rate_limited=False)
    error = _make_http_status_error(500)

    with patch.object(client.client, "get", side_effect=error) as mock_get:
        with patch("time.sleep"):
            results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert mock_get.call_count == 3
    assert results == []


def test_polygon_no_delay_when_not_rate_limited(mock_polygon_ohlcv_response: dict) -> None:
    """fetch_ohlcv should not call time.sleep when rate_limited=False."""
    mock_polygon_ohlcv_response["next_url"] = None
    client = PolygonClient(api_key="test_key", rate_limited=False)
    mock_resp = _make_mock_response(mock_polygon_ohlcv_response)

    with patch.object(client.client, "get", return_value=mock_resp):
        with patch("time.sleep") as mock_sleep:
            client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_ticker_details
# ---------------------------------------------------------------------------

def test_polygon_fetch_ticker_details() -> None:
    """fetch_ticker_details should call /v3/reference/tickers/AAPL and return the results dict."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    fake_details = {"ticker": "AAPL", "name": "Apple Inc.", "market": "stocks"}
    api_response = {"results": fake_details, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        result = client.fetch_ticker_details("AAPL")

    call_args = mock_get.call_args
    url = call_args[0][0]
    assert "/v3/reference/tickers/AAPL" in url
    assert result == fake_details


# ---------------------------------------------------------------------------
# fetch_dividends / splits / short_interest
# ---------------------------------------------------------------------------

def test_polygon_fetch_dividends() -> None:
    """fetch_dividends should call the dividends endpoint with ticker and limit params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {"results": [{"cash_amount": 0.24}], "next_url": None, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_dividends("AAPL")

    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", {})
    assert "/stocks/v1/dividends" in url
    assert params.get("ticker") == "AAPL"
    assert params.get("limit") == 1000


def test_polygon_fetch_dividends_custom_limit() -> None:
    """fetch_dividends should pass a custom limit when specified."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {"results": [], "next_url": None, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_dividends("AAPL", limit=500)

    params = mock_get.call_args[1].get("params", {})
    assert params.get("limit") == 500


def test_polygon_fetch_splits() -> None:
    """fetch_splits should call the splits endpoint with ticker and limit params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {"results": [{"split_from": 1, "split_to": 4}], "next_url": None, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_splits("AAPL")

    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", {})
    assert "/stocks/v1/splits" in url
    assert params.get("limit") == 1000


def test_polygon_fetch_short_interest() -> None:
    """fetch_short_interest should call the short-interest endpoint with ticker and limit params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {"results": [{"short_interest": 1000000}], "next_url": None, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_short_interest("AAPL")

    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", {})
    assert "/stocks/v1/short-interest" in url
    assert params.get("limit") == 1000


# ---------------------------------------------------------------------------
# fetch_treasury_yields
# ---------------------------------------------------------------------------

def test_polygon_fetch_treasury_yields() -> None:
    """fetch_treasury_yields should call the treasury-yields endpoint with date range params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {"results": [{"yield_10_year": 4.5}], "next_url": None, "status": "OK"}
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_treasury_yields("2024-01-01", "2024-03-01")

    call_args = mock_get.call_args
    url = call_args[0][0]
    assert "/fed/v1/treasury-yields" in url


# ---------------------------------------------------------------------------
# fetch_market_holidays
# ---------------------------------------------------------------------------

def test_polygon_fetch_market_holidays() -> None:
    """fetch_market_holidays should call /v1/marketstatus/upcoming."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = [{"date": "2024-07-04", "name": "Independence Day"}]
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_market_holidays()

    call_args = mock_get.call_args
    url = call_args[0][0]
    assert "/v1/marketstatus/upcoming" in url


# ---------------------------------------------------------------------------
# fetch_8k_filings
# ---------------------------------------------------------------------------

def test_polygon_fetch_8k_filings() -> None:
    """fetch_8k_filings should call the filings endpoint with ticker and date range params."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    api_response = {
        "results": [{"accession_number": "0001234567-24-000001"}],
        "next_url": None,
        "status": "OK",
    }
    mock_resp = _make_mock_response(api_response)

    with patch.object(client.client, "get", return_value=mock_resp) as mock_get:
        client.fetch_8k_filings("AAPL", "2024-01-01", "2024-03-01")

    call_args = mock_get.call_args
    url = call_args[0][0]
    params = call_args[1].get("params", {})
    assert "filings" in url
    assert params.get("ticker") == "AAPL"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_polygon_api_error_returns_empty_list() -> None:
    """fetch_ohlcv should return an empty list when a ConnectError is raised after all retries."""
    client = PolygonClient(api_key="test_key", rate_limited=False)

    with patch.object(client.client, "get", side_effect=httpx.ConnectError("Connection refused")):
        with patch("time.sleep"):
            results = client.fetch_ohlcv("AAPL", "2024-01-01", "2024-01-05")

    assert results == []
