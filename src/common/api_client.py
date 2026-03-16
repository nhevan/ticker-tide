"""
API clients for Polygon.io and Finnhub.

Polygon: Paid tier (Starter) — unlimited calls, no rate limiting needed.
Finnhub: Free tier — 60 calls/min, must enforce delays.
"""

import logging
import time
from typing import Any

import finnhub
import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


def _is_retryable_http_error(exc: Exception) -> bool:
    """
    Determine whether an exception is a retryable HTTP error.

    Retryable conditions:
    - HTTPStatusError with status code in {429, 500, 502, 503, 504}
    - httpx.ConnectError (network connectivity failure)
    - httpx.TimeoutException (request timed out)

    Args:
        exc: The exception to evaluate.

    Returns:
        bool: True if the error is retryable, False otherwise.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException)):
        return True
    return False


class PolygonClient:
    """
    HTTP client for the Polygon.io REST API.

    Handles authentication, pagination, and retry logic with exponential
    backoff for transient server errors. Designed for Polygon's Starter
    tier which has no per-minute rate limiting.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        rate_limited: bool = False,
    ) -> None:
        """
        Initialize the Polygon API client.

        Args:
            api_key: Polygon.io API key for authentication.
            base_url: Base URL for all API calls. Defaults to the Polygon production URL.
            rate_limited: If True, adds a small delay between requests. Set False for
                Polygon Starter tier which has no rate limiting.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.rate_limited = rate_limited
        self.client = httpx.Client()
        self.logger = logging.getLogger(__name__)

    def close(self) -> None:
        """Close the underlying httpx.Client, releasing connection pool resources."""
        self.client.close()

    def __enter__(self) -> "PolygonClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _execute_request(self, url: str, params: dict) -> dict:
        """
        Execute a single GET request with retry logic for transient errors.

        Decorated with tenacity retry: up to 3 attempts, exponential backoff
        between 1 and 10 seconds, only for retryable HTTP errors.

        Args:
            url: Full URL for the GET request.
            params: Query parameters to include in the request.

        Returns:
            dict: Parsed JSON response body.

        Raises:
            httpx.HTTPStatusError: If the response has a non-2xx status code
                that is not retryable, or if all retries are exhausted.
            httpx.ConnectError: If the connection cannot be established after retries.
        """
        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    # Apply retry decorator after method definition via a wrapper approach.
    # We use a separate decorated helper to avoid issues with 'self' and tenacity.

    def _get_retrying_execute(self):
        """
        Return _execute_request wrapped with tenacity retry logic.

        Retries up to 3 times with exponential backoff (1–10s) for retryable
        HTTP errors. Used by both _make_request and _follow_pagination so retry
        behaviour is consistent across initial and paginated requests.

        Returns:
            Callable: A retry-wrapped version of _execute_request.
        """
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception(_is_retryable_http_error),
            reraise=True,
        )(self._execute_request)

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Build the full URL, attach the API key, and execute the request.

        Wraps _execute_request_with_retry with try/except so callers always
        receive a dict (possibly empty) rather than an exception.

        Args:
            endpoint: API endpoint path, e.g. '/v2/aggs/ticker/AAPL/range/1/day/...'.
            params: Optional query parameters dict. The apiKey is added automatically.

        Returns:
            dict: Parsed JSON response, or {} on any exception.
        """
        url = self.base_url + endpoint
        merged_params = dict(params or {})
        merged_params["apiKey"] = self.api_key

        try:
            retrying_execute = self._get_retrying_execute()
            return retrying_execute(url, merged_params)
        except Exception as exc:
            self.logger.error(
                f"Request failed for endpoint '{endpoint}': {exc!r}"
            )
            return {}

    def _follow_pagination(self, endpoint: str, params: dict) -> list:
        """
        Fetch all pages from a paginated Polygon API endpoint.

        Makes the initial request, then follows 'next_url' links until there
        are no more pages. Each page's 'results' list is concatenated.

        Args:
            endpoint: API endpoint path for the first request.
            params: Query parameters for the first request (apiKey added automatically).

        Returns:
            list: Combined results from all pages. Returns [] if initial request fails.
        """
        response = self._make_request(endpoint, params)
        all_results = list(response.get("results", []))

        next_url = response.get("next_url")
        while next_url:
            self.logger.info(f"Following pagination next_url for endpoint '{endpoint}'")
            try:
                retrying_execute = self._get_retrying_execute()
                page_data = retrying_execute(next_url, {"apiKey": self.api_key})
            except Exception as exc:
                self.logger.error(f"Pagination request failed: {exc!r}")
                break

            page_results = page_data.get("results", [])
            all_results.extend(page_results)
            next_url = page_data.get("next_url")

        return all_results

    def fetch_ohlcv(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        adjusted: bool = True,
    ) -> list[dict]:
        """
        Fetch daily OHLCV (Open, High, Low, Close, Volume) bars for a ticker.

        Retrieves one-day aggregated bars from Polygon's aggregates endpoint,
        following pagination to collect all results. Each result row contains
        Polygon's raw fields: o, h, l, c, v, vw, t, n.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.
            from_date: Start date in 'YYYY-MM-DD' format (inclusive).
            to_date: End date in 'YYYY-MM-DD' format (inclusive).
            adjusted: If True, returns split/dividend-adjusted prices. Defaults to True.

        Returns:
            list[dict]: List of daily bar dicts with keys o, h, l, c, v, vw, t, n.
                Returns [] if the request fails or no data is available.
        """
        endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
        params = {
            "adjusted": adjusted,
            "limit": 50000,
            "sort": "asc",
        }
        self.logger.info(
            f"Fetching OHLCV for ticker={ticker} from={from_date} to={to_date}"
        )
        return self._follow_pagination(endpoint, params)

    def fetch_news(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        limit: int = 1000,
    ) -> list[dict]:
        """
        Fetch news articles for a ticker within a date range.

        Retrieves news from Polygon's reference news endpoint. Each article
        includes publisher info, title, description, and sentiment insights
        (if available from Polygon's AI analysis).

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.
            from_date: Start date in 'YYYY-MM-DD' format (inclusive).
            to_date: End date in 'YYYY-MM-DD' format (inclusive).
            limit: Maximum articles per page request. Defaults to 1000.

        Returns:
            list[dict]: List of news article dicts, each including sentiment
                and sentiment_reasoning fields extracted from the insights array.
                Returns [] if the request fails.
        """
        endpoint = "/v2/reference/news"
        params = {
            "ticker": ticker,
            "published_utc.gte": from_date,
            "published_utc.lte": to_date,
            "limit": limit,
            "sort": "published_utc",
            "order": "asc",
        }
        self.logger.info(
            f"Fetching news for ticker={ticker} from={from_date} to={to_date}"
        )
        raw_articles = self._follow_pagination(endpoint, params)
        return [self._extract_sentiment(article) for article in raw_articles]

    def _extract_sentiment(self, article: dict) -> dict:
        """
        Enrich a news article dict with top-level sentiment fields.

        Extracts the first matching insight for the article's ticker from the
        'insights' array and promotes 'sentiment' and 'sentiment_reasoning'
        to top-level keys.

        Args:
            article: A raw news article dict from the Polygon API.

        Returns:
            dict: The same article dict with 'sentiment' and 'sentiment_reasoning'
                added as top-level keys (None if no insights found).
        """
        insights = article.get("insights", [])
        sentiment = None
        sentiment_reasoning = None

        if insights:
            first_insight = insights[0]
            sentiment = first_insight.get("sentiment")
            sentiment_reasoning = first_insight.get("sentiment_reasoning")

        enriched = dict(article)
        enriched["sentiment"] = sentiment
        enriched["sentiment_reasoning"] = sentiment_reasoning
        return enriched

    def fetch_ticker_details(self, ticker: str) -> dict:
        """
        Fetch detailed reference information for a ticker.

        Retrieves static company information such as name, SIC code, market
        capitalization, and description from Polygon's reference tickers endpoint.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.

        Returns:
            dict: Ticker details dict from the 'results' key of the API response.
                Returns {} if the request fails or no data is found.
        """
        endpoint = f"/v3/reference/tickers/{ticker}"
        self.logger.info(f"Fetching ticker details for ticker={ticker}")
        response = self._make_request(endpoint)
        return response.get("results", {})

    def fetch_8k_filings(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> list[dict]:
        """
        Fetch 8-K SEC filings for a ticker within a date range.

        Retrieves 8-K filings from Polygon's text filings endpoint. 8-K filings
        are material event disclosures (earnings announcements, M&A, CEO changes, etc.)

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.
            from_date: Start filing date in 'YYYY-MM-DD' format (inclusive).
            to_date: End filing date in 'YYYY-MM-DD' format (inclusive).

        Returns:
            list[dict]: List of 8-K filing dicts. Returns [] if request fails.
        """
        endpoint = "/stocks/filings/8-K/vX/text"
        params = {
            "ticker": ticker,
            "filing_date.gte": from_date,
            "filing_date.lte": to_date,
        }
        self.logger.info(
            f"Fetching 8-K filings for ticker={ticker} from={from_date} to={to_date}"
        )
        return self._follow_pagination(endpoint, params)

    def fetch_dividends(self, ticker: str) -> list[dict]:
        """
        Fetch historical dividend records for a ticker.

        Retrieves cash dividend payment history from Polygon's dividends endpoint,
        including ex-dividend dates, payment dates, and cash amounts per share.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.

        Returns:
            list[dict]: List of dividend event dicts. Returns [] if request fails.
        """
        endpoint = "/stocks/v1/dividends"
        params = {"ticker": ticker}
        self.logger.info(f"Fetching dividends for ticker={ticker}")
        return self._follow_pagination(endpoint, params)

    def fetch_splits(self, ticker: str) -> list[dict]:
        """
        Fetch historical stock split records for a ticker.

        Retrieves stock split history from Polygon's splits endpoint.
        Split data is important for calculating adjusted price series correctly.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.

        Returns:
            list[dict]: List of stock split event dicts. Returns [] if request fails.
        """
        endpoint = "/stocks/v1/splits"
        params = {"ticker": ticker}
        self.logger.info(f"Fetching splits for ticker={ticker}")
        return self._follow_pagination(endpoint, params)

    def fetch_short_interest(self, ticker: str) -> list[dict]:
        """
        Fetch short interest data for a ticker.

        Retrieves settlement-date short interest records from Polygon's
        short-interest endpoint. Short interest is a measure of bearish sentiment.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.

        Returns:
            list[dict]: List of short interest records by settlement date.
                Returns [] if request fails.
        """
        endpoint = "/stocks/v1/short-interest"
        params = {"ticker": ticker}
        self.logger.info(f"Fetching short interest for ticker={ticker}")
        return self._follow_pagination(endpoint, params)

    def fetch_treasury_yields(self, from_date: str, to_date: str) -> list[dict]:
        """
        Fetch US Treasury yield curve data for a date range.

        Retrieves daily Treasury yield rates across maturities (1-month to 30-year)
        from Polygon's Federal Reserve data endpoint.

        Args:
            from_date: Start date in 'YYYY-MM-DD' format (inclusive).
            to_date: End date in 'YYYY-MM-DD' format (inclusive).

        Returns:
            list[dict]: List of daily yield curve records. Returns [] if request fails.
        """
        endpoint = "/fed/v1/treasury-yields"
        params = {
            "date.gte": from_date,
            "date.lte": to_date,
            "limit": 50000,
            "sort": "date.asc",
        }
        self.logger.info(
            f"Fetching treasury yields from={from_date} to={to_date}"
        )
        return self._follow_pagination(endpoint, params)

    def fetch_market_holidays(self) -> list:
        """
        Fetch upcoming market holidays and trading status.

        Retrieves the list of upcoming NYSE market holidays from Polygon's
        market status endpoint. Useful for skipping non-trading days in pipelines.

        Returns:
            list: List of upcoming holiday dicts, or the raw response if it is
                already a list. Returns [] if request fails.
        """
        endpoint = "/v1/marketstatus/upcoming"
        self.logger.info("Fetching upcoming market holidays")
        response = self._make_request(endpoint)
        # The /v1/marketstatus/upcoming endpoint returns a JSON array directly
        if isinstance(response, list):
            return response
        return response.get("results", [])


class FinnhubClient:
    """
    Client for the Finnhub financial data API.

    Wraps the official finnhub-python library with rate limiting to stay
    within the free tier limit of 60 calls per minute. Enforces a minimum
    delay between consecutive API calls.
    """

    def __init__(self, api_key: str, delay_seconds: float = 1.0) -> None:
        """
        Initialize the Finnhub client with rate limiting.

        Args:
            api_key: Finnhub API key for authentication.
            delay_seconds: Minimum number of seconds to wait between consecutive
                API calls. Defaults to 1.0 second (60 calls/min limit).
        """
        self.api_key = api_key
        self.delay_seconds = delay_seconds
        self.fh_client = finnhub.Client(api_key=api_key)
        self._last_call_time: float = 0.0
        self.logger = logging.getLogger(__name__)

    def _rate_limit(self) -> None:
        """
        Enforce the minimum delay between consecutive API calls.

        Checks how much time has elapsed since the last API call. If less than
        delay_seconds has passed, sleeps for the remaining duration. Updates the
        last call timestamp after sleeping.

        Returns:
            None
        """
        elapsed = time.time() - self._last_call_time
        if elapsed < self.delay_seconds:
            sleep_duration = self.delay_seconds - elapsed
            self.logger.debug(f"Rate limiting: sleeping {sleep_duration:.3f}s")
            time.sleep(sleep_duration)
        self._last_call_time = time.time()

    def fetch_earnings_calendar(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> list[dict]:
        """
        Fetch the earnings calendar for a specific ticker and date range.

        Retrieves scheduled and historical earnings dates with EPS and revenue
        estimates and actuals from Finnhub's earnings calendar endpoint.

        Args:
            ticker: Stock ticker symbol to filter results, e.g. 'AAPL'.
            from_date: Start date in 'YYYY-MM-DD' format.
            to_date: End date in 'YYYY-MM-DD' format.

        Returns:
            list[dict]: List of earnings records, each with keys:
                symbol, date, epsActual, epsEstimate, revenueActual, revenueEstimate.
                Returns [] on API error.
        """
        self._rate_limit()
        try:
            result = self.fh_client.earnings_calendar(
                _from=from_date, to=to_date, symbol=ticker
            )
            earnings_list = result.get("earningsCalendar", [])
            self.logger.info(
                f"Fetched {len(earnings_list)} earnings records "
                f"ticker={ticker} from={from_date} to={to_date}"
            )
            return earnings_list
        except Exception as exc:
            self.logger.error(
                f"Failed to fetch earnings calendar ticker={ticker} from={from_date} to={to_date}: {exc!r}"
            )
            return []

    def fetch_company_news(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> list[dict]:
        """
        Fetch company news articles for a ticker within a date range.

        Retrieves news headlines and summaries from Finnhub's company news
        endpoint. Useful as a supplement to Polygon news data.

        Args:
            ticker: Stock ticker symbol, e.g. 'AAPL'.
            from_date: Start date in 'YYYY-MM-DD' format (inclusive).
            to_date: End date in 'YYYY-MM-DD' format (inclusive).

        Returns:
            list[dict]: List of news article dicts, each with keys:
                headline, summary, url, datetime. Returns [] on API error.
        """
        self._rate_limit()
        try:
            result = self.fh_client.company_news(ticker, _from=from_date, to=to_date)
            self.logger.info(
                f"Fetched {len(result)} news articles for ticker={ticker} "
                f"from={from_date} to={to_date}"
            )
            return result
        except Exception as exc:
            self.logger.error(
                f"Failed to fetch company news for ticker={ticker}: {exc!r}"
            )
            return []
