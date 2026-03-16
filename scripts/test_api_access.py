#!/usr/bin/env python3
"""
API endpoint verifier for the Stock Signal Engine.

Tests each configured API endpoint with a minimal request (limit=1, recent dates)
and prints a results table with pass/fail status. Exits 0 if all critical endpoints
work (OHLCV, News, Ticker Details), 1 otherwise.

Usage:
    python scripts/test_api_access.py
"""

import os
import sys
from datetime import date, timedelta

# Ensure the project root is on sys.path so src.* can be imported
# regardless of the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.common.api_client import FinnhubClient, PolygonClient  # noqa: E402
from src.common.config import load_env  # noqa: E402


def check_polygon_ohlcv(client: PolygonClient, test_ticker: str, from_date: str, to_date: str) -> tuple[bool, str]:
    """
    Test the Polygon OHLCV endpoint with a single-day range.

    Args:
        client: Configured PolygonClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_daily_ohlcv(test_ticker, from_date, to_date)
        if results:
            return True, f"{len(results)} bar(s) returned"
        return False, "No data returned"
    except Exception as exc:
        return False, str(exc)


def check_polygon_news(client: PolygonClient, test_ticker: str, from_date: str, to_date: str) -> tuple[bool, str]:
    """
    Test the Polygon news endpoint with a limited date range.

    Args:
        client: Configured PolygonClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_news(test_ticker, from_date, to_date, limit=1)
        return True, f"{len(results)} article(s) returned"
    except Exception as exc:
        return False, str(exc)


def check_polygon_ticker_details(client: PolygonClient, test_ticker: str) -> tuple[bool, str]:
    """
    Test the Polygon ticker details endpoint.

    Args:
        client: Configured PolygonClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        details = client.fetch_ticker_details(test_ticker)
        if details:
            name = details.get("name", "unknown")
            return True, f"name='{name}'"
        return False, "Empty response"
    except Exception as exc:
        return False, str(exc)


def check_polygon_8k_filings(client: PolygonClient, test_ticker: str, from_date: str, to_date: str) -> tuple[bool, str]:
    """
    Test the Polygon 8-K filings endpoint.

    Args:
        client: Configured PolygonClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_8k_filings(test_ticker, from_date, to_date)
        return True, f"{len(results)} filing(s) returned"
    except Exception as exc:
        return False, str(exc)


def check_polygon_dividends(client: PolygonClient, test_ticker: str) -> tuple[bool, str]:
    """
    Test the Polygon dividends endpoint.

    Args:
        client: Configured PolygonClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_dividends(test_ticker)
        return True, f"{len(results)} dividend record(s) returned"
    except Exception as exc:
        return False, str(exc)


def check_finnhub_company_news(client: FinnhubClient, test_ticker: str, from_date: str, to_date: str) -> tuple[bool, str]:
    """
    Test the Finnhub company news endpoint.

    Args:
        client: Configured FinnhubClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_company_news(test_ticker, from_date, to_date)
        return True, f"{len(results)} article(s) returned"
    except Exception as exc:
        return False, str(exc)


def check_finnhub_earnings(client: FinnhubClient, test_ticker: str, from_date: str, to_date: str) -> tuple[bool, str]:
    """
    Test the Finnhub earnings calendar endpoint.

    Args:
        client: Configured FinnhubClient instance.
        test_ticker: Ticker symbol to test with, e.g. 'AAPL'.
        from_date: Start date in 'YYYY-MM-DD' format.
        to_date: End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (success bool, detail message string).
    """
    try:
        results = client.fetch_earnings_calendar(test_ticker, from_date, to_date)
        return True, f"{len(results)} earnings record(s) returned"
    except Exception as exc:
        return False, str(exc)


def print_results_table(results: list[tuple[str, bool, str, bool]]) -> None:
    """
    Print a formatted table of API check results.

    Args:
        results: List of (endpoint_name, success, detail, is_critical) tuples.
    """
    col_width_name = max(len(row[0]) for row in results) + 2
    print()
    print(f"{'Endpoint':<{col_width_name}} {'Status':<10} {'Critical':<10} {'Detail'}")
    print("-" * (col_width_name + 50))
    for name, success, detail, is_critical in results:
        status_icon = "OK" if success else "FAIL"
        critical_label = "YES" if is_critical else "no"
        print(
            f"{name:<{col_width_name}} {status_icon:<10} {critical_label:<10} {detail}"
        )
    print()


def main() -> int:
    """
    Run all API endpoint checks and report results.

    Returns:
        int: Exit code — 0 if all critical endpoints pass, 1 otherwise.
    """
    load_env()

    polygon_api_key = os.getenv("POLYGON_API_KEY", "")
    finnhub_api_key = os.getenv("FINNHUB_API_KEY", "")

    if not polygon_api_key:
        print("ERROR: POLYGON_API_KEY not set in environment", file=sys.stderr)
        return 1
    if not finnhub_api_key:
        print("WARNING: FINNHUB_API_KEY not set — Finnhub checks will fail")

    polygon = PolygonClient(polygon_api_key)
    finnhub = FinnhubClient(finnhub_api_key) if finnhub_api_key else None

    today = date.today()
    to_date = today.isoformat()
    from_date_week = (today - timedelta(days=7)).isoformat()
    from_date_month = (today - timedelta(days=30)).isoformat()
    test_ticker = "AAPL"

    print(f"Testing API access using ticker={test_ticker} ...")

    results: list[tuple[str, bool, str, bool]] = []

    ok, detail = check_polygon_ohlcv(polygon, test_ticker, from_date_week, to_date)
    results.append(("Polygon: OHLCV", ok, detail, True))

    ok, detail = check_polygon_news(polygon, test_ticker, from_date_week, to_date)
    results.append(("Polygon: News", ok, detail, True))

    ok, detail = check_polygon_ticker_details(polygon, test_ticker)
    results.append(("Polygon: Ticker Details", ok, detail, True))

    ok, detail = check_polygon_8k_filings(polygon, test_ticker, from_date_month, to_date)
    results.append(("Polygon: 8-K Filings", ok, detail, False))

    ok, detail = check_polygon_dividends(polygon, test_ticker)
    results.append(("Polygon: Dividends", ok, detail, False))

    if finnhub:
        ok, detail = check_finnhub_company_news(finnhub, test_ticker, from_date_week, to_date)
        results.append(("Finnhub: Company News", ok, detail, False))

        ok, detail = check_finnhub_earnings(finnhub, test_ticker, from_date_month, to_date)
        results.append(("Finnhub: Earnings Calendar", ok, detail, False))
    else:
        results.append(("Finnhub: Company News", False, "API key not set", False))
        results.append(("Finnhub: Earnings Calendar", False, "API key not set", False))

    print_results_table(results)

    critical_failures = [name for name, ok, _, is_critical in results if is_critical and not ok]
    if critical_failures:
        print(f"CRITICAL failures: {', '.join(critical_failures)}")
        return 1

    all_passed = all(ok for _, ok, _, _ in results)
    if all_passed:
        print("All endpoints OK.")
    else:
        non_critical_failures = [name for name, ok, _, is_critical in results if not is_critical and not ok]
        print(f"Non-critical failures: {', '.join(non_critical_failures)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
