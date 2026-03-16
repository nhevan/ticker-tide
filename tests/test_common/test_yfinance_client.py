"""
Tests for src/common/yfinance_client.py — yfinance-based data fetchers.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.common.yfinance_client import (
    fetch_earnings_dates,
    fetch_fundamentals,
    fetch_vix_data,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_income_stmt(revenue: float = 100_000_000_000, net_income: float = 25_000_000_000, eps: float = 1.62) -> pd.DataFrame:
    """
    Build a minimal quarterly_income_stmt DataFrame mimicking yfinance structure.

    yfinance returns quarterly_income_stmt with financial field names as the row index
    and quarter-end dates as columns. Each cell is the value for that field in that quarter.
    """
    dates = pd.to_datetime(["2024-09-30", "2024-06-30", "2023-09-30", "2023-06-30"])
    # Rows = field names (index), columns = quarter dates
    data = {
        dates[0]: {"Total Revenue": revenue, "Net Income": net_income, "Basic EPS": eps},
        dates[1]: {"Total Revenue": revenue * 0.95, "Net Income": net_income * 0.95, "Basic EPS": eps * 0.95},
        dates[2]: {"Total Revenue": revenue * 0.90, "Net Income": net_income * 0.90, "Basic EPS": eps * 0.90},
        dates[3]: {"Total Revenue": revenue * 0.85, "Net Income": net_income * 0.85, "Basic EPS": eps * 0.85},
    }
    return pd.DataFrame(data)


def _make_balance_sheet(total_debt: float = 50_000_000_000, equity: float = 60_000_000_000) -> pd.DataFrame:
    """
    Build a minimal quarterly_balance_sheet DataFrame mimicking yfinance structure.

    yfinance returns quarterly_balance_sheet with field names as row index
    and quarter-end dates as columns.
    """
    dates = pd.to_datetime(["2024-09-30", "2024-06-30"])
    data = {
        dates[0]: {"Total Debt": total_debt, "Stockholders Equity": equity},
        dates[1]: {"Total Debt": total_debt * 0.98, "Stockholders Equity": equity * 0.97},
    }
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# fetch_fundamentals — income statement fields
# ---------------------------------------------------------------------------

def test_fetch_fundamentals_income_statement() -> None:
    """fetch_fundamentals should extract revenue, net_income, eps from quarterly_income_stmt."""
    mock_ticker = MagicMock()
    mock_ticker.quarterly_income_stmt = _make_income_stmt(
        revenue=100_000_000_000,
        net_income=25_000_000_000,
        eps=1.62,
    )
    mock_ticker.quarterly_balance_sheet = _make_balance_sheet()
    mock_ticker.info = {}

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_fundamentals("AAPL")

    assert "revenue" in result
    assert result["revenue"] is not None
    assert result["revenue"] > 0

    assert "net_income" in result
    assert result["net_income"] is not None

    assert "eps" in result
    assert result["eps"] is not None


# ---------------------------------------------------------------------------
# fetch_fundamentals — balance sheet fields
# ---------------------------------------------------------------------------

def test_fetch_fundamentals_balance_sheet() -> None:
    """fetch_fundamentals should compute debt_to_equity as Total Debt / Stockholders Equity."""
    total_debt = 50_000_000_000
    equity = 60_000_000_000

    mock_ticker = MagicMock()
    mock_ticker.quarterly_income_stmt = _make_income_stmt()
    mock_ticker.quarterly_balance_sheet = _make_balance_sheet(total_debt=total_debt, equity=equity)
    mock_ticker.info = {}

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_fundamentals("AAPL")

    assert "debt_to_equity" in result
    assert result["debt_to_equity"] is not None
    expected_dte = total_debt / equity
    assert abs(result["debt_to_equity"] - expected_dte) < 1e-6


# ---------------------------------------------------------------------------
# fetch_fundamentals — .info fields
# ---------------------------------------------------------------------------

def test_fetch_fundamentals_info() -> None:
    """fetch_fundamentals should extract pe_ratio, market_cap, and dividend_yield from .info."""
    mock_ticker = MagicMock()
    mock_ticker.quarterly_income_stmt = _make_income_stmt()
    mock_ticker.quarterly_balance_sheet = _make_balance_sheet()
    mock_ticker.info = {
        "trailingPE": 28.5,
        "marketCap": 2_800_000_000_000,
        "dividendYield": 0.005,
    }

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_fundamentals("AAPL")

    assert result.get("pe_ratio") == 28.5
    assert result.get("market_cap") == 2_800_000_000_000
    assert result.get("dividend_yield") == 0.005


# ---------------------------------------------------------------------------
# fetch_fundamentals — NaN handling
# ---------------------------------------------------------------------------

def test_fetch_fundamentals_handles_missing_fields() -> None:
    """fetch_fundamentals should convert NaN values to None (no crash, no zero)."""
    dates = pd.to_datetime(["2024-09-30", "2024-06-30"])
    income_df = pd.DataFrame(
        {
            dates[0]: {"Total Revenue": np.nan, "Net Income": np.nan, "Basic EPS": np.nan},
            dates[1]: {"Total Revenue": np.nan, "Net Income": np.nan, "Basic EPS": np.nan},
        }
    )
    balance_df = pd.DataFrame(
        {
            dates[0]: {"Total Debt": np.nan, "Stockholders Equity": np.nan},
            dates[1]: {"Total Debt": np.nan, "Stockholders Equity": np.nan},
        }
    )

    mock_ticker = MagicMock()
    mock_ticker.quarterly_income_stmt = income_df
    mock_ticker.quarterly_balance_sheet = balance_df
    mock_ticker.info = {}

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_fundamentals("AAPL")

    # Should not raise; fields with NaN should come back as None
    assert result.get("revenue") is None
    assert result.get("net_income") is None
    assert result.get("eps") is None


# ---------------------------------------------------------------------------
# fetch_vix_data
# ---------------------------------------------------------------------------

def test_fetch_vix_data() -> None:
    """fetch_vix_data should return a DataFrame with columns: date, open, high, low, close, volume."""
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    mock_df = pd.DataFrame(
        {
            "Open": [15.0, 15.5, 14.8],
            "High": [16.0, 16.2, 15.3],
            "Low": [14.5, 14.8, 14.2],
            "Close": [15.2, 15.8, 14.6],
            "Volume": [0, 0, 0],
        },
        index=dates,
    )

    with patch("yfinance.download", return_value=mock_df):
        result = fetch_vix_data("2024-01-01", "2024-03-01")

    assert isinstance(result, pd.DataFrame)
    for col in ("date", "open", "high", "low", "close", "volume"):
        assert col in result.columns, f"Missing column '{col}' in VIX DataFrame"


def test_fetch_vix_data_date_range() -> None:
    """fetch_vix_data should call yfinance.download with the specified start and end dates."""
    dates = pd.to_datetime(["2024-01-02"])
    mock_df = pd.DataFrame(
        {"Open": [15.0], "High": [16.0], "Low": [14.5], "Close": [15.2], "Volume": [0]},
        index=dates,
    )

    with patch("yfinance.download", return_value=mock_df) as mock_download:
        fetch_vix_data("2024-01-01", "2024-03-01")

    mock_download.assert_called_once()
    call_kwargs = mock_download.call_args[1]
    assert call_kwargs.get("start") == "2024-01-01"
    assert call_kwargs.get("end") == "2024-03-01"


# ---------------------------------------------------------------------------
# fetch_fundamentals — ticker not found
# ---------------------------------------------------------------------------

def test_yfinance_ticker_not_found() -> None:
    """fetch_fundamentals should return empty dict when yfinance raises an exception."""
    with patch("yfinance.Ticker", side_effect=Exception("Ticker not found")):
        result = fetch_fundamentals("INVALID_TICKER_XYZ")

    assert result == {}


# ---------------------------------------------------------------------------
# fetch_earnings_dates
# ---------------------------------------------------------------------------

def test_fetch_earnings_dates() -> None:
    """fetch_earnings_dates should return a list of dicts with earnings_date field."""
    dates_index = pd.to_datetime(["2024-02-01", "2024-05-02", "2024-08-01"])
    mock_earnings_df = pd.DataFrame(
        {
            "EPS Estimate": [2.10, 1.85, 2.05],
            "Reported EPS": [2.18, 1.90, 2.12],
            "Surprise(%)": [3.81, 2.70, 3.41],
        },
        index=dates_index,
    )

    mock_ticker = MagicMock()
    mock_ticker.get_earnings_dates.return_value = mock_earnings_df

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_earnings_dates("AAPL")

    assert isinstance(result, list)
    assert len(result) == 3
    for record in result:
        assert "earnings_date" in record, "Missing 'earnings_date' in earnings record"
