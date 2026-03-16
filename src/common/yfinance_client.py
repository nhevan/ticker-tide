"""
yfinance client for fetching fundamental data, earnings dates, and VIX data.

Used as a fallback since Polygon's financials endpoints are not available
on our plan. No API key needed.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
import yfinance

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    """
    Convert a value to float, returning None if it is NaN, None, or unconvertible.

    Used to sanitize DataFrame cell values before storing them in result dicts,
    preventing NaN values from propagating into downstream data pipelines.

    Args:
        value: Any value to attempt converting to float.

    Returns:
        float | None: The float value, or None if the value is NaN, None,
            or raises an exception during conversion.
    """
    if value is None:
        return None
    try:
        float_value = float(value)
        if math.isnan(float_value):
            return None
        return float_value
    except (TypeError, ValueError):
        return None


def fetch_fundamentals(ticker: str) -> dict:
    """
    Fetch the most recent quarterly fundamental data for a ticker.

    Retrieves financial metrics from yfinance including income statement,
    balance sheet, and basic valuation ratios. Uses the most recent available
    quarterly data. Computes revenue_growth_yoy as year-over-year change.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        dict: Fundamental data with keys:
            revenue, net_income, eps, debt_to_equity,
            pe_ratio, pb_ratio, ps_ratio, roa, roe,
            free_cash_flow, market_cap, dividend_yield,
            revenue_growth_yoy.
            Any unavailable field is None. Returns {} on any exception.
    """
    try:
        yf_ticker = yfinance.Ticker(ticker)
        result: dict = {}

        # --- Income statement ---
        income_stmt = yf_ticker.quarterly_income_stmt
        result["revenue"] = _extract_income_field(income_stmt, "Total Revenue")
        result["net_income"] = _extract_income_field(income_stmt, "Net Income")
        result["eps"] = _extract_income_field(income_stmt, "Basic EPS")

        # Revenue growth YoY: compare most recent quarter vs same quarter ~1 year ago
        result["revenue_growth_yoy"] = _compute_revenue_growth_yoy(income_stmt)

        # --- Balance sheet ---
        balance_sheet = yf_ticker.quarterly_balance_sheet
        result["debt_to_equity"] = _compute_debt_to_equity(balance_sheet)

        # --- .info fields ---
        info = yf_ticker.info or {}
        result["pe_ratio"] = _safe_float(info.get("trailingPE"))
        result["pb_ratio"] = _safe_float(info.get("priceToBook"))
        result["ps_ratio"] = _safe_float(info.get("priceToSalesTrailing12Months"))
        result["roa"] = _safe_float(info.get("returnOnAssets"))
        result["roe"] = _safe_float(info.get("returnOnEquity"))
        result["free_cash_flow"] = _safe_float(info.get("freeCashflow"))
        result["market_cap"] = _safe_float(info.get("marketCap"))
        result["dividend_yield"] = _safe_float(info.get("dividendYield"))

        logger.info(f"Fetched fundamentals for ticker={ticker}")
        return result

    except Exception as exc:
        logger.warning(f"Failed to fetch fundamentals for ticker={ticker}: {exc!r}")
        return {}


def _extract_income_field(income_stmt: pd.DataFrame, field_name: str) -> float | None:
    """
    Extract the most recent quarter's value for a named row in an income statement.

    Args:
        income_stmt: quarterly_income_stmt DataFrame from yfinance (rows=fields, cols=dates).
        field_name: The row label to extract (e.g. 'Total Revenue', 'Net Income').

    Returns:
        float | None: The most recent quarter's value, or None if not available.
    """
    if income_stmt is None or income_stmt.empty:
        return None
    if field_name not in income_stmt.index:
        return None
    row = income_stmt.loc[field_name]
    if row.empty:
        return None
    # Most recent quarter is first column
    return _safe_float(row.iloc[0])


def _compute_revenue_growth_yoy(income_stmt: pd.DataFrame) -> float | None:
    """
    Compute year-over-year revenue growth from quarterly income statement data.

    Compares the most recent quarter's revenue to the same quarter one year ago
    (approximately 4 quarters back).

    Args:
        income_stmt: quarterly_income_stmt DataFrame from yfinance.

    Returns:
        float | None: Revenue growth as a fraction (e.g. 0.12 for 12% growth).
            Returns None if insufficient data is available.
    """
    if income_stmt is None or income_stmt.empty:
        return None
    if "Total Revenue" not in income_stmt.index:
        return None

    revenue_row = income_stmt.loc["Total Revenue"]
    if len(revenue_row) < 5:
        return None

    current_revenue = _safe_float(revenue_row.iloc[0])
    prior_year_revenue = _safe_float(revenue_row.iloc[4])

    if current_revenue is None or prior_year_revenue is None:
        return None
    if prior_year_revenue == 0 or prior_year_revenue < 0:
        return None

    return (current_revenue - prior_year_revenue) / prior_year_revenue


def _compute_debt_to_equity(balance_sheet: pd.DataFrame) -> float | None:
    """
    Compute debt-to-equity ratio from the most recent quarterly balance sheet.

    Args:
        balance_sheet: quarterly_balance_sheet DataFrame from yfinance.

    Returns:
        float | None: Total Debt / Stockholders Equity for the most recent quarter,
            or None if either field is unavailable or equity is zero.
    """
    if balance_sheet is None or balance_sheet.empty:
        return None

    total_debt = None
    equity = None

    if "Total Debt" in balance_sheet.index:
        total_debt = _safe_float(balance_sheet.loc["Total Debt"].iloc[0])

    if "Stockholders Equity" in balance_sheet.index:
        equity = _safe_float(balance_sheet.loc["Stockholders Equity"].iloc[0])

    if total_debt is None or equity is None or equity == 0:
        return None

    return total_debt / equity


def fetch_fundamentals_history(ticker: str, lookback_years: int = 5) -> list[dict]:
    """
    Fetch historical quarterly fundamental records for a ticker.

    Returns one record per available quarter, up to lookback_years * 4 quarters.
    Each record includes the report date, fiscal period label, revenue, net income,
    and EPS.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.
        lookback_years: Number of years of quarterly history to return.
            Defaults to 5 (up to 20 quarters).

    Returns:
        list[dict]: List of quarterly records, each with keys:
            report_date (ISO string), period (e.g. 'Q1'), revenue, net_income, eps.
            Returns [] on any exception.
    """
    try:
        yf_ticker = yfinance.Ticker(ticker)
        income_stmt = yf_ticker.quarterly_income_stmt

        if income_stmt is None or income_stmt.empty:
            return []

        max_quarters = lookback_years * 4
        records: list[dict] = []

        for col_index, date_col in enumerate(income_stmt.columns[:max_quarters]):
            report_date = date_col.strftime("%Y-%m-%d") if hasattr(date_col, "strftime") else str(date_col)

            revenue = None
            if "Total Revenue" in income_stmt.index:
                revenue = _safe_float(income_stmt.loc["Total Revenue", date_col])

            net_income = None
            if "Net Income" in income_stmt.index:
                net_income = _safe_float(income_stmt.loc["Net Income", date_col])

            eps = None
            if "Basic EPS" in income_stmt.index:
                eps = _safe_float(income_stmt.loc["Basic EPS", date_col])

            # Derive a period label from the month (Q1=Jan-Mar, Q2=Apr-Jun, etc.)
            month = date_col.month if hasattr(date_col, "month") else 1
            period = f"Q{(month - 1) // 3 + 1}"

            records.append({
                "report_date": report_date,
                "period": period,
                "revenue": revenue,
                "net_income": net_income,
                "eps": eps,
            })

        logger.info(
            f"Fetched {len(records)} quarterly records for ticker={ticker}"
        )
        return records

    except Exception as exc:
        logger.warning(
            f"Failed to fetch fundamental history for ticker={ticker}: {exc!r}"
        )
        return []


def fetch_vix_data(from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch historical VIX (CBOE Volatility Index) data for a date range.

    Downloads VIX price data using yfinance and returns it as a clean DataFrame
    with standardized lowercase column names.

    Args:
        from_date: Start date in 'YYYY-MM-DD' format (inclusive).
        to_date: End date in 'YYYY-MM-DD' format (inclusive).

    Returns:
        pd.DataFrame: DataFrame with columns: date, open, high, low, close, volume.
            Returns an empty DataFrame with those same columns on any exception.
    """
    empty_df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    try:
        raw_df = yfinance.download("^VIX", start=from_date, end=to_date, auto_adjust=True)

        if raw_df is None or raw_df.empty:
            return empty_df

        # Flatten MultiIndex columns if present (yfinance sometimes returns them)
        if isinstance(raw_df.columns, pd.MultiIndex):
            raw_df.columns = raw_df.columns.get_level_values(0)

        # Rename to lowercase standard names
        rename_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
        raw_df = raw_df.rename(columns=rename_map)

        result_df = raw_df[["open", "high", "low", "close", "volume"]].copy()
        result_df.insert(0, "date", result_df.index.strftime("%Y-%m-%d"))
        result_df = result_df.reset_index(drop=True)

        logger.info(
            f"Fetched {len(result_df)} VIX rows from={from_date} to={to_date}"
        )
        return result_df

    except Exception as exc:
        logger.warning(f"Failed to fetch VIX data from={from_date} to={to_date}: {exc!r}")
        return empty_df


def fetch_ticker_info(ticker: str) -> dict:
    """
    Fetch static company and valuation information for a ticker.

    Retrieves a subset of yfinance's .info dict containing sector, industry,
    and key valuation and financial metrics.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        dict: Info dict with keys: sector, industry, marketCap, trailingPE,
            forwardPE, priceToBook, dividendYield, returnOnEquity, returnOnAssets.
            Any unavailable field is None. Returns {} on any exception.
    """
    try:
        yf_ticker = yfinance.Ticker(ticker)
        info = yf_ticker.info or {}

        result = {
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": _safe_float(info.get("marketCap")),
            "trailing_pe": _safe_float(info.get("trailingPE")),
            "forward_pe": _safe_float(info.get("forwardPE")),
            "price_to_book": _safe_float(info.get("priceToBook")),
            "dividend_yield": _safe_float(info.get("dividendYield")),
            "return_on_equity": _safe_float(info.get("returnOnEquity")),
            "return_on_assets": _safe_float(info.get("returnOnAssets")),
        }
        logger.info(f"Fetched ticker info for ticker={ticker}")
        return result

    except Exception as exc:
        logger.warning(f"Failed to fetch ticker info for ticker={ticker}: {exc!r}")
        return {}


def fetch_earnings_dates(ticker: str) -> list[dict]:
    """
    Fetch upcoming and recent earnings dates for a ticker.

    Retrieves earnings date history including EPS estimates, reported EPS,
    and surprise percentages from yfinance.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns:
        list[dict]: List of earnings event dicts, each with keys:
            earnings_date (ISO string), eps_estimate, reported_eps, surprise_pct.
            NaN values are converted to None. Returns [] on any exception.
    """
    try:
        yf_ticker = yfinance.Ticker(ticker)
        earnings_df = yf_ticker.get_earnings_dates(limit=20)

        if earnings_df is None or earnings_df.empty:
            return []

        records: list[dict] = []
        for date_index, row in earnings_df.iterrows():
            earnings_date = (
                date_index.strftime("%Y-%m-%d")
                if hasattr(date_index, "strftime")
                else str(date_index)
            )
            records.append({
                "earnings_date": earnings_date,
                "eps_estimate": _safe_float(row.get("EPS Estimate")),
                "reported_eps": _safe_float(row.get("Reported EPS")),
                "surprise_pct": _safe_float(row.get("Surprise(%)")),
            })

        logger.info(f"Fetched {len(records)} earnings dates for ticker={ticker}")
        return records

    except Exception as exc:
        logger.warning(f"Failed to fetch earnings dates for ticker={ticker}: {exc!r}")
        return []
