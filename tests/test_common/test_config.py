"""
Tests for src/common/config.py — configuration loader and helpers.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.common.config import (
    get_active_tickers,
    get_market_benchmarks,
    get_sector_etfs,
    load_config,
    load_env,
)


def test_load_config_tickers() -> None:
    """load_config('tickers') should return dict with tickers list, sector_etfs list, and market_benchmarks dict."""
    config = load_config("tickers")

    assert "tickers" in config
    assert isinstance(config["tickers"], list)
    assert len(config["tickers"]) > 0

    assert "sector_etfs" in config
    assert isinstance(config["sector_etfs"], list)

    assert "market_benchmarks" in config
    assert isinstance(config["market_benchmarks"], dict)
    assert "spy" in config["market_benchmarks"]
    assert "qqq" in config["market_benchmarks"]
    assert "vix" in config["market_benchmarks"]


def test_load_config_backfiller() -> None:
    """load_config('backfiller') should return dict with ohlcv.lookback_years=5 and rate_limit settings."""
    config = load_config("backfiller")

    assert "ohlcv" in config
    assert config["ohlcv"]["lookback_years"] == 5

    assert "rate_limit" in config
    assert config["rate_limit"]["polygon_rate_limited"] is False


def test_load_config_calculator() -> None:
    """load_config('calculator') should return dict with indicators.ema_periods and rsi_period."""
    config = load_config("calculator")

    assert "indicators" in config
    assert config["indicators"]["ema_periods"] == [9, 21, 50]
    assert config["indicators"]["rsi_period"] == 14


def test_load_config_scorer() -> None:
    """load_config('scorer') should return adaptive_weights with trending, ranging, volatile keys each summing to 1.0."""
    config = load_config("scorer")

    assert "adaptive_weights" in config
    weights = config["adaptive_weights"]

    for regime in ("trending", "ranging", "volatile"):
        assert regime in weights, f"Missing regime key: {regime}"
        total = sum(weights[regime].values())
        assert abs(total - 1.0) < 1e-9, f"Weights for '{regime}' sum to {total}, expected 1.0"


def test_load_config_notifier() -> None:
    """load_config('notifier') should return telegram section with expected keys."""
    config = load_config("notifier")

    assert "telegram" in config
    assert config["telegram"]["confidence_threshold"] == 40
    assert "admin_chat_id" in config["telegram"]
    assert "subscriber_chat_ids" in config["telegram"]


def test_load_config_database() -> None:
    """load_config('database') should return dict with path='data/signals.db'."""
    config = load_config("database")

    assert config["path"] == "data/signals.db"


def test_load_config_missing_file_raises() -> None:
    """load_config('nonexistent') should raise FileNotFoundError with a descriptive message."""
    with pytest.raises(FileNotFoundError) as exc_info:
        load_config("nonexistent")

    error_message = str(exc_info.value)
    assert "nonexistent" in error_message


def test_get_active_tickers() -> None:
    """get_active_tickers() should return only tickers where active=True."""
    fake_tickers_config = {
        "tickers": [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": True},
            {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": True},
            {"symbol": "DEACTIVATED", "sector": "Financials", "sector_etf": "XLF", "added": "2026-01-01", "active": False},
        ],
        "sector_etfs": ["XLK", "XLF"],
        "market_benchmarks": {"spy": "SPY", "qqq": "QQQ", "vix": "^VIX"},
    }

    with patch("src.common.config.load_config", return_value=fake_tickers_config):
        active = get_active_tickers()

    symbols = [ticker["symbol"] for ticker in active]
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "DEACTIVATED" not in symbols
    assert len(active) == 2


def test_get_active_tickers_returns_correct_fields() -> None:
    """Each returned ticker from get_active_tickers() should have symbol, sector, sector_etf, added, active keys."""
    fake_tickers_config = {
        "tickers": [
            {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK", "added": "2026-01-01", "active": True},
        ],
        "sector_etfs": ["XLK"],
        "market_benchmarks": {"spy": "SPY", "qqq": "QQQ", "vix": "^VIX"},
    }

    with patch("src.common.config.load_config", return_value=fake_tickers_config):
        active = get_active_tickers()

    assert len(active) == 1
    ticker = active[0]
    for key in ("symbol", "sector", "sector_etf", "added", "active"):
        assert key in ticker, f"Missing key '{key}' in returned ticker dict"


def test_get_sector_etfs() -> None:
    """get_sector_etfs() should return the sector_etfs list from tickers.json."""
    etfs = get_sector_etfs()

    assert isinstance(etfs, list)
    assert len(etfs) > 0
    # Verify well-known sector ETFs are present
    for expected_etf in ("XLK", "XLF", "XLV"):
        assert expected_etf in etfs, f"Expected ETF '{expected_etf}' not found in sector ETFs"


def test_get_market_benchmarks() -> None:
    """get_market_benchmarks() should return dict with spy, qqq, vix keys mapping to their ticker symbols."""
    benchmarks = get_market_benchmarks()

    assert isinstance(benchmarks, dict)
    assert benchmarks["spy"] == "SPY"
    assert benchmarks["qqq"] == "QQQ"
    assert benchmarks["vix"] == "^VIX"


def test_load_env_loads_variables(tmp_path: Path) -> None:
    """load_env() with a tmp .env path should load variables into os.environ."""
    env_file = tmp_path / ".env"
    env_file.write_text("POLYGON_API_KEY=test_key_123\n")

    # Clear any existing value first
    if "POLYGON_API_KEY" in os.environ:
        del os.environ["POLYGON_API_KEY"]

    load_env(env_path=str(env_file))

    assert os.getenv("POLYGON_API_KEY") == "test_key_123"
