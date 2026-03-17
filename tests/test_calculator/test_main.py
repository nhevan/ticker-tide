"""
Tests for src/calculator/main.py

Covers the Calculator orchestrator (Phase 2b):
- run_calculator: top-level entry point with pre-flight checks, event gating,
  per-ticker processing, ETF handling, pipeline event writing, and Telegram progress
- run_calculator_for_ticker: per-ticker module execution with dependency ordering
  and fine-grained error handling
- should_recompute_profiles: DB-based check for profile staleness

All external API calls, config loading, and sub-module imports are mocked.
The db_connection fixture from conftest.py provides a real in-memory SQLite DB.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call

import pytest


# ── Helpers ──────────────────────────────────────────────────────────────────

def _today() -> str:
    """Return today's UTC date as YYYY-MM-DD."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _days_ago(days: int) -> str:
    """Return an ISO 8601 UTC timestamp N days ago."""
    return (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()


def _insert_pipeline_event(
    conn: sqlite3.Connection, event: str, date: str, status: str
) -> None:
    """Write a pipeline_events row directly for test setup."""
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_events (event, date, status, timestamp) "
        "VALUES (?, ?, ?, ?)",
        (event, date, status, datetime.now(tz=timezone.utc).isoformat()),
    )
    conn.commit()


def _insert_indicator_profile(
    conn: sqlite3.Connection, ticker: str, computed_at: str
) -> None:
    """Insert a minimal indicator_profiles row for test setup."""
    conn.execute(
        """INSERT OR REPLACE INTO indicator_profiles
               (ticker, indicator, p5, p20, p50, p80, p95, mean, std,
                window_start, window_end, computed_at)
           VALUES (?, 'rsi_14', 30, 40, 50, 60, 70, 50, 10, '2024-01-01', '2025-01-01', ?)""",
        (ticker, computed_at),
    )
    conn.commit()


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_calc_config() -> dict:
    """Minimal calculator config dict used in tests."""
    return {
        "indicators": {
            "ema_periods": [9, 21, 50],
            "macd": {"fast": 12, "slow": 26, "signal": 9},
            "adx_period": 14,
            "rsi_period": 14,
            "stochastic": {"k": 14, "d": 3, "smooth_k": 3},
            "cci_period": 20,
            "williams_r_period": 14,
            "bollinger": {"period": 20, "std_dev": 2},
            "atr_period": 14,
            "keltner_period": 20,
            "cmf_period": 20,
        },
        "swing_points": {"lookback_candles": 5},
        "support_resistance": {"price_tolerance_pct": 1.5, "min_touches": 2},
        "patterns": {},
        "divergences": {
            "indicators": ["rsi", "macd_histogram", "obv", "stochastic"],
            "min_swing_distance_days": 5,
            "max_swing_distance_days": 60,
        },
        "gaps": {"volume_breakaway_threshold": 2.0, "volume_average_period": 20},
        "profiles": {
            "rolling_window_days": 504,
            "recompute_frequency": "weekly",
            "blend_alpha_max": 0.85,
            "blend_alpha_denominator": 756,
        },
        "weekly": {"week_start_day": "Monday"},
        "relative_strength": {"period_days": 20},
    }


@pytest.fixture
def three_tickers() -> list[dict]:
    """Three ticker config dicts for orchestrator-level tests."""
    return [
        {"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK"},
        {"symbol": "MSFT", "sector": "Technology", "sector_etf": "XLK"},
        {"symbol": "JPM", "sector": "Financials", "sector_etf": "XLF"},
    ]


def _patch_run_calculator_deps(mocker, db_connection, tickers, calc_config):
    """
    Apply all patches needed for run_calculator() tests.

    Returns a namespace object with all mock references for assertion.
    """
    ns = MagicMock()
    ns.load_env = mocker.patch("src.calculator.main.load_env")
    def _load_config_side_effect(name: str) -> dict:
        if name == "database":
            return {"path": ":memory:"}
        return calc_config

    ns.load_config = mocker.patch(
        "src.calculator.main.load_config", side_effect=_load_config_side_effect
    )
    ns.get_active_tickers = mocker.patch(
        "src.calculator.main.get_active_tickers", return_value=tickers
    )
    ns.get_sector_etfs = mocker.patch(
        "src.calculator.main.get_sector_etfs", return_value=["XLK", "XLF", "SPY"]
    )
    ns.get_market_benchmarks = mocker.patch(
        "src.calculator.main.get_market_benchmarks",
        return_value={"spy": "SPY", "qqq": "QQQ"},
    )
    ns.get_connection = mocker.patch(
        "src.calculator.main.get_connection", return_value=db_connection
    )
    ns.create_all_tables = mocker.patch("src.calculator.main.create_all_tables")
    ns.compute_indicators = mocker.patch(
        "src.calculator.main.compute_indicators_for_ticker", return_value=100
    )
    ns.detect_crossovers = mocker.patch(
        "src.calculator.main.detect_crossovers_for_ticker", return_value=5
    )
    ns.detect_gaps = mocker.patch(
        "src.calculator.main.detect_gaps_for_ticker", return_value=3
    )
    ns.detect_swing_points = mocker.patch(
        "src.calculator.main.detect_swing_points_for_ticker", return_value=10
    )
    ns.detect_support_resistance = mocker.patch(
        "src.calculator.main.detect_support_resistance_for_ticker", return_value=4
    )
    ns.detect_patterns = mocker.patch(
        "src.calculator.main.detect_all_patterns_for_ticker",
        return_value={"candlestick_count": 2, "structural_count": 1},
    )
    ns.detect_divergences = mocker.patch(
        "src.calculator.main.detect_divergences_for_ticker", return_value=2
    )
    ns.compute_profile = mocker.patch(
        "src.calculator.main.compute_profile_for_ticker", return_value=15
    )
    ns.compute_all_profiles = mocker.patch(
        "src.calculator.main.compute_all_profiles"
    )
    ns.compute_weekly = mocker.patch(
        "src.calculator.main.compute_weekly_for_ticker", return_value=52
    )
    ns.aggregate_news = mocker.patch(
        "src.calculator.main.aggregate_news_for_ticker", return_value=7
    )
    ns.send_telegram = mocker.patch(
        "src.calculator.main.send_telegram_message", return_value=1001
    )
    ns.edit_telegram = mocker.patch(
        "src.calculator.main.edit_telegram_message", return_value=True
    )
    return ns


# ── Tests: should_recompute_profiles ─────────────────────────────────────────

class TestShouldRecomputeProfiles:
    """Tests for should_recompute_profiles."""

    def test_returns_true_when_no_profiles_exist(
        self, db_connection: sqlite3.Connection, sample_calc_config: dict
    ) -> None:
        """Returns True when indicator_profiles has no rows for this ticker."""
        from src.calculator.main import should_recompute_profiles

        result = should_recompute_profiles(db_connection, "AAPL", sample_calc_config)

        assert result is True

    def test_returns_true_when_profile_older_than_7_days(
        self, db_connection: sqlite3.Connection, sample_calc_config: dict
    ) -> None:
        """Returns True when the most recent computed_at is older than 7 days."""
        from src.calculator.main import should_recompute_profiles

        old_computed_at = _days_ago(8)
        _insert_indicator_profile(db_connection, "AAPL", old_computed_at)

        result = should_recompute_profiles(db_connection, "AAPL", sample_calc_config)

        assert result is True

    def test_returns_false_when_profile_is_recent(
        self, db_connection: sqlite3.Connection, sample_calc_config: dict
    ) -> None:
        """Returns False when the most recent computed_at is within 7 days."""
        from src.calculator.main import should_recompute_profiles

        recent_computed_at = _days_ago(3)
        _insert_indicator_profile(db_connection, "AAPL", recent_computed_at)

        result = should_recompute_profiles(db_connection, "AAPL", sample_calc_config)

        assert result is False

    def test_returns_true_exactly_at_7_day_boundary(
        self, db_connection: sqlite3.Connection, sample_calc_config: dict
    ) -> None:
        """Returns True when computed_at is exactly 7 days ago (boundary is exclusive)."""
        from src.calculator.main import should_recompute_profiles

        boundary_computed_at = _days_ago(7)
        _insert_indicator_profile(db_connection, "AAPL", boundary_computed_at)

        result = should_recompute_profiles(db_connection, "AAPL", sample_calc_config)

        assert result is True


# ── Tests: run_calculator_for_ticker ─────────────────────────────────────────

class TestRunCalculatorForTicker:
    """Tests for run_calculator_for_ticker (uses db_connection directly)."""

    def test_calls_all_modules_in_full_mode(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """All sub-modules are called once for a single ticker in full mode."""
        from src.calculator.main import run_calculator_for_ticker

        compute_indicators = mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker", return_value=100
        )
        detect_crossovers = mocker.patch(
            "src.calculator.main.detect_crossovers_for_ticker", return_value=5
        )
        detect_gaps = mocker.patch(
            "src.calculator.main.detect_gaps_for_ticker", return_value=3
        )
        detect_swing_points = mocker.patch(
            "src.calculator.main.detect_swing_points_for_ticker", return_value=10
        )
        detect_support_resistance = mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker", return_value=4
        )
        detect_patterns = mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            return_value={"candlestick_count": 2, "structural_count": 1},
        )
        detect_divergences = mocker.patch(
            "src.calculator.main.detect_divergences_for_ticker", return_value=2
        )
        compute_profile = mocker.patch(
            "src.calculator.main.compute_profile_for_ticker", return_value=15
        )
        compute_weekly = mocker.patch(
            "src.calculator.main.compute_weekly_for_ticker", return_value=52
        )
        aggregate_news = mocker.patch(
            "src.calculator.main.aggregate_news_for_ticker", return_value=7
        )

        result = run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        assert result["ticker"] == "AAPL"
        compute_indicators.assert_called_once()
        detect_crossovers.assert_called_once()
        detect_gaps.assert_called_once()
        detect_swing_points.assert_called_once()
        detect_support_resistance.assert_called_once()
        detect_patterns.assert_called_once()
        detect_divergences.assert_called_once()
        compute_profile.assert_called_once()
        compute_weekly.assert_called_once()
        aggregate_news.assert_called_once()

    def test_returns_correct_result_fields(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """Return dict has all expected keys with correct values from sub-modules."""
        from src.calculator.main import run_calculator_for_ticker

        mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker", return_value=200
        )
        mocker.patch(
            "src.calculator.main.detect_crossovers_for_ticker", return_value=6
        )
        mocker.patch("src.calculator.main.detect_gaps_for_ticker", return_value=4)
        mocker.patch(
            "src.calculator.main.detect_swing_points_for_ticker", return_value=12
        )
        mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker", return_value=5
        )
        mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            return_value={"candlestick_count": 3, "structural_count": 2},
        )
        mocker.patch(
            "src.calculator.main.detect_divergences_for_ticker", return_value=4
        )
        mocker.patch(
            "src.calculator.main.compute_profile_for_ticker", return_value=15
        )
        mocker.patch(
            "src.calculator.main.compute_weekly_for_ticker", return_value=52
        )
        mocker.patch(
            "src.calculator.main.aggregate_news_for_ticker", return_value=10
        )

        result = run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        assert result["ticker"] == "AAPL"
        assert result["indicators_rows"] == 200
        assert result["crossovers_found"] == 6
        assert result["gaps_found"] == 4
        assert result["swing_points_found"] == 12
        assert result["sr_levels_found"] == 5
        assert result["patterns"]["candlestick"] == 3
        assert result["patterns"]["structural"] == 2
        assert result["divergences_found"] == 4
        assert result["profiles_computed"] == 15
        assert result["weekly_candles"] == 52
        assert result["news_summaries"] == 10
        assert result["status"] == "success"
        assert result["errors"] == []

    def test_dependency_order_indicators_before_crossovers(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """indicators runs before crossovers, divergences, patterns, and profiles."""
        from src.calculator.main import run_calculator_for_ticker

        call_order: list[str] = []

        def record(name):
            def _mock(*args, **kwargs):
                call_order.append(name)
                if name == "patterns":
                    return {"candlestick_count": 0, "structural_count": 0}
                return 0
            return _mock

        mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker",
            side_effect=record("indicators"),
        )
        mocker.patch(
            "src.calculator.main.detect_crossovers_for_ticker",
            side_effect=record("crossovers"),
        )
        mocker.patch(
            "src.calculator.main.detect_gaps_for_ticker",
            side_effect=record("gaps"),
        )
        mocker.patch(
            "src.calculator.main.detect_swing_points_for_ticker",
            side_effect=record("swing_points"),
        )
        mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker",
            side_effect=record("support_resistance"),
        )
        mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            side_effect=record("patterns"),
        )
        mocker.patch(
            "src.calculator.main.detect_divergences_for_ticker",
            side_effect=record("divergences"),
        )
        mocker.patch(
            "src.calculator.main.compute_profile_for_ticker",
            side_effect=record("profiles"),
        )
        mocker.patch(
            "src.calculator.main.compute_weekly_for_ticker",
            side_effect=record("weekly"),
        )
        mocker.patch(
            "src.calculator.main.aggregate_news_for_ticker",
            side_effect=record("news"),
        )

        run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        # indicators must precede all indicator-dependent modules
        assert call_order.index("indicators") < call_order.index("crossovers")
        assert call_order.index("indicators") < call_order.index("divergences")
        assert call_order.index("indicators") < call_order.index("patterns")
        assert call_order.index("indicators") < call_order.index("profiles")
        # swing_points must precede support_resistance, patterns, divergences
        assert call_order.index("swing_points") < call_order.index("support_resistance")
        assert call_order.index("swing_points") < call_order.index("patterns")
        assert call_order.index("swing_points") < call_order.index("divergences")
        # support_resistance must precede patterns
        assert call_order.index("support_resistance") < call_order.index("patterns")

    def test_incremental_mode_passes_mode_to_indicators_and_weekly(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """In incremental mode, indicators and weekly receive mode='incremental'."""
        from src.calculator.main import run_calculator_for_ticker

        compute_indicators = mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker", return_value=5
        )
        compute_weekly = mocker.patch(
            "src.calculator.main.compute_weekly_for_ticker", return_value=1
        )
        mocker.patch("src.calculator.main.detect_crossovers_for_ticker", return_value=0)
        mocker.patch("src.calculator.main.detect_gaps_for_ticker", return_value=0)
        mocker.patch("src.calculator.main.detect_swing_points_for_ticker", return_value=0)
        mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker", return_value=0
        )
        mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            return_value={"candlestick_count": 0, "structural_count": 0},
        )
        mocker.patch("src.calculator.main.detect_divergences_for_ticker", return_value=0)
        mocker.patch("src.calculator.main.compute_profile_for_ticker", return_value=0)
        mocker.patch("src.calculator.main.aggregate_news_for_ticker", return_value=0)

        run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="incremental"
        )

        # Both should be called with mode="incremental"
        _, kwargs_ind = compute_indicators.call_args
        assert kwargs_ind.get("mode") == "incremental" or (
            compute_indicators.call_args[0][-1] == "incremental"
        )
        _, kwargs_wk = compute_weekly.call_args
        assert kwargs_wk.get("mode") == "incremental" or (
            compute_weekly.call_args[0][-1] == "incremental"
        )

    def test_indicators_failure_skips_all_downstream_modules(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """If indicators fails, crossovers/patterns/divergences/profiles are skipped."""
        from src.calculator.main import run_calculator_for_ticker

        mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker",
            side_effect=RuntimeError("indicators exploded"),
        )
        detect_crossovers = mocker.patch(
            "src.calculator.main.detect_crossovers_for_ticker"
        )
        detect_gaps = mocker.patch("src.calculator.main.detect_gaps_for_ticker", return_value=0)
        detect_swing_points = mocker.patch("src.calculator.main.detect_swing_points_for_ticker", return_value=0)
        detect_support_resistance = mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker"
        )
        detect_patterns = mocker.patch("src.calculator.main.detect_all_patterns_for_ticker")
        detect_divergences = mocker.patch("src.calculator.main.detect_divergences_for_ticker")
        compute_profile = mocker.patch("src.calculator.main.compute_profile_for_ticker")
        mocker.patch("src.calculator.main.compute_weekly_for_ticker", return_value=0)
        mocker.patch("src.calculator.main.aggregate_news_for_ticker", return_value=0)

        result = run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        assert result["status"] == "failed"
        assert len(result["errors"]) >= 1
        detect_crossovers.assert_not_called()
        detect_patterns.assert_not_called()
        detect_divergences.assert_not_called()
        compute_profile.assert_not_called()

    def test_divergences_failure_does_not_block_independent_modules(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """If divergences fails, profiles/weekly/news still run."""
        from src.calculator.main import run_calculator_for_ticker

        mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker", return_value=100
        )
        mocker.patch("src.calculator.main.detect_crossovers_for_ticker", return_value=5)
        mocker.patch("src.calculator.main.detect_gaps_for_ticker", return_value=3)
        mocker.patch(
            "src.calculator.main.detect_swing_points_for_ticker", return_value=10
        )
        mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker", return_value=4
        )
        mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            return_value={"candlestick_count": 2, "structural_count": 1},
        )
        mocker.patch(
            "src.calculator.main.detect_divergences_for_ticker",
            side_effect=RuntimeError("divergences failed"),
        )
        compute_profile = mocker.patch(
            "src.calculator.main.compute_profile_for_ticker", return_value=15
        )
        compute_weekly = mocker.patch(
            "src.calculator.main.compute_weekly_for_ticker", return_value=52
        )
        aggregate_news = mocker.patch(
            "src.calculator.main.aggregate_news_for_ticker", return_value=7
        )

        result = run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        assert result["status"] == "partial"
        assert len(result["errors"]) >= 1
        # Independent modules still ran
        compute_profile.assert_called_once()
        compute_weekly.assert_called_once()
        aggregate_news.assert_called_once()

    def test_swing_points_failure_skips_support_resistance_and_divergences(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """If swing_points fails, support_resistance and divergences are skipped."""
        from src.calculator.main import run_calculator_for_ticker

        mocker.patch(
            "src.calculator.main.compute_indicators_for_ticker", return_value=100
        )
        mocker.patch("src.calculator.main.detect_crossovers_for_ticker", return_value=5)
        mocker.patch("src.calculator.main.detect_gaps_for_ticker", return_value=3)
        mocker.patch(
            "src.calculator.main.detect_swing_points_for_ticker",
            side_effect=RuntimeError("swing_points failed"),
        )
        detect_support_resistance = mocker.patch(
            "src.calculator.main.detect_support_resistance_for_ticker"
        )
        detect_divergences = mocker.patch("src.calculator.main.detect_divergences_for_ticker")
        # patterns still runs (candlestick doesn't need swing points)
        mocker.patch(
            "src.calculator.main.detect_all_patterns_for_ticker",
            return_value={"candlestick_count": 2, "structural_count": 0},
        )
        mocker.patch(
            "src.calculator.main.compute_profile_for_ticker", return_value=15
        )
        mocker.patch("src.calculator.main.compute_weekly_for_ticker", return_value=52)
        mocker.patch("src.calculator.main.aggregate_news_for_ticker", return_value=7)

        result = run_calculator_for_ticker(
            db_connection, "AAPL", sample_calc_config, mode="full"
        )

        assert result["status"] == "partial"
        detect_support_resistance.assert_not_called()
        detect_divergences.assert_not_called()


# ── Tests: run_calculator (top-level orchestrator) ────────────────────────────

class TestRunCalculator:
    """Tests for run_calculator() top-level function."""

    def test_full_mode_calls_all_modules_once_per_ticker(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """In full mode with 3 tickers, each sub-module is called exactly 3 times."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        assert ns.compute_indicators.call_count >= 3
        assert ns.detect_crossovers.call_count == 3
        assert ns.detect_gaps.call_count == 3
        assert ns.detect_swing_points.call_count == 3
        assert ns.detect_support_resistance.call_count == 3
        assert ns.detect_patterns.call_count == 3
        assert ns.detect_divergences.call_count == 3
        assert ns.compute_weekly.call_count >= 3  # also called for ETFs

    def test_dependency_order_per_ticker(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """For each ticker, dependency ordering is respected."""
        from src.calculator.main import run_calculator

        tickers = [{"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK"}]
        ns = _patch_run_calculator_deps(mocker, db_connection, tickers, sample_calc_config)

        call_order: list[str] = []

        def _record(name, default_return):
            def _side(*args, **kwargs):
                call_order.append(name)
                return default_return
            return _side

        ns.compute_indicators.side_effect = _record("indicators", 100)
        ns.detect_crossovers.side_effect = _record("crossovers", 5)
        ns.detect_swing_points.side_effect = _record("swing_points", 10)
        ns.detect_support_resistance.side_effect = _record("support_resistance", 4)
        ns.detect_patterns.side_effect = _record(
            "patterns", {"candlestick_count": 2, "structural_count": 1}
        )
        ns.detect_divergences.side_effect = _record("divergences", 2)
        ns.detect_gaps.side_effect = _record("gaps", 3)

        run_calculator(mode="full")

        # Verify ordering for AAPL processing
        assert call_order.index("indicators") < call_order.index("crossovers")
        assert call_order.index("indicators") < call_order.index("divergences")
        assert call_order.index("swing_points") < call_order.index("support_resistance")
        assert call_order.index("swing_points") < call_order.index("patterns")
        assert call_order.index("support_resistance") < call_order.index("patterns")
        assert call_order.index("indicators") < call_order.index("patterns")

    def test_incremental_mode_passes_mode_to_sub_modules(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """In incremental mode, indicators and weekly are called with mode='incremental'."""
        from src.calculator.main import run_calculator

        tickers = [{"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK"}]
        # Set up fetcher_done event so incremental mode can proceed
        _insert_pipeline_event(db_connection, "fetcher_done", _today(), "completed")

        ns = _patch_run_calculator_deps(mocker, db_connection, tickers, sample_calc_config)

        run_calculator(mode="incremental")

        # compute_indicators_for_ticker called with mode="incremental"
        ind_calls = ns.compute_indicators.call_args_list
        assert len(ind_calls) >= 1
        for ind_call in ind_calls:
            args, kwargs = ind_call
            assert kwargs.get("mode") == "incremental" or (
                len(args) >= 4 and args[3] == "incremental"
            )

        # compute_weekly_for_ticker called with mode="incremental"
        wk_calls = ns.compute_weekly.call_args_list
        assert len(wk_calls) >= 1
        for wk_call in wk_calls:
            args, kwargs = wk_call
            assert kwargs.get("mode") == "incremental" or (
                len(args) >= 4 and args[3] == "incremental"
            )

    def test_continues_on_ticker_failure(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """If ticker 2 (MSFT) indicators fail, ticker 1 and ticker 3 are fully processed."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        ticker_2_symbol = three_tickers[1]["symbol"]  # "MSFT"

        def indicators_side_effect(db_conn, ticker, config, mode="full"):
            if ticker == ticker_2_symbol:
                raise RuntimeError(f"Indicators failed for {ticker}")
            return 100

        ns.compute_indicators.side_effect = indicators_side_effect

        result = run_calculator(mode="full")

        # Ticker 1 and 3 fully processed
        assert ns.detect_crossovers.call_count == 2
        # Result tracks failures
        assert result["tickers_failed"] >= 1
        assert result["tickers_processed"] >= 2
        # Alert written for ticker 2
        alerts = db_connection.execute(
            "SELECT * FROM alerts_log WHERE ticker = ?", (ticker_2_symbol,)
        ).fetchall()
        assert len(alerts) >= 1

    def test_continues_on_module_failure_for_single_ticker(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
    ) -> None:
        """If divergences fails for AAPL, profiles/weekly/news still run for AAPL."""
        from src.calculator.main import run_calculator

        tickers = [{"symbol": "AAPL", "sector": "Technology", "sector_etf": "XLK"}]
        ns = _patch_run_calculator_deps(mocker, db_connection, tickers, sample_calc_config)
        ns.detect_divergences.side_effect = RuntimeError("divergences boom")

        run_calculator(mode="full")

        # Independent modules ran despite divergences failure
        ns.compute_profile.assert_called()
        ns.compute_weekly.assert_called()
        ns.aggregate_news.assert_called()

    def test_writes_pipeline_event_completed(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """After a successful run, pipeline_events has 'calculator_done' with status='completed'."""
        from src.calculator.main import run_calculator

        _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        row = db_connection.execute(
            "SELECT status FROM pipeline_events WHERE event = 'calculator_done' AND date = ?",
            (_today(),),
        ).fetchone()
        assert row is not None
        assert row["status"] == "completed"

    def test_writes_pipeline_event_completed_on_partial_failure(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """Even if some tickers fail, pipeline_event is still written as 'completed'."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )
        ns.compute_indicators.side_effect = RuntimeError("boom for everyone")

        run_calculator(mode="full")

        row = db_connection.execute(
            "SELECT status FROM pipeline_events WHERE event = 'calculator_done' AND date = ?",
            (_today(),),
        ).fetchone()
        assert row is not None
        assert row["status"] == "completed"

    def test_logs_pipeline_run(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """After run, pipeline_runs has an entry with phase='calculator' and duration."""
        from src.calculator.main import run_calculator

        _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        row = db_connection.execute(
            "SELECT * FROM pipeline_runs WHERE phase = 'calculator' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row["phase"] == "calculator"
        assert row["duration_seconds"] is not None
        assert row["duration_seconds"] >= 0
        assert row["tickers_processed"] is not None

    def test_sends_telegram_progress_messages(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """Telegram send is called for initial message; edit is called per ticker."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )
        mocker.patch("src.calculator.main.os").getenv.side_effect = lambda key, default=None: (
            "fake_token" if key == "TELEGRAM_BOT_TOKEN"
            else "123456" if key == "TELEGRAM_CHAT_ID"
            else default
        )

        run_calculator(mode="full")

        # At minimum: initial progress message + final summary
        assert ns.send_telegram.call_count >= 2

    def test_sends_telegram_final_summary_with_stats(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """The final Telegram message contains ticker counts, duration, and module stats."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )
        mocker.patch("src.calculator.main.os").getenv.side_effect = lambda key, default=None: (
            "fake_token" if key == "TELEGRAM_BOT_TOKEN"
            else "123456" if key == "TELEGRAM_CHAT_ID"
            else default
        )

        run_calculator(mode="full")

        # Last send_telegram_message call should be the summary
        all_calls = ns.send_telegram.call_args_list
        assert len(all_calls) >= 1
        final_call_text = all_calls[-1][0][2]  # positional arg: (token, chat_id, text)
        assert "Calculator" in final_call_text or "calculator" in final_call_text.lower()
        # Should mention some numeric stats
        assert any(char.isdigit() for char in final_call_text)

    def test_skips_if_already_done_today(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """If calculator_done already has status='completed' for today, no sub-modules run."""
        from src.calculator.main import run_calculator

        _insert_pipeline_event(db_connection, "calculator_done", _today(), "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        ns.compute_indicators.assert_not_called()
        ns.detect_crossovers.assert_not_called()

    def test_force_reruns_despite_completed_event(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """force=True runs all sub-modules even when calculator_done is already 'completed'."""
        from src.calculator.main import run_calculator

        _insert_pipeline_event(db_connection, "calculator_done", _today(), "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full", force=True)

        assert ns.compute_indicators.call_count >= 3

    def test_retries_if_previous_run_failed(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """If calculator_done has status='failed', the run proceeds (retry)."""
        from src.calculator.main import run_calculator

        _insert_pipeline_event(db_connection, "calculator_done", _today(), "failed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        assert ns.compute_indicators.call_count >= 3

    def test_incremental_mode_waits_for_fetcher_event(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """In incremental mode, if fetcher_done event is missing, returns without processing."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )
        # No fetcher_done event in DB for today

        run_calculator(mode="incremental")

        ns.compute_indicators.assert_not_called()

    def test_processes_sector_etfs_and_benchmarks(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """Sector ETFs and market benchmarks get indicators + weekly computed."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        # ETFs/benchmarks should cause additional calls to indicators + weekly
        # (3 stock tickers + at least the ETFs from get_sector_etfs + get_market_benchmarks)
        etf_symbols = {"XLK", "XLF", "SPY", "QQQ"}
        all_indicator_calls = ns.compute_indicators.call_args_list
        called_tickers = {c[0][1] for c in all_indicator_calls}  # second positional arg is ticker
        assert called_tickers.issuperset(
            etf_symbols - {"AAPL", "MSFT", "JPM"}
        ) or ns.compute_indicators.call_count > 3, (
            "Expected indicators to be computed for ETFs/benchmarks in addition to stock tickers"
        )

    def test_single_ticker_filter(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """When ticker_filter='AAPL', only AAPL stock ticker is processed."""
        from src.calculator.main import run_calculator

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full", ticker_filter="AAPL")

        # Only AAPL should appear as a stock ticker in indicators calls
        stock_calls = [
            c for c in ns.compute_indicators.call_args_list
            if c[0][1] not in {"XLK", "XLF", "SPY", "QQQ"}
        ]
        called_stock_tickers = {c[0][1] for c in stock_calls}
        assert "AAPL" in called_stock_tickers
        assert "MSFT" not in called_stock_tickers
        assert "JPM" not in called_stock_tickers

    def test_returns_summary_dict(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """run_calculator returns a dict with all required summary fields."""
        from src.calculator.main import run_calculator

        _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        result = run_calculator(mode="full")

        required_keys = {
            "tickers_processed",
            "tickers_failed",
            "duration_seconds",
            "indicators_rows",
            "patterns_found",
            "divergences_found",
            "weekly_candles",
            "profiles_computed",
            "news_summaries",
        }
        assert required_keys.issubset(result.keys()), (
            f"Missing keys: {required_keys - result.keys()}"
        )
        assert isinstance(result["tickers_processed"], int)
        assert isinstance(result["tickers_failed"], int)
        assert isinstance(result["duration_seconds"], float)
        assert result["tickers_processed"] >= 0
        assert result["tickers_failed"] >= 0
