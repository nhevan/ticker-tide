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


# ── Tests: pipeline event date matches actual data date ───────────────────────

def _insert_indicators_row(conn: sqlite3.Connection, ticker: str, date: str) -> None:
    """Insert a minimal indicators_daily row for event-date tests."""
    conn.execute(
        """INSERT OR REPLACE INTO indicators_daily
               (ticker, date, ema_9, ema_21, ema_50, macd_line, macd_signal,
                macd_histogram, adx, rsi_14, stoch_k, stoch_d, cci_20,
                williams_r, obv, cmf_20, ad_line, bb_upper, bb_lower, bb_pctb,
                atr_14, keltner_upper, keltner_lower)
           VALUES (?, ?, 101, 100, 99, 0.5, 0.3, 0.2, 22, 55, 60, 55, 30,
                   -30, 1000000, 0.1, 500000, 105, 95, 0.6, 1.5, 106, 94)""",
        (ticker, date),
    )
    conn.commit()


class TestCalculatorPipelineEventDate:
    """The calculator should write calculator_done for the latest data date, not today."""

    def test_writes_event_with_latest_data_date_not_today(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """calculator_done event is written for the MAX(date) in indicators_daily,
        not for today's wall-clock date."""
        from src.calculator.main import run_calculator

        DATA_DATE = "2025-01-15"  # a past trading date

        # Pre-insert indicator rows so MAX(date) resolves to DATA_DATE
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_indicators_row(db_connection, ticker, DATA_DATE)

        _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        # Event should be written for DATA_DATE, not today
        row_data = db_connection.execute(
            "SELECT status FROM pipeline_events WHERE event = 'calculator_done' AND date = ?",
            (DATA_DATE,),
        ).fetchone()
        assert row_data is not None, (
            f"Expected calculator_done for data date {DATA_DATE}"
        )
        assert row_data["status"] == "completed"

    def test_skips_if_data_date_already_done(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """If calculator_done exists for the latest data date (not today), the run is skipped."""
        from src.calculator.main import run_calculator

        DATA_DATE = "2025-01-15"

        # Simulate: indicators exist for DATA_DATE, and event already completed
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_indicators_row(db_connection, ticker, DATA_DATE)
        _insert_pipeline_event(db_connection, "calculator_done", DATA_DATE, "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        # Sub-modules should NOT have been called (skipped)
        ns.compute_indicators.assert_not_called()

    def test_falls_back_to_today_when_no_indicator_data(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """When indicators_daily is empty, event is written for today as fallback."""
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


# ── Tests: run_calculator target_date parameter ───────────────────────────────

class TestRunCalculatorTargetDate:
    """
    Tests for the target_date parameter on run_calculator().

    The daily pipeline runs at 00:00 UTC and fetches data for yesterday (target_date).
    The fetcher stores fetcher_done for target_date, not for today. The calculator
    must check fetcher_done against target_date, not today, so that incremental mode
    actually proceeds.
    """

    def test_uses_target_date_for_fetcher_done_check(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """
        When target_date="2026-03-30" is supplied, the calculator should check
        fetcher_done for "2026-03-30", not for today's UTC date.

        Inserting fetcher_done for "2026-03-30" (not today) must be enough
        for incremental mode to proceed and call compute_indicators.
        """
        from src.calculator.main import run_calculator

        target_date = "2026-03-30"
        # Insert fetcher_done for target_date, NOT for today
        _insert_pipeline_event(db_connection, "fetcher_done", target_date, "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="incremental", target_date=target_date)

        ns.compute_indicators.assert_called(), (
            "compute_indicators was not called — the calculator skipped despite "
            f"fetcher_done being present for target_date={target_date!r}. "
            "It is likely still checking fetcher_done against today's date."
        )

    def test_falls_back_to_today_when_no_target_date(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """
        When target_date is not provided, the calculator should fall back to
        today's UTC date for the fetcher_done check (existing behaviour preserved).

        Inserting fetcher_done for today must be enough for incremental mode to proceed.
        """
        from src.calculator.main import run_calculator

        _insert_pipeline_event(db_connection, "fetcher_done", _today(), "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="incremental")  # no target_date

        ns.compute_indicators.assert_called(), (
            "compute_indicators was not called — the calculator skipped despite "
            "fetcher_done being present for today. Backward-compatible fallback is broken."
        )


# ── Helpers for OHLCV-based skip tests ───────────────────────────────────────

def _insert_ohlcv_row(conn: sqlite3.Connection, ticker: str, date: str) -> None:
    """Insert a minimal ohlcv_daily row for skip-logic tests."""
    conn.execute(
        """INSERT OR REPLACE INTO ohlcv_daily
               (ticker, date, open, high, low, close, volume)
           VALUES (?, ?, 100, 105, 99, 102, 1000000)""",
        (ticker, date),
    )
    conn.commit()


# ── Tests: _resolve_ohlcv_max_date ───────────────────────────────────────────

class TestResolveOhlcvMaxDate:
    """Tests for the _resolve_ohlcv_max_date helper."""

    def test_returns_none_when_empty(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns None when ohlcv_daily has no rows."""
        from src.calculator.main import _resolve_ohlcv_max_date

        result = _resolve_ohlcv_max_date(db_connection)

        assert result is None

    def test_returns_latest_date(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns the latest date across all tickers when rows exist."""
        from src.calculator.main import _resolve_ohlcv_max_date

        _insert_ohlcv_row(db_connection, "AAPL", "2026-03-28")
        _insert_ohlcv_row(db_connection, "AAPL", "2026-03-31")
        _insert_ohlcv_row(db_connection, "MSFT", "2026-03-29")

        result = _resolve_ohlcv_max_date(db_connection)

        assert result == "2026-03-31"


# ── Tests: incremental skip uses OHLCV vs indicators comparison ───────────────

class TestIncrementalSkipLogic:
    """
    In incremental mode the calculator should skip only when
    MAX(ohlcv_daily.date) <= MAX(indicators_daily.date), not when
    calculator_done pipeline event is present for the old date.
    """

    def test_incremental_skips_when_indicators_current(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """Calculator skips when OHLCV max date equals indicators max date."""
        from src.calculator.main import run_calculator

        date = "2026-03-31"
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_ohlcv_row(db_connection, ticker, date)
            _insert_indicators_row(db_connection, ticker, date)
        _insert_pipeline_event(db_connection, "fetcher_done", date, "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="incremental", target_date=date)

        ns.compute_indicators.assert_not_called(), (
            "compute_indicators was called despite indicators already being "
            "up-to-date with OHLCV — the incremental skip should have fired."
        )

    def test_incremental_runs_when_ohlcv_newer(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """Calculator runs even when calculator_done is completed for the old date,
        as long as OHLCV has a newer date than indicators."""
        from src.calculator.main import run_calculator

        old_date = "2026-03-28"
        new_date = "2026-03-31"

        # Indicators exist only for old_date; OHLCV has new_date
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_indicators_row(db_connection, ticker, old_date)
            _insert_ohlcv_row(db_connection, ticker, new_date)

        # calculator_done already completed for old_date — this is the bug scenario
        _insert_pipeline_event(db_connection, "calculator_done", old_date, "completed")
        _insert_pipeline_event(db_connection, "fetcher_done", new_date, "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="incremental", target_date=new_date)

        assert ns.compute_indicators.call_count >= 3, (
            "compute_indicators was not called despite OHLCV having a newer date "
            f"({new_date}) than indicators ({old_date}). The event-based skip "
            "should NOT fire in incremental mode."
        )

    def test_full_mode_skip_uses_event_not_ohlcv(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """In full mode, skip check still uses calculator_done event, not OHLCV vs indicators."""
        from src.calculator.main import run_calculator

        old_date = "2026-03-28"
        new_date = "2026-03-31"

        # OHLCV is newer than indicators — but we're in full mode
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_indicators_row(db_connection, ticker, old_date)
            _insert_ohlcv_row(db_connection, ticker, new_date)

        # calculator_done exists for old_date (which _resolve_data_date returns)
        _insert_pipeline_event(db_connection, "calculator_done", old_date, "completed")

        ns = _patch_run_calculator_deps(
            mocker, db_connection, three_tickers, sample_calc_config
        )

        run_calculator(mode="full")

        ns.compute_indicators.assert_not_called(), (
            "Full mode should still use event-based skip even when OHLCV is newer."
        )


# ── Tests: calculator_done event written for OHLCV max date ──────────────────

class TestCalculatorEventWrittenForOhlcvDate:
    """The calculator_done event should be written for MAX(ohlcv_daily.date),
    not for today's wall-clock date, so the scorer/notifier can find it."""

    def test_writes_event_for_ohlcv_max_date(
        self,
        mocker,
        db_connection: sqlite3.Connection,
        sample_calc_config: dict,
        three_tickers: list[dict],
    ) -> None:
        """calculator_done is written for the OHLCV trading date, not today."""
        from src.calculator.main import run_calculator

        trading_date = "2026-03-31"
        for ticker in ("AAPL", "MSFT", "JPM"):
            _insert_ohlcv_row(db_connection, ticker, trading_date)

        _patch_run_calculator_deps(mocker, db_connection, three_tickers, sample_calc_config)

        run_calculator(mode="full")

        row = db_connection.execute(
            "SELECT status FROM pipeline_events WHERE event = 'calculator_done' AND date = ?",
            (trading_date,),
        ).fetchone()
        assert row is not None, (
            f"Expected calculator_done for trading date {trading_date}"
        )
        assert row["status"] == "completed"
        # Should NOT be written for today's wall-clock date
        today = _today()
        if today != trading_date:
            today_row = db_connection.execute(
                "SELECT status FROM pipeline_events WHERE event = 'calculator_done' AND date = ?",
                (today,),
            ).fetchone()
            assert today_row is None, (
                f"calculator_done was written for today ({today}) instead of "
                f"the OHLCV trading date ({trading_date})"
            )
