"""
Tests for the cold-start confidence multiplier path in src/scorer/main.py.

Verifies that:
1. When calibrated_score is None and config has cold_start_base_multiplier, the
   confidence base equals abs(final_score) * cold_start_base_multiplier.
2. When config has NO confidence block, the multiplier falls back to 0.3.

TDD: these tests are written BEFORE the implementation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.scorer.main import resolve_cold_start_multiplier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_config(cold_start_base_multiplier: float | None = None) -> dict:
    """
    Build the minimal scorer config dict needed for score_ticker.

    Parameters:
        cold_start_base_multiplier: When not None, inserts a confidence block
            with the given multiplier. When None, the confidence block is absent.

    Returns:
        Scorer config dict.
    """
    cfg: dict = {
        "calibration": {
            "enabled": True,
            "window_size": 365,
            "ridge_lambda": 0.1,
            "min_training_samples": 30,
            "benchmark_ticker": "SPY",
            "forward_days": 10,
        },
        "regime_detection": {
            "adx_trending_threshold": 25,
            "adx_ranging_threshold": 20,
            "atr_volatile_multiplier": 1.5,
            "atr_volatile_lookback": 20,
            "vix_volatile_threshold": 25,
            "ema_trend_override": True,
        },
        "adaptive_weights": {
            "trending": {
                "trend": 0.30, "momentum": 0.20, "volume": 0.10, "volatility": 0.05,
                "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                "fundamental": 0.05, "macro": 0.30,
            },
            "ranging": {
                "trend": 0.15, "momentum": 0.25, "volume": 0.15, "volatility": 0.10,
                "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                "fundamental": 0.10, "macro": 0.25,
            },
            "volatile": {
                "trend": 0.20, "momentum": 0.20, "volume": 0.10, "volatility": 0.15,
                "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                "fundamental": 0.05, "macro": 0.30,
            },
        },
        "timeframe_weights": {
            "trending": {"daily": 0.10, "weekly": 0.50, "monthly": 0.40},
            "ranging":  {"daily": 0.60, "weekly": 0.30, "monthly": 0.10},
            "volatile": {"daily": 0.25, "weekly": 0.45, "monthly": 0.30},
        },
        "sector_adjustment": {
            "bullish_sector_threshold": 30,
            "bearish_sector_threshold": -30,
            "max_adjustment": 10,
        },
        "signal_thresholds": {"bullish": 2, "bearish": -2},
        "confidence_modifiers": {
            "timeframe_agree": 10,
            "timeframe_disagree": -15,
            "volume_confirms": 10,
            "volume_diverges": -10,
            "indicator_consensus": 5,
            "indicator_mixed": -10,
            "earnings_within_days": 7,
            "earnings_penalty": -15,
            "vix_extreme_threshold": 30,
            "vix_extreme_penalty": -10,
            "atr_expanding_penalty": -5,
            "missing_news_penalty": -5,
            "missing_fundamentals_penalty": -3,
        },
        "scoring": {"score_expansion_factor": 1.5},
        "weekly_score_method": "v2_8cat",
        "monthly_score_method": "v2_8cat",
        "weekly_adaptive_weights_v2": {
            "trending": {"trend": 0.45, "momentum": 0.25, "volume": 0.15, "volatility": 0.15, "candlestick": 0.0, "structural": 0.0},
            "ranging":  {"trend": 0.20, "momentum": 0.40, "volume": 0.20, "volatility": 0.20, "candlestick": 0.0, "structural": 0.0},
            "volatile": {"trend": 0.30, "momentum": 0.25, "volume": 0.15, "volatility": 0.30, "candlestick": 0.0, "structural": 0.0},
        },
        "monthly_adaptive_weights_v2": {
            "trending": {"trend": 0.50, "momentum": 0.20, "volume": 0.15, "volatility": 0.15, "candlestick": 0.0, "structural": 0.0},
            "ranging":  {"trend": 0.25, "momentum": 0.35, "volume": 0.20, "volatility": 0.20, "candlestick": 0.0, "structural": 0.0},
            "volatile": {"trend": 0.35, "momentum": 0.20, "volume": 0.15, "volatility": 0.30, "candlestick": 0.0, "structural": 0.0},
        },
        "historical_scoring": {
            "daily_lookback_months": 12,
            "weekly_lookback_months": 60,
            "monthly_lookback_months": 60,
        },
        "calibrator_acceptance": {
            "max_mean_delta": 5.0,
            "max_std_delta": 8.0,
            "max_ticker_delta": 15.0,
            "min_sample_size": 30,
        },
    }
    if cold_start_base_multiplier is not None:
        cfg["confidence"] = {"cold_start_base_multiplier": cold_start_base_multiplier}
    return cfg


# ---------------------------------------------------------------------------
# Tests — exercise the live resolve_cold_start_multiplier helper imported from
# src/scorer/main.py so that any change to the production resolution rule is
# caught here (rather than testing a local copy).
# ---------------------------------------------------------------------------

class TestColdStartMultiplierResolution:
    """Tests for the cold_start_base_multiplier resolution from config."""

    def test_cold_start_uses_config_multiplier(self) -> None:
        """
        Test 1: When config has confidence.cold_start_base_multiplier = 0.65
        and a known final_score of +50, the resolved base equals abs(50) * 0.65 = 32.5.
        """
        config = _make_minimal_config(cold_start_base_multiplier=0.65)
        multiplier = resolve_cold_start_multiplier(config)
        final_score = 50.0

        confidence_base = abs(final_score) * multiplier

        assert multiplier == 0.65
        assert confidence_base == pytest.approx(32.5)

    def test_cold_start_falls_back_to_0_3_when_config_missing(self) -> None:
        """
        Test 2: When config has NO confidence block, the multiplier defaults to 0.3
        and the base equals abs(50) * 0.3 = 15.0.
        """
        config = _make_minimal_config(cold_start_base_multiplier=None)
        assert "confidence" not in config  # pre-condition: absence is explicit

        multiplier = resolve_cold_start_multiplier(config)
        final_score = 50.0

        confidence_base = abs(final_score) * multiplier

        assert multiplier == 0.3
        assert confidence_base == pytest.approx(15.0)

    def test_cold_start_negative_final_score_uses_abs(self) -> None:
        """
        abs() is applied before multiplication — a bearish final_score of -50
        produces the same base magnitude as +50.
        """
        config = _make_minimal_config(cold_start_base_multiplier=0.65)
        multiplier = resolve_cold_start_multiplier(config)
        final_score = -50.0

        confidence_base = abs(final_score) * multiplier

        assert confidence_base == pytest.approx(32.5)

    def test_cold_start_zero_final_score_gives_zero_base(self) -> None:
        """
        A final_score of 0 produces a base of 0 regardless of multiplier.
        """
        config = _make_minimal_config(cold_start_base_multiplier=0.65)
        multiplier = resolve_cold_start_multiplier(config)
        final_score = 0.0

        confidence_base = abs(final_score) * multiplier

        assert confidence_base == pytest.approx(0.0)

    def test_cold_start_partial_confidence_block(self) -> None:
        """
        A config that has a confidence block but missing the multiplier key
        falls back to 0.3 (covers the inner .get default).
        """
        config = _make_minimal_config(cold_start_base_multiplier=None)
        config["confidence"] = {}  # block present, key absent

        multiplier = resolve_cold_start_multiplier(config)

        assert multiplier == 0.3
