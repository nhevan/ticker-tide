"""
Tests for src/scorer/regime.py — market regime detection.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.regime import detect_regime, get_regime_weights


SAMPLE_CONFIG = {
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
            "trend": 0.30, "momentum": 0.15, "volume": 0.10, "volatility": 0.05,
            "candlestick": 0.05, "structural": 0.15, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
        "ranging": {
            "trend": 0.10, "momentum": 0.25, "volume": 0.10, "volatility": 0.10,
            "candlestick": 0.10, "structural": 0.15, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
        "volatile": {
            "trend": 0.20, "momentum": 0.15, "volume": 0.10, "volatility": 0.15,
            "candlestick": 0.10, "structural": 0.10, "sentiment": 0.10,
            "fundamental": 0.05, "macro": 0.05,
        },
    },
}


class TestDetectRegime:
    def test_detect_regime_trending(self) -> None:
        """ADX=30 (above trending threshold of 25) → trending."""
        regime = detect_regime(adx=30, atr=1.0, atr_sma_20=1.0, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "trending"

    def test_detect_regime_ranging(self) -> None:
        """ADX=15 (below ranging threshold of 20) → ranging."""
        regime = detect_regime(adx=15, atr=1.0, atr_sma_20=1.0, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "ranging"

    def test_detect_regime_volatile_atr(self) -> None:
        """ADX=22 (between thresholds), ATR is 2x its 20-day SMA → volatile."""
        regime = detect_regime(adx=22, atr=2.0, atr_sma_20=1.0, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "volatile"

    def test_detect_regime_volatile_vix(self) -> None:
        """ADX=22, ATR normal, but VIX=32 (above threshold of 25) → volatile."""
        regime = detect_regime(adx=22, atr=1.0, atr_sma_20=1.0, vix_close=32.0, config=SAMPLE_CONFIG)
        assert regime == "volatile"

    def test_detect_regime_priority_volatile_over_trending(self) -> None:
        """ADX=30 would be trending BUT VIX=35 → volatile (volatile takes priority)."""
        regime = detect_regime(adx=30, atr=1.0, atr_sma_20=1.0, vix_close=35.0, config=SAMPLE_CONFIG)
        assert regime == "volatile"

    def test_detect_regime_priority_volatile_over_ranging(self) -> None:
        """ADX=15 would be ranging BUT ATR is 2x average → volatile."""
        regime = detect_regime(adx=15, atr=2.0, atr_sma_20=1.0, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "volatile"

    def test_detect_regime_ambiguous_defaults_to_ranging(self) -> None:
        """ADX=22 (between 20 and 25), ATR normal, VIX normal → ranging (default)."""
        regime = detect_regime(adx=22, atr=1.0, atr_sma_20=1.0, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "ranging"

    def test_detect_regime_uses_config_thresholds(self) -> None:
        """Override config: adx_trending_threshold=30, adx_ranging_threshold=15.
        ADX=27 should be ranging (not trending with the higher threshold)."""
        config = {
            "regime_detection": {
                "adx_trending_threshold": 30,
                "adx_ranging_threshold": 15,
                "atr_volatile_multiplier": 1.5,
                "atr_volatile_lookback": 20,
                "vix_volatile_threshold": 25,
            },
            "adaptive_weights": SAMPLE_CONFIG["adaptive_weights"],
        }
        regime = detect_regime(adx=27, atr=1.0, atr_sma_20=1.0, vix_close=15.0, config=config)
        assert regime == "ranging"

    def test_detect_regime_handles_missing_vix(self) -> None:
        """VIX=None — regime still detected using ADX and ATR only, no crash."""
        regime = detect_regime(adx=30, atr=1.0, atr_sma_20=1.0, vix_close=None, config=SAMPLE_CONFIG)
        assert regime == "trending"

    def test_detect_regime_handles_missing_atr(self) -> None:
        """ATR=None — regime still detected using ADX and VIX only, no crash."""
        regime = detect_regime(adx=30, atr=None, atr_sma_20=None, vix_close=15.0, config=SAMPLE_CONFIG)
        assert regime == "trending"

    def test_detect_regime_handles_all_none(self) -> None:
        """All inputs None → default to ranging, no crash."""
        regime = detect_regime(adx=None, atr=None, atr_sma_20=None, vix_close=None, config=SAMPLE_CONFIG)
        assert regime == "ranging"


class TestGetRegimeWeights:
    def test_get_regime_weights_trending(self) -> None:
        """get_regime_weights('trending', config) returns trending weight dict from config."""
        weights = get_regime_weights("trending", SAMPLE_CONFIG)
        assert weights["trend"] == pytest.approx(0.30)
        assert weights["momentum"] == pytest.approx(0.15)
        assert weights["volume"] == pytest.approx(0.10)
        assert weights["volatility"] == pytest.approx(0.05)
        assert weights["candlestick"] == pytest.approx(0.05)
        assert weights["structural"] == pytest.approx(0.15)
        assert weights["sentiment"] == pytest.approx(0.10)
        assert weights["fundamental"] == pytest.approx(0.05)
        assert weights["macro"] == pytest.approx(0.05)

    def test_get_regime_weights_sum_to_one(self) -> None:
        """For each regime (trending, ranging, volatile), verify weights sum to 1.0."""
        for regime in ("trending", "ranging", "volatile"):
            weights = get_regime_weights(regime, SAMPLE_CONFIG)
            total = sum(weights.values())
            assert total == pytest.approx(1.0, abs=1e-9), f"{regime} weights sum to {total}"


class TestEMATrendOverride:
    """Tests for EMA stack alignment override in detect_regime()."""

    def test_bullish_ema_stack_overrides_ranging_to_trending(self) -> None:
        """ADX=18 (ranging), but close > ema9 > ema21 > ema50 → trending via EMA override."""
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=140.0,
        )
        assert regime == "trending"

    def test_bearish_ema_stack_overrides_ranging_to_trending(self) -> None:
        """ADX=18 (ranging), but close < ema9 < ema21 < ema50 → trending via EMA override."""
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=130.0, ema_9=135.0, ema_21=140.0, ema_50=145.0,
        )
        assert regime == "trending"

    def test_non_aligned_emas_stay_ranging(self) -> None:
        """ADX=18, close > ema9 but ema9 < ema21 (not fully aligned) → ranging."""
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=152.0, ema_50=140.0,
        )
        assert regime == "ranging"

    def test_override_disabled_via_config(self) -> None:
        """Fully aligned EMAs, but ema_trend_override=false → ranging (not overridden)."""
        config_disabled = {
            **SAMPLE_CONFIG,
            "regime_detection": {
                **SAMPLE_CONFIG["regime_detection"],
                "ema_trend_override": False,
            },
        }
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=config_disabled,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=140.0,
        )
        assert regime == "ranging"

    def test_volatile_still_takes_priority_over_ema_override(self) -> None:
        """VIX=35 (volatile), fully aligned EMAs → volatile (volatile > EMA override)."""
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=35.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=140.0,
        )
        assert regime == "volatile"

    def test_adx_already_trending_unchanged(self) -> None:
        """ADX=30 (already trending), fully aligned EMAs → trending (same result)."""
        regime = detect_regime(
            adx=30, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=140.0,
        )
        assert regime == "trending"

    def test_missing_ema_values_graceful_fallback(self) -> None:
        """ADX=18, ema_50=None → can't determine alignment, falls back to ranging."""
        regime = detect_regime(
            adx=18, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=None,
        )
        assert regime == "ranging"

    def test_ambiguous_adx_zone_with_aligned_emas(self) -> None:
        """ADX=22 (between 20-25, ambiguous zone), fully aligned EMAs → trending via override."""
        regime = detect_regime(
            adx=22, atr=1.0, atr_sma_20=1.0, vix_close=15.0,
            config=SAMPLE_CONFIG,
            close=155.0, ema_9=150.0, ema_21=145.0, ema_50=140.0,
        )
        assert regime == "trending"
