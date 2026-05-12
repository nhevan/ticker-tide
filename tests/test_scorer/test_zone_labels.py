"""
Drift-guard tests for src/scorer/zone_labels.py — zone_label_for_rsi().

Pins exact label strings for profile-path and fallback-path cases.
Also verifies sign agreement with score_with_percentile for RSI (higher_is_bullish=False):
  oversold zones → positive score (bullish)
  overbought zones → negative score (bearish)
"""

from __future__ import annotations

import pytest

from src.scorer.zone_labels import zone_label_for_rsi
from src.scorer.indicator_scorer import score_with_percentile

# Synthetic profile used for profile-path tests.
_PROFILE = {"p5": 20.0, "p20": 35.0, "p50": 50.0, "p80": 65.0, "p95": 80.0,
             "mean": 50.0, "std": 15.0}

_THRESHOLDS = {"oversold": 30.0, "overbought": 70.0}


class TestZoneLabelProfilePath:
    """zone_label_for_rsi with a real profile — six possible labels."""

    def test_extreme_oversold(self) -> None:
        """RSI=5, below p5=20 → extreme_oversold."""
        label = zone_label_for_rsi(5.0, _PROFILE, _THRESHOLDS)
        assert label == "extreme_oversold"

    def test_oversold(self) -> None:
        """RSI=25, p5 ≤ 25 < p20=35 → oversold."""
        label = zone_label_for_rsi(25.0, _PROFILE, _THRESHOLDS)
        assert label == "oversold"

    def test_below_mid(self) -> None:
        """RSI=40, p20 ≤ 40 < p50=50 → below_mid."""
        label = zone_label_for_rsi(40.0, _PROFILE, _THRESHOLDS)
        assert label == "below_mid"

    def test_above_mid_at_p50_boundary(self) -> None:
        """RSI=50, exactly at p50 — [p50, p80) → above_mid."""
        label = zone_label_for_rsi(50.0, _PROFILE, _THRESHOLDS)
        assert label == "above_mid"

    def test_overbought(self) -> None:
        """RSI=75, p80 ≤ 75 < p95=80 → overbought."""
        label = zone_label_for_rsi(75.0, _PROFILE, _THRESHOLDS)
        assert label == "overbought"

    def test_extreme_overbought(self) -> None:
        """RSI=90, ≥ p95=80 → extreme_overbought."""
        label = zone_label_for_rsi(90.0, _PROFILE, _THRESHOLDS)
        assert label == "extreme_overbought"


class TestZoneLabelFallbackPath:
    """zone_label_for_rsi with profile=None — four fallback labels."""

    def test_oversold_fallback(self) -> None:
        """RSI=25, below oversold=30 → oversold."""
        label = zone_label_for_rsi(25.0, None, _THRESHOLDS)
        assert label == "oversold"

    def test_below_mid_fallback(self) -> None:
        """RSI=40, oversold ≤ 40 < midpoint=50 → below_mid."""
        label = zone_label_for_rsi(40.0, None, _THRESHOLDS)
        assert label == "below_mid"

    def test_above_mid_fallback(self) -> None:
        """RSI=55, midpoint ≤ 55 < overbought=70 → above_mid."""
        label = zone_label_for_rsi(55.0, None, _THRESHOLDS)
        assert label == "above_mid"

    def test_overbought_fallback(self) -> None:
        """RSI=75, ≥ overbought=70 → overbought."""
        label = zone_label_for_rsi(75.0, None, _THRESHOLDS)
        assert label == "overbought"


class TestZoneLabelSignAgreement:
    """
    For the profile path: label direction agrees with score_with_percentile sign.

    RSI uses higher_is_bullish=False, so:
      oversold zones (low RSI) → positive score (bullish)
      overbought zones (high RSI) → negative score (bearish)
    """

    def test_extreme_oversold_score_is_positive(self) -> None:
        """extreme_oversold label → score_with_percentile > 0 (bullish)."""
        label = zone_label_for_rsi(5.0, _PROFILE, _THRESHOLDS)
        score = score_with_percentile(5.0, _PROFILE, higher_is_bullish=False)
        assert label in ("extreme_oversold", "oversold")
        assert score > 0, f"Expected positive score for oversold zone, got {score}"

    def test_oversold_score_is_positive(self) -> None:
        """oversold label → score_with_percentile > 0 (bullish)."""
        label = zone_label_for_rsi(25.0, _PROFILE, _THRESHOLDS)
        score = score_with_percentile(25.0, _PROFILE, higher_is_bullish=False)
        assert label == "oversold"
        assert score > 0, f"Expected positive score for oversold zone, got {score}"

    def test_overbought_score_is_negative(self) -> None:
        """overbought label → score_with_percentile < 0 (bearish)."""
        label = zone_label_for_rsi(75.0, _PROFILE, _THRESHOLDS)
        score = score_with_percentile(75.0, _PROFILE, higher_is_bullish=False)
        assert label == "overbought"
        assert score < 0, f"Expected negative score for overbought zone, got {score}"

    def test_extreme_overbought_score_is_negative(self) -> None:
        """extreme_overbought label → score_with_percentile < 0 (bearish)."""
        label = zone_label_for_rsi(90.0, _PROFILE, _THRESHOLDS)
        score = score_with_percentile(90.0, _PROFILE, higher_is_bullish=False)
        assert label in ("extreme_overbought", "overbought")
        assert score < 0, f"Expected negative score for overbought zone, got {score}"
