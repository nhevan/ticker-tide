"""
Drift-guard tests for src/scorer/zone_labels.py — zone_label_for_rsi().

Pins exact label strings for profile-path and fallback-path cases.
Also verifies sign agreement with score_with_percentile for RSI (higher_is_bullish=False):
  oversold zones → positive score (bullish)
  overbought zones → negative score (bearish)
"""

from __future__ import annotations

import pytest

from src.scorer.zone_labels import zone_label_for_adx, zone_label_for_cci, zone_label_for_rsi, zone_label_for_stoch_k
from src.scorer.indicator_scorer import score_adx, score_with_percentile

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


# ── Stochastic %K zone label tests ───────────────────────────────────────────

# Stoch %K profile: p5=5, p20=20, p50=50, p80=80, p95=95 for clean boundary testing.
_STOCH_PROFILE = {
    "p5": 5.0, "p20": 20.0, "p50": 50.0, "p80": 80.0, "p95": 95.0,
    "mean": 50.0, "std": 25.0,
}

# Standard stoch thresholds (20=oversold, 80=overbought).
_STOCH_THRESHOLDS = {"oversold": 20.0, "overbought": 80.0}


class TestZoneLabelForStochK:
    """Tests for zone_label_for_stoch_k()."""

    # ── Profile path ──────────────────────────────────────────────────────────

    def test_profile_extreme_oversold(self) -> None:
        """stoch_k=2, below p5=5 → extreme_oversold."""
        label = zone_label_for_stoch_k(2.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "extreme_oversold"

    def test_profile_oversold(self) -> None:
        """stoch_k=12, p5 ≤ 12 < p20=20 → oversold."""
        label = zone_label_for_stoch_k(12.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "oversold"

    def test_profile_below_mid(self) -> None:
        """stoch_k=35, p20 ≤ 35 < p50=50 → below_mid."""
        label = zone_label_for_stoch_k(35.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "below_mid"

    def test_profile_above_mid_at_p50_boundary(self) -> None:
        """stoch_k=50, exactly at p50 — [p50, p80) → above_mid."""
        label = zone_label_for_stoch_k(50.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "above_mid"

    def test_profile_overbought(self) -> None:
        """stoch_k=85, p80 ≤ 85 < p95=95 → overbought."""
        label = zone_label_for_stoch_k(85.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "overbought"

    def test_profile_extreme_overbought(self) -> None:
        """stoch_k=98, ≥ p95=95 → extreme_overbought."""
        label = zone_label_for_stoch_k(98.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "extreme_overbought"

    # ── Fallback path ─────────────────────────────────────────────────────────

    def test_fallback_oversold(self) -> None:
        """stoch_k=10, below oversold=20 → oversold."""
        label = zone_label_for_stoch_k(10.0, None, _STOCH_THRESHOLDS)
        assert label == "oversold"

    def test_fallback_below_mid(self) -> None:
        """stoch_k=35, oversold ≤ 35 < midpoint=50 → below_mid."""
        label = zone_label_for_stoch_k(35.0, None, _STOCH_THRESHOLDS)
        assert label == "below_mid"

    def test_fallback_above_mid(self) -> None:
        """stoch_k=65, midpoint ≤ 65 < overbought=80 → above_mid."""
        label = zone_label_for_stoch_k(65.0, None, _STOCH_THRESHOLDS)
        assert label == "above_mid"

    def test_fallback_overbought(self) -> None:
        """stoch_k=90, ≥ overbought=80 → overbought."""
        label = zone_label_for_stoch_k(90.0, None, _STOCH_THRESHOLDS)
        assert label == "overbought"

    # ── Boundary values ───────────────────────────────────────────────────────

    def test_boundary_at_p5(self) -> None:
        """stoch_k exactly at p5=5.0 → oversold (inclusive lower bound [p5, p20))."""
        label = zone_label_for_stoch_k(5.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "oversold"

    def test_boundary_at_p20(self) -> None:
        """stoch_k exactly at p20=20.0 → below_mid (inclusive lower bound [p20, p50))."""
        label = zone_label_for_stoch_k(20.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "below_mid"

    def test_boundary_at_p80(self) -> None:
        """stoch_k exactly at p80=80.0 → overbought (inclusive lower bound [p80, p95))."""
        label = zone_label_for_stoch_k(80.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "overbought"

    def test_boundary_at_p95(self) -> None:
        """stoch_k exactly at p95=95.0 → extreme_overbought (value ≥ p95)."""
        label = zone_label_for_stoch_k(95.0, _STOCH_PROFILE, _STOCH_THRESHOLDS)
        assert label == "extreme_overbought"

    def test_fallback_boundary_at_oversold(self) -> None:
        """stoch_k exactly at oversold=20.0 → below_mid (inclusive lower bound [oversold, mid))."""
        label = zone_label_for_stoch_k(20.0, None, _STOCH_THRESHOLDS)
        assert label == "below_mid"

    def test_fallback_boundary_at_overbought(self) -> None:
        """stoch_k exactly at overbought=80.0 → overbought (value ≥ overbought)."""
        label = zone_label_for_stoch_k(80.0, None, _STOCH_THRESHOLDS)
        assert label == "overbought"

    def test_fallback_midpoint(self) -> None:
        """stoch_k exactly at midpoint=50.0 → above_mid (inclusive lower bound [mid, overbought))."""
        label = zone_label_for_stoch_k(50.0, None, _STOCH_THRESHOLDS)
        assert label == "above_mid"

    # ── Sign-agreement test (Revision 2) ──────────────────────────────────────

    def test_oversold_label_agrees_with_ranging_score_sign(self) -> None:
        """
        In the ranging regime, stoch_k is treated as higher_is_bullish=False
        (low stoch_k = oversold = bullish). Assert that a stoch_k value in the
        oversold zone produces a positive score from score_with_percentile.

        The assumption here is the ranging convention. In trending regimes the
        sign is flipped by the scorer, but zone labels are regime-agnostic.
        """
        value = 10.0  # clearly in oversold territory (below p5=5 AND fallback oversold=20)
        label = zone_label_for_stoch_k(value, None, _STOCH_THRESHOLDS)
        assert label == "oversold", f"Expected 'oversold' label, got {label!r}"
        # Use a profile where value=10 is in the oversold band (below p20=20).
        oversold_profile = {
            "p5": 3.0, "p20": 20.0, "p50": 50.0, "p80": 80.0, "p95": 95.0,
            "mean": 50.0, "std": 25.0,
        }
        score = score_with_percentile(value, oversold_profile, higher_is_bullish=False)
        assert score > 0, (
            f"Expected positive score for oversold stoch_k in ranging regime, got {score}"
        )

    def test_overbought_label_agrees_with_ranging_score_sign(self) -> None:
        """
        Symmetric to the oversold test: in the ranging regime, a stoch_k value
        in the overbought zone produces a negative score from score_with_percentile
        (mean-reversion convention: high %K = bearish). Trending sign-flip happens
        in the scorer; zone labels are regime-agnostic.

        Note: the label assertion uses the fallback dict (None profile, value=90
        above overbought=80) — that path is already exercised by the fallback
        zone tests; the new payload here is the sign-agreement assertion.
        """
        value = 90.0
        label = zone_label_for_stoch_k(value, None, _STOCH_THRESHOLDS)
        assert label == "overbought", f"Expected 'overbought' label, got {label!r}"
        # Profile where value=90 lands in the overbought band (p80 ≤ 90 < p95).
        overbought_profile = {
            "p5": 5.0, "p20": 20.0, "p50": 50.0, "p80": 80.0, "p95": 97.0,
            "mean": 50.0, "std": 25.0,
        }
        score = score_with_percentile(value, overbought_profile, higher_is_bullish=False)
        assert score < 0, (
            f"Expected negative score for overbought stoch_k in ranging regime, got {score}"
        )


# ── ADX zone label tests ─────────────────────────────────────────────────────

class TestZoneLabelForAdx:
    """Tests for zone_label_for_adx()."""

    def test_below_20_is_ranging(self) -> None:
        """ADX values below 20 → ranging zone."""
        assert zone_label_for_adx(0.0) == "ranging"
        assert zone_label_for_adx(10.0) == "ranging"
        assert zone_label_for_adx(19.999) == "ranging"

    def test_at_20_is_weak_trend_developing(self) -> None:
        """ADX exactly at 20 → weak_trend_developing (boundary pin, mirrors >= 20 in score_adx)."""
        assert zone_label_for_adx(20.0) == "weak_trend_developing"

    def test_between_20_and_25_is_weak(self) -> None:
        """ADX between 20 and 25 (exclusive) → weak_trend_developing."""
        assert zone_label_for_adx(22.5) == "weak_trend_developing"

    def test_at_25_is_developing_trend(self) -> None:
        """ADX exactly at 25 → developing_trend (boundary pin, matches score_adx >= 25)."""
        assert zone_label_for_adx(25.0) == "developing_trend"

    def test_between_25_and_40_is_developing(self) -> None:
        """ADX between 25 and 40 (exclusive) → developing_trend."""
        assert zone_label_for_adx(33.0) == "developing_trend"

    def test_at_40_is_strong_trend(self) -> None:
        """ADX exactly at 40 → strong_trend (boundary pin, matches score_adx >= 40)."""
        assert zone_label_for_adx(40.0) == "strong_trend"

    def test_above_40_is_strong_trend(self) -> None:
        """ADX values above 40 → strong_trend."""
        assert zone_label_for_adx(55.0) == "strong_trend"
        assert zone_label_for_adx(80.0) == "strong_trend"
        assert zone_label_for_adx(100.0) == "strong_trend"

    def test_label_score_sign_agreement_at_boundaries(self) -> None:
        """
        At each boundary (20, 25, 40), the sign of score_adx agrees with
        the band the label points to. No scenario where label = "weak_trend_developing"
        but score is +40 (the discontinuity gap).
        """
        # ADX=20: label → weak_trend_developing, score = 0.0 (boundary between -20..0 and 0..20)
        label_20 = zone_label_for_adx(20.0)
        score_20 = score_adx(20.0)
        assert label_20 == "weak_trend_developing", f"At ADX=20, expected weak_trend_developing, got {label_20!r}"
        assert score_20 == 0.0, f"At ADX=20, expected score 0.0, got {score_20}"

        # ADX=25: label → developing_trend, score = 40.0 (bottom of +40..+80 band)
        label_25 = zone_label_for_adx(25.0)
        score_25 = score_adx(25.0)
        assert label_25 == "developing_trend", f"At ADX=25, expected developing_trend, got {label_25!r}"
        assert score_25 == 40.0, f"At ADX=25, expected score 40.0, got {score_25}"
        # Guard: weak_trend_developing must NOT produce score 40+ (the discontinuity gap)
        for adx_val in [20.0, 22.5, 24.999]:
            label = zone_label_for_adx(adx_val)
            score = score_adx(adx_val)
            assert label == "weak_trend_developing", (
                f"ADX={adx_val}: expected weak_trend_developing, got {label!r}"
            )
            assert score < 40.0, (
                f"ADX={adx_val}: score {score} should be below the +40 gap threshold"
            )

        # ADX=40: label → strong_trend, score = 80.0 (cap)
        label_40 = zone_label_for_adx(40.0)
        score_40 = score_adx(40.0)
        assert label_40 == "strong_trend", f"At ADX=40, expected strong_trend, got {label_40!r}"
        assert score_40 == 80.0, f"At ADX=40, expected score 80.0, got {score_40}"


# ── CCI(20) zone label tests ──────────────────────────────────────────────────

# Synthetic CCI profile used for profile-path tests.
_CCI_PROFILE = {
    "p5": -150.0, "p20": -60.0, "p50": 0.0, "p80": 60.0, "p95": 150.0,
    "mean": 0.0, "std": 80.0,
}


class TestZoneLabelForCciProfilePath:
    """Tests for zone_label_for_cci() with a real percentile profile (six zones)."""

    def test_extreme_oversold(self) -> None:
        """CCI=-200, below p5=-150 → extreme_oversold."""
        label = zone_label_for_cci(-200.0, _CCI_PROFILE)
        assert label == "extreme_oversold"

    def test_oversold(self) -> None:
        """CCI=-100, p5 ≤ -100 < p20=-60 → oversold."""
        label = zone_label_for_cci(-100.0, _CCI_PROFILE)
        assert label == "oversold"

    def test_below_mid(self) -> None:
        """CCI=-30, p20 ≤ -30 < p50=0 → below_mid."""
        label = zone_label_for_cci(-30.0, _CCI_PROFILE)
        assert label == "below_mid"

    def test_above_mid_at_p50_boundary(self) -> None:
        """CCI=0, exactly at p50=0 — [p50, p80) → above_mid."""
        label = zone_label_for_cci(0.0, _CCI_PROFILE)
        assert label == "above_mid"

    def test_overbought(self) -> None:
        """CCI=100, p80 ≤ 100 < p95=150 → overbought."""
        label = zone_label_for_cci(100.0, _CCI_PROFILE)
        assert label == "overbought"

    def test_extreme_overbought(self) -> None:
        """CCI=200, ≥ p95=150 → extreme_overbought."""
        label = zone_label_for_cci(200.0, _CCI_PROFILE)
        assert label == "extreme_overbought"


class TestZoneLabelForCciFallbackPath:
    """Tests for zone_label_for_cci() with profile=None (five fallback zones)."""

    def test_hyper_oversold_below_minus_200(self) -> None:
        """CCI=-201, below -200 → hyper_oversold."""
        label = zone_label_for_cci(-201.0, None)
        assert label == "hyper_oversold"

    def test_hyper_oversold_at_minus_200(self) -> None:
        """CCI=-200, ≤ -200 → hyper_oversold (inclusive boundary)."""
        label = zone_label_for_cci(-200.0, None)
        assert label == "hyper_oversold"

    def test_oversold_mid_range(self) -> None:
        """CCI=-150, -200 < -150 ≤ -100 → oversold."""
        label = zone_label_for_cci(-150.0, None)
        assert label == "oversold"

    def test_oversold_at_minus_100(self) -> None:
        """CCI=-100, exactly -100 (≤ -100 boundary) → oversold."""
        label = zone_label_for_cci(-100.0, None)
        assert label == "oversold"

    def test_neutral_just_above_minus_100(self) -> None:
        """CCI=-99, -100 < -99 < 100 → neutral."""
        label = zone_label_for_cci(-99.0, None)
        assert label == "neutral"

    def test_neutral_at_zero(self) -> None:
        """CCI=0 → neutral."""
        label = zone_label_for_cci(0.0, None)
        assert label == "neutral"

    def test_neutral_just_below_100(self) -> None:
        """CCI=99, still below +100 → neutral."""
        label = zone_label_for_cci(99.0, None)
        assert label == "neutral"

    def test_overbought_at_100(self) -> None:
        """CCI=100, 100 ≤ 100 < 200 → overbought."""
        label = zone_label_for_cci(100.0, None)
        assert label == "overbought"

    def test_overbought_mid_range(self) -> None:
        """CCI=150, 100 ≤ 150 < 200 → overbought."""
        label = zone_label_for_cci(150.0, None)
        assert label == "overbought"

    def test_hyper_overbought_at_200(self) -> None:
        """CCI=200, ≥ 200 → hyper_overbought."""
        label = zone_label_for_cci(200.0, None)
        assert label == "hyper_overbought"

    def test_hyper_overbought_above_200(self) -> None:
        """CCI=201, above 200 → hyper_overbought."""
        label = zone_label_for_cci(201.0, None)
        assert label == "hyper_overbought"
