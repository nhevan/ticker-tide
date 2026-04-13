"""
Tests for src/scorer/indicator_scorer.py — individual indicator scoring.
"""

from __future__ import annotations

import pytest

from src.scorer.indicator_scorer import (
    score_adx,
    score_all_indicators,
    score_ema_alignment,
    score_macd_histogram,
    score_rsi,
    score_with_percentile,
    score_with_zscore,
)


class TestScoreWithPercentile:
    def test_score_below_p5_bullish(self) -> None:
        """Value below p5 with higher_is_bullish=False → extreme bullish (+80 to +100)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0}
        score = score_with_percentile(25.0, profile, higher_is_bullish=False)
        assert 80 <= score <= 100

    def test_score_above_p95_bearish(self) -> None:
        """Value above p95 with higher_is_bullish=False → extreme bearish (-80 to -100)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0}
        score = score_with_percentile(85.0, profile, higher_is_bullish=False)
        assert -100 <= score <= -80

    def test_score_near_median_neutral(self) -> None:
        """Value near p50 → neutral score (-10 to +10)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0}
        score = score_with_percentile(53.0, profile, higher_is_bullish=False)
        assert -10 <= score <= 10

    def test_score_p80_to_p95_strong_bearish(self) -> None:
        """Value between p80 and p95 with higher_is_bullish=False → strong bearish (-40 to -80)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0}
        score = score_with_percentile(72.0, profile, higher_is_bullish=False)
        assert -80 <= score <= -40

    def test_score_higher_is_bullish_true(self) -> None:
        """With higher_is_bullish=True, a high value is bullish."""
        profile = {"p5": 10.0, "p20": 30.0, "p50": 50.0, "p80": 70.0, "p95": 90.0}
        score = score_with_percentile(95.0, profile, higher_is_bullish=True)
        assert score >= 80


class TestScoreWithZscore:
    def test_zscore_high_positive_bullish(self) -> None:
        """z > +2.0 → score between +80 and +100."""
        score = score_with_zscore(value=5.0, mean=0.1, std=1.2)
        assert 80 <= score <= 100

    def test_zscore_high_negative_bearish(self) -> None:
        """z < -2.0 → score between -100 and -80."""
        score = score_with_zscore(value=-5.0, mean=0.1, std=1.2)
        assert -100 <= score <= -80

    def test_zscore_near_zero_neutral(self) -> None:
        """z near 0 → score near 0."""
        score = score_with_zscore(value=0.1, mean=0.1, std=1.2)
        assert -10 <= score <= 10

    def test_zscore_moderate_positive(self) -> None:
        """z between +1.0 and +2.0 → score between +40 and +80."""
        # z = (2.5 - 0.1) / 1.2 ≈ 2.0 — boundary, use 1.5
        score = score_with_zscore(value=1.9, mean=0.1, std=1.2)
        # z = (1.9 - 0.1) / 1.2 = 1.5 → should be in +40 to +80
        assert 40 <= score <= 80


class TestScoreEMAAlignment:
    def test_score_ema_alignment_bullish(self) -> None:
        """Perfect bullish stack: price > ema_9 > ema_21 > ema_50 → score near +100."""
        score = score_ema_alignment(close=110.0, ema_9=108.0, ema_21=105.0, ema_50=100.0)
        assert score >= 80

    def test_score_ema_alignment_bearish(self) -> None:
        """Perfect bearish stack: price < ema_9 < ema_21 < ema_50 → score near -100."""
        score = score_ema_alignment(close=90.0, ema_9=92.0, ema_21=95.0, ema_50=100.0)
        assert score <= -80

    def test_score_ema_alignment_mixed(self) -> None:
        """price > ema_9 but ema_9 < ema_21 → neutral-ish score."""
        score = score_ema_alignment(close=110.0, ema_9=108.0, ema_21=109.0, ema_50=100.0)
        assert -50 <= score <= 50


class TestScoreRSI:
    def test_score_rsi_overbought(self) -> None:
        """RSI=75, p80=68, p95=78 → between p80 and p95 → bearish (-80 to -40)."""
        profile = {"p5": 25.0, "p20": 35.0, "p50": 53.0, "p80": 68.0, "p95": 78.0,
                   "mean": 52.0, "std": 12.0}
        score = score_rsi(75.0, profile)
        assert -80 <= score <= -40

    def test_score_rsi_oversold(self) -> None:
        """RSI=25, p5=30 → below p5 → bullish (+80 to +100)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0,
                   "mean": 52.0, "std": 12.0}
        score = score_rsi(25.0, profile)
        assert 80 <= score <= 100

    def test_score_rsi_neutral(self) -> None:
        """RSI=52, p50=53 → neutral (-10 to +10)."""
        profile = {"p5": 30.0, "p20": 40.0, "p50": 53.0, "p80": 67.0, "p95": 78.0,
                   "mean": 52.0, "std": 12.0}
        score = score_rsi(52.0, profile)
        assert -10 <= score <= 10

    def test_score_rsi_uses_stock_profile(self) -> None:
        """Same RSI=70, but different profiles → different scores."""
        # Stock A: p80=67 (70 is above p80 → bearish)
        profile_a = {"p5": 25.0, "p20": 35.0, "p50": 50.0, "p80": 67.0, "p95": 78.0,
                     "mean": 50.0, "std": 12.0}
        # Stock B: p80=78 (70 is below p80 → not overbought for B)
        profile_b = {"p5": 30.0, "p20": 45.0, "p50": 58.0, "p80": 78.0, "p95": 88.0,
                     "mean": 58.0, "std": 12.0}
        score_a = score_rsi(70.0, profile_a)
        score_b = score_rsi(70.0, profile_b)
        assert score_a < score_b  # A should be more bearish than B

    def test_score_rsi_no_profile_uses_fixed(self) -> None:
        """No profile → fall back to fixed thresholds (70=overbought, 30=oversold)."""
        score_overbought = score_rsi(75.0, None)
        score_oversold = score_rsi(25.0, None)
        assert score_overbought < 0
        assert score_oversold > 0


class TestScoreMACDHistogram:
    def test_score_macd_histogram_positive(self) -> None:
        """Positive histogram, high z-score → bullish."""
        profile = {"mean": 0.1, "std": 1.2}
        score = score_macd_histogram(2.5, profile)
        assert score > 0

    def test_score_macd_histogram_negative(self) -> None:
        """Negative histogram, negative z-score → bearish."""
        profile = {"mean": 0.1, "std": 1.2}
        score = score_macd_histogram(-3.0, profile)
        assert score < 0

    def test_score_macd_histogram_uses_zscore(self) -> None:
        """Score reflects z-score magnitude, not raw value."""
        profile = {"mean": 0.1, "std": 1.2}
        # z = (2.5 - 0.1) / 1.2 = 2.0 → strong bullish
        score = score_macd_histogram(2.5, profile)
        assert score >= 60


class TestScoreWilliamsR:
    def test_williams_r_oversold_bullish_with_profile(self) -> None:
        """Williams %R near -100 (oversold) → bullish score with profile."""
        # Profile: typical Williams %R distribution
        profile = {"p5": -92.0, "p20": -75.0, "p50": -50.0, "p80": -25.0, "p95": -8.0}
        score = score_with_percentile(-88.0, profile, higher_is_bullish=False)
        assert score > 0, f"Oversold Williams %R should be bullish, got {score}"

    def test_williams_r_overbought_bearish_with_profile(self) -> None:
        """Williams %R near 0 (overbought) → bearish score with profile."""
        profile = {"p5": -92.0, "p20": -75.0, "p50": -50.0, "p80": -25.0, "p95": -8.0}
        score = score_with_percentile(-5.0, profile, higher_is_bullish=False)
        assert score < 0, f"Overbought Williams %R should be bearish, got {score}"

    def test_williams_r_oversold_bullish_no_profile(self) -> None:
        """Williams %R=-85 (oversold), no profile → bullish (fixed fallback)."""
        indicators = {"williams_r": -85.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={})
        assert result["williams_r"] > 0, f"Oversold WR should be bullish, got {result['williams_r']}"

    def test_williams_r_overbought_bearish_no_profile(self) -> None:
        """Williams %R=-5 (overbought), no profile → bearish (fixed fallback)."""
        indicators = {"williams_r": -5.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={})
        assert result["williams_r"] < 0, f"Overbought WR should be bearish, got {result['williams_r']}"


class TestScoreOBVAndADLine:
    def test_obv_with_profile_bullish(self) -> None:
        """OBV well above its mean → bullish score via z-score."""
        indicators = {"obv": 5_000_000.0}
        profiles = {"obv": {"mean": 1_000_000.0, "std": 1_500_000.0}}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={})
        assert result["obv"] is not None
        assert result["obv"] > 0

    def test_obv_without_profile_returns_none(self) -> None:
        """OBV with no profile → None (cannot score a scalar without context)."""
        indicators = {"obv": 1_000_000.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={})
        assert result["obv"] is None

    def test_ad_line_with_profile_bearish(self) -> None:
        """A/D Line well below its mean → bearish score via z-score."""
        indicators = {"ad_line": -3_000_000.0}
        profiles = {"ad_line": {"mean": 0.0, "std": 1_000_000.0}}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={})
        assert result["ad_line"] is not None
        assert result["ad_line"] < 0

    def test_ad_line_without_profile_returns_none(self) -> None:
        """A/D Line with no profile → None."""
        indicators = {"ad_line": 500_000.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={})
        assert result["ad_line"] is None


class TestScoreADX:
    def test_score_adx_strong_trend(self) -> None:
        """ADX=35 (above trending threshold) → positive score."""
        score = score_adx(35.0)
        assert score >= 40

    def test_score_adx_no_trend(self) -> None:
        """ADX=12 → low/neutral score."""
        score = score_adx(12.0)
        assert score <= 10


class TestScoreAllIndicators:
    def test_score_all_indicators_returns_dict(self) -> None:
        """score_all_indicators returns a dict mapping indicator names to scores."""
        indicators = {
            "rsi_14": 55.0,
            "macd_histogram": 0.5,
            "ema_9": 108.0,
            "ema_21": 105.0,
            "ema_50": 100.0,
            "close": 110.0,
            "adx": 28.0,
        }
        profiles: dict = {}
        result = score_all_indicators(indicators, close=110.0, profiles=profiles, config={})
        assert isinstance(result, dict)
        assert "rsi_14" in result
        assert "macd_histogram" in result
        assert "ema_alignment" in result

    def test_score_all_indicators_handles_none(self) -> None:
        """Indicators that are None are skipped (None in result)."""
        indicators = {
            "rsi_14": None,
            "macd_histogram": 0.5,
            "close": 110.0,
        }
        result = score_all_indicators(indicators, close=110.0, profiles={}, config={})
        assert result.get("rsi_14") is None
        assert result.get("macd_histogram") is not None


class TestScoreRSIRegimeAware:
    """Tests for higher_is_bullish parameter on score_rsi()."""

    _PROFILE = {
        "p5": 25.0, "p20": 35.0, "p50": 53.0,
        "p80": 68.0, "p95": 78.0, "mean": 52.0, "std": 12.0,
    }

    def test_trending_overbought_rsi_is_bullish_with_profile(self) -> None:
        """RSI=75 with higher_is_bullish=True (trending) → bullish (positive score)."""
        score = score_rsi(75.0, self._PROFILE, higher_is_bullish=True)
        assert score > 0, f"Trending RSI=75 should be bullish, got {score}"

    def test_trending_oversold_rsi_is_bearish_with_profile(self) -> None:
        """RSI=25 with higher_is_bullish=True (trending) → bearish (negative score)."""
        score = score_rsi(25.0, self._PROFILE, higher_is_bullish=True)
        assert score < 0, f"Trending RSI=25 should be bearish (downtrend continuation), got {score}"

    def test_trending_overbought_rsi_is_bullish_no_profile(self) -> None:
        """RSI=75, no profile, higher_is_bullish=True → bullish (positive score)."""
        score = score_rsi(75.0, None, higher_is_bullish=True)
        assert score > 0, f"Trending RSI=75 (no profile) should be bullish, got {score}"

    def test_trending_oversold_rsi_is_bearish_no_profile(self) -> None:
        """RSI=25, no profile, higher_is_bullish=True → bearish (negative score)."""
        score = score_rsi(25.0, None, higher_is_bullish=True)
        assert score < 0, f"Trending RSI=25 (no profile) should be bearish, got {score}"

    def test_default_higher_is_bullish_false_unchanged(self) -> None:
        """Default (higher_is_bullish=False) preserves original mean-reversion behaviour."""
        original = score_rsi(75.0, self._PROFILE)
        explicit = score_rsi(75.0, self._PROFILE, higher_is_bullish=False)
        assert original == explicit

    def test_trending_and_ranging_scores_are_opposite_sign(self) -> None:
        """score_rsi(75) trending and ranging should have opposite signs."""
        ranging = score_rsi(75.0, self._PROFILE, higher_is_bullish=False)
        trending = score_rsi(75.0, self._PROFILE, higher_is_bullish=True)
        assert ranging < 0 and trending > 0


class TestScoreAllIndicatorsRegimeAware:
    """Tests for regime-aware oscillator flipping in score_all_indicators()."""

    _PROFILE_RSI = {
        "p5": 25.0, "p20": 35.0, "p50": 53.0,
        "p80": 68.0, "p95": 78.0, "mean": 52.0, "std": 12.0,
    }
    _PROFILE_STOCH = {
        "p5": 8.0, "p20": 20.0, "p50": 50.0,
        "p80": 80.0, "p95": 92.0, "mean": 50.0, "std": 20.0,
    }
    _PROFILE_CCI = {
        "p5": -150.0, "p20": -80.0, "p50": 0.0,
        "p80": 80.0, "p95": 150.0, "mean": 0.0, "std": 60.0,
    }
    _PROFILE_WR = {
        "p5": -95.0, "p20": -75.0, "p50": -50.0,
        "p80": -25.0, "p95": -5.0, "mean": -50.0, "std": 25.0,
    }

    def test_trending_rsi_overbought_is_bullish_with_profile(self) -> None:
        """In trending regime, RSI=75 (overbought) → bullish score."""
        indicators = {"rsi_14": 75.0}
        profiles = {"rsi_14": self._PROFILE_RSI}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="trending")
        assert result["rsi_14"] > 0, f"Expected bullish RSI in trending, got {result['rsi_14']}"

    def test_ranging_rsi_overbought_is_bearish_with_profile(self) -> None:
        """In ranging regime, RSI=75 (overbought) → bearish score (mean-reversion)."""
        indicators = {"rsi_14": 75.0}
        profiles = {"rsi_14": self._PROFILE_RSI}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        assert result["rsi_14"] < 0, f"Expected bearish RSI in ranging, got {result['rsi_14']}"

    def test_volatile_rsi_overbought_is_bearish_with_profile(self) -> None:
        """In volatile regime, RSI=75 (overbought) → bearish score (mean-reversion)."""
        indicators = {"rsi_14": 75.0}
        profiles = {"rsi_14": self._PROFILE_RSI}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="volatile")
        assert result["rsi_14"] < 0, f"Expected bearish RSI in volatile, got {result['rsi_14']}"

    def test_default_regime_omitted_is_ranging(self) -> None:
        """Omitting regime defaults to ranging (backward-compatible mean-reversion)."""
        indicators = {"rsi_14": 75.0}
        profiles = {"rsi_14": self._PROFILE_RSI}
        explicit_ranging = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        default = score_all_indicators(indicators, close=100.0, profiles=profiles, config={})
        assert default["rsi_14"] == explicit_ranging["rsi_14"]

    def test_trending_stoch_overbought_is_bullish_with_profile(self) -> None:
        """In trending regime, Stoch %K=85 (overbought) → bullish score."""
        indicators = {"stoch_k": 85.0}
        profiles = {"stoch_k": self._PROFILE_STOCH}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="trending")
        assert result["stoch_k"] > 0, f"Expected bullish Stoch in trending, got {result['stoch_k']}"

    def test_ranging_stoch_overbought_is_bearish_with_profile(self) -> None:
        """In ranging regime, Stoch %K=85 → bearish score."""
        indicators = {"stoch_k": 85.0}
        profiles = {"stoch_k": self._PROFILE_STOCH}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        assert result["stoch_k"] < 0, f"Expected bearish Stoch in ranging, got {result['stoch_k']}"

    def test_trending_stoch_overbought_is_bullish_no_profile(self) -> None:
        """In trending regime, Stoch %K=85, no profile → bullish (fixed fallback flipped)."""
        indicators = {"stoch_k": 85.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="trending")
        assert result["stoch_k"] > 0, f"Expected bullish Stoch no-profile trending, got {result['stoch_k']}"

    def test_ranging_stoch_overbought_is_bearish_no_profile(self) -> None:
        """In ranging regime, Stoch %K=85, no profile → bearish (fixed fallback)."""
        indicators = {"stoch_k": 85.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="ranging")
        assert result["stoch_k"] < 0, f"Expected bearish Stoch no-profile ranging, got {result['stoch_k']}"

    def test_trending_cci_high_is_bullish_with_profile(self) -> None:
        """In trending regime, CCI=150 (overbought) → bullish score."""
        indicators = {"cci_20": 150.0}
        profiles = {"cci_20": self._PROFILE_CCI}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="trending")
        assert result["cci_20"] > 0, f"Expected bullish CCI in trending, got {result['cci_20']}"

    def test_ranging_cci_high_is_bearish_with_profile(self) -> None:
        """In ranging regime, CCI=150 → bearish score."""
        indicators = {"cci_20": 150.0}
        profiles = {"cci_20": self._PROFILE_CCI}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        assert result["cci_20"] < 0, f"Expected bearish CCI in ranging, got {result['cci_20']}"

    def test_trending_cci_high_is_bullish_no_profile(self) -> None:
        """In trending regime, CCI=200, no profile → bullish (fixed fallback flipped)."""
        indicators = {"cci_20": 200.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="trending")
        assert result["cci_20"] > 0, f"Expected bullish CCI no-profile trending, got {result['cci_20']}"

    def test_ranging_cci_high_is_bearish_no_profile(self) -> None:
        """In ranging regime, CCI=200, no profile → bearish (fixed fallback)."""
        indicators = {"cci_20": 200.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="ranging")
        assert result["cci_20"] < 0, f"Expected bearish CCI no-profile ranging, got {result['cci_20']}"

    def test_trending_williams_r_overbought_is_bullish_with_profile(self) -> None:
        """In trending regime, Williams %R=-5 (overbought) → bullish score."""
        indicators = {"williams_r": -5.0}
        profiles = {"williams_r": self._PROFILE_WR}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="trending")
        assert result["williams_r"] > 0, f"Expected bullish WR in trending, got {result['williams_r']}"

    def test_ranging_williams_r_overbought_is_bearish_with_profile(self) -> None:
        """In ranging regime, Williams %R=-5 → bearish score."""
        indicators = {"williams_r": -5.0}
        profiles = {"williams_r": self._PROFILE_WR}
        result = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        assert result["williams_r"] < 0, f"Expected bearish WR in ranging, got {result['williams_r']}"

    def test_trending_williams_r_overbought_is_bullish_no_profile(self) -> None:
        """In trending regime, Williams %R=-5, no profile → bullish (fixed fallback flipped)."""
        indicators = {"williams_r": -5.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="trending")
        assert result["williams_r"] > 0, f"Expected bullish WR no-profile trending, got {result['williams_r']}"

    def test_ranging_williams_r_overbought_is_bearish_no_profile(self) -> None:
        """In ranging regime, Williams %R=-5, no profile → bearish (fixed fallback)."""
        indicators = {"williams_r": -5.0}
        result = score_all_indicators(indicators, close=100.0, profiles={}, config={}, regime="ranging")
        assert result["williams_r"] < 0, f"Expected bearish WR no-profile ranging, got {result['williams_r']}"

    def test_bb_pctb_not_flipped_in_trending_regime(self) -> None:
        """BB %B is never flipped — high value stays bearish even in trending regime."""
        profile_bb = {
            "p5": 0.05, "p20": 0.2, "p50": 0.5,
            "p80": 0.8, "p95": 0.95, "mean": 0.5, "std": 0.2,
        }
        indicators = {"bb_pctb": 0.95}
        profiles = {"bb_pctb": profile_bb}
        ranging = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="ranging")
        trending = score_all_indicators(indicators, close=100.0, profiles=profiles, config={}, regime="trending")
        assert ranging["bb_pctb"] < 0, "BB %B near upper band should be bearish in ranging"
        assert trending["bb_pctb"] == ranging["bb_pctb"], "BB %B should not change between regimes"
