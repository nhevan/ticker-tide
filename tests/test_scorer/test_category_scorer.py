"""
Tests for src/scorer/category_scorer.py — category rollup and adaptive weights.
"""

from __future__ import annotations

import pytest

from src.scorer.category_scorer import (
    apply_adaptive_weights,
    compute_all_category_scores,
    rollup_category,
)


TRENDING_WEIGHTS = {
    "trend": 0.30, "momentum": 0.15, "volume": 0.10, "volatility": 0.05,
    "candlestick": 0.05, "structural": 0.15, "sentiment": 0.10,
    "fundamental": 0.05, "macro": 0.05,
}

RANGING_WEIGHTS = {
    "trend": 0.10, "momentum": 0.25, "volume": 0.10, "volatility": 0.10,
    "candlestick": 0.10, "structural": 0.15, "sentiment": 0.10,
    "fundamental": 0.05, "macro": 0.05,
}


class TestRollupCategory:
    def test_rollup_trend_category(self) -> None:
        """Average of ema_alignment=+80, macd=+60, adx=+40, crossover=+70 = 62.5."""
        scores = {
            "ema_alignment": 80.0,
            "macd_histogram": 60.0,
            "adx": 40.0,
            "crossover_ema_9_21": 70.0,
        }
        result = rollup_category("trend", scores)
        assert result == pytest.approx(62.5, abs=0.1)

    def test_rollup_handles_missing_components(self) -> None:
        """None values excluded from denominator."""
        scores = {"ema_alignment": 80.0, "macd_histogram": None, "adx": 60.0}
        result = rollup_category("trend", scores)
        assert result == pytest.approx(70.0, abs=0.1)

    def test_rollup_all_components_none(self) -> None:
        """All scores None → 0 (no data for this category)."""
        scores = {"ema_alignment": None, "macd_histogram": None}
        result = rollup_category("trend", scores)
        assert result == 0

    def test_rollup_result_clamped(self) -> None:
        """Category rollup result is clamped to [-100, +100]."""
        scores = {"a": 150.0, "b": 200.0}
        result = rollup_category("test", scores)
        assert result <= 100

    def test_rollup_negative_average_clamped(self) -> None:
        """Very negative average is clamped to -100."""
        scores = {"a": -150.0, "b": -200.0}
        result = rollup_category("test", scores)
        assert result >= -100


class TestComputeAllCategoryScores:
    def test_compute_all_category_scores_returns_nine_categories(self) -> None:
        """compute_all_category_scores returns dict with all 9 category names."""
        indicator_scores = {
            "ema_alignment": 60.0, "macd_histogram": 40.0, "adx": 30.0,
            "rsi_14": -20.0, "stoch_k": -15.0, "cci_20": -10.0, "williams_r": -25.0,
            "obv": 30.0, "cmf_20": 20.0, "ad_line": 15.0,
            "bb_pctb": -10.0, "atr_14": 0.0,
        }
        pattern_scores = {
            "candlestick_pattern_score": 45.0,
            "structural_pattern_score": 60.0,
            "gap_score": 30.0,
            "fibonacci_score": 20.0,
            "divergence_rsi": 40.0,
            "divergence_macd": 30.0,
            "crossover_ema_9_21": 50.0,
        }
        sentiment_scores = {"news_sentiment_score": 30.0, "short_interest_score": -20.0}
        result = compute_all_category_scores(
            indicator_scores=indicator_scores,
            pattern_scores=pattern_scores,
            sentiment_scores=sentiment_scores,
            fundamental_score=25.0,
            macro_score=30.0,
        )
        expected_categories = {
            "trend", "momentum", "volume", "volatility", "candlestick",
            "structural", "sentiment", "fundamental", "macro",
        }
        assert set(result.keys()) == expected_categories

    def test_compute_all_category_scores_values_in_range(self) -> None:
        """All category scores are between -100 and +100."""
        indicator_scores = {
            "ema_alignment": 60.0, "macd_histogram": 40.0, "adx": 30.0,
            "rsi_14": -20.0, "stoch_k": -15.0, "cci_20": -10.0, "williams_r": -25.0,
            "obv": 30.0, "cmf_20": 20.0, "ad_line": 15.0,
            "bb_pctb": -10.0, "atr_14": 0.0,
        }
        result = compute_all_category_scores(
            indicator_scores=indicator_scores,
            pattern_scores={},
            sentiment_scores={},
            fundamental_score=0.0,
            macro_score=0.0,
        )
        for category, score in result.items():
            assert -100 <= score <= 100, f"{category} score {score} out of range"


class TestApplyAdaptiveWeights:
    def test_apply_adaptive_weights_trending_uniform(self) -> None:
        """All categories at +50, trending regime → weighted score = 50 (weights cancel)."""
        category_scores = {cat: 50.0 for cat in TRENDING_WEIGHTS}
        result = apply_adaptive_weights(category_scores, TRENDING_WEIGHTS)
        assert result == pytest.approx(50.0, abs=0.01)

    def test_apply_adaptive_weights_different_scores(self) -> None:
        """trend=+80, momentum=+20, volume=+60, rest=0, trending weights → 33.0."""
        category_scores = {
            "trend": 80.0, "momentum": 20.0, "volume": 60.0,
            "volatility": 0.0, "candlestick": 0.0, "structural": 0.0,
            "sentiment": 0.0, "fundamental": 0.0, "macro": 0.0,
        }
        # 0.30*80 + 0.15*20 + 0.10*60 + 0 + ... = 24 + 3 + 6 = 33
        result = apply_adaptive_weights(category_scores, TRENDING_WEIGHTS)
        assert result == pytest.approx(33.0, abs=0.01)

    def test_apply_adaptive_weights_ranging_vs_trending(self) -> None:
        """Same scores → ranging regime produces different weighted score than trending."""
        category_scores = {
            "trend": 80.0, "momentum": 20.0, "volume": 0.0,
            "volatility": 0.0, "candlestick": 0.0, "structural": 0.0,
            "sentiment": 0.0, "fundamental": 0.0, "macro": 0.0,
        }
        score_trending = apply_adaptive_weights(category_scores, TRENDING_WEIGHTS)
        score_ranging = apply_adaptive_weights(category_scores, RANGING_WEIGHTS)
        # trending weights trend=0.30, ranging weights trend=0.10 → different results
        assert score_trending != score_ranging

    def test_weighted_score_is_clamped(self) -> None:
        """Weighted score is clamped to [-100, +100]."""
        # Give extreme scores to high-weight categories
        category_scores = {cat: 200.0 for cat in TRENDING_WEIGHTS}
        result = apply_adaptive_weights(category_scores, TRENDING_WEIGHTS)
        assert result <= 100

        category_scores_neg = {cat: -200.0 for cat in TRENDING_WEIGHTS}
        result_neg = apply_adaptive_weights(category_scores_neg, TRENDING_WEIGHTS)
        assert result_neg >= -100
