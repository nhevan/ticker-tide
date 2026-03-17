"""
Tests for src/scorer/pattern_scorer.py — pattern and signal scoring.
"""

from __future__ import annotations

import pytest

from src.scorer.pattern_scorer import (
    score_candlestick_patterns,
    score_crossovers,
    score_divergences,
    score_fibonacci,
    score_fundamentals,
    score_gaps,
    score_macro,
    score_news_sentiment,
    score_short_interest,
    score_structural_patterns,
)


class TestScoreCandlestickPatterns:
    def test_score_candlestick_bullish_engulfing(self) -> None:
        """Bullish engulfing, strength=3 → positive score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bullish", "strength": 3, "pattern_name": "engulfing"}
        ]
        score = score_candlestick_patterns(patterns, scoring_date="2024-01-10")
        assert score > 0

    def test_score_candlestick_bearish_engulfing(self) -> None:
        """Bearish engulfing, strength=4 → negative score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bearish", "strength": 4, "pattern_name": "engulfing"}
        ]
        score = score_candlestick_patterns(patterns, scoring_date="2024-01-10")
        assert score < 0

    def test_score_candlestick_no_patterns(self) -> None:
        """Empty pattern list → 0 (neutral)."""
        score = score_candlestick_patterns([], scoring_date="2024-01-10")
        assert score == 0

    def test_score_candlestick_mixed_patterns(self) -> None:
        """2 bullish + 1 bearish → net positive (majority bullish)."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bullish", "strength": 3, "pattern_name": "hammer"},
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bullish", "strength": 2, "pattern_name": "morning_star"},
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bearish", "strength": 2, "pattern_name": "shooting_star"},
        ]
        score = score_candlestick_patterns(patterns, scoring_date="2024-01-10")
        assert score > 0

    def test_score_candlestick_strength_affects_score(self) -> None:
        """strength=5 produces a higher magnitude score than strength=1."""
        patterns_strong = [
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bullish", "strength": 5, "pattern_name": "engulfing"}
        ]
        patterns_weak = [
            {"date": "2024-01-10", "pattern_category": "candlestick",
             "direction": "bullish", "strength": 1, "pattern_name": "engulfing"}
        ]
        score_strong = score_candlestick_patterns(patterns_strong, scoring_date="2024-01-10")
        score_weak = score_candlestick_patterns(patterns_weak, scoring_date="2024-01-10")
        assert score_strong > score_weak


class TestScoreStructuralPatterns:
    def test_score_structural_double_bottom(self) -> None:
        """double_bottom, bullish → strong positive score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "double_bottom", "direction": "bullish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score > 30

    def test_score_structural_double_top(self) -> None:
        """double_top → strong negative score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "double_top", "direction": "bearish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score < -30

    def test_score_structural_breakout(self) -> None:
        """breakout, bullish → positive score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "breakout", "direction": "bullish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score > 0

    def test_score_structural_breakdown(self) -> None:
        """breakdown, bearish → negative score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "breakdown", "direction": "bearish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score < 0

    def test_score_structural_bull_flag(self) -> None:
        """bull_flag → positive score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "bull_flag", "direction": "bullish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score > 0

    def test_score_structural_bear_flag(self) -> None:
        """bear_flag → negative score."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "bear_flag", "direction": "bearish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score < 0

    def test_score_structural_false_breakout(self) -> None:
        """false_breakout → negative score (bearish trap)."""
        patterns = [
            {"date": "2024-01-10", "pattern_category": "structural",
             "pattern_name": "false_breakout", "direction": "bearish", "strength": 3}
        ]
        score = score_structural_patterns(patterns, scoring_date="2024-01-10")
        assert score < 0

    def test_score_structural_recency_weighting(self) -> None:
        """A breakout from 1 day ago gets a higher score than one from 15 days ago."""
        patterns_recent = [
            {"date": "2024-01-09", "pattern_category": "structural",
             "pattern_name": "breakout", "direction": "bullish", "strength": 3}
        ]
        patterns_old = [
            {"date": "2023-12-20", "pattern_category": "structural",
             "pattern_name": "breakout", "direction": "bullish", "strength": 3}
        ]
        score_recent = score_structural_patterns(patterns_recent, scoring_date="2024-01-10")
        score_old = score_structural_patterns(patterns_old, scoring_date="2024-01-10")
        assert score_recent > score_old


class TestScoreGaps:
    def test_score_gaps_bullish_breakaway(self) -> None:
        """breakaway gap up → positive score."""
        gaps = [
            {"date": "2024-01-10", "gap_type": "breakaway",
             "direction": "up", "gap_size_pct": 2.5}
        ]
        score = score_gaps(gaps, scoring_date="2024-01-10")
        assert score > 0

    def test_score_gaps_bearish_breakaway(self) -> None:
        """breakaway gap down → negative score."""
        gaps = [
            {"date": "2024-01-10", "gap_type": "breakaway",
             "direction": "down", "gap_size_pct": 2.5}
        ]
        score = score_gaps(gaps, scoring_date="2024-01-10")
        assert score < 0

    def test_score_gaps_common_low_weight(self) -> None:
        """common gap → small score magnitude."""
        gaps_common = [
            {"date": "2024-01-10", "gap_type": "common",
             "direction": "up", "gap_size_pct": 0.5}
        ]
        gaps_breakaway = [
            {"date": "2024-01-10", "gap_type": "breakaway",
             "direction": "up", "gap_size_pct": 0.5}
        ]
        score_common = score_gaps(gaps_common, scoring_date="2024-01-10")
        score_breakaway = score_gaps(gaps_breakaway, scoring_date="2024-01-10")
        assert abs(score_common) < abs(score_breakaway)


class TestScoreFibonacci:
    def test_score_fibonacci_near_support(self) -> None:
        """Price near 61.8% retracement (strong support) → bullish score (legacy dict)."""
        fib_result = {
            "near_level": True,
            "level_pct": 61.8,
            "direction": "support",
        }
        score = score_fibonacci(fib_result)
        assert score > 0

    def test_score_fibonacci_not_near_level(self) -> None:
        """Price not near any fib level → 0."""
        fib_result = {"near_level": False}
        score = score_fibonacci(fib_result)
        assert score == 0

    def test_score_fibonacci_none(self) -> None:
        """fib_result=None → 0."""
        score = score_fibonacci(None)
        assert score == 0

    def test_score_fibonacci_real_calculator_output_support(self) -> None:
        """AAPL near 38.2% level; price above swing midpoint → support → bullish (+25).

        Mirrors the real compute_fibonacci_for_ticker() output shape.
        swing midpoint = (200+290)/2 = 245; current_price=252.82 > 245 → support.
        """
        fib_result = {
            "swing_low": {"price": 200.0, "date": "2023-10-01"},
            "swing_high": {"price": 290.0, "date": "2024-01-01"},
            "levels": [],
            "current_price": 252.82,
            "nearest_level": {
                "level_pct": 38.2,
                "level_price": 252.88,
                "distance_pct": 0.025,
                "is_near": True,
            },
            "is_near_level": True,
        }
        score = score_fibonacci(fib_result)
        assert score != 0, "Should return non-zero score when is_near_level=True"
        assert score == pytest.approx(25.0)  # 38.2% level = +25 (support)

    def test_score_fibonacci_real_calculator_output_resistance(self) -> None:
        """Price below swing midpoint → recovering → fib level is resistance → bearish (-25).

        swing midpoint = (200+290)/2 = 245; current_price=235.0 < 245 → resistance.
        """
        fib_result = {
            "swing_low": {"price": 200.0, "date": "2023-10-01"},
            "swing_high": {"price": 290.0, "date": "2024-01-01"},
            "levels": [],
            "current_price": 235.0,
            "nearest_level": {
                "level_pct": 38.2,
                "level_price": 234.5,
                "distance_pct": 0.2,
                "is_near": True,
            },
            "is_near_level": True,
        }
        score = score_fibonacci(fib_result)
        assert score != 0
        assert score == pytest.approx(-25.0)  # 38.2% level = -25 (resistance)

    def test_score_fibonacci_real_is_near_level_false(self) -> None:
        """is_near_level=False → 0 even when nearest_level dict is present."""
        fib_result = {
            "current_price": 270.0,
            "nearest_level": None,
            "is_near_level": False,
        }
        score = score_fibonacci(fib_result)
        assert score == 0

    def test_score_fibonacci_61_8_level_higher_score_than_38_2(self) -> None:
        """61.8% level scores higher magnitude than 38.2% (stronger support zone).

        Both prices above midpoint → both support → both positive; 61.8 > 38.2.
        """
        fib_618 = {
            "swing_low": {"price": 200.0, "date": "2023-10-01"},
            "swing_high": {"price": 290.0, "date": "2024-01-01"},
            "is_near_level": True,
            "nearest_level": {"level_pct": 61.8, "level_price": 255.0},
            "current_price": 254.9,  # above midpoint 245 → support
        }
        fib_382 = {
            "swing_low": {"price": 200.0, "date": "2023-10-01"},
            "swing_high": {"price": 290.0, "date": "2024-01-01"},
            "is_near_level": True,
            "nearest_level": {"level_pct": 38.2, "level_price": 252.0},
            "current_price": 251.9,  # above midpoint 245 → support
        }
        assert score_fibonacci(fib_618) > score_fibonacci(fib_382)


class TestScoreDivergences:
    def test_score_divergences_regular_bullish(self) -> None:
        """Regular bullish RSI divergence → strong positive score."""
        divergences = [
            {"date": "2024-01-10", "indicator": "rsi_14",
             "divergence_type": "regular_bullish", "strength": 3}
        ]
        score = score_divergences(divergences, scoring_date="2024-01-10")
        assert score > 30

    def test_score_divergences_regular_bearish(self) -> None:
        """Regular bearish divergence → strong negative score."""
        divergences = [
            {"date": "2024-01-10", "indicator": "rsi_14",
             "divergence_type": "regular_bearish", "strength": 3}
        ]
        score = score_divergences(divergences, scoring_date="2024-01-10")
        assert score < -30

    def test_score_divergences_hidden_bullish(self) -> None:
        """Hidden bullish divergence → moderate positive score."""
        divergences = [
            {"date": "2024-01-10", "indicator": "rsi_14",
             "divergence_type": "hidden_bullish", "strength": 3}
        ]
        score = score_divergences(divergences, scoring_date="2024-01-10")
        assert score > 0

    def test_score_divergences_multiple(self) -> None:
        """RSI bullish + MACD bearish → net score accounts for both."""
        divergences = [
            {"date": "2024-01-10", "indicator": "rsi_14",
             "divergence_type": "regular_bullish", "strength": 3},
            {"date": "2024-01-10", "indicator": "macd_histogram",
             "divergence_type": "regular_bearish", "strength": 3},
        ]
        score_both = score_divergences(divergences, scoring_date="2024-01-10")
        # Net should be less extreme than a single bullish divergence
        divergences_bullish_only = [divergences[0]]
        score_bullish_only = score_divergences(divergences_bullish_only, scoring_date="2024-01-10")
        assert abs(score_both) < abs(score_bullish_only) or score_both != score_bullish_only


class TestScoreCrossovers:
    def test_score_crossovers_bullish_recent(self) -> None:
        """EMA 9/21 bullish crossover, 1 day ago → strong positive score."""
        crossovers = [
            {"date": "2024-01-09", "crossover_type": "ema_9_21",
             "direction": "bullish", "days_ago": 1}
        ]
        score = score_crossovers(crossovers, scoring_date="2024-01-10")
        assert score > 20

    def test_score_crossovers_bullish_old(self) -> None:
        """Same crossover but 9 days ago → lower positive score (decayed)."""
        crossovers_recent = [
            {"date": "2024-01-09", "crossover_type": "ema_9_21",
             "direction": "bullish", "days_ago": 1}
        ]
        crossovers_old = [
            {"date": "2024-01-01", "crossover_type": "ema_9_21",
             "direction": "bullish", "days_ago": 9}
        ]
        score_recent = score_crossovers(crossovers_recent, scoring_date="2024-01-10")
        score_old = score_crossovers(crossovers_old, scoring_date="2024-01-10")
        assert score_recent > score_old

    def test_score_crossovers_bearish(self) -> None:
        """Bearish crossover → negative score."""
        crossovers = [
            {"date": "2024-01-09", "crossover_type": "ema_9_21",
             "direction": "bearish", "days_ago": 1}
        ]
        score = score_crossovers(crossovers, scoring_date="2024-01-10")
        assert score < 0


class TestScoreShortInterest:
    def test_score_short_interest_high(self) -> None:
        """days_to_cover=8 → bearish score."""
        score = score_short_interest(8.0)
        assert score < -30

    def test_score_short_interest_low(self) -> None:
        """days_to_cover=1 → near-neutral score."""
        score = score_short_interest(1.0)
        assert -20 <= score <= 5

    def test_score_short_interest_none(self) -> None:
        """None → 0."""
        score = score_short_interest(None)
        assert score == 0


class TestScoreNewsSentiment:
    def test_score_news_sentiment_positive(self) -> None:
        """avg_sentiment=0.6, 5 articles → positive score."""
        score = score_news_sentiment(avg_sentiment=0.6, article_count=5, filing_flag=False)
        assert score > 0

    def test_score_news_sentiment_negative(self) -> None:
        """avg_sentiment=-0.5 → negative score."""
        score = score_news_sentiment(avg_sentiment=-0.5, article_count=5, filing_flag=False)
        assert score < 0

    def test_score_news_sentiment_none(self) -> None:
        """avg_sentiment=None → 0."""
        score = score_news_sentiment(avg_sentiment=None, article_count=0, filing_flag=False)
        assert score == 0


class TestScoreFundamentals:
    def test_score_fundamentals_positive(self) -> None:
        """Good fundamentals → positive score."""
        fundamentals = {
            "pe_ratio": 15.0,
            "sector_pe_median": 22.0,  # undervalued
            "eps_growth_yoy": 0.15,    # 15% growth
            "revenue_growth_yoy": 0.10,
            "debt_to_equity": 0.3,
        }
        score = score_fundamentals(fundamentals)
        assert score > 0

    def test_score_fundamentals_none(self) -> None:
        """None → 0."""
        score = score_fundamentals(None)
        assert score == 0


class TestScoreMacro:
    def test_score_macro_all_positive(self) -> None:
        """All positive macro signals → positive score."""
        score = score_macro(
            spy_trend=50.0,
            vix_score=30.0,
            sector_etf_trend=40.0,
            treasury_trend=-10.0,
            rs_market=60.0,
            rs_sector=40.0,
        )
        assert score > 0

    def test_score_macro_handles_none(self) -> None:
        """None rs values are skipped, no crash."""
        score = score_macro(
            spy_trend=50.0,
            vix_score=30.0,
            sector_etf_trend=40.0,
            treasury_trend=-10.0,
            rs_market=None,
            rs_sector=None,
        )
        assert isinstance(score, float)
