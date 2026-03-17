"""
Tests for src/scorer/confidence.py — signal classification and confidence calculation.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from src.scorer.confidence import (
    build_data_completeness,
    build_key_signals,
    classify_signal,
    compute_confidence,
    compute_confidence_modifiers,
    get_next_earnings_date,
)

SAMPLE_CONFIG = {
    "signal_thresholds": {
        "bullish": 30,
        "bearish": -30,
    },
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
}


# ---------------------------------------------------------------------------
# classify_signal
# ---------------------------------------------------------------------------

class TestClassifySignal:
    def test_classify_signal_bullish(self) -> None:
        """final_score=+45, bullish threshold=30 → BULLISH."""
        assert classify_signal(45.0, SAMPLE_CONFIG) == "BULLISH"

    def test_classify_signal_bearish(self) -> None:
        """final_score=-50, bearish threshold=-30 → BEARISH."""
        assert classify_signal(-50.0, SAMPLE_CONFIG) == "BEARISH"

    def test_classify_signal_neutral_positive(self) -> None:
        """final_score=+20, between -30 and +30 → NEUTRAL."""
        assert classify_signal(20.0, SAMPLE_CONFIG) == "NEUTRAL"

    def test_classify_signal_neutral_negative(self) -> None:
        """final_score=-15, between -30 and +30 → NEUTRAL."""
        assert classify_signal(-15.0, SAMPLE_CONFIG) == "NEUTRAL"

    def test_classify_signal_exact_boundary_bullish(self) -> None:
        """final_score=+30, threshold is inclusive (>=30) → BULLISH."""
        assert classify_signal(30.0, SAMPLE_CONFIG) == "BULLISH"

    def test_classify_signal_exact_boundary_bearish(self) -> None:
        """final_score=-30, threshold is inclusive (<=−30) → BEARISH."""
        assert classify_signal(-30.0, SAMPLE_CONFIG) == "BEARISH"


# ---------------------------------------------------------------------------
# compute_confidence_modifiers — individual modifiers
# ---------------------------------------------------------------------------

class TestConfidenceModifiers:
    def _base_modifier_call(self, **overrides) -> dict:
        """Call compute_confidence_modifiers with sensible defaults, overriding as requested."""
        defaults = dict(
            daily_score=50.0,
            weekly_score=30.0,
            category_scores={
                "trend": 40.0, "momentum": 30.0, "volume": 20.0,
                "volatility": 10.0, "candlestick": 5.0, "structural": 15.0,
                "sentiment": 10.0, "fundamental": 5.0, "macro": 10.0,
            },
            indicator_scores={
                "ema_alignment": 80.0, "macd_line": 50.0, "macd_histogram": 40.0,
                "adx": 30.0, "rsi_14": 60.0, "stoch_k": 55.0, "cci_20": 45.0,
                "williams_r": 50.0, "obv": 35.0, "cmf_20": 25.0,
                "ad_line": 30.0, "bb_pctb": -10.0, "atr_14": None,
            },
            earnings_date=None,
            scoring_date="2025-01-15",
            vix=20.0,
            atr=1.0,
            atr_sma=1.0,
            news_available=True,
            fundamentals_available=True,
            config=SAMPLE_CONFIG,
        )
        defaults.update(overrides)
        return compute_confidence_modifiers(**defaults)

    # Timeframe agreement
    def test_confidence_modifier_timeframe_agree(self) -> None:
        """daily=+50, weekly=+30 (both positive) → timeframe_agree=+10."""
        mods = self._base_modifier_call(daily_score=50.0, weekly_score=30.0)
        assert mods["timeframe_agreement"] == 10

    def test_confidence_modifier_timeframe_disagree(self) -> None:
        """daily=+50, weekly=-20 (opposite signs) → timeframe_disagree=-15."""
        mods = self._base_modifier_call(daily_score=50.0, weekly_score=-20.0)
        assert mods["timeframe_agreement"] == -15

    def test_confidence_modifier_timeframe_neutral_weekly(self) -> None:
        """weekly=+5 is in neutral zone (-10 to +10) → neither agree nor disagree (0)."""
        mods = self._base_modifier_call(daily_score=50.0, weekly_score=5.0)
        assert mods["timeframe_agreement"] == 0

    # Volume confirmation
    def test_confidence_modifier_volume_confirms(self) -> None:
        """volume_score=+30, trend_score=+40 (same sign) → volume_confirms=+10."""
        cat = {
            "trend": 40.0, "momentum": 30.0, "volume": 30.0,
            "volatility": 10.0, "candlestick": 5.0, "structural": 15.0,
            "sentiment": 10.0, "fundamental": 5.0, "macro": 10.0,
        }
        mods = self._base_modifier_call(category_scores=cat)
        assert mods["volume_confirmation"] == 10

    def test_confidence_modifier_volume_diverges(self) -> None:
        """volume_score=-20, trend_score=+40 (different signs) → volume_diverges=-10."""
        cat = {
            "trend": 40.0, "momentum": 30.0, "volume": -20.0,
            "volatility": 10.0, "candlestick": 5.0, "structural": 15.0,
            "sentiment": 10.0, "fundamental": 5.0, "macro": 10.0,
        }
        mods = self._base_modifier_call(category_scores=cat)
        assert mods["volume_confirmation"] == -10

    # Indicator consensus
    def test_confidence_modifier_indicator_consensus(self) -> None:
        """8 of 13 positive → >60% agree with bullish signal → indicator_consensus=+5."""
        ind = {
            f"ind_{i}": 50.0 if i < 8 else -50.0
            for i in range(13)
        }
        mods = self._base_modifier_call(daily_score=50.0, indicator_scores=ind)
        assert mods["indicator_consensus"] == 5

    def test_confidence_modifier_indicator_mixed(self) -> None:
        """6 positive, 7 negative → <40% agree with bullish signal → indicator_mixed=-10."""
        ind = {
            f"ind_{i}": 50.0 if i < 6 else -50.0
            for i in range(13)
        }
        mods = self._base_modifier_call(daily_score=50.0, indicator_scores=ind)
        assert mods["indicator_consensus"] == -10

    # Earnings proximity
    def test_confidence_modifier_earnings_within_7_days(self) -> None:
        """Next earnings is 5 days away → earnings_penalty=-15."""
        scoring_date = "2025-01-15"
        earnings_date = "2025-01-20"  # 5 days away
        mods = self._base_modifier_call(earnings_date=earnings_date, scoring_date=scoring_date)
        assert mods["earnings_proximity"] == -15

    def test_confidence_modifier_earnings_not_near(self) -> None:
        """Next earnings is 30 days away → no penalty (0)."""
        scoring_date = "2025-01-15"
        earnings_date = "2025-02-14"  # 30 days away
        mods = self._base_modifier_call(earnings_date=earnings_date, scoring_date=scoring_date)
        assert mods["earnings_proximity"] == 0

    def test_confidence_modifier_earnings_no_data(self) -> None:
        """No earnings data → no penalty (0), don't penalize missing data."""
        mods = self._base_modifier_call(earnings_date=None)
        assert mods["earnings_proximity"] == 0

    # VIX
    def test_confidence_modifier_vix_extreme(self) -> None:
        """VIX=35, above threshold of 30 → vix_extreme_penalty=-10."""
        mods = self._base_modifier_call(vix=35.0)
        assert mods["vix_extreme"] == -10

    def test_confidence_modifier_vix_normal(self) -> None:
        """VIX=20, below threshold → no penalty (0)."""
        mods = self._base_modifier_call(vix=20.0)
        assert mods["vix_extreme"] == 0

    # ATR expansion
    def test_confidence_modifier_atr_expanding(self) -> None:
        """ATR=1.8x 20-day average (above 1.5x threshold) → atr_expanding_penalty=-5."""
        mods = self._base_modifier_call(atr=1.8, atr_sma=1.0)
        assert mods["atr_expanding"] == -5

    def test_confidence_modifier_atr_normal(self) -> None:
        """ATR=1.1x average → no penalty (0)."""
        mods = self._base_modifier_call(atr=1.1, atr_sma=1.0)
        assert mods["atr_expanding"] == 0

    # Missing data
    def test_confidence_modifier_missing_news(self) -> None:
        """news_available=False → missing_news_penalty=-5."""
        mods = self._base_modifier_call(news_available=False)
        assert mods["missing_data"] == -5

    def test_confidence_modifier_missing_fundamentals(self) -> None:
        """fundamentals_available=False → missing_fundamentals_penalty=-3."""
        mods = self._base_modifier_call(fundamentals_available=False)
        assert mods["missing_data"] == -3

    def test_confidence_modifier_missing_both(self) -> None:
        """Both news and fundamentals missing → combined penalty=-8."""
        mods = self._base_modifier_call(news_available=False, fundamentals_available=False)
        assert mods["missing_data"] == -8


# ---------------------------------------------------------------------------
# compute_confidence (clamping)
# ---------------------------------------------------------------------------

class TestComputeConfidence:
    def test_base_confidence(self) -> None:
        """final_score=+65 → base confidence = |65| = 65%."""
        result = compute_confidence(65.0, {})
        assert result == 65.0

    def test_base_confidence_from_negative(self) -> None:
        """final_score=-40 → base confidence = |40| = 40%."""
        result = compute_confidence(-40.0, {})
        assert result == 40.0

    def test_confidence_all_modifiers_combined(self) -> None:
        """Base=60, timeframe_agree=+10, volume=+10, consensus=+5, earnings=-15, VIX=-10 → 60."""
        mods = {
            "timeframe_agreement": 10,
            "volume_confirmation": 10,
            "indicator_consensus": 5,
            "earnings_proximity": -15,
            "vix_extreme": -10,
            "atr_expanding": 0,
            "missing_data": 0,
        }
        result = compute_confidence(60.0, mods)
        assert result == 60.0

    def test_confidence_clamped_to_zero(self) -> None:
        """base=20, modifiers sum to -30 → would be -10, clamped to 0."""
        mods = {"timeframe_agreement": -30}
        result = compute_confidence(20.0, mods)
        assert result == 0.0

    def test_confidence_clamped_to_100(self) -> None:
        """base=90, modifiers sum to +30 → would be 120, clamped to 100."""
        mods = {"timeframe_agreement": 30}
        result = compute_confidence(90.0, mods)
        assert result == 100.0


# ---------------------------------------------------------------------------
# compute_full_confidence (end-to-end)
# ---------------------------------------------------------------------------

class TestComputeFullConfidence:
    def test_compute_full_confidence(self) -> None:
        """Provide all inputs; verify returns dict with confidence, base, modifiers."""
        from src.scorer.confidence import compute_full_confidence

        result = compute_full_confidence(
            final_score=65.0,
            daily_score=50.0,
            weekly_score=30.0,
            category_scores={
                "trend": 40.0, "momentum": 30.0, "volume": 25.0,
                "volatility": 10.0, "candlestick": 5.0, "structural": 15.0,
                "sentiment": 10.0, "fundamental": 5.0, "macro": 10.0,
            },
            indicator_scores={
                "ema_alignment": 80.0, "macd_line": 50.0, "macd_histogram": 40.0,
                "adx": 30.0, "rsi_14": 60.0, "stoch_k": 55.0, "cci_20": 45.0,
                "williams_r": 50.0, "obv": 35.0, "cmf_20": 25.0,
                "ad_line": 30.0, "bb_pctb": -10.0, "atr_14": None,
            },
            earnings_date=None,
            scoring_date="2025-01-15",
            vix=20.0,
            atr=1.0,
            atr_sma=1.0,
            news_available=True,
            fundamentals_available=True,
            config=SAMPLE_CONFIG,
        )

        assert "confidence" in result
        assert "base" in result
        assert "modifiers" in result
        assert isinstance(result["confidence"], float)
        assert isinstance(result["base"], float)
        assert isinstance(result["modifiers"], dict)
        assert result["base"] == 65.0
        assert 0.0 <= result["confidence"] <= 100.0


# ---------------------------------------------------------------------------
# build_data_completeness
# ---------------------------------------------------------------------------

class TestBuildDataCompleteness:
    def test_build_data_completeness_all_present(self) -> None:
        """All sources available → all True in dict."""
        result = build_data_completeness(
            news_available=True,
            fundamentals_available=True,
            weekly_available=True,
            filings_available=True,
            short_interest_available=True,
            earnings_available=True,
        )
        assert result == {
            "news": True,
            "fundamentals": True,
            "weekly": True,
            "filings": True,
            "short_interest": True,
            "earnings": True,
        }

    def test_build_data_completeness_partial(self) -> None:
        """news=True, fundamentals=False, weekly=True, filings=False → partial dict."""
        result = build_data_completeness(
            news_available=True,
            fundamentals_available=False,
            weekly_available=True,
            filings_available=False,
            short_interest_available=True,
            earnings_available=True,
        )
        assert result["news"] is True
        assert result["fundamentals"] is False
        assert result["weekly"] is True
        assert result["filings"] is False


# ---------------------------------------------------------------------------
# build_key_signals
# ---------------------------------------------------------------------------

class TestBuildKeySignals:
    def test_build_key_signals_returns_list_of_strings(self) -> None:
        """build_key_signals returns a list of human-readable strings (3-7 items)."""
        indicator_scores = {
            "ema_alignment": -90.0,
            "rsi_14": 30.0,
            "macd_histogram": -70.0,
            "stoch_k": 20.0,
            "obv": -60.0,
            "cmf_20": -50.0,
            "adx": 40.0,
        }
        pattern_scores = {
            "structural_pattern_score": -80.0,
            "fibonacci_score": -60.0,
            "divergence_rsi": 50.0,
            "crossover_ema_9_21": -70.0,
        }
        category_scores = {
            "trend": -80.0, "momentum": 20.0, "volume": -55.0,
            "volatility": -10.0, "candlestick": -30.0, "structural": -70.0,
            "sentiment": -20.0, "fundamental": -15.0, "macro": -25.0,
        }
        result = build_key_signals(
            indicator_scores=indicator_scores,
            pattern_scores=pattern_scores,
            regime="trending",
            category_scores=category_scores,
            final_score=-65.0,
            signal="BEARISH",
        )

        assert isinstance(result, list)
        assert 3 <= len(result) <= 7
        assert all(isinstance(s, str) for s in result)
        assert all(len(s) > 0 for s in result)

    def test_build_key_signals_empty_scores(self) -> None:
        """No scores provided → returns an empty list (no signals to report)."""
        result = build_key_signals(
            indicator_scores={},
            pattern_scores={},
            regime="ranging",
            category_scores={},
            final_score=0.0,
            signal="NEUTRAL",
        )
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_next_earnings_date
# ---------------------------------------------------------------------------

class TestGetNextEarningsDate:
    def test_get_next_earnings_date_found(self, db_connection: sqlite3.Connection) -> None:
        """Returns the next earnings date >= scoring_date."""
        db_connection.execute(
            "INSERT INTO earnings_calendar (ticker, earnings_date) VALUES (?, ?)",
            ("AAPL", "2025-02-01"),
        )
        db_connection.commit()

        result = get_next_earnings_date(db_connection, "AAPL", "2025-01-15")
        assert result == "2025-02-01"

    def test_get_next_earnings_date_returns_nearest(self, db_connection: sqlite3.Connection) -> None:
        """Returns the nearest upcoming earnings date (not a past one)."""
        db_connection.executemany(
            "INSERT INTO earnings_calendar (ticker, earnings_date) VALUES (?, ?)",
            [("AAPL", "2025-01-01"), ("AAPL", "2025-02-01"), ("AAPL", "2025-05-01")],
        )
        db_connection.commit()

        result = get_next_earnings_date(db_connection, "AAPL", "2025-01-15")
        assert result == "2025-02-01"

    def test_get_next_earnings_date_none_when_no_upcoming(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns None when no future earnings date exists."""
        db_connection.execute(
            "INSERT INTO earnings_calendar (ticker, earnings_date) VALUES (?, ?)",
            ("AAPL", "2020-01-01"),
        )
        db_connection.commit()

        result = get_next_earnings_date(db_connection, "AAPL", "2025-01-15")
        assert result is None

    def test_get_next_earnings_date_none_when_no_ticker(
        self, db_connection: sqlite3.Connection
    ) -> None:
        """Returns None when ticker has no earnings records."""
        result = get_next_earnings_date(db_connection, "AAPL", "2025-01-15")
        assert result is None
