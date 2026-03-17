"""
Tests for src/scorer/timeframe_merger.py — dual timeframe score merging.
"""

from __future__ import annotations

import pytest

from src.scorer.timeframe_merger import merge_timeframes


SAMPLE_CONFIG = {
    "timeframe_weights": {
        "daily": 0.6,
        "weekly": 0.4,
    }
}


class TestMergeTimeframes:
    def test_merge_daily_weekly_same_direction(self) -> None:
        """daily=+60, weekly=+50, weights 0.6/0.4 → 0.6*60 + 0.4*50 = 56.0."""
        result = merge_timeframes(daily_score=60.0, weekly_score=50.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(56.0, abs=0.01)

    def test_merge_daily_weekly_opposite_direction(self) -> None:
        """daily=+60, weekly=-40 → 0.6*60 + 0.4*(-40) = 20.0 (conflict → closer to neutral)."""
        result = merge_timeframes(daily_score=60.0, weekly_score=-40.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(20.0, abs=0.01)

    def test_merge_weekly_not_available(self) -> None:
        """weekly_score=None → merged = daily_score only."""
        result = merge_timeframes(daily_score=60.0, weekly_score=None, config=SAMPLE_CONFIG)
        assert result == pytest.approx(60.0, abs=0.01)

    def test_merge_uses_config_weights(self) -> None:
        """daily=+100, weekly=0, custom weights daily=0.7/weekly=0.3 → 70.0."""
        config = {"timeframe_weights": {"daily": 0.7, "weekly": 0.3}}
        result = merge_timeframes(daily_score=100.0, weekly_score=0.0, config=config)
        assert result == pytest.approx(70.0, abs=0.01)

    def test_merge_result_is_clamped(self) -> None:
        """daily=+100, weekly=+100 → merged does not exceed +100."""
        result = merge_timeframes(daily_score=100.0, weekly_score=100.0, config=SAMPLE_CONFIG)
        assert result == pytest.approx(100.0, abs=0.01)
