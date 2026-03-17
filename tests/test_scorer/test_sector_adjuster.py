"""
Tests for src/scorer/sector_adjuster.py — sector ETF score and adjustment.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.scorer.sector_adjuster import apply_sector_adjustment


SAMPLE_CONFIG = {
    "sector_adjustment": {
        "bullish_sector_threshold": 30,
        "bearish_sector_threshold": -30,
        "max_adjustment": 10,
    }
}


class TestApplySectorAdjustment:
    def test_sector_adjustment_bullish_sector(self) -> None:
        """Sector ETF score=+40 (above +30 threshold), ticker raw=+50 → adjusted +55 to +60."""
        adjusted = apply_sector_adjustment(raw_score=50.0, sector_etf_score=40.0, config=SAMPLE_CONFIG)
        assert 50 < adjusted <= 60

    def test_sector_adjustment_bearish_sector(self) -> None:
        """Sector ETF score=-40 (below -30 threshold), ticker raw=+50 → adjusted +40 to +45."""
        adjusted = apply_sector_adjustment(raw_score=50.0, sector_etf_score=-40.0, config=SAMPLE_CONFIG)
        assert 40 <= adjusted < 50

    def test_sector_adjustment_neutral_sector(self) -> None:
        """Sector ETF score=+10 (between -30 and +30) → no adjustment, score unchanged."""
        adjusted = apply_sector_adjustment(raw_score=50.0, sector_etf_score=10.0, config=SAMPLE_CONFIG)
        assert adjusted == pytest.approx(50.0, abs=0.01)

    def test_sector_adjustment_clamped(self) -> None:
        """raw_score=+95, sector adds +10 → clamped to +100."""
        adjusted = apply_sector_adjustment(raw_score=95.0, sector_etf_score=100.0, config=SAMPLE_CONFIG)
        assert adjusted == pytest.approx(100.0, abs=0.01)

    def test_sector_adjustment_uses_config_max(self) -> None:
        """max_adjustment=15 in config → adjustment doesn't exceed 15."""
        config = {
            "sector_adjustment": {
                "bullish_sector_threshold": 30,
                "bearish_sector_threshold": -30,
                "max_adjustment": 15,
            }
        }
        adjusted = apply_sector_adjustment(raw_score=50.0, sector_etf_score=100.0, config=config)
        assert adjusted <= 65.0

    def test_sector_adjustment_missing_sector_data(self) -> None:
        """sector_etf_score=None → no adjustment applied, no crash."""
        adjusted = apply_sector_adjustment(raw_score=50.0, sector_etf_score=None, config=SAMPLE_CONFIG)
        assert adjusted == pytest.approx(50.0, abs=0.01)

    def test_sector_adjustment_negative_clamped(self) -> None:
        """raw_score=-95, bearish sector → clamped to -100."""
        adjusted = apply_sector_adjustment(raw_score=-95.0, sector_etf_score=-100.0, config=SAMPLE_CONFIG)
        assert adjusted == pytest.approx(-100.0, abs=0.01)
