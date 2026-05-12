"""
Tests that score_rsi reads oversold/overbought thresholds from config.

Verifies:
- Mutating the config changes the fallback-path score.
- Default thresholds (30/70) match pre-refactor behaviour at canonical RSI values.
"""

from __future__ import annotations

import pytest

from src.scorer.indicator_scorer import score_rsi


def _make_config(oversold: float, overbought: float) -> dict:
    """Build a minimal scorer config with the given RSI thresholds."""
    return {
        "indicator_thresholds": {
            "rsi_14": {"oversold": oversold, "overbought": overbought},
        }
    }


_DEFAULT_CONFIG = _make_config(30.0, 70.0)


class TestScoreRsiConfig:
    """score_rsi reads oversold/overbought thresholds from the config dict."""

    def test_rsi_oversold_from_config_no_profile(self) -> None:
        """RSI below the config oversold threshold → bullish score."""
        config = _make_config(oversold=30.0, overbought=70.0)
        score = score_rsi(25.0, None, config)
        assert score > 0, f"RSI=25 below oversold=30 should be bullish, got {score}"

    def test_rsi_overbought_from_config_no_profile(self) -> None:
        """RSI above the config overbought threshold → bearish score."""
        config = _make_config(oversold=30.0, overbought=70.0)
        score = score_rsi(75.0, None, config)
        assert score < 0, f"RSI=75 above overbought=70 should be bearish, got {score}"

    def test_mutating_oversold_threshold_changes_score(self) -> None:
        """Raising oversold from 30→45 makes RSI=40 (previously neutral) score bullish."""
        config_standard = _make_config(oversold=30.0, overbought=70.0)
        config_raised = _make_config(oversold=45.0, overbought=70.0)
        score_standard = score_rsi(40.0, None, config_standard)
        score_raised = score_rsi(40.0, None, config_raised)
        # RSI=40 is in the neutral zone for standard, but below oversold=45 for raised.
        assert score_raised > score_standard, (
            f"Raised oversold threshold should produce a more bullish score. "
            f"Standard={score_standard}, raised={score_raised}"
        )

    def test_mutating_overbought_threshold_changes_score(self) -> None:
        """Lowering overbought from 70→55 makes RSI=60 (previously neutral) score bearish."""
        config_standard = _make_config(oversold=30.0, overbought=70.0)
        config_lowered = _make_config(oversold=30.0, overbought=55.0)
        score_standard = score_rsi(60.0, None, config_standard)
        score_lowered = score_rsi(60.0, None, config_lowered)
        assert score_lowered < score_standard, (
            f"Lowered overbought threshold should produce a more bearish score. "
            f"Standard={score_standard}, lowered={score_lowered}"
        )

    def test_default_values_match_prerefactor_at_rsi_15(self) -> None:
        """RSI=15: well below oversold=30 → strongly bullish (>50)."""
        score = score_rsi(15.0, None, _DEFAULT_CONFIG)
        assert score > 50, f"RSI=15 should be strongly bullish with default config, got {score}"

    def test_default_values_match_prerefactor_at_rsi_30(self) -> None:
        """RSI=30: at the oversold boundary → bullish or at least non-negative."""
        score = score_rsi(30.0, None, _DEFAULT_CONFIG)
        assert score >= 0, f"RSI=30 at oversold boundary should be non-negative, got {score}"

    def test_default_values_match_prerefactor_at_rsi_50(self) -> None:
        """RSI=50: at the neutral midpoint → score near zero (|score| <= 30)."""
        score = score_rsi(50.0, None, _DEFAULT_CONFIG)
        assert abs(score) <= 30, f"RSI=50 at midpoint should be near neutral, got {score}"

    def test_default_values_match_prerefactor_at_rsi_70(self) -> None:
        """RSI=70: at the overbought boundary → bearish or at least non-positive."""
        score = score_rsi(70.0, None, _DEFAULT_CONFIG)
        assert score <= 0, f"RSI=70 at overbought boundary should be non-positive, got {score}"

    def test_default_values_match_prerefactor_at_rsi_85(self) -> None:
        """RSI=85: well above overbought=70 → strongly bearish (<-50)."""
        score = score_rsi(85.0, None, _DEFAULT_CONFIG)
        assert score < -50, f"RSI=85 should be strongly bearish with default config, got {score}"
