"""
Tests for GET /api/scoring-rules endpoint.

Verifies:
- 200 with correct shape when authenticated.
- 401 when not authenticated.
- Values match the scorer_config passed to create_app.
- score_expansion_factor comes from config (not a literal).
- approximation_caveat is present; neutral_zone is NOT present.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.common.db import create_all_tables


_TEST_WEB_CONFIG = {
    "port": 8765,
    "session_ttl_hours": 168,
    "login_rate_limit": {"max_attempts": 5, "window_seconds": 60},
    "llm_rate_limit": {"window_seconds": 60},
    "sparkline": {"daily_days": 15, "weekly_weeks": 6, "monthly_months": 6},
    "ai_reasoner": {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 800,
        "temperature": 0.3,
        "target_words": 150,
    },
    "why_bullets": {"limit": 3},
    "signal_flip_lookback_days": 14,
    "verdict": {"max_lines": 5},
}

_TEST_SCORER_CONFIG = {
    "indicator_thresholds": {
        "rsi_14": {"oversold": 30.0, "overbought": 70.0},
        "stoch_k": {"oversold": 20.0, "overbought": 80.0},
        "adx": {"ranging_max": 20.0, "weak_max": 25.0, "developing_max": 40.0},
    },
    "adaptive_weights": {
        "trending": {"trend": 0.30, "momentum": 0.20, "volume": 0.10, "volatility": 0.05,
                     "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                     "fundamental": 0.05, "macro": 0.30},
        "ranging": {"trend": 0.15, "momentum": 0.25, "volume": 0.15, "volatility": 0.10,
                    "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                    "fundamental": 0.10, "macro": 0.25},
        "volatile": {"trend": 0.20, "momentum": 0.20, "volume": 0.10, "volatility": 0.15,
                     "candlestick": 0.0, "structural": 0.0, "sentiment": 0.0,
                     "fundamental": 0.05, "macro": 0.30},
    },
    "timeframe_weights": {
        "trending": {"daily": 0.10, "weekly": 0.50, "monthly": 0.40},
        "ranging":  {"daily": 0.60, "weekly": 0.30, "monthly": 0.10},
        "volatile": {"daily": 0.25, "weekly": 0.45, "monthly": 0.30},
    },
    "scoring": {
        "score_expansion_factor": 1.5,
    },
}


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Create a temporary database with minimal schema."""
    path = str(tmp_path / "test_rules.db")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    create_all_tables(conn)
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(db_path: str) -> Generator[TestClient, None, None]:
    """Create a TestClient with scorer_config wired in."""
    with patch.dict(
        "os.environ",
        {
            "WEB_PASSWORD": "testpass",
            "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
        },
    ):
        from src.web.app import create_app

        app = create_app(
            db_path=db_path,
            config=_TEST_WEB_CONFIG,
            scorer_config=_TEST_SCORER_CONFIG,
        )
        with TestClient(app, raise_server_exceptions=True) as tc:
            yield tc


def _login(client: TestClient) -> None:
    """Helper: POST /api/login with valid credentials and assert 200."""
    resp = client.post("/api/login", json={"password": "testpass"})
    assert resp.status_code == 200


class TestScoringRulesEndpoint:
    """Tests for GET /api/scoring-rules."""

    def test_unauthenticated_returns_401(self, client: TestClient) -> None:
        """GET /api/scoring-rules without session → 401."""
        resp = client.get("/api/scoring-rules")
        assert resp.status_code == 401

    def test_authenticated_returns_200(self, client: TestClient) -> None:
        """GET /api/scoring-rules with valid session → 200."""
        _login(client)
        resp = client.get("/api/scoring-rules")
        assert resp.status_code == 200

    def test_response_has_rsi_block(self, client: TestClient) -> None:
        """Response contains rsi key with expected sub-keys."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "rsi" in data
        rsi = data["rsi"]
        assert "thresholds" in rsi
        assert "scoring_method" in rsi
        assert "fallback_zones" in rsi
        assert "profile_zones" in rsi

    def test_rsi_thresholds_match_config(self, client: TestClient) -> None:
        """RSI thresholds in response match the scorer_config."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["rsi"]["thresholds"]["oversold"] == 30.0
        assert data["rsi"]["thresholds"]["overbought"] == 70.0

    def test_score_expansion_factor_matches_config(self, client: TestClient) -> None:
        """score_expansion_factor comes from config, not a hard-coded literal."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["score_expansion_factor"] == 1.5

    def test_regime_weights_present(self, client: TestClient) -> None:
        """regime_weights block is present with trending/ranging/volatile keys."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "regime_weights" in data
        for regime in ("trending", "ranging", "volatile"):
            assert regime in data["regime_weights"]

    def test_approximation_caveat_present(self, client: TestClient) -> None:
        """approximation_caveat key must be present in the response."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "approximation_caveat" in data
        assert isinstance(data["approximation_caveat"], str)
        assert len(data["approximation_caveat"]) > 0

    def test_neutral_zone_not_present(self, client: TestClient) -> None:
        """neutral_zone key must NOT be present in the response."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "neutral_zone" not in data

    def test_fallback_zones_and_profile_zones_exact(self, client: TestClient) -> None:
        """Exact zone label lists match the documented contract."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["rsi"]["fallback_zones"] == [
            "oversold", "below_mid", "above_mid", "overbought"
        ]
        assert data["rsi"]["profile_zones"] == [
            "extreme_oversold", "oversold", "below_mid",
            "above_mid", "overbought", "extreme_overbought"
        ]

    def test_timeframe_weights_all_three_regime_keys_present(self, client: TestClient) -> None:
        """timeframe_weights block is present with trending/ranging/volatile keys."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "timeframe_weights" in data
        for regime in ("trending", "ranging", "volatile"):
            assert regime in data["timeframe_weights"]

    def test_timeframe_weights_ranging_daily_value(self, client: TestClient) -> None:
        """timeframe_weights[ranging][daily] must equal 0.60 (from test scorer config)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["timeframe_weights"]["ranging"]["daily"] == 0.60

    def test_timeframe_weights_trending_weekly_value(self, client: TestClient) -> None:
        """timeframe_weights[trending][weekly] must equal 0.50 (from test scorer config)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["timeframe_weights"]["trending"]["weekly"] == 0.50

    def test_timeframe_weights_each_regime_has_daily_weekly_monthly(self, client: TestClient) -> None:
        """Each regime in timeframe_weights must have daily, weekly, and monthly keys."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        for regime in ("trending", "ranging", "volatile"):
            entry = data["timeframe_weights"][regime]
            assert "daily" in entry
            assert "weekly" in entry
            assert "monthly" in entry

    def test_response_has_stoch_k_block(self, client: TestClient) -> None:
        """Response contains stoch_k key with the four expected sub-keys."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "stoch_k" in data
        stoch_k = data["stoch_k"]
        assert "thresholds" in stoch_k
        assert "scoring_method" in stoch_k
        assert "fallback_zones" in stoch_k
        assert "profile_zones" in stoch_k

    def test_stoch_k_thresholds_come_from_config(self, client: TestClient) -> None:
        """stoch_k thresholds in response match the scorer_config (not hardcoded literals)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["stoch_k"]["thresholds"]["oversold"] == 20.0
        assert data["stoch_k"]["thresholds"]["overbought"] == 80.0

    def test_stoch_k_fallback_zones_exact(self, client: TestClient) -> None:
        """stoch_k fallback_zones match the documented contract."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["stoch_k"]["fallback_zones"] == [
            "oversold", "below_mid", "above_mid", "overbought"
        ]

    def test_stoch_k_profile_zones_exact(self, client: TestClient) -> None:
        """stoch_k profile_zones match the documented contract."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["stoch_k"]["profile_zones"] == [
            "extreme_oversold", "oversold", "below_mid",
            "above_mid", "overbought", "extreme_overbought",
        ]

    def test_existing_rsi_block_unchanged(self, client: TestClient) -> None:
        """Adding the stoch_k block must not alter the existing rsi block."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        rsi = data["rsi"]
        assert rsi["thresholds"]["oversold"] == 30.0
        assert rsi["thresholds"]["overbought"] == 70.0
        assert rsi["scoring_method"] == "percentile_blended_with_fallback"
        assert rsi["fallback_zones"] == ["oversold", "below_mid", "above_mid", "overbought"]
        assert rsi["profile_zones"] == [
            "extreme_oversold", "oversold", "below_mid",
            "above_mid", "overbought", "extreme_overbought",
        ]


class TestScoringRulesAdxBlock:
    """Tests for the adx block in GET /api/scoring-rules."""

    def test_adx_key_present(self, client: TestClient) -> None:
        """Response contains the adx key."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "adx" in data

    def test_adx_scoring_method(self, client: TestClient) -> None:
        """adx.scoring_method is 'fixed_band_piecewise'."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["adx"]["scoring_method"] == "fixed_band_piecewise"

    def test_adx_bands_count(self, client: TestClient) -> None:
        """adx.bands has exactly 4 entries."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert len(data["adx"]["bands"]) == 4

    def test_adx_bands_names_in_order(self, client: TestClient) -> None:
        """adx.bands names are in the correct order."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        names = [b["name"] for b in data["adx"]["bands"]]
        assert names == [
            "ranging",
            "weak_trend_developing",
            "developing_trend",
            "strong_trend",
        ]

    def test_adx_weak_band_score_max_is_20(self, client: TestClient) -> None:
        """
        BLOCKER 2: weak_trend_developing.score_max == 20.0.

        This is the band's actual ceiling (NOT 40.0). The +20 → +40 gap to
        developing_trend.score_min is the documented discontinuity.
        """
        _login(client)
        data = client.get("/api/scoring-rules").json()
        weak_band = data["adx"]["bands"][1]
        assert weak_band["name"] == "weak_trend_developing"
        assert weak_band["score_max"] == 20.0

    def test_adx_developing_band_score_min_is_40(self, client: TestClient) -> None:
        """
        BLOCKER 2: developing_trend.score_min == 40.0.

        Together with weak_trend_developing.score_max=20.0, this pins the
        discontinuity into the API contract.
        """
        _login(client)
        data = client.get("/api/scoring-rules").json()
        developing_band = data["adx"]["bands"][2]
        assert developing_band["name"] == "developing_trend"
        assert developing_band["score_min"] == 40.0

    def test_adx_discontinuity_at(self, client: TestClient) -> None:
        """adx.discontinuity_at == 25.0 (the weak_max threshold value from config)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["adx"]["discontinuity_at"] == 25.0

    def test_adx_score_range(self, client: TestClient) -> None:
        """adx.score_range == [-20.0, 80.0]."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert data["adx"]["score_range"] == [-20.0, 80.0]

    def test_adx_no_profile_zones(self, client: TestClient) -> None:
        """profile_zones must NOT be in the adx block (ADX has no profile path)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "profile_zones" not in data["adx"]

    def test_adx_no_thresholds(self, client: TestClient) -> None:
        """thresholds must NOT be in the adx block (no oversold/overbought pair)."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        assert "thresholds" not in data["adx"]

    def test_existing_rsi_and_stoch_k_blocks_unchanged(self, client: TestClient) -> None:
        """Adding the adx block must not alter the existing rsi and stoch_k blocks."""
        _login(client)
        data = client.get("/api/scoring-rules").json()
        rsi = data["rsi"]
        assert rsi["thresholds"]["oversold"] == 30.0
        assert rsi["thresholds"]["overbought"] == 70.0
        assert rsi["scoring_method"] == "percentile_blended_with_fallback"
        stoch_k = data["stoch_k"]
        assert stoch_k["thresholds"]["oversold"] == 20.0
        assert stoch_k["thresholds"]["overbought"] == 80.0
        assert stoch_k["scoring_method"] == "percentile_profile_with_threshold_fallback"

