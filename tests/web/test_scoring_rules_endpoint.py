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
