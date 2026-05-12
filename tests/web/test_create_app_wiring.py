"""
Tests that create_app wires scorer_config through correctly.

Verifies:
- create_app accepts scorer_config parameter.
- Routes created with scorer_config have access to it (via /api/scoring-rules).
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
        "score_expansion_factor": 2.0,  # Distinctive value to assert against.
    },
}


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Create a temporary database with minimal schema."""
    path = str(tmp_path / "test_wiring.db")
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
    resp = client.post("/api/login", json={"password": "testpass"})
    assert resp.status_code == 200


class TestCreateAppWiring:
    """create_app correctly wires scorer_config into routes."""

    def test_create_app_accepts_scorer_config(self, db_path: str) -> None:
        """create_app does not raise when scorer_config is provided."""
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
        assert app is not None

    def test_create_app_without_scorer_config_uses_empty_dict(self, db_path: str) -> None:
        """create_app works when scorer_config is omitted (backward compatible)."""
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
            )
        assert app is not None

    def test_scoring_rules_reflects_scorer_config(self, client: TestClient) -> None:
        """score_expansion_factor in /api/scoring-rules matches scorer_config."""
        _login(client)
        resp = client.get("/api/scoring-rules")
        assert resp.status_code == 200
        data = resp.json()
        assert data["score_expansion_factor"] == 2.0

    def test_scoring_rules_route_exists(self, client: TestClient) -> None:
        """GET /api/scoring-rules returns 401 (not 404) for unauthenticated requests."""
        resp = client.get("/api/scoring-rules")
        assert resp.status_code == 401
