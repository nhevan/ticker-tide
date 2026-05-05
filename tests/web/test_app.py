"""
Tests for src/web/app.py — FastAPI routes, auth middleware, and API endpoints.

Uses FastAPI TestClient with a temporary SQLite DB. All external calls (Claude,
Telegram) are mocked. Tests follow the spec in the locked decisions section.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.common.db import create_all_tables


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path) -> str:
    """Create a temporary database with full schema and seed data."""
    path = str(tmp_path / "test_app.db")
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    create_all_tables(conn)

    # Insert minimal ticker
    conn.execute(
        "INSERT OR REPLACE INTO tickers(symbol, name, active) VALUES ('AAPL', 'Apple', 1)"
    )
    # Insert a scores_daily row
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES ('AAPL','2026-04-25','BULLISH',72.5,55.0,'trending',
                  40.0,30.0,20.0,-10.0,25.0,15.0,5.0,8.0,-3.0,1.42)"""
    )
    # Insert a scores_weekly row
    conn.execute(
        """INSERT OR REPLACE INTO scores_weekly(
            ticker, week_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES ('AAPL','2026-04-21',48.0,'ranging',35.0,20.0,15.0,-5.0,10.0,12.0)"""
    )
    # Insert a scores_monthly row
    conn.execute(
        """INSERT OR REPLACE INTO scores_monthly(
            ticker, month_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES ('AAPL','2026-04-01',38.0,'ranging',30.0,15.0,10.0,-8.0,NULL,11.0)"""
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(db_path: str) -> Generator[TestClient, None, None]:
    """Create a FastAPI TestClient with test config and credentials."""
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
            config={
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
            },
        )
        with TestClient(app, raise_server_exceptions=True) as tc:
            yield tc


def _login(client: TestClient) -> None:
    """Helper: POST /login with valid credentials."""
    client.post("/login", data={"password": "testpass"}, follow_redirects=False)


# ---------------------------------------------------------------------------
# Auth tests
# ---------------------------------------------------------------------------

class TestAuth:
    """Tests for login/logout and middleware."""

    def test_unauthenticated_get_root_redirects_to_login(
        self, client: TestClient
    ) -> None:
        """Unauthenticated GET / must redirect to /login."""
        response = client.get("/", follow_redirects=False)
        assert response.status_code in (302, 307)
        assert "/login" in response.headers.get("location", "")

    def test_unauthenticated_api_returns_401(self, client: TestClient) -> None:
        """Unauthenticated GET /api/tickers must return 401, not redirect."""
        response = client.get("/api/tickers")
        assert response.status_code == 401

    def test_correct_password_sets_session_and_redirects(
        self, client: TestClient
    ) -> None:
        """Correct password must set a session cookie and redirect to /."""
        response = client.post(
            "/login", data={"password": "testpass"}, follow_redirects=False
        )
        assert response.status_code in (302, 307)
        assert "/" in response.headers.get("location", "")
        # Session cookie must be present
        assert "session" in client.cookies

    def test_wrong_password_returns_login_page_with_error(
        self, client: TestClient
    ) -> None:
        """Wrong password must return the login page (200 or 401) without setting session."""
        response = client.post(
            "/login", data={"password": "wrongpass"}, follow_redirects=False
        )
        assert response.status_code in (200, 401)
        # Session cookie must NOT be set for failed login
        assert "session" not in client.cookies

    def test_authenticated_get_root_returns_200(self, client: TestClient) -> None:
        """After login, GET / must return 200."""
        _login(client)
        response = client.get("/")
        assert response.status_code == 200

    def test_rate_limit_returns_429_after_max_attempts(
        self, client: TestClient
    ) -> None:
        """Exceeding 5 failed login attempts must return 429."""
        for _ in range(5):
            client.post(
                "/login", data={"password": "wrong"}, follow_redirects=False
            )
        response = client.post(
            "/login", data={"password": "wrong"}, follow_redirects=False
        )
        assert response.status_code == 429


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

class TestApiTickers:
    """Tests for GET /api/tickers."""

    def test_returns_alphabetized_list(self, client: TestClient) -> None:
        """GET /api/tickers must return a sorted list of active tickers."""
        _login(client)
        response = client.get("/api/tickers")
        assert response.status_code == 200
        tickers = response.json()
        assert isinstance(tickers, list)
        assert "AAPL" in tickers
        assert tickers == sorted(tickers)


class TestApiDates:
    """Tests for GET /api/dates."""

    def test_returns_min_max_for_known_ticker(self, client: TestClient) -> None:
        """GET /api/dates?ticker=AAPL must return min and max dates."""
        _login(client)
        response = client.get("/api/dates?ticker=AAPL")
        assert response.status_code == 200
        data = response.json()
        assert "min" in data
        assert "max" in data


class TestApiSnapshot:
    """Tests for GET /api/snapshot."""

    def test_returns_daily_weekly_monthly_structure(
        self, client: TestClient
    ) -> None:
        """Snapshot must contain daily, weekly, monthly top-level keys."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        data = response.json()
        assert "daily" in data
        assert "weekly" in data
        assert "monthly" in data

    def test_daily_has_categories_array(self, client: TestClient) -> None:
        """Snapshot daily section must include a categories array."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        daily = response.json()["daily"]
        assert "categories" in daily
        assert isinstance(daily["categories"], list)

    def test_weekly_has_categories_array(self, client: TestClient) -> None:
        """Snapshot weekly section must include a categories array."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        weekly = response.json()["weekly"]
        assert "categories" in weekly
        assert isinstance(weekly["categories"], list)

    def test_monthly_has_5_categories(self, client: TestClient) -> None:
        """Monthly categories must have exactly 5 entries (candlestick excluded)."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        monthly = response.json()["monthly"]
        assert monthly["data_available"] is True
        assert len(monthly["categories"]) == 5
        assert "candlestick" not in monthly["categories"]

    def test_unknown_ticker_returns_404(self, client: TestClient) -> None:
        """Snapshot for an unknown ticker must return 404."""
        _login(client)
        response = client.get("/api/snapshot?ticker=ZZZZ&date=2026-04-25")
        assert response.status_code == 404

    def test_daily_has_key_signals_key(self, client: TestClient) -> None:
        """Daily section must include key_signals key (list, possibly empty)."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        daily = response.json()["daily"]
        assert "key_signals" in daily
        assert isinstance(daily["key_signals"], list)

    def test_daily_has_earnings_key(self, client: TestClient) -> None:
        """Daily section must include earnings key with next and last_surprise subkeys."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        daily = response.json()["daily"]
        assert "earnings" in daily
        assert "next" in daily["earnings"]
        assert "last_surprise" in daily["earnings"]

    def test_daily_has_signal_flip_key(self, client: TestClient) -> None:
        """Daily section must include signal_flip key (dict or null)."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        daily = response.json()["daily"]
        assert "signal_flip" in daily


class TestApiLlm:
    """Tests for POST /api/llm."""

    def test_llm_calls_claude_and_returns_text(self, client: TestClient) -> None:
        """POST /api/llm must call the LLM wrapper and return {text}."""
        _login(client)
        with patch(
            "src.web.app.call_claude_for_web",
            return_value="AAPL looks bullish based on weekly indicators.",
        ):
            response = client.post(
                "/api/llm",
                json={
                    "ticker": "AAPL",
                    "date": "2026-04-25",
                    "timeframe": "daily",
                },
            )
        assert response.status_code == 200
        data = response.json()
        assert "text" in data
        assert len(data["text"]) > 0

    def test_second_llm_call_within_window_returns_429(
        self, client: TestClient
    ) -> None:
        """Second LLM call for same (session, ticker, date, timeframe) within 60s must return 429."""
        _login(client)
        with patch(
            "src.web.app.call_claude_for_web",
            return_value="Some analysis text.",
        ):
            # First call succeeds
            response1 = client.post(
                "/api/llm",
                json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
            )
            assert response1.status_code == 200
            # Second call within debounce window
            response2 = client.post(
                "/api/llm",
                json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
            )
        assert response2.status_code == 429

    def test_llm_failure_returns_503_with_friendly_message(
        self, client: TestClient
    ) -> None:
        """Claude API failure must return 503 with a friendly message, not a 500 stack trace."""
        _login(client)
        with patch(
            "src.web.app.call_claude_for_web",
            side_effect=Exception("API connection failed"),
        ):
            response = client.post(
                "/api/llm",
                json={
                    "ticker": "AAPL",
                    "date": "2026-04-25",
                    "timeframe": "weekly",
                },
            )
        assert response.status_code == 503
        data = response.json()
        assert "detail" in data
        # Must be a user-friendly message, not a raw exception string
        assert "unavailable" in data["detail"].lower() or "try again" in data["detail"].lower()
