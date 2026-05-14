"""
Tests for src/web/app.py — FastAPI JSON API routes, auth middleware, static-serve,
and catch-all SPA routing.

Uses FastAPI TestClient with a temporary SQLite DB. All external calls (Claude)
are mocked. Tests follow the Vite+React migration spec (JSON API only — no Jinja,
no HTML responses from auth routes).
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.common.db import create_all_tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_CONFIG = {
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
    "price_chart": {
        "range_days": {"1M": 22, "3M": 66, "6M": 132, "1Y": 252, "ALL": 5000}
    },
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Create a temporary database with full schema and seed data."""
    path = str(tmp_path / "test_app.db")
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    create_all_tables(conn)

    conn.execute(
        "INSERT OR REPLACE INTO tickers(symbol, name, active) VALUES ('AAPL', 'Apple', 1)"
    )
    conn.execute(
        """INSERT OR REPLACE INTO scores_daily(
            ticker, date, signal, confidence, final_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score, sentiment_score,
            fundamental_score, macro_score, calibrated_score
        ) VALUES ('AAPL','2026-04-25','BULLISH',72.5,55.0,'trending',
                  40.0,30.0,20.0,-10.0,25.0,15.0,5.0,8.0,-3.0,1.42)"""
    )
    conn.execute(
        """INSERT OR REPLACE INTO scores_weekly(
            ticker, week_start, composite_score, regime,
            trend_score, momentum_score, volume_score, volatility_score,
            candlestick_score, structural_score
        ) VALUES ('AAPL','2026-04-21',48.0,'ranging',35.0,20.0,15.0,-5.0,10.0,12.0)"""
    )
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
    """
    Create a FastAPI TestClient with test config, test credentials, and no dist_dir.

    dist_dir is omitted so that catch-all and asset routes exercise the
    '503 when dist missing' path unless a specific dist_dir is passed.
    """
    with patch.dict(
        "os.environ",
        {
            "WEB_PASSWORD": "testpass",
            "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
        },
    ):
        from src.web.app import create_app

        app = create_app(db_path=db_path, config=_TEST_CONFIG)
        with TestClient(app, raise_server_exceptions=True) as tc:
            yield tc


@pytest.fixture
def client_with_dist(db_path: str, tmp_path: Path) -> Generator[TestClient, None, None]:
    """
    Create a TestClient with a fake web/dist directory for static-serve tests.

    Creates: tmp_path/dist/index.html, tmp_path/dist/favicon.ico,
    tmp_path/dist/robots.txt, tmp_path/dist/assets/index-abc123.js
    """
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text(
        "<!DOCTYPE html><html><body><div id='root'></div></body></html>"
    )
    (dist / "favicon.ico").write_bytes(b"\x00" * 16)
    (dist / "robots.txt").write_text("User-agent: *\nDisallow: /\n")
    assets = dist / "assets"
    assets.mkdir()
    (assets / "index-abc123.js").write_text("console.log('app');")

    with patch.dict(
        "os.environ",
        {
            "WEB_PASSWORD": "testpass",
            "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
        },
    ):
        from src.web.app import create_app

        app = create_app(db_path=db_path, config=_TEST_CONFIG, dist_dir=str(dist))
        with TestClient(app, raise_server_exceptions=True) as tc:
            yield tc


def _login(client: TestClient) -> None:
    """Helper: POST /api/login with valid credentials and assert 200."""
    resp = client.post("/api/login", json={"password": "testpass"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


# ---------------------------------------------------------------------------
# Login / logout / me tests
# ---------------------------------------------------------------------------


class TestLogin:
    """Tests for POST /api/login."""

    def test_login_returns_200_with_correct_password(
        self, client: TestClient
    ) -> None:
        """Correct password must return 200 {"ok": true}."""
        response = client.post("/api/login", json={"password": "testpass"})
        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_login_returns_401_with_wrong_password(
        self, client: TestClient
    ) -> None:
        """Wrong password must return 401 with detail key."""
        response = client.post("/api/login", json={"password": "wrongpass"})
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data
        assert "Invalid password" in data["detail"]

    def test_login_returns_422_with_malformed_body(
        self, client: TestClient
    ) -> None:
        """Empty JSON body (missing required 'password' field) must return 422."""
        response = client.post("/api/login", json={})
        assert response.status_code == 422


class TestLogout:
    """Tests for POST /api/logout."""

    def test_logout_clears_cookie(self, client: TestClient) -> None:
        """After login and logout, GET /api/me must return 401."""
        _login(client)
        # Confirm authenticated
        me_resp = client.get("/api/me")
        assert me_resp.status_code == 200

        # Logout
        logout_resp = client.post("/api/logout")
        assert logout_resp.status_code == 200
        assert logout_resp.json() == {"ok": True}

        # Now /api/me must return 401
        me_after = client.get("/api/me")
        assert me_after.status_code == 401


class TestMe:
    """Tests for GET /api/me."""

    def test_me_returns_200_when_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/me must return 200 {"authenticated": true} after login."""
        _login(client)
        response = client.get("/api/me")
        assert response.status_code == 200
        assert response.json() == {"authenticated": True}

    def test_me_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/me must return 401 when not logged in."""
        response = client.get("/api/me")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data


# ---------------------------------------------------------------------------
# Snapshot auth guard
# ---------------------------------------------------------------------------


class TestSnapshotAuth:
    """Auth guard tests for GET /api/snapshot."""

    def test_snapshot_returns_json_when_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/snapshot must return 200 JSON with daily/weekly/monthly keys."""
        _login(client)
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 200
        data = response.json()
        assert "daily" in data
        assert "weekly" in data
        assert "monthly" in data

    def test_snapshot_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/snapshot must return 401 when not logged in."""
        response = client.get("/api/snapshot?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data


# ---------------------------------------------------------------------------
# LLM auth guard and behavior
# ---------------------------------------------------------------------------


class TestLlmAuth:
    """Auth guard and behavior tests for POST /api/llm."""

    def test_llm_returns_json_when_authenticated(
        self, client: TestClient
    ) -> None:
        """POST /api/llm must call the LLM wrapper and return {"text": ...}."""
        _login(client)
        with patch(
            "src.web.app.call_claude_for_web",
            return_value="AAPL looks bullish.",
        ):
            response = client.post(
                "/api/llm",
                json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "text" in data
        assert len(data["text"]) > 0

    def test_llm_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """POST /api/llm must return 401 when not logged in."""
        response = client.post(
            "/api/llm",
            json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
        )
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data

    def test_llm_debounce_returns_429_on_repeat(
        self, client: TestClient
    ) -> None:
        """Second POST /api/llm for the same (ticker,date,timeframe) within the
        debounce window must return 429."""
        _login(client)
        with patch(
            "src.web.app.call_claude_for_web",
            return_value="AAPL looks bullish.",
        ):
            first = client.post(
                "/api/llm",
                json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
            )
            assert first.status_code == 200
            second = client.post(
                "/api/llm",
                json={"ticker": "AAPL", "date": "2026-04-25", "timeframe": "daily"},
            )
        assert second.status_code == 429
        assert "detail" in second.json()


# ---------------------------------------------------------------------------
# /api/verdict tests
# ---------------------------------------------------------------------------


class TestVerdict:
    """Auth and behavior tests for GET/POST /api/verdict."""

    def test_get_verdict_returns_404_when_not_cached(
        self, client: TestClient
    ) -> None:
        """GET /api/verdict must return 404 when no row exists for (ticker, date)."""
        _login(client)
        response = client.get("/api/verdict?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_get_verdict_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/verdict must return 401 when not logged in."""
        response = client.get("/api/verdict?ticker=AAPL&date=2026-04-25")
        assert response.status_code == 401

    def test_post_verdict_generates_caches_and_returns_text(
        self, client: TestClient
    ) -> None:
        """POST /api/verdict must call Claude, persist, and return the verdict."""
        _login(client)
        with patch(
            "src.web.app.generate_dashboard_verdict",
            return_value="BUY\nStrong momentum.",
        ) as mock_gen:
            response = client.post(
                "/api/verdict",
                json={"ticker": "AAPL", "date": "2026-04-25"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["verdict"].startswith("BUY")
        assert "generated_at" in data
        mock_gen.assert_called_once()

        # GET must now return the cached row
        cached = client.get("/api/verdict?ticker=AAPL&date=2026-04-25")
        assert cached.status_code == 200
        assert cached.json()["verdict"].startswith("BUY")

    def test_post_verdict_is_idempotent_when_cached(
        self, client: TestClient
    ) -> None:
        """POST /api/verdict must reuse the cached row without calling Claude again."""
        _login(client)
        with patch(
            "src.web.app.generate_dashboard_verdict",
            return_value="BUY\nFirst call.",
        ) as mock_gen:
            client.post(
                "/api/verdict",
                json={"ticker": "AAPL", "date": "2026-04-25"},
            )
            assert mock_gen.call_count == 1
            response = client.post(
                "/api/verdict",
                json={"ticker": "AAPL", "date": "2026-04-25"},
            )
        assert response.status_code == 200
        assert response.json()["verdict"].startswith("BUY")
        assert mock_gen.call_count == 1  # no second Claude call

    def test_post_verdict_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """POST /api/verdict must return 401 when not logged in."""
        response = client.post(
            "/api/verdict",
            json={"ticker": "AAPL", "date": "2026-04-25"},
        )
        assert response.status_code == 401

    def test_post_verdict_returns_400_on_missing_fields(
        self, client: TestClient
    ) -> None:
        """POST /api/verdict must return 400 when ticker or date is missing."""
        _login(client)
        response = client.post("/api/verdict", json={"ticker": "AAPL"})
        assert response.status_code == 400

    def test_post_verdict_returns_503_on_claude_failure(
        self, client: TestClient
    ) -> None:
        """POST /api/verdict must return 503 when Claude raises."""
        _login(client)
        with patch(
            "src.web.app.generate_dashboard_verdict",
            side_effect=RuntimeError("claude down"),
        ):
            response = client.post(
                "/api/verdict",
                json={"ticker": "AAPL", "date": "2026-04-25"},
            )
        assert response.status_code == 503


# ---------------------------------------------------------------------------
# Login rate limit
# ---------------------------------------------------------------------------


class TestLoginRateLimit:
    """Tests for the per-IP rate limit on POST /api/login."""

    def test_login_rate_limit_returns_429_after_threshold(
        self, client: TestClient
    ) -> None:
        """Exceeding max_attempts wrong-password posts within the window must
        return 429."""
        for _ in range(5):
            resp = client.post("/api/login", json={"password": "wrongpass"})
            assert resp.status_code == 401
        rate_limited = client.post("/api/login", json={"password": "wrongpass"})
        assert rate_limited.status_code == 429
        assert "detail" in rate_limited.json()


# ---------------------------------------------------------------------------
# /api/tickers and /api/dates auth guards
# ---------------------------------------------------------------------------


class TestTickersAuth:
    """Auth guard for GET /api/tickers."""

    def test_tickers_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/tickers must return 401 when not logged in."""
        response = client.get("/api/tickers")
        assert response.status_code == 401
        assert "detail" in response.json()

    def test_tickers_returns_json_when_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/tickers must return a JSON list when logged in."""
        _login(client)
        response = client.get("/api/tickers")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestTickersListAuth:
    """Auth guard and basic response shape for GET /api/tickers-list."""

    def test_tickers_list_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/tickers-list must return 401 when not logged in."""
        response = client.get("/api/tickers-list")
        assert response.status_code == 401
        assert "detail" in response.json()

    def test_tickers_list_returns_json_list_when_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/tickers-list must return a JSON list when logged in."""
        _login(client)
        response = client.get("/api/tickers-list")
        assert response.status_code == 200
        body = response.json()
        assert isinstance(body, list)
        # Each row (if any) must carry symbol at minimum.
        for row in body:
            assert "symbol" in row


class TestDatesAuth:
    """Auth guard for GET /api/dates."""

    def test_dates_returns_401_when_not_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/dates must return 401 when not logged in."""
        response = client.get("/api/dates?ticker=AAPL")
        assert response.status_code == 401
        assert "detail" in response.json()

    def test_dates_returns_json_when_authenticated(
        self, client: TestClient
    ) -> None:
        """GET /api/dates must return JSON with min/max when logged in."""
        _login(client)
        response = client.get("/api/dates?ticker=AAPL")
        assert response.status_code == 200
        data = response.json()
        assert "min" in data and "max" in data


# ---------------------------------------------------------------------------
# Catch-all / static-serve tests
# ---------------------------------------------------------------------------


class TestCatchAll:
    """Tests for the SPA catch-all route and dist-missing 503 guard."""

    def test_catchall_serves_index_html_for_unknown_path(
        self, client_with_dist: TestClient
    ) -> None:
        """GET /some/spa/route must return 200 text/html (index.html) when dist exists."""
        response = client_with_dist.get("/some/spa/route")
        assert response.status_code == 200
        content_type = response.headers.get("content-type", "")
        assert "text/html" in content_type

    def test_catchall_returns_503_when_dist_missing(
        self, db_path: str
    ) -> None:
        """GET /anything must return 503 JSON when dist_dir is nonexistent."""
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
                config=_TEST_CONFIG,
                dist_dir="/nonexistent_dist_path_xyz",
            )
            with TestClient(app, raise_server_exceptions=True) as tc:
                response = tc.get("/anything")
        assert response.status_code == 503
        data = response.json()
        assert data == {"detail": "Frontend not built."}

    def test_unknown_api_path_returns_404_not_index_html(
        self, client: TestClient
    ) -> None:
        """GET /api/does-not-exist must return 404 JSON, not index.html."""
        response = client.get("/api/does-not-exist")
        assert response.status_code == 404
        content_type = response.headers.get("content-type", "")
        assert "application/json" in content_type


# ---------------------------------------------------------------------------
# Favicon / robots.txt explicit handlers
# ---------------------------------------------------------------------------


class TestRootAssets:
    """Tests for explicit GET /favicon.ico and GET /robots.txt handlers."""

    def test_favicon_served_when_present(
        self, client_with_dist: TestClient
    ) -> None:
        """GET /favicon.ico must return 200 when dist/favicon.ico exists."""
        response = client_with_dist.get("/favicon.ico")
        assert response.status_code == 200

    def test_favicon_returns_404_when_missing(
        self, db_path: str, tmp_path: Path
    ) -> None:
        """GET /favicon.ico must return 404 when favicon.ico is absent from dist."""
        dist = tmp_path / "dist_no_favicon"
        dist.mkdir()
        (dist / "index.html").write_text("<html></html>")
        # Intentionally no favicon.ico

        with patch.dict(
            "os.environ",
            {
                "WEB_PASSWORD": "testpass",
                "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
            },
        ):
            from src.web.app import create_app

            app = create_app(
                db_path=db_path, config=_TEST_CONFIG, dist_dir=str(dist)
            )
            with TestClient(app, raise_server_exceptions=True) as tc:
                response = tc.get("/favicon.ico")
        assert response.status_code == 404

    def test_robots_served_when_present(
        self, client_with_dist: TestClient
    ) -> None:
        """GET /robots.txt must return 200 when dist/robots.txt exists."""
        response = client_with_dist.get("/robots.txt")
        assert response.status_code == 200

    def test_robots_returns_404_when_missing(
        self, db_path: str, tmp_path: Path
    ) -> None:
        """GET /robots.txt must return 404 when robots.txt is absent from dist."""
        dist = tmp_path / "dist_no_robots"
        dist.mkdir()
        (dist / "index.html").write_text("<html></html>")
        # Intentionally no robots.txt

        with patch.dict(
            "os.environ",
            {
                "WEB_PASSWORD": "testpass",
                "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
            },
        ):
            from src.web.app import create_app

            app = create_app(
                db_path=db_path, config=_TEST_CONFIG, dist_dir=str(dist)
            )
            with TestClient(app, raise_server_exceptions=True) as tc:
                response = tc.get("/robots.txt")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# /assets static mount
# ---------------------------------------------------------------------------


class TestAssetsMount:
    """Tests for the /assets StaticFiles mount."""

    def test_assets_mount_serves_hashed_files(
        self, client_with_dist: TestClient
    ) -> None:
        """GET /assets/index-abc123.js must return 200 with the JS content."""
        response = client_with_dist.get("/assets/index-abc123.js")
        assert response.status_code == 200
        assert b"console.log" in response.content


# ---------------------------------------------------------------------------
# /api/scoring-rules — CCI block test
# ---------------------------------------------------------------------------


class TestScoringRulesCci:
    """Tests for the CCI block in GET /api/scoring-rules."""

    def test_scoring_rules_includes_cci_thresholds(
        self, client: TestClient
    ) -> None:
        """GET /api/scoring-rules must include the full cci block with thresholds,
        fallback_zones, and profile_zones."""
        _login(client)
        response = client.get("/api/scoring-rules")
        assert response.status_code == 200
        body = response.json()

        assert "cci" in body, "cci block missing from /api/scoring-rules response"
        cci = body["cci"]

        # Verify thresholds shape and canonical values.
        assert "thresholds" in cci
        thresholds = cci["thresholds"]
        assert thresholds["hyper_oversold"] == -200
        assert thresholds["oversold"] == -100
        assert thresholds["overbought"] == 100
        assert thresholds["hyper_overbought"] == 200

        # Verify fallback_zones list.
        assert "fallback_zones" in cci
        assert cci["fallback_zones"] == [
            "hyper_oversold", "oversold", "neutral", "overbought", "hyper_overbought"
        ]

        # Verify profile_zones list.
        assert "profile_zones" in cci
        assert cci["profile_zones"] == [
            "extreme_oversold", "oversold", "below_mid",
            "above_mid", "overbought", "extreme_overbought",
        ]


# ---------------------------------------------------------------------------
# /api/shrinkage-path endpoint tests
# ---------------------------------------------------------------------------


class TestShrinkagePathEndpoint:
    """Tests for GET /api/shrinkage-path."""

    def test_shrinkage_path_endpoint_requires_auth(
        self, client: TestClient
    ) -> None:
        """Unauthenticated request must return 401."""
        response = client.get("/api/shrinkage-path")
        assert response.status_code == 401
        assert "detail" in response.json()

    def test_shrinkage_path_endpoint_invalid_date_format(
        self, client: TestClient
    ) -> None:
        """?date=not-a-date must return 422."""
        _login(client)
        response = client.get("/api/shrinkage-path?date=not-a-date")
        assert response.status_code == 422

    def test_shrinkage_path_endpoint_cold_start_returns_200_with_flag(
        self, client: TestClient
    ) -> None:
        """
        Cold-start must return 200 with cold_start=True. The client fixture
        leaves scores_daily empty, so fetch_shrinkage_path exits via the
        no-scoring-dates branch — no patching needed.
        """
        _login(client)
        response = client.get("/api/shrinkage-path")

        assert response.status_code == 200
        body = response.json()
        assert body["cold_start"] is True
        assert body["training_samples"] == 0

    def test_shrinkage_path_endpoint_returns_json(
        self, client: TestClient
    ) -> None:
        """Authenticated happy-path request returns 200 JSON with expected keys."""
        _login(client)
        from unittest.mock import patch
        from src.scorer.calibrator import DEFAULT_SHRINKAGE_LAMBDAS, FEATURE_NAMES

        happy_payload = {
            "cold_start": False,
            "scoring_date": "2026-04-25",
            "production_lambda": 0.1,
            "training_samples": 50,
            "lambdas": DEFAULT_SHRINKAGE_LAMBDAS,
            "features": [
                {
                    "name": name,
                    "label": name,
                    "category": "trend",
                    "coefs": [0.1] * len(DEFAULT_SHRINKAGE_LAMBDAS),
                }
                for name in FEATURE_NAMES
            ],
        }
        with patch("src.web.app.fetch_shrinkage_path", return_value=happy_payload):
            response = client.get("/api/shrinkage-path")

        assert response.status_code == 200
        body = response.json()
        assert body["cold_start"] is False
        assert body["production_lambda"] == 0.1
        assert len(body["features"]) == 17
        assert len(body["lambdas"]) == len(DEFAULT_SHRINKAGE_LAMBDAS)


# ---------------------------------------------------------------------------
# /api/price-chart tests
# ---------------------------------------------------------------------------


def _insert_ohlcv_daily(conn: sqlite3.Connection, ticker: str, rows: list) -> None:
    """Seed ohlcv_daily rows for price-chart tests.

    Parameters:
        conn:   Open SQLite connection.
        ticker: Ticker symbol.
        rows:   List of (date, open, high, low, close, volume) tuples.
    """
    conn.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily(ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(ticker, date, open_, high, low, close, volume)
         for date, open_, high, low, close, volume in rows],
    )
    conn.commit()


class TestPriceChart:
    """Tests for GET /api/price-chart."""

    def test_price_chart_happy_path_returns_200_with_bars(
        self, db_path: str, tmp_path: Path
    ) -> None:
        """Authenticated request returns 200 with bars list and specific close values."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _insert_ohlcv_daily(conn, "AAPL", [
            ("2026-04-21", 168.0, 170.0, 167.0, 169.0, 1_000_000),
            ("2026-04-22", 169.0, 171.0, 168.0, 170.0, 1_100_000),
            ("2026-04-23", 170.0, 172.0, 169.0, 171.0, 1_200_000),
            ("2026-04-24", 171.0, 173.0, 170.0, 172.0, 1_300_000),
            ("2026-04-25", 172.0, 174.0, 171.0, 173.0, 1_400_000),
        ])
        conn.close()

        with patch.dict(
            "os.environ",
            {
                "WEB_PASSWORD": "testpass",
                "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
            },
        ):
            from src.web.app import create_app

            app = create_app(db_path=db_path, config=_TEST_CONFIG)
            with TestClient(app, raise_server_exceptions=True) as tc:
                tc.post("/api/login", json={"password": "testpass"})
                response = tc.get("/api/price-chart?ticker=AAPL&range=3M")

        assert response.status_code == 200
        body = response.json()
        assert len(body["bars"]) == 5
        most_recent = body["bars"][-1]
        assert most_recent["date"] == "2026-04-25"
        assert most_recent["close"] == 173.0

    def test_price_chart_unauthenticated_returns_401(
        self, client: TestClient
    ) -> None:
        """Unauthenticated request must return 401."""
        response = client.get("/api/price-chart?ticker=AAPL&range=3M")
        assert response.status_code == 401
        assert "detail" in response.json()

    def test_price_chart_invalid_range_returns_422(
        self, client: TestClient
    ) -> None:
        """Request with an unrecognised range must return 422 with a descriptive detail."""
        _login(client)
        response = client.get("/api/price-chart?ticker=AAPL&range=7D")
        assert response.status_code == 422
        body = response.json()
        assert "Unknown range: 7D" in body["detail"]

    def test_price_chart_unknown_ticker_returns_200_empty_bars(
        self, client: TestClient
    ) -> None:
        """Request for a ticker absent from ohlcv_daily must return 200 with empty bars."""
        _login(client)
        response = client.get("/api/price-chart?ticker=ZZZZ&range=3M")
        assert response.status_code == 200
        body = response.json()
        assert body["bars"] == []

    def test_price_chart_uppercases_ticker(
        self, db_path: str
    ) -> None:
        """Lowercase ticker in the request must produce the same result as uppercase."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _insert_ohlcv_daily(conn, "AAPL", [
            ("2026-04-25", 172.0, 174.0, 171.0, 173.0, 1_400_000),
        ])
        conn.close()

        with patch.dict(
            "os.environ",
            {
                "WEB_PASSWORD": "testpass",
                "WEB_SECRET_KEY": "test-secret-key-for-sessions-32b",
            },
        ):
            from src.web.app import create_app

            app = create_app(db_path=db_path, config=_TEST_CONFIG)
            with TestClient(app, raise_server_exceptions=True) as tc:
                tc.post("/api/login", json={"password": "testpass"})
                lower_resp = tc.get("/api/price-chart?ticker=aapl&range=3M")
                upper_resp = tc.get("/api/price-chart?ticker=AAPL&range=3M")

        assert lower_resp.status_code == 200
        assert lower_resp.json()["bars"] == upper_resp.json()["bars"]
