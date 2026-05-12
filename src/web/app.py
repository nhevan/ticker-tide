"""
FastAPI application factory for the Ticker Tide signal browser.

Pure JSON API under /api/*. Serves a Vite-built React SPA from web/dist/
with a catch-all route for client-side routing. Single-worker only — the
in-memory LLM debounce and login rate-limit are process-local.

Route registration order (MUST be preserved):
  1. /api/* routes
  2. /assets StaticFiles mount (conditional on dist/assets existing)
  3. /favicon.ico and /robots.txt explicit handlers
  4. Catch-all /{full_path:path}  ← registered LAST

# Catch-all MUST be the last route registered. Add new routes ABOVE this line.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

from src.common.db import create_all_tables, get_connection
from src.web.auth import (
    check_rate_limit,
    is_correct_password,
    prune_old_login_attempts,
    record_login_attempt,
)
from src.web.llm import call_claude_for_web, generate_dashboard_verdict
from src.web.queries import fetch_active_tickers, fetch_date_range, fetch_snapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request body models
# ---------------------------------------------------------------------------


class LoginBody(BaseModel):
    """Request body for POST /api/login."""

    password: str


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def _run_llm_analysis(
    db_path: str,
    ticker: str,
    date_str: str,
    timeframe: str,
    config: dict,
) -> str:
    """
    Open a worker-thread SQLite connection and run the LLM analysis pipeline.

    Mirrors the threading discipline of _generate_and_persist_verdict: sqlite3
    connections are thread-bound, so the to_thread worker must own its own
    connection rather than reusing one opened in the event-loop thread.

    Parameters:
        db_path: Absolute path to the SQLite database file.
        ticker: Ticker symbol.
        date_str: Date being analyzed (YYYY-MM-DD).
        timeframe: One of 'daily', 'weekly', 'monthly'.
        config: Web config dict (passed through to the LLM layer).

    Returns:
        Claude's analysis text.
    """
    conn = get_connection(db_path)
    try:
        score_row_db = conn.execute(
            "SELECT * FROM scores_daily WHERE ticker = ? AND date = ?",
            (ticker, date_str),
        ).fetchone()
        score_row = dict(score_row_db) if score_row_db else {}
        return call_claude_for_web(conn, ticker, date_str, timeframe, score_row, config)
    finally:
        conn.close()


def _generate_and_persist_verdict(
    db_path: str,
    ticker: str,
    date_str: str,
    config: dict,
) -> tuple[str, str]:
    """
    Open a worker-thread SQLite connection, generate a verdict via Claude,
    INSERT OR REPLACE the result, and return (verdict_text, generated_at).

    sqlite3.Connection objects are thread-bound by default, so this helper
    must own the connection it uses. Called via asyncio.to_thread() from the
    POST /api/verdict handler.

    Parameters:
        db_path: Absolute path to the SQLite database file.
        ticker: Ticker symbol.
        date_str: Date being analyzed (YYYY-MM-DD).
        config: Web config dict (passed through to the LLM layer).

    Returns:
        (verdict_text, generated_at_iso_utc) tuple.
    """
    conn = get_connection(db_path)
    try:
        verdict_text = generate_dashboard_verdict(conn, ticker, date_str, config)
        generated_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_verdicts(ticker, date, verdict, generated_at) "
            "VALUES (?, ?, ?, ?)",
            (ticker, date_str, verdict_text, generated_at),
        )
        conn.commit()
        return verdict_text, generated_at
    finally:
        conn.close()


def create_app(
    db_path: str,
    config: dict,
    dist_dir: Optional[str] = None,
    scorer_config: Optional[dict] = None,
) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Sets up SessionMiddleware, JSON API routes, and optional static-file
    serving from a Vite build output directory. When dist_dir is None or
    points to a non-existent path, all non-API routes return 503 JSON.

    Accepts dist_dir as a parameter to allow dependency injection in tests
    without relying on filesystem conventions.

    Parameters:
        db_path: Absolute path to the SQLite database file.
        config: Web config dict loaded from config/web.json.
        dist_dir: Path to the Vite build output directory (web/dist). When
            None, the catch-all route returns 503 for all non-API requests.
        scorer_config: Scorer config dict loaded from config/scorer.json.
            When None, an empty dict is used (RSI zone labels use fallback thresholds;
            /api/scoring-rules returns empty regime_weights and default values).

    Returns:
        Configured FastAPI application instance.
    """
    resolved_scorer_config = scorer_config if scorer_config is not None else {}
    web_password = os.environ.get("WEB_PASSWORD", "")
    secret_key = os.environ.get("WEB_SECRET_KEY", "change-me-in-production")
    session_ttl_seconds = config.get("session_ttl_hours", 168) * 3600

    # Ensure web-owned tables exist on existing databases (idempotent).
    _bootstrap_conn = get_connection(db_path)
    try:
        create_all_tables(_bootstrap_conn)
    finally:
        _bootstrap_conn.close()

    # In-memory LLM debounce: maps (session_id, ticker, date, timeframe) → last_call_time.
    # Stored in app state so each create_app() call gets a fresh dict (important for tests).
    llm_debounce: dict[tuple, float] = {}

    app = FastAPI(title="Ticker Tide Web UI")

    app.add_middleware(
        SessionMiddleware,
        secret_key=secret_key,
        max_age=session_ttl_seconds,
        same_site="lax",
        https_only=False,  # TestClient uses HTTP; production Uvicorn behind Caddy is HTTPS
    )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _is_authenticated(request: Request) -> bool:
        """Return True if the session contains a valid auth marker."""
        return request.session.get("authenticated") is True

    def _open_db() -> sqlite3.Connection:
        """Open a per-request SQLite connection."""
        return get_connection(db_path)

    # ── /api/login ────────────────────────────────────────────────────────────

    @app.post("/api/login")
    async def api_login(body: LoginBody, request: Request) -> Response:
        """
        Authenticate with a JSON password body.

        Checks rate limit, validates password, sets session cookie on success.
        Records and prunes login attempts on every call.

        Parameters:
            body: JSON body with 'password' field.
            request: FastAPI request object for session and IP extraction.

        Returns:
            200 {"ok": true} with Set-Cookie on success.
            401 {"detail": "Invalid password."} on failure.
            429 {"detail": ...} when rate-limited.
        """
        client_ip = request.client.host if request.client else "unknown"
        conn = _open_db()
        try:
            prune_old_login_attempts(conn)
            rate_limit_cfg = config.get("login_rate_limit", {})
            if check_rate_limit(conn, client_ip, rate_limit_cfg):
                logger.warning(f"Login rate limit exceeded: ip={client_ip!r}")
                return JSONResponse(
                    {"detail": "Too many login attempts. Please wait and try again."},
                    status_code=429,
                )
            record_login_attempt(conn, client_ip)
            if is_correct_password(body.password, web_password):
                request.session["authenticated"] = True
                logger.info(f"Successful login: ip={client_ip!r}")
                return JSONResponse({"ok": True})
            logger.warning(f"Failed login attempt: ip={client_ip!r}")
            return JSONResponse({"detail": "Invalid password."}, status_code=401)
        finally:
            conn.close()

    # ── /api/logout ───────────────────────────────────────────────────────────

    @app.post("/api/logout")
    async def api_logout(request: Request) -> Response:
        """
        Clear the session cookie.

        Parameters:
            request: FastAPI request object for session access.

        Returns:
            200 {"ok": true}
        """
        request.session.clear()
        return JSONResponse({"ok": True})

    # ── /api/me ───────────────────────────────────────────────────────────────

    @app.get("/api/me")
    async def api_me(request: Request) -> Response:
        """
        Return the current authentication state.

        Parameters:
            request: FastAPI request object for session access.

        Returns:
            200 {"authenticated": true} if logged in.
            401 {"detail": "Not authenticated."} otherwise.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        return JSONResponse({"authenticated": True})

    # ── /api/tickers ─────────────────────────────────────────────────────────

    @app.get("/api/tickers")
    async def api_tickers(request: Request) -> Response:
        """
        Return the alphabetized list of active ticker symbols.

        Parameters:
            request: FastAPI request object for session access.

        Returns:
            200 list[str] if authenticated.
            401 {"detail": "Not authenticated."} otherwise.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            tickers = fetch_active_tickers(conn)
            return JSONResponse(tickers)
        finally:
            conn.close()

    # ── /api/dates ────────────────────────────────────────────────────────────

    @app.get("/api/dates")
    async def api_dates(request: Request, ticker: str) -> Response:
        """
        Return min and max available dates for a ticker from scores_daily.

        Parameters:
            request: FastAPI request object for session access.
            ticker: Ticker symbol query parameter.

        Returns:
            200 {"min": str, "max": str} if authenticated.
            401 {"detail": "Not authenticated."} otherwise.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            date_range = fetch_date_range(conn, ticker)
            return JSONResponse(date_range)
        finally:
            conn.close()

    # ── /api/snapshot ─────────────────────────────────────────────────────────

    @app.get("/api/snapshot")
    async def api_snapshot(request: Request, ticker: str, date: str) -> Response:
        """
        Return the full three-card snapshot for a ticker and picked date.

        Parameters:
            request: FastAPI request object for session access.
            ticker: Ticker symbol query parameter.
            date: Date string (YYYY-MM-DD) query parameter.

        Returns:
            200 snapshot dict if authenticated and data exists.
            401 {"detail": "Not authenticated."} if not logged in.
            404 {"detail": ...} if ticker has no data.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            date_range = fetch_date_range(conn, ticker)
            if date_range["min"] is None:
                return JSONResponse(
                    {"detail": f"No data found for ticker {ticker!r}."},
                    status_code=404,
                )
            snapshot = fetch_snapshot(
                conn, ticker, date, config=config, scorer_config=resolved_scorer_config
            )
            return JSONResponse(snapshot)
        finally:
            conn.close()

    # ── /api/llm ──────────────────────────────────────────────────────────────

    @app.post("/api/llm")
    async def api_llm(request: Request) -> Response:
        """
        Generate LLM analysis for a ticker/date/timeframe combination.

        Applies a per-(session, ticker, date, timeframe) debounce. Returns 401
        if not authenticated, 429 on debounce, 503 on Claude failure.

        Parameters:
            request: FastAPI request object for session and body access.

        Returns:
            200 {"text": str} on success.
            400 {"detail": ...} on bad input.
            401 {"detail": "Not authenticated."} if not logged in.
            429 {"detail": ...} within debounce window.
            503 {"detail": ...} on Claude API failure.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)

        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            return JSONResponse({"detail": "Invalid JSON body."}, status_code=400)

        ticker = body.get("ticker", "")
        date_str = body.get("date", "")
        timeframe = body.get("timeframe", "daily")

        if not ticker or not date_str:
            return JSONResponse(
                {"detail": "ticker and date are required."}, status_code=400
            )

        # Ensure a stable session identifier is stored in the signed cookie.
        # Must be set before building debounce_key so both first and subsequent
        # calls use the same key.
        if "session_id" not in request.session:
            request.session["session_id"] = str(time.time_ns())
        session_id = request.session["session_id"]

        debounce_key = (session_id, ticker, date_str, timeframe)
        debounce_window = config.get("llm_rate_limit", {}).get("window_seconds", 60)
        now = time.monotonic()
        # Use None as sentinel for "never called" — monotonic starts near 0.0 at
        # process boot, so the default of 0.0 would incorrectly trigger the debounce.
        last_call = llm_debounce.get(debounce_key)

        if last_call is not None and now - last_call < debounce_window:
            logger.info(
                f"LLM debounce triggered: ticker={ticker!r}, date={date_str!r}, "
                f"timeframe={timeframe!r}"
            )
            return JSONResponse(
                {"detail": "Analysis already requested. Please wait before requesting again."},
                status_code=429,
            )

        llm_debounce[debounce_key] = now

        try:
            analysis_text = await asyncio.to_thread(
                _run_llm_analysis, db_path, ticker, date_str, timeframe, config
            )
        except Exception as exc:
            logger.error(
                f"LLM analysis failed: ticker={ticker!r}, timeframe={timeframe!r}, "
                f"error={exc!r}"
            )
            # Reset debounce so user can retry
            llm_debounce.pop(debounce_key, None)
            return JSONResponse(
                {"detail": "AI analysis is temporarily unavailable. Please try again later."},
                status_code=503,
            )

        return JSONResponse({"text": analysis_text})

    # ── /api/verdict ──────────────────────────────────────────────────────────

    @app.get("/api/verdict")
    async def api_verdict_get(request: Request, ticker: str, date: str) -> Response:
        """
        Return the cached dashboard verdict for a ticker and date.

        Parameters:
            request: FastAPI request object for session access.
            ticker: Ticker symbol query parameter.
            date: Date string (YYYY-MM-DD) query parameter.

        Returns:
            200 {"verdict": str, "generated_at": str} if cached.
            401 {"detail": ...} if not authenticated.
            404 {"detail": ...} if no cached verdict exists.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            row = conn.execute(
                "SELECT verdict, generated_at FROM dashboard_verdicts "
                "WHERE ticker = ? AND date = ?",
                (ticker, date),
            ).fetchone()
            if row is None:
                return JSONResponse(
                    {"detail": "No verdict cached for this ticker and date."},
                    status_code=404,
                )
            return JSONResponse(
                {"verdict": row["verdict"], "generated_at": row["generated_at"]}
            )
        finally:
            conn.close()

    @app.post("/api/verdict")
    async def api_verdict_post(request: Request) -> Response:
        """
        Generate (or return cached) dashboard verdict for a ticker and date.

        Idempotent: if a cached row exists, returns it without calling Claude.
        Otherwise generates via Claude, persists with INSERT OR REPLACE, and
        returns the new verdict. 503 on Claude failure.

        Parameters:
            request: FastAPI request object for session and body access.

        Returns:
            200 {"verdict": str, "generated_at": str} on success or cache hit.
            400 {"detail": ...} on missing/invalid body.
            401 {"detail": ...} if not authenticated.
            503 {"detail": ...} on Claude failure.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)

        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            return JSONResponse({"detail": "Invalid JSON body."}, status_code=400)

        ticker = body.get("ticker", "")
        date_str = body.get("date", "")
        if not ticker or not date_str:
            return JSONResponse(
                {"detail": "ticker and date are required."}, status_code=400
            )

        # Cache check uses an event-loop-thread connection.
        conn = _open_db()
        try:
            cached = conn.execute(
                "SELECT verdict, generated_at FROM dashboard_verdicts "
                "WHERE ticker = ? AND date = ?",
                (ticker, date_str),
            ).fetchone()
            if cached is not None:
                return JSONResponse(
                    {"verdict": cached["verdict"], "generated_at": cached["generated_at"]}
                )
        finally:
            conn.close()

        # Generation + persistence run on a worker thread with their own connection
        # because sqlite3.Connection objects are thread-bound by default.
        try:
            verdict_text, generated_at = await asyncio.to_thread(
                _generate_and_persist_verdict, db_path, ticker, date_str, config
            )
        except Exception as exc:
            logger.error(
                f"Verdict generation failed: ticker={ticker!r}, date={date_str!r}, "
                f"error={exc!r}"
            )
            return JSONResponse(
                {"detail": "Verdict generation is temporarily unavailable. Please try again later."},
                status_code=503,
            )

        return JSONResponse({"verdict": verdict_text, "generated_at": generated_at})

    # ── /api/scoring-rules ────────────────────────────────────────────────────

    @app.get("/api/scoring-rules")
    async def api_scoring_rules(request: Request) -> Response:
        """
        Return static scoring rules and thresholds from the scorer config.

        Process-static: the response is constant for the lifetime of the process.
        After editing config/scorer.json, restart the web service to refresh.

        Parameters:
            request: FastAPI request object for session access.

        Returns:
            200 scoring rules dict if authenticated.
            401 {"detail": "Not authenticated."} otherwise.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        logger.debug("api_scoring_rules: serving scoring rules")
        rsi_thresholds = resolved_scorer_config.get(
            "indicator_thresholds", {}
        ).get("rsi_14", {"oversold": 30.0, "overbought": 70.0})
        stoch_k_thresholds = resolved_scorer_config.get(
            "indicator_thresholds", {}
        ).get("stoch_k", {"oversold": 20.0, "overbought": 80.0})
        adx_t = resolved_scorer_config.get(
            "indicator_thresholds", {}
        ).get("adx", {"ranging_max": 20.0, "weak_max": 25.0, "developing_max": 40.0})
        regime_weights = resolved_scorer_config.get("adaptive_weights", {})
        expansion_factor = resolved_scorer_config.get("scoring", {}).get(
            "score_expansion_factor", 1.0
        )
        timeframe_weights = resolved_scorer_config.get("timeframe_weights", {})
        return JSONResponse({
            "rsi": {
                "thresholds": rsi_thresholds,
                "scoring_method": "percentile_blended_with_fallback",
                "fallback_zones": ["oversold", "below_mid", "above_mid", "overbought"],
                "profile_zones": [
                    "extreme_oversold", "oversold", "below_mid",
                    "above_mid", "overbought", "extreme_overbought",
                ],
            },
            # scoring_method is display-only; no frontend code currently branches on it.
            # Stochastic %K uses a three-tier step function fallback (not the same as RSI's
            # linear interpolation within percentile bands) — hence the distinct method string.
            "stoch_k": {
                "thresholds": stoch_k_thresholds,
                "scoring_method": "percentile_profile_with_threshold_fallback",
                "fallback_zones": ["oversold", "below_mid", "above_mid", "overbought"],
                "profile_zones": [
                    "extreme_oversold", "oversold", "below_mid",
                    "above_mid", "overbought", "extreme_overbought",
                ],
            },
            # ADX uses a fixed-band piecewise scoring model (no profile path, no fallback).
            # See score_adx() at indicator_scorer.py:381-407 for the authoritative literals.
            # The discontinuity at ADX=25 is explicit: weak_trend_developing.score_max=20.0
            # is the band's actual ceiling and the +20 → +40 gap to developing_trend.score_min
            # is the documented discontinuity, additionally signalled by "discontinuity_at".
            # Thresholds are read from config/scorer.json indicator_thresholds.adx (display-only;
            # scorer hardcodes the same literals — keep manually in sync).
            "adx": {
                "scoring_method": "fixed_band_piecewise",
                "bands": [
                    {
                        "name": "ranging",
                        "min": 0.0,
                        "max": adx_t["ranging_max"],
                        "score_min": -20.0,
                        "score_max": 0.0,
                    },
                    {
                        "name": "weak_trend_developing",
                        "min": adx_t["ranging_max"],
                        "max": adx_t["weak_max"],
                        "score_min": 0.0,
                        "score_max": 20.0,  # band ceiling; score JUMPS to 40 at ADX=25
                    },
                    {
                        "name": "developing_trend",
                        "min": adx_t["weak_max"],
                        "max": adx_t["developing_max"],
                        "score_min": 40.0,
                        "score_max": 80.0,
                    },
                    {
                        "name": "strong_trend",
                        "min": adx_t["developing_max"],
                        "max": 100.0,
                        "score_min": 80.0,
                        "score_max": 80.0,
                    },
                ],
                "discontinuity_at": adx_t["weak_max"],
                "score_range": [-20.0, 80.0],
            },
            "regime_weights": regime_weights,
            "score_expansion_factor": expansion_factor,
            "timeframe_weights": timeframe_weights,
            "approximation_caveat": (
                "Item-level contributions do not sum to the final composite score "
                "due to clamping at ±100, sector adjustment, and timeframe merging."
            ),
        })

    # ── Static file serving ───────────────────────────────────────────────────
    # Route registration order: /assets mount → explicit root assets → catch-all.
    # All static-serve routes are conditional on dist_dir being present.

    _dist_dir: str = dist_dir or ""

    # Mount /assets only when the assets subdirectory actually exists at startup.
    # Vite outputs hashed files to dist/assets/; the mount serves them with
    # immutable-friendly default headers. The directory check prevents a startup
    # crash when the frontend has not been built yet.
    _assets_dir = os.path.join(_dist_dir, "assets") if _dist_dir else ""
    if _assets_dir and os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

    @app.get("/favicon.ico")
    async def get_favicon() -> Response:
        """
        Serve favicon.ico from the Vite build output.

        Returns:
            200 FileResponse if dist/favicon.ico exists.
            404 JSON if absent.
        """
        path = os.path.join(_dist_dir, "favicon.ico") if _dist_dir else ""
        if path and os.path.isfile(path):
            return FileResponse(path)
        return JSONResponse({"detail": "Not found."}, status_code=404)

    @app.get("/robots.txt")
    async def get_robots() -> Response:
        """
        Serve robots.txt from the Vite build output.

        Returns:
            200 FileResponse if dist/robots.txt exists.
            404 JSON if absent.
        """
        path = os.path.join(_dist_dir, "robots.txt") if _dist_dir else ""
        if path and os.path.isfile(path):
            return FileResponse(path)
        return JSONResponse({"detail": "Not found."}, status_code=404)

    # Catch-all MUST be the last route registered. Add new routes ABOVE this line.
    @app.get("/{full_path:path}")
    async def spa_catchall(full_path: str) -> Response:
        """
        Serve index.html for all unmatched GET paths (SPA client-side routing).

        Paths under /api/ that reached the catch-all have no matching route —
        return 404 JSON so API clients receive a proper not-found response
        rather than the SPA HTML or a 503.

        Returns 503 JSON when the Vite build output is missing, so ops can
        diagnose a failed build-frontend CI step without a cryptic 404.
        Cache-Control: no-cache is set so browsers always revalidate index.html,
        ensuring new deployments are picked up without a hard refresh.

        Parameters:
            full_path: The unmatched path segment (captured by FastAPI).

        Returns:
            404 JSON for unmatched /api/* paths.
            200 FileResponse (text/html) when dist/index.html exists.
            503 {"detail": "Frontend not built."} when the dist directory is absent.
        """
        # API paths that reached the catch-all have no matching route.
        if full_path.startswith("api/") or full_path == "api":
            return JSONResponse({"detail": "Not found."}, status_code=404)

        index_path = os.path.join(_dist_dir, "index.html") if _dist_dir else ""
        if index_path and os.path.isfile(index_path):
            return FileResponse(
                index_path,
                headers={"Cache-Control": "no-cache"},
            )
        return JSONResponse({"detail": "Frontend not built."}, status_code=503)

    return app
