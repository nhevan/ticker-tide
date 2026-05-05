"""
FastAPI application factory for the read-only stock signal web UI.

Provides the main app with auth middleware, login/logout routes, and API
endpoints for tickers, date ranges, snapshots, and LLM analysis.
Single-worker only — the in-memory LLM debounce is process-local.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from typing import Any, Optional

from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from src.common.db import create_all_tables, get_connection
from src.web.auth import (
    check_rate_limit,
    is_correct_password,
    prune_old_login_attempts,
    record_login_attempt,
)
from src.web.llm import call_claude_for_web
from src.web.queries import fetch_active_tickers, fetch_date_range, fetch_snapshot

logger = logging.getLogger(__name__)

# Path to the templates and static directories relative to this file
_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def create_app(db_path: str, config: dict) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Sets up SessionMiddleware, static file serving, template rendering,
    and all routes. Accepts db_path and config as parameters to allow
    dependency injection in tests.

    Parameters:
        db_path: Absolute path to the SQLite database file.
        config: Web config dict loaded from config/web.json.

    Returns:
        Configured FastAPI application instance.
    """
    web_password = os.environ.get("WEB_PASSWORD", "")
    secret_key = os.environ.get("WEB_SECRET_KEY", "change-me-in-production")
    session_ttl_seconds = config.get("session_ttl_hours", 168) * 3600

    # Ensure web-owned tables exist on existing databases (idempotent).
    _bootstrap_conn = get_connection(db_path)
    try:
        create_all_tables(_bootstrap_conn)
    finally:
        _bootstrap_conn.close()

    # In-memory LLM debounce: maps (session_id, ticker, date, timeframe) → last_call_time
    # Stored in app state so each create_app() call gets a fresh dict (important for tests)
    llm_debounce: dict[tuple, float] = {}

    app = FastAPI(title="Ticker Tide Web UI")

    app.add_middleware(
        SessionMiddleware,
        secret_key=secret_key,
        max_age=session_ttl_seconds,
        same_site="lax",
        https_only=False,  # TestClient uses HTTP; production Uvicorn behind Caddy is HTTPS
    )

    templates = Jinja2Templates(directory=_TEMPLATES_DIR)

    if os.path.isdir(_STATIC_DIR):
        app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def _is_authenticated(request: Request) -> bool:
        """Return True if the session contains a valid auth marker."""
        return request.session.get("authenticated") is True

    def _open_db() -> sqlite3.Connection:
        """Open a per-request SQLite connection."""
        return get_connection(db_path)

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.get("/login", response_class=HTMLResponse)
    async def get_login(request: Request) -> Response:
        """Render the login page."""
        return templates.TemplateResponse(request, "login.html", {"error": None})

    @app.post("/login")
    async def post_login(
        request: Request,
        password: str = Form(...),
    ) -> Response:
        """
        Handle login form submission.

        Checks rate limit, validates password, sets session on success.
        Records and prunes login attempts on every call.

        Returns:
            Redirect to / on success, or login page with error on failure.
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
            if is_correct_password(password, web_password):
                request.session["authenticated"] = True
                logger.info(f"Successful login: ip={client_ip!r}")
                return RedirectResponse(url="/", status_code=302)
            logger.warning(f"Failed login attempt: ip={client_ip!r}")
            return templates.TemplateResponse(
                request, "login.html", {"error": "Incorrect password."}, status_code=401
            )
        finally:
            conn.close()

    @app.post("/logout")
    async def post_logout(request: Request) -> Response:
        """Clear the session and redirect to login."""
        request.session.clear()
        return RedirectResponse(url="/login", status_code=302)

    @app.get("/", response_class=HTMLResponse)
    async def get_index(request: Request) -> Response:
        """Render the main index page if authenticated, else redirect to login."""
        if not _is_authenticated(request):
            return RedirectResponse(url="/login", status_code=302)
        conn = _open_db()
        try:
            tickers = fetch_active_tickers(conn)
        finally:
            conn.close()
        return templates.TemplateResponse(request, "index.html", {"tickers": tickers})

    # ── API routes ────────────────────────────────────────────────────────────

    @app.get("/api/tickers")
    async def api_tickers(request: Request) -> Response:
        """
        Return the alphabetized list of active ticker symbols.

        Returns 401 if not authenticated.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            tickers = fetch_active_tickers(conn)
            return JSONResponse(tickers)
        finally:
            conn.close()

    @app.get("/api/dates")
    async def api_dates(request: Request, ticker: str) -> Response:
        """
        Return min and max available dates for a ticker from scores_daily.

        Returns 401 if not authenticated.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            date_range = fetch_date_range(conn, ticker)
            return JSONResponse(date_range)
        finally:
            conn.close()

    @app.get("/api/snapshot")
    async def api_snapshot(request: Request, ticker: str, date: str) -> Response:
        """
        Return the full three-card snapshot for a ticker and picked date.

        Returns 401 if not authenticated, 404 if ticker has no data.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        conn = _open_db()
        try:
            # Validate ticker exists by checking date range
            date_range = fetch_date_range(conn, ticker)
            if date_range["min"] is None:
                return JSONResponse(
                    {"detail": f"No data found for ticker {ticker!r}."},
                    status_code=404,
                )
            snapshot = fetch_snapshot(conn, ticker, date, config=config)
            return JSONResponse(snapshot)
        finally:
            conn.close()

    @app.post("/api/llm")
    async def api_llm(request: Request) -> Response:
        """
        Generate LLM analysis for a ticker/date/timeframe combination.

        Applies a per-(session, ticker, date, timeframe) debounce of 60 seconds.
        Returns 401 if not authenticated, 429 on debounce, 503 on Claude failure.
        """
        if not _is_authenticated(request):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)

        try:
            body = await request.json()
        except Exception:
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

        conn = _open_db()
        try:
            # Load daily score row for daily context (needed by build_ticker_context)
            score_row_db = conn.execute(
                "SELECT * FROM scores_daily WHERE ticker = ? AND date = ?",
                (ticker, date_str),
            ).fetchone()
            score_row = dict(score_row_db) if score_row_db else {}

            try:
                analysis_text = await asyncio.to_thread(
                    call_claude_for_web,
                    conn,
                    ticker,
                    date_str,
                    timeframe,
                    score_row,
                    config,
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
        finally:
            conn.close()

    return app
