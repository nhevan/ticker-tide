"""
Entry point for the Ticker Tide web UI server.

Boots Uvicorn with a single worker (required — in-memory rate-limit and LLM
debounce assume single-worker process), binding to 127.0.0.1 on the port
configured in config/web.json. Runs behind Caddy (HTTPS reverse proxy) on EC2.

Usage:
    python scripts/run_web.py

Environment variables (must be set in .env):
    WEB_PASSWORD     — shared password for the web UI
    WEB_SECRET_KEY   — secret key for Starlette SessionMiddleware (min 32 chars)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Add project root to sys.path for consistent imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

import uvicorn

logger = logging.getLogger(__name__)


def _load_web_config(config_path: str) -> dict:
    """
    Load and return the web config from a JSON file.

    Parameters:
        config_path: Path to the config/web.json file.

    Returns:
        Web config dict.

    Raises:
        SystemExit: If the config file is missing or invalid JSON.
    """
    if not os.path.isfile(config_path):
        logger.error(f"Web config not found: {config_path!r}")
        sys.exit(1)
    with open(config_path, "r") as fh:
        return json.load(fh)


def _load_database_config(config_path: str) -> dict:
    """
    Load and return the database config from a JSON file.

    Parameters:
        config_path: Path to the config/database.json file.

    Returns:
        Database config dict.

    Raises:
        SystemExit: If the config file is missing or invalid JSON.
    """
    if not os.path.isfile(config_path):
        logger.error(f"Database config not found: {config_path!r}")
        sys.exit(1)
    with open(config_path, "r") as fh:
        return json.load(fh)


def main() -> None:
    """
    Load configs, create the FastAPI app, and boot Uvicorn.

    Uses a single worker. Binds to 127.0.0.1 to only accept connections from
    the local Caddy reverse proxy — not exposed directly to the internet.
    --proxy-headers is equivalent to Uvicorn's proxy_headers=True which trusts
    X-Forwarded-For from the reverse proxy for correct IP extraction.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run the Ticker Tide web UI.")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload on source changes (dev only). Watches src/ and "
        "config/. Imports the app via src.web.asgi:app, so a fresh app is built "
        "on every reload.",
    )
    args = parser.parse_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    web_config_path = os.path.join(project_root, "config", "web.json")
    db_config_path = os.path.join(project_root, "config", "database.json")

    web_config = _load_web_config(web_config_path)
    db_config = _load_database_config(db_config_path)

    port: int = web_config.get("port", 8765)
    db_path = os.path.join(project_root, db_config.get("path", "data/signals.db"))

    _check_required_env_vars()

    # dist_dir is a structural convention: web/dist relative to the repo root.
    # It is intentionally NOT a config key (see CONFIG.md). Pass it explicitly
    # so create_app() can enable static-serve and the SPA catch-all route.
    dist_dir = (
        (Path(os.path.abspath(__file__)).parent.parent / "web" / "dist").as_posix()
    )

    logger.info(
        f"Starting Ticker Tide web UI: host=127.0.0.1, port={port}, "
        f"workers=1, db={db_path!r}, dist_dir={dist_dir!r}, reload={args.reload}"
    )

    if args.reload:
        # Reload mode: uvicorn re-imports src.web.asgi:app on every source change.
        uvicorn.run(
            "src.web.asgi:app",
            host="127.0.0.1",
            port=port,
            reload=True,
            reload_dirs=[
                os.path.join(project_root, "src"),
                os.path.join(project_root, "config"),
            ],
            proxy_headers=True,
            forwarded_allow_ips="127.0.0.1",
            log_level="info",
        )
        return

    from src.web.app import create_app

    app = create_app(db_path=db_path, config=web_config, dist_dir=dist_dir)

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=port,
        workers=1,
        proxy_headers=True,
        forwarded_allow_ips="127.0.0.1",
        log_level="info",
    )


def _check_required_env_vars() -> None:
    """
    Warn if required environment variables are not set.

    Logs a warning for each missing variable but does not exit — the app will
    run with empty/default values, but auth will not work correctly without them.

    Returns:
        None
    """
    required = ["WEB_PASSWORD", "WEB_SECRET_KEY"]
    for key in required:
        if not os.environ.get(key):
            logger.warning(
                f"Environment variable {key!r} is not set — "
                f"web UI auth will not function correctly."
            )


if __name__ == "__main__":
    main()
