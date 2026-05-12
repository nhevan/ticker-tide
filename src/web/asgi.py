"""
ASGI entrypoint for uvicorn --reload.

Builds the FastAPI app at module import time using the same config loading as
scripts/run_web.py. uvicorn imports this module by string (`src.web.asgi:app`)
on every reload, so a fresh app is constructed each time source files change.

Production (`scripts/run_web.py` without --reload) still passes the app
instance directly — this module is only required for the reload path.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.web.app import create_app

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_json(path: Path) -> dict:
    """Load and return a JSON config file as a dict."""
    with path.open("r") as fh:
        return json.load(fh)


_web_config = _load_json(_PROJECT_ROOT / "config" / "web.json")
_db_config = _load_json(_PROJECT_ROOT / "config" / "database.json")
_scorer_config = _load_json(_PROJECT_ROOT / "config" / "scorer.json")
_db_path = str(_PROJECT_ROOT / _db_config.get("path", "data/signals.db"))
_dist_dir = str(_PROJECT_ROOT / "web" / "dist")

app = create_app(
    db_path=_db_path,
    config=_web_config,
    dist_dir=_dist_dir,
    scorer_config=_scorer_config,
)
