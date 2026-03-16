"""
Configuration loader for the Stock Signal Engine.

Loads JSON config files from the config/ directory and provides
typed access to configuration values.
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def get_config_dir() -> Path:
    """
    Return the absolute path to the project's config/ directory.

    Uses __file__ to locate the src/common/ directory and navigates up two
    levels to reach the project root, then appends 'config'.

    Returns:
        Path: Absolute path to the config/ directory.
    """
    # __file__ is src/common/config.py — go up two directories to project root
    project_root = Path(__file__).resolve().parent.parent.parent
    return project_root / "config"


def load_config(config_name: str) -> dict:
    """
    Load a JSON configuration file from the config/ directory.

    Reads the file config/{config_name}.json and returns its contents as a dict.

    Args:
        config_name: The base name of the config file (without .json extension).

    Returns:
        dict: Parsed JSON configuration data.

    Raises:
        FileNotFoundError: If config/{config_name}.json does not exist, with a
            descriptive message indicating the missing file path.
    """
    config_path = get_config_dir() / f"{config_name}.json"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: '{config_path}'. "
            f"Expected a file named '{config_name}.json' in the config/ directory."
        )

    logger.info(f"Loading config file: {config_path}")
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def load_tickers() -> list[dict]:
    """
    Load and return the full tickers list from tickers.json.

    Convenience wrapper around load_config('tickers') that extracts the
    'tickers' list directly.

    Returns:
        list[dict]: List of all ticker configuration dicts (both active and inactive).
    """
    config = load_config("tickers")
    return config["tickers"]


def get_active_tickers() -> list[dict]:
    """
    Return only tickers where active=True from the tickers configuration.

    Loads the tickers configuration and filters to include only tickers
    whose 'active' field evaluates to True.

    Returns:
        list[dict]: List of active ticker dicts, each with keys:
            symbol, sector, sector_etf, added, active.
    """
    config = load_config("tickers")
    all_tickers = config["tickers"]
    active_tickers = [ticker for ticker in all_tickers if ticker.get("active")]
    logger.info(
        f"Loaded {len(active_tickers)} active tickers out of {len(all_tickers)} total"
    )
    return active_tickers


def get_sector_etfs() -> list[str]:
    """
    Return the list of sector ETF symbols from tickers.json.

    Loads the tickers configuration and returns the 'sector_etfs' list,
    which contains symbols like XLK, XLF, XLV, etc.

    Returns:
        list[str]: List of sector ETF ticker symbols.
    """
    config = load_config("tickers")
    return config["sector_etfs"]


def get_market_benchmarks() -> dict:
    """
    Return the market benchmarks dict from tickers.json.

    Loads the tickers configuration and returns the 'market_benchmarks' dict,
    which maps human-readable keys ('spy', 'qqq', 'vix') to their ticker symbols.

    Returns:
        dict: Mapping of benchmark name to ticker symbol, e.g.
            {"spy": "SPY", "qqq": "QQQ", "vix": "^VIX"}.
    """
    config = load_config("tickers")
    return config["market_benchmarks"]


def load_env(env_path: str = None) -> None:
    """
    Load environment variables from a .env file.

    If env_path is provided, loads that specific file. Otherwise loads the
    .env file from the project root directory. Issues a warning if the file
    is not found.

    Args:
        env_path: Optional absolute or relative path to a .env file. If None,
            defaults to the .env file at the project root.

    Returns:
        None
    """
    if env_path is not None:
        dotenv_path = Path(env_path)
    else:
        project_root = Path(__file__).resolve().parent.parent.parent
        dotenv_path = project_root / ".env"

    if not dotenv_path.exists():
        logger.warning(
            f"Environment file not found at '{dotenv_path}'. "
            "Environment variables may not be loaded."
        )
        return

    load_dotenv(dotenv_path=str(dotenv_path), override=True)
    logger.info(f"Loaded environment variables from: {dotenv_path}")
