"""
Logging setup for the Stock Signal Engine.

Provides a consistent logging format across all modules.
Format: [YYYY-MM-DD HH:MM:SS] LEVEL [module_name] message
"""

import logging


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get or create a logger with the given name and level.

    If the logger does not already have handlers, adds a StreamHandler with the
    format: [%(asctime)s] %(levelname)s [%(name)s] %(message)s
    and datefmt %Y-%m-%d %H:%M:%S.

    Calling this function multiple times with the same name returns the same
    logger instance and never adds duplicate handlers.

    Args:
        name: The logger name (e.g. "backfiller.ohlcv", "fetcher").
        level: The logging level (default: logging.INFO).

    Returns:
        A configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
