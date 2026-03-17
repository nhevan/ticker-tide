"""
Logging setup for the Stock Signal Engine.

Provides a consistent logging format across all modules.
Format: [YYYY-MM-DD HH:MM:SS] LEVEL [module_name] message
"""

import logging

_LOG_FORMAT = "[%(asctime)s] %(levelname)s [%(name)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_root_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger for console output.

    Calls logging.basicConfig with force=True so that all loggers in the
    application (which use logging.getLogger(__name__)) inherit a StreamHandler
    and produce output. Using force=True ensures this is effective even if
    logging was previously partially configured (e.g., by pytest or another library).

    Should be called once at the top of every entry-point script (e.g. run_backfill.py)
    before any pipeline code runs.

    Args:
        level: The logging level for the root logger. Defaults to logging.INFO.

    Returns:
        None
    """
    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        force=True,
    )


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
            fmt=_LOG_FORMAT,
            datefmt=_DATE_FORMAT,
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
