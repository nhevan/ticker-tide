"""
Tests for src/common/logger.py — setup_logger function.
"""

import io
import logging

import pytest

from src.common.logger import setup_logger


@pytest.fixture(autouse=True)
def reset_loggers():
    """Remove all handlers from test loggers before each test to ensure isolation."""
    yield
    for name in ["test_module", "backfiller.ohlcv", "test", "my_module"]:
        logger = logging.getLogger(name)
        logger.handlers.clear()


def test_setup_logger_returns_logger():
    """setup_logger should return a logging.Logger instance."""
    result = setup_logger("test_module")
    assert isinstance(result, logging.Logger)


def test_setup_logger_has_correct_name():
    """Logger name should match the name argument passed in."""
    result = setup_logger("backfiller.ohlcv")
    assert result.name == "backfiller.ohlcv"


def test_setup_logger_default_level_is_info():
    """Default log level should be INFO."""
    result = setup_logger("test")
    assert result.level == logging.INFO


def test_setup_logger_custom_level():
    """Logger level should match a custom level argument."""
    result = setup_logger("test", level=logging.DEBUG)
    assert result.level == logging.DEBUG


def test_setup_logger_has_handler():
    """Logger should have at least one handler attached."""
    result = setup_logger("test")
    assert len(result.handlers) >= 1


def test_setup_logger_format_includes_timestamp():
    """Log output should contain a timestamp matching YYYY-MM-DD HH:MM:SS pattern."""
    import re

    logger = setup_logger("test")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.handlers = [handler]

    logger.info("timestamp check")
    output = stream.getvalue()

    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
    assert re.search(timestamp_pattern, output), f"No timestamp found in: {output!r}"


def test_setup_logger_format_includes_level():
    """Log output should contain the level name INFO."""
    logger = setup_logger("test")
    stream = io.StringIO()
    logger.handlers[0].stream = stream

    logger.info("test message")
    output = stream.getvalue()

    assert "INFO" in output


def test_setup_logger_format_includes_name():
    """Log output should contain the logger name."""
    logger = setup_logger("my_module")
    stream = io.StringIO()
    logger.handlers[0].stream = stream

    logger.info("name check")
    output = stream.getvalue()

    assert "my_module" in output


def test_setup_logger_same_name_returns_same_logger():
    """Calling setup_logger twice with the same name should return the same instance."""
    logger1 = setup_logger("test")
    logger2 = setup_logger("test")
    assert logger1 is logger2


def test_setup_logger_no_duplicate_handlers():
    """Calling setup_logger three times with the same name should result in exactly 1 handler."""
    setup_logger("test")
    setup_logger("test")
    result = setup_logger("test")
    assert len(result.handlers) == 1
