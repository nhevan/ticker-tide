"""
Tests for src/common/logger.py — setup_logger and setup_root_logging functions.
"""

import io
import logging

import pytest

from src.common.logger import setup_logger, setup_root_logging


@pytest.fixture(autouse=True)
def reset_loggers():
    """Save and restore root logger state; clear named test loggers around each test."""
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = root.handlers[:]
    root.handlers.clear()

    yield

    for name in ["test_module", "backfiller.ohlcv", "test", "my_module",
                 "src.backfiller.main"]:
        logging.getLogger(name).handlers.clear()
    root.handlers.clear()
    root.handlers.extend(saved_handlers)
    root.setLevel(saved_level)


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


# ---------------------------------------------------------------------------
# Tests for setup_root_logging
# ---------------------------------------------------------------------------

def test_setup_root_logging_adds_handler() -> None:
    """setup_root_logging should add at least one handler to the root logger."""
    setup_root_logging()
    root = logging.getLogger()
    assert len(root.handlers) >= 1


def test_setup_root_logging_sets_info_level_by_default() -> None:
    """setup_root_logging should set the root logger level to INFO by default."""
    setup_root_logging()
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_setup_root_logging_custom_level() -> None:
    """setup_root_logging should respect a custom level argument."""
    setup_root_logging(level=logging.DEBUG)
    root = logging.getLogger()
    assert root.level == logging.DEBUG


def test_setup_root_logging_produces_output() -> None:
    """After setup_root_logging, a child logger's INFO message should appear in output."""
    import re

    setup_root_logging()
    root = logging.getLogger()
    stream = io.StringIO()
    root.handlers[0].stream = stream

    child_logger = logging.getLogger("src.backfiller.main")
    child_logger.info("pipeline started")

    output = stream.getvalue()
    assert "pipeline started" in output
    assert "INFO" in output
    assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", output)
