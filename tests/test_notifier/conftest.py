"""
Shared pytest configuration for tests/test_notifier/.

Installs minimal stubs for packages that are not importable in the current
environment before any test module in this directory is imported.
Uses a real import attempt so that installed packages (e.g. inside a venv)
are never replaced with stubs.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType
from unittest.mock import MagicMock


def _stub_if_uninstallable(module_name: str, attrs: dict) -> None:
    """
    Register a stub only if the package cannot actually be imported.

    If the package is installed (e.g. inside a venv), the real module is
    used. If it is absent, a minimal stub with the given attributes is
    inserted into sys.modules so source files that import it can still be
    collected by pytest.
    """
    if module_name in sys.modules:
        return
    try:
        importlib.import_module(module_name)
    except ImportError:
        stub = ModuleType(module_name)
        for attr_name, attr_value in attrs.items():
            setattr(stub, attr_name, attr_value)
        sys.modules[module_name] = stub


_stub_if_uninstallable("telegram", {"Update": MagicMock()})
_stub_if_uninstallable("telegram.ext", {
    "Application": MagicMock(),
    "CommandHandler": MagicMock(),
    "ContextTypes": MagicMock(),
})
_stub_if_uninstallable("mplfinance", {
    "make_addplot": MagicMock(),
    "plot": MagicMock(),
    "make_mpf_style": MagicMock(),
})
