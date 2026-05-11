"""
Drift-guard test: ensures the TypeScript INDICATOR_CATEGORY_MAP in
web/src/lib/scoring/categoryMap.ts stays in sync with the authoritative
Python INDICATOR_CATEGORY_MAP in src/scorer/category_scorer.py.

If the regex fails to find the TS map body, ts_keys will be empty and the
len(ts_keys) > 0 assertion will fail fast — guarding against a vacuous pass.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.scorer.category_scorer import INDICATOR_CATEGORY_MAP as PYTHON_MAP


REPO_ROOT = Path(__file__).resolve().parents[2]
TS_CATEGORY_MAP_PATH = REPO_ROOT / "web" / "src" / "lib" / "scoring" / "categoryMap.ts"


def _parse_ts_indicator_category_map(ts_source: str) -> dict[str, str]:
    """
    Extract key/value pairs from the INDICATOR_CATEGORY_MAP object literal in
    the given TypeScript source string.

    Matches the block between the opening brace of the map assignment and its
    closing ``};``. Each entry must be on its own line in the form:
        key: 'value',  (with optional trailing comment and comma)

    Parameters:
        ts_source: Full text of the TypeScript file.

    Returns:
        Dict mapping indicator key strings to category strings.
    """
    # Capture everything between "INDICATOR_CATEGORY_MAP: Record<string, Category> = {"
    # and the matching closing "};".
    block_match = re.search(
        r"INDICATOR_CATEGORY_MAP\s*:\s*Record<string,\s*Category>\s*=\s*\{([^}]+)\}",
        ts_source,
        re.DOTALL,
    )
    if not block_match:
        return {}

    block = block_match.group(1)

    # Match lines like:  ema_alignment: 'trend',
    entry_pattern = re.compile(r"""^\s*([\w]+)\s*:\s*['"](\w+)['"]\s*,?\s*(?://.*)?$""", re.MULTILINE)
    result: dict[str, str] = {}
    for match in entry_pattern.finditer(block):
        key, value = match.group(1), match.group(2)
        result[key] = value
    return result


@pytest.fixture(scope="module")
def ts_map() -> dict[str, str]:
    """Read and parse the TypeScript INDICATOR_CATEGORY_MAP from disk."""
    ts_source = TS_CATEGORY_MAP_PATH.read_text(encoding="utf-8")
    return _parse_ts_indicator_category_map(ts_source)


def test_ts_map_is_not_empty(ts_map: dict[str, str]) -> None:
    """Guard against a vacuous pass caused by a broken regex."""
    assert len(ts_map) > 0, (
        f"TypeScript INDICATOR_CATEGORY_MAP parsed as empty — "
        f"check the regex against {TS_CATEGORY_MAP_PATH}"
    )


def test_ts_map_keys_match_python(ts_map: dict[str, str]) -> None:
    """Both maps must define exactly the same set of indicator keys."""
    python_keys = set(PYTHON_MAP.keys())
    ts_keys = set(ts_map.keys())
    assert ts_keys == python_keys, (
        f"Key mismatch between TS and Python maps.\n"
        f"  Only in Python: {python_keys - ts_keys}\n"
        f"  Only in TS:     {ts_keys - python_keys}"
    )


def test_ts_map_values_match_python(ts_map: dict[str, str]) -> None:
    """For every shared key, the category value must be identical."""
    mismatches: list[str] = []
    for key in PYTHON_MAP:
        python_val = PYTHON_MAP[key]
        ts_val = ts_map.get(key)
        if ts_val != python_val:
            mismatches.append(f"  {key!r}: Python={python_val!r}, TS={ts_val!r}")
    assert not mismatches, "Category value mismatches:\n" + "\n".join(mismatches)
