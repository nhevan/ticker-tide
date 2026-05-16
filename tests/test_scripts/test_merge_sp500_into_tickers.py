"""
Tests for scripts/merge_sp500_into_tickers.py

All tests run fully offline — pandas.read_html is monkeypatched at the
module level so no HTTP requests are made.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers — shared test data
# ---------------------------------------------------------------------------

TODAY = "2026-05-16"


def _make_config(tickers: list[dict[str, Any]], **extra_blocks: Any) -> dict[str, Any]:
    """
    Build a minimal config dict that mirrors config/tickers.json structure.

    Args:
        tickers: List of ticker entry dicts.
        **extra_blocks: Additional top-level keys (e.g. sector_etfs, market_benchmarks).

    Returns:
        dict: Config dict with 'tickers' key plus any extra_blocks.
    """
    config: dict[str, Any] = {"tickers": tickers}
    config.update(extra_blocks)
    return config


def _write_config(path: Path, config: dict[str, Any]) -> None:
    """Write a config dict to a JSON file with trailing newline."""
    path.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _read_config(path: Path) -> dict[str, Any]:
    """Read a config JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def _ticker_entry(
    symbol: str,
    sector: str = "Technology",
    sector_etf: str | None = "XLK",
    added: str = "2026-01-01",
    active: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    """
    Build a minimal ticker entry dict.

    Args:
        symbol: Ticker symbol.
        sector: Sector label.
        sector_etf: Sector ETF symbol or None.
        added: ISO date string.
        active: Whether the ticker is active.
        **extra: Additional fields (e.g. former_symbol, symbol_since).

    Returns:
        dict: Ticker entry.
    """
    entry: dict[str, Any] = {
        "symbol": symbol,
        "sector": sector,
        "sector_etf": sector_etf,
        "added": added,
        "active": active,
    }
    entry.update(extra)
    return entry


def _make_fetch_df(rows: list[dict[str, str]]) -> pd.DataFrame:
    """
    Build a DataFrame that looks like a Wikipedia S&P 500 table row for testing.

    Args:
        rows: List of dicts with 'Symbol' and 'GICS Sector' keys (at minimum).

    Returns:
        pd.DataFrame: DataFrame with S&P 500 Wikipedia table columns.
    """
    columns = ["Symbol", "Security", "GICS Sector", "GICS Sub-Industry",
               "Headquarters Location", "Date added", "CIK", "Founded"]
    data = []
    for row in rows:
        data.append({
            "Symbol": row["Symbol"],
            "Security": row.get("Security", "Test Corp"),
            "GICS Sector": row["GICS Sector"],
            "GICS Sub-Industry": row.get("GICS Sub-Industry", "Test Sub"),
            "Headquarters Location": row.get("Headquarters Location", "Test City"),
            "Date added": row.get("Date added", "2000-01-01"),
            "CIK": row.get("CIK", "000001"),
            "Founded": row.get("Founded", "1900"),
        })
    return pd.DataFrame(data, columns=columns)


# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

import scripts.merge_sp500_into_tickers as merge_mod
from scripts.merge_sp500_into_tickers import (
    MergeReport,
    fetch_sp500_constituents,
    map_gics_sector_to_config,
    merge_into_existing,
    normalize_symbol,
    sort_tickers,
    write_config,
)


# ---------------------------------------------------------------------------
# Test 1: fetch_parses_wikipedia_fixture
# ---------------------------------------------------------------------------

def test_fetch_parses_wikipedia_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    fetch_sp500_constituents should parse a Wikipedia-style table via pandas.read_html.

    Monkeypatches pandas.read_html with a DataFrame mirroring the real Wikipedia
    columns (no network, no lxml dependency). Exercises column validation and
    dot-form symbol preservation.
    """
    fixture_df = pd.DataFrame({
        "Symbol": ["AAPL", "BRK.B", "JNJ", "GOOGL", "BF.B", "NEE"],
        "Security": [
            "Apple Inc.",
            "Berkshire Hathaway",
            "Johnson & Johnson",
            "Alphabet Inc. (Class A)",
            "Brown-Forman (Class B)",
            "NextEra Energy",
        ],
        "GICS Sector": [
            "Information Technology",
            "Financials",
            "Health Care",
            "Communication Services",
            "Consumer Staples",
            "Utilities",
        ],
        "GICS Sub-Industry": [""] * 6,
        "Headquarters Location": [""] * 6,
        "Date added": [""] * 6,
        "CIK": [""] * 6,
        "Founded": [""] * 6,
    })

    monkeypatch.setattr("pandas.read_html", lambda url, **kwargs: [fixture_df])

    result = fetch_sp500_constituents("http://fake-url")

    symbols = [entry["symbol"] for entry in result]
    sectors = [entry["gics_sector"] for entry in result]

    assert "AAPL" in symbols
    assert "BRK.B" in symbols  # dot-form preserved
    assert "BF.B" in symbols   # another dot-form
    assert "Information Technology" in sectors
    assert "Health Care" in sectors
    assert "Communication Services" in sectors


# ---------------------------------------------------------------------------
# Test 2: normalize_symbol
# ---------------------------------------------------------------------------

def test_normalize_symbol_slash_to_dot() -> None:
    """normalize_symbol should uppercase and convert / to ."""
    assert normalize_symbol("RDS/A") == "RDS.A"
    assert normalize_symbol("AAPL") == "AAPL"
    assert normalize_symbol("brk.b") == "BRK.B"
    assert normalize_symbol("brkb") == "BRKB"


# ---------------------------------------------------------------------------
# Test 3: map_gics_sector_translations
# ---------------------------------------------------------------------------

def test_map_gics_sector_translations() -> None:
    """map_gics_sector_to_config should map all 11 GICS sectors correctly."""
    expected = {
        "Communication Services": ("Communication Services", "XLC"),
        "Consumer Discretionary": ("Consumer Discretionary", "XLY"),
        "Consumer Staples": ("Consumer Staples", "XLP"),
        "Energy": ("Energy", "XLE"),
        "Financials": ("Financials", "XLF"),
        "Health Care": ("Healthcare", "XLV"),
        "Industrials": ("Industrials", "XLI"),
        "Information Technology": ("Technology", "XLK"),
        "Materials": ("Materials", "XLB"),
        "Real Estate": ("Real Estate", "XLRE"),
        "Utilities": ("Utilities", "XLU"),
    }
    for gics_sector, expected_tuple in expected.items():
        result = map_gics_sector_to_config(gics_sector)
        assert result == expected_tuple, f"Sector '{gics_sector}' mapped to {result!r}, expected {expected_tuple!r}"

    with pytest.raises(KeyError):
        map_gics_sector_to_config("Crypto")


# ---------------------------------------------------------------------------
# Test 4: merge_clean_no_duplicates
# ---------------------------------------------------------------------------

def test_merge_clean_no_duplicates() -> None:
    """
    merge_into_existing with 3-entry overlap and 2 new: result has 5 entries,
    2 new entries have correct fields.
    """
    existing_tickers = [
        _ticker_entry("AAPL", sector="Technology", sector_etf="XLK"),
        _ticker_entry("MSFT", sector="Technology", sector_etf="XLK"),
        _ticker_entry("JPM", sector="Financials", sector_etf="XLF"),
    ]
    existing_config = _make_config(existing_tickers)

    new_entries = [
        {"symbol": "AAPL", "gics_sector": "Information Technology"},
        {"symbol": "MSFT", "gics_sector": "Information Technology"},
        {"symbol": "JPM", "gics_sector": "Financials"},
        {"symbol": "NEE", "gics_sector": "Utilities"},
        {"symbol": "XOM", "gics_sector": "Energy"},
    ]

    result_config, report = merge_into_existing(existing_config, new_entries, TODAY)

    result_tickers = result_config["tickers"]
    assert len(result_tickers) == 5

    result_symbols = {entry["symbol"] for entry in result_tickers}
    assert "NEE" in result_symbols
    assert "XOM" in result_symbols

    # New entries should have added=TODAY, active=True, correct sector/sector_etf
    nee_entry = next(e for e in result_tickers if e["symbol"] == "NEE")
    assert nee_entry["added"] == TODAY
    assert nee_entry["active"] is True
    assert nee_entry["sector"] == "Utilities"
    assert nee_entry["sector_etf"] == "XLU"

    xom_entry = next(e for e in result_tickers if e["symbol"] == "XOM")
    assert xom_entry["sector"] == "Energy"
    assert xom_entry["sector_etf"] == "XLE"

    assert len(report.skipped_already_present) == 3
    assert len(report.new_appended) == 2
    assert "NEE" in report.new_appended
    assert "XOM" in report.new_appended


# ---------------------------------------------------------------------------
# Test 5: merge_all_already_present_noop
# ---------------------------------------------------------------------------

def test_merge_all_already_present_noop() -> None:
    """
    When all fetched entries match existing, no new entries should be appended.
    """
    existing_tickers = [
        _ticker_entry("AAPL", sector="Technology", sector_etf="XLK"),
        _ticker_entry("MSFT", sector="Technology", sector_etf="XLK"),
    ]
    existing_config = _make_config(existing_tickers)

    new_entries = [
        {"symbol": "AAPL", "gics_sector": "Information Technology"},
        {"symbol": "MSFT", "gics_sector": "Information Technology"},
    ]

    result_config, report = merge_into_existing(existing_config, new_entries, TODAY)

    assert len(result_config["tickers"]) == 2
    assert len(report.new_appended) == 0
    assert len(report.skipped_already_present) == 2


# ---------------------------------------------------------------------------
# Test 6: merge_idempotent_two_runs
# ---------------------------------------------------------------------------

def test_merge_idempotent_two_runs(tmp_path: Path) -> None:
    """
    Running merge twice with same fetch input and same today_iso produces
    byte-equal output.
    """
    existing_tickers = [
        _ticker_entry("AAPL", sector="Technology", sector_etf="XLK"),
    ]
    existing_config = _make_config(
        existing_tickers,
        sector_etfs=["XLK"],
        market_benchmarks={"spy": "SPY"},
    )

    new_entries = [
        {"symbol": "AAPL", "gics_sector": "Information Technology"},
        {"symbol": "NEE", "gics_sector": "Utilities"},
    ]

    result1, _ = merge_into_existing(existing_config, new_entries, TODAY)
    result2, _ = merge_into_existing(result1, new_entries, TODAY)

    bytes1 = json.dumps(result1, indent=2, ensure_ascii=False) + "\n"
    bytes2 = json.dumps(result2, indent=2, ensure_ascii=False) + "\n"
    assert bytes1 == bytes2


# ---------------------------------------------------------------------------
# Test 7: merge_unmapped_sector_aborts_before_write
# ---------------------------------------------------------------------------

def test_merge_unmapped_sector_aborts_before_write(tmp_path: Path) -> None:
    """
    main() should return exit code 2 and leave the config file byte-unchanged
    when a fetched entry has an unmapped GICS sector.
    """
    config_path = tmp_path / "tickers.json"
    existing_config = _make_config(
        [_ticker_entry("AAPL", sector="Technology", sector_etf="XLK")],
    )
    _write_config(config_path, existing_config)
    original_bytes = config_path.read_bytes()

    fetch_df = _make_fetch_df([
        {"Symbol": "AAPL", "GICS Sector": "Information Technology"},
        {"Symbol": "FAKECOIN", "GICS Sector": "Crypto"},  # unmapped
    ])

    with patch("pandas.read_html", return_value=[fetch_df]):
        exit_code = merge_mod.main(
            args=["--config-path", str(config_path)]
        )

    assert exit_code == 2
    assert config_path.read_bytes() == original_bytes


# ---------------------------------------------------------------------------
# Test 8: merge_normalises_before_dedupe
# ---------------------------------------------------------------------------

def test_merge_normalises_before_dedupe() -> None:
    """
    When existing config has 'BRK.B' and fetch contains 'BRK.B',
    it should be classified as already-present (not appended).
    """
    existing_tickers = [
        _ticker_entry("BRK.B", sector="Financials", sector_etf="XLF"),
    ]
    existing_config = _make_config(existing_tickers)

    new_entries = [
        {"symbol": "BRK.B", "gics_sector": "Financials"},
    ]

    result_config, report = merge_into_existing(existing_config, new_entries, TODAY)

    assert len(result_config["tickers"]) == 1
    assert len(report.new_appended) == 0
    assert "BRK.B" in report.skipped_already_present


# ---------------------------------------------------------------------------
# Test 9: merge_flags_rename_collision
# ---------------------------------------------------------------------------

def test_merge_flags_rename_collision() -> None:
    """
    When existing entry has former_symbol=FB and fetch contains FB,
    it should appear in manual_review and NOT in new_appended.
    """
    existing_tickers = [
        _ticker_entry(
            "META",
            sector="Communication Services",
            sector_etf="XLC",
            former_symbol="FB",
        ),
    ]
    existing_config = _make_config(existing_tickers)

    new_entries = [
        {"symbol": "FB", "gics_sector": "Communication Services"},
    ]

    result_config, report = merge_into_existing(existing_config, new_entries, TODAY)

    assert len(report.manual_review) > 0
    assert "FB" not in report.new_appended
    # The entry should NOT be appended
    result_symbols = [entry["symbol"] for entry in result_config["tickers"]]
    assert "FB" not in result_symbols


# ---------------------------------------------------------------------------
# Test 10: merge_preserves_extra_fields_verbatim
# ---------------------------------------------------------------------------

def test_merge_preserves_extra_fields_verbatim() -> None:
    """
    Existing entries with extra fields (former_symbol, symbol_since) must be
    preserved verbatim after merge.
    """
    meta_entry = _ticker_entry(
        "META",
        sector="Communication Services",
        sector_etf="XLC",
        added="2026-03-16",
        former_symbol="FB",
        symbol_since="2022-06-09",
    )
    existing_config = _make_config([meta_entry])

    new_entries = [
        {"symbol": "META", "gics_sector": "Communication Services"},
        {"symbol": "NEE", "gics_sector": "Utilities"},
    ]

    result_config, report = merge_into_existing(existing_config, new_entries, TODAY)

    result_meta = next(e for e in result_config["tickers"] if e["symbol"] == "META")
    assert result_meta.get("former_symbol") == "FB"
    assert result_meta.get("symbol_since") == "2022-06-09"
    assert result_meta.get("sector") == "Communication Services"


# ---------------------------------------------------------------------------
# Test 11: sort_tickers_stocks_then_index
# ---------------------------------------------------------------------------

def test_sort_tickers_stocks_then_index() -> None:
    """
    sort_tickers should place non-Index stocks first alphabetically,
    then Index entries alphabetically.
    """
    tickers = [
        _ticker_entry("TSLA", sector="Consumer Discretionary", sector_etf="XLY"),
        _ticker_entry("QQQ", sector="Index", sector_etf=None),
        _ticker_entry("AAPL", sector="Technology", sector_etf="XLK"),
        _ticker_entry("DIA", sector="Index", sector_etf=None),
        _ticker_entry("MSFT", sector="Technology", sector_etf="XLK"),
    ]

    sorted_result = sort_tickers(tickers)
    symbols = [entry["symbol"] for entry in sorted_result]

    # Stocks come first, alphabetically
    assert symbols[:3] == ["AAPL", "MSFT", "TSLA"]
    # Index entries come after, alphabetically
    assert symbols[3:] == ["DIA", "QQQ"]


# ---------------------------------------------------------------------------
# Test 12: write_preserves_sibling_blocks
# ---------------------------------------------------------------------------

def test_write_preserves_sibling_blocks(tmp_path: Path) -> None:
    """
    write_config should preserve sibling blocks (sector_etfs, market_benchmarks)
    unchanged in the output JSON.
    """
    config = _make_config(
        [_ticker_entry("AAPL", sector="Technology", sector_etf="XLK")],
        sector_etfs=["XLK", "XLF", "XLV"],
        market_benchmarks={"spy": "SPY", "qqq": "QQQ", "vix": "^VIX"},
    )
    output_path = tmp_path / "tickers.json"
    write_config(output_path, config)

    result = _read_config(output_path)
    assert result["sector_etfs"] == ["XLK", "XLF", "XLV"]
    assert result["market_benchmarks"] == {"spy": "SPY", "qqq": "QQQ", "vix": "^VIX"}
    assert len(result["tickers"]) == 1


# ---------------------------------------------------------------------------
# Test 13: fetch_failure_exits_nonzero_no_write
# ---------------------------------------------------------------------------

def test_fetch_failure_exits_nonzero_no_write(tmp_path: Path) -> None:
    """
    When pandas.read_html raises, main() should return exit code 3
    and the config file should be byte-unchanged.
    """
    config_path = tmp_path / "tickers.json"
    existing_config = _make_config(
        [_ticker_entry("AAPL", sector="Technology", sector_etf="XLK")],
    )
    _write_config(config_path, existing_config)
    original_bytes = config_path.read_bytes()

    with patch("pandas.read_html", side_effect=ValueError("network unreachable")):
        exit_code = merge_mod.main(
            args=["--config-path", str(config_path)]
        )

    assert exit_code == 3
    assert config_path.read_bytes() == original_bytes


# ---------------------------------------------------------------------------
# Test 14: two_run_growth_full_alphabetical
# ---------------------------------------------------------------------------

def test_two_run_growth_full_alphabetical(tmp_path: Path) -> None:
    """
    After two merge runs with growing fetch sets, the final ticker list should
    be fully alphabetised (stocks first, then Index entries) including newly
    interleaved additions.
    """
    config_path = tmp_path / "tickers.json"
    base_config = _make_config(
        [_ticker_entry("TSLA", sector="Consumer Discretionary", sector_etf="XLY")],
        sector_etfs=["XLY"],
        market_benchmarks={"spy": "SPY"},
    )
    _write_config(config_path, base_config)

    # Run 1: append AMZN, NVDA, TSLA (TSLA already present)
    fetch_df_1 = _make_fetch_df([
        {"Symbol": "AMZN", "GICS Sector": "Consumer Discretionary"},
        {"Symbol": "NVDA", "GICS Sector": "Information Technology"},
        {"Symbol": "TSLA", "GICS Sector": "Consumer Discretionary"},
    ])
    with patch("pandas.read_html", return_value=[fetch_df_1]):
        exit_code_1 = merge_mod.main(args=["--config-path", str(config_path)])
    assert exit_code_1 == 0

    config_after_run1 = _read_config(config_path)
    symbols_run1 = [e["symbol"] for e in config_after_run1["tickers"]]
    assert symbols_run1 == ["AMZN", "NVDA", "TSLA"]

    # Run 2: append DASH, FOO, XYZ (AMZN+NVDA already present)
    fetch_df_2 = _make_fetch_df([
        {"Symbol": "DASH", "GICS Sector": "Consumer Discretionary"},
        {"Symbol": "FOO", "GICS Sector": "Information Technology"},
        {"Symbol": "XYZ", "GICS Sector": "Materials"},
        {"Symbol": "AMZN", "GICS Sector": "Consumer Discretionary"},
        {"Symbol": "NVDA", "GICS Sector": "Information Technology"},
    ])
    with patch("pandas.read_html", return_value=[fetch_df_2]):
        exit_code_2 = merge_mod.main(args=["--config-path", str(config_path)])
    assert exit_code_2 == 0

    config_after_run2 = _read_config(config_path)
    symbols_run2 = [e["symbol"] for e in config_after_run2["tickers"]]

    # All 6 stocks interleaved alphabetically, no Index entries
    assert symbols_run2 == ["AMZN", "DASH", "FOO", "NVDA", "TSLA", "XYZ"]


# ---------------------------------------------------------------------------
# Test 15: merge_rejects_malformed_config_missing_tickers_key
# ---------------------------------------------------------------------------

def test_merge_rejects_malformed_config_missing_tickers_key() -> None:
    """
    merge_into_existing must raise ValueError when 'tickers' key is absent,
    to prevent silently rebuilding an empty config and losing curated metadata.
    """
    malformed = {"sector_etfs": ["XLK", "XLF"]}  # no 'tickers' key

    with pytest.raises(ValueError, match="missing the required 'tickers' key"):
        merge_into_existing(malformed, [], TODAY)
