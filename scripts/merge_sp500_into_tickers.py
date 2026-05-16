"""
One-off operator script: merge S&P 500 constituents from Wikipedia into config/tickers.json.

Fetches the current S&P 500 list from Wikipedia, normalises symbols to dot-form
(matching Polygon's API requirement), and appends any tickers not already present
in the config. Existing entries are never modified or reconstructed — only their
position in the list may change due to sorting.

Usage:
    python scripts/merge_sp500_into_tickers.py [--dry-run] [--url URL] [--config-path PATH]

Exit codes:
    0 — success or no-op (nothing new to add)
    2 — unmapped GICS sector (KeyError); config file not written
    3 — fetch/parse failure (ValueError, network error, etc.); config file not written
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Operator-script constant. Overridable via --url; the Wikipedia page is the
# de-facto free source for S&P 500 constituents and is not expected to change.
WIKIPEDIA_URL: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Default path relative to repo root; overridden by --config-path in tests.
_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "tickers.json"

# ---------------------------------------------------------------------------
# MergeReport dataclass
# ---------------------------------------------------------------------------


@dataclass
class MergeReport:
    """
    Summary of what happened during a merge operation.

    Attributes:
        existing_active: Number of active entries in the existing config before merge.
        existing_inactive: Number of inactive entries in the existing config before merge.
        fetched_count: Total number of entries returned by the Wikipedia fetch.
        skipped_already_present: Symbols that were fetched but already exist in the config.
        manual_review: List of (candidate_symbol, existing_symbol) pairs where the
            candidate matches an existing entry's former_symbol — needs human review.
        new_appended: Symbols that were appended as new entries.
        normalisations_applied: List of (raw, normalised) pairs where the raw symbol
            differed from its normalised form.
    """

    existing_active: int = 0
    existing_inactive: int = 0
    fetched_count: int = 0
    skipped_already_present: list[str] = field(default_factory=list)
    manual_review: list[tuple[str, str]] = field(default_factory=list)
    new_appended: list[str] = field(default_factory=list)
    normalisations_applied: list[tuple[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def fetch_sp500_constituents(url: str) -> list[dict[str, str]]:
    """
    Fetch the current S&P 500 constituent list from a Wikipedia-style HTML table.

    Reads the first <table> returned by pandas.read_html(url). Expects the table
    to have columns named 'Symbol' and 'GICS Sector'. Strips leading/trailing
    whitespace from both fields.

    Args:
        url: URL of the Wikipedia S&P 500 page (or any compatible HTML table source).

    Returns:
        list[dict]: Each dict has keys 'symbol' (str) and 'gics_sector' (str).

    Raises:
        ValueError: If the table is not found, the URL is unreachable, or the
            expected columns are missing from the first DataFrame returned.
    """
    logger.info(f"phase=fetch url={url} — fetching S&P 500 constituents")
    try:
        tables = pd.read_html(url)
    except Exception as exc:
        raise ValueError(f"Failed to fetch or parse HTML from url={url!r}: {exc}") from exc

    if not tables:
        raise ValueError(f"No tables found at url={url!r}")

    df = tables[0]

    expected_columns = {"Symbol", "GICS Sector"}
    actual_columns = set(df.columns.tolist())
    if not expected_columns.issubset(actual_columns):
        raise ValueError(
            f"Expected columns {sorted(expected_columns)} not found in table. "
            f"Found columns: {sorted(actual_columns)}"
        )

    result: list[dict[str, str]] = []
    for _, row in df.iterrows():
        raw_symbol = str(row["Symbol"]).strip()
        gics_sector = str(row["GICS Sector"]).strip()
        result.append({"symbol": raw_symbol, "gics_sector": gics_sector})

    logger.info(f"phase=fetch — parsed {len(result)} constituents from table")
    return result


def normalize_symbol(raw_symbol: str) -> str:
    """
    Normalise a raw ticker symbol to the pipeline's canonical (Polygon dot-form).

    Converts to uppercase and replaces '/' with '.' (slash → dot). Wikipedia
    almost never uses slash, but the substitution is defensive. Dot-form is the
    canonical form because Polygon (primary OHLCV source) requires it.

    Args:
        raw_symbol: Raw ticker string as scraped from the source (e.g. 'BRK.B',
            'RDS/A', 'brk.b').

    Returns:
        str: Normalised symbol in Polygon dot-form (e.g. 'BRK.B', 'RDS.A').
    """
    return raw_symbol.upper().replace("/", ".")


def map_gics_sector_to_config(gics_sector: str) -> tuple[str, str]:
    """
    Map a GICS sector string (as used on Wikipedia) to the pipeline's
    (sector_label, sector_etf) tuple.

    The mapping is a hardcoded 11-row table covering all standard GICS sectors.
    Raises KeyError for any unrecognised sector to surface data quality issues
    immediately rather than silently inserting a malformed entry.

    Args:
        gics_sector: GICS sector string, e.g. 'Information Technology'.

    Returns:
        tuple[str, str]: (sector_label, sector_etf) as used in config/tickers.json.
            For example: ('Technology', 'XLK').

    Raises:
        KeyError: If gics_sector is not in the 11-row mapping table.
    """
    mapping: dict[str, tuple[str, str]] = {
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
    if gics_sector not in mapping:
        raise KeyError(
            f"Unmapped GICS sector: {gics_sector!r}. "
            f"Known sectors: {sorted(mapping.keys())}"
        )
    return mapping[gics_sector]


def merge_into_existing(
    existing_config: dict[str, Any],
    new_entries: list[dict[str, str]],
    today_iso: str,
) -> tuple[dict[str, Any], MergeReport]:
    """
    Merge fetched S&P 500 entries into an existing config dict.

    Pure function — does not read or write files. Preserves existing entry dicts
    verbatim (no reconstruction). Only sorts the final list.

    Rules:
    - If the normalised symbol matches an existing entry's 'symbol' field:
      mark as already_present, skip.
    - If the normalised symbol matches any existing entry's 'former_symbol' field:
      add to manual_review, skip (do not append).
    - Otherwise: append a new entry with the mapped sector/sector_etf,
      added=today_iso, active=True.

    Args:
        existing_config: The full config dict loaded from tickers.json.
        new_entries: List of dicts from fetch_sp500_constituents, each with
            'symbol' and 'gics_sector' keys.
        today_iso: ISO 8601 date string (YYYY-MM-DD) to stamp new entries' 'added' field.

    Returns:
        tuple[dict, MergeReport]: Updated config dict and a MergeReport summary.
            The config dict's 'tickers' list is sorted (stocks alphabetical, then
            Index entries alphabetical). All other top-level keys are unchanged.
    """
    if "tickers" not in existing_config:
        raise ValueError(
            "existing_config is missing the required 'tickers' key — refusing to "
            "merge into a malformed config (would silently lose all existing entries)"
        )
    existing_tickers: list[dict[str, Any]] = existing_config["tickers"]

    report = MergeReport()
    report.existing_active = sum(1 for t in existing_tickers if t.get("active", True))
    report.existing_inactive = sum(1 for t in existing_tickers if not t.get("active", True))

    # Build lookup sets from existing tickers
    existing_symbols: set[str] = {entry["symbol"] for entry in existing_tickers}
    # Map former_symbol → existing entry symbol for collision detection
    former_symbol_map: dict[str, str] = {}
    for entry in existing_tickers:
        former = entry.get("former_symbol")
        if former:
            former_symbol_map[former] = entry["symbol"]

    report.fetched_count = len(new_entries)
    tickers_to_add: list[dict[str, Any]] = []

    for raw_entry in new_entries:
        raw_symbol = raw_entry["symbol"]
        normalised = normalize_symbol(raw_symbol)

        if normalised != raw_symbol:
            report.normalisations_applied.append((raw_symbol, normalised))
            logger.info(
                f"phase=merge — normalised symbol raw={raw_symbol!r} to={normalised!r}"
            )

        if normalised in existing_symbols:
            report.skipped_already_present.append(normalised)
            logger.info(f"phase=merge symbol={normalised} — already present, skipping")
            continue

        if normalised in former_symbol_map:
            existing_current = former_symbol_map[normalised]
            report.manual_review.append((normalised, existing_current))
            logger.warning(
                f"phase=merge symbol={normalised} — matches former_symbol of "
                f"existing entry symbol={existing_current!r}; flagged for manual review"
            )
            continue

        # Map sector; let KeyError propagate so caller can handle exit code 2
        sector_label, sector_etf = map_gics_sector_to_config(raw_entry["gics_sector"])

        new_ticker: dict[str, Any] = {
            "symbol": normalised,
            "sector": sector_label,
            "sector_etf": sector_etf,
            "added": today_iso,
            "active": True,
        }
        tickers_to_add.append(new_ticker)
        report.new_appended.append(normalised)
        logger.info(
            f"phase=merge symbol={normalised} sector={sector_label} "
            f"sector_etf={sector_etf} — appended"
        )

    combined = list(existing_tickers) + tickers_to_add
    sorted_combined = sort_tickers(combined)

    result_config = dict(existing_config)
    result_config["tickers"] = sorted_combined

    return result_config, report


def sort_tickers(tickers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Sort tickers so that non-Index stocks come first (alphabetically by symbol),
    followed by Index entries (alphabetically by symbol).

    Args:
        tickers: List of ticker entry dicts, each with 'symbol' and 'sector' keys.

    Returns:
        list[dict]: Sorted copy of the input list.
    """
    stocks = sorted(
        [entry for entry in tickers if entry.get("sector") != "Index"],
        key=lambda entry: entry["symbol"],
    )
    indexes = sorted(
        [entry for entry in tickers if entry.get("sector") == "Index"],
        key=lambda entry: entry["symbol"],
    )
    return stocks + indexes


def write_config(path: Path, config: dict[str, Any]) -> None:
    """
    Write a config dict to a JSON file atomically with a trailing newline.

    Uses a .tmp sibling file and os.replace() for atomicity. If the serialised
    bytes are identical to the existing file content, skips the write entirely.

    Args:
        path: Destination path (e.g. config/tickers.json).
        config: Config dict to serialise.

    Returns:
        None
    """
    new_bytes = (json.dumps(config, indent=2, ensure_ascii=False) + "\n").encode("utf-8")

    if path.exists():
        existing_bytes = path.read_bytes()
        if new_bytes == existing_bytes:
            logger.info(f"phase=write path={path} — bytes identical, skipping write")
            return

    tmp_path = path.parent / (path.name + ".tmp")
    try:
        tmp_path.write_bytes(new_bytes)
        os.replace(tmp_path, path)
        logger.info(f"phase=write path={path} — written successfully")
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def _print_report(report: MergeReport, dry_run: bool) -> None:
    """
    Print a human-readable merge report to stdout.

    Args:
        report: The MergeReport from merge_into_existing.
        dry_run: If True, prefix output with [DRY RUN] notice.

    Returns:
        None
    """
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}=== Merge S&P 500 into tickers.json ===")
    print(f"  Existing (active):    {report.existing_active}")
    print(f"  Existing (inactive):  {report.existing_inactive}")
    print(f"  Fetched from source:  {report.fetched_count}")
    print(f"  Already present:      {len(report.skipped_already_present)}")
    print(f"  New appended:         {len(report.new_appended)}")
    print(f"  Manual review needed: {len(report.manual_review)}")
    print(f"  Normalisations:       {len(report.normalisations_applied)}")

    if report.new_appended:
        print("\n  New symbols appended:")
        for sym in sorted(report.new_appended):
            print(f"    + {sym}")

    if report.manual_review:
        print("\n  Manual review required (candidate → existing):")
        for candidate, existing in report.manual_review:
            print(f"    ! {candidate} → {existing} (former_symbol collision)")

    if report.normalisations_applied:
        print("\n  Normalisations applied (raw → canonical):")
        for raw, normalised in report.normalisations_applied:
            print(f"    ~ {raw} → {normalised}")

    if dry_run:
        print("\n  [DRY RUN] No files were written.")
    print()


def main(args: list[str] | None = None) -> int:
    """
    CLI entry point for merging S&P 500 constituents into config/tickers.json.

    Parses arguments, fetches Wikipedia data, computes the merge, and (unless
    --dry-run) writes the updated config file atomically.

    Args:
        args: Argument list for testing (passed to argparse). If None, reads
            from sys.argv.

    Returns:
        int: Exit code — 0 success/no-op, 2 unmapped sector, 3 fetch/parse failure.
    """
    parser = argparse.ArgumentParser(
        description="Merge S&P 500 Wikipedia constituents into config/tickers.json."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the merge report without writing any files.",
    )
    parser.add_argument(
        "--url",
        default=WIKIPEDIA_URL,
        help=f"Wikipedia S&P 500 URL (default: {WIKIPEDIA_URL})",
    )
    parser.add_argument(
        "--config-path",
        default=str(_DEFAULT_CONFIG_PATH),
        help="Path to config/tickers.json (default: repo-relative path).",
    )
    parsed = parser.parse_args(args)

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        level=logging.INFO,
    )

    config_path = Path(parsed.config_path)
    dry_run: bool = parsed.dry_run
    url: str = parsed.url

    # --- Load existing config ---
    logger.info(f"phase=load config_path={config_path}")
    try:
        existing_config: dict[str, Any] = json.loads(
            config_path.read_text(encoding="utf-8")
        )
    except Exception as exc:
        logger.error(f"phase=load config_path={config_path} — failed to read: {exc!r}")
        return 3

    # --- Fetch S&P 500 constituents ---
    try:
        new_entries = fetch_sp500_constituents(url)
    except ValueError as exc:
        logger.error(f"phase=fetch — {exc!r}")
        return 3

    # --- Merge ---
    today_iso = datetime.date.today().isoformat()

    try:
        result_config, report = merge_into_existing(existing_config, new_entries, today_iso)
    except KeyError as exc:
        logger.error(f"phase=merge — unmapped GICS sector: {exc!r}")
        return 2
    except ValueError as exc:
        logger.error(f"phase=merge — malformed existing config: {exc!r}")
        return 3

    # --- Report ---
    _print_report(report, dry_run)
    logger.info(
        f"phase=summary new_appended={len(report.new_appended)} "
        f"manual_review={len(report.manual_review)} "
        f"already_present={len(report.skipped_already_present)}"
    )

    # --- Write ---
    if dry_run:
        logger.info("phase=write dry_run=True — skipping file write")
        return 0

    if not report.new_appended:
        logger.info("phase=write — no new entries; config file unchanged")
        return 0

    try:
        write_config(config_path, result_config)
    except Exception as exc:
        logger.error(f"phase=write config_path={config_path} — write failed: {exc!r}")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
