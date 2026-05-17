"""
One-off operator script: select the top-N stocks by realised volatility and toggle
active flags in config/tickers.json accordingly.

Reads ohlcv_daily from the local SQLite database, computes 90-trading-day
annualised realised volatility and average dollar volume (ADV) for every
non-Index ticker, selects the top target_count by vol (with an ADV floor),
and writes updated active flags back to config/tickers.json.

Index entries (sector == "Index") always bypass the rule and are kept active.
Backfilled history for deactivated tickers is preserved untouched.

Usage:
    python scripts/select_volatile_universe.py [--dry-run] [--target N] \\
           [--liquidity-floor DOLLARS] [--window-trading-days D] \\
           [--min-history D] [--as-of YYYY-MM-DD] \\
           [--config-path PATH] [--universe-config-path PATH]

Exit codes:
    0 — success or no-op (bytes-equal)
    2 — unrecoverable internal error (e.g. DB unreadable)
    3 — fewer than target_count candidates eligible (partial result still written)
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import math
import os
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from statistics import pstdev
from typing import Any

logger = logging.getLogger(__name__)

# Default paths relative to repo root.
_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "tickers.json"
_DEFAULT_UNIVERSE_CONFIG_PATH = Path(__file__).parent.parent / "config" / "universe_selection.json"
_DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "signals.db"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class UniverseConfig:
    """
    Configuration for universe selection loaded from universe_selection.json.

    Attributes:
        target_count: Number of active stocks to select (Index entries are additional).
        liquidity_floor_usd: Minimum 90-day average dollar volume in USD.
        window_trading_days: Number of trading days used to compute vol and ADV.
        window_calendar_days: Calendar-day lookback for the SQL WHERE clause.
            Should be approximately 1.45 × window_trading_days (e.g. 130 for 90 trading days).
        min_history_returns: Minimum number of log returns required before a ticker
            is eligible. Below this threshold, vol is returned as None and the ticker
            is excluded from selection.
    """

    target_count: int
    liquidity_floor_usd: float
    window_trading_days: int
    window_calendar_days: int
    min_history_returns: int


@dataclass
class TickerMetric:
    """
    Computed metrics for a single ticker used in universe selection.

    Attributes:
        ticker: Ticker symbol.
        vol: Annualised realised volatility as a percentage (e.g. 31.7 means 31.7%).
            None if insufficient history.
        adv: Average dollar volume (close × volume) over the window period.
    """

    ticker: str
    vol: float | None
    adv: float


@dataclass
class SelectionResult:
    """
    Output from select_universe().

    Attributes:
        selected: Ordered list of selected ticker symbols (highest vol first).
        filtered_low_history: Tickers excluded because vol was None (insufficient data).
        filtered_low_liquidity: Tickers excluded because ADV was below the floor.
        cutoff_vol: Annualised vol of the last-selected ticker, or None if no tickers
            were selected.
    """

    selected: list[str]
    filtered_low_history: list[str]
    filtered_low_liquidity: list[str]
    cutoff_vol: float | None


# ---------------------------------------------------------------------------
# Pure computation functions
# ---------------------------------------------------------------------------


def load_universe_config(path: Path) -> UniverseConfig:
    """
    Load universe selection configuration from a JSON file.

    Args:
        path: Path to universe_selection.json.

    Returns:
        UniverseConfig populated from the JSON file.

    Raises:
        FileNotFoundError: If the config file does not exist.
        KeyError: If any expected key is missing from the JSON.
        ValueError: If the JSON is malformed.
    """
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return UniverseConfig(
        target_count=int(raw["target_count"]),
        liquidity_floor_usd=float(raw["liquidity_floor_usd"]),
        window_trading_days=int(raw["window_trading_days"]),
        window_calendar_days=int(raw["window_calendar_days"]),
        min_history_returns=int(raw["min_history_returns"]),
    )


def compute_realized_vol(closes: list[float], min_history_returns: int) -> float | None:
    """
    Compute annualised realised volatility from a sequence of daily close prices.

    Uses population standard deviation (pstdev, not sample stdev) of log returns,
    scaled by sqrt(252) × 100 to express the result as an annualised percentage.
    Population stdev is chosen because we are ranking tickers against each other —
    the rank order is identical to sample stdev, and pstdev avoids the undefined
    case when only one return is available.

    Args:
        closes: Ordered list of daily close prices (oldest first).
        min_history_returns: Minimum number of valid log returns required.
            If the resulting return series is shorter than this, returns None.

    Returns:
        Annualised realised vol as a percentage (e.g. 31.7 for 31.7%), or None
        if the closes list is too short to produce min_history_returns valid returns.
        A constant-price series (all closes identical) legitimately returns 0.0.
    """
    if len(closes) < 2:
        return None

    log_returns: list[float] = []
    for index in range(1, len(closes)):
        prior = closes[index - 1]
        current = closes[index]
        if prior <= 0 or current <= 0:
            continue
        log_returns.append(math.log(current / prior))

    if len(log_returns) < min_history_returns:
        return None

    daily_vol = pstdev(log_returns)
    return daily_vol * math.sqrt(252) * 100


def compute_avg_dollar_volume(bars: list[tuple[float, float]]) -> float:
    """
    Compute the average dollar volume over a list of (close, volume) bars.

    Dollar volume per bar is close × volume. Note that volume is REAL (fractional
    shares from Polygon), not integer — do not cast to int.

    Args:
        bars: List of (close_price, volume) tuples. May be empty.

    Returns:
        Mean of close × volume across all bars. Returns 0.0 for an empty list.
    """
    if not bars:
        return 0.0
    total_dollar_volume = sum(close * volume for close, volume in bars)
    return total_dollar_volume / len(bars)


def select_universe(
    metrics: list[TickerMetric],
    target_count: int,
    liquidity_floor: float,
) -> SelectionResult:
    """
    Select the top target_count tickers by realised volatility subject to a
    liquidity floor.

    Filters:
    1. Tickers with vol=None (insufficient history) are excluded into filtered_low_history.
    2. Tickers with ADV < liquidity_floor are excluded into filtered_low_liquidity.
    3. Remaining eligible tickers are sorted by (-vol, ticker) — primary sort by vol
       descending, secondary sort by ticker symbol ascending as a deterministic tie-breaker.
    4. The top target_count are selected.

    Args:
        metrics: List of TickerMetric (ticker, vol, adv) for all candidates.
        target_count: Maximum number of tickers to select.
        liquidity_floor: Minimum ADV in USD to be eligible.

    Returns:
        SelectionResult with the selected symbols, exclusion lists, and the cutoff vol
        (vol of the last-selected ticker, or None if nothing was selected).
    """
    filtered_low_history: list[str] = []
    filtered_low_liquidity: list[str] = []
    eligible: list[TickerMetric] = []

    for metric in metrics:
        if metric.vol is None:
            filtered_low_history.append(metric.ticker)
            logger.info(
                f"phase=select ticker={metric.ticker} — excluded: insufficient history"
            )
        elif metric.adv < liquidity_floor:
            filtered_low_liquidity.append(metric.ticker)
            logger.info(
                f"phase=select ticker={metric.ticker} adv={metric.adv:.0f} "
                f"floor={liquidity_floor:.0f} — excluded: below liquidity floor"
            )
        else:
            eligible.append(metric)

    eligible.sort(key=lambda metric: (-metric.vol, metric.ticker))

    selected_metrics = eligible[:target_count]
    selected = [metric.ticker for metric in selected_metrics]
    cutoff_vol = selected_metrics[-1].vol if selected_metrics else None

    return SelectionResult(
        selected=selected,
        filtered_low_history=filtered_low_history,
        filtered_low_liquidity=filtered_low_liquidity,
        cutoff_vol=cutoff_vol,
    )


def update_active_flags(
    existing_tickers: list[dict[str, Any]],
    selected_symbols: set[str],
) -> list[dict[str, Any]]:
    """
    Update the active flag on each ticker entry based on whether it appears in
    the selected set. Mutates entry dicts in place — no reconstruction.

    Index entries (sector == "Index") are always set to active=True regardless
    of whether they appear in selected_symbols. All other entries are set to
    active=True if and only if their symbol is in selected_symbols.

    Args:
        existing_tickers: List of ticker entry dicts from config/tickers.json.
            Each dict is mutated in place; all non-active fields are preserved verbatim.
        selected_symbols: Set of ticker symbols that should be marked active.

    Returns:
        The same list with active flags updated in place.
    """
    for entry in existing_tickers:
        if entry.get("sector") == "Index":
            entry["active"] = True
        else:
            entry["active"] = entry["symbol"] in selected_symbols
    return existing_tickers


def sort_tickers(tickers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Sort tickers so that non-Index stocks come first (alphabetically by symbol),
    followed by Index entries (alphabetically by symbol).

    This is a fresh implementation; do not import this function from
    scripts/merge_sp500_into_tickers.py to avoid a cross-script dependency.

    Args:
        tickers: List of ticker entry dicts, each with 'symbol' and 'sector' keys.

    Returns:
        New sorted list (does not mutate the input list).
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
    bytes are identical to the existing file content, skips the write entirely
    (bytes-equal short-circuit).

    This is a fresh implementation; do not import this function from
    scripts/merge_sp500_into_tickers.py to avoid a cross-script dependency.

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


# ---------------------------------------------------------------------------
# DB query helpers
# ---------------------------------------------------------------------------


def _fetch_ohlcv_bars(
    db_path: Path,
    candidate_symbols: list[str],
    lower_bound: str,
    as_of: str,
) -> dict[str, list[tuple[str, float, float]]]:
    """
    Fetch OHLCV bars from ohlcv_daily for a set of ticker symbols within a date window.

    Issues a single parameterised SQL query. When the candidate pool exceeds SQLite's
    999-parameter limit, the query is chunked automatically.

    Args:
        db_path: Path to the SQLite database file.
        candidate_symbols: List of ticker symbols to query (non-Index only).
        lower_bound: Start date string (YYYY-MM-DD), inclusive.
        as_of: End date string (YYYY-MM-DD), inclusive. Required so --as-of replay works.

    Returns:
        Dict mapping ticker symbol → list of (date, close, volume) tuples, unordered.
    """
    results: dict[str, list[tuple[str, float, float]]] = {sym: [] for sym in candidate_symbols}

    if not candidate_symbols:
        return results

    con = sqlite3.connect(str(db_path))
    try:
        # Read-only access — no PRAGMA needed; CLAUDE.md guarantees WAL globally.
        # SQLite parameter limit is 999 by default. Chunk to stay safe.
        chunk_size = 900
        for chunk_start in range(0, len(candidate_symbols), chunk_size):
            chunk = candidate_symbols[chunk_start : chunk_start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = f"""
                SELECT ticker, date, close, volume
                FROM ohlcv_daily
                WHERE date >= ?
                  AND date <= ?
                  AND ticker IN ({placeholders})
                ORDER BY ticker, date
            """
            params: list[Any] = [lower_bound, as_of] + list(chunk)
            for row in con.execute(sql, params):
                ticker_sym: str = row[0]
                date_str: str = row[1]
                close_val: float = float(row[2])
                volume_val: float = float(row[3])
                if ticker_sym in results:
                    results[ticker_sym].append((date_str, close_val, volume_val))
    finally:
        con.close()

    return results


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------


def _format_adv(adv: float) -> str:
    """
    Format an average dollar volume value as a human-readable string.

    Args:
        adv: Average dollar volume in USD.

    Returns:
        String like '$427M' or '$1.2B'.
    """
    if adv >= 1_000_000_000:
        return f"${adv / 1_000_000_000:.1f}B"
    return f"${adv / 1_000_000:.0f}M"


def _print_report(
    universe_cfg: UniverseConfig,
    as_of: str,
    candidate_count: int,
    index_count: int,
    result: SelectionResult,
    metrics_by_ticker: dict[str, TickerMetric],
    active_count_after: int,
    deactivated_count: int,
    dry_run: bool,
) -> None:
    """
    Print the structured universe selection report to stdout.

    Args:
        universe_cfg: Config used for the selection run.
        as_of: The as-of date string used.
        candidate_count: Total non-Index tickers considered.
        index_count: Number of Index entries that bypass the rule.
        result: SelectionResult from select_universe().
        metrics_by_ticker: Dict of ticker → TickerMetric for selected tickers.
        active_count_after: Total active count after flag updates (stocks + Index).
        deactivated_count: Number of tickers flipped from active to inactive.
        dry_run: Whether this is a dry-run (nothing was written).

    Returns:
        None
    """
    prefix = "[DRY RUN] " if dry_run else ""
    eligible_count = (
        candidate_count
        - len(result.filtered_low_history)
        - len(result.filtered_low_liquidity)
    )

    liquidity_filter_detail = ""
    if result.filtered_low_liquidity:
        detail_parts = []
        for sym in result.filtered_low_liquidity[:3]:
            metric = metrics_by_ticker.get(sym)
            if metric is not None:
                detail_parts.append(f"{sym} @ {_format_adv(metric.adv)}")
            else:
                detail_parts.append(sym)
        liquidity_filter_detail = f" ({', '.join(detail_parts)})"

    cutoff_str = f"{result.cutoff_vol:.1f}%" if result.cutoff_vol is not None else "N/A"

    print(f"\n{prefix}=== Universe selection ===")
    print(f"As of:                    {as_of}")
    print(f"Window:                   {universe_cfg.window_trading_days} trading days ({universe_cfg.window_calendar_days} calendar)")
    print(f"Min history:              {universe_cfg.min_history_returns} returns")
    print(f"Liquidity floor:          ${universe_cfg.liquidity_floor_usd / 1_000_000:.0f}M / day")
    print(f"Target count:             {universe_cfg.target_count}")
    print()
    print(f"Candidate pool:           {candidate_count} stocks ({index_count} Index entries bypass the rule)")
    print(f"Filtered (history):       {len(result.filtered_low_history)}")
    print(f"Filtered (liquidity):     {len(result.filtered_low_liquidity)}{liquidity_filter_detail}")
    print(f"Eligible:                 {eligible_count}")
    print(f"Selected:                 {len(result.selected)}")
    print(f"Cutoff vol:               {cutoff_str}")
    print()

    top_selected = result.selected[:10]
    if top_selected:
        print("Top 10 by vol:")
        for sym in top_selected:
            metric = metrics_by_ticker.get(sym)
            if metric is not None and metric.vol is not None:
                print(f"  {sym:<8} {metric.vol:>6.1f}%   ADV {_format_adv(metric.adv)}")
        print()

    bottom_selected = result.selected[-10:] if len(result.selected) > 10 else []
    if bottom_selected:
        print("Bottom 10 of selected:")
        for sym in bottom_selected:
            metric = metrics_by_ticker.get(sym)
            if metric is not None and metric.vol is not None:
                print(f"  {sym:<8} {metric.vol:>6.1f}%   ADV {_format_adv(metric.adv)}")
        print()

    print(f"Active count after write: {active_count_after} ({len(result.selected)} stocks + {index_count} Index entries)")
    print(f"Deactivated:              {deactivated_count}")
    if dry_run:
        print()
        print("[DRY RUN] No files were written.")
    print()


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------


def main(args: list[str] | None = None) -> int:
    """
    CLI entry point for selecting the volatile universe and updating tickers.json.

    Parses CLI arguments (with defaults from universe_selection.json), queries
    ohlcv_daily for realised vol and ADV, selects the top target_count tickers
    subject to the liquidity floor, updates active flags in tickers.json, and
    writes the result atomically.

    Args:
        args: Argument list for testing (passed to argparse). If None, reads
            from sys.argv.

    Returns:
        int: Exit code — 0 success/no-op, 2 unrecoverable DB error, 3 fewer
            than target_count candidates eligible (partial result still written).
    """
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        level=logging.INFO,
    )

    parser = argparse.ArgumentParser(
        description="Select the top-N stocks by realised vol and update active flags in tickers.json."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the selection report without writing any files.",
    )
    parser.add_argument(
        "--target",
        type=int,
        default=None,
        help="Override target_count from config.",
    )
    parser.add_argument(
        "--liquidity-floor",
        type=float,
        default=None,
        help="Override liquidity_floor_usd from config (in dollars).",
    )
    parser.add_argument(
        "--window-trading-days",
        type=int,
        default=None,
        help="Override window_trading_days from config.",
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=None,
        help="Override min_history_returns from config.",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="As-of date for the selection window (YYYY-MM-DD). Defaults to today UTC.",
    )
    parser.add_argument(
        "--config-path",
        default=str(_DEFAULT_CONFIG_PATH),
        help="Path to config/tickers.json.",
    )
    parser.add_argument(
        "--universe-config-path",
        default=str(_DEFAULT_UNIVERSE_CONFIG_PATH),
        help="Path to config/universe_selection.json.",
    )
    parser.add_argument(
        "--db-path",
        default=str(_DEFAULT_DB_PATH),
        help="Path to the SQLite database file.",
    )

    parsed = parser.parse_args(args)

    # --- Load universe config ---
    universe_config_path = Path(parsed.universe_config_path)
    try:
        universe_cfg = load_universe_config(universe_config_path)
    except Exception as exc:
        logger.error(f"phase=config — failed to load universe config: {exc!r}")
        return 2

    # Apply CLI overrides
    if parsed.target is not None:
        universe_cfg.target_count = parsed.target
    if parsed.liquidity_floor is not None:
        universe_cfg.liquidity_floor_usd = parsed.liquidity_floor
    if parsed.window_trading_days is not None:
        universe_cfg.window_trading_days = parsed.window_trading_days
    if parsed.min_history is not None:
        universe_cfg.min_history_returns = parsed.min_history

    # --- Resolve as_of ---
    if parsed.as_of is not None:
        as_of = parsed.as_of
    else:
        as_of = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    logger.info(f"phase=init as_of={as_of} target={universe_cfg.target_count}")

    # --- Load tickers config ---
    config_path = Path(parsed.config_path)
    try:
        tickers_config: dict[str, Any] = json.loads(
            config_path.read_text(encoding="utf-8")
        )
    except Exception as exc:
        logger.error(f"phase=load config_path={config_path} — failed to read: {exc!r}")
        return 2

    if "tickers" not in tickers_config:
        logger.error("phase=load — tickers.json missing 'tickers' key")
        return 2

    all_tickers: list[dict[str, Any]] = tickers_config["tickers"]

    # Separate candidates (non-Index) from Index entries
    candidate_entries = [entry for entry in all_tickers if entry.get("sector") != "Index"]
    index_entries = [entry for entry in all_tickers if entry.get("sector") == "Index"]
    candidate_symbols = [entry["symbol"] for entry in candidate_entries]

    logger.info(
        f"phase=load candidates={len(candidate_symbols)} index_entries={len(index_entries)}"
    )

    # --- Query ohlcv_daily ---
    db_path = Path(parsed.db_path)
    as_of_date = datetime.date.fromisoformat(as_of)
    lower_bound_date = as_of_date - datetime.timedelta(days=universe_cfg.window_calendar_days)
    lower_bound = lower_bound_date.isoformat()

    logger.info(
        f"phase=query lower_bound={lower_bound} as_of={as_of} "
        f"tickers={len(candidate_symbols)}"
    )

    try:
        bars_by_ticker = _fetch_ohlcv_bars(
            db_path=db_path,
            candidate_symbols=candidate_symbols,
            lower_bound=lower_bound,
            as_of=as_of,
        )
    except Exception as exc:
        logger.error(f"phase=query db_path={db_path} — DB query failed: {exc!r}")
        return 2

    # --- Compute metrics per ticker ---
    metrics: list[TickerMetric] = []
    for symbol in candidate_symbols:
        all_bars = bars_by_ticker.get(symbol, [])
        # Sort ascending by date (query already orders, but defensive sort is cheap)
        all_bars.sort(key=lambda bar: bar[0])

        # Take the last window_trading_days + 1 rows → N+1 closes → N log returns
        window_bars = all_bars[-(universe_cfg.window_trading_days + 1):]

        closes = [bar[1] for bar in window_bars]
        close_volume_pairs = [(bar[1], bar[2]) for bar in window_bars]

        vol = compute_realized_vol(closes, universe_cfg.min_history_returns)
        adv = compute_avg_dollar_volume(close_volume_pairs)

        metrics.append(TickerMetric(ticker=symbol, vol=vol, adv=adv))
        logger.info(
            f"phase=metrics ticker={symbol} "
            f"bars={len(window_bars)} "
            f"vol={'None' if vol is None else f'{vol:.1f}%'} "
            f"adv={_format_adv(adv)}"
        )

    # --- Select universe ---
    result = select_universe(
        metrics=metrics,
        target_count=universe_cfg.target_count,
        liquidity_floor=universe_cfg.liquidity_floor_usd,
    )

    logger.info(
        f"phase=select selected={len(result.selected)} "
        f"filtered_history={len(result.filtered_low_history)} "
        f"filtered_liquidity={len(result.filtered_low_liquidity)}"
    )

    # Build metrics lookup for the report
    metrics_by_ticker: dict[str, TickerMetric] = {metric.ticker: metric for metric in metrics}

    # --- Update active flags ---
    selected_set = set(result.selected)

    # Count deactivated: currently active non-Index tickers not in selected set
    previously_active_non_index = {
        entry["symbol"]
        for entry in candidate_entries
        if entry.get("active", True)
    }
    deactivated_count = len(previously_active_non_index - selected_set)

    update_active_flags(all_tickers, selected_set)
    sorted_tickers = sort_tickers(all_tickers)

    active_count_after = sum(1 for entry in sorted_tickers if entry.get("active", False))

    # --- Print report ---
    _print_report(
        universe_cfg=universe_cfg,
        as_of=as_of,
        candidate_count=len(candidate_symbols),
        index_count=len(index_entries),
        result=result,
        metrics_by_ticker=metrics_by_ticker,
        active_count_after=active_count_after,
        deactivated_count=deactivated_count,
        dry_run=parsed.dry_run,
    )

    # --- Determine exit code ---
    exit_code = 0
    if len(result.selected) < universe_cfg.target_count:
        logger.warning(
            f"phase=select — only {len(result.selected)} tickers eligible, "
            f"fewer than target {universe_cfg.target_count}"
        )
        exit_code = 3

    # --- Write ---
    if parsed.dry_run:
        logger.info("phase=write dry_run=True — skipping file write")
        return exit_code

    result_config = dict(tickers_config)
    result_config["tickers"] = sorted_tickers

    try:
        write_config(config_path, result_config)
    except Exception as exc:
        logger.error(f"phase=write config_path={config_path} — write failed: {exc!r}")
        return 2

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
