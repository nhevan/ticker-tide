"""
Tests for src/notifier/why_command.py — /why Telegram command backend.

Covers: load_why_payload, resolve_name_token, format_why_default,
format_why_all, format_why_drilldown, and dispatch_why.

All external API calls are mocked. All tests use tmp_path for DB files.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

import pytest

from src.notifier.why_command import (
    _NULL_DATA_SENTINEL,
    dispatch_why,
    format_why_all,
    format_why_default,
    format_why_drilldown,
    load_why_payload,
    resolve_name_token,
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

_SCORING_DATE = "2026-05-05"

_SAMPLE_CONFIGS = {
    "notifier": {
        "why_top_n": 5,
        "why_list_max_entries": 50,
    }
}


def _make_item(
    name: str,
    kind: str = "indicator",
    raw_value: float = 55.0,
    score: float = 60.0,
    category: str = "momentum",
    category_weight: float = 0.20,
    contribution: float = 12.0,
) -> dict:
    return {
        "name": name,
        "kind": kind,
        "raw_value": raw_value,
        "score": score,
        "category": category,
        "category_weight": category_weight,
        "contribution": contribution,
    }


def _make_payload(items: list[dict], version: int = 1) -> dict:
    return {"v": version, "items": items}


def _setup_scores_daily(
    conn: sqlite3.Connection,
    ticker: str,
    payload: Any,
    date_str: str = _SCORING_DATE,
) -> None:
    """Insert a minimal scores_daily row with the given key_signals_data payload."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS scores_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            signal TEXT,
            confidence REAL,
            final_score REAL,
            regime TEXT,
            key_signals_data TEXT,
            UNIQUE(ticker, date)
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO scores_daily (ticker, date, signal, confidence, final_score, regime, key_signals_data) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            ticker,
            date_str,
            "BUY",
            72.0,
            18.5,
            "trending",
            json.dumps(payload) if payload is not None else None,
        ),
    )
    conn.commit()


def _setup_indicator_profiles(
    conn: sqlite3.Connection,
    ticker: str,
    indicator: str,
    p5: float = 20.0,
    p20: float = 30.0,
    p50: float = 50.0,
    p80: float = 70.0,
    p95: float = 85.0,
    mean: float = 50.0,
    std: float = 15.0,
) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS indicator_profiles (
            ticker TEXT NOT NULL,
            indicator TEXT NOT NULL,
            p5 REAL,
            p20 REAL,
            p50 REAL,
            p80 REAL,
            p95 REAL,
            mean REAL,
            std REAL,
            window_start TEXT,
            window_end TEXT,
            computed_at TEXT,
            UNIQUE(ticker, indicator)
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO indicator_profiles "
        "(ticker, indicator, p5, p20, p50, p80, p95, mean, std) VALUES (?,?,?,?,?,?,?,?,?)",
        (ticker, indicator, p5, p20, p50, p80, p95, mean, std),
    )
    conn.commit()


@pytest.fixture
def db(tmp_path):
    """Return an in-memory-style SQLite connection backed by a tmp file."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def _thirty_items() -> list[dict]:
    names = [
        "rsi_14", "macd_histogram", "adx", "ema_alignment", "stoch_k",
        "cci_20", "williams_r", "obv", "cmf_20", "ad_line",
        "bb_pctb", "atr_14", "crossover_ema_9_21", "crossover_ema_21_50",
        "crossover_macd_signal", "divergence_rsi", "divergence_macd",
        "divergence_stoch", "divergence_obv", "candlestick_pattern_score",
        "structural_pattern_score", "gap_score", "fibonacci_score",
        "macd_line", "rsi_14_w", "stoch_k_w", "cci_20_w",
        "obv_w", "bb_pctb_w", "atr_14_w",
    ]
    items = []
    for i, name in enumerate(names):
        kind = "pattern" if name.startswith(("crossover", "divergence", "candlestick", "structural", "gap", "fibonacci")) else "indicator"
        items.append(_make_item(name, kind=kind, contribution=float(30 - i)))
    return items


# ---------------------------------------------------------------------------
# load_why_payload
# ---------------------------------------------------------------------------

class TestLoadWhyPayload:
    def test_no_scores_row_returns_none(self, db: sqlite3.Connection) -> None:
        """When no scores_daily row exists for the ticker, return None."""
        db.execute(
            """CREATE TABLE IF NOT EXISTS scores_daily (
                ticker TEXT NOT NULL, date TEXT NOT NULL,
                signal TEXT, confidence REAL, final_score REAL, regime TEXT,
                key_signals_data TEXT, UNIQUE(ticker, date)
            )"""
        )
        db.commit()
        result = load_why_payload("AAPL", db)
        assert result is None

    def test_null_key_signals_data_returns_sentinel(self, db: sqlite3.Connection) -> None:
        """When key_signals_data is NULL, return the null-data sentinel."""
        _setup_scores_daily(db, "AAPL", None)
        result = load_why_payload("AAPL", db)
        assert result == _NULL_DATA_SENTINEL

    def test_malformed_json_returns_sentinel_and_logs_warning(
        self, db: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Malformed JSON → null-data sentinel + WARNING log."""
        db.execute(
            """CREATE TABLE IF NOT EXISTS scores_daily (
                ticker TEXT NOT NULL, date TEXT NOT NULL,
                signal TEXT, confidence REAL, final_score REAL, regime TEXT,
                key_signals_data TEXT, UNIQUE(ticker, date)
            )"""
        )
        db.execute(
            "INSERT INTO scores_daily (ticker, date, key_signals_data) VALUES (?, ?, ?)",
            ("AAPL", _SCORING_DATE, "{bad json}"),
        )
        db.commit()
        with caplog.at_level(logging.WARNING):
            result = load_why_payload("AAPL", db)
        assert result == _NULL_DATA_SENTINEL
        assert any("WARNING" in r.levelname or r.levelno >= logging.WARNING for r in caplog.records)

    def test_version_mismatch_returns_sentinel_and_logs_warning(
        self, db: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Payload with v != 1 → null-data sentinel + WARNING log."""
        bad_payload = {"v": 99, "items": []}
        _setup_scores_daily(db, "AAPL", bad_payload)
        with caplog.at_level(logging.WARNING):
            result = load_why_payload("AAPL", db)
        assert result == _NULL_DATA_SENTINEL
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_missing_version_key_returns_sentinel(
        self, db: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Payload with no 'v' key → null-data sentinel + WARNING log."""
        bad_payload = {"items": []}
        _setup_scores_daily(db, "AAPL", bad_payload)
        with caplog.at_level(logging.WARNING):
            result = load_why_payload("AAPL", db)
        assert result == _NULL_DATA_SENTINEL

    def test_valid_payload_returned(self, db: sqlite3.Connection) -> None:
        """Valid payload is returned as-is."""
        items = [_make_item("rsi_14")]
        payload = _make_payload(items)
        _setup_scores_daily(db, "AAPL", payload)
        result = load_why_payload("AAPL", db)
        assert result is not None
        assert result["v"] == 1
        assert len(result["items"]) == 1


# ---------------------------------------------------------------------------
# format_why_default
# ---------------------------------------------------------------------------

class TestFormatWhyDefault:
    def test_30_items_top5_shows_5_entries_and_footer(self) -> None:
        """30-item payload with top_n=5 → 5 entries + truncation footer."""
        items = _thirty_items()
        payload = _make_payload(items)
        output = format_why_default(payload, top_n=5)
        assert isinstance(output, str)
        # There should be a footer mentioning remaining items
        assert "+25 more" in output or "25 more" in output
        # Count entry lines — each entry should appear once
        entry_count = sum(1 for item in items[:5] if item["name"] in output)
        assert entry_count == 5

    def test_3_items_top5_no_footer(self) -> None:
        """3-item payload with top_n=5 → 3 entries, no truncation footer."""
        items = [_make_item(n) for n in ["rsi_14", "adx", "ema_alignment"]]
        payload = _make_payload(items)
        output = format_why_default(payload, top_n=5)
        assert "more" not in output
        for item in items:
            assert item["name"] in output

    def test_pattern_top_item_renders(self) -> None:
        """Pattern as top item renders with pattern-specific labelling."""
        items = [
            _make_item("crossover_macd_signal", kind="pattern", contribution=20.0),
            _make_item("rsi_14", contribution=10.0),
        ]
        payload = _make_payload(items)
        output = format_why_default(payload, top_n=5)
        assert "crossover_macd_signal" in output
        # Pattern kind should be indicated
        assert "pattern" in output.lower() or "Pattern" in output

    def test_output_is_string(self) -> None:
        """format_why_default always returns a string."""
        payload = _make_payload([_make_item("rsi_14")])
        result = format_why_default(payload, top_n=5)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_pattern_raw_value_none_does_not_crash(self) -> None:
        """Patterns have raw_value=None per contract — must not crash :.2f formatter."""
        items = [
            _make_item("crossover_macd_signal", kind="pattern", raw_value=None),
            _make_item("rsi_14", raw_value=55.0),
        ]
        payload = _make_payload(items)
        output = format_why_default(payload, top_n=5)
        assert "crossover_macd_signal" in output
        # Pattern entry must NOT contain a 'raw=' or 'raw:' line
        pattern_block = output.split("crossover_macd_signal")[1].split("rsi_14")[0]
        assert "raw" not in pattern_block.lower()

    def test_default_entry_shows_full_sibling_math(self) -> None:
        """Each top-N entry must walk through siblings → magnitude → share → contribution."""
        items = [
            _make_item("rsi_14", score=80.0, category="momentum",
                       category_weight=0.30, contribution=14.4),
            _make_item("macd_histogram", score=65.0, category="momentum",
                       category_weight=0.30, contribution=9.5),
            _make_item("stoch_k", score=40.0, category="momentum",
                       category_weight=0.30, contribution=3.6),
        ]
        payload = _make_payload(items)
        output = format_why_default(payload, top_n=5)
        # Sibling list
        assert "Momentum siblings:" in output
        assert "rsi_14" in output and "macd_histogram" in output and "stoch_k" in output
        # Magnitude sum line
        assert "Total magnitude:" in output
        # Share calculation explicit
        assert "Share:" in output and "÷" in output
        # Regime weight
        assert "Regime weight (momentum):" in output
        # Expansion factor is shown explicitly so the math reconciles
        assert "Expansion factor:" in output
        # Final contribution multiplication uses = (not ≈) since expansion is shown
        assert "Contribution:" in output


# ---------------------------------------------------------------------------
# format_why_all
# ---------------------------------------------------------------------------

class TestFormatWhyAll:
    def test_30_items_max50_all_shown_no_footer(self) -> None:
        """30-item payload with max_entries=50 → all 30 shown, no overflow footer."""
        items = _thirty_items()
        payload = _make_payload(items)
        output = format_why_all(payload, max_entries=50)
        assert isinstance(output, str)
        assert "Showing 30 of 30" not in output or "of 30" not in output
        # All items appear
        for item in items:
            assert item["name"] in output

    def test_50_items_max50_all_shown_no_footer(self) -> None:
        """Exactly 50-item payload with max_entries=50 → all 50, no overflow footer."""
        names = [f"ind_{i:02d}" for i in range(50)]
        items = [_make_item(n, contribution=float(50 - i)) for i, n in enumerate(names)]
        payload = _make_payload(items)
        output = format_why_all(payload, max_entries=50)
        assert "Showing 50 of 50" not in output or "use `/why" not in output
        for item in items:
            assert item["name"] in output

    def test_60_items_max50_shows_50_with_footer(self) -> None:
        """60-item payload with max_entries=50 → 50 shown + overflow footer."""
        names = [f"ind_{i:02d}" for i in range(60)]
        items = [_make_item(n, contribution=float(60 - i)) for i, n in enumerate(names)]
        payload = _make_payload(items)
        output = format_why_all(payload, max_entries=50)
        # Footer should mention 60 total and drill-down hint
        assert "60" in output
        assert "50" in output
        # Only 50 distinct entries shown
        shown = sum(1 for item in items[:50] if item["name"] in output)
        hidden = sum(1 for item in items[50:] if item["name"] in output)
        assert shown == 50
        assert hidden == 0

    def test_output_within_telegram_limit(self) -> None:
        """50-entry output stays under 4096 characters (Telegram limit)."""
        names = [f"ind_{i:02d}" for i in range(50)]
        items = [_make_item(n, contribution=float(50 - i)) for i, n in enumerate(names)]
        payload = _make_payload(items)
        output = format_why_all(payload, max_entries=50)
        assert len(output) <= 4096


# ---------------------------------------------------------------------------
# format_why_drilldown
# ---------------------------------------------------------------------------

class TestFormatWhyDrilldown:
    def _make_drilldown_payload(self, name: str, kind: str = "indicator") -> dict:
        items = [
            _make_item(name, kind=kind, raw_value=30.0, score=75.0, contribution=15.0),
        ]
        return {
            "v": 1,
            "items": items,
            "ticker": "AAPL",
            "date": _SCORING_DATE,
            "signal": "BUY",
            "confidence": 72.0,
            "final_score": 18.5,
            "regime": "trending",
        }

    def test_profile_free_indicator_renders_fixed_ladder(self, db: sqlite3.Connection) -> None:
        """Profile-free indicator (adx) → renders FIXED_LADDER bands."""
        payload = self._make_drilldown_payload("adx")
        output = format_why_drilldown(payload, "adx", db, "AAPL")
        assert "adx" in output.lower()
        # FIXED_LADDER bands for adx
        assert "trend" in output.lower()

    def test_profile_driven_with_profile_row(self, db: sqlite3.Connection) -> None:
        """Profile-driven indicator with a profile row → renders percentile ladder."""
        _setup_indicator_profiles(db, "AAPL", "rsi_14")
        payload = self._make_drilldown_payload("rsi_14")
        output = format_why_drilldown(payload, "rsi_14", db, "AAPL")
        assert "rsi_14" in output.lower()
        # Should contain percentile info
        assert any(p in output for p in ["p5", "p20", "p50", "p80", "p95", "20.0", "30.0", "50.0"])

    def test_profile_driven_without_profile_row(self, db: sqlite3.Connection) -> None:
        """Profile-driven indicator with no profile row → graceful message, no crash."""
        db.execute(
            """CREATE TABLE IF NOT EXISTS indicator_profiles (
                ticker TEXT, indicator TEXT, p5 REAL, p20 REAL, p50 REAL,
                p80 REAL, p95 REAL, mean REAL, std REAL,
                window_start TEXT, window_end TEXT, computed_at TEXT,
                UNIQUE(ticker, indicator)
            )"""
        )
        db.commit()
        payload = self._make_drilldown_payload("rsi_14")
        output = format_why_drilldown(payload, "rsi_14", db, "AAPL")
        assert "profile not built" in output.lower() or "profile" in output.lower()

    def test_pattern_with_description(self, db: sqlite3.Connection) -> None:
        """Pattern with entry in PATTERN_RULE_DESCRIPTIONS → renders description."""
        payload = self._make_drilldown_payload("crossover_macd_signal", kind="pattern")
        output = format_why_drilldown(payload, "crossover_macd_signal", db, "AAPL")
        assert "crossover_macd_signal" in output.lower()
        # Should include description text
        assert "macd" in output.lower() and "signal" in output.lower()

    def test_pattern_without_description(self, db: sqlite3.Connection) -> None:
        """Pattern not in PATTERN_RULE_DESCRIPTIONS → fallback message."""
        payload = self._make_drilldown_payload("unknown_pattern_xyz", kind="pattern")
        output = format_why_drilldown(payload, "unknown_pattern_xyz", db, "AAPL")
        assert "rule not available" in output.lower() or "see source" in output.lower()

    def test_drilldown_shows_math_chain(self, db: sqlite3.Connection) -> None:
        """Drill-down output walks through the full sibling-share math."""
        _setup_indicator_profiles(db, "AAPL", "rsi_14")
        payload = self._make_drilldown_payload("rsi_14")
        output = format_why_drilldown(payload, "rsi_14", db, "AAPL")
        # Score and contribution numbers appear
        assert "75" in output or "75.0" in output  # score
        assert "15" in output or "15.0" in output  # contribution
        # Full sibling-share math is rendered, not just a thin "score → contrib"
        assert "siblings:" in output.lower()
        assert "Total magnitude:" in output
        assert "Share:" in output and "÷" in output
        assert "Regime weight" in output

    def test_drilldown_shows_header_info(self, db: sqlite3.Connection) -> None:
        """Drill-down output includes ticker, date, and regime in header."""
        _setup_indicator_profiles(db, "AAPL", "rsi_14")
        payload = self._make_drilldown_payload("rsi_14")
        output = format_why_drilldown(payload, "rsi_14", db, "AAPL")
        assert "AAPL" in output
        assert _SCORING_DATE in output
        assert "trending" in output.lower()


# ---------------------------------------------------------------------------
# resolve_name_token
# ---------------------------------------------------------------------------

class TestResolveNameToken:
    def _payload_with_names(self, names: list[str]) -> dict:
        items = [_make_item(n) for n in names]
        return _make_payload(items)

    def test_ambiguous_returns_ambiguous_dict(self) -> None:
        """Token 'ema' matching multiple keys → {'ambiguous': [...matches]}."""
        payload = self._payload_with_names([
            "ema_alignment", "crossover_ema_9_21", "crossover_ema_21_50"
        ])
        result = resolve_name_token("ema", payload)
        assert "ambiguous" in result
        assert len(result["ambiguous"]) >= 2
        assert all("ema" in m for m in result["ambiguous"])

    def test_unknown_returns_unknown_dict(self) -> None:
        """Token 'xyz' matching nothing → {'unknown': True}."""
        payload = self._payload_with_names(["rsi_14", "adx", "macd_histogram"])
        result = resolve_name_token("xyz", payload)
        assert result == {"unknown": True}

    def test_case_insensitive_unique_match(self) -> None:
        """Token 'RSI' matches 'rsi_14' uniquely → {'match': 'rsi_14'}."""
        payload = self._payload_with_names(["rsi_14", "adx", "macd_histogram"])
        result = resolve_name_token("RSI", payload)
        assert result == {"match": "rsi_14"}

    def test_exact_match(self) -> None:
        """Exact token 'adx' → {'match': 'adx'}."""
        payload = self._payload_with_names(["adx", "rsi_14"])
        result = resolve_name_token("adx", payload)
        assert result == {"match": "adx"}

    def test_substring_match_unique(self) -> None:
        """Token 'macd_hist' uniquely matches 'macd_histogram' → match."""
        payload = self._payload_with_names(["macd_histogram", "rsi_14", "adx"])
        result = resolve_name_token("macd_hist", payload)
        assert result == {"match": "macd_histogram"}


# ---------------------------------------------------------------------------
# dispatch_why
# ---------------------------------------------------------------------------

class TestDispatchWhy:
    def _payload_aapl(self) -> dict:
        items = _thirty_items()
        return _make_payload(items)

    def _insert_valid_payload(self, db: sqlite3.Connection) -> None:
        _setup_scores_daily(db, "AAPL", self._payload_aapl())

    def test_empty_args_calls_default(self, db: sqlite3.Connection) -> None:
        """No args → default mode output (top_n from config)."""
        self._insert_valid_payload(db)
        result = dispatch_why("AAPL", [], db, _SAMPLE_CONFIGS)
        assert isinstance(result, str)
        assert len(result) > 0
        # Default mode has a 'more' footer since 30 > 5
        assert "more" in result

    def test_all_lowercase_calls_all_mode(self, db: sqlite3.Connection) -> None:
        """['all'] → all mode output."""
        self._insert_valid_payload(db)
        result = dispatch_why("AAPL", ["all"], db, _SAMPLE_CONFIGS)
        assert isinstance(result, str)
        # All mode uses code block formatting
        assert "```" in result or len(result) > 100

    def test_all_uppercase_routes_to_all_mode(self, db: sqlite3.Connection) -> None:
        """['ALL'] → also routes to all mode (case-insensitive keyword)."""
        self._insert_valid_payload(db)
        result = dispatch_why("AAPL", ["ALL"], db, _SAMPLE_CONFIGS)
        assert isinstance(result, str)
        # Should not be a usage error
        assert "usage" not in result.lower() and "error" not in result.lower()

    def test_name_arg_calls_drilldown(self, db: sqlite3.Connection) -> None:
        """['rsi'] → name resolution → drill-down mode."""
        _setup_scores_daily(db, "AAPL", self._payload_aapl())
        _setup_indicator_profiles(db, "AAPL", "rsi_14")
        result = dispatch_why("AAPL", ["rsi"], db, _SAMPLE_CONFIGS)
        assert isinstance(result, str)
        assert "rsi" in result.lower()

    def test_two_args_returns_usage_error(self, db: sqlite3.Connection) -> None:
        """2+ args → usage error string."""
        self._insert_valid_payload(db)
        result = dispatch_why("AAPL", ["rsi", "extra"], db, _SAMPLE_CONFIGS)
        assert "usage" in result.lower() or "/why" in result

    def test_all_keyword_checked_before_name_resolution(self, db: sqlite3.Connection) -> None:
        """'all' keyword routing happens before name resolution (no ambiguity crash)."""
        # Even if there were items starting with 'all_', the keyword should still route
        items = [_make_item("all_signals"), _make_item("rsi_14")]
        payload = _make_payload(items)
        _setup_scores_daily(db, "AAPL", payload)
        # 'all' should route to all-mode, not name resolution
        result = dispatch_why("AAPL", ["all"], db, _SAMPLE_CONFIGS)
        # All mode renders a list — should show items
        assert isinstance(result, str)
        assert "all_signals" in result

    def test_unknown_name_returns_error_message(self, db: sqlite3.Connection) -> None:
        """Unknown name token → error message string."""
        self._insert_valid_payload(db)
        result = dispatch_why("AAPL", ["xyz_unknown_99"], db, _SAMPLE_CONFIGS)
        assert "unknown" in result.lower() or "not found" in result.lower() or "no signal" in result.lower()

    def test_ambiguous_name_returns_error_message(self, db: sqlite3.Connection) -> None:
        """Ambiguous name token → message listing matches."""
        self._insert_valid_payload(db)  # has ema_alignment + crossover_ema_* → ambiguous 'ema'
        result = dispatch_why("AAPL", ["ema"], db, _SAMPLE_CONFIGS)
        assert "ema" in result.lower()
        assert "ambiguous" in result.lower() or "multiple" in result.lower() or "did you mean" in result.lower()
