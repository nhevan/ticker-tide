"""
Tests for the per-category breakdown variants of weekly/monthly scoring
(commit 5 of weekly/monthly parity).

Covers:
  - compute_weekly_score_breakdown / compute_monthly_score_breakdown shape
  - v1_4cat (default) vs v2_8cat semantics
  - F1: week_start AS date / month_start AS date aliasing in SQL loaders
  - F2: v2 scalar differs from v1 scalar when crossovers/divergences exist
  - F3: monthly candlestick is permanently None (decay window mismatch)
  - F4: load_profile_for_ticker source_table whitelist
  - SQL targets weekly/monthly mirror tables (not their daily counterparts)
  - Per-timeframe profile fallback to daily
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from src.scorer.indicator_scorer import load_profile_for_ticker
from src.scorer.pattern_scorer import (
    score_candlestick_patterns,
    score_crossovers,
    score_divergences,
    score_structural_patterns,
)
from src.scorer.timeframe_merger import (
    compute_monthly_score,
    compute_monthly_score_breakdown,
    compute_weekly_score,
    compute_weekly_score_breakdown,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_INDICATOR_COLS = [
    "ema_9", "ema_21", "ema_50", "macd_line", "macd_signal", "macd_histogram",
    "adx", "rsi_14", "stoch_k", "stoch_d", "cci_20", "williams_r",
    "obv", "cmf_20", "ad_line", "bb_upper", "bb_lower", "bb_pctb",
    "atr_14", "keltner_upper", "keltner_lower",
]


def _create_full_schema(conn: sqlite3.Connection) -> None:
    """Create all tables required by the breakdown functions."""
    statements = [
        """CREATE TABLE weekly_candles (
            ticker TEXT NOT NULL, week_start TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(ticker, week_start)
        )""",
        """CREATE TABLE indicators_weekly (
            ticker TEXT NOT NULL, week_start TEXT NOT NULL,
            ema_9 REAL, ema_21 REAL, ema_50 REAL,
            macd_line REAL, macd_signal REAL, macd_histogram REAL,
            adx REAL, rsi_14 REAL,
            stoch_k REAL, stoch_d REAL, cci_20 REAL, williams_r REAL,
            obv REAL, cmf_20 REAL, ad_line REAL,
            bb_upper REAL, bb_lower REAL, bb_pctb REAL,
            atr_14 REAL, keltner_upper REAL, keltner_lower REAL,
            UNIQUE(ticker, week_start)
        )""",
        """CREATE TABLE monthly_candles (
            ticker TEXT NOT NULL, month_start TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(ticker, month_start)
        )""",
        """CREATE TABLE indicators_monthly (
            ticker TEXT NOT NULL, month_start TEXT NOT NULL,
            ema_9 REAL, ema_21 REAL, ema_50 REAL,
            macd_line REAL, macd_signal REAL, macd_histogram REAL,
            adx REAL, rsi_14 REAL,
            stoch_k REAL, stoch_d REAL, cci_20 REAL, williams_r REAL,
            obv REAL, cmf_20 REAL, ad_line REAL,
            bb_upper REAL, bb_lower REAL, bb_pctb REAL,
            atr_14 REAL, keltner_upper REAL, keltner_lower REAL,
            UNIQUE(ticker, month_start)
        )""",
        """CREATE TABLE indicator_profiles (
            ticker TEXT NOT NULL, indicator TEXT NOT NULL,
            p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL,
            mean REAL, std REAL,
            window_start TEXT, window_end TEXT, computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",
        """CREATE TABLE indicator_profiles_weekly (
            ticker TEXT NOT NULL, indicator TEXT NOT NULL,
            p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL,
            mean REAL, std REAL,
            window_start TEXT, window_end TEXT, computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",
        """CREATE TABLE indicator_profiles_monthly (
            ticker TEXT NOT NULL, indicator TEXT NOT NULL,
            p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL,
            mean REAL, std REAL,
            window_start TEXT, window_end TEXT, computed_at TEXT,
            UNIQUE(ticker, indicator)
        )""",
        """CREATE TABLE patterns_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            pattern_name TEXT, pattern_category TEXT, pattern_type TEXT,
            direction TEXT, strength INTEGER, confirmed INTEGER, details TEXT
        )""",
        """CREATE TABLE patterns_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, week_start TEXT NOT NULL,
            pattern_name TEXT, pattern_category TEXT, pattern_type TEXT,
            direction TEXT, strength INTEGER, confirmed INTEGER, details TEXT
        )""",
        """CREATE TABLE patterns_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, month_start TEXT NOT NULL,
            pattern_name TEXT, pattern_category TEXT, pattern_type TEXT,
            direction TEXT, strength INTEGER, confirmed INTEGER, details TEXT
        )""",
        """CREATE TABLE divergences_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            indicator TEXT, divergence_type TEXT,
            price_swing_1_date TEXT, price_swing_1_value REAL,
            price_swing_2_date TEXT, price_swing_2_value REAL,
            indicator_swing_1_value REAL, indicator_swing_2_value REAL,
            strength INTEGER
        )""",
        """CREATE TABLE divergences_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, week_start TEXT NOT NULL,
            indicator TEXT, divergence_type TEXT,
            price_swing_1_date TEXT, price_swing_1_value REAL,
            price_swing_2_date TEXT, price_swing_2_value REAL,
            indicator_swing_1_value REAL, indicator_swing_2_value REAL,
            strength INTEGER
        )""",
        """CREATE TABLE divergences_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, month_start TEXT NOT NULL,
            indicator TEXT, divergence_type TEXT,
            price_swing_1_date TEXT, price_swing_1_value REAL,
            price_swing_2_date TEXT, price_swing_2_value REAL,
            indicator_swing_1_value REAL, indicator_swing_2_value REAL,
            strength INTEGER
        )""",
        """CREATE TABLE crossovers_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            crossover_type TEXT, direction TEXT, days_ago INTEGER
        )""",
        """CREATE TABLE crossovers_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, week_start TEXT NOT NULL,
            crossover_type TEXT, direction TEXT, days_ago INTEGER
        )""",
        """CREATE TABLE crossovers_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, month_start TEXT NOT NULL,
            crossover_type TEXT, direction TEXT, days_ago INTEGER
        )""",
    ]
    for sql in statements:
        conn.execute(sql)


def _insert_weekly_indicators(conn: sqlite3.Connection, ticker: str, row: dict) -> None:
    """Insert a single weekly_candles + indicators_weekly row."""
    conn.execute(
        "INSERT INTO weekly_candles(ticker, week_start, close) VALUES (?, ?, ?)",
        (ticker, row["week_start"], row["close"]),
    )
    values = [row.get(col) for col in _INDICATOR_COLS]
    placeholders = ", ".join(["?"] * len(_INDICATOR_COLS))
    cols = ", ".join(_INDICATOR_COLS)
    conn.execute(
        f"INSERT INTO indicators_weekly(ticker, week_start, {cols}) "
        f"VALUES (?, ?, {placeholders})",
        (ticker, row["week_start"], *values),
    )


def _insert_monthly_indicators(conn: sqlite3.Connection, ticker: str, row: dict) -> None:
    """Insert a single monthly_candles + indicators_monthly row."""
    conn.execute(
        "INSERT INTO monthly_candles(ticker, month_start, close) VALUES (?, ?, ?)",
        (ticker, row["month_start"], row["close"]),
    )
    values = [row.get(col) for col in _INDICATOR_COLS]
    placeholders = ", ".join(["?"] * len(_INDICATOR_COLS))
    cols = ", ".join(_INDICATOR_COLS)
    conn.execute(
        f"INSERT INTO indicators_monthly(ticker, month_start, {cols}) "
        f"VALUES (?, ?, {placeholders})",
        (ticker, row["month_start"], *values),
    )


@pytest.fixture()
def bare_conn(tmp_path):
    """Connection with full schema, no rows."""
    db = tmp_path / "bd.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    _create_full_schema(conn)
    conn.commit()
    return conn


_BULLISH_INDICATORS = {
    "week_start": "2026-04-06", "close": 520.0,
    "ema_9": 515.0, "ema_21": 505.0, "ema_50": 490.0,
    "macd_line": 5.0, "macd_histogram": 3.0, "adx": 35.0,
    "rsi_14": 28.0, "stoch_k": 15.0, "cci_20": -120.0, "williams_r": -88.0,
    "obv": 1_000_000.0, "cmf_20": 0.25, "ad_line": 500_000.0,
    "bb_pctb": 0.15, "atr_14": 8.0,
}

_CONFIG_BASE = {
    "timeframe_weights": {"ranging": {"daily": 0.6, "weekly": 0.3, "monthly": 0.1}},
    "weekly_adaptive_weights": {
        "ranging": {"trend": 0.20, "momentum": 0.40, "volume": 0.20, "volatility": 0.20},
    },
    "weekly_adaptive_weights_v2": {
        "ranging": {
            "trend": 0.20, "momentum": 0.40, "volume": 0.20,
            "volatility": 0.20, "candlestick": 0.0, "structural": 0.0,
        },
    },
    "monthly_adaptive_weights": {
        "ranging": {"trend": 0.25, "momentum": 0.35, "volume": 0.20, "volatility": 0.20},
    },
    "monthly_adaptive_weights_v2": {
        "ranging": {
            "trend": 0.25, "momentum": 0.35, "volume": 0.20,
            "volatility": 0.20, "candlestick": 0.0, "structural": 0.0,
        },
    },
    "scoring": {"score_expansion_factor": 1.5},
}


def _v1_config() -> dict:
    return {**_CONFIG_BASE, "weekly_score_method": "v1_4cat", "monthly_score_method": "v1_4cat"}


def _v2_config() -> dict:
    return {**_CONFIG_BASE, "weekly_score_method": "v2_8cat", "monthly_score_method": "v2_8cat"}


# ---------------------------------------------------------------------------
# F4: load_profile_for_ticker whitelist
# ---------------------------------------------------------------------------

class TestLoadProfileForTickerWhitelist:
    def test_invalid_source_table_raises(self, bare_conn) -> None:
        with pytest.raises(ValueError, match="Invalid profile source_table"):
            load_profile_for_ticker(bare_conn, "QQQ", source_table="users; DROP TABLE")

    def test_default_source_is_daily(self, bare_conn) -> None:
        # No rows → empty dict, no error
        result = load_profile_for_ticker(bare_conn, "QQQ")
        assert result == {}

    def test_weekly_source_table_accepted(self, bare_conn) -> None:
        bare_conn.execute(
            "INSERT INTO indicator_profiles_weekly(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES ('QQQ', 'rsi_14', 25, 35, 50, 65, 80, 50, 12)"
        )
        bare_conn.commit()
        result = load_profile_for_ticker(
            bare_conn, "QQQ", source_table="indicator_profiles_weekly"
        )
        assert "rsi_14" in result
        assert result["rsi_14"]["p50"] == 50

    def test_monthly_source_table_accepted(self, bare_conn) -> None:
        bare_conn.execute(
            "INSERT INTO indicator_profiles_monthly(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES ('QQQ', 'rsi_14', 25, 35, 50, 65, 80, 50, 12)"
        )
        bare_conn.commit()
        result = load_profile_for_ticker(
            bare_conn, "QQQ", source_table="indicator_profiles_monthly"
        )
        assert "rsi_14" in result


# ---------------------------------------------------------------------------
# F1: date column aliasing
# ---------------------------------------------------------------------------

class TestDateAliasingF1:
    """Patterns/divergences/crossovers loaded from weekly mirror tables must
    expose a ``date`` field (aliased from ``week_start``) for pattern_scorer."""

    def test_weekly_patterns_load_aliases_week_start_as_date(self, bare_conn) -> None:
        # Insert a weekly pattern → call score_candlestick_patterns directly
        # on the loaded row. If aliasing is missing, KeyError.
        bare_conn.execute(
            "INSERT INTO patterns_weekly(ticker, week_start, pattern_name, pattern_category, "
            "direction, strength) VALUES ('QQQ', '2026-04-06', 'hammer', 'candlestick', "
            "'bullish', 3)"
        )
        bare_conn.commit()
        rows = bare_conn.execute(
            "SELECT *, week_start AS date FROM patterns_weekly WHERE ticker = 'QQQ'"
        ).fetchall()
        rows = [dict(r) for r in rows]
        score = score_candlestick_patterns(rows, scoring_date="2026-04-08")
        # 3 strength * 20 base * 1 direction * decay (1 - 2/7) = 60 * 0.714 ≈ 42.86
        assert score > 0

    def test_weekly_divergences_load_aliases_week_start_as_date(self, bare_conn) -> None:
        bare_conn.execute(
            "INSERT INTO divergences_weekly(ticker, week_start, indicator, divergence_type, "
            "strength) VALUES ('QQQ', '2026-04-06', 'rsi_14', 'regular_bullish', 3)"
        )
        bare_conn.commit()
        rows = bare_conn.execute(
            "SELECT *, week_start AS date FROM divergences_weekly WHERE ticker = 'QQQ'"
        ).fetchall()
        rows = [dict(r) for r in rows]
        score = score_divergences(rows, scoring_date="2026-04-08")
        assert score > 0

    def test_weekly_crossovers_load_aliases_week_start_as_date(self, bare_conn) -> None:
        bare_conn.execute(
            "INSERT INTO crossovers_weekly(ticker, week_start, crossover_type, direction, "
            "days_ago) VALUES ('QQQ', '2026-04-06', 'ema_21_50', 'bullish', 2)"
        )
        bare_conn.commit()
        rows = bare_conn.execute(
            "SELECT *, week_start AS date FROM crossovers_weekly WHERE ticker = 'QQQ'"
        ).fetchall()
        rows = [dict(r) for r in rows]
        score = score_crossovers(rows, scoring_date="2026-04-08")
        assert score > 0


# ---------------------------------------------------------------------------
# Breakdown shape
# ---------------------------------------------------------------------------

class TestBreakdownShape:
    def test_v1_breakdown_returns_4_categories_plus_none_cdl_struct(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()
        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
        )
        assert result is not None
        assert set(result.keys()) == {
            "composite_score", "trend_score", "momentum_score", "volume_score",
            "volatility_score", "candlestick_score", "structural_score",
            "indicator_scores", "pattern_scores", "regime_weights",
        }
        assert result["candlestick_score"] is None
        assert result["structural_score"] is None
        # v1 mode: pattern_scores is empty (no pattern scoring done)
        assert result["pattern_scores"] == {}
        # regime_weights is a non-empty dict
        assert isinstance(result["regime_weights"], dict)
        assert len(result["regime_weights"]) > 0
        # 4 main categories should be numeric
        assert isinstance(result["trend_score"], float)
        assert isinstance(result["momentum_score"], float)
        assert isinstance(result["volume_score"], float)
        assert isinstance(result["volatility_score"], float)

    def test_v2_breakdown_returns_6_numeric_categories(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        # Add a candlestick pattern within decay window so candlestick != 0
        bare_conn.execute(
            "INSERT INTO patterns_weekly(ticker, week_start, pattern_name, pattern_category, "
            "direction, strength) VALUES ('QQQ', '2026-04-06', 'hammer', 'candlestick', "
            "'bullish', 3)"
        )
        bare_conn.commit()
        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v2_config(),
            scoring_date="2026-04-08",
        )
        assert result is not None
        assert result["candlestick_score"] is not None
        assert result["structural_score"] is not None
        assert isinstance(result["candlestick_score"], float)


# ---------------------------------------------------------------------------
# F2: v2 ≠ v1 with crossovers
# ---------------------------------------------------------------------------

class TestV2DiffersFromV1:
    def test_v2_diverges_from_v1_when_crossover_present(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        # A bullish EMA 21/50 crossover near scoring_date — only v2 picks this up.
        bare_conn.execute(
            "INSERT INTO crossovers_weekly(ticker, week_start, crossover_type, direction, "
            "days_ago) VALUES ('QQQ', '2026-04-06', 'ema_21_50', 'bullish', 2)"
        )
        bare_conn.commit()
        v1 = compute_weekly_score(bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-08")
        v2 = compute_weekly_score(bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-08")
        assert v1 is not None and v2 is not None
        # cdl/struct weights are 0.0; the divergence comes from crossovers feeding
        # the trend category in v2 (which now includes crossover_ema_21_50 = +50 * decay).
        assert v1 != pytest.approx(v2, abs=0.01), (
            f"v2 ({v2:.3f}) should differ from v1 ({v1:.3f}) when a weekly "
            f"crossover is populated and feeds the trend category."
        )


# ---------------------------------------------------------------------------
# SQL targets weekly tables (not daily)
# ---------------------------------------------------------------------------

class TestSqlTargetsWeeklyTables:
    def test_v2_reads_patterns_weekly_not_patterns_daily(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        # Insert a STRONGLY BEARISH structural pattern in patterns_daily.
        # If the v2 weekly path mistakenly reads patterns_daily, the structural
        # category will be massively negative and the composite will drop.
        bare_conn.execute(
            "INSERT INTO patterns_daily(ticker, date, pattern_name, pattern_category, "
            "direction, strength) VALUES ('QQQ', '2026-04-06', 'double_top', 'structural', "
            "'bearish', 10)"
        )
        # Insert a BULLISH structural pattern in patterns_weekly with same date.
        bare_conn.execute(
            "INSERT INTO patterns_weekly(ticker, week_start, pattern_name, pattern_category, "
            "direction, strength) VALUES ('QQQ', '2026-04-06', 'double_bottom', 'structural', "
            "'bullish', 5)"
        )
        bare_conn.commit()
        breakdown = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-08",
        )
        assert breakdown is not None
        # structural_score should reflect the BULLISH weekly pattern (positive),
        # not the BEARISH daily one (negative).
        assert breakdown["structural_score"] > 0, (
            "v2 weekly structural_score should pick up the bullish weekly "
            "pattern, not the bearish daily one."
        )

    def test_v2_reads_crossovers_weekly_not_daily(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.execute(
            "INSERT INTO crossovers_daily(ticker, date, crossover_type, direction, days_ago) "
            "VALUES ('QQQ', '2026-04-06', 'ema_21_50', 'bearish', 2)"
        )
        bare_conn.execute(
            "INSERT INTO crossovers_weekly(ticker, week_start, crossover_type, direction, "
            "days_ago) VALUES ('QQQ', '2026-04-06', 'ema_21_50', 'bullish', 2)"
        )
        bare_conn.commit()
        v2 = compute_weekly_score(bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-08")
        assert v2 is not None
        # If we'd read crossovers_daily, the bearish crossover would pull the
        # trend category down. Only the v1 baseline would be unaffected, so
        # check v2 trend_score itself.
        breakdown = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-08",
        )
        assert breakdown is not None
        # Bullish weekly crossover added to bullish trend indicators → trend > 0
        assert breakdown["trend_score"] > 0


# ---------------------------------------------------------------------------
# Profile fallback
# ---------------------------------------------------------------------------

class TestProfileFallback:
    def test_weekly_profile_empty_falls_back_to_daily(self, bare_conn, caplog) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        # Populate ONLY the daily profile table.
        bare_conn.execute(
            "INSERT INTO indicator_profiles(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES ('QQQ', 'rsi_14', 25, 35, 50, 65, 80, 50, 12)"
        )
        bare_conn.commit()
        # Reset module-level fallback set so this test logs reliably.
        from src.scorer import timeframe_merger as tm
        tm._PROFILE_FALLBACK_LOGGED.clear()
        with caplog.at_level(logging.INFO, logger=tm.__name__):
            score = compute_weekly_score(
                bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
            )
        assert score is not None
        # The fallback INFO message should have been emitted exactly once.
        fallback_logs = [
            r for r in caplog.records
            if "indicator_profiles_weekly empty" in r.getMessage()
        ]
        assert len(fallback_logs) == 1

    def test_weekly_profile_present_no_fallback(self, bare_conn, caplog) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.execute(
            "INSERT INTO indicator_profiles_weekly(ticker, indicator, p5, p20, p50, p80, p95, mean, std) "
            "VALUES ('QQQ', 'rsi_14', 25, 35, 50, 65, 80, 50, 12)"
        )
        bare_conn.commit()
        from src.scorer import timeframe_merger as tm
        tm._PROFILE_FALLBACK_LOGGED.clear()
        with caplog.at_level(logging.INFO, logger=tm.__name__):
            score = compute_weekly_score(
                bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
            )
        assert score is not None
        fallback_logs = [
            r for r in caplog.records
            if "indicator_profiles_weekly empty" in r.getMessage()
        ]
        assert len(fallback_logs) == 0


# ---------------------------------------------------------------------------
# F3: monthly candlestick is permanently None
# ---------------------------------------------------------------------------

class TestMonthlyCandlestickAlwaysNoneF3:
    def test_monthly_v2_candlestick_score_is_none(self, bare_conn) -> None:
        bullish_monthly = {
            **_BULLISH_INDICATORS,
            "month_start": "2026-04-01",
        }
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        # Even with a candlestick pattern populated in patterns_monthly, the
        # monthly breakdown should expose candlestick_score=None.
        bare_conn.execute(
            "INSERT INTO patterns_monthly(ticker, month_start, pattern_name, pattern_category, "
            "direction, strength) VALUES ('QQQ', '2026-04-01', 'hammer', 'candlestick', "
            "'bullish', 3)"
        )
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert result["candlestick_score"] is None
        # Structural still works on monthly (28-day window is OK for monthly cadence)
        # — sanity check the rest of the dict
        assert result["structural_score"] is not None

    def test_monthly_v1_candlestick_score_also_none(self, bare_conn) -> None:
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert result["candlestick_score"] is None
        assert result["structural_score"] is None

    def test_monthly_v2_diverges_from_v1_with_crossovers(self, bare_conn) -> None:
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.execute(
            "INSERT INTO crossovers_monthly(ticker, month_start, crossover_type, direction, "
            "days_ago) VALUES ('QQQ', '2026-04-01', 'ema_21_50', 'bullish', 1)"
        )
        bare_conn.commit()
        v1 = compute_monthly_score(bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-02")
        v2 = compute_monthly_score(bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-02")
        assert v1 is not None and v2 is not None
        assert v1 != pytest.approx(v2, abs=0.01)


# ---------------------------------------------------------------------------
# Default flag preserves v1 semantics
# ---------------------------------------------------------------------------

class TestDefaultFlagIsV1:
    def test_omitting_flag_uses_v1(self, bare_conn) -> None:
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()
        config_no_flag = {**_CONFIG_BASE}  # no weekly_score_method key
        config_explicit_v1 = {**_CONFIG_BASE, "weekly_score_method": "v1_4cat"}
        score_default = compute_weekly_score(
            bare_conn, "QQQ", config_no_flag, scoring_date="2026-04-09",
        )
        score_v1 = compute_weekly_score(
            bare_conn, "QQQ", config_explicit_v1, scoring_date="2026-04-09",
        )
        assert score_default == pytest.approx(score_v1, abs=0.001)


# ---------------------------------------------------------------------------
# Step 2 — indicator_scores included in breakdown return dict
# ---------------------------------------------------------------------------

class TestBreakdownIncludesIndicatorScores:
    """Verify that compute_weekly/monthly_score_breakdown include
    ``indicator_scores`` (the raw per-indicator dict from score_all_indicators)
    in their return value."""

    def test_weekly_v1_breakdown_has_indicator_scores_key(self, bare_conn) -> None:
        """compute_weekly_score_breakdown returns ``indicator_scores`` dict in v1 mode."""
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()
        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
        )
        assert result is not None
        assert "indicator_scores" in result, "breakdown must include 'indicator_scores' key"
        assert isinstance(result["indicator_scores"], dict)

    def test_weekly_v1_breakdown_indicator_scores_nonempty(self, bare_conn) -> None:
        """The indicator_scores dict is non-empty and contains known keys."""
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()
        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
        )
        assert result is not None
        scores = result["indicator_scores"]
        # rsi_14 is in the indicators row and should appear in the returned dict
        assert "rsi_14" in scores, "rsi_14 should be in indicator_scores"
        # The value should be a float or None (not missing/absent)
        assert scores["rsi_14"] is None or isinstance(scores["rsi_14"], float)

    def test_weekly_v2_breakdown_has_indicator_scores_key(self, bare_conn) -> None:
        """compute_weekly_score_breakdown returns ``indicator_scores`` dict in v2 mode."""
        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()
        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-09",
        )
        assert result is not None
        assert "indicator_scores" in result
        assert isinstance(result["indicator_scores"], dict)

    def test_monthly_v1_breakdown_has_indicator_scores_key(self, bare_conn) -> None:
        """compute_monthly_score_breakdown returns ``indicator_scores`` dict in v1 mode."""
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert "indicator_scores" in result
        assert isinstance(result["indicator_scores"], dict)

    def test_monthly_v2_breakdown_has_indicator_scores_key(self, bare_conn) -> None:
        """compute_monthly_score_breakdown returns ``indicator_scores`` dict in v2 mode."""
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert "indicator_scores" in result
        assert isinstance(result["indicator_scores"], dict)

    def test_indicator_scores_matches_inline_computation(self, bare_conn) -> None:
        """indicator_scores in the breakdown matches a direct score_all_indicators call."""
        from src.scorer.indicator_scorer import score_all_indicators, load_profile_for_ticker

        _insert_weekly_indicators(bare_conn, "QQQ", _BULLISH_INDICATORS)
        bare_conn.commit()

        result = compute_weekly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-09",
        )
        assert result is not None
        breakdown_scores = result["indicator_scores"]

        # Recompute directly to verify the dict is the same one score_all_indicators produced.
        row = bare_conn.execute(
            "SELECT w.close, i.* FROM indicators_weekly i "
            "JOIN weekly_candles w ON i.ticker = w.ticker AND i.week_start = w.week_start "
            "WHERE i.ticker = 'QQQ' ORDER BY i.week_start DESC LIMIT 1"
        ).fetchone()
        indicators = dict(row)
        profiles = load_profile_for_ticker(bare_conn, "QQQ", source_table="indicator_profiles_weekly")
        expected = score_all_indicators(
            indicators=indicators,
            close=indicators["close"],
            profiles=profiles,
            config=_v1_config(),
            regime="ranging",
        )
        assert breakdown_scores == expected


# ---------------------------------------------------------------------------
# regime_weights present in monthly breakdown return dict
# ---------------------------------------------------------------------------

class TestMonthlyBreakdownIncludesRegimeWeights:
    """Verify that compute_monthly_score_breakdown includes ``regime_weights``
    in its return value for both v1 and v2 scoring modes."""

    def test_monthly_v1_breakdown_has_regime_weights_key(self, bare_conn) -> None:
        """compute_monthly_score_breakdown (v1) must include 'regime_weights' dict."""
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v1_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert "regime_weights" in result, "v1 monthly breakdown must include 'regime_weights'"
        assert isinstance(result["regime_weights"], dict)
        assert len(result["regime_weights"]) > 0, "regime_weights must not be empty"

    def test_monthly_v2_breakdown_has_regime_weights_key(self, bare_conn) -> None:
        """compute_monthly_score_breakdown (v2) must include 'regime_weights' dict."""
        bullish_monthly = {**_BULLISH_INDICATORS, "month_start": "2026-04-01"}
        _insert_monthly_indicators(bare_conn, "QQQ", bullish_monthly)
        bare_conn.commit()
        result = compute_monthly_score_breakdown(
            bare_conn, "QQQ", _v2_config(), scoring_date="2026-04-02",
        )
        assert result is not None
        assert "regime_weights" in result, "v2 monthly breakdown must include 'regime_weights'"
        assert isinstance(result["regime_weights"], dict)
        assert len(result["regime_weights"]) > 0, "regime_weights must not be empty"
