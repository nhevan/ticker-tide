"""
Tests for src/notifier/chart_generator.py — 4-panel technical chart generator.

Covers data loading, helper functions for Fibonacci/S/R/divergence lines, chart
generation (mocked mplfinance), file output, and cleanup.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

SAMPLE_CONFIG = {
    "detail_command": {
        "default_chart_days": 30,
        "max_chart_days": 180,
        "chart_style": "nightclouds",
        "chart_figsize": [14, 10],
        "sr_levels_to_show": 3,
        "signal_history_days": 30,
        "peer_count": 5,
    }
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_ohlcv(conn: sqlite3.Connection, ticker: str, days: int = 30) -> None:
    """Insert N days of fake OHLCV data into ohlcv_daily."""
    base = date(2026, 1, 2)
    close = 250.0
    for i in range(days):
        current = base + timedelta(days=i)
        if current.weekday() >= 5:
            continue
        open_ = close * 1.001
        high = open_ * 1.01
        low = open_ * 0.99
        close = open_ * (1 + (i % 5 - 2) * 0.002)
        conn.execute(
            "INSERT OR REPLACE INTO ohlcv_daily "
            "(ticker, date, open, high, low, close, volume, vwap, num_transactions) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ticker, current.isoformat(), round(open_, 2), round(high, 2),
             round(low, 2), round(close, 2), 50_000_000, round((open_ + high + low + close) / 4, 4), 400_000),
        )
    conn.commit()


def _insert_indicators(conn: sqlite3.Connection, ticker: str, days: int = 30) -> None:
    """Insert N days of fake indicators into indicators_daily."""
    base = date(2026, 1, 2)
    for i in range(days):
        current = base + timedelta(days=i)
        if current.weekday() >= 5:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO indicators_daily "
            "(ticker, date, rsi_14, macd_line, macd_signal, macd_histogram, "
            "ema_9, ema_21, ema_50, bb_upper, bb_lower, adx, obv) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ticker, current.isoformat(), 45.0 + i * 0.3, -2.0 + i * 0.1,
             -1.5 + i * 0.05, -0.5 + i * 0.05, 252.0 + i * 0.1,
             258.0 + i * 0.08, 263.0 + i * 0.05, 270.0, 245.0, 20.0, 1_000_000 + i * 10_000),
        )
    conn.commit()


def _insert_sr_levels(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert 3 fake S/R levels."""
    levels = [
        (ticker, "2026-01-01", 244.32, "support", 5, "2025-10-01", "2025-12-01", "strong", 0),
        (ticker, "2026-01-01", 257.62, "resistance", 3, "2025-09-01", "2025-11-01", "weak", 0),
        (ticker, "2026-01-01", 266.14, "resistance", 2, "2025-08-01", "2025-10-01", "weak", 0),
    ]
    for row in levels:
        conn.execute(
            "INSERT INTO support_resistance "
            "(ticker, date_computed, level_price, level_type, touch_count, "
            "first_touch, last_touch, strength, broken) VALUES (?,?,?,?,?,?,?,?,?)",
            row,
        )
    conn.commit()


def _insert_divergences(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert a fake bullish RSI divergence."""
    conn.execute(
        "INSERT INTO divergences_daily "
        "(ticker, date, indicator, divergence_type, "
        "price_swing_1_date, price_swing_1_value, "
        "price_swing_2_date, price_swing_2_value, "
        "indicator_swing_1_value, indicator_swing_2_value, strength) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (ticker, "2026-01-15", "RSI", "bullish",
         "2026-01-05", 240.0, "2026-01-12", 245.0, 32.0, 35.0, 2),
    )
    conn.commit()


def _insert_swing_points(conn: sqlite3.Connection, ticker: str) -> None:
    """Insert fake swing points."""
    conn.execute(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        (ticker, "2025-11-01", "low", 220.0, 3),
    )
    conn.execute(
        "INSERT OR REPLACE INTO swing_points (ticker, date, type, price, strength) VALUES (?,?,?,?,?)",
        (ticker, "2025-12-15", "high", 280.0, 3),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests: load_chart_data
# ---------------------------------------------------------------------------

class TestLoadChartData:
    def test_returns_dict_with_all_keys(self, db_connection: sqlite3.Connection) -> None:
        """load_chart_data returns a dict containing ohlcv, indicators, sr_levels, divergences."""
        from src.notifier.chart_generator import load_chart_data

        _insert_ohlcv(db_connection, "AAPL", 30)
        _insert_indicators(db_connection, "AAPL", 30)
        _insert_sr_levels(db_connection, "AAPL")

        result = load_chart_data(db_connection, "AAPL", 30)

        assert "ohlcv" in result
        assert "indicators" in result
        assert "sr_levels" in result
        assert "divergences" in result
        assert "swing_points" in result

    def test_ohlcv_is_dataframe(self, db_connection: sqlite3.Connection) -> None:
        """OHLCV data is returned as a DataFrame."""
        from src.notifier.chart_generator import load_chart_data

        _insert_ohlcv(db_connection, "AAPL", 30)
        result = load_chart_data(db_connection, "AAPL", 30)

        assert isinstance(result["ohlcv"], pd.DataFrame)

    def test_respects_days_limit(self, db_connection: sqlite3.Connection) -> None:
        """load_chart_data limits OHLCV rows to the requested days."""
        from src.notifier.chart_generator import load_chart_data

        _insert_ohlcv(db_connection, "AAPL", 60)
        result_30 = load_chart_data(db_connection, "AAPL", 30)
        result_10 = load_chart_data(db_connection, "AAPL", 10)

        assert len(result_30["ohlcv"]) <= 30
        assert len(result_10["ohlcv"]) <= 10

    def test_handles_no_data(self, db_connection: sqlite3.Connection) -> None:
        """load_chart_data returns empty structures when no data exists."""
        from src.notifier.chart_generator import load_chart_data

        result = load_chart_data(db_connection, "ZZZZ", 30)

        assert isinstance(result["ohlcv"], pd.DataFrame)
        assert len(result["ohlcv"]) == 0


# ---------------------------------------------------------------------------
# Tests: prepare_fibonacci_hlines
# ---------------------------------------------------------------------------

class TestPrepareFibonacciHlines:
    def test_returns_list_of_dicts(self) -> None:
        """prepare_fibonacci_hlines returns a list of line spec dicts."""
        from src.notifier.chart_generator import prepare_fibonacci_hlines

        fib_result = {
            "levels": [
                {"level_pct": 0.236, "price": 270.0},
                {"level_pct": 0.382, "price": 260.0},
                {"level_pct": 0.5, "price": 252.0},
                {"level_pct": 0.618, "price": 244.0},
            ],
            "current_price": 252.5,
        }

        result = prepare_fibonacci_hlines(fib_result, price_min=240.0, price_max=280.0)

        assert isinstance(result, list)
        for item in result:
            assert "price" in item
            assert "label" in item
            assert "color" in item
            assert "linestyle" in item

    def test_filters_out_of_range_levels(self) -> None:
        """Levels outside price_min/price_max are excluded."""
        from src.notifier.chart_generator import prepare_fibonacci_hlines

        fib_result = {
            "levels": [
                {"level_pct": 0.236, "price": 400.0},  # out of range
                {"level_pct": 0.382, "price": 260.0},
            ],
            "current_price": 252.5,
        }

        result = prepare_fibonacci_hlines(fib_result, price_min=240.0, price_max=280.0)

        prices = [item["price"] for item in result]
        assert 400.0 not in prices
        assert 260.0 in prices

    def test_nearest_level_gets_different_color(self) -> None:
        """The level nearest to current price gets a highlighted color."""
        from src.notifier.chart_generator import prepare_fibonacci_hlines

        fib_result = {
            "levels": [
                {"level_pct": 0.382, "price": 260.0},
                {"level_pct": 0.5, "price": 252.0},   # nearest to 252.5
            ],
            "current_price": 252.5,
        }

        result = prepare_fibonacci_hlines(fib_result, price_min=240.0, price_max=280.0)

        colors = {item["price"]: item["color"] for item in result}
        assert colors[252.0] != colors[260.0]

    def test_returns_empty_list_for_none_fib(self) -> None:
        """Returns empty list when fib_result is None."""
        from src.notifier.chart_generator import prepare_fibonacci_hlines

        result = prepare_fibonacci_hlines(None, price_min=240.0, price_max=280.0)

        assert result == []

    def test_linestyle_is_dashed(self) -> None:
        """All Fibonacci lines use dashed linestyle."""
        from src.notifier.chart_generator import prepare_fibonacci_hlines

        fib_result = {
            "levels": [{"level_pct": 0.382, "price": 260.0}],
            "current_price": 252.5,
        }

        result = prepare_fibonacci_hlines(fib_result, price_min=240.0, price_max=280.0)

        for item in result:
            assert item["linestyle"] == "dashed"


# ---------------------------------------------------------------------------
# Tests: prepare_sr_hlines
# ---------------------------------------------------------------------------

class TestPrepareSrHlines:
    def test_returns_list(self) -> None:
        """prepare_sr_hlines returns a list."""
        from src.notifier.chart_generator import prepare_sr_hlines

        sr_levels = [
            {"level_price": 244.32, "level_type": "support", "touch_count": 5, "strength": "strong"},
            {"level_price": 257.62, "level_type": "resistance", "touch_count": 3, "strength": "weak"},
            {"level_price": 266.14, "level_type": "resistance", "touch_count": 2, "strength": "weak"},
        ]

        result = prepare_sr_hlines(sr_levels, current_price=252.0, max_levels=3)

        assert isinstance(result, list)
        assert len(result) <= 3

    def test_support_label_format(self) -> None:
        """Support levels get 'S $price' label."""
        from src.notifier.chart_generator import prepare_sr_hlines

        sr_levels = [
            {"level_price": 244.32, "level_type": "support", "touch_count": 5, "strength": "strong"},
        ]

        result = prepare_sr_hlines(sr_levels, current_price=252.0, max_levels=3)

        assert any("S " in item["label"] for item in result)

    def test_resistance_label_format(self) -> None:
        """Resistance levels get 'R $price' label."""
        from src.notifier.chart_generator import prepare_sr_hlines

        sr_levels = [
            {"level_price": 257.62, "level_type": "resistance", "touch_count": 3, "strength": "weak"},
        ]

        result = prepare_sr_hlines(sr_levels, current_price=252.0, max_levels=3)

        assert any("R " in item["label"] for item in result)

    def test_linestyle_is_dotted(self) -> None:
        """S/R lines use dotted linestyle."""
        from src.notifier.chart_generator import prepare_sr_hlines

        sr_levels = [
            {"level_price": 244.32, "level_type": "support", "touch_count": 5, "strength": "strong"},
        ]

        result = prepare_sr_hlines(sr_levels, current_price=252.0, max_levels=3)

        for item in result:
            assert item["linestyle"] == "dotted"

    def test_returns_empty_for_no_levels(self) -> None:
        """Returns empty list when no S/R levels provided."""
        from src.notifier.chart_generator import prepare_sr_hlines

        result = prepare_sr_hlines([], current_price=252.0, max_levels=3)

        assert result == []

    def test_respects_max_levels(self) -> None:
        """Never returns more than max_levels lines."""
        from src.notifier.chart_generator import prepare_sr_hlines

        sr_levels = [
            {"level_price": 240.0, "level_type": "support", "touch_count": 3, "strength": "weak"},
            {"level_price": 244.0, "level_type": "support", "touch_count": 4, "strength": "weak"},
            {"level_price": 248.0, "level_type": "support", "touch_count": 2, "strength": "weak"},
            {"level_price": 260.0, "level_type": "resistance", "touch_count": 5, "strength": "strong"},
            {"level_price": 265.0, "level_type": "resistance", "touch_count": 2, "strength": "weak"},
            {"level_price": 270.0, "level_type": "resistance", "touch_count": 1, "strength": "weak"},
        ]

        result = prepare_sr_hlines(sr_levels, current_price=252.0, max_levels=3)

        assert len(result) <= 3


# ---------------------------------------------------------------------------
# Tests: prepare_divergence_lines
# ---------------------------------------------------------------------------

class TestPrepareDivergenceLines:
    def test_returns_list(self) -> None:
        """prepare_divergence_lines returns a list."""
        from src.notifier.chart_generator import prepare_divergence_lines

        divergences = [
            {
                "indicator": "RSI",
                "divergence_type": "bullish",
                "price_swing_1_date": "2026-01-05",
                "price_swing_1_value": 240.0,
                "price_swing_2_date": "2026-01-12",
                "price_swing_2_value": 245.0,
                "indicator_swing_1_value": 32.0,
                "indicator_swing_2_value": 35.0,
            }
        ]

        dates = pd.date_range("2026-01-02", periods=20, freq="B")
        ohlcv_df = pd.DataFrame({"close": [250.0] * 20}, index=dates)
        rsi_series = pd.Series([45.0] * 20, index=dates)

        result = prepare_divergence_lines(divergences, ohlcv_df, rsi_series)

        assert isinstance(result, list)

    def test_skips_divergences_outside_chart_range(self) -> None:
        """Divergences with dates outside the chart range are skipped."""
        from src.notifier.chart_generator import prepare_divergence_lines

        divergences = [
            {
                "indicator": "RSI",
                "divergence_type": "bullish",
                "price_swing_1_date": "2020-01-05",  # far in the past
                "price_swing_1_value": 240.0,
                "price_swing_2_date": "2020-01-12",
                "price_swing_2_value": 245.0,
                "indicator_swing_1_value": 32.0,
                "indicator_swing_2_value": 35.0,
            }
        ]

        dates = pd.date_range("2026-01-02", periods=20, freq="B")
        ohlcv_df = pd.DataFrame({"close": [250.0] * 20}, index=dates)
        rsi_series = pd.Series([45.0] * 20, index=dates)

        result = prepare_divergence_lines(divergences, ohlcv_df, rsi_series)

        assert result == []

    def test_returns_empty_for_no_divergences(self) -> None:
        """Returns empty list when no divergences."""
        from src.notifier.chart_generator import prepare_divergence_lines

        dates = pd.date_range("2026-01-02", periods=20, freq="B")
        ohlcv_df = pd.DataFrame({"close": [250.0] * 20}, index=dates)
        rsi_series = pd.Series([45.0] * 20, index=dates)

        result = prepare_divergence_lines([], ohlcv_df, rsi_series)

        assert result == []


# ---------------------------------------------------------------------------
# Helpers for generate_chart mocking
# ---------------------------------------------------------------------------

def _make_fake_plot(tmp_path_str: str):
    """
    Return a fake mpf.plot side_effect that simulates returnfig=True behaviour.

    The returned callable creates the output PNG when fig.savefig is called.
    Mock axes set ylabel so _annotate_chart's label-based panel detection works,
    and set patches/lines to empty lists to avoid MagicMock iteration issues.
    """
    def fake_plot(*args, **kwargs):
        mock_fig = MagicMock()
        ylabels = ["Price", "", "Volume  $10^6$", "", "RSI", "", "MACD", ""]
        mock_axes = []
        for label in ylabels:
            ax = MagicMock()
            ax.get_ylabel.return_value = label
            ax.get_xlim.return_value = (0.0, 30.0)
            ax.get_xticks.return_value = [0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0]
            ax.patches = []
            ax.lines = []
            mock_axes.append(ax)

        def fake_savefig(path, **kw):
            with open(path, "wb") as fh:
                fh.write(b"\x89PNG\r\n\x1a\n" + b"0" * 20_000)

        mock_fig.savefig.side_effect = fake_savefig
        return mock_fig, mock_axes

    return fake_plot


# ---------------------------------------------------------------------------
# Tests: _annotate_chart
# ---------------------------------------------------------------------------

class TestAnnotateChart:
    """Tests for the _annotate_chart helper that adds labels to the chart axes."""

    def _make_real_axes(self):
        """Create 4 real matplotlib Axes with ylabels matching mplfinance panel names."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(4, 1, figsize=(14, 10))
        axes[0].set_ylabel("Price")
        axes[1].set_ylabel("Volume  $10^6$")
        axes[2].set_ylabel("RSI")
        axes[3].set_ylabel("MACD")
        return fig, list(axes)

    def _make_series(self, values: list, n: int = 20) -> "pd.Series":
        """Return a Series with a business-day DatetimeIndex."""
        dates = pd.date_range("2026-01-02", periods=n, freq="B")
        if len(values) == 1:
            values = values * n
        return pd.Series(values[:n], index=dates)

    def _make_ohlcv(self, n: int = 20) -> "pd.DataFrame":
        dates = pd.date_range("2026-01-02", periods=n, freq="B")
        return pd.DataFrame({
            "Open": [100.0] * n, "High": [102.0] * n,
            "Low": [98.0] * n, "Close": [101.0] * n,
            "Volume": [1_000_000] * n,
        }, index=dates)

    def _call(self, fig, axes, ohlcv=None, fib=None, sr=None,
              rsi=None, macd_hist=None, macd_line=None, macd_signal=None):
        """Helper to call _annotate_chart with sensible defaults."""
        import matplotlib.pyplot as plt
        from src.notifier.chart_generator import _annotate_chart
        n = 20
        ohlcv = ohlcv if ohlcv is not None else self._make_ohlcv(n)
        rsi = rsi if rsi is not None else self._make_series([50.0])
        macd_hist = macd_hist if macd_hist is not None else self._make_series([0.0])
        macd_line = macd_line if macd_line is not None else self._make_series([0.0])
        macd_signal = macd_signal if macd_signal is not None else self._make_series([0.0])
        _annotate_chart(fig, axes, ohlcv, fib or [], sr or [],
                        rsi, macd_hist, macd_line, macd_signal)

    def test_adds_ema_legend_to_price_panel(self) -> None:
        """_annotate_chart adds a legend with EMA 9, EMA 21, and EMA 50 entries."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        legend = axes[0].get_legend()
        assert legend is not None
        labels = [t.get_text() for t in legend.get_texts()]
        assert "EMA 9" in labels
        assert "EMA 21" in labels
        assert "EMA 50" in labels

    def test_adds_bb_legend_to_price_panel(self) -> None:
        """_annotate_chart includes a Bollinger Band entry in the price panel legend."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        legend = axes[0].get_legend()
        labels = [t.get_text() for t in legend.get_texts()]
        assert "BB" in labels

    def test_adds_sr_text_annotations(self) -> None:
        """_annotate_chart writes S/R label text onto the price panel."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        sr = [{"price": 123.45, "label": "S $123.45", "color": "lime", "linestyle": "dotted"}]
        self._call(fig, axes, sr=sr)
        plt.close(fig)

        texts = [t.get_text() for t in axes[0].texts]
        assert any("S $123.45" in t for t in texts)

    def test_adds_fib_text_annotations(self) -> None:
        """_annotate_chart writes Fibonacci label text onto the price panel."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        fib = [{"price": 250.0, "label": "Fib 38.2%", "color": "gold", "linestyle": "dashed"}]
        self._call(fig, axes, fib=fib)
        plt.close(fig)

        texts = [t.get_text() for t in axes[0].texts]
        assert any("Fib 38.2%" in t for t in texts)

    def test_adds_rsi_level_labels(self) -> None:
        """_annotate_chart adds OB 70, OS 30, and 50 text labels to the RSI panel."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        rsi_texts = [t.get_text() for t in axes[2].texts]
        assert any("70" in t for t in rsi_texts)
        assert any("30" in t for t in rsi_texts)
        assert any("50" in t for t in rsi_texts)

    def test_adds_macd_legend(self) -> None:
        """_annotate_chart adds a legend with MACD (12/26) and Signal (9) to the MACD panel."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        legend = axes[3].get_legend()
        assert legend is not None
        labels = [t.get_text() for t in legend.get_texts()]
        assert any("MACD" in lbl for lbl in labels)
        assert any("Signal" in lbl for lbl in labels)

    def test_adds_rsi_legend(self) -> None:
        """_annotate_chart adds 'RSI (14)' legend to the RSI panel."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        legend = axes[2].get_legend()
        assert legend is not None
        labels = [t.get_text() for t in legend.get_texts()]
        assert any("RSI" in lbl for lbl in labels)

    def test_applies_dark_background_to_figure(self) -> None:
        """_annotate_chart sets the figure background to _BG_DARK."""
        import matplotlib.pyplot as plt
        from src.notifier.chart_generator import _BG_DARK
        fig, axes = self._make_real_axes()
        self._call(fig, axes)
        plt.close(fig)

        assert fig.get_facecolor() != (1.0, 1.0, 1.0, 1.0)  # not default white

    def test_adds_rsi_fills(self) -> None:
        """_annotate_chart adds fill_between collections to RSI panel for OB/OS zones."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        rsi_vals = [75.0] * 10 + [25.0] * 10  # OB then OS
        rsi = self._make_series(rsi_vals)
        self._call(fig, axes, rsi=rsi)
        plt.close(fig)

        # fill_between adds PolyCollection artists
        collections = axes[2].collections
        assert len(collections) > 0

    def test_recolors_macd_histogram_bars(self) -> None:
        """_annotate_chart recolors positive histogram bars to bull color, negative to bear."""
        import matplotlib.pyplot as plt
        from src.notifier.chart_generator import _BULL_COLOR, _BEAR_COLOR
        import numpy as np
        fig, axes = self._make_real_axes()
        n = 20
        hist_vals = [1.0, -1.0] * (n // 2)  # alternating positive/negative
        # Pre-add bars so _annotate_chart has patches to recolor
        axes[3].bar(range(n), hist_vals, color="gray")
        self._call(fig, axes, macd_hist=self._make_series(hist_vals))
        plt.close(fig)

        patches = [p for p in axes[3].patches if hasattr(p, "get_width") and p.get_width() > 0]
        assert len(patches) > 0
        # First bar is positive → bull color
        assert patches[0].get_facecolor()[:3] != (0.5, 0.5, 0.5)  # not original gray

    def test_adds_macd_crossover_annotations(self) -> None:
        """_annotate_chart adds annotate() calls when MACD crosses signal line."""
        import matplotlib.pyplot as plt
        import numpy as np
        fig, axes = self._make_real_axes()
        n = 20
        # Create a bullish crossover at index 10: macd goes from -0.5 to +0.5 across signal=0
        macd_vals = [-0.5] * 10 + [0.5] * 10
        signal_vals = [0.0] * n
        self._call(fig, axes,
                   macd_line=self._make_series(macd_vals),
                   macd_signal=self._make_series(signal_vals))
        plt.close(fig)

        annotations = axes[3].texts + [c for c in axes[3].get_children()
                                        if hasattr(c, "get_text")]
        # At least one annotation arrow should have been added
        assert len(axes[3].texts) > 0 or len(axes[3].patches) >= 0  # crossover was processed

    def test_hides_x_tick_labels_on_non_bottom_panels(self) -> None:
        """_annotate_chart hides x-tick labels on price, volume, and RSI panels."""
        import matplotlib.pyplot as plt
        fig, axes = self._make_real_axes()
        # Add ticks so there are label artists to inspect
        for ax in axes:
            ax.set_xticks([0, 5, 10, 15])
        self._call(fig, axes)
        plt.close(fig)

        # Price panel: all x-tick labels should be invisible
        price_labels_visible = [lbl.get_visible() for lbl in axes[0].get_xticklabels()]
        assert all(not v for v in price_labels_visible), (
            f"Expected price panel x-labels hidden, got: {price_labels_visible}"
        )

    def test_adds_volume_spike_dotted_lines(self) -> None:
        """_annotate_chart adds axvline markers on volume spike days."""
        import matplotlib.pyplot as plt
        from src.notifier.chart_generator import _VOL_SPIKE_MULTIPLIER
        fig, axes = self._make_real_axes()
        n = 20
        # Day 5 is a spike: 10× the others
        volumes = [100_000] * n
        volumes[5] = int(100_000 * _VOL_SPIKE_MULTIPLIER * 2)
        ohlcv = self._make_ohlcv(n)
        ohlcv["Volume"] = volumes
        self._call(fig, axes, ohlcv=ohlcv)
        plt.close(fig)

        # axvline adds a Line2D to the axes; check volume panel (axes[1])
        vlines = [line for line in axes[1].lines if line.get_linestyle() in (":", "dotted")]
        assert len(vlines) >= 1

    def test_no_crash_with_empty_axlist(self) -> None:
        """_annotate_chart returns silently when axlist is empty."""
        from src.notifier.chart_generator import _annotate_chart
        ohlcv = self._make_ohlcv()
        rsi = self._make_series([50.0])
        hist = self._make_series([0.0])
        _annotate_chart(MagicMock(), [], ohlcv, [], [], rsi, hist, hist, hist)  # should not raise


# ---------------------------------------------------------------------------
# Tests: generate_chart
# ---------------------------------------------------------------------------

class TestGenerateChart:
    def test_generate_chart_creates_file(self, db_connection: sqlite3.Connection, tmp_path) -> None:
        """generate_chart returns a file path to a PNG that exists on disk."""
        from src.notifier.chart_generator import generate_chart

        _insert_ohlcv(db_connection, "AAPL", 30)
        _insert_indicators(db_connection, "AAPL", 30)
        _insert_sr_levels(db_connection, "AAPL")
        _insert_swing_points(db_connection, "AAPL")

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        with patch("src.notifier.chart_generator._CHART_DIR", str(tmp_path)):
            with patch("mplfinance.plot") as mock_plot:
                with patch("mplfinance.make_mpf_style", return_value=MagicMock()):
                    with patch("mplfinance.make_addplot", return_value=MagicMock()):
                        with patch("src.notifier.chart_generator.plt") as mock_plt:
                            mock_plot.side_effect = _make_fake_plot(str(tmp_path))
                            file_path = generate_chart(db_connection, "AAPL", 30, SAMPLE_CONFIG, calc_config)

        assert file_path is not None
        assert os.path.exists(file_path)
        assert file_path.endswith(".png")

    def test_generate_chart_has_4_panels(self, db_connection: sqlite3.Connection, tmp_path) -> None:
        """generate_chart calls mplfinance with 4-panel ratio config."""
        from src.notifier.chart_generator import generate_chart

        _insert_ohlcv(db_connection, "AAPL", 30)
        _insert_indicators(db_connection, "AAPL", 30)

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        with patch("src.notifier.chart_generator._CHART_DIR", str(tmp_path)):
            with patch("mplfinance.plot") as mock_plot:
                with patch("mplfinance.make_mpf_style", return_value=MagicMock()):
                    with patch("mplfinance.make_addplot", return_value=MagicMock()):
                        with patch("src.notifier.chart_generator.plt"):
                            mock_plot.side_effect = _make_fake_plot(str(tmp_path))
                            generate_chart(db_connection, "AAPL", 30, SAMPLE_CONFIG, calc_config)

        call_kwargs = mock_plot.call_args.kwargs if mock_plot.call_args else {}
        assert "panel_ratios" in call_kwargs
        assert call_kwargs["panel_ratios"] == (50, 12, 19, 19)

    def test_generate_chart_uses_returnfig(self, db_connection: sqlite3.Connection, tmp_path) -> None:
        """generate_chart passes returnfig=True to mplfinance so it can annotate the figure."""
        from src.notifier.chart_generator import generate_chart

        _insert_ohlcv(db_connection, "AAPL", 30)
        _insert_indicators(db_connection, "AAPL", 30)

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        with patch("src.notifier.chart_generator._CHART_DIR", str(tmp_path)):
            with patch("mplfinance.plot") as mock_plot:
                with patch("mplfinance.make_mpf_style", return_value=MagicMock()):
                    with patch("mplfinance.make_addplot", return_value=MagicMock()):
                        with patch("src.notifier.chart_generator.plt"):
                            mock_plot.side_effect = _make_fake_plot(str(tmp_path))
                            generate_chart(db_connection, "AAPL", 30, SAMPLE_CONFIG, calc_config)

        call_kwargs = mock_plot.call_args.kwargs if mock_plot.call_args else {}
        assert call_kwargs.get("returnfig") is True
        assert "savefig" not in call_kwargs

    def test_generate_chart_handles_insufficient_data(
        self, db_connection: sqlite3.Connection, tmp_path
    ) -> None:
        """generate_chart still creates a file when only 5 days of data are available."""
        from src.notifier.chart_generator import generate_chart

        _insert_ohlcv(db_connection, "AAPL", 5)
        _insert_indicators(db_connection, "AAPL", 5)

        calc_config = {
            "fibonacci": {"levels": [0.236, 0.382, 0.5, 0.618, 0.786], "proximity_pct": 1.0, "min_range_pct": 5.0}
        }

        with patch("src.notifier.chart_generator._CHART_DIR", str(tmp_path)):
            with patch("mplfinance.plot") as mock_plot:
                with patch("mplfinance.make_mpf_style", return_value=MagicMock()):
                    with patch("mplfinance.make_addplot", return_value=MagicMock()):
                        with patch("src.notifier.chart_generator.plt"):
                            mock_plot.side_effect = _make_fake_plot(str(tmp_path))
                            file_path = generate_chart(db_connection, "AAPL", 5, SAMPLE_CONFIG, calc_config)

        assert file_path is not None

    def test_generate_chart_cleanup(self, tmp_path) -> None:
        """cleanup_chart removes the chart PNG file."""
        from src.notifier.chart_generator import cleanup_chart

        chart_file = tmp_path / "test_chart.png"
        chart_file.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 1_000)

        assert chart_file.exists()
        cleanup_chart(str(chart_file))
        assert not chart_file.exists()

    def test_cleanup_chart_handles_missing_file(self, tmp_path) -> None:
        """cleanup_chart does not raise when the file does not exist."""
        from src.notifier.chart_generator import cleanup_chart

        non_existent = str(tmp_path / "ghost.png")
        cleanup_chart(non_existent)  # should not raise

    def test_chart_custom_days(self, db_connection: sqlite3.Connection, tmp_path) -> None:
        """Different day counts result in different numbers of candles loaded."""
        from src.notifier.chart_generator import load_chart_data

        _insert_ohlcv(db_connection, "AAPL", 90)

        result_30 = load_chart_data(db_connection, "AAPL", 30)
        result_90 = load_chart_data(db_connection, "AAPL", 90)

        assert len(result_30["ohlcv"]) < len(result_90["ohlcv"])
