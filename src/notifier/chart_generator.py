"""
Technical chart generator using mplfinance.

Generates a 4-panel chart for the /detail command:
  Panel 1 (50%): Candlestick + EMA 9/21/50 + Bollinger Bands (shaded) +
                 Fibonacci levels (dashed) + S/R levels (dotted)
  Panel 2 (12%): Volume bars (green up, red down)
  Panel 3 (19%): RSI with 30/70 zones + divergence lines
  Panel 4 (19%): MACD line + signal + histogram

Uses mplfinance with 'nightclouds' style (dark mode).
Charts are saved as temporary PNG files and cleaned up after sending.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from typing import Any

import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D

from src.calculator.fibonacci import compute_fibonacci_for_ticker

logger = logging.getLogger(__name__)

_CHART_DIR = "/tmp"

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
_BULL_COLOR = "#4fc3f7"
_BEAR_COLOR = "#f48fb1"
_BG_DARK = "#0d0d0d"
_BG_PANEL = "#111111"
_GRID_COLOR = "#2a2a2a"
_SPINE_COLOR = "#333333"
_TICK_COLOR = "#aaaaaa"
_LEGEND_BG = "#1e1e1e"
_MA9_COLOR = "#66ff99"
_MA21_COLOR = "#ff6ec7"
_MA50_COLOR = "#ffd700"
_BB_COLOR = "#888888"
_RSI_OB_COLOR = "#ff6666"
_RSI_OS_COLOR = "#66ff99"
_MACD_LINE_COLOR = "#00e5ff"
_MACD_SIGNAL_COLOR = "#ff9800"
_VOL_SPIKE_MULTIPLIER = 1.5


def _build_chart_style() -> Any:
    """
    Build a fully custom dark-mode mplfinance style.

    Candles use _BULL_COLOR / _BEAR_COLOR. Figure and panel backgrounds are
    near-black. Grid lines are subtle dark gray.

    Returns:
        mplfinance style object.
    """
    mc = mpf.make_marketcolors(
        up=_BULL_COLOR,
        down=_BEAR_COLOR,
        edge={"up": _BULL_COLOR, "down": _BEAR_COLOR},
        wick={"up": _BULL_COLOR, "down": _BEAR_COLOR},
        volume={"up": _BULL_COLOR, "down": _BEAR_COLOR},
    )
    return mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=mc,
        facecolor=_BG_DARK,
        figcolor=_BG_DARK,
        gridcolor=_GRID_COLOR,
        gridstyle="--",
        gridaxis="both",
        rc={
            "axes.labelcolor": _TICK_COLOR,
            "xtick.color": _TICK_COLOR,
            "ytick.color": _TICK_COLOR,
        },
    )


def load_chart_data(
    db_conn: sqlite3.Connection, ticker: str, days: int
) -> dict:
    """
    Load all data required for the 4-panel chart.

    Queries ohlcv_daily, indicators_daily, support_resistance, divergences_daily,
    and swing_points for the given ticker over the most recent ``days`` trading days.

    Parameters:
        db_conn: Open SQLite connection with row_factory=sqlite3.Row.
        ticker: Ticker symbol.
        days: Number of trading days to load.

    Returns:
        Dict with keys:
          ohlcv (pd.DataFrame) — indexed by date with OHLCV columns
          indicators (pd.DataFrame) — indicator values for the same dates
          sr_levels (list[dict]) — support/resistance levels
          divergences (list[dict]) — divergence records
          swing_points (list[dict]) — swing point records
    """
    ohlcv_rows = db_conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily "
        "WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        (ticker, days),
    ).fetchall()

    if not ohlcv_rows:
        return {
            "ohlcv": pd.DataFrame(),
            "indicators": pd.DataFrame(),
            "sr_levels": [],
            "divergences": [],
            "swing_points": [],
        }

    ohlcv_df = pd.DataFrame([dict(r) for r in reversed(ohlcv_rows)])
    ohlcv_df["date"] = pd.to_datetime(ohlcv_df["date"])
    ohlcv_df = ohlcv_df.set_index("date")
    ohlcv_df.columns = [col.capitalize() for col in ohlcv_df.columns]

    date_min = ohlcv_df.index.min().date().isoformat()
    date_max = ohlcv_df.index.max().date().isoformat()

    ind_rows = db_conn.execute(
        "SELECT * FROM indicators_daily WHERE ticker = ? AND date >= ? AND date <= ? ORDER BY date ASC",
        (ticker, date_min, date_max),
    ).fetchall()
    indicators_df = pd.DataFrame([dict(r) for r in ind_rows]) if ind_rows else pd.DataFrame()
    if not indicators_df.empty:
        indicators_df["date"] = pd.to_datetime(indicators_df["date"])
        indicators_df = indicators_df.set_index("date")

    sr_rows = db_conn.execute(
        "SELECT level_price, level_type, touch_count, strength FROM support_resistance "
        "WHERE ticker = ? AND broken = 0 ORDER BY level_price ASC",
        (ticker,),
    ).fetchall()
    sr_levels = [dict(r) for r in sr_rows]

    div_rows = db_conn.execute(
        "SELECT * FROM divergences_daily WHERE ticker = ? AND date >= ? AND date <= ? ORDER BY date ASC",
        (ticker, date_min, date_max),
    ).fetchall()
    divergences = [dict(r) for r in div_rows]

    sp_rows = db_conn.execute(
        "SELECT date, type, price, strength FROM swing_points "
        "WHERE ticker = ? AND date >= ? AND date <= ? ORDER BY date ASC",
        (ticker, date_min, date_max),
    ).fetchall()
    swing_points = [dict(r) for r in sp_rows]

    return {
        "ohlcv": ohlcv_df,
        "indicators": indicators_df,
        "sr_levels": sr_levels,
        "divergences": divergences,
        "swing_points": swing_points,
    }


def prepare_fibonacci_hlines(
    fib_result: dict | None, price_min: float, price_max: float
) -> list[dict]:
    """
    Convert Fibonacci levels into horizontal line specifications for mplfinance.

    Only includes levels that fall within the visible price range (price_min to price_max).
    The level nearest to the current price is highlighted with a gold color; all others
    use dimgray.

    Parameters:
        fib_result: Output from compute_fibonacci_for_ticker, or None.
        price_min: Lower bound of the visible price range.
        price_max: Upper bound of the visible price range.

    Returns:
        List of dicts with keys: price (float), label (str), color (str), linestyle (str).
    """
    if fib_result is None:
        return []

    levels = fib_result.get("levels", [])
    current_price = fib_result.get("current_price", 0.0)

    visible = [lv for lv in levels if price_min <= lv["price"] <= price_max]
    if not visible:
        return []

    nearest = min(visible, key=lambda lv: abs(lv["price"] - current_price))

    result = []
    for lv in visible:
        pct_label = f"{lv['level_pct'] * 100:.1f}%"
        color = "gold" if lv["price"] == nearest["price"] else "dimgray"
        result.append({
            "price": lv["price"],
            "label": f"Fib {pct_label}",
            "color": color,
            "linestyle": "dashed",
        })
    return result


def prepare_sr_hlines(
    sr_levels: list[dict], current_price: float, max_levels: int = 3
) -> list[dict]:
    """
    Select the nearest S/R levels and format them as horizontal line specs.

    Picks up to max_levels levels by proximity to current_price, splitting
    them into resistance (above) and support (below).  Resistance lines are
    red, support lines are green.

    Parameters:
        sr_levels: List of S/R dicts with keys: level_price, level_type, touch_count, strength.
        current_price: The ticker's current close price.
        max_levels: Maximum number of lines to return.

    Returns:
        List of dicts with keys: price (float), label (str), color (str), linestyle (str).
    """
    if not sr_levels:
        return []

    sorted_by_proximity = sorted(sr_levels, key=lambda lv: abs(lv["level_price"] - current_price))
    selected = sorted_by_proximity[:max_levels]

    result = []
    for lv in selected:
        price = lv["level_price"]
        if lv["level_type"] == "support" or price < current_price:
            prefix = "S"
            color = "lime"
        else:
            prefix = "R"
            color = "red"
        label = f"{prefix} ${price:.2f}"
        if lv.get("strength") and lv["strength"] != "weak":
            label += f" ({lv['strength']})"
        result.append({
            "price": price,
            "label": label,
            "color": color,
            "linestyle": "dotted",
        })
    return result


def prepare_divergence_lines(
    divergences: list[dict],
    ohlcv_df: pd.DataFrame,
    rsi_series: pd.Series,
) -> list[dict]:
    """
    Build line specifications for drawing divergence annotations on the chart.

    For each divergence whose swing dates fall within the chart date range, two
    lines are created: one on the price panel (connecting price swing points)
    and one on the RSI panel (connecting RSI swing values).

    Parameters:
        divergences: List of divergence dicts with swing date and value fields.
        ohlcv_df: OHLCV DataFrame indexed by datetime.
        rsi_series: RSI values indexed by datetime (same index as ohlcv_df).

    Returns:
        List of line spec dicts with keys:
          panel (int), x1 (str), y1 (float), x2 (str), y2 (float), color (str).
    """
    if not divergences or ohlcv_df.empty:
        return []

    chart_dates = set(ohlcv_df.index.strftime("%Y-%m-%d"))
    result = []

    for div in divergences:
        d1 = div.get("price_swing_1_date", "")
        d2 = div.get("price_swing_2_date", "")

        if d1 not in chart_dates or d2 not in chart_dates:
            continue

        color = "lime" if div.get("divergence_type") == "bullish" else "tomato"

        result.append({
            "panel": 0,
            "x1": d1,
            "y1": div["price_swing_1_value"],
            "x2": d2,
            "y2": div["price_swing_2_value"],
            "color": color,
        })

        result.append({
            "panel": 2,
            "x1": d1,
            "y1": div["indicator_swing_1_value"],
            "x2": d2,
            "y2": div["indicator_swing_2_value"],
            "color": color,
        })

    return result


def _annotate_chart(
    fig: Any,
    axlist: list,
    ohlcv_df: pd.DataFrame,
    fib_hlines: list[dict],
    sr_hlines: list[dict],
    rsi: pd.Series,
    macd_hist: pd.Series,
    macd_line: pd.Series,
    macd_signal: pd.Series,
) -> None:
    """
    Apply all visual styling, labels, legends, and annotations to the chart figure.

    Operates on the matplotlib axes returned by mplfinance when returnfig=True.
    Panels are located by their y-axis label ("RSI", "MACD", "Volume") to be
    robust against mplfinance version differences in axes ordering.

    Parameters:
        fig: The matplotlib Figure object.
        axlist: List of Axes as returned by mplfinance returnfig=True.
        ohlcv_df: OHLCV DataFrame indexed by date (used for volume spike detection
            and integer x-coordinate mapping).
        fib_hlines: Fibonacci hline specs (price, label, color, linestyle).
        sr_hlines: S/R hline specs (price, label, color, linestyle).
        rsi: RSI series aligned to ohlcv_df.index.
        macd_hist: MACD histogram series aligned to ohlcv_df.index.
        macd_line: MACD line series aligned to ohlcv_df.index.
        macd_signal: MACD signal series aligned to ohlcv_df.index.

    Returns:
        None
    """
    if not axlist:
        return

    ax_price = axlist[0]
    ax_vol = next((ax for ax in axlist if "Volume" in ax.get_ylabel()), None)
    ax_rsi = next((ax for ax in axlist if "RSI" in ax.get_ylabel()), None)
    ax_macd = next((ax for ax in axlist if "MACD" in ax.get_ylabel()), None)

    n = len(ohlcv_df)
    x = np.arange(n)

    # ------------------------------------------------------------------
    # A. Dark theme — applied to every axis
    # ------------------------------------------------------------------
    fig.patch.set_facecolor(_BG_DARK)
    for ax in axlist:
        ax.set_facecolor(_BG_PANEL)
        for spine in ax.spines.values():
            spine.set_edgecolor(_SPINE_COLOR)
        ax.tick_params(colors=_TICK_COLOR, labelsize=8)
        ax.yaxis.label.set_color(_TICK_COLOR)
        ax.yaxis.label.set_fontsize(9)
        ax.grid(True, color=_GRID_COLOR, linestyle="--", linewidth=0.5, zorder=0)

    # ------------------------------------------------------------------
    # B. Price panel
    # ------------------------------------------------------------------
    price_handles = [
        Line2D([0], [0], color=_MA9_COLOR, linewidth=0.9, label="EMA 9"),
        Line2D([0], [0], color=_MA21_COLOR, linewidth=0.9, label="EMA 21"),
        Line2D([0], [0], color=_MA50_COLOR, linewidth=0.9, label="EMA 50"),
        Line2D([0], [0], color=_BB_COLOR, linewidth=0.6, linestyle="dashed", label="BB"),
    ]
    ax_price.legend(
        handles=price_handles, loc="upper right", fontsize=7,
        facecolor=_LEGEND_BG, edgecolor=_SPINE_COLOR, framealpha=0.9,
    )

    bb_lower_vals = [item["price"] for item in fib_hlines if "Fib" not in item.get("label", "")]
    # BB fill — look up bb_upper/bb_lower from the ohlcv_df is not needed;
    # we receive them implicitly via the add_plot lines. Instead, extract from
    # the line artists already drawn on ax_price.
    bb_lines = [
        line for line in ax_price.lines
        if line.get_linestyle() in ("--", "dashed")
        and line.get_color() in (_BB_COLOR, "#888888")
    ]
    if len(bb_lines) >= 2:
        upper_data = bb_lines[0].get_ydata()
        lower_data = bb_lines[1].get_ydata()
        ax_price.fill_between(
            np.arange(len(upper_data)), lower_data, upper_data,
            alpha=0.05, color=_BB_COLOR, zorder=1,
        )

    # S/R and Fib right-edge text labels
    xlim = ax_price.get_xlim()
    x_right = xlim[1]
    for item in sr_hlines + fib_hlines:
        ax_price.text(
            x_right, item["price"], f" {item['label']}",
            color=item["color"], fontsize=7, va="center", ha="left", clip_on=False,
        )

    # Volume spike event arrows on price panel
    vol_mean = ohlcv_df["Volume"].rolling(20, min_periods=1).mean()
    spike_mask = ohlcv_df["Volume"] > vol_mean * _VOL_SPIKE_MULTIPLIER
    for i, (dt, is_spike) in enumerate(spike_mask.items()):
        if not is_spike:
            continue
        is_bull = float(ohlcv_df.loc[dt, "Close"]) >= float(ohlcv_df.loc[dt, "Open"])
        y_anchor = float(ohlcv_df.loc[dt, "High"] if not is_bull else ohlcv_df.loc[dt, "Low"])
        label = "Sell" if not is_bull else "Buy"
        color = _BEAR_COLOR if not is_bull else _BULL_COLOR
        y_offset = 12 if not is_bull else -12
        ax_price.annotate(
            label,
            xy=(i, y_anchor),
            xytext=(0, y_offset),
            textcoords="offset points",
            color=color,
            fontsize=6,
            ha="center",
            arrowprops=dict(arrowstyle="->", color=color, lw=0.8),
            bbox=dict(boxstyle="round,pad=0.2", facecolor=_LEGEND_BG, edgecolor=color, alpha=0.8),
            zorder=5,
        )

    # ------------------------------------------------------------------
    # C. Volume panel
    # ------------------------------------------------------------------
    if ax_vol is not None:
        vol_patches = [p for p in ax_vol.patches if hasattr(p, "get_width") and p.get_width() > 0]
        for patch in vol_patches:
            patch.set_alpha(0.75)

        spike_indices = [i for i, v in enumerate(spike_mask) if v]
        for idx in spike_indices:
            ax_vol.axvline(x=idx, color=_TICK_COLOR, linestyle=":", linewidth=0.8, zorder=3)

    # ------------------------------------------------------------------
    # D. RSI panel
    # ------------------------------------------------------------------
    if ax_rsi is not None:
        ax_rsi.axhline(70, color=_RSI_OB_COLOR, linestyle="--", linewidth=0.8, zorder=2)
        ax_rsi.axhline(30, color=_RSI_OS_COLOR, linestyle="--", linewidth=0.8, zorder=2)
        ax_rsi.axhline(50, color="#555555", linestyle=":", linewidth=0.6, zorder=2)

        rsi_vals = rsi.values
        ax_rsi.fill_between(x, rsi_vals, 70, where=(rsi_vals > 70),
                            color=_RSI_OB_COLOR, alpha=0.15, zorder=1)
        ax_rsi.fill_between(x, rsi_vals, 30, where=(rsi_vals < 30),
                            color=_RSI_OS_COLOR, alpha=0.15, zorder=1)

        xlim_rsi = ax_rsi.get_xlim()
        x_rsi_right = xlim_rsi[1] + 0.2
        ax_rsi.text(x_rsi_right, 70, "OB 70", color=_RSI_OB_COLOR,
                    fontsize=7, va="center", ha="left", clip_on=False)
        ax_rsi.text(x_rsi_right, 30, "OS 30", color=_RSI_OS_COLOR,
                    fontsize=7, va="center", ha="left", clip_on=False)
        ax_rsi.text(x_rsi_right, 50, "50", color="#555555",
                    fontsize=7, va="center", ha="left", clip_on=False)

        ax_rsi.legend(
            handles=[Line2D([0], [0], color="white", linewidth=0.9, label="RSI (14)")],
            loc="upper left", fontsize=7,
            facecolor=_LEGEND_BG, edgecolor=_SPINE_COLOR, framealpha=0.9,
        )

    # ------------------------------------------------------------------
    # E. MACD panel
    # ------------------------------------------------------------------
    if ax_macd is not None:
        # Recolor histogram bar patches
        bar_patches = [p for p in ax_macd.patches
                       if hasattr(p, "get_width") and p.get_width() > 0]
        for patch, val in zip(bar_patches, macd_hist.values):
            patch.set_facecolor(_BULL_COLOR if val >= 0 else _BEAR_COLOR)
            patch.set_alpha(0.6)

        ax_macd.axhline(0, color="#555555", linewidth=0.8, zorder=2)

        # Crossover annotations
        diff = (macd_line - macd_signal).values
        for i in range(1, len(diff)):
            prev, curr = diff[i - 1], diff[i]
            if np.isnan(prev) or np.isnan(curr):
                continue
            if prev < 0 and curr >= 0:
                cross_type = "bullish"
            elif prev > 0 and curr <= 0:
                cross_type = "bearish"
            else:
                continue
            color = _BULL_COLOR if cross_type == "bullish" else _BEAR_COLOR
            arrow_label = "↑" if cross_type == "bullish" else "↓"
            y_anchor = float(macd_line.iloc[i])
            y_offset = 8 if cross_type == "bullish" else -8
            ax_macd.annotate(
                arrow_label,
                xy=(i, y_anchor),
                xytext=(0, y_offset),
                textcoords="offset points",
                color=color,
                fontsize=8,
                ha="center",
                arrowprops=dict(arrowstyle="->", color=color, lw=0.8),
                zorder=5,
            )

        macd_handles = [
            Line2D([0], [0], color=_MACD_LINE_COLOR, linewidth=0.9, label="MACD (12/26)"),
            Line2D([0], [0], color=_MACD_SIGNAL_COLOR, linewidth=0.9, label="Signal (9)"),
        ]
        ax_macd.legend(
            handles=macd_handles, loc="upper left", fontsize=7,
            facecolor=_LEGEND_BG, edgecolor=_SPINE_COLOR, framealpha=0.9,
        )

    # ------------------------------------------------------------------
    # F. X-axis: hide on non-bottom panels, format bottom panel
    # ------------------------------------------------------------------
    for ax in axlist:
        if ax is not ax_macd:
            ax.tick_params(labelbottom=False)

    if ax_macd is not None:
        valid_ticks = [int(t) for t in ax_macd.get_xticks() if 0 <= t < n]
        if valid_ticks:
            ax_macd.set_xticks(valid_ticks)
            ax_macd.set_xticklabels(
                [ohlcv_df.index[t].strftime("%b %d") for t in valid_ticks],
                rotation=45, ha="right", color=_TICK_COLOR, fontsize=8,
            )


def generate_chart(
    db_conn: sqlite3.Connection,
    ticker: str,
    days: int,
    config: dict,
    calc_config: dict,
) -> str | None:
    """
    Generate a 4-panel technical chart PNG for a ticker and return its file path.

    Panel layout:
      0 (50%): Candlestick + EMA 9/21/50 + Bollinger Bands + Fibonacci + S/R
      1 (12%): Volume (green/red bars)
      2 (19%): RSI with 30/70 zones
      3 (19%): MACD line + signal + histogram

    Parameters:
        db_conn: Open SQLite connection.
        ticker: Ticker symbol.
        days: Number of trading days to plot.
        config: Notifier config dict containing config["detail_command"].
        calc_config: Calculator config dict containing config["fibonacci"].

    Returns:
        Absolute file path of the saved PNG, or None on failure.
    """
    detail_cfg = config.get("detail_command", {})
    figsize = detail_cfg.get("chart_figsize", [14, 10])
    max_sr = detail_cfg.get("sr_levels_to_show", 3)

    chart_data = load_chart_data(db_conn, ticker, days)
    ohlcv_df = chart_data["ohlcv"]

    if ohlcv_df.empty or len(ohlcv_df) < 2:
        logger.warning("ticker=%s phase=chart_generator insufficient data for chart", ticker)
        return None

    fib_result = compute_fibonacci_for_ticker(db_conn, ticker, calc_config)

    price_min = float(ohlcv_df["Low"].min())
    price_max = float(ohlcv_df["High"].max())
    current_price = float(ohlcv_df["Close"].iloc[-1])

    fib_hlines = prepare_fibonacci_hlines(fib_result, price_min, price_max)
    sr_hlines = prepare_sr_hlines(chart_data["sr_levels"], current_price, max_levels=max_sr)

    indicators_df = chart_data["indicators"]

    def _aligned_series(col: str, fill: float = 0.0) -> pd.Series:
        """Return indicator series aligned to ohlcv_df index, filling gaps."""
        if indicators_df.empty or col not in indicators_df.columns:
            return pd.Series([fill] * len(ohlcv_df), index=ohlcv_df.index)
        return indicators_df[col].reindex(ohlcv_df.index, method="ffill").fillna(fill)

    ema_9 = _aligned_series("ema_9", fill=current_price)
    ema_21 = _aligned_series("ema_21", fill=current_price)
    ema_50 = _aligned_series("ema_50", fill=current_price)
    bb_upper = _aligned_series("bb_upper", fill=price_max)
    bb_lower = _aligned_series("bb_lower", fill=price_min)
    rsi = _aligned_series("rsi_14", fill=50.0)
    macd_line = _aligned_series("macd_line", fill=0.0)
    macd_signal = _aligned_series("macd_signal", fill=0.0)
    macd_hist = _aligned_series("macd_histogram", fill=0.0)

    add_plots = [
        mpf.make_addplot(ema_9, color=_MA9_COLOR, width=0.9, panel=0),
        mpf.make_addplot(ema_21, color=_MA21_COLOR, width=0.9, panel=0),
        mpf.make_addplot(ema_50, color=_MA50_COLOR, width=0.9, panel=0),
        mpf.make_addplot(bb_upper, color=_BB_COLOR, width=0.6, linestyle="dashed", panel=0),
        mpf.make_addplot(bb_lower, color=_BB_COLOR, width=0.6, linestyle="dashed", panel=0),
        mpf.make_addplot(rsi, color="white", panel=2, ylabel="RSI"),
        mpf.make_addplot(macd_line, color=_MACD_LINE_COLOR, panel=3, ylabel="MACD"),
        mpf.make_addplot(macd_signal, color=_MACD_SIGNAL_COLOR, panel=3),
        mpf.make_addplot(macd_hist, type="bar", color=_BB_COLOR, alpha=0.4, panel=3),
    ]

    hlines_prices = [item["price"] for item in fib_hlines + sr_hlines]
    hlines_colors = [item["color"] for item in fib_hlines + sr_hlines]
    hlines_styles = [item["linestyle"] for item in fib_hlines + sr_hlines]

    hlines_cfg: dict = {}
    if hlines_prices:
        hlines_cfg = {
            "hlines": hlines_prices,
            "colors": hlines_colors,
            "linestyle": hlines_styles,
            "linewidths": [0.5] * len(hlines_prices),
        }

    style = _build_chart_style()

    timestamp = int(time.time())
    output_path = os.path.join(_CHART_DIR, f"ticker_tide_chart_{ticker}_{timestamp}.png")

    plot_kwargs: dict = {
        "type": "candle",
        "style": style,
        "addplot": add_plots,
        "volume": True,
        "volume_panel": 1,
        "panel_ratios": (50, 12, 19, 19),
        "figsize": tuple(figsize),
        "title": f"{ticker} — {days} Day Technical Analysis",
        "returnfig": True,
        "warn_too_much_data": len(ohlcv_df) + 1,
    }
    if hlines_cfg:
        plot_kwargs["hlines"] = hlines_cfg

    try:
        fig, axlist = mpf.plot(ohlcv_df, **plot_kwargs)
        _annotate_chart(
            fig, axlist, ohlcv_df,
            fib_hlines, sr_hlines,
            rsi, macd_hist, macd_line, macd_signal,
        )
        fig.savefig(output_path, bbox_inches="tight", dpi=150)
        plt.close(fig)
    except (ValueError, TypeError, OSError) as exc:
        logger.error("ticker=%s phase=chart_generator chart generation failed: %s", ticker, exc)
        return None

    logger.info("ticker=%s phase=chart_generator chart saved to %s", ticker, output_path)
    return output_path


def cleanup_chart(file_path: str) -> None:
    """
    Delete the chart PNG file after it has been sent via Telegram.

    Parameters:
        file_path: Absolute path to the PNG file to delete.

    Returns:
        None
    """
    try:
        os.remove(file_path)
        logger.info("phase=chart_generator cleaned up chart file %s", file_path)
    except FileNotFoundError:
        logger.debug("phase=chart_generator chart file already gone: %s", file_path)
    except Exception as exc:
        logger.warning("phase=chart_generator cleanup failed for %s: %s", file_path, exc)
