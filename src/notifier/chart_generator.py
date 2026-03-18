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
import pandas as pd
from matplotlib.lines import Line2D

from src.calculator.fibonacci import compute_fibonacci_for_ticker

logger = logging.getLogger(__name__)

_CHART_DIR = "/tmp"


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
    fib_hlines: list[dict],
    sr_hlines: list[dict],
) -> None:
    """
    Add legend entries and text annotations to the 4-panel chart figure.

    Operates on the matplotlib axes returned by mplfinance when returnfig=True.
    mplfinance creates twin axes for each panel, so the list length is typically
    double the number of panels. Panels are located by their y-axis label
    ("RSI", "MACD") rather than by index to be robust against version differences.

    Labels added:
      - Price panel legend: EMA 9 / EMA 21 / EMA 50 / BB
      - Price panel right-margin text: S/R and Fibonacci level labels
      - RSI panel inline text: "70" and "30" reference line labels
      - MACD panel legend: MACD / Signal

    Parameters:
        fig: The matplotlib Figure object.
        axlist: List of Axes as returned by mplfinance returnfig=True.
        fib_hlines: Fibonacci hline specs (price, label, color, linestyle).
        sr_hlines: S/R hline specs (price, label, color, linestyle).

    Returns:
        None
    """
    if not axlist:
        return

    ax_price = axlist[0]
    ax_rsi = next((ax for ax in axlist if "RSI" in ax.get_ylabel()), None)
    ax_macd = next((ax for ax in axlist if "MACD" in ax.get_ylabel()), None)

    price_legend_handles = [
        Line2D([0], [0], color="cyan", linewidth=0.8, label="EMA 9"),
        Line2D([0], [0], color="yellow", linewidth=0.8, label="EMA 21"),
        Line2D([0], [0], color="magenta", linewidth=0.8, label="EMA 50"),
        Line2D([0], [0], color="gray", linewidth=0.5, linestyle="dashed", label="BB"),
    ]
    ax_price.legend(handles=price_legend_handles, loc="upper left", fontsize=7, framealpha=0.3)

    xlim = ax_price.get_xlim()
    x_right = xlim[1]
    for item in sr_hlines + fib_hlines:
        ax_price.text(
            x_right, item["price"], f" {item['label']}",
            color=item["color"], fontsize=7, va="center", ha="left", clip_on=False,
        )

    if ax_rsi is not None:
        xlim_rsi = ax_rsi.get_xlim()
        x_left = xlim_rsi[0]
        ax_rsi.text(x_left, 70.5, " 70", color="dimgray", fontsize=7, va="bottom", ha="left")
        ax_rsi.text(x_left, 30.5, " 30", color="dimgray", fontsize=7, va="bottom", ha="left")

    if ax_macd is not None:
        macd_legend_handles = [
            Line2D([0], [0], color="cyan", label="MACD"),
            Line2D([0], [0], color="orange", label="Signal"),
        ]
        ax_macd.legend(handles=macd_legend_handles, loc="upper left", fontsize=7, framealpha=0.3)


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
    chart_style = detail_cfg.get("chart_style", "nightclouds")
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

    rsi_70 = pd.Series([70.0] * len(ohlcv_df), index=ohlcv_df.index)
    rsi_30 = pd.Series([30.0] * len(ohlcv_df), index=ohlcv_df.index)

    add_plots = [
        mpf.make_addplot(ema_9, color="cyan", width=0.8, panel=0),
        mpf.make_addplot(ema_21, color="yellow", width=0.8, panel=0),
        mpf.make_addplot(ema_50, color="magenta", width=0.8, panel=0),
        mpf.make_addplot(bb_upper, color="gray", width=0.5, linestyle="dashed", panel=0),
        mpf.make_addplot(bb_lower, color="gray", width=0.5, linestyle="dashed", panel=0),
        mpf.make_addplot(rsi, color="white", panel=2, ylabel="RSI"),
        mpf.make_addplot(rsi_70, color="dimgray", width=0.5, panel=2),
        mpf.make_addplot(rsi_30, color="dimgray", width=0.5, panel=2),
        mpf.make_addplot(macd_line, color="cyan", panel=3, ylabel="MACD"),
        mpf.make_addplot(macd_signal, color="orange", panel=3),
        mpf.make_addplot(macd_hist, type="bar", color="dimgray", panel=3),
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

    try:
        style = mpf.make_mpf_style(base_mpf_style=chart_style)
    except (ValueError, KeyError):
        style = mpf.make_mpf_style(base_mpf_style="nightclouds")

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
        _annotate_chart(fig, axlist, fib_hlines, sr_hlines)
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
