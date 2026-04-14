"""
/scatter command handler for the Telegram bot.

Usage: /scatter N [TICKER] [days_back]

  N          — number of trading days after the signal to measure price change
  TICKER     — optional symbol to filter (must be in active ticker list)
  days_back  — optional number of calendar days of signal history to use (default 90)

Queries historical signals from scores_daily, joins with ohlcv_daily to compute
the actual % price change N trading days after each signal was generated, then
renders a scatter plot of confidence (X) vs. forward return (Y).

For BEARISH signals the return is inverted so that a price drop counts as a
positive ("correct prediction") outcome.

Signals that do not yet have N future days of OHLCV data are silently dropped.

The chart is generated as a temporary PNG, sent to the Telegram chat, then deleted.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
from datetime import date, timedelta
from typing import Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from src.common.progress import send_telegram_message
from src.notifier.detail_command import send_photo_to_chat

matplotlib.use("Agg")

logger = logging.getLogger(__name__)

_PHASE = "scatter_command"

# ---------------------------------------------------------------------------
# Color palette — signal type → scatter dot color
# ---------------------------------------------------------------------------

_SIGNAL_COLORS: dict[str, str] = {
    "BULLISH": "#4caf50",
    "BEARISH": "#ef5350",
    "NEUTRAL": "#9e9e9e",
}

_SIGNAL_LABELS: dict[str, str] = {
    "BULLISH": "Bullish",
    "BEARISH": "Bearish",
    "NEUTRAL": "Neutral",
}

_USAGE_TEXT = (
    "📊 *Confidence vs Forward Return*\n\n"
    "Usage: `/scatter N [TICKER] [days_back]`\n"
    "• `N` — trading days to look forward (required)\n"
    "• `TICKER` — symbol filter (optional)\n"
    "• `days_back` — signal history window in days (optional, default 90)\n\n"
    "Examples:\n"
    "  `/scatter 10` — 10-day return, all tickers, last 90 days\n"
    "  `/scatter 5 AAPL` — 5-day return for AAPL only\n"
    "  `/scatter 20 AAPL 180` — 20-day return, AAPL, last 180 days"
)


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------

def parse_scatter_command(
    message_text: str,
    active_tickers: list[dict],
    config: dict,
) -> dict:
    """
    Parse the /scatter command text and validate inputs.

    Accepts:
      /scatter N
      /scatter N TICKER
      /scatter N days_back
      /scatter N TICKER days_back

    Token disambiguation: if the second token is purely numeric it is treated
    as days_back; if it matches a known ticker symbol it is treated as TICKER;
    otherwise it is invalid.

    Parameters:
        message_text: Raw Telegram message text (e.g. "/scatter 10 AAPL 180").
        active_tickers: List of active ticker dicts each with a 'symbol' key.
        config: Notifier config dict containing config["scatter_command"].

    Returns:
        dict with keys: n_days (int), ticker (Optional[str]), days_back (int).

    Raises:
        ValueError: If N is missing, non-numeric, or TICKER is not in active_tickers.
    """
    scatter_cfg = config["scatter_command"]
    max_n = scatter_cfg["max_n_days"]
    default_days_back = scatter_cfg["default_days_back"]
    max_days_back = scatter_cfg["max_days_back"]

    valid_symbols = {t["symbol"].upper() for t in active_tickers}

    tokens = message_text.strip().split()
    # tokens[0] is the command (/scatter), tokens[1:] are args
    args = tokens[1:]

    if not args:
        raise ValueError("N (number of forward trading days) is required.")

    # Parse N
    try:
        n_days = int(args[0])
    except ValueError:
        raise ValueError(f"N must be an integer, got '{args[0]}'.")

    n_days = max(1, min(n_days, max_n))

    ticker: Optional[str] = None
    days_back: int = default_days_back

    if len(args) >= 2:
        second = args[1].upper()
        if second in valid_symbols:
            ticker = second
            if len(args) >= 3:
                try:
                    days_back = int(args[2])
                except ValueError:
                    raise ValueError(f"days_back must be an integer, got '{args[2]}'.")
        else:
            # Must be numeric (days_back) or invalid
            try:
                days_back = int(args[1])
            except ValueError:
                raise ValueError(
                    f"'{args[1]}' is not a recognized ticker symbol. "
                    f"Valid symbols: {sorted(valid_symbols)}"
                )

    days_back = max(1, min(days_back, max_days_back))

    return {"n_days": n_days, "ticker": ticker, "days_back": days_back}


# ---------------------------------------------------------------------------
# Forward return calculation
# ---------------------------------------------------------------------------

def fetch_signals_with_forward_returns(
    conn: sqlite3.Connection,
    n_days: int,
    ticker_filter: Optional[str],
    days_back: int,
) -> list[dict]:
    """
    Query historical signals and compute the actual excess return (vs SPY)
    N trading days after each signal date.

    Uses calibrated_score (predicted excess return from ridge regression) for
    the X-axis when available. Falls back to final_score / 100 for signals
    that predate calibration.

    Signals without OHLCV data on the signal date, or without a closing price
    N trading days later, are silently dropped.

    Parameters:
        conn: Open SQLite connection (row_factory=sqlite3.Row expected).
        n_days: Number of trading days ahead to look for the future close.
        ticker_filter: If provided, only signals for this ticker are included.
        days_back: How many calendar days of signal history to include.

    Returns:
        List of dicts, each with keys:
            ticker (str), signal_date (str), signal (str),
            confidence (float), signed_confidence (float),
            forward_return_pct (float), model_r2 (float).
    """
    cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()

    if ticker_filter:
        query = (
            "SELECT ticker, date, signal, confidence, final_score, "
            "calibrated_score, model_r2 "
            "FROM scores_daily "
            "WHERE date >= ? AND ticker = ? AND signal IS NOT NULL "
            "ORDER BY ticker, date"
        )
        params: tuple = (cutoff_date, ticker_filter)
    else:
        query = (
            "SELECT ticker, date, signal, confidence, final_score, "
            "calibrated_score, model_r2 "
            "FROM scores_daily "
            "WHERE date >= ? AND signal IS NOT NULL "
            "ORDER BY ticker, date"
        )
        params = (cutoff_date,)

    signal_rows = conn.execute(query, params).fetchall()

    result: list[dict] = []

    for row in signal_rows:
        ticker = row["ticker"]
        signal_date = row["date"]
        signal = row["signal"]
        confidence = row["confidence"]
        final_score = row["final_score"]
        cal_score = row["calibrated_score"]
        r2 = row["model_r2"]

        if confidence is None:
            continue

        # Close price on signal date
        signal_close_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
            (ticker, signal_date),
        ).fetchone()
        if not signal_close_row or signal_close_row["close"] is None:
            continue
        signal_close: float = signal_close_row["close"]
        if signal_close == 0:
            continue

        # Close price of the Nth trading day after signal_date (OFFSET is 0-based)
        future_close_row = conn.execute(
            "SELECT close FROM ohlcv_daily "
            "WHERE ticker = ? AND date > ? "
            "ORDER BY date "
            "LIMIT 1 OFFSET ?",
            (ticker, signal_date, n_days - 1),
        ).fetchone()
        if not future_close_row or future_close_row["close"] is None:
            continue
        future_close: float = future_close_row["close"]

        ticker_return_pct = (future_close - signal_close) / signal_close * 100.0

        # Compute SPY's return over the same period for excess return
        spy_sig_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = 'SPY' AND date = ?",
            (signal_date,),
        ).fetchone()
        spy_fwd_row = conn.execute(
            "SELECT close FROM ohlcv_daily "
            "WHERE ticker = 'SPY' AND date > ? "
            "ORDER BY date LIMIT 1 OFFSET ?",
            (signal_date, n_days - 1),
        ).fetchone()

        if spy_sig_row and spy_fwd_row and spy_sig_row["close"] and spy_sig_row["close"] > 0:
            spy_return = (spy_fwd_row["close"] - spy_sig_row["close"]) / spy_sig_row["close"] * 100.0
            excess_return_pct = ticker_return_pct - spy_return
        else:
            excess_return_pct = ticker_return_pct

        # X-axis: calibrated_score when available, else fall back to final_score / 100
        if cal_score is not None:
            signed_confidence = float(cal_score)
        else:
            signed_confidence = (final_score / 100.0) if final_score is not None else 0.0

        result.append(
            {
                "ticker": ticker,
                "signal_date": signal_date,
                "signal": signal,
                "confidence": confidence,
                "signed_confidence": round(signed_confidence, 4),
                "forward_return_pct": round(excess_return_pct, 4),
                "model_r2": float(r2) if r2 is not None else 0.0,
            }
        )

    logger.info(
        "phase=%s n_days=%d ticker_filter=%s days_back=%d signals_with_returns=%d dropped=%d",
        _PHASE,
        n_days,
        ticker_filter or "all",
        days_back,
        len(result),
        len(signal_rows) - len(result),
    )

    return result


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

def generate_scatter_chart(
    data: list[dict],
    n_days: int,
    ticker_filter: Optional[str],
    days_back: int,
) -> str:
    """
    Render a confidence vs. forward-return scatter plot as a dark-mode PNG.

    Plots each signal as a dot (signed_confidence on X, raw forward return on Y),
    colored by signal type. A linear regression line is drawn per signal type
    when that type has at least 2 data points. Horizontal and vertical reference
    lines mark y=0 and x=0 respectively.

    Parameters:
        data: List of dicts from fetch_signals_with_forward_returns.
        n_days: Forward horizon used for labelling.
        ticker_filter: Ticker symbol if filtered, else None (affects title).
        days_back: Signal history window used for labelling.

    Returns:
        Absolute path to the saved PNG file (caller is responsible for cleanup).
    """
    fig, ax = plt.subplots(figsize=(10, 7))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    ax.axhline(y=0, color="#555577", linewidth=0.8, linestyle="--", zorder=1)
    ax.axvline(x=0, color="#555577", linewidth=0.8, linestyle="--", zorder=1)

    if not data:
        _render_empty_chart(ax, n_days, ticker_filter, days_back)
    else:
        _render_scatter_with_regression(ax, data, n_days)
        _apply_axis_labels(ax, n_days, ticker_filter, days_back, len(data), data=data)

    _apply_common_style(ax)

    tmp_file = tempfile.NamedTemporaryFile(
        prefix="scatter_", suffix=".png", delete=False, dir="/tmp"
    )
    tmp_path = tmp_file.name
    tmp_file.close()

    fig.savefig(tmp_path, dpi=120, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    logger.info("phase=%s chart saved to %s", _PHASE, tmp_path)
    return tmp_path


def _render_empty_chart(
    ax: plt.Axes,
    n_days: int,
    ticker_filter: Optional[str],
    days_back: int,
) -> None:
    """Render a placeholder message when there is no data."""
    ax.text(
        0.5,
        0.5,
        "No signals with sufficient\nforward price data found.",
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=13,
        color="#aaaacc",
    )
    scope = ticker_filter or "All tickers"
    ax.set_title(
        f"Predicted vs Actual Excess Return ({n_days}d)\n{scope} · last {days_back} days",
        color="#e0e0ff",
        fontsize=13,
        pad=12,
    )


def _render_scatter_with_regression(
    ax: plt.Axes,
    data: list[dict],
    n_days: int,
) -> None:
    """Plot scatter dots and per-signal-type regression lines."""
    by_signal: dict[str, tuple[list[float], list[float]]] = {
        sig: ([], []) for sig in ("BULLISH", "BEARISH", "NEUTRAL")
    }

    for row in data:
        sig = row["signal"]
        if sig in by_signal:
            by_signal[sig][0].append(row["signed_confidence"])
            by_signal[sig][1].append(row["forward_return_pct"])

    for signal_type, (xs, ys) in by_signal.items():
        if not xs:
            continue
        color = _SIGNAL_COLORS[signal_type]
        label = _SIGNAL_LABELS[signal_type]
        ax.scatter(xs, ys, c=color, label=label, alpha=0.65, s=40, zorder=3)

        if len(xs) >= 2:
            x_arr = np.array(xs)
            y_arr = np.array(ys)
            coeffs = np.polyfit(x_arr, y_arr, 1)
            x_line = np.linspace(x_arr.min(), x_arr.max(), 100)
            y_line = np.polyval(coeffs, x_line)
            ax.plot(x_line, y_line, color=color, linewidth=1.5, alpha=0.8, zorder=4)

    # Auto-scale X-axis; add 45-degree reference line (perfect prediction)
    all_x = [row["signed_confidence"] for row in data]
    all_y = [row["forward_return_pct"] for row in data]
    if all_x and all_y:
        xy_min = min(min(all_x), min(all_y))
        xy_max = max(max(all_x), max(all_y))
        ref_range = np.linspace(xy_min, xy_max, 100)
        ax.plot(ref_range, ref_range, color="#555577", linewidth=1.0, linestyle=":",
                alpha=0.6, zorder=2, label="Perfect prediction")


def _apply_axis_labels(
    ax: plt.Axes,
    n_days: int,
    ticker_filter: Optional[str],
    days_back: int,
    count: int,
    data: Optional[list[dict]] = None,
) -> None:
    """Set axis labels and chart title with R² annotation."""
    scope = ticker_filter or "All tickers"

    # Compute overall correlation R between predicted and actual
    r_text = ""
    if data and len(data) >= 2:
        xs = np.array([row["signed_confidence"] for row in data])
        ys = np.array([row["forward_return_pct"] for row in data])
        if np.std(xs) > 0 and np.std(ys) > 0:
            correlation = float(np.corrcoef(xs, ys)[0, 1])
            r_text = f" · R={correlation:.2f}"

    ax.set_title(
        f"Predicted vs Actual Excess Return ({n_days}d)\n"
        f"{scope} · last {days_back} days · {count} signals{r_text}",
        color="#e0e0ff",
        fontsize=13,
        pad=12,
    )
    ax.set_xlabel("Predicted Excess Return (%)", color="#aaaacc", fontsize=11)
    ax.set_ylabel(f"Actual {n_days}-Day Excess Return (%)", color="#aaaacc", fontsize=11)


def _apply_common_style(ax: plt.Axes) -> None:
    """Apply dark-mode styling to tick marks, grid, spines, and legend."""
    ax.tick_params(colors="#aaaacc")
    ax.grid(True, color="#2a2a4a", linewidth=0.5, zorder=0)
    for spine in ax.spines.values():
        spine.set_edgecolor("#3a3a5a")

    legend = ax.legend(
        facecolor="#2a2a4a",
        edgecolor="#3a3a5a",
        labelcolor="#e0e0ff",
        fontsize=10,
        loc="upper left",
    )
    handles, _ = ax.get_legend_handles_labels()
    if legend and not handles:
        legend.remove()


# ---------------------------------------------------------------------------
# Public command handler
# ---------------------------------------------------------------------------

def handle_scatter_command(
    conn: sqlite3.Connection,
    chat_id: str,
    message_text: str,
    bot_token: str,
    config: dict,
    active_tickers: list[dict],
) -> None:
    """
    Orchestrate the full /scatter command flow.

    Parses the command, fetches forward returns from the database, generates
    the scatter chart, sends it to Telegram, and cleans up the temporary file.

    On parse errors an explanatory usage message is sent instead of a chart.

    Parameters:
        conn: Open SQLite connection (WAL mode, row_factory=sqlite3.Row).
        chat_id: Telegram chat ID to reply to.
        message_text: Full incoming message text (e.g. "/scatter 10 AAPL 90").
        bot_token: Telegram Bot API token.
        config: Notifier config dict containing config["scatter_command"].
        active_tickers: List of active ticker dicts (each has 'symbol' key).

    Returns:
        None
    """
    # --- Parse ---
    try:
        parsed = parse_scatter_command(message_text, active_tickers, config)
    except ValueError as exc:
        logger.warning("phase=%s parse error: %s", _PHASE, exc)
        send_telegram_message(
            bot_token,
            chat_id,
            f"❌ {exc}\n\n{_USAGE_TEXT}",
            parse_mode="Markdown",
        )
        return

    n_days = parsed["n_days"]
    ticker_filter = parsed["ticker"]
    days_back = parsed["days_back"]

    logger.info(
        "phase=%s chat_id=%s n_days=%d ticker=%s days_back=%d",
        _PHASE,
        chat_id,
        n_days,
        ticker_filter or "all",
        days_back,
    )

    # --- Fetch data ---
    try:
        data = fetch_signals_with_forward_returns(conn, n_days, ticker_filter, days_back)
    except sqlite3.Error as exc:
        logger.error("phase=%s db error: %s", _PHASE, exc)
        send_telegram_message(bot_token, chat_id, "❌ Database error generating scatter plot.")
        return

    # --- Generate chart ---
    try:
        chart_path = generate_scatter_chart(data, n_days, ticker_filter, days_back)
    except Exception as exc:
        logger.error("phase=%s chart generation failed: %s", _PHASE, exc)
        send_telegram_message(bot_token, chat_id, "❌ Failed to generate scatter chart.")
        return

    # --- Build caption ---
    scope = ticker_filter or "all tickers"
    count_label = f"{len(data)} signal{'s' if len(data) != 1 else ''}"
    caption = (
        f"📊 Predicted vs Actual Excess Return ({n_days}d)\n"
        f"{scope} · last {days_back} days · {count_label}"
    )

    # --- Send and clean up ---
    try:
        sent = send_photo_to_chat(bot_token, chat_id, chart_path, caption=caption)
        if not sent:
            logger.error("phase=%s send_photo_to_chat returned False", _PHASE)
            send_telegram_message(bot_token, chat_id, "❌ Failed to send scatter chart.")
    except Exception as exc:
        logger.error("phase=%s send_photo failed: %s", _PHASE, exc)
        send_telegram_message(bot_token, chat_id, "❌ Failed to send scatter chart.")
    finally:
        if os.path.exists(chart_path):
            try:
                os.unlink(chart_path)
            except OSError as exc:
                logger.warning("phase=%s could not delete temp chart %s: %s", _PHASE, chart_path, exc)
