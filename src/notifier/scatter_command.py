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
    Query historical signals and compute the actual % price change N trading
    days after each signal date.

    Signals without OHLCV data on the signal date, or without a closing price
    N trading days later, are silently dropped.

    Direction is encoded as a signed confidence score (final_score / 100):
    negative for BEARISH, positive for BULLISH, near-zero for NEUTRAL.
    The forward return is the raw unmodified price change — no inversion applied.

    Parameters:
        conn: Open SQLite connection (row_factory=sqlite3.Row expected).
        n_days: Number of trading days ahead to look for the future close.
        ticker_filter: If provided, only signals for this ticker are included.
        days_back: How many calendar days of signal history to include.

    Returns:
        List of dicts, each with keys:
            ticker (str), signal_date (str), signal (str),
            confidence (float), signed_confidence (float), forward_return_pct (float).
    """
    cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()

    if ticker_filter:
        query = (
            "SELECT ticker, date, signal, confidence, final_score "
            "FROM scores_daily "
            "WHERE date >= ? AND ticker = ? AND signal IS NOT NULL "
            "ORDER BY ticker, date"
        )
        params: tuple = (cutoff_date, ticker_filter)
    else:
        query = (
            "SELECT ticker, date, signal, confidence, final_score "
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

        raw_return_pct = (future_close - signal_close) / signal_close * 100.0

        # Direction is encoded on the X-axis via signed_confidence; Y is raw return.
        signed_confidence = (final_score / 100.0) if final_score is not None else 0.0

        result.append(
            {
                "ticker": ticker,
                "signal_date": signal_date,
                "signal": signal,
                "confidence": confidence,
                "signed_confidence": round(signed_confidence, 4),
                "forward_return_pct": round(raw_return_pct, 4),
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
        _apply_axis_labels(ax, n_days, ticker_filter, days_back, len(data))

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
        f"Confidence vs {n_days}-Day Forward Return\n{scope} · last {days_back} days",
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

    ax.set_xlim(-1.05, 1.05)
    ax.set_xticks([-1, -0.5, 0, 0.5, 1])
    ax.set_xticklabels(["-1\n(Bearish)", "-0.5", "0\n(Neutral)", "+0.5", "+1\n(Bullish)"])


def _apply_axis_labels(
    ax: plt.Axes,
    n_days: int,
    ticker_filter: Optional[str],
    days_back: int,
    count: int,
) -> None:
    """Set axis labels and chart title."""
    scope = ticker_filter or "All tickers"
    ax.set_title(
        f"Confidence vs {n_days}-Day Forward Return\n{scope} · last {days_back} days · {count} signals",
        color="#e0e0ff",
        fontsize=13,
        pad=12,
    )
    ax.set_xlabel("Confidence Score  ←Bearish · Neutral · Bullish→", color="#aaaacc", fontsize=11)
    ax.set_ylabel(f"{n_days}-Day Forward Return (%)", color="#aaaacc", fontsize=11)


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
        f"📊 Confidence vs {n_days}-Day Forward Return\n"
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
