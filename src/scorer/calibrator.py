"""
Rolling ridge regression calibrator for signal scoring.

Replaces the static composite score with a data-driven prediction of
expected excess return. Trains a ridge regression on a rolling window
of recent signals and their realized 10-day excess returns (vs SPY),
then uses the learned weights to predict the current signal's expected
return.

Features (17 total):
  6 category scores  — trend, momentum, volume, volatility, fundamental, macro
  6 raw indicators   — RSI, ADX, MACD histogram, Stochastic %K, BB %B, CMF
  3 EMA positions    — price-EMA9, EMA9-EMA21, EMA21-EMA50 spreads (%)
  1 weekly score     — prior-week composite score (same scale as category scores)
  1 monthly score    — prior-month composite score (same scale as category scores)

Config keys (under scorer.json → "calibration"):
  enabled             — master switch (bool)
  window_size         — number of calendar days to look back for training signals (int)
  ridge_lambda        — L2 regularisation strength (float)
  min_training_samples — minimum samples required; fewer triggers cold-start fallback (int)
  benchmark_ticker    — ticker whose return is subtracted from each signal's return (str)
  forward_days        — trading-day horizon for measuring forward returns (int)
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

import numpy as np
from datetime import date, timedelta

logger = logging.getLogger(__name__)

_PHASE = "calibrator"

# Feature order — must match build_feature_vector() and fetch_training_data()
FEATURE_NAMES: list[str] = [
    "trend_score", "momentum_score", "volume_score",
    "volatility_score", "fundamental_score", "macro_score",
    "rsi_14", "adx", "macd_histogram", "stoch_k", "bb_pctb", "cmf_20",
    "price_ema9_spread", "ema9_ema21_spread", "ema21_ema50_spread",
    "weekly_score",
    "monthly_score",
]

# Defaults applied when the calibration sub-config is missing keys. The
# production source of truth is config/scorer.json → calibration.
DEFAULT_RIDGE_LAMBDA: float = 0.1
DEFAULT_MIN_TRAINING_SAMPLES: int = 30


def build_shrinkage_lambdas(production_lambda: float) -> list[float]:
    """
    Build the log-spaced λ grid for the ridge shrinkage path.

    Forces `production_lambda` onto the grid as an exact member so the
    chart's ReferenceLine and the sidebar's `lambdas.indexOf(...)` lookup
    align with a real data point regardless of which production λ the
    calibration config specifies.

    Parameters:
        production_lambda: The production ridge λ from the calibration
                           sub-config. Must be > 0.

    Returns:
        Sorted list of 50 unique λ values, log-spaced between 1e-4 and
        1e+4, with `production_lambda` forced onto the grid.
    """
    base = np.logspace(-4, 4, 49).tolist()
    return sorted(set(base + [float(production_lambda)]))


# Grid built with the default production λ. Kept for callers that don't
# have the calibration config in scope (e.g. tests). Endpoint callers
# must rebuild with the actual configured ridge_lambda — see
# fetch_shrinkage_path in src/web/queries.py.
DEFAULT_SHRINKAGE_LAMBDAS: list[float] = build_shrinkage_lambdas(DEFAULT_RIDGE_LAMBDA)

# Display label and category for each of the 17 features. Categories
# match `category_scorer._INDICATOR_CATEGORY_MAP` for raw indicators;
# synthetic features (EMA spreads, weekly/monthly scores) are assigned
# by analogy and documented here.
FEATURE_METADATA: dict[str, dict[str, str]] = {
    "trend_score":        {"label": "Trend score",       "category": "trend"},
    "momentum_score":     {"label": "Momentum score",    "category": "momentum"},
    "volume_score":       {"label": "Volume score",      "category": "volume"},
    "volatility_score":   {"label": "Volatility score",  "category": "volatility"},
    "fundamental_score":  {"label": "Fundamental score", "category": "fundamental"},
    "macro_score":        {"label": "Macro score",       "category": "macro"},
    "rsi_14":             {"label": "RSI 14",            "category": "momentum"},
    "adx":                {"label": "ADX",               "category": "trend"},   # per _INDICATOR_CATEGORY_MAP
    "macd_histogram":     {"label": "MACD hist",         "category": "trend"},      # per INDICATOR_CATEGORY_MAP
    "stoch_k":            {"label": "Stoch %K",          "category": "momentum"},
    "bb_pctb":            {"label": "BB %B",             "category": "volatility"},
    "cmf_20":             {"label": "CMF 20",            "category": "volume"},
    "price_ema9_spread":  {"label": "Px−EMA9 %",   "category": "trend"},   # EMA spreads track trend
    "ema9_ema21_spread":  {"label": "EMA9−21 %",   "category": "trend"},
    "ema21_ema50_spread": {"label": "EMA21−50 %",  "category": "trend"},
    "weekly_score":       {"label": "Weekly score",      "category": "temporal"},  # synthetic temporal feature
    "monthly_score":      {"label": "Monthly score",     "category": "temporal"},
}


# ---------------------------------------------------------------------------
# Feature construction
# ---------------------------------------------------------------------------

def build_feature_vector(
    category_scores: dict,
    raw_indicators: dict,
    ema_positions: dict,
    weekly_score: Optional[float] = None,
    monthly_score: Optional[float] = None,
) -> list[float]:
    """
    Assemble a 17-element feature vector from scored categories, raw
    indicator values, EMA position spreads, the weekly composite score,
    and the monthly composite score.

    None values are replaced with 0.0 so the model always receives a
    complete numeric vector.

    Parameters:
        category_scores: Dict with keys trend, momentum, volume, volatility,
                         fundamental, macro — each a float or None.
        raw_indicators:  Dict with keys rsi_14, adx, macd_histogram, stoch_k,
                         bb_pctb, cmf_20 — each a float or None.
        ema_positions:   Dict with keys price_ema9_spread, ema9_ema21_spread,
                         ema21_ema50_spread — each a float.
        weekly_score:    Prior-week composite score (same scale as category
                         scores, not divided by 100). None → 0.0.
        monthly_score:   Prior-month composite score (same scale as category
                         scores, not divided by 100). None → 0.0.

    Returns:
        List of 17 floats in the canonical feature order.
    """
    def _safe(val: Optional[float]) -> float:
        return float(val) if val is not None else 0.0

    return [
        _safe(category_scores.get("trend")),
        _safe(category_scores.get("momentum")),
        _safe(category_scores.get("volume")),
        _safe(category_scores.get("volatility")),
        _safe(category_scores.get("fundamental")),
        _safe(category_scores.get("macro")),
        _safe(raw_indicators.get("rsi_14")),
        _safe(raw_indicators.get("adx")),
        _safe(raw_indicators.get("macd_histogram")),
        _safe(raw_indicators.get("stoch_k")),
        _safe(raw_indicators.get("bb_pctb")),
        _safe(raw_indicators.get("cmf_20")),
        _safe(ema_positions.get("price_ema9_spread")),
        _safe(ema_positions.get("ema9_ema21_spread")),
        _safe(ema_positions.get("ema21_ema50_spread")),
        _safe(weekly_score),
        _safe(monthly_score),
    ]


# ---------------------------------------------------------------------------
# Excess return
# ---------------------------------------------------------------------------

def compute_excess_return(
    ticker_return_pct: float,
    benchmark_return_pct: Optional[float],
) -> float:
    """
    Compute the excess return of a ticker over a benchmark.

    Parameters:
        ticker_return_pct:    The ticker's N-day forward return (%).
        benchmark_return_pct: The benchmark's N-day forward return (%), or None.

    Returns:
        Excess return in percent. Falls back to raw ticker return if
        benchmark is None.
    """
    if benchmark_return_pct is None:
        return ticker_return_pct
    return ticker_return_pct - benchmark_return_pct


# ---------------------------------------------------------------------------
# Ridge regression
# ---------------------------------------------------------------------------

def train_ridge_and_predict(
    X_train: np.ndarray,
    y_train: np.ndarray,
    x_new: np.ndarray,
    ridge_lambda: float = 0.1,
) -> dict:
    """
    Train a ridge regression on (X_train, y_train) and predict for x_new.

    Appends an intercept column to X_train automatically.
    Returns prediction=0.0 and model_r2=0.0 when X_train has fewer than
    2 rows (not enough data to fit a meaningful model).

    Parameters:
        X_train: (n_samples, n_features) training feature matrix.
        y_train: (n_samples,) target vector of excess returns.
        x_new:   (n_features,) feature vector for the current signal.
        ridge_lambda: L2 regularisation strength.

    Returns:
        Dict with keys:
            prediction (float) — predicted excess return for x_new
            model_r2   (float) — in-sample R² of the training fit
            weights    (list[float]) — learned coefficients incl. intercept
    """
    n_samples = X_train.shape[0]
    if n_samples < 2:
        n_feats = x_new.shape[0] if x_new.ndim > 0 else 0
        return {
            "prediction": 0.0,
            "model_r2": 0.0,
            "weights": [0.0] * (n_feats + 1),
            "col_mean": [0.0] * n_feats,
            "col_std": [1.0] * n_feats,
            "x_new_scaled": [0.0] * n_feats,
        }

    # Suppress spurious BLAS warnings from Apple Accelerate on macOS.
    # The matmul results are correct; the Accelerate backend triggers
    # false-positive divide/overflow/invalid FP exceptions.
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        return _fit_ridge(X_train, y_train, x_new, ridge_lambda, n_samples)


def _fit_ridge(
    X_train: np.ndarray,
    y_train: np.ndarray,
    x_new: np.ndarray,
    ridge_lambda: float,
    n_samples: int,
) -> dict:
    """Internal: standardise, solve ridge via augmented least-squares, predict."""
    # Standardise features
    col_mean = X_train.mean(axis=0)
    col_std = X_train.std(axis=0)
    col_std[col_std == 0] = 1.0  # avoid divide-by-zero for constant features
    X_scaled = (X_train - col_mean) / col_std
    x_new_scaled = (x_new - col_mean) / col_std

    # Append intercept column
    ones_train = np.ones((n_samples, 1))
    X_aug = np.hstack([X_scaled, ones_train])

    # Ridge via augmented least-squares (more stable than normal equations)
    n_cols = X_aug.shape[1]
    sqrt_lambda = np.sqrt(ridge_lambda)
    reg_rows = sqrt_lambda * np.eye(n_cols)
    reg_rows[-1, -1] = 0.0  # don't regularise the intercept
    X_ridge = np.vstack([X_aug, reg_rows])
    y_ridge = np.concatenate([y_train, np.zeros(n_cols)])

    weights, _, _, _ = np.linalg.lstsq(X_ridge, y_ridge, rcond=None)

    # Predict for x_new
    x_new_aug = np.append(x_new_scaled, 1.0)
    prediction = float(x_new_aug @ weights)

    # In-sample R² (on original data, not augmented)
    y_pred = X_aug @ weights
    ss_res = float(np.sum((y_train - y_pred) ** 2))
    ss_tot = float(np.sum((y_train - y_train.mean()) ** 2))
    model_r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    model_r2 = max(0.0, model_r2)  # clamp negative R² to 0

    return {
        "prediction": prediction,
        "model_r2": model_r2,
        "weights": weights.tolist(),
        "col_mean": col_mean.tolist(),
        "col_std": col_std.tolist(),
        "x_new_scaled": x_new_scaled.tolist(),
    }


# ---------------------------------------------------------------------------
# Shrinkage path
# ---------------------------------------------------------------------------

def compute_shrinkage_path(
    X_train: np.ndarray,
    y_train: np.ndarray,
    lambdas: list[float],
) -> np.ndarray:
    """
    Compute the ridge regression shrinkage path across a grid of λ values.

    For each λ in the grid, fits a ridge regression on (X_train, y_train) and
    records the standardised-space coefficients for the 17 features (intercept
    excluded). The result is the (n_lambdas, 17) matrix of coefficients, where
    larger λ shrinks all coefficients toward zero.

    Uses _fit_ridge internally with x_new=zeros so the prediction step is a
    no-op — only the weight vector matters here.

    Parameters:
        X_train: (n_samples, n_features) training feature matrix, where n_features
                 must equal len(FEATURE_NAMES) == 17.
        y_train: (n_samples,) target vector of excess returns.
        lambdas: Ordered list of positive λ values to evaluate. Typical usage
                 passes DEFAULT_SHRINKAGE_LAMBDAS.

    Returns:
        (n_lambdas, 17) numpy array of standardised-space feature coefficients.
        Row i corresponds to lambdas[i]; intercept column is dropped.
    """
    n_features = len(FEATURE_NAMES)
    n_samples = X_train.shape[0]
    x_new_zeros = np.zeros(n_features)
    path = np.empty((len(lambdas), n_features))

    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        for idx, lam in enumerate(lambdas):
            result = _fit_ridge(X_train, y_train, x_new_zeros, lam, n_samples)
            weights = result["weights"]
            assert len(weights) == n_features + 1, (
                f"compute_shrinkage_path: expected {n_features + 1} weights, "
                f"got {len(weights)} — FEATURE_NAMES / _fit_ridge drift detected"
            )
            path[idx, :] = weights[:n_features]

    return path


# ---------------------------------------------------------------------------
# Calibrator decomposition payload
# ---------------------------------------------------------------------------

def build_calibrator_payload(
    feature_names: list[str],
    x_new_raw: list[float],
    col_mean: list[float],
    col_std: list[float],
    feature_weights: list[float],
    intercept: float,
    prediction: float,
    model_r2: float,
    n_training_samples: int,
) -> dict:
    """
    Build a per-feature decomposition payload explaining a ridge regression prediction.

    The identity implemented here is:
        prediction ≈ intercept + Σ (weight_i × z_i)
        where z_i = (x_new_raw[i] − col_mean[i]) / col_std[i]

    The ``feature_weights`` argument must contain exactly len(feature_names)
    weights — i.e. feature_weights[:len(FEATURE_NAMES)] from the ridge output.
    The ridge intercept term (at index -1 of the full weights vector) must NOT
    be passed here; it is passed separately via the ``intercept`` argument.

    Parameters:
        feature_names:     Ordered list of feature names (length N).
        x_new_raw:         Raw (unstandardised) feature values for the current
                           signal (length N).
        col_mean:          Training-window column means used for standardisation
                           (length N).
        col_std:           Training-window column stds used for standardisation,
                           with zeros already replaced by 1.0 (length N).
        feature_weights:   Ridge coefficients for the N features only — must
                           NOT include the intercept element (length N).
        intercept:         Ridge intercept coefficient (the -1 element from the
                           full weights vector).
        prediction:        The calibrated score (output of the ridge model).
        model_r2:          In-sample R² of the training fit.
        n_training_samples: Number of rows used to train this model.

    Returns:
        Dict with keys:
            intercept (float)
            prediction (float)
            training_samples (int)
            in_sample_r2 (float)
            feature_count (int)  — derived from len(contributions); NEVER a literal.
            contributions (list[dict]) — each with keys:
                name, raw, mean, std, z, weight, contribution.
        Invariant: abs(intercept + sum(c["contribution"] for c in contributions) - prediction) < 1e-6
        (holds when the prediction was computed from the same x_new_raw values).
    """
    n = len(feature_names)
    if not (len(x_new_raw) == len(col_mean) == len(col_std) == len(feature_weights) == n):
        raise ValueError(
            f"build_calibrator_payload length mismatch: feature_names={n}, "
            f"x_new_raw={len(x_new_raw)}, col_mean={len(col_mean)}, "
            f"col_std={len(col_std)}, feature_weights={len(feature_weights)}"
        )

    contributions = []
    for i in range(n):
        z_value = (x_new_raw[i] - col_mean[i]) / col_std[i]
        contrib_value = z_value * feature_weights[i]
        contributions.append({
            "name": feature_names[i],
            "raw": x_new_raw[i],
            "mean": col_mean[i],
            "std": col_std[i],
            "z": z_value,
            "weight": feature_weights[i],
            "contribution": contrib_value,
        })

    return {
        "intercept": intercept,
        "prediction": prediction,
        "training_samples": n_training_samples,
        "in_sample_r2": model_r2,
        "feature_count": len(contributions),
        "contributions": contributions,
    }


# ---------------------------------------------------------------------------
# Training data fetch
# ---------------------------------------------------------------------------

def fetch_training_data(
    conn: sqlite3.Connection,
    scoring_date: str,
    config: dict,
    excluded_tickers: Optional[set[str]] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Fetch historical signals with their 17-feature vectors and realized
    excess returns to serve as training data for the ridge regression.

    Queries scored signals within the past `window_size` calendar days (across all
    tickers) that have both:
      - A known forward close price N trading days after the signal
      - A known SPY close price for the same forward period

    Tickers in `excluded_tickers` are skipped entirely. Pass the result of
    ``get_training_excluded_tickers()`` here to omit sector ETFs, market
    benchmarks, and index ETFs — basket products whose feature-return
    relationships differ from individual stocks and distort the model.

    Parameters:
        conn:              Open SQLite connection with row_factory=sqlite3.Row.
        scoring_date:      The current scoring date (YYYY-MM-DD). Only signals
                           before this date are eligible for training.
        config:            Calibration config dict.
        excluded_tickers:  Optional set of ticker symbols to exclude from the
                           training window. None or empty set excludes nothing.

    Returns:
        (X, y) tuple where X is (n_samples, 17) feature matrix and
        y is (n_samples,) vector of excess returns. Both may be empty
        if no training data is available.
    """
    window_size = config.get("window_size", 90)
    forward_days = config.get("forward_days", 10)
    benchmark = config.get("benchmark_ticker", "SPY")

    # Compute the calendar-day cutoff: only signals within the last window_size days
    scoring_dt = date.fromisoformat(scoring_date)
    cutoff_date = (scoring_dt - timedelta(days=window_size)).isoformat()

    # Build the query; add NOT IN clause only when there are tickers to exclude
    base_sql = """
        SELECT s.ticker, s.date,
               s.trend_score, s.momentum_score, s.volume_score,
               s.volatility_score, s.fundamental_score, s.macro_score,
               s.weekly_score, s.monthly_score,
               i.rsi_14, i.adx, i.macd_histogram, i.stoch_k, i.bb_pctb, i.cmf_20,
               i.ema_9, i.ema_21, i.ema_50,
               o_sig.close AS signal_close
        FROM scores_daily s
        JOIN indicators_daily i ON s.ticker = i.ticker AND s.date = i.date
        JOIN ohlcv_daily o_sig ON s.ticker = o_sig.ticker AND s.date = o_sig.date
        WHERE s.date < ?
          AND s.date >= ?
          AND s.signal IS NOT NULL
          AND o_sig.close > 0"""

    params: list = [scoring_date, cutoff_date]
    if excluded_tickers:
        placeholders = ",".join("?" * len(excluded_tickers))
        base_sql += f"\n          AND s.ticker NOT IN ({placeholders})"
        params.extend(sorted(excluded_tickers))

    base_sql += "\n        ORDER BY s.date DESC"

    # Fetch scored signals within the calendar window, across all tickers
    rows = conn.execute(base_sql, params).fetchall()

    if not rows:
        return np.empty((0, 17)), np.empty(0)

    X_list: list[list[float]] = []
    y_list: list[float] = []

    for row in rows:
        ticker = row["ticker"]
        signal_date = row["date"]
        signal_close = row["signal_close"]

        # Get ticker's forward close
        future_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
            "ORDER BY date LIMIT 1 OFFSET ?",
            (ticker, signal_date, forward_days - 1),
        ).fetchone()
        if not future_row or future_row["close"] is None:
            continue
        ticker_return = (future_row["close"] - signal_close) / signal_close * 100.0

        # Get SPY's forward return over the same period
        spy_sig_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date = ?",
            (benchmark, signal_date),
        ).fetchone()
        spy_fwd_row = conn.execute(
            "SELECT close FROM ohlcv_daily WHERE ticker = ? AND date > ? "
            "ORDER BY date LIMIT 1 OFFSET ?",
            (benchmark, signal_date, forward_days - 1),
        ).fetchone()

        benchmark_return: Optional[float] = None
        if spy_sig_row and spy_fwd_row and spy_sig_row["close"] and spy_sig_row["close"] > 0:
            benchmark_return = (
                (spy_fwd_row["close"] - spy_sig_row["close"]) / spy_sig_row["close"] * 100.0
            )

        excess = compute_excess_return(ticker_return, benchmark_return)

        # Build feature vector from row data
        ema_9 = row["ema_9"]
        ema_21 = row["ema_21"]
        ema_50 = row["ema_50"]
        close = signal_close

        price_ema9_spread = ((close - ema_9) / ema_9 * 100.0) if ema_9 and ema_9 != 0 else 0.0
        ema9_ema21_spread = ((ema_9 - ema_21) / ema_21 * 100.0) if ema_9 and ema_21 and ema_21 != 0 else 0.0
        ema21_ema50_spread = ((ema_21 - ema_50) / ema_50 * 100.0) if ema_21 and ema_50 and ema_50 != 0 else 0.0

        category_scores = {
            "trend": row["trend_score"],
            "momentum": row["momentum_score"],
            "volume": row["volume_score"],
            "volatility": row["volatility_score"],
            "fundamental": row["fundamental_score"],
            "macro": row["macro_score"],
        }
        raw_indicators = {
            "rsi_14": row["rsi_14"],
            "adx": row["adx"],
            "macd_histogram": row["macd_histogram"],
            "stoch_k": row["stoch_k"],
            "bb_pctb": row["bb_pctb"],
            "cmf_20": row["cmf_20"],
        }
        ema_positions = {
            "price_ema9_spread": price_ema9_spread,
            "ema9_ema21_spread": ema9_ema21_spread,
            "ema21_ema50_spread": ema21_ema50_spread,
        }

        features = build_feature_vector(
            category_scores, raw_indicators, ema_positions,
            weekly_score=row["weekly_score"],
            monthly_score=row["monthly_score"],
        )
        X_list.append(features)
        y_list.append(excess)

    if not X_list:
        return np.empty((0, 17)), np.empty(0)

    return np.array(X_list), np.array(y_list)


# ---------------------------------------------------------------------------
# Main calibration entry point
# ---------------------------------------------------------------------------

def calibrate_score(
    conn: sqlite3.Connection,
    scoring_date: str,
    category_scores: dict,
    raw_indicators: dict,
    ema_positions: dict,
    config: dict,
    weekly_score: Optional[float] = None,
    monthly_score: Optional[float] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> dict:
    """
    Calibrate the current signal using a rolling ridge regression.

    Fetches recent historical signals with known forward excess returns,
    trains a ridge regression on their features, and predicts the expected
    excess return for the current signal's feature vector.

    Returns calibrated_score=None in three cases:
      1. Calibration is disabled in config.
      2. Fewer than min_training_samples are available (cold start).
      3. The ridge solve fails (numerical issues).

    Parameters:
        conn:              Open SQLite connection (row_factory=sqlite3.Row).
        scoring_date:      Current scoring date (YYYY-MM-DD).
        category_scores:   Dict of 6 category scores (trend, momentum, ...).
        raw_indicators:    Dict of 6 raw indicator values (rsi_14, adx, ...).
        ema_positions:     Dict of 3 EMA position spreads.
        config:            Calibration config dict (from scorer.json["calibration"]).
        weekly_score:      Prior-week composite score (same scale as category scores).
                           None → 0.0 in the feature vector.
        monthly_score:     Prior-month composite score (same scale as category scores).
                           None → 0.0 in the feature vector.
        excluded_tickers:  Tickers to omit from the training window. Pass the result of
                           ``get_training_excluded_tickers()`` to exclude sector ETFs,
                           market benchmarks, and index ETFs.

    Returns:
        Dict with keys:
            calibrated_score (Optional[float]) — predicted excess return (%),
                or None if calibration is unavailable.
            model_r2 (float) — in-sample R² of the training window.
            weights (Optional[list[float]]) — learned regression weights,
                or None if calibration is unavailable.
    """
    if not config.get("enabled", True):
        logger.info("phase=%s calibration disabled", _PHASE)
        return {"calibrated_score": None, "model_r2": 0.0, "weights": None, "calibrator_payload": None}

    min_samples = config.get("min_training_samples", 30)
    ridge_lambda = config.get("ridge_lambda", 0.1)

    # Fetch training data, skipping ETFs and benchmarks
    X_train, y_train = fetch_training_data(
        conn, scoring_date, config, excluded_tickers=excluded_tickers
    )

    if len(X_train) < min_samples:
        logger.info(
            "phase=%s cold start — only %d training samples (need %d)",
            _PHASE, len(X_train), min_samples,
        )
        return {"calibrated_score": None, "model_r2": 0.0, "weights": None, "calibrator_payload": None}

    # Build feature vector for the current signal
    x_new = np.array(build_feature_vector(
        category_scores, raw_indicators, ema_positions,
        weekly_score=weekly_score,
        monthly_score=monthly_score,
    ))

    # Train and predict
    result = train_ridge_and_predict(X_train, y_train, x_new, ridge_lambda)

    calibrated_score = result["prediction"]
    model_r2 = result["model_r2"]

    logger.info(
        "phase=%s date=%s n_train=%d model_r2=%.4f calibrated_score=%.2f%%",
        _PHASE, scoring_date, len(X_train), model_r2, calibrated_score,
    )
    logger.info(
        "phase=%s feature_weights=%s",
        _PHASE,
        {name: round(w, 4) for name, w in zip(FEATURE_NAMES, result["weights"])},
    )

    # Build per-feature decomposition payload. Isolate failures so a payload
    # construction bug degrades to calibrator_payload=None rather than losing
    # the score for this ticker.
    try:
        x_new_raw_list = x_new.tolist()
        calibrator_payload: Optional[dict] = build_calibrator_payload(
            feature_names=FEATURE_NAMES,
            x_new_raw=x_new_raw_list,
            col_mean=result["col_mean"],
            col_std=result["col_std"],
            feature_weights=result["weights"][:len(FEATURE_NAMES)],
            intercept=result["weights"][-1],
            prediction=result["prediction"],
            model_r2=result["model_r2"],
            n_training_samples=len(X_train),
        )
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        logger.warning(
            "phase=%s date=%s build_calibrator_payload failed (%s) — payload set to None",
            _PHASE, scoring_date, exc,
        )
        calibrator_payload = None

    return {
        "calibrated_score": calibrated_score,
        "model_r2": model_r2,
        "weights": result["weights"],
        "calibrator_payload": calibrator_payload,
    }
