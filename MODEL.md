# MODEL.md — Calibrator Performance & Design

## 1. Performance Snapshot

> ⚠️ Snapshot as of 2026-05-14. Numbers go stale on every retrain. Regenerate via [helper script TBD] before relying on them.

| Metric | Value |
|---|---|
| Pearson r | +0.1274 |
| OOS R² | +0.0074 |
| RMSE | 10.22% |
| MAE | 6.59% |
| Hit rate (directional) | 54.1% |
| N (predictions) | 28,847 |

---

## 2. Recent Changes

| Date | Change |
|---|---|
| 2026-05-14 | Persist realized returns on `scores_daily` (5 new columns + backfill + daily populator + `verify_pipeline` coverage check). Enables direct SQL accuracy queries; no model logic changed. |

---

## 3. Model Overview

The calibrator uses a rolling ridge regression trained on a 365-day sliding window of historical signals across all tickers. Each training example pairs a 17-feature vector (6 category scores, 6 raw indicator values, 3 EMA position spreads, prior weekly composite, prior monthly composite — see `src/scorer/calibrator.py:40-47`) with the realized 10-trading-day forward excess return versus SPY. At scoring time, the same 17 features are extracted from the current signal and fed into the trained model to produce a predicted excess return (`calibrated_score`). Regularisation strength is λ=0.1. Sector ETFs, market benchmarks, and index ETFs are excluded from the training window because their feature-return relationships differ from individual stocks.

---

## 4. Known Limitations

- **In-sample R² clamp:** `max(0.0, r2)` is applied at `src/scorer/calibrator.py:218`. An audit confirmed the clamp never fires in practice (R² is always ≥ 0 on the training distributions seen so far); it is kept as a safety net against degenerate windows.
- **No dashboard surface for accuracy:** The realized-return columns are queryable via SQL but not surfaced in the dashboard UI. A dedicated accuracy panel is planned (see §5).
- **Short training history:** The 365-day rolling window will not be fully populated with v2-scored rows until approximately 2027-04-29 (365 days after the v2 scoring flip on 2026-04-29). Accuracy metrics computed before that date are contaminated by mixed-semantics training rows from the v1 era.
- **No per-prediction confidence intervals:** The model produces a point estimate only. Prediction intervals (e.g., bootstrap or conformal) are not implemented.
- **Horizon decoupling:** `analytics.forward_days` (used by `populate_realized_returns`) is intentionally decoupled from `calibration.forward_days` (used by `fetch_training_data`). If they diverge, a WARNING is logged at populate time but historical `realized_excess` values reflect whichever horizon was active when the row was populated. Drift between the two keys is not automatically detected at scoring time.

---

## 5. Planned Improvements

- **Accuracy dashboard panel** — surface hit rate, Pearson r, and rolling RMSE on the dashboard. `[planned]`
- **`scripts/compute_model_metrics.py` helper** — auto-regenerate the Performance Snapshot table above from the live database. `[planned]`
- **Feature ablation study** — measure individual feature contributions to OOS R² by leave-one-out re-training. `[exploring]`
- **Walk-forward CV report** — rolling cross-validation with expanding or rolling train/test splits to get a more reliable OOS estimate. `[exploring]`
- **Elastic-net or gradient-boosted residual model** — explore whether a more expressive learner materially improves hit rate given the current feature set. `[exploring]`
