# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-16 | Exclude ETFs from calibrator training window: `get_training_excluded_tickers()` in `config.py` (sector ETFs + market benchmarks + sector="Index" tickers), `excluded_tickers` param on `fetch_training_data` + `calibrate_score`, propagated through `score_ticker` and `run_historical_scoring`. 26 calibrator tests pass. | ✅ Done |
| 2026-04-17 | Implement monthly timeframe: `monthly_candles` + `indicators_monthly` tables, `monthly_score` in `scores_daily`, `src/calculator/monthly.py`, 3-way `merge_timeframes()` with renorm, `compute_monthly_score()`, calibrator 16→17 features, confidence `monthly_available`, migration script, verify_pipeline monthly checks, all 6 docs updated. 1312 tests pass. | ✅ Done |
| 2026-04-15 | Add `weekly_score` as 16th calibrator feature: `build_feature_vector`, `fetch_training_data`, `calibrate_score`, `main.py` call site. Added weight-vector INFO log. 21 calibrator tests pass, 1317 total pass. | ✅ Done |
| 2026-04-15 | Fix confidence base: use `min(abs(cal),8.0)*10` (warm) / `abs(fs)*0.3` (cold) instead of `abs(final_score)`. Data-driven: accuracy peaks at \|cal\|≈7 (63%), drops above 8. Cap prevents overfit extremes from inflating confidence. 6 new tests, 263 pass. | ✅ Fixed |
| 2026-04-15 | Fix `final_score` mixed-scales bug: column now always holds ±100 composite; `raw_composite_score` column removed; `check_signal_score_consistency` updated; migration script written | ✅ Fixed, all tests pass |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- **Monthly timeframe added**: 3-way merge with regime-adaptive weights (trending 0.10d/0.50w/0.40m, ranging 0.60d/0.30w/0.10m, volatile 0.25d/0.45w/0.30m). When monthly is absent, remaining weights are renormalized to sum to 1.0.
- **Calibrator 17 features**: 6 category + 6 raw + 3 EMA spreads + weekly_score + monthly_score. Monthly_score = 0.0 during cold start (ridge handles gracefully).
- **~60 monthly bars** from 5 years of data; EMA-50 on monthly will be NULL for first ~50 months — acceptable, scorer handles None indicators as 0.0.
- **Confidence base uses calibrated_score** (warm): `min(abs(cal), 8.0) * 10`. Cap at 8.0 — accuracy drops above |cal|=8 (57.6%) and |cal|=12 (47.7%) due to calibrator overfitting. Cold start: `abs(final_score) * 0.3`.
- **`final_score` column is always ±100** (merged timeframe composite). `calibrated_score` (≈ ±2–15%) is separate.
- Rolling ridge calibrator: window=90, lambda=0.1, 17 features. Validated R²=0.1025 (was 0.0972 with 16 features) across 15 diverse tickers.
- Anti-predictive categories (candlestick, structural, sentiment) zeroed in adaptive weights.

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- **Run migration on production DB**: `python scripts/migrate_add_monthly.py` (then `run_calculator.py --mode full` and `run_scorer.py --historical --force`)
- Re-run calibrator `--historical --force` to populate monthly_score in training data and measure new R²
- Monitor anti-predictive tickers and BEARISH asymmetry (previously flagged as improvement area)
