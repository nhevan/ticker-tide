# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-15 | Add `weekly_score` as 16th calibrator feature: `build_feature_vector`, `fetch_training_data`, `calibrate_score`, `main.py` call site. Added weight-vector INFO log. 21 calibrator tests pass, 1317 total pass. | ✅ Done |
| 2026-04-15 | Fix confidence base: use `min(abs(cal),8.0)*10` (warm) / `abs(fs)*0.3` (cold) instead of `abs(final_score)`. Data-driven: accuracy peaks at \|cal\|≈7 (63%), drops above 8. Cap prevents overfit extremes from inflating confidence. 6 new tests, 263 pass. | ✅ Fixed |
| 2026-04-15 | Fix `check_weighted_score_math` false positives: was using hardcoded trending weights (0.2d/0.8w) for all tickers; now looks up regime-specific weights per ticker from config | ✅ Fixed, 3 new tests |
| 2026-04-15 | Fix `final_score` mixed-scales bug: column now always holds ±100 composite; `raw_composite_score` column removed; `check_signal_score_consistency` updated; migration script written | ✅ Fixed, all tests pass |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- **Confidence base uses calibrated_score** (warm): `min(abs(cal), 8.0) * 10`. Cap at 8.0 — accuracy drops above |cal|=8 (57.6%) and |cal|=12 (47.7%) due to calibrator overfitting. Cold start: `abs(final_score) * 0.3`.
- **`final_score` column is always ±100** (merged timeframe composite). `calibrated_score` (≈ ±2–15%) is separate. `raw_composite_score` column was removed — it was a redundant patch.
- Run `scripts/migrate_scores_final_score.py` against the production DB before the next pipeline run to repair existing rows and drop the old column.
- Signal thresholds scaled to ±2 (bullish:2, bearish:-2) to match calibrated_score range in config/scorer.json
- `check_signal_score_consistency` uses `calibrated_score` as arbiter when non-NULL, falls back to `final_score`.
- Rolling ridge calibrator: window=90, lambda=0.1, 16 features (6 category + 6 raw + 3 EMA spreads + weekly_score), validated R=0.47 across all 62 tickers
- Anti-predictive categories (candlestick, structural, sentiment) zeroed in adaptive weights; calibrator independently ignores them
- Timeframe weights now regime-adaptive: trending 0.2d/0.8w, ranging 0.8d/0.2w, volatile 0.5/0.5

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- **Run migration on production DB**: `python scripts/migrate_scores_final_score.py`
- Re-run scatter plot to verify improved correlation (target: R > 0.4)
- Monitor calibrated_score distribution and quintile separation
- Confidence avg now 46.9% (was 12.7%); verify signal flip quality improved
