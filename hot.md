# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-15 | Fix `check_weighted_score_math` false positives: was using hardcoded trending weights (0.2d/0.8w) for all tickers; now looks up regime-specific weights per ticker from config | ✅ Fixed, 3 new tests |
| 2026-04-15 | Fix `final_score` mixed-scales bug: column now always holds ±100 composite; `raw_composite_score` column removed; `check_signal_score_consistency` updated; migration script written | ✅ Fixed, all tests pass |
| 2026-04-14 | Fix confidence scale collapse: pass raw_composite_score (±100) instead of calibrated_score (±8) as confidence base in main.py | ✅ Fixed & re-scored |
| 2026-04-14 | Fix migrate_add_calibration_columns.py wrong default DB path (ticker_tide.db → config/database.json) | ✅ Fixed & migration run |
| 2026-04-14 | Rolling ridge regression calibrator — replaces static composite with data-driven predicted excess return | ✅ Implemented & integrated |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- **`final_score` column is always ±100** (merged timeframe composite). `calibrated_score` (≈ ±2–15%) is separate. `raw_composite_score` column was removed — it was a redundant patch.
- Run `scripts/migrate_scores_final_score.py` against the production DB before the next pipeline run to repair existing rows and drop the old column.
- Signal thresholds scaled to ±2 (bullish:2, bearish:-2) to match calibrated_score range in config/scorer.json
- `check_signal_score_consistency` uses `calibrated_score` as arbiter when non-NULL, falls back to `final_score`.
- Rolling ridge calibrator: window=90, lambda=0.1, 15 features (6 category + 6 raw + 3 EMA spreads), validated R=0.47 across all 62 tickers
- Anti-predictive categories (candlestick, structural, sentiment) zeroed in adaptive weights; calibrator independently ignores them
- Timeframe weights now regime-adaptive: trending 0.2d/0.8w, ranging 0.8d/0.2w, volatile 0.5/0.5

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- **Run migration on production DB**: `python scripts/migrate_scores_final_score.py`
- Re-run scatter plot to verify improved correlation (target: R > 0.4)
- Monitor calibrated_score distribution and quintile separation
- Confidence avg now 46.9% (was 12.7%); verify signal flip quality improved
