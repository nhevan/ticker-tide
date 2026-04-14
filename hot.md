# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-14 | Fix build_signal_history date.today() drift — added reference_date param | ✅ Fixed |
| 2026-04-14 | Rolling ridge regression calibrator — replaces static composite with data-driven predicted excess return | ✅ Implemented & integrated |
| 2026-04-14 | EMA stack alignment override for regime detection | ✅ Merged & re-scored |
| 2026-04-14 | Widen weekly score distribution (full 14-indicator pipeline) | ✅ Merged & re-scored |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- Rolling ridge calibrator: window=90, lambda=0.1, 15 features (6 category + 6 raw + 3 EMA spreads), validated R=0.47 across all 62 tickers
- Anti-predictive categories (candlestick, structural, sentiment) zeroed in adaptive weights; calibrator independently ignores them
- Timeframe weights now regime-adaptive: trending 0.2d/0.8w, ranging 0.8d/0.2w, volatile 0.5/0.5
- Scatter plot now shows predicted vs actual excess return (vs SPY) with R correlation display
- DB migration script: `scripts/migrate_add_calibration_columns.py` (must run before deploying)

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- Deploy: run migration script, then `run_scorer.py --force` to populate calibrated_score
- Re-run scatter plot to verify improved correlation (target: R > 0.4)
- Monitor calibrated_score distribution and quintile separation
