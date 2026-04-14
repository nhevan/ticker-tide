# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-14 | Fix confidence scale collapse: pass raw_composite_score (±100) instead of calibrated_score (±8) as confidence base in main.py | ✅ Fixed & re-scored |
| 2026-04-14 | Fix migrate_add_calibration_columns.py wrong default DB path (ticker_tide.db → config/database.json) | ✅ Fixed & migration run |
| 2026-04-14 | Fix build_signal_history date.today() drift — added reference_date param | ✅ Fixed |
| 2026-04-14 | Rolling ridge regression calibrator — replaces static composite with data-driven predicted excess return | ✅ Implemented & integrated |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- Confidence base = abs(raw_composite_score) always (±100 scale). calibrated_score (±8%) is used ONLY for signal classification and DB storage. Never use calibrated_score as a confidence base.
- Signal thresholds scaled to ±2 (bullish:2, bearish:-2) to match calibrated_score range in config/scorer.json
- Rolling ridge calibrator: window=90, lambda=0.1, 15 features (6 category + 6 raw + 3 EMA spreads), validated R=0.47 across all 62 tickers
- Anti-predictive categories (candlestick, structural, sentiment) zeroed in adaptive weights; calibrator independently ignores them
- Timeframe weights now regime-adaptive: trending 0.2d/0.8w, ranging 0.8d/0.2w, volatile 0.5/0.5

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- Re-run scatter plot to verify improved correlation (target: R > 0.4)
- Monitor calibrated_score distribution and quintile separation
- Confidence avg now 46.9% (was 12.7%); verify signal flip quality improved
