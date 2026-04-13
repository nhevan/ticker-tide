# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-04-14 | EMA stack alignment override for regime detection | ✅ Merged & re-scored |
| 2026-04-14 | Widen weekly score distribution (full 14-indicator pipeline) | ✅ Merged & re-scored |
| 2026-04-14 | Bearish accuracy analysis across 62 tickers | ✅ Complete |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- Signal thresholds raised from ±20 to ±30 (committed)
- Bearish accuracy improved 42% → 48.7% via EMA override; ranging bearish still weak at 39.1%
- Weekly scoring now uses full indicator pipeline (was 6 of 14), weight split remains 0.2 daily / 0.8 weekly

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- Consider further raising signal thresholds (±30 → ±35) based on accuracy data
- Investigate remaining ranging-regime bearish underperformance (39.1%)
