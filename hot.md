# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
None (Idle).

<!-- Last completed: Stoch %K explainer tear-down (2026-05-12) -->

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-05-12 | **Stoch %K explainer — full build + tear-down (Task #11).** All 7 steps live on real backend data. Commits: c70c031 (backend), 1b6b0a4 (Step 2), 0773617 (Step 3), 5a509c6 (Step 4), 2a50772 (Step 5), 7306112 (Steps 6–7), 6d9509f (Step 7 wiring complete), tear-down (this task). Tear-down: renamed `RsiPercentileStrip.tsx` → `PercentileStrip.tsx` and `RsiMappingChart.tsx` → `PercentileMappingChart.tsx` via `git mv`; updated all import sites and JSX tags in `IndicatorExplainerPanel.tsx`; updated MACD Step 3 comment (Required Fix #3); updated CLAUDE.md recipe (Required Fix #4); rewrote DESIGN.md §15.6 as full 7-step trace (Required Fix #5/6); cleaned stale "File rename deferred" note. Build clean; 153/153 tests pass. Grep confirms 0 residual `RsiPercentileStrip`/`RsiMappingChart` references. | ✅ Done |
| 2026-05-12 | **Verdict header — Trend/Mom columns replaced with SignedBar bars; dashed border wrapper.** Promoted Prototype B from `TimeframeSummaryTable.tsx`. Build clean; 153/153 frontend tests pass. | ✅ Done |
| 2026-05-12 | **Section equation row + cross-section banner promoted from prototype.** New `equationSummary.ts` (35 tests). Build clean. DESIGN.md §15.4b + CONFIG.md updated. | ✅ Done |
| 2026-05-12 | **Matrix section header math chain.** `computeTimeframeHeaderContributions` pure function. TDD order followed. Build clean. DESIGN.md §15.4 and §15.4a updated. | ✅ Done |
| 2026-05-12 | **MACD line 7-step explainer panel — fully wired.** All 7 steps in `MacdLinePanel`. Build clean. DESIGN.md §15.5 added. | ✅ Done |
## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
- **TONE_CLASS rethemed to --up/--down CSS vars (2026-05-11).** `MatrixTable` cell backgrounds now use `bg-[hsl(var(--up)/0.18)]` and `bg-[hsl(var(--down)/0.18)]` instead of Tailwind emerald/rose. The `data-tone` attribute is preserved; existing tests continue to assert on it. Green cells automatically use the amber-on-black theme's bright green; red cells use the bright red.
- **TickerTape strip as pure presenter (2026-05-11).** Mounted as sibling between `</Header>` and `<main>` in `DashboardPage`. Derives price and % change from `snapshot.daily.sparkline` last two closes; unknown signal strings render as muted pills without throwing.
- **Pattern rows use scorer's canonical recency windows (2026-05-11).** `_fetch_recent_patterns` imports `_CANDLESTICK_WINDOW_DAYS` and `_STRUCTURAL_WINDOW_DAYS` directly from `pattern_scorer.py` so the matrix display boundary stays in lockstep with score decay. Monthly excludes candlestick (same reason as scoring). No scorer or schema change.
- **Per-indicator persistence: Path A (sidecar tables, not inline columns) (2026-05-11).** Three new tables rather than adding 12+ columns to `scores_daily/weekly/monthly`. Rationale: keeps the wide score tables stable; sidecar tables can grow as new indicators are added without altering the primary scoring schema.
- **Indicators-only v1 for the matrix (2026-05-11).** The dashboard matrix shows only the 12 indicators in `INDICATOR_CATEGORY_MAP` (not patterns, crossovers, or divergences). Rationale: patterns/crossovers/divergences already have their own scored categories; indicators are the granular "raw signal" layer that benefits most from per-item visibility.
- **`atr_14` and `keltner` removed from `INDICATOR_CATEGORY_MAP` (2026-05-11).** `atr_14` is a confidence-modifier input (not directional — high ATR = higher uncertainty, not bullish or bearish); `keltner` is never emitted by `score_all_indicators`. Both would always produce null/zero cells in the matrix, adding noise. Map is now 12 entries.
- **Daily uses `composite_score` alias `final_score` for matrix direction (2026-05-11).** `queries.py` line 181 maps `scores_daily.final_score` to `composite_score` in the daily snapshot section; frontend uses `scoreToDirection(composite_score)` uniformly across all three timeframes.
- **Atomic deploy of all three `/why` source phases (2026-05-06).** `send_telegram_message` reply_markup extension, `why_command.py` handler/wrapper additions, and `bot.py` handler + button wiring all landed in a single commit. Rationale: splitting across deploys would leave a window where the inline button exists in `/detail` output but the `CallbackQueryHandler` is not yet registered — tapping the button would silently fail. Atomic commit eliminates the failure window.
- **Persisted contribution payload at scoring time (2026-05-05).** `scores_daily.key_signals_data` is written during the daily scorer run rather than recomputed on `/why` invocation. Rationale: avoids config-drift (payload is consistent with the score it annotates), simplifies pattern handling (no need to re-query pattern tables in the bot event loop), and keeps the Telegram bot's async event loop unblocked.
- **Daily card enriched with `key_signals` top-3, earnings row, and signal-flip badge (2026-04-29).** Adversary dropped market-context banner (QQQ-only chrome — macro already in category bars) and news feed (sentiment ≈ 0, headlines clickbait-quality, no click-through). All three picks are daily-only; weekly/monthly cards are unchanged. Signal-flip `id DESC` tiebreaker is mandatory due to production duplicates (ASTS ×3 same-date, LLY ×2 contradictory same-date). Earnings `actual_eps IS NULL` guard excludes 18 stale-null past rows.
- **Per-timeframe data parity (commits 1-9)**: weekly + monthly now mirror daily's event tables (patterns, divergences, crossovers, swing_points, S/R, profiles) + score breakdown. Schema chose **Option A** (separate `scores_weekly` + `scores_monthly` tables keyed by week_start/month_start) over extending `scores_daily`. UI rendering chose **Option C** (per-timeframe score + categorical breakdown only — NO discrete BUY/SELL signal classification per timeframe; the merged daily signal remains the single decision the engine publishes).
- **`weekly_score_method` / `monthly_score_method` = `v2_8cat` in production (flipped 2026-04-29)**. v2_8cat loads weekly/monthly events from the mirror tables and routes crossovers→trend, divergences→momentum/volume, candlestick→cdl, structural→struct (cdl/struct weights currently 0.0, so the v2 scalar differs from v1 only via trend/momentum/volume getting weekly events). Flip was gated by a shadow acceptance gate run that PASSED with mean_delta=+0.004, std_delta=−0.006 — orders of magnitude inside thresholds. Reverting requires the same gate procedure in reverse (snapshot, flip back, re-historical, check).
- **scores_weekly/_monthly are denormalized snapshots** for query/UI — NOT in the scoring critical path. `merge_timeframes()` consumes the in-memory composite from `compute_*_score_breakdown()`.
- **Closed-period gate**: weekly closes when `scoring_date >= week_start + 7 days` (Sunday is in-progress; Monday closes the prior week). Monthly closes when `(scoring_date.year, .month) > (month_start.year, .month)`. Persistence skips in-progress periods; live merged signal uses partial bars.
- **Monthly candlestick scoring is permanently disabled** (decay-window mismatch — `_CANDLESTICK_WINDOW_DAYS=7` zeroes out monthly bars). `candlestick_score = None` in monthly breakdown regardless of v1/v2 mode.
- **`weekly_score` is in the LLM prompt context** (`ai_reasoner.py:803`), `monthly_score` is NOT. Flipping v2 will subtly shift Telegram blurb tone via the LLM input shift; mitigated by manual pre/post diff in the flip procedure.
- **Calibrator acceptance gate** uses distribution-level thresholds (max_mean_delta=5.0, max_std_delta=8.0 on calibrated_score across all tickers at a fixed scoring_date) rather than R²-based thresholds. Stores per-ticker values in baseline JSON to surface bipolar shifts. Three-tier output (PASS / PASS-with-WARNING ≥70% of threshold / FAIL).
- **Monthly timeframe added** (pre-parity): 3-way merge with regime-adaptive weights (trending 0.10d/0.50w/0.40m, ranging 0.60d/0.30w/0.10m, volatile 0.25d/0.45w/0.30m). When monthly is absent, remaining weights are renormalized to sum to 1.0.
- **Calibrator 17 features**: 6 category + 6 raw + 3 EMA spreads + weekly_score + monthly_score. Rolling ridge: window=365, lambda=0.1. Validated R²=0.1025.
- **Confidence base uses calibrated_score** (warm): `min(abs(cal), 8.0) * 10`. Cold start: `abs(final_score) * 0.3`.
- **`final_score` column is always ±100** (merged composite). `calibrated_score` (≈ ±2–15%) is separate.

## Next Up
<!-- Known upcoming tasks or follow-ups -->
- **Scorer fallback literal→config refactor** — `src/scorer/indicator_scorer.py` lines 529-535 hardcode 80/20 thresholds for stoch_k fallback. A follow-on task should read from `resolved_scorer_config["indicator_thresholds"]["stoch_k"]` so the threshold is configurable. A comment was added at the site marking this deferred work.
- **Playwright E2E follow-up** — add browser-level end-to-end tests for login flow, snapshot load, and Ask AI (explicitly out of scope for the Vite migration PR).
- **~2027-04-29 (365 days post-v2 flip)**: re-run the calibrator acceptance gate with a fresh baseline. By then the 365-day training window will be fully v2-scored, so the readout will no longer be contaminated by mixed-semantics training rows.
- **Investigate the lone verify_pipeline warning**: `monthly_divergence_count: 0 divergences_monthly rows in last 12 months`. Pre-existing, unrelated to v2.
- Monitor anti-predictive tickers and BEARISH asymmetry (previously flagged improvement area).

## Known Issues
- **`always_include_flips` dead key in `config/notifier.json`** (pre-existing, not addressed in this PR). The key is read nowhere; flips are always included regardless. Candidate for removal.
- **`reason_all_qualifying_tickers` exceeds CLAUDE.md 50-line guidance.** After the `invoke_claude` flag was added (2026-05-08), the function grew further. Pre-existing violation slightly worsened; deferred for follow-up to keep scope tight.
- **`ZONE_LABEL_DESCRIPTIONS` (lines 24–30 of `IndicatorExplainerPanel.tsx`) still says "historical RSI"** even though the zone-label keys are reused across indicators. Per-indicator dicts (e.g. `STOCH_ZONE_LABEL_DESCRIPTIONS`) work around this. Track for a future per-indicator-dict cleanup or a parameterised prose generator if a third indicator panel adopts the same pattern.
