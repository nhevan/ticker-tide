# Hot — Active Work Tracker

> Auto-updated by agents. Tracks current work and recent history so context carries across sessions.

## Current Task
_No active task — daily-report-empty-body bug fixed and ready for review._

## Recent History
<!-- Most recent first, keep last 3-5 items -->

| When | What | Status |
|---|---|---|
| 2026-05-08 | **Fixed daily Telegram report body always saying "No significant signals today." when `include_ai_reasoning=false`.** Added `invoke_claude` flag to `reason_all_qualifying_tickers`; `main.py` now calls unconditionally. Removed dead try/except that was masking DB errors. New tests in `test_main.py` (1A rewritten, 1B deleted, 1C+1D added) and `test_ai_reasoner.py` (1E added). All 387 notifier tests pass. | ✅ Done |
| 2026-05-06 | **Sitting 2 — `/why` wiring complete.** Extended `send_telegram_message` with `reply_markup` keyword; added `handle_why_command` + `_WHY_TICKER_PATTERN` + `_WHY_USAGE_HINT` to `why_command.py`; new `handle_why_command_wrapper` and `handle_why_callback_wrapper` in `bot.py`; inline "🔍 Why this signal?" button attached to `/detail` msg #3 final chunk; 31 new tests; all three source phases (progress, why_command, bot) shipped atomically in one commit. | ✅ Done |
| 2026-05-05 | **Sitting 1 — `/why` schema + scorer + backend.** Added `scores_daily.key_signals_data` column via `run_migrations` infrastructure in `src/common/db.py`. Extracted `INDICATOR_CATEGORY_MAP` + `PATTERN_CATEGORY_MAP` as module-level constants in `category_scorer.py`. Added `PATTERN_RULE_DESCRIPTIONS` to `pattern_scorer.py`; `PROFILE_FREE_INDICATORS` + `FIXED_LADDER` to `indicator_scorer.py`. New `src/scorer/contribution.py` builds the per-indicator/per-pattern contribution payload. `src/scorer/main.py` calls the builder after `apply_adaptive_weights` and persists JSON via `save_score_to_db`. New `src/notifier/why_command.py` with `dispatch_why` + three formatters (`format_why_default`, `format_why_all`, `format_why_drilldown`) + `load_why_payload` + `resolve_name_token`. Added `why_top_n: 5` and `why_list_max_entries: 50` to `config/notifier.json`. Docs: DESIGN.md §11.1, DEVELOPMENT.md, CONFIG.md. | ✅ Done |
| 2026-05-05 | **Plan B — Restructure `/detail` msg #2 into 5 structured sections.** Replaced single free-form AI prose with: VERDICT (AI), TIMEFRAME SUMMARY (deterministic table + AI 1-line), REASONING (AI), CONFIDENCE (deterministic, with calibration sign-flip flag), LEVELS & TRIGGERS (existing builders). 22 new tests + 4 updated. 62 total detail-command tests green. 4 new config keys in `config/notifier.json`. `verify_pipeline.py`: 0 FAIL. Docs: DESIGN.md §"AI vs deterministic content boundary", CONFIG.md. | ✅ Done |
| 2026-05-05 | **Plan A — Fix `/detail` scoring chain display bug.** Regime-adaptive 3-way weights now flow through `scorer_cfg` parameter. 7 new tests + 3 updated, all 40 detail tests green. Docs: DESIGN.md §"Regime-Adaptive 3-Way Timeframe Merge". | ✅ Done |
| 2026-05-05 | **Simplified Telegram signal report.** Added `telegram.include_ai_reasoning` boolean flag. When `false`, Claude API call is skipped and reasoning sections are omitted. 65/65 notifier tests pass. Docs: CONFIG.md, DESIGN.md §13.2+§13.4. | ✅ Done |

## Key Decisions
<!-- Recent trade-offs and choices that affect future work -->
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
- **~2027-04-29 (365 days post-v2 flip)**: re-run the calibrator acceptance gate with a fresh baseline. By then the 365-day training window will be fully v2-scored, so the readout will no longer be contaminated by mixed-semantics training rows.
- **Investigate the lone verify_pipeline warning**: `monthly_divergence_count: 0 divergences_monthly rows in last 12 months`. Pre-existing, unrelated to v2.
- Monitor anti-predictive tickers and BEARISH asymmetry (previously flagged improvement area).

## Known Issues
- **`always_include_flips` dead key in `config/notifier.json`** (pre-existing, not addressed in this PR). The key is read nowhere; flips are always included regardless. Candidate for removal.
- **`reason_all_qualifying_tickers` exceeds CLAUDE.md 50-line guidance.** After the `invoke_claude` flag was added (2026-05-08), the function grew further. Pre-existing violation slightly worsened; deferred for follow-up to keep scope tight.
