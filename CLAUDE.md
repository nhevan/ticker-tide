# CLAUDE.md — Stock Signal Engine

## Coding Standards
- Python 3.9+
- Write tests FIRST (TDD) using pytest. Always create/update tests before implementing.
- Every function must have type hints on all parameters and return values
- Every function must have a descriptive docstring explaining: what it does, parameters, return value
- Prefer verbosity and readability over clever abstractions
- No single-letter variable names (except loop counters i, j, k)
- Use descriptive function names that read like English (e.g., fetch_daily_ohlcv_for_ticker, not fetch)
- Keep functions focused — if a function exceeds 50 lines, break it up
- Use f-strings for string formatting
- All magic numbers must come from config files — no hardcoded thresholds, window sizes, or URLs
- Use logging (standard library) with structured messages
- Log at INFO for normal ops, WARNING for recoverable issues, ERROR for failures
- Always include context in log messages (which ticker, which phase, what date)

## Dashboard Workflow

For ANY visual change to the dashboard — new chart, panel, restyle, layout shift, color/typography tweak — follow this discipline:

- ALWAYS mock 2–3 variations first, inline in the affected component, with dummy data clearly labeled `[PROTOTYPE]`.
- Wait for the user to visually pick before any planning or implementation. Do NOT delegate to `plan-implementer` until they've chosen.
- After the user picks: standard plan → `adversarial-reviewer` → `plan-implementer` → `code-reviewer` → commit cycle.
- Mockups are scaffolding only. After the pick, promote the chosen variant to a real component file, then remove all prototype scaffolding atomically (extract → wire → build green → remove). **Never commit prototypes.**

This applies to anything the user will see (charts, layout, components, styling). It does NOT apply to backend work, wiring changes invisible to the user, or pure refactors.

## Error Handling
- Never silently swallow exceptions
- Catch specific exceptions, not bare except
- For API calls: retry with exponential backoff (tenacity), max 3 retries
- If a ticker fails, log the error, skip it, continue with next ticker
- Log all skipped tickers to alerts_log database table

## Database
- SQLite3 with WAL mode enabled
- All timestamps stored as UTC strings (YYYY-MM-DD or ISO 8601)
- UNIQUE constraints on (ticker, date) for all daily tables
- Use INSERT OR REPLACE to handle idempotent re-runs
- Always use parameterized queries — never string concatenation for SQL

## Testing
- Use pytest with fixtures
- Mock ALL external API calls — never call real APIs in tests
- Test happy path, edge cases, and error handling
- Use tmp_path fixture for temporary database files
- Create reusable fixtures for sample OHLCV data, sample indicators, etc.
- Assert specific values, not just "not None"
- Mirror src/ structure in tests/ directory

### Post-Change Verification
- After implementing any change to indicators, patterns, or scoring:
  - Run the relevant test suite: `python -m pytest tests/test_calculator/ -v`
  - Run pipeline verification: `python scripts/verify_pipeline.py`
  - Check Telegram output for any anomalies
- After any config change:
  - Re-run the affected phase with `--force`
  - Run: `python scripts/verify_pipeline.py`
  - Compare signal distribution before and after

## Project Layout
- All source code in src/
- All tests in tests/ mirroring src/ structure
- All configs in config/ as JSON files
- Entry point scripts in scripts/
- API keys in .env file (loaded via python-dotenv, never committed)

## Documentation
- Every code change must be followed by a doc update before the task is considered done — no exceptions
- The current doc files and what triggers an update in each:
  - **README.md** — architecture changes, new/removed scripts, new dependencies, changes to quick-start commands or cron setup
  - **OPERATIONS.md** — changes to script flags, cron schedule, pipeline phase order, monitoring queries (schema changes), troubleshooting steps, or the migration process
  - **CONFIG.md** — any key added, removed, or renamed in any `config/*.json` file; any new `.env` variable; any change to which phase a config change requires re-running
  - **DEVELOPMENT.md** — changes to module responsibilities, inter-module dependencies, the indicator/data-source addition workflows, or DB migration steps
  - **DESIGN.md** — changes to pipeline architecture, database schema, API endpoints, or scoring/signal logic
  - **hot.md** — updated at the start and end of every agent session; tracks current task, recent history, key decisions, and next steps
- Scan every `.md` file in the repo before closing a task — new docs may have been added since this file was last updated
- Never leave a doc describing behavior that no longer matches the code

## Session Handoff
- At the **start** of a session: read `hot.md` to understand current context
- At the **end** of a session (or when a significant task completes): update `hot.md` with:
  - Move current task to Recent History with status
  - Set new Current Task (or clear it)
  - Add any key decisions made
  - Update Next Up if priorities changed
- Keep Recent History to the last 3-5 items — remove older entries

## Git
- Never include AI attribution in commit messages — no "Generated by Claude", "Co-authored-by: Claude", or any similar AI contribution notices

## Verification Guidelines

Every change to indicators, patterns, scoring logic, or thresholds must be verified against the pipeline verification system.

### When to run verification:

| Change Type | Run verify_backfill.py | Run verify_pipeline.py |
|---|---|---|
| New ticker added | ✅ | ✅ (after calculator) |
| Config threshold changed | ❌ | ✅ |
| New indicator added | ❌ | ✅ |
| New pattern added | ❌ | ✅ |
| Scoring logic modified | ❌ | ✅ |
| Backfill re-run | ✅ | ✅ |
| Database restored from backup | ✅ | ✅ |
| Daily pipeline health check | ❌ | ✅ |

### Adding a new indicator:
1. Add parameter to `config/calculator.json`
2. Add column to `indicators_daily` table (update `db.py` `create_all_tables`)
3. Implement computation in `src/calculator/indicators.py`
4. Add to `PROFILED_INDICATORS` in `src/calculator/profiles.py`
5. Add scoring logic in `src/scorer/indicator_scorer.py`
6. Map to the appropriate category in `src/scorer/category_scorer.py`
6b. Add the same entry to `web/src/lib/scoring/categoryMap.ts` (both `INDICATOR_CATEGORY_MAP` and `INDICATOR_DISPLAY_LABELS`) so the dashboard matrix renders the new row. The drift-guard test `tests/web/test_category_map_sync.py` will fail loudly on drift.
7. Add range definition to `INDICATOR_RANGES` in `src/backfiller/verify_pipeline.py`
8. Write tests for each step above
9. Run: `python scripts/verify_pipeline.py`
10. Verify the new indicator appears in profiles and scores affect the signal
11. Re-run scorer with `--force` to backfill `indicator_scores_*` for historical rows; the matrix will otherwise show empty cells for old dates: `python scripts/run_scorer.py --historical --force`

### Adding a new pattern:
1. Add detection parameters to `config/calculator.json`
2. Implement detection in `src/calculator/patterns.py`
3. Add scoring logic in `src/scorer/pattern_scorer.py`
4. Write tests
5. Run: `python scripts/verify_pipeline.py`
6. Check pattern count is reasonable (not 0, not thousands)

### Adding an indicator explainer:

For adding a click-to-expand explainer panel for an indicator (parallel to the existing `rsi_14` panel — see `DESIGN.md` §15 for the full contract and the RSI implementation as the canonical reference).

#### Pre-work
1. Read `DESIGN.md` §15.
2. Identify the indicator's category in `web/src/lib/scoring/categoryMap.ts` (`INDICATOR_CATEGORY_MAP`).
3. Identify the scoring path in `src/scorer/indicator_scorer.py` (which `score_<name>` function, whether percentile-profile or fallback, regime-sensitivity).

#### Reusable components — REUSE verbatim
- `web/src/components/CategoryWeightBar.tsx` — step 6. Already generic over any category.
- `web/src/components/ContributionMathChain.tsx` — step 7. Already generic over any indicator.
- `web/src/components/MomentumShareBar.tsx` — step 5, **only if the indicator is in the `momentum` category**. For other categories, generalise on first use: rename to `CategoryShareBar`, parameterise the category filter; the rest of the component is category-agnostic.

#### Per-indicator components — NEEDS NEW or GENERALISE
- Step 2 trend chart: new `<X>TrendChart.tsx`. Use `RsiTrendChart.tsx` as a template. Different sparkline data shape (signed, unbounded, bounded differently) and reference bands per indicator.
- Step 3 percentile strip: `PercentileStrip.tsx` is already generic over any bounded 0–100 indicator via the `label` prop. RSI and Stoch %K both consume it; reuse for any other 0–100 oscillator (Williams %R, MFI).
- Step 4 mapping chart: `PercentileMappingChart.tsx` is generic over `score_with_percentile`-scored indicators via the `label` prop. Reuse for any indicator that scores through that path. For indicators that score differently (e.g. MACD via z-score), write a new `<X>MappingChart.tsx`.

#### Backend extensions — IF NEEDED
- Per-indicator sparkline field on `snapshot.daily` (template: `_fetch_rsi_sparkline` in `src/web/queries.py`).
- Per-indicator profile lookup (template: `_fetch_rsi_profile`).
- Per-indicator zone-label helper in `src/scorer/zone_labels.py` (template: `zone_label_for_rsi`).
- New entry in `/api/scoring-rules` response if the indicator has its own thresholds (template: the `rsi` block in `src/web/app.py`).
- New `<x>_sparkline_days` key in `config/web.json` `sparkline` block if the trend chart window differs from RSI's 100 days.

#### Per-step workflow
For each step's visualisation, follow the `## Dashboard Workflow` rule above: mock 2–3 variations inline in `IndicatorExplainerPanel.tsx` with dummy data, wait for the user to pick, then promote the chosen variant to a real component (extract → wire → build green → remove scaffolding atomically).

The frontend orchestrator is `web/src/components/IndicatorExplainerPanel.tsx`. The `RsiPanel` function inside is the per-indicator pattern to mirror — copy its structure for a new `<X>Panel`, then dispatch in the top-level component switch.

#### Gotchas (from the RSI build)
1. **Math sign**: contribution is `score × |score| / Σ|score| × regime_weight × expansion`, NOT `score × score / Σ|score|`. For negative scores these differ in sign and the latter silently inverts bearish indicators into bullish-looking contributions. Caught in `ContribVariationA` during the RSI build; fixed in `ContributionMathChain`.
2. **Persisted vs current config**: steps 4/5/7 read from `snapshot.daily.contributions_payload` (persisted at scoring time); step 6 reads from `/api/scoring-rules` (current config). Drift between them is covered by the `approximation_caveat` shown in step 7. Don't change this convention.
3. **`useId()` for SVG `<defs>` IDs**: SVG IDs are document-global. Use React's `useId()` to scope per-instance — otherwise rendering two panels at once causes gradient/clip-path collisions.
4. **`Number.isFinite` guards on every numeric prop**: DB columns are nullable even when TS types say `number`. `isNaN(null) === false` in JS, so `isNaN`-based guards silently pass — use `Number.isFinite`.
5. **Math display pattern**: every formula renders symbolic-first, then with values substituted, then simplified, then result. Names anchor the numbers to earlier steps. Example: `= (RSI − p80) ÷ (p95 − p80) = (73.2 − 66.8) ÷ (77.9 − 66.8) = 6.4 ÷ 11.1`.
6. **Always render fallback prose**: components never return `null` into a step card — empty cards look broken. When inputs are non-finite, render the `Approximately X points.` prose (or equivalent).
7. **Frontend tests deferred**: accepted project policy for pure UI components built under this recipe. Acknowledge explicitly in the implementation PR or `hot.md` entry; don't re-litigate.

#### Verification
- `cd web && npm run build` clean.
- `cd web && npm test -- --run` green.
- Manual: click the indicator's matrix row, confirm all 7 steps render with real data for a representative ticker/date.
- `git diff --stat` matches only the files declared in the implementer's plan.

#### Docs
- Update `DESIGN.md` §15 with the new indicator's 7-step trace (mirror the RSI subsection format).
- Update `hot.md` per the session-handoff ritual.
- Update `CONFIG.md` only if new config keys were added.

### Modifying scoring thresholds:
1. Update `config/scorer.json`
2. Re-run scorer: `python scripts/run_scorer.py --force`
3. Run: `python scripts/verify_pipeline.py`
4. Check signal distribution changed as expected (not all same signal)
5. Check confidence distribution is reasonable
6. Check weighted score math still holds

### Modifying confidence modifiers:
1. Update `config/scorer.json` `confidence_modifiers` section
2. Re-run scorer: `python scripts/run_scorer.py --force`
3. Run: `python scripts/verify_pipeline.py`
4. Check confidence distribution shifted as expected
5. Verify no tickers have confidence > 100% or < 0%

### Verification output expectations:
- `verify_backfill.py`: all raw data checks should PASS
- `verify_pipeline.py`: all computed data checks should PASS
- If any check FAILS after a change, the change likely introduced a bug
- WARNINGS are acceptable (some are informational)
- Both scripts send results to admin Telegram for visibility

### Cron Job Management

All cron jobs are managed by deploy.sh between TICKER-TIDE-START and
TICKER-TIDE-END markers. Never ask the user to manually edit crontab.

When adding or modifying a cron job:
1. Update the cron block in deploy.sh
2. Update the cron job descriptions in deploy.sh output
3. Update OPERATIONS.md with the new schedule
4. Run deploy.sh to apply changes

Current cron jobs:
- Daily pipeline: 0 0 * * * (00:00 UTC) — scripts/run_daily.py
- Weekly verification: 0 6 * * 0 (06:00 UTC Sunday) — scripts/verify_pipeline.py
- Log cleanup: 0 6 * * 0 (06:00 UTC Sunday) — delete logs > 30 days

### Bot Service Management

The Telegram bot (scripts/run_bot.py) runs as a systemd service on EC2.
Never start it manually in tmux or as a background process.

deploy.sh installs deploy/ticker-tide-bot.service to /etc/systemd/system/
and runs systemctl enable + systemctl restart on every deploy.

When modifying the bot service:
1. Edit deploy/ticker-tide-bot.service
2. Update OPERATIONS.md if management commands change
3. Run deploy.sh to apply (or: sudo systemctl daemon-reload && sudo systemctl restart ticker-tide-bot)

## Dependencies
- pandas for data manipulation
- numpy for numerical computation
- ta for technical indicators (NOT TA-Lib, NOT pandas_ta)
- httpx for HTTP API calls (synchronous)
- tenacity for retry logic
- python-dotenv for environment variables
- anthropic for Claude API
- python-telegram-bot for Telegram
- yfinance for fundamentals data
- finnhub-python for earnings calendar
- pytest + pytest-mock for testing
