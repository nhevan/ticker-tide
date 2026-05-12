# DEVELOPMENT.md — Developer Guide

## Setup

```bash
git clone <repo-url> /home/ec2-user/ticker-tide
cd /home/ec2-user/ticker-tide
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/setup_db.py   # creates data/signals.db with full schema
```

For a fully populated dev database, run the backfill after setting up `.env`:

```bash
cp .env.example .env
# fill in all 5 keys
python scripts/test_api_access.py
python scripts/run_backfill.py
python scripts/run_calculator.py
python scripts/run_scorer.py --historical
```

---

## Running Tests

```bash
# Full test suite
python -m pytest tests/ -v

# Single module
python -m pytest tests/test_calculator/test_indicators.py -v

# Single test
python -m pytest tests/test_calculator/test_indicators.py::test_compute_ema_returns_expected_values -v

# All tests matching a name pattern
python -m pytest tests/ -v -k "test_regime"
```

Tests mock all external API calls (`pytest-mock`). No API keys are needed to run the test suite. `tests/conftest.py` provides shared fixtures:

| Fixture | Type | Description |
|---|---|---|
| `sample_ohlcv_dataframe` | `pd.DataFrame` | 30 days of AAPL OHLCV data |
| `sample_ticker_config` | `dict` | Single ticker dict matching `tickers.json` format |
| `sample_tickers_list` | `list[dict]` | List of ticker dicts |
| `db_connection` | `sqlite3.Connection` | Temporary DB with full schema; WAL mode enabled; uses `tmp_path` |

---

## Project Structure

### Module responsibilities

| Module | Responsibility |
|---|---|
| `src/common/api_client.py` | Polygon + Finnhub HTTP clients; `httpx` + `tenacity` retry (max 3, exponential backoff) |
| `src/common/config.py` | `load_config(name)` → reads `config/{name}.json`; `load_env()` → loads `.env`; `get_active_tickers()` |
| `src/common/db.py` | `get_connection(path)` → WAL mode + `row_factory`; `create_all_tables()` → idempotent schema creation; `run_migrations(conn)` → idempotent `ALTER TABLE … ADD COLUMN` migrations guarded by `PRAGMA table_info` (called by every pipeline entry point after `create_all_tables`) |
| `src/common/events.py` | `pipeline_events` read/write; `alerts_log` insert; `pipeline_runs` logging; trading day detection |
| `src/common/logger.py` | `setup_root_logging()` — call once per entry-point script; format: `[YYYY-MM-DD HH:MM:SS] LEVEL [module] msg` |
| `src/common/progress.py` | `ProgressTracker` class; `send_telegram_message(bot_token, chat_id, text, *, reply_markup=None)` — `reply_markup` accepts a python-telegram-bot `InlineKeyboardMarkup`-compatible dict for attaching inline buttons; `edit_telegram_message()` |
| `src/common/validators.py` | Input validation helpers (OHLCV row checks, date format, etc.) |
| `src/common/yfinance_client.py` | yfinance wrapper for fundamentals, VIX, earnings calendar |
| `src/backfiller/main.py` | Orchestrates all backfill phases; `sync_tickers_from_config()`; `run_full_backfill()` |
| `src/backfiller/ohlcv.py` | Polygon OHLCV fetch; handles ticker renames via `former_symbol`/`symbol_since` |
| `src/backfiller/fundamentals.py` | yfinance quarterly financials and ratios |
| `src/backfiller/earnings.py` | yfinance earnings calendar (~50 events per ticker) |
| `src/backfiller/corporate_actions.py` | Polygon dividends, splits, short interest |
| `src/backfiller/macro.py` | Polygon treasury yields; yfinance VIX |
| `src/backfiller/news.py` | Polygon + Finnhub news; AI sentiment extraction |
| `src/backfiller/filings.py` | Polygon 8-K filings |
| `src/backfiller/verify.py` | 10 raw data quality checks; `run_full_verification()` → `VerificationReport` |
| `src/backfiller/verify_pipeline.py` | ~45 computed data checks (indicators, scores, patterns, profiles, weekly/monthly parity, period integrity); `run_full_pipeline_verification()` → `VerificationReport`. Commit 8 added 13 new parity checks: `weekly_pattern_count`, `monthly_pattern_count`, `weekly_divergence_count`, `monthly_divergence_count`, `weekly_crossover_count`, `monthly_crossover_count`, `scores_weekly_table_coverage`, `scores_monthly_table_coverage`, `scores_weekly_score_range`, `scores_monthly_score_range`, `scores_weekly_category_math`, `scores_monthly_category_math`, `monthly_indicator_coverage`, plus the new period-integrity check `no_open_period_persisted`. The renamed `monthly_score_column_coverage` (formerly `monthly_score_coverage`) inspects the `scores_daily.monthly_score` column — disambiguated from the new `scores_monthly`-table check `scores_monthly_table_coverage`. |
| `src/fetcher/main.py` | Daily fetch orchestrator; gated on market calendar; writes `fetcher_done` event |
| `src/fetcher/earnings.py` | Periodic earnings calendar refresh |
| `src/fetcher/market_calendar.py` | `is_market_open_today()` via Polygon market holidays endpoint |
| `src/calculator/main.py` | `run_calculator(mode, target_date)` — orchestrates all sub-modules per ticker; `target_date` is the trading date the fetcher processed (yesterday UTC in daily pipeline) and drives the `fetcher_done` pre-flight check |
| `src/calculator/indicators.py` | 15 technical indicators via `ta` library → `indicators_daily` |
| `src/calculator/weekly.py` | Weekly OHLCV candles + weekly indicators → `weekly_candles`, `indicators_weekly`. After indicator persistence, `compute_weekly_for_ticker` runs a six-step sub-pipeline against the weekly mirror tables in dependency order: swing_points → S/R → patterns → divergences → crossovers → profiles. Each step is wrapped in its own try/except (mirrors the daily orchestrator pattern) and writes to `alerts_log` on failure under `phase='calculator-weekly'`. The keyword-only `skip_event_detection: bool = False` parameter bypasses all six sub-steps for ETFs/benchmarks (matches daily ETF policy). Both `mode='full'` and `mode='incremental'` re-run the sub-pipeline on the full ticker history because the detectors do not operate on a date window — acceptable because weekly bar counts are 5x fewer than daily. |
| `src/calculator/monthly.py` | Monthly OHLCV candles (YYYY-MM-01 key) + monthly indicators → `monthly_candles`, `indicators_monthly`. Same six-step sub-pipeline as weekly, against monthly mirror tables; failures land in `alerts_log` under `phase='calculator-monthly'`. The keyword-only `skip_event_detection: bool = False` parameter bypasses all six sub-steps for ETFs/benchmarks. Both modes re-run the sub-pipeline on the full ticker history (monthly bar counts are 22x fewer than daily, so the cost is bounded). |
| `src/calculator/profiles.py` | Percentile profiles (p5–p95); sector profile blending → `indicator_profiles` (default daily). Timeframe-parametrized: keyword-only `source_indicators_table`, `source_indicators_date_column`, `dest_table` route reads/writes to weekly (`indicators_weekly` → `indicator_profiles_weekly`) or monthly (`indicators_monthly` → `indicator_profiles_monthly`) mirrors. Identifiers are whitelist-validated. |
| `src/calculator/crossovers.py` | EMA and MACD crossover detection → `crossovers_daily` (default daily). Timeframe-parametrized: keyword-only `source_indicators_table`, `source_indicators_date_column`, `dest_table`, `dest_date_column` route reads/writes to weekly (`indicators_weekly` → `crossovers_weekly` keyed by `week_start`) or monthly (`indicators_monthly` → `crossovers_monthly` keyed by `month_start`) mirrors. Identifiers are whitelist-validated. |
| `src/calculator/gaps.py` | Gap classification (Breakaway/Continuation/Exhaustion/Common) → `gaps_daily` |
| `src/calculator/swing_points.py` | Swing high/low detection → `swing_points` (default daily). Timeframe-parametrized: keyword-only `source_candles_table`, `source_date_column`, `dest_table`, `date_column_name` route reads/writes to weekly (`weekly_candles` → `swing_points_weekly` keyed by `week_start`) or monthly (`monthly_candles` → `swing_points_monthly` keyed by `month_start`) mirrors. Identifiers are whitelist-validated. |
| `src/calculator/support_resistance.py` | Cluster swing points into S/R levels → `support_resistance` (default daily, keyed by `date_computed`). Timeframe-parametrized: keyword-only `source_swing_table`, `source_swing_date_column`, `source_candles_table`, `source_candles_date_column`, `dest_table`, `dest_date_column` route reads/writes to weekly mirrors (`swing_points_weekly` + `weekly_candles` → `support_resistance_weekly` keyed by `week_start`) or monthly mirrors (`swing_points_monthly` + `monthly_candles` → `support_resistance_monthly` keyed by `month_start`). Identifiers are whitelist-validated. |
| `src/calculator/patterns.py` | 7 candlestick + 7 structural patterns → `patterns_daily` (default daily). Trend-context window for hammer / shooting-star is read from `config["patterns"]["trend_context_candles"]` (default 5). Timeframe-parametrized: keyword-only `source_candles_table`, `source_candles_date_column`, `source_indicators_table`, `source_indicators_date_column`, `source_swing_table`, `source_swing_date_column`, `source_sr_table`, `dest_table`, `dest_date_column` route reads/writes to weekly or monthly mirror tables. Identifiers are whitelist-validated. |
| `src/calculator/divergences.py` | Regular/Hidden Bullish/Bearish divergences → `divergences_daily` (default daily). Timeframe-parametrized: keyword-only `source_swing_table`, `source_swing_date_column`, `source_indicators_table`, `source_indicators_date_column`, `dest_table`, `dest_date_column` route reads/writes to weekly (`swing_points_weekly` + `indicators_weekly` → `divergences_weekly`) or monthly mirrors. The persisted `indicator` value for RSI is `"rsi_14"` (matches the indicators-table column name and the scorer's filter). Identifiers are whitelist-validated. |
| `src/calculator/fibonacci.py` | Fibonacci retracement levels (on-the-fly; not stored) |
| `src/calculator/relative_strength.py` | RS vs SPY and sector ETF (on-the-fly; not stored) |
| `src/calculator/news_aggregator.py` | `news_articles` → `news_daily_summary` per ticker per day |
| `src/scorer/main.py` | `run_scorer()` and `run_historical_scoring()`; per-ticker `score_ticker()` pipeline. `final_score` in `scores_daily` is always the ±100 merged timeframe composite; `calibrated_score` is the ridge prediction (≈ ±2–15%) or NULL; `effective_score` (local only, never persisted) drives signal classification. After `save_score_to_db`, `score_ticker` calls `persist_weekly_score_row` + `persist_monthly_score_row` (commit 6) to write closed-period snapshots; per-step try/except + `alerts_log` warning ensure persistence failures cannot break daily scoring. `run_historical_scoring(mode=...)` accepts `daily`, `weekly`, `monthly`, `both` (back-compat alias — now also covers monthly), and `all`. Helpers `_get_weekly_dates` / `_get_monthly_dates` drive the respective iterations. |
| `src/scorer/period_gate.py` | Closed-period gate helpers used by the persistence layer. `is_week_closed(week_start, scoring_date)` returns True when `scoring_date >= week_start + 7 days` (Sunday is mid-week, Monday closes). `is_month_closed(month_start, scoring_date)` returns True when `scoring_date` falls in any later `(year, month)` than `month_start`. |
| `src/scorer/persistence.py` | `persist_weekly_score_row` + `persist_monthly_score_row` — closed-period writers for `scores_weekly` / `scores_monthly`. Resolve the latest `indicators_*.{week_start,month_start} <= scoring_date`, apply the closed-period gate, inherit `fundamental_score` + `macro_score` from the most recent in-period `scores_daily` row via `_inherit_fundamental_macro` (period_end = `week_start + 4 days` for weekly, `calendar.monthrange(year, month)[1]` for monthly), and write via `INSERT OR REPLACE`. `data_completeness` and `key_signals` are JSON-serialised to TEXT. Both helpers also accept an `indicator_scores: Optional[dict[str, Optional[float]]] = None` kwarg; when provided, per-indicator signed scores are written to `indicator_scores_weekly` / `indicator_scores_monthly` using the same resolved period key. `persist_indicator_scores_daily` handles the daily sidecar, called from `score_ticker` after `save_score_to_db`. All three sidecar writes are try/except-isolated and do not raise. |
| `src/scorer/regime.py` | Trending/Ranging/Volatile detection from ADX, ATR, VIX; EMA stack alignment override (close/EMA9/EMA21/EMA50 fully aligned → Trending even with low ADX) |
| `src/scorer/indicator_scorer.py` | Maps indicator values → [−100, +100] using percentile profiles; momentum oscillators (RSI, Stochastic %K, CCI, Williams %R) accept a `regime` parameter — `"trending"` flips to `higher_is_bullish=True` (trend-continuation), `"ranging"`/`"volatile"` use mean-reversion. `load_profile_for_ticker(db_conn, ticker, *, source_table="indicator_profiles")` reads from a whitelisted profile table (`indicator_profiles`, `indicator_profiles_weekly`, or `indicator_profiles_monthly`); raises `ValueError` for any other value (mirrors the parametrization pattern used in commit 2/3). |
| `src/scorer/pattern_scorer.py` | Scores patterns, divergences, crossovers, gaps, Fibonacci, news, fundamentals, macro |
| `src/scorer/category_scorer.py` | Aggregates component scores into 9 categories; applies adaptive weights |
| `src/scorer/sector_adjuster.py` | Sector ETF trend score → ±5 to ±10 adjustment on final score |
| `src/scorer/timeframe_merger.py` | 3-way merge of daily + weekly + monthly into composite score using regime-adaptive weights (trending: 0.10d/0.50w/0.40m, ranging: 0.60d/0.30w/0.10m, volatile: 0.25d/0.45w/0.30m); weights renormalized when a timeframe is absent. Public scoring API: `compute_weekly_score_breakdown()` / `compute_monthly_score_breakdown()` return a 7-key dict (`composite_score`, 4 main categories, `candlestick_score`, `structural_score`); thin shims `compute_weekly_score()` / `compute_monthly_score()` return just the composite scalar (used by `src/scorer/main.py`). Mode is gated on `config['weekly_score_method']` / `config['monthly_score_method']` ∈ {`v1_4cat`, `v2_8cat`} — defaults to `v1_4cat`. v2 also reads `patterns_*`, `divergences_*`, `crossovers_*` (with `week_start AS date` / `month_start AS date` aliasing) and routes them through `pattern_scorer`. Profiles come from `indicator_profiles_weekly` / `_monthly` with daily fallback (logs INFO once per ticker). Requires `scoring_date` and `regime`. Monthly candlestick category is permanently `None` (decay-window mismatch). |
| `src/scorer/calibrator.py` | Rolling ridge regression calibrator: trains on recent signals + realized 10-day excess returns (vs SPY), predicts expected excess return for current signal; 17 features (6 category scores + 6 raw indicators + 3 EMA spreads + weekly_score + monthly_score); cold-start fallback when < 30 samples; `calibrate_score()` is the main entry point |
| `src/scorer/confidence.py` | Signal classification; confidence modifiers; `data_completeness`; `key_signals` |
| `src/scorer/contribution.py` | Builds the per-indicator/per-pattern contribution payload that backs `/why`; called by `score_ticker` after `apply_adaptive_weights` and persisted as `scores_daily.key_signals_data` JSON |
| `src/scorer/flip_detector.py` | Detects signal direction changes → `signal_flips` |
| `src/notifier/main.py` | `run_notifier()` — queries scores, calls AI reasoner, formats, sends Telegram |
| `src/notifier/ai_reasoner.py` | `reason_all_qualifying_tickers()` — Claude API calls per qualifying ticker |
| `src/notifier/sentiment_enrichment.py` | Finnhub sentiment enrichment via Claude Haiku; `run_sentiment_enrichment()` + `enrich_batch()` |
| `src/notifier/formatter.py` | Formats full report, heartbeat, and no-signals variants |
| `src/notifier/telegram.py` | Telegram send/edit helpers |
| `src/notifier/bot.py` | Long-polling bot; `/detail`, `/scatter`, `/tickers`, `/help`, `/why` handlers; `CallbackQueryHandler(pattern="^why:")` handles inline button taps from `/detail` msg #3; logs every incoming command to `telegram_message_log` |
| `src/notifier/why_command.py` | Backend formatters for `/why` (default, all, drill-down modes): `dispatch_why`, `format_why_default`, `format_why_all`, `format_why_drilldown`, `load_why_payload`, `resolve_name_token`. Public entry point used by the bot wrappers: `handle_why_command(db_conn, chat_id, message_text, bot_token, configs)`. Callback data format is `"why:{TICKER}"`, validated in `bot.py` by `_WHY_TICKER_PATTERN` regex; `await query.answer()` is in a `finally` block so the Telegram loading spinner is always dismissed even on error. |
| `src/notifier/tickers_command.py` | `/tickers` Telegram bot command handler; logs invocations to `telegram_message_log` |
| `src/notifier/scatter_command.py` | `/scatter` bot command handler; queries `scores_daily` + `ohlcv_daily` to compute N-day forward excess returns (vs SPY), plots calibrated_score (predicted) vs actual excess return scatter chart with IC annotation (Spearman rank correlation via `compute_ic()`), sends PNG via Telegram |
| `src/web/app.py` | `create_app(db_path, config, dist_dir=None)` FastAPI JSON API factory. Sets up `SessionMiddleware`, registers JSON routes under `/api/*`, conditionally mounts `/assets` StaticFiles (only when `dist_dir/assets` exists), adds explicit `/favicon.ico` + `/robots.txt` handlers, and registers the SPA catch-all last. Catch-all returns 503 JSON when `dist/index.html` is absent; returns 404 JSON for `/api/*` paths that don't match any route. `dist_dir` defaults to `None` (useful in tests — pass a `tmp_path` to exercise static-serve). Per-(session, ticker, date, timeframe) in-memory LLM debounce stored in closure dict (isolated per `create_app()` call). Single worker required; debounce is process-local. |
| `src/web/auth.py` | `is_correct_password(submitted, expected)` — constant-time comparison via `secrets.compare_digest`. `record_login_attempt(conn, ip)` — writes UTC timestamp to `web_login_attempts`. `check_rate_limit(conn, ip, config)` — counts rows within window. `prune_old_login_attempts(conn)` — deletes rows older than 1 hour (called on each login attempt). |
| `src/web/queries.py` | `fetch_active_tickers(conn)`, `fetch_date_range(conn, ticker)`, `fetch_snapshot(conn, ticker, date, config)`. `fetch_snapshot` returns a 3-key dict (`daily`, `weekly`, `monthly`) each with `data_available`, `categories` (UI contract array), `scores`, `indicators`, `patterns`, `recent_patterns`, `sparkline`, and period metadata. Sparkline applies strict `<= picked_date` bound. Monthly categories array permanently excludes `"candlestick"` (decay-window mismatch). Daily section additionally includes three enrichment fields (daily-only): `key_signals` (top-N why-bullets from `scores_daily.key_signals` via `_extract_key_signals()`), `earnings` (`{next, last_surprise}` from `earnings_calendar` via `_fetch_earnings()`), and `signal_flip` (most-recent flip within lookback window from `signal_flips` via `_fetch_signal_flip()`). `_fetch_recent_patterns()` queries patterns within the scorer's canonical recency windows (constants imported from `pattern_scorer.py`) and is wired into all three section builders. |
| `src/web/llm.py` | `build_daily_context(conn, ticker, score_row, date)` — wraps `build_ticker_context()` from `ai_reasoner.py` (full context: indicators, patterns, news, fundamentals, macro). `build_timeframe_context(conn, ticker, date, timeframe)` — weekly/monthly only; reads `indicators_{weekly,monthly}` and `patterns_{weekly,monthly}` directly; does NOT include news/fundamentals/macro (daily-only scope). `analyze_daily()` / `analyze_timeframe()` — prompt builders + `call_claude()` via thin config adapter. `call_claude_for_web()` — single dispatch entry point used by `/api/llm`. |

### Module dependency graph

```
common/
  api_client      ← (no internal deps)
  yfinance_client ← (no internal deps)
  config          ← (no internal deps)
  db              ← (no internal deps)
  events          ← db
  logger          ← (no internal deps)
  progress        ← config, logger
  validators      ← (no internal deps)

backfiller        ← api_client, yfinance_client, validators, events, progress, config, db
fetcher           ← api_client, yfinance_client, validators, events, progress, config, db
calculator        ← config, db, events, progress  (ta library for indicators)
scorer            ← config, db, events, progress, calculator output tables
notifier          ← config, db, events, progress, anthropic, telegram
web (backend)     ← common/db, notifier/ai_reasoner (build_ticker_context + call_claude), fastapi
web (frontend)    ← React, TanStack Query, React Router, Recharts, Tailwind (built by Vite)
```

---

## Frontend Local Development

### Prerequisites

- Node 20 LTS (build-time only; not needed at runtime on EC2)
- npm (bundled with Node)

### Setup

```bash
cd web && npm install
```

### Dev server

```bash
# Terminal 1: FastAPI JSON API (port 8765)
source .venv/bin/activate
python scripts/run_web.py

# Terminal 2: Vite dev server (port 5173, proxies /api/* to :8765)
cd web && npm run dev
```

Open `http://localhost:5173`. Hot-module replacement works out of the box.

### Tests, build, and lint

```bash
cd web

# Run Vitest tests (unit + component)
npm run test

# One-shot run (no watch mode)
npm run test -- --run

# Production build → web/dist/
npm run build

# Lint (ESLint 9 flat config)
npm run lint
```

### Component and file locations

| Location | Purpose |
|---|---|
| `web/src/pages/` | Route-level page components (`LoginPage`, `DashboardPage`) |
| `web/src/components/` | Shared domain components (`TimeframeCard`, `AskAI`, `CategoryBars`, etc.) |
| `web/src/components/ui/` | shadcn-style primitives (`button`, `card`, `input`, `badge`, `skeleton`) |
| `web/src/lib/api/` | `client.ts` (fetch wrapper), `endpoints.ts` (typed functions), `types.ts` (shapes) |
| `web/src/lib/hooks/` | TanStack Query hooks (`useMe`, `useSnapshot`, `useTickers`, `useDateRange`, `useLlm`) |
| `web/src/lib/scoring/categoryMap.ts` | `INDICATOR_CATEGORY_MAP` + `INDICATOR_DISPLAY_LABELS` (12 entries) — mirrors Python's `category_scorer.py`; drift-guarded by `tests/web/test_category_map_sync.py` |
| `web/src/lib/scoring/patternLabels.ts` | `PATTERN_DISPLAY_LABELS` map + `humanizePatternName()` utility — converts snake_case DB pattern names to human-readable labels for the matrix pattern rows |
| `web/src/styles/globals.css` | Tailwind directives + shadcn CSS variables |
| `web/public/` | `favicon.ico`, `robots.txt` — Vite copies to `dist/` root verbatim |
| `web/dist/` | Vite build output (gitignored; produced by CI `build-frontend` job) |

### Adding a shadcn component

```bash
cd web
npx shadcn-ui@latest add <component>
# e.g.: npx shadcn-ui@latest add dialog
```

Shadcn components land in `web/src/components/ui/`.

### Adding a new API endpoint (4 steps)

1. **Backend route** in `src/web/app.py` — add above the catch-all comment.
2. **Typed function** in `web/src/lib/api/endpoints.ts` — use `apiFetch<T>()`.
3. **TanStack hook** in `web/src/lib/hooks/` — `useQuery` for GET, `useMutation` for POST.
4. **Consume hook** in the relevant page or component.

### MatrixTable categories prop

`MatrixTable` accepts a `categories` prop (`DailyCategory[] | WeeklyCategory[] | MonthlyCategory[]`) that drives both the column headers and the per-row cell iteration. `DashboardPage.tsx` passes `snapshot.{daily,weekly,monthly}.categories` from the API response. This fixes the pre-existing 9-column rendering bug on monthly where the monthly section (5 categories, no candlestick) was previously rendered with the full 9-column header set from the hardcoded `CATEGORIES` constant.

The `patternLabels.ts` module is the single source of truth for pattern name display labels in the frontend. When new patterns are added to `src/calculator/patterns.py` or `src/scorer/pattern_scorer.py`, also add an entry to `PATTERN_DISPLAY_LABELS` in `web/src/lib/scoring/patternLabels.ts`. The fallback `humanizePatternName()` function will title-case unknown names, so omitting an entry is safe but produces less precise labels.

### `web/dist` convention

`dist_dir` is hardcoded to `web/dist` relative to repo root in `scripts/run_web.py`. It is intentionally NOT a config key. Do not add it to `config/web.json`. Override is available only via the `dist_dir` parameter to `create_app()` for tests.

---

## Adding a New Indicator

1. **Add params to `config/calculator.json`** under `indicators`:

    ```json
    "my_indicator_period": 10
    ```

2. **Implement in `src/calculator/indicators.py`** — add computation inside `compute_indicators_for_ticker()`:

    ```python
    # uses ta library
    df["my_indicator"] = ta.trend.my_indicator(df["close"], window=config["indicators"]["my_indicator_period"])
    ```

3. **Add column to schema in `src/common/db.py`** — inside `_build_schema_statements()`, add the column to both `indicators_daily` and `indicators_weekly` CREATE TABLE statements:

    ```sql
    my_indicator REAL,
    ```

    Run `python scripts/setup_db.py` — it uses `CREATE TABLE IF NOT EXISTS` so it will not touch existing data, but new columns require `ALTER TABLE` for existing DBs:

    ```bash
    python -c "
    import sqlite3
    conn = sqlite3.connect('data/signals.db')
    conn.execute('ALTER TABLE indicators_daily ADD COLUMN my_indicator REAL')
    conn.execute('ALTER TABLE indicators_weekly ADD COLUMN my_indicator REAL')
    conn.commit()
    conn.close()
    "
    ```

4. **Add to percentile profiles in `src/calculator/profiles.py`** — include `"my_indicator"` in the list of tracked indicators so it gets p5/p50/p95 computed.

5. **Add scoring logic in `src/scorer/indicator_scorer.py`** — map the indicator value to [−100, +100] using its percentile profile, following the existing pattern for RSI, ADX, etc. If the indicator is a momentum oscillator where overbought/oversold interpretation changes between trending and ranging markets, pass `oscillator_higher_is_bullish` (derived from `regime == "trending"`) as the `higher_is_bullish` argument, consistent with RSI, Stochastic %K, CCI, and Williams %R.

5b. **Register in category and contribution maps:**
    - Add the indicator key to `INDICATOR_CATEGORY_MAP` in `src/scorer/category_scorer.py` so the category rollup and the `/why` contribution builder both know which category it belongs to.
    - If the indicator bypasses the percentile-profile path (i.e., it is scored by a fixed formula rather than a profile lookup), add it to `PROFILE_FREE_INDICATORS` in `src/scorer/indicator_scorer.py`.
    - If the indicator also uses a fixed discrete ladder (e.g., regime-based step scores rather than linear interpolation), add it to `FIXED_LADDER` in `src/scorer/indicator_scorer.py`.

5c. **Mirror the category map entry in the frontend:**
    - Add the same indicator key and category to `INDICATOR_CATEGORY_MAP` in `web/src/lib/scoring/categoryMap.ts` so the dashboard indicator-agreement matrix renders the new row.
    - Add a human-readable label to `INDICATOR_DISPLAY_LABELS` in the same file.
    - The drift-guard test `tests/web/test_category_map_sync.py` will fail loudly if the Python and TypeScript maps fall out of sync.

5d. **Add a zone classifier in `src/scorer/zone_labels.py`** (optional but recommended for indicators that will have their own explainer panel):
    - Follow the `zone_label_for_rsi()` pattern: one function for the profile path (six labels using the same percentile boundaries as `score_with_percentile`) and one for the fallback path (fixed thresholds).
    - Add a corresponding block in `src/web/queries.py` `_build_daily_section` to populate the zone label in the snapshot.
    - Add zone label descriptions to `IndicatorExplainerPanel.tsx` and replace the placeholder with a real step trace.
    - Add drift-guard tests in `tests/test_scorer/test_zone_labels.py` following the RSI pattern.

6. **Write tests first (TDD)**:
    - `tests/test_calculator/test_indicators.py` — test that the value is computed and stored correctly
    - `tests/test_scorer/test_indicator_scorer.py` — test the score mapping

7. **Re-run calculator and scorer in full mode** to populate the new column and backfill the sidecar tables:

    ```bash
    python scripts/run_calculator.py --mode full
    python scripts/run_scorer.py --historical --force
    ```

    The `--force` flag ensures `indicator_scores_*` sidecar rows are written for all historical dates. Without it, the dashboard matrix will show empty cells for old dates.

---

## Adding a New Pattern

1. **Add detection params to `config/calculator.json`** under `patterns`.

2. **Implement detection in `src/calculator/patterns.py`** inside `detect_all_patterns_for_ticker()`.

3. **Add scoring logic in `src/scorer/pattern_scorer.py`** under the appropriate category (candlestick, structural, etc.).

4. **Register in category and contribution maps:**
    - Add the pattern key to `PATTERN_CATEGORY_MAP` in `src/scorer/category_scorer.py` so the category rollup and the `/why` contribution builder classify it correctly.
    - Add a human-readable description string to `PATTERN_RULE_DESCRIPTIONS` in `src/scorer/pattern_scorer.py` so `/why` drill-down can explain the rule.

5. **Write tests first (TDD)** — `tests/test_calculator/test_patterns.py` and `tests/test_scorer/test_pattern_scorer.py`.

6. **Re-run calculator and scorer:**

    ```bash
    python scripts/run_calculator.py --mode full
    python scripts/run_scorer.py --historical
    python scripts/verify_pipeline.py
    ```

---

## Adding a New Data Source

1. **Add a client** in `src/common/` (e.g. `my_source_client.py`). Use `httpx` for HTTP calls; wrap retries with `tenacity`. Follow the `PolygonClient` pattern.

2. **Add a backfiller module** in `src/backfiller/my_source.py`:
    - Implement `backfill_my_data_for_ticker(db_conn, ticker, config)` → fetches and stores data.
    - Handle `skip_if_fresh_days` pattern: check last fetch date before calling the API.
    - Add config keys to `config/backfiller.json`.

3. **Register the phase in `src/backfiller/main.py`** — add `"my_source"` to the `VALID_PHASES` list and call the new module from `run_full_backfill()`.

4. **Add a fetcher module** in `src/fetcher/my_source.py` for daily updates, following the `fetcher/earnings.py` pattern.

5. **Wire into daily fetch** in `src/fetcher/main.py`.

6. **Add to schema** in `src/common/db.py` — new table in `_build_schema_statements()`.

7. **Surface in scorer** if relevant — add scoring logic in `src/scorer/pattern_scorer.py` under the appropriate category.

8. **Write tests** in `tests/test_backfiller/test_my_source.py` and `tests/test_fetcher/test_my_source.py` — mock all HTTP calls.

---

## Database Migrations

Schema is created in `src/common/db.py` → `_build_schema_statements()`. All statements use `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`, so re-running `setup_db.py` is safe but will not add new columns to existing tables.

### Idempotent column migrations via `run_migrations(conn)`

For lightweight column additions (no PRIMARY KEY change, no type change), prefer the `run_migrations` pattern over a standalone migration script. `src/common/db.py::run_migrations(conn)` runs a series of `ALTER TABLE … ADD COLUMN` statements, each guarded by a `PRAGMA table_info` pre-check so re-running the function on an already-migrated database is safe:

```python
# Pattern used inside run_migrations():
existing = {row[1] for row in conn.execute("PRAGMA table_info(scores_daily)")}
if "my_new_column" not in existing:
    conn.execute("ALTER TABLE scores_daily ADD COLUMN my_new_column TEXT")
    conn.commit()
```

`run_migrations` is called from every pipeline entry point (`scripts/run_scorer.py`, `scripts/run_daily.py`, `scripts/run_bot.py`) immediately after `create_all_tables`, so new columns are added on first deploy without a manual step. This is appropriate for nullable columns where `NULL` is a valid "not yet computed" sentinel. For non-nullable columns, `DEFAULT` constraints, or structural changes, use a standalone migration script instead.

`run_migrations` also creates the three indicator-score sidecar tables (`indicator_scores_daily`, `indicator_scores_weekly`, `indicator_scores_monthly`) on existing databases via idempotent `CREATE TABLE IF NOT EXISTS` statements. Fresh databases get them through `create_all_tables` directly.

### Add a new column to an existing table

1. Add the column to the `CREATE TABLE` statement in `_build_schema_statements()`.

2. Run an `ALTER TABLE` against the existing database:

    ```bash
    python -c "
    import sqlite3
    conn = sqlite3.connect('data/signals.db')
    conn.execute('ALTER TABLE scores_daily ADD COLUMN my_new_column REAL')
    conn.commit()
    conn.close()
    print('Migration complete')
    "
    ```

3. Backfill the new column if needed by re-running the relevant phase with `--mode full`.

### Add a new table

Add the `CREATE TABLE IF NOT EXISTS` statement to `_build_schema_statements()` and run:

```bash
python scripts/setup_db.py
```

### Change a table's PRIMARY KEY or add a constraint

SQLite does not support `ALTER TABLE` to modify a PRIMARY KEY. Use the create-new/copy/drop/rename pattern:

1. Update the `CREATE TABLE` statement in `src/common/db.py` with the new schema.
2. Write a migration script in `scripts/migrate_<description>.py`:
    ```python
    conn.execute("CREATE TABLE my_table_new (..., PRIMARY KEY (col_a, col_b))")
    conn.execute("INSERT INTO my_table_new SELECT * FROM my_table")
    conn.execute("DROP TABLE my_table")
    conn.execute("ALTER TABLE my_table_new RENAME TO my_table")
    # Recreate any indexes that were on the old table
    conn.execute("CREATE INDEX IF NOT EXISTS idx_... ON my_table(...)")
    ```
3. Wrap all SQL in a single `with conn:` transaction so failures roll back cleanly.
4. Verify row count before and after — they must match.
5. Run the migration: `python scripts/migrate_<description>.py`

**Example:** `scripts/migrate_news_articles_pk.py` changes `news_articles` PRIMARY KEY from `id TEXT` to `(id, ticker)` so that the same Polygon article can be stored independently for each ticker that mentions it.

### Weekly / monthly parity tables

`src/common/db.py` defines a set of weekly and monthly mirror tables that share the same column shape as their daily counterparts but key on `week_start` or `month_start` instead of `date`. They are created by `setup_db.py` (or backfilled with `scripts/migrate_add_timeframe_parity.py` for already-deployed databases):

| Daily table | Weekly mirror | Monthly mirror |
|---|---|---|
| `swing_points` | `swing_points_weekly` | `swing_points_monthly` |
| `support_resistance` | `support_resistance_weekly` | `support_resistance_monthly` |
| `patterns_daily` | `patterns_weekly` | `patterns_monthly` |
| `divergences_daily` | `divergences_weekly` | `divergences_monthly` |
| `crossovers_daily` | `crossovers_weekly` | `crossovers_monthly` |
| `indicator_profiles` | `indicator_profiles_weekly` | `indicator_profiles_monthly` |

Two new score snapshot tables are also part of this set:

- `scores_weekly` — composite PRIMARY KEY `(ticker, week_start)`. Stores the weekly composite, regime, eight category scores, `data_completeness`, and a JSON `key_signals` array.
- `scores_monthly` — same shape, PRIMARY KEY `(ticker, month_start)`.

Each parity table has a matching `idx_<table>_ticker_<datecol>` index. Adding a new mirror table follows the standard "Add a new table" recipe above; just remember to:

1. Add the `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` to `_build_schema_statements()`.
2. Append the table to `tests/test_common/test_db.py` (`ALL_TABLES`, `EXPECTED_INDEXES`, and a column-shape test).
3. Add the table + index DDL to `scripts/migrate_add_timeframe_parity.py` so already-deployed databases pick it up.
4. Run `python scripts/setup_db.py` (or the migration) and re-run the test suite.
5. **Add a `verify_pipeline` check** for the new mirror table — see commit 8's parity checks (`weekly_pattern_count`, `scores_weekly_category_math`, `no_open_period_persisted`, etc.) for the pattern. Each parity check should: (a) accept `db_conn` plus relevant args, (b) return a `CheckResult` with a unique `name`, (c) read its thresholds from `config/verify_pipeline.json` via `_load_verify_threshold`, (d) be wired into `run_full_pipeline_verification` under the right section heading, and (e) ship with pass + fail/warn + edge-case tests using the `_insert_*` helpers in `tests/test_backfiller/test_verify_pipeline.py`. Score-table coverage checks must INNER JOIN against the corresponding indicators table so warm-up tickers (candles but no indicators) are not falsely flagged. Composite-math checks should accept either v1 or v2 weight sets within `category_math_tolerance` to allow a `weekly_score_method` flip mid-history.

#### Timeframe-parametrized calculator modules

Six calculator modules accept keyword-only timeframe overrides. Defaults preserve daily behaviour; pass weekly/monthly identifiers to drive the mirror tables. All identifiers are validated against an internal whitelist — passing an unknown name raises `ValueError` before any SQL runs (SQLite parameter binding does not cover identifiers, hence the explicit allow-list pattern).

| Module | Function(s) | Daily defaults | Weekly mirrors | Monthly mirrors |
|---|---|---|---|---|
| `swing_points.py` | `detect_swing_points_for_ticker`, `save_swing_points_to_db` | `ohlcv_daily` → `swing_points` keyed by `date` | `weekly_candles` → `swing_points_weekly` keyed by `week_start` | `monthly_candles` → `swing_points_monthly` keyed by `month_start` |
| `support_resistance.py` | `detect_support_resistance_for_ticker`, `save_sr_levels_to_db` | `swing_points` + `ohlcv_daily` → `support_resistance` keyed by `date_computed` | `swing_points_weekly` + `weekly_candles` → `support_resistance_weekly` keyed by `week_start` | `swing_points_monthly` + `monthly_candles` → `support_resistance_monthly` keyed by `month_start` |
| `patterns.py` | `detect_all_patterns_for_ticker`, `save_patterns_to_db` | `ohlcv_daily` + `indicators_daily` + `swing_points` + `support_resistance` → `patterns_daily` keyed by `date` | weekly equivalents → `patterns_weekly` keyed by `week_start` | monthly equivalents → `patterns_monthly` keyed by `month_start` |
| `divergences.py` | `detect_divergences_for_ticker`, `save_divergences_to_db` | `swing_points` + `indicators_daily` → `divergences_daily` keyed by `date` | `swing_points_weekly` + `indicators_weekly` → `divergences_weekly` keyed by `week_start` | `swing_points_monthly` + `indicators_monthly` → `divergences_monthly` keyed by `month_start` |
| `crossovers.py` | `detect_crossovers_for_ticker`, `save_crossovers_to_db` | `indicators_daily` → `crossovers_daily` keyed by `date` | `indicators_weekly` → `crossovers_weekly` keyed by `week_start` | `indicators_monthly` → `crossovers_monthly` keyed by `month_start` |
| `profiles.py` | `compute_profile_for_ticker`, `compute_sector_profile`, `compute_all_profiles` | `indicators_daily` → `indicator_profiles` | `indicators_weekly` → `indicator_profiles_weekly` | `indicators_monthly` → `indicator_profiles_monthly` |

Note: the `indicator_profiles*` tables have no per-row date column — only `window_start` / `window_end` text fields — so the profile callers expose only `source_indicators_table` / `source_indicators_date_column` / `dest_table` (no destination date-column override).

```python
from src.calculator.swing_points import detect_swing_points_for_ticker
from src.calculator.support_resistance import detect_support_resistance_for_ticker
from src.calculator.patterns import detect_all_patterns_for_ticker
from src.calculator.divergences import detect_divergences_for_ticker
from src.calculator.crossovers import detect_crossovers_for_ticker
from src.calculator.profiles import compute_profile_for_ticker

# Daily (defaults — original behaviour, no kwargs needed):
detect_swing_points_for_ticker(db_conn, ticker, config)
detect_support_resistance_for_ticker(db_conn, ticker, config)
detect_all_patterns_for_ticker(db_conn, ticker, config)
detect_divergences_for_ticker(db_conn, ticker, config)
detect_crossovers_for_ticker(db_conn, ticker, config)
compute_profile_for_ticker(db_conn, ticker, config)

# Weekly (selected examples — full set follows the same pattern):
detect_swing_points_for_ticker(
    db_conn, ticker, config,
    source_candles_table="weekly_candles",
    source_date_column="week_start",
    dest_table="swing_points_weekly",
    date_column_name="week_start",
)
detect_all_patterns_for_ticker(
    db_conn, ticker, config,
    source_candles_table="weekly_candles",
    source_candles_date_column="week_start",
    source_indicators_table="indicators_weekly",
    source_indicators_date_column="week_start",
    source_swing_table="swing_points_weekly",
    source_swing_date_column="week_start",
    source_sr_table="support_resistance_weekly",
    dest_table="patterns_weekly",
    dest_date_column="week_start",
)
detect_divergences_for_ticker(
    db_conn, ticker, config,
    source_swing_table="swing_points_weekly",
    source_swing_date_column="week_start",
    source_indicators_table="indicators_weekly",
    source_indicators_date_column="week_start",
    dest_table="divergences_weekly",
    dest_date_column="week_start",
)
detect_crossovers_for_ticker(
    db_conn, ticker, config,
    source_indicators_table="indicators_weekly",
    source_indicators_date_column="week_start",
    dest_table="crossovers_weekly",
    dest_date_column="week_start",
)
compute_profile_for_ticker(
    db_conn, ticker, config,
    source_indicators_table="indicators_weekly",
    source_indicators_date_column="week_start",
    dest_table="indicator_profiles_weekly",
)

# Monthly: substitute monthly_candles / indicators_monthly / swing_points_monthly /
# support_resistance_monthly / month_start / *_monthly mirror tables.
```

The `*_to_db` save helpers (`save_swing_points_to_db`, `save_sr_levels_to_db`, `save_patterns_to_db`, `save_divergences_to_db`, `save_crossovers_to_db`) carry the matching `dest_table` / `date_column_name` keyword-only parameters with the same daily defaults.

#### Per-timeframe sub-pipeline (commit 4)

In production, callers do **not** invoke the six parameterized detectors directly for the weekly/monthly timeframes. Instead, `compute_weekly_for_ticker(...)` and `compute_monthly_for_ticker(...)` run the sub-pipeline internally after candles + indicators have been persisted:

1. `swing_points_weekly` / `swing_points_monthly`
2. `support_resistance_weekly` / `support_resistance_monthly` (depends on 1)
3. `patterns_weekly` / `patterns_monthly` (depends on 1 + 2)
4. `divergences_weekly` / `divergences_monthly` (depends on 1)
5. `crossovers_weekly` / `crossovers_monthly`
6. `indicator_profiles_weekly` / `indicator_profiles_monthly`

Each sub-step is wrapped in its own try/except — a single detector failure logs to `alerts_log` (`phase='calculator-weekly'` or `'calculator-monthly'`) but does not block the rest. Both `mode='full'` and `mode='incremental'` re-run all six sub-steps against the **full ticker history** every call, because none of the detectors operate on a date window. The cost is acceptable: weekly bar counts are 5× fewer than daily and monthly bar counts 22× fewer.

`compute_*_for_ticker(..., skip_event_detection=True)` bypasses all six sub-steps. This matches the daily ETF policy: in `run_calculator_for_etfs_and_benchmarks`, ETFs and market benchmarks only get candles + indicators (no swing/SR/patterns/divergences/crossovers/profiles). Regular-ticker callers in `run_calculator_for_one_ticker` pass `skip_event_detection=False` (the default) explicitly for documentation clarity.

Note: `config['profiles']['rolling_window_days']` is daily-tuned (default 504 trading days). On weekly/monthly bars this exceeds available history; `compute_profile_for_ticker` falls back to using all available data with a warning, which is acceptable.

#### Latent rsi_14 divergence bug (fixed)

Prior to commit 3 of the weekly/monthly parity work, `detect_all_divergences()` stored divergences for the RSI indicator under `indicator='rsi'` while the scorer's `_load_divergences` filter looked for `indicator='rsi_14'`. The scorer therefore silently contributed zero to the daily RSI-divergence score for every ticker. The fix standardises the persisted indicator name to `'rsi_14'` (matching the indicators-table column name) so the scorer filter actually matches. Regression tests live in `tests/test_calculator/test_divergences.py::test_rsi_divergence_stored_indicator_is_rsi_14` and `tests/test_calculator/test_divergences.py::test_scorer_filter_picks_up_rsi_14_divergence`. After deploying this commit, recompute divergences (`python scripts/run_calculator.py --mode full`) so that historical rows are rewritten under the new value before re-running the scorer.

#### Calibrator acceptance gate (commit 7)

| File | Responsibility |
|---|---|
| `src/scorer/acceptance_gate.py` | Pure helpers: `compute_calibrated_score_distribution`, `find_latest_scoring_date_with_calibration`, `validate_snapshot_compatibility`, `compare_distributions`. No DB writes, no Telegram. |
| `scripts/check_calibrator_acceptance.py` | CLI wrapping the helpers. Subcommands `snapshot` and `check`. Loads `.env`, sends Telegram (wrapped in try/except so a Telegram outage doesn't change the gate's exit code). |
| `tests/test_scorer/test_acceptance_gate.py` | Module-level unit tests (distribution math, validation, comparison, three-tier classification). |
| `tests/test_scripts/test_check_calibrator_acceptance.py` | CLI-level tests via `monkeypatch.setattr(sys, 'argv', ...)` + `SystemExit` capture (no subprocess). Telegram patched. |

Procedure for flipping `weekly_score_method`: see OPERATIONS.md.

---

## Code Conventions

→ [CLAUDE.md](CLAUDE.md)

Key rules that affect most code changes:

- **TDD** — write failing tests before implementing
- **No magic numbers** — every threshold and period comes from a config file
- **Error handling** — catch specific exceptions; log with ticker + phase + date; `continue` to next ticker; never abort the full pipeline
- **Parameterized SQL** — always use `?` placeholders; never f-string SQL
- **WAL mode** — every `get_connection()` call enables WAL; never open SQLite directly without it
