# DESIGN.md — Stock Signal Engine

## 1. Overview

A signal generation engine that analyzes ~50 US stock tickers and 3 index ETFs (QQQ, VOO, DIA) daily and produces:
- Signal: BULLISH / BEARISH / NEUTRAL
- Confidence: 0-100%
- Reasoning: AI-generated explanation of why

No trade execution, no portfolio management — pure signal intelligence.

## 2. Architecture

Pipeline is event-driven using a pipeline_events table in SQLite (Option B).
Each phase writes a "done" event. The next phase polls for it.
Runs on EC2 Amazon Linux.


tickers.json ──► BACKFILLER (one-time) ──► SQLite3 │ FETCHER (daily cron 00:00 UTC) ──► SQLite3 │ event: fetcher_done ▼ CALCULATOR ──► SQLite3 │ event: calculator_done ▼ SCORER ──► SQLite3 │ event: scorer_done ▼ AI + TELEGRAM ──► User


## 2.1 Backfiller Modules

The backfiller is a one-time historical data loader with the following modules:

| Module | Data | Source |
|---|---|---|
| `src/backfiller/ohlcv.py` | 5 years of daily OHLCV bars | Polygon |
| `src/backfiller/macro.py` | Treasury yields, VIX | Polygon, yfinance |
| `src/backfiller/fundamentals.py` | Quarterly income, ratios, YoY growth | yfinance |
| `src/backfiller/earnings.py` | Earnings dates, EPS estimates/actuals (~50 events) | yfinance |
| `src/backfiller/corporate_actions.py` | Dividends, splits, short interest | Polygon |
| `src/backfiller/news.py` | News articles + AI sentiment (3 months) | Polygon + Finnhub |
| `src/backfiller/filings.py` | 8-K SEC filings (6 months) | Polygon |
| `src/backfiller/main.py` | Orchestrator: ticker sync + all phases | — |
| `src/backfiller/verify.py` | Post-backfill raw data verification + report (10 checks) | — |
| `src/backfiller/verify_pipeline.py` | Post-calculation computed data verification + report (29 checks) | — |

Each module follows the same pattern: per-ticker function + batch function with ProgressTracker + Telegram progress updates. Polygon's Starter tier has no rate limiting. Finnhub free tier enforces 1 second delay between calls via `FinnhubClient._rate_limit()`.

### Backfill Orchestrator (`src/backfiller/main.py`)

`sync_tickers_from_config` synchronises the tickers table with `config/tickers.json`:
- Inserts new tickers (preserving `added_date` on re-insert via `INSERT OR IGNORE`)
- Updates name, SIC code, market cap from Polygon's ticker details endpoint
- Deactivates tickers removed from config (`active=0`) without deleting their data
- Reactivates tickers added back to config (`active=1`)

`run_full_backfill` runs all phases in order:
1. `sync` — ticker sync
2. `ohlcv` — 5yr OHLCV bars
3. `macro` — treasury yields + VIX
4. `fundamentals` — quarterly financials
5. `earnings` — earnings calendar
6. `corporate_actions` — dividends, splits, short interest
7. `news` — Polygon + Finnhub news articles
8. `filings` — 8-K SEC filings

Phase failures are caught and logged; remaining phases continue. A `pipeline_runs` entry is written on completion. Supports `ticker_filter` (single ticker) and `phase_filter` (single phase) for targeted re-runs.

### OHLCV Backfiller — Ticker Aliases (`src/backfiller/ohlcv.py`)

When a company changes its Nasdaq/NYSE ticker symbol, Polygon stores historical OHLCV data under the **original** ticker. Querying the new ticker for dates before the change returns no data, creating a false gap.

To handle renames, add `"former_symbol"` and `"symbol_since"` to the ticker's entry in `config/tickers.json`:

```json
{ "symbol": "META", "former_symbol": "FB", "symbol_since": "2022-06-09" }
```

`backfill_ohlcv_for_ticker` detects this and performs a **split fetch**:
- Fetches `META` bars from `symbol_since` → today
- Fetches `FB` bars from `from_date` → day before `symbol_since`
- Stores **all rows under the current ticker** (`META`)

If `symbol_since` predates the lookback window (i.e., all available history is already under the current ticker), a normal single fetch is performed.

Currently configured aliases: `META` (former `FB`, since 2022-06-09).

### Earnings Backfiller (`src/backfiller/earnings.py`)

- Uses yfinance `get_earnings_dates(limit=40)` to fetch ~50 earnings events per ticker
- Returns actual earnings announcement dates (not fiscal period end dates), EPS estimate, reported EPS, and absolute EPS surprise
- `fiscal_quarter`, `fiscal_year`, `revenue_estimated`, `revenue_actual` stored as NULL (not available from yfinance `get_earnings_dates`)
- No rate limiting required (no API key)

### Periodic Earnings Refresh (`src/fetcher/earnings.py`)

- `run_periodic_earnings` refreshes earnings for all tickers using yfinance
- Reads `earnings_calendar_days` from `fetcher.json` (default: 7) to skip tickers with fresh data
- Uses INSERT OR REPLACE for idempotent upserts

### News Backfiller (`src/backfiller/news.py`)

- Polygon: fetches last 3 months of articles per ticker; extracts per-ticker sentiment from the `insights` array using `extract_sentiment_for_ticker` (correctly handles multi-ticker articles)
- Finnhub: fetches last 1 month of articles per ticker; generates deterministic IDs via `generate_finnhub_article_id` (format: `finnhub_{ticker}_{datetime}_{sha256[:8]}`)
- Both sources stored in `news_articles` with `source` column distinguishing them
- Polygon and Finnhub are attempted independently per ticker — a Finnhub failure does not block Polygon data
- **Finnhub articles have NULL sentiment** — they are enriched separately via `src/notifier/sentiment_enrichment.py` (see below)

### Finnhub Sentiment Enrichment (`src/notifier/sentiment_enrichment.py`)

Finnhub does not provide sentiment scores. This module uses Claude Haiku to classify each Finnhub article's sentiment post-hoc.

**When it runs:**
- **Daily**: as a post-processing step inside `run_daily_fetch()` after news is fetched. Processes up to `max_articles_per_run` new NULL-sentiment articles automatically.
- **Backfill**: via `scripts/enrich_finnhub_sentiment.py --all` — processes all historical NULL-sentiment articles (~15,000 articles ≈ $1.50 at Haiku pricing).

**Key design decisions:**
- Uses Claude Haiku (`claude-haiku-4-20250514`) — 10× cheaper than Sonnet; classification is a simple task
- `temperature=0.0` — deterministic, consistent results
- `batch_size=20` — 20 articles per Claude API call (single batched prompt)
- `max_articles_per_run=500` — safety cap to control daily cost (~$0.05/day for 50 articles)
- `WHERE id = ? AND ticker = ? AND sentiment IS NULL` guard in UPDATE prevents overwriting Polygon sentiment (higher quality NLP pipeline); both `id` and `ticker` required because `news_articles` uses a composite PK
- After enrichment, `news_daily_summary` is recomputed for affected (ticker, date) pairs via `aggregate_news_for_ticker()` so the scorer immediately benefits from the updated `avg_sentiment_score`

**Cost estimate:** ~$0.001 per article. Daily enrichment (~10–50 articles) is negligible.

### 8-K Filings Backfiller (`src/backfiller/filings.py`)

- Fetches last 6 months of 8-K filings per ticker from Polygon
- Stores in `filings_8k` with accession_number as PRIMARY KEY for idempotency

### Backfill Verifier (`src/backfiller/verify.py`)

Run after `run_full_backfill` via `python scripts/verify_backfill.py` to validate data quality.

**Checks performed (10 total):**

| Check | Failure condition | Status |
|---|---|---|
| `table_row_counts` | `ohlcv_daily` is empty | fail; other tables below min → warn |
| `ticker_coverage_ohlcv_daily` | Any active ticker missing from `ohlcv_daily` | fail |
| `ticker_coverage_*` | Any ticker missing from fundamentals/news/etc. | warn |
| `date_range_all_tickers` | Ticker has < 50% of expected 1260 trading days | fail; < 80% → warn |
| `date_gaps_all_tickers` | Ticker has > 5 missing Mon-Fri trading days | warn |
| `data_freshness` | Any ticker is > 30 days behind today | fail; > 5 days → warn |
| `value_sanity` | Zero or negative close/volume | fail; 500%+ price jump → warn |
| `cross_table_consistency` | Active ticker in `tickers` table has no OHLCV rows | fail |
| `fundamentals_null_coverage` | > 50% NULL in pe_ratio/eps/revenue/debt_to_equity | warn |
| `news_sentiment_coverage` | Overall sentiment coverage < 50% | warn |

**Data classes:** `CheckResult` (name, status, message, details, data), `VerificationReport` (checks, overall_status, pass_count, warn_count, fail_count, timestamp).

**Entry point:** `scripts/verify_backfill.py --quiet --no-telegram --ticker AAPL --db-path PATH`. Exits 0 on PASS, 1 on FAIL.

## 2.2 Calculator Modules

The calculator runs after the fetcher completes (`fetcher_done` event) and writes to its output tables.

| Module | Computes | Output Table |
|---|---|---|
| `src/calculator/indicators.py` | 15 technical indicators (EMA, MACD, ADX, RSI, Stochastic, CCI, Williams %R, OBV, CMF, A/D, Bollinger, ATR, Keltner) | `indicators_daily` |
| `src/calculator/weekly.py` | Weekly OHLCV candles (open=Mon, high/low=week extremes, close=Fri, volume=sum); same 15 indicators on weekly candles | `weekly_candles`, `indicators_weekly` |
| `src/calculator/monthly.py` | Monthly OHLCV candles (YYYY-MM-01 key, open=first trading day, high/low=month extremes, close=last trading day, volume=sum); same 15 indicators on monthly candles | `monthly_candles`, `indicators_monthly` |
| `src/calculator/profiles.py` | Per-stock percentile profiles (p5/p20/p50/p80/p95 + mean/std) over 504-day rolling window; blended with sector profile using α=min(0.85, days/756) | `indicator_profiles` |
| `src/calculator/crossovers.py` | EMA 9/21, EMA 21/50, MACD signal line crossovers | `crossovers_daily` |
| `src/calculator/gaps.py` | Gap up/down with Breakaway/Continuation/Exhaustion/Common classification | `gaps_daily` |
| `src/calculator/swing_points.py` | Swing highs/lows (N candles dominant on both sides, default N=5) with strength | `swing_points` |
| `src/calculator/support_resistance.py` | Cluster swing points into S/R levels with touch count, strength, and broken detection | `support_resistance` |
| `src/calculator/patterns.py` | 7 candlestick patterns + 7 structural patterns (Double Top/Bottom, Bull/Bear Flag, Breakout, Breakdown, False Breakout) | `patterns_daily` |
| `src/calculator/divergences.py` | Regular/Hidden Bullish/Bearish divergences across RSI, MACD histogram, OBV, Stochastic | `divergences_daily` |
| `src/calculator/fibonacci.py` | Fibonacci retracement levels from most recent significant swing pair; on-the-fly computation | *(not stored — used by scorer)* |
| `src/calculator/relative_strength.py` | RS_market = (1+r_ticker)/(1+r_SPY); RS_sector = (1+r_ticker)/(1+r_sector_ETF) over 20-day period | *(not stored — scorer calls on-the-fly)* |
| `src/calculator/news_aggregator.py` | Aggregates news_articles into daily sentiment summaries (avg_score, counts, top_headline, filing_flag) | `news_daily_summary` |

### Entry Points
- `detect_swing_points_for_ticker(db_conn, ticker, config)` → populates `swing_points`
- `detect_support_resistance_for_ticker(db_conn, ticker, config)` → populates `support_resistance`
- `detect_all_patterns_for_ticker(db_conn, ticker, config)` → populates `patterns_daily` (candlestick + structural)
- `detect_divergences_for_ticker(db_conn, ticker, config)` → populates `divergences_daily`
- `compute_fibonacci_for_ticker(db_conn, ticker, config)` → returns result dict (scorer calls on-the-fly)
- `compute_weekly_for_ticker(db_conn, ticker, config, mode, *, skip_event_detection=False)` → populates `weekly_candles` and `indicators_weekly`; supports `mode="full"` (rebuild all) and `mode="incremental"` (new weeks only). When `skip_event_detection=False` (regular tickers), additionally runs the per-timeframe sub-pipeline against the weekly mirror tables: swing_points → S/R → patterns → divergences → crossovers → profiles. ETFs/benchmarks pass `skip_event_detection=True` to mirror the daily ETF policy. The same surface exists on `compute_monthly_for_ticker` for the monthly mirrors.
- `compute_profile_for_ticker(db_conn, ticker, config)` → populates `indicator_profiles`
- `compute_all_profiles(db_conn, tickers, config)` → processes all tickers, computes sector profiles, blends stock+sector
- `compute_relative_strength_for_ticker(db_conn, ticker, config)` → returns `{"rs_market": float|None, "rs_sector": float|None}`
- `aggregate_news_for_ticker(db_conn, ticker, start_date, end_date)` → populates `news_daily_summary`
- `aggregate_all_news(db_conn, tickers, start_date, end_date)` → processes all tickers

All modules follow the same per-ticker error handling: catch specific exceptions, log with ticker+phase+date context, write to `alerts_log`, continue to next ticker.

### Calculator Orchestrator (`src/calculator/main.py`)

`run_calculator` is the Phase 2b entry point, analogous to `run_full_backfill` for the backfiller.

**Modes:**
- `full` — recompute everything from scratch for all historical data (used after backfill)
- `incremental` — compute only new data for the current day (used in the daily pipeline; gated on `fetcher_done` event)

**Pre-flight checks:**
1. Incremental mode verifies `fetcher_done` event exists for `target_date` (the trading date passed by the daily pipeline, i.e. yesterday UTC); falls back to today when `target_date` is not provided. If missing, logs a warning and returns early.
2. **Incremental mode** — skips if `MAX(ohlcv_daily.date) <= MAX(indicators_daily.date)` (indicators already current with OHLCV). This is data-driven so the calculator always runs when the fetcher has deposited new rows, regardless of prior event state. **Full mode** — skips if `calculator_done` is already `"completed"` for `data_date` or today (event-based, idempotent). If `"failed"`, retries in both modes.
3. Writes `calculator_done` with `status="processing"` for `MAX(ohlcv_daily.date)` (the canonical trading date) before starting.

**Processing order:**
1. `run_calculator_for_etfs_and_benchmarks` — indicators + weekly only for all sector ETFs (XLK, XLF, etc.) and market benchmarks (SPY, QQQ); needed by the scorer for sector scoring and relative strength.
2. Per stock ticker: `run_calculator_for_ticker` in dependency order (see below).
3. `compute_all_profiles` — sector profile blending after all individual tickers finish (full mode or when profiles are stale).

**Dependency order per ticker (`run_calculator_for_ticker`):**
```
Step 1: indicators          (CRITICAL — failure blocks steps 2, 6, 7, 8)
Step 2: crossovers          depends on indicators
Step 3: gaps                independent
Step 4: swing_points        (failure blocks steps 5, 6, 7)
Step 5: support_resistance  depends on swing_points
Step 6: patterns            depends on indicators + swing_points + support_resistance
Step 7: divergences         depends on indicators + swing_points
Step 8: profiles            depends on indicators (weekly recompute, skipped if recent)
Step 9: weekly              independent (runs full per-timeframe sub-pipeline)
Step 9b: monthly            independent (runs full per-timeframe sub-pipeline)
Step 10: news               independent
```

**Per-timeframe sub-pipelines (commit 4):**
After steps 9 and 9b persist their candles + indicators, each runs a six-step sub-pipeline against its mirror tables:

```
weekly_candles + indicators_weekly →
  swing_points_weekly → support_resistance_weekly →
  patterns_weekly + divergences_weekly + crossovers_weekly + indicator_profiles_weekly

monthly_candles + indicators_monthly →
  swing_points_monthly → support_resistance_monthly →
  patterns_monthly + divergences_monthly + crossovers_monthly + indicator_profiles_monthly
```

Each sub-step has its own try/except, logs to `alerts_log` under `phase='calculator-weekly'` or `'calculator-monthly'` on failure, and does not block the other sub-steps. Both `mode='full'` and `mode='incremental'` re-run the sub-pipeline against the **full ticker history** every call (none of the detectors operate on a date window). ETFs/benchmarks bypass the entire sub-pipeline via `skip_event_detection=True`, mirroring the daily ETF policy.

**Failure propagation:**
- `indicators` fails → ticker marked `"failed"`, crossovers/patterns/divergences/profiles skipped.
- `swing_points` fails → support_resistance and divergences skipped.
- All other modules fail independently: logged to `alerts_log`, remaining modules continue.
- Result status: `"success"` (no errors), `"partial"` (some modules failed), `"failed"` (indicators failed).

**Profile recompute logic (`should_recompute_profiles`):**
- Returns `True` if no profiles exist or latest `computed_at` is ≥ 7 days old.
- In full mode the orchestrator always recomputes profiles regardless.

**Post-flight:**
- Updates `calculator_done` to `status="completed"` using `MAX(ohlcv_daily.date)` as the event date (falls back to `MAX(indicators_daily.date)` if OHLCV table is empty). This ensures the scorer and notifier can find the event by the trading date.
- Writes `pipeline_runs` entry with phase, duration, tickers processed/failed, status.
- Sends Telegram summary with per-module counts and duration.

**Return value:** dict with keys: `tickers_processed`, `tickers_failed`, `duration_seconds`, `indicators_rows`, `patterns_found`, `divergences_found`, `weekly_candles`, `monthly_candles`, `profiles_computed`, `news_summaries`.

**Entry point script:** `scripts/run_calculator.py` with `--mode` (full/incremental), `--ticker` (optional), `--db-path` (optional).



## 2.3 Scorer Modules

The scorer runs after the calculator completes (`calculator_done` event) and produces BULLISH/BEARISH/NEUTRAL signals with confidence scores.

| Module | Purpose |
|---|---|
| `src/scorer/regime.py` | Market regime detection (Trending/Ranging/Volatile) from ADX, ATR, VIX; EMA stack alignment override reclassifies to Trending when close/EMA9/EMA21/EMA50 are fully aligned |
| `src/scorer/indicator_scorer.py` | Maps each indicator value to -100 to +100 using percentile profiles; applies regime-aware direction for momentum oscillators (RSI, Stochastic %K, CCI, Williams %R) |
| `src/scorer/pattern_scorer.py` | Scores candlestick/structural patterns, divergences, crossovers, gaps, Fibonacci, news, fundamentals, macro |
| `src/scorer/category_scorer.py` | Rolls up component scores into 9 categories; applies regime-based adaptive weights |
| `src/scorer/sector_adjuster.py` | Computes sector ETF trend score and applies adjustment (±5 to ±10) |
| `src/scorer/timeframe_merger.py` | 3-way merge of daily + weekly + monthly composite scores with regime-adaptive weights (trending: 0.10d/0.50w/0.40m, ranging: 0.60d/0.30w/0.10m, volatile: 0.25d/0.45w/0.30m); computes weekly and monthly scores from their respective indicator tables; renormalizes weights when a timeframe is absent; uses `scoring_date` to prevent look-ahead bias |
| `src/scorer/calibrator.py` | Rolling ridge regression: trains on recent signals + their realized 10-day excess returns (vs SPY), predicts expected excess return for current signal; 17 features (6 category scores + 6 raw indicators + 3 EMA spreads + weekly_score + monthly_score); cold-start fallback when < 30 samples |
| `src/scorer/confidence.py` | Signal classification (BULLISH/BEARISH/NEUTRAL), confidence modifiers, data_completeness dict, key_signals list |
| `src/scorer/flip_detector.py` | Detects signal direction changes; saves to signal_flips table |
| `src/scorer/main.py` | Orchestrator: per-ticker score_ticker() + run_scorer() for daily pipeline + run_historical_scoring() for Option E |

### Signal Classification
Uses `calibrated_score` (rolling ridge predicted excess return in %) when available
(>= 30 training samples); falls back to `final_score` during cold start.
- Score >= bullish_threshold (config: +2) → BULLISH
- Score <= bearish_threshold (config: -2) → BEARISH
- Otherwise → NEUTRAL

### Confidence Calculation

**Base score:** Derived from `calibrated_score` when available (warm start), or
discounted `final_score` during cold start.

- **Warm start** (`calibrated_score` is not None): `min(abs(calibrated_score), 8.0) * 10.0`  
  Maps the ridge-predicted excess return to a 0–80 confidence base. The cap at 8.0
  prevents extreme predictions from inflating confidence: empirically, accuracy peaks
  at `|cal| ≈ 6–8` (63%) and *drops* for `|cal| > 8` (57.6%) and `|cal| > 12` (47.7%)
  due to calibrator overfitting. So `|cal| = 5 → base 50`, `|cal| = 8+ → base 80`.

- **Cold start** (`calibrated_score` is None): `abs(final_score) * 0.3`  
  The raw composite has near-zero return correlation (R ≈ −0.006), so it is discounted
  heavily. Confidence in this state is driven primarily by the quality modifiers below.

**Modifiers** (applied to base):
| Modifier | Condition | Value |
|---|---|---|
| timeframe_agree | Daily and weekly both same direction | +10 |
| timeframe_disagree | Daily and weekly opposite directions | -15 |
| volume_confirms | Volume category agrees with trend direction | +10 |
| volume_diverges | Volume category opposes trend direction | -10 |
| indicator_consensus | >60% of indicators agree with signal direction | +5 |
| indicator_mixed | <50% of indicators agree with signal direction | -10 |
| earnings_penalty | Next earnings within 7 days | -15 |
| vix_extreme | VIX > 30 | -10 |
| atr_expanding | ATR > 1.5× its 20-day SMA | -5 |
| missing_news | No news data available | -5 |
| missing_fundamentals | No fundamentals data available | -3 |

Final confidence is clamped to [0, 100].

### Scorer Orchestrator (`src/scorer/main.py`)

**`run_scorer()`** — daily pipeline entry point:
1. Checks `calculator_done` event; returns early if missing.
2. Checks if `scorer_done` already completed for today; skips if so.
3. Writes `scorer_done` with `status="processing"`.
4. Scores all active tickers via `score_ticker()`.
5. Detects signal flips via `detect_flips_for_all()`.
6. Writes `scorer_done` with `status="completed"`.
7. Logs `pipeline_runs` entry.
8. Sends Telegram summary with signal distribution and flip count.

**`run_historical_scoring(mode="both")`** — Option E historical backfill:
- `mode="daily"`: scores last 12 months of trading days from `ohlcv_daily` dates.
- `mode="weekly"`: scores months 13-60 using week_start dates from `weekly_candles`.
- `mode="both"`: runs daily first, then weekly.

**`score_ticker()`** — full pipeline for one ticker on one date:
1. Load indicators + close price (returns None if absent).
2. Detect regime; get adaptive weights. Regime detection priority:
   volatile (VIX/ATR) > EMA-stack-trending (close/EMA9/EMA21/EMA50 fully aligned,
   config-gated via `ema_trend_override`) > ADX-trending > ADX-ranging > default-ranging.
3. Score all 15 indicators — momentum oscillators (RSI, Stochastic %K, CCI, Williams %R)
   use **regime-aware direction**: `higher_is_bullish=True` in `"trending"` regime
   (high readings = trend continuation = bullish) vs. `higher_is_bullish=False` in
   `"ranging"` / `"volatile"` (high readings = overbought = bearish, mean-reversion).
   BB %B is not affected — it measures Bollinger Band position, not momentum direction.
4. Load and score patterns, divergences, crossovers, gaps, Fibonacci (on-the-fly).
5. Score news sentiment, short interest.
6. Score fundamentals; compute macro (SPY trend + VIX + sector ETF + treasury + RS).
7. Compute 9 category scores → apply adaptive weights → daily_score.
8. Apply sector adjustment.
9. Compute weekly score; compute monthly score; merge 3-way timeframes with regime-adaptive weights → static composite (renormalizes when monthly is absent).
10. **Calibrate score** via rolling ridge regression: fetch recent signals with realized
    forward excess returns (vs SPY) as training data (sector ETFs, market benchmarks, and
    index ETFs are excluded from the training window — they are scored normally but must
    not be training examples because their feature-return relationships differ from
    individual stocks), train ridge on 17 features
    (6 category scores + 6 raw indicators + 3 EMA spreads + weekly_score + monthly_score), predict expected excess
    return for current signal → `calibrated_score`. Falls back to None (cold start)
    if fewer than `min_training_samples` are available.
11. Classify signal using `calibrated_score` if available, otherwise `final_score`. `effective_score`
    is a local variable only — it is never persisted.
12. Compute confidence: base = `min(abs(calibrated_score), 8.0) * 10.0` when warm
    (range 0–80), or `abs(final_score) * 0.3` during cold start; then add modifiers.
    Build data_completeness and key_signals.
13. Save to `scores_daily` (INSERT OR REPLACE) — `final_score` always holds the ±100 composite;
    `calibrated_score` holds the ridge prediction (or NULL); `model_r2` holds the training R².
14. Detect and save any signal flip to `signal_flips`.

**Entry point script:** `scripts/run_scorer.py` with `--ticker` (optional), `--historical` (flag), `--db-path` (optional).


Base URL: https://api.polygon.io
Auth: apiKey query parameter

Confirmed working endpoints:
| Data | Endpoint | Key Params |
|---|---|---|
| OHLCV | GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to} | adjusted=true, limit=50000, sort=asc |
| Ticker Details | GET /v3/reference/tickers/{ticker} | — |
| News + Sentiment | GET /v2/reference/news | ticker, published_utc.gte, limit=1000 |
| 8-K Filings | GET /stocks/filings/8-K/vX/text | ticker, filing_date filters |
| Dividends | GET /stocks/v1/dividends | ticker, date filters |
| Splits | GET /stocks/v1/splits | ticker, date filters |
| Short Interest | GET /stocks/v1/short-interest | ticker |
| Treasury Yields | GET /fed/v1/treasury-yields | date filters |
| Market Holidays | GET /v1/marketstatus/upcoming | — |

NOT authorized (use fallbacks):
- /stocks/financials/v1/ratios → yfinance
- /stocks/financials/v1/income-statements → yfinance
- /stocks/financials/v1/balance-sheets → yfinance
- /v2/aggs/ticker/I:SPX/... → SPY ETF via Polygon
- /benzinga/v1/earnings → yfinance (get_earnings_dates)

Response pagination: if next_url is present, follow it to get more results.

### 3.2 yfinance (Python library)
No API key needed. Used for:
- Income statements (quarterly + annual)
- Balance sheets (quarterly + annual)
- Financial ratios (P/E, P/B, D/E, ROA, ROE, etc.)
- Market cap, EPS, revenue
- VIX data (ticker: ^VIX)
- Earnings calendar (announcement dates, EPS estimates/actuals, ~50 events per ticker)

### 3.3 Finnhub (finnhub.io)
Free tier: 60 calls/min.
Used for:
- Supplementary company news

## 4. Database Schema

All tables use UNIQUE constraints on (ticker, date) where applicable.
Enable WAL mode on connection.

### Core Tables

**tickers** — synced from tickers.json
- symbol TEXT PRIMARY KEY
- name TEXT
- sector TEXT
- sector_etf TEXT
- sic_code TEXT
- sic_description TEXT
- market_cap REAL
- active BOOLEAN DEFAULT 1
- added_date TEXT
- updated_at TEXT

**ohlcv_daily** — raw price data
- ticker TEXT NOT NULL
- date TEXT NOT NULL
- open REAL, high REAL, low REAL, close REAL
- volume REAL
- vwap REAL
- num_transactions INTEGER
- UNIQUE(ticker, date)

### Fundamental Tables

**fundamentals** — quarterly financials from yfinance
- ticker TEXT NOT NULL
- report_date TEXT NOT NULL
- period TEXT (Q1/Q2/Q3/Q4/annual/ttm)
- revenue REAL, revenue_growth_yoy REAL
- net_income REAL
- eps REAL, eps_growth_yoy REAL
- pe_ratio REAL, pb_ratio REAL, ps_ratio REAL
- debt_to_equity REAL
- return_on_assets REAL, return_on_equity REAL
- free_cash_flow REAL
- market_cap REAL
- dividend_yield REAL
- fetched_at TEXT
- UNIQUE(ticker, report_date, period)

**earnings_calendar** — from yfinance
- ticker TEXT NOT NULL
- earnings_date TEXT NOT NULL (earnings announcement date)
- fiscal_quarter TEXT (NULL — not provided by yfinance)
- fiscal_year INTEGER (NULL — not provided by yfinance)
- estimated_eps REAL
- actual_eps REAL
- eps_surprise REAL (actual_eps - estimated_eps)
- revenue_estimated REAL (NULL — not provided by yfinance)
- revenue_actual REAL (NULL — not provided by yfinance)
- fetched_at TEXT
- UNIQUE(ticker, earnings_date)

### News & Filings Tables

**news_articles** — from Polygon + Finnhub
- id TEXT NOT NULL
- ticker TEXT NOT NULL
- date TEXT NOT NULL
- source TEXT (polygon/finnhub)
- headline TEXT
- summary TEXT
- url TEXT
- sentiment TEXT (positive/negative/neutral)
- sentiment_reasoning TEXT
- published_utc TEXT
- fetched_at TEXT
- PRIMARY KEY (id, ticker) — composite key allows the same Polygon article to be stored once per ticker it mentions

**news_daily_summary** — aggregated per ticker per day
- ticker TEXT NOT NULL
- date TEXT NOT NULL
- avg_sentiment_score REAL
- article_count INTEGER
- positive_count INTEGER
- negative_count INTEGER
- neutral_count INTEGER
- top_headline TEXT
- filing_flag BOOLEAN DEFAULT 0
- UNIQUE(ticker, date)

**filings_8k** — parsed SEC 8-K filings
- accession_number TEXT PRIMARY KEY
- ticker TEXT NOT NULL
- filing_date TEXT NOT NULL
- form_type TEXT
- items_text TEXT
- filing_url TEXT
- fetched_at TEXT

### Corporate Actions Tables

**dividends**
- id TEXT PRIMARY KEY
- ticker TEXT NOT NULL
- ex_dividend_date TEXT
- pay_date TEXT
- cash_amount REAL
- frequency INTEGER
- fetched_at TEXT

**splits**
- id TEXT PRIMARY KEY
- ticker TEXT NOT NULL
- execution_date TEXT
- split_from REAL
- split_to REAL
- fetched_at TEXT

**short_interest**
- ticker TEXT NOT NULL
- settlement_date TEXT NOT NULL
- short_interest INTEGER
- avg_daily_volume INTEGER
- days_to_cover REAL
- fetched_at TEXT
- UNIQUE(ticker, settlement_date)

### Macro Tables

**treasury_yields**
- date TEXT PRIMARY KEY
- yield_1_month REAL, yield_3_month REAL, yield_6_month REAL
- yield_1_year REAL, yield_2_year REAL, yield_3_year REAL
- yield_5_year REAL, yield_7_year REAL, yield_10_year REAL
- yield_20_year REAL, yield_30_year REAL

### Indicator Tables

**indicators_daily** — computed per ticker per day
- ticker TEXT NOT NULL, date TEXT NOT NULL
- ema_9 REAL, ema_21 REAL, ema_50 REAL
- macd_line REAL, macd_signal REAL, macd_histogram REAL
- adx REAL
- rsi_14 REAL
- stoch_k REAL, stoch_d REAL
- cci_20 REAL
- williams_r REAL
- obv REAL
- cmf_20 REAL
- ad_line REAL
- bb_upper REAL, bb_lower REAL, bb_pctb REAL
- atr_14 REAL
- keltner_upper REAL, keltner_lower REAL
- UNIQUE(ticker, date)

**indicator_profiles** — per-stock percentile profiles (2yr rolling)
- ticker TEXT NOT NULL
- indicator TEXT NOT NULL
- p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL
- mean REAL, std REAL
- window_start TEXT, window_end TEXT
- computed_at TEXT
- UNIQUE(ticker, indicator)

**weekly_candles**
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- open REAL, high REAL, low REAL, close REAL
- volume REAL
- UNIQUE(ticker, week_start)

**indicators_weekly** — same indicator columns as indicators_daily
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- ema_9 REAL, ema_21 REAL, ema_50 REAL
- macd_line REAL, macd_signal REAL, macd_histogram REAL
- adx REAL, rsi_14 REAL, stoch_k REAL, stoch_d REAL
- cci_20 REAL, williams_r REAL, obv REAL, cmf_20 REAL, ad_line REAL
- bb_upper REAL, bb_lower REAL, bb_pctb REAL
- atr_14 REAL, keltner_upper REAL, keltner_lower REAL
- UNIQUE(ticker, week_start)

**monthly_candles**
- ticker TEXT NOT NULL, month_start TEXT NOT NULL (YYYY-MM-01)
- open REAL, high REAL, low REAL, close REAL
- volume REAL
- UNIQUE(ticker, month_start)

**indicators_monthly** — same indicator columns as indicators_daily
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- ema_9 REAL, ema_21 REAL, ema_50 REAL
- macd_line REAL, macd_signal REAL, macd_histogram REAL
- adx REAL, rsi_14 REAL, stoch_k REAL, stoch_d REAL
- cci_20 REAL, williams_r REAL, obv REAL, cmf_20 REAL, ad_line REAL
- bb_upper REAL, bb_lower REAL, bb_pctb REAL
- atr_14 REAL, keltner_upper REAL, keltner_lower REAL
- UNIQUE(ticker, month_start)

**indicator_profiles_weekly** — same shape as `indicator_profiles`, computed over weekly indicators
- ticker TEXT NOT NULL, indicator TEXT NOT NULL
- p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL
- mean REAL, std REAL
- window_start TEXT, window_end TEXT
- computed_at TEXT
- UNIQUE(ticker, indicator)

**indicator_profiles_monthly** — same shape as `indicator_profiles`, computed over monthly indicators
- ticker TEXT NOT NULL, indicator TEXT NOT NULL
- p5 REAL, p20 REAL, p50 REAL, p80 REAL, p95 REAL
- mean REAL, std REAL
- window_start TEXT, window_end TEXT
- computed_at TEXT
- UNIQUE(ticker, indicator)

### Pattern & Signal Tables

**swing_points**
- ticker TEXT NOT NULL, date TEXT NOT NULL
- type TEXT (high/low)
- price REAL
- strength INTEGER
- UNIQUE(ticker, date, type)

**swing_points_weekly** — mirrors `swing_points` over weekly candles
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- type TEXT (high/low)
- price REAL
- strength INTEGER
- UNIQUE(ticker, week_start, type)

**swing_points_monthly** — mirrors `swing_points` over monthly candles
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- type TEXT (high/low)
- price REAL
- strength INTEGER
- UNIQUE(ticker, month_start, type)

**support_resistance**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL
- date_computed TEXT NOT NULL
- level_price REAL
- level_type TEXT (support/resistance)
- touch_count INTEGER
- first_touch TEXT, last_touch TEXT
- strength TEXT (weak/moderate/strong)
- broken BOOLEAN DEFAULT 0
- broken_date TEXT

**support_resistance_weekly** — mirrors `support_resistance` keyed on `week_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL
- week_start TEXT NOT NULL
- level_price REAL
- level_type TEXT (support/resistance)
- touch_count INTEGER
- first_touch TEXT, last_touch TEXT
- strength TEXT (weak/moderate/strong)
- broken BOOLEAN DEFAULT 0
- broken_date TEXT

**support_resistance_monthly** — mirrors `support_resistance` keyed on `month_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL
- month_start TEXT NOT NULL
- level_price REAL
- level_type TEXT (support/resistance)
- touch_count INTEGER
- first_touch TEXT, last_touch TEXT
- strength TEXT (weak/moderate/strong)
- broken BOOLEAN DEFAULT 0
- broken_date TEXT

**patterns_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- pattern_name TEXT
- pattern_category TEXT (candlestick/structural)
- pattern_type TEXT (reversal/continuation/breakout)
- direction TEXT (bullish/bearish)
- strength INTEGER (1-5)
- confirmed BOOLEAN DEFAULT 0
- details TEXT (JSON)

**patterns_weekly** — mirrors `patterns_daily` keyed on `week_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- pattern_name TEXT
- pattern_category TEXT
- pattern_type TEXT
- direction TEXT
- strength INTEGER
- confirmed BOOLEAN DEFAULT 0
- details TEXT (JSON)

**patterns_monthly** — mirrors `patterns_daily` keyed on `month_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- pattern_name TEXT
- pattern_category TEXT
- pattern_type TEXT
- direction TEXT
- strength INTEGER
- confirmed BOOLEAN DEFAULT 0
- details TEXT (JSON)

**divergences_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- indicator TEXT (`rsi_14` / `macd_histogram` / `obv` / `stochastic`) — matches the corresponding indicators-table column name so the scorer's `indicator == "rsi_14"` filter actually fires. Prior to commit 3 of the weekly/monthly parity work this column held `"rsi"` and the scorer filtered on `"rsi_14"`, silently zero-ing the daily RSI-divergence contribution to every ticker's score. Recompute divergences (`run_calculator.py --mode full`) and re-run the scorer after deploying that fix.
- divergence_type TEXT (regular_bullish/regular_bearish/hidden_bullish/hidden_bearish)
- price_swing_1_date TEXT, price_swing_1_value REAL
- price_swing_2_date TEXT, price_swing_2_value REAL
- indicator_swing_1_value REAL, indicator_swing_2_value REAL
- strength INTEGER (1-5)

**divergences_weekly** — mirrors `divergences_daily` keyed on `week_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- indicator TEXT
- divergence_type TEXT
- price_swing_1_date TEXT, price_swing_1_value REAL
- price_swing_2_date TEXT, price_swing_2_value REAL
- indicator_swing_1_value REAL, indicator_swing_2_value REAL
- strength INTEGER

**divergences_monthly** — mirrors `divergences_daily` keyed on `month_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- indicator TEXT
- divergence_type TEXT
- price_swing_1_date TEXT, price_swing_1_value REAL
- price_swing_2_date TEXT, price_swing_2_value REAL
- indicator_swing_1_value REAL, indicator_swing_2_value REAL
- strength INTEGER

**crossovers_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- crossover_type TEXT (ema_9_21/ema_21_50/macd_signal)
- direction TEXT (bullish/bearish)
- days_ago INTEGER

**crossovers_weekly** — mirrors `crossovers_daily` keyed on `week_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- crossover_type TEXT
- direction TEXT
- days_ago INTEGER

**crossovers_monthly** — mirrors `crossovers_daily` keyed on `month_start`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- crossover_type TEXT
- direction TEXT
- days_ago INTEGER

**gaps_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- gap_type TEXT (breakaway/continuation/exhaustion/common)
- direction TEXT (up/down)
- gap_size_pct REAL
- volume_ratio REAL
- filled BOOLEAN DEFAULT 0

### Scoring Tables

**scores_daily**
- ticker TEXT NOT NULL, date TEXT NOT NULL
- signal TEXT (BULLISH/BEARISH/NEUTRAL)
- confidence REAL (0-100)
- final_score REAL — **always** the ±100 merged 3-way timeframe composite (daily×w + weekly×w + monthly×w, renormalized when monthly is absent). Never stores the calibrated value.
- regime TEXT (trending/ranging/volatile)
- daily_score REAL, weekly_score REAL, monthly_score REAL
- trend_score REAL, momentum_score REAL, volume_score REAL
- volatility_score REAL, candlestick_score REAL, structural_score REAL
- sentiment_score REAL, fundamental_score REAL, macro_score REAL
- calibrated_score REAL — ridge regression predicted excess return (≈ ±2–15%), or NULL during cold start / when calibration is disabled
- model_r2 REAL
- data_completeness TEXT (JSON)
- key_signals TEXT (JSON array)
- key_signals_data TEXT (nullable) — JSON contribution payload used by the `/why` command. See §11.1 below for the schema and approximation notes. **Daily-only**: `scores_weekly` and `scores_monthly` do not carry this column because the `/why` feature operates on daily signals.
- UNIQUE(ticker, date)

**scores_weekly** — denormalized weekly composite snapshot for query/UI consumers (e.g., `/detail` and scatter views). NOT in the scoring critical path: the runtime `merge_timeframes()` still consumes the in-memory composite and writes the final ±100 to `scores_daily.final_score`. This table is a write-through projection so historical queries do not need to recompute weekly aggregates from `indicators_weekly`.
- ticker TEXT NOT NULL, week_start TEXT NOT NULL
- composite_score REAL NOT NULL — weekly composite (`weekly_score_method` controls how it's computed; see `config/scorer.json`)
- regime TEXT (trending/ranging/volatile) — regime classification on the weekly window
- trend_score REAL, momentum_score REAL, volume_score REAL
- volatility_score REAL, candlestick_score REAL, structural_score REAL
- fundamental_score REAL, macro_score REAL — inherited from the most recent `scores_daily` row whose date falls in the closed week (`<= week_start + 4 days`, i.e. Friday). NULL when no daily row exists.
- data_completeness TEXT — JSON object (mirrors `scores_daily.data_completeness`). Stored as TEXT after commit 6's `migrate_fix_scores_completeness_type.py` correction.
- key_signals TEXT — JSON-encoded array of contributing weekly signals
- PRIMARY KEY (ticker, week_start)

**scores_monthly** — denormalized monthly composite snapshot, same role as `scores_weekly`. Also NOT in the critical path; populated alongside the daily score so long-horizon queries don't have to recompute.
- ticker TEXT NOT NULL, month_start TEXT NOT NULL
- composite_score REAL NOT NULL
- regime TEXT
- trend_score REAL, momentum_score REAL, volume_score REAL
- volatility_score REAL, candlestick_score REAL, structural_score REAL
- fundamental_score REAL, macro_score REAL — inherited from the most recent `scores_daily` row whose date is `<= calendar.monthrange(year, month)[1]`-th day of the month. NULL when no daily row exists.
- data_completeness TEXT — JSON object (corrected from REAL → TEXT in commit 6)
- key_signals TEXT (JSON array)
- PRIMARY KEY (ticker, month_start)

#### Closed-period gate (commit 6)

Both `scores_weekly` and `scores_monthly` are populated by per-ticker hooks
in `score_ticker` (`src/scorer/main.py`, after `save_score_to_db`). The
hooks are gated by `src/scorer/period_gate.py` so we never persist a
snapshot of an in-progress period:

```text
is_week_closed(week_start, scoring_date)  := scoring_date >= week_start + 7 days
is_month_closed(month_start, scoring_date) := (scoring_date.year, scoring_date.month)
                                              > (month_start.year, month_start.month)
```

Sunday is therefore considered the LAST day of an in-progress week — the
gate flips closed only when the next Monday begins. This matches the
"completed period" mental model used by the UI: a user opening the dashboard
on Sunday sees results through last Monday's `week_start`, not through the
current `week_start` whose Monday is still less than seven days old.

The persistence helpers (`src/scorer/persistence.py`) are wrapped in
per-step try/except so a failure to write `scores_weekly` or `scores_monthly`
cannot break the daily scoring path. Failures are logged to `alerts_log`
(`severity='warning'`, `phase='scorer'`) and a WARNING is emitted via the
standard logger.

**signal_flips**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- previous_signal TEXT, new_signal TEXT
- previous_confidence REAL, new_confidence REAL

### Pipeline Tables

**pipeline_events**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- event TEXT NOT NULL
- date TEXT NOT NULL
- status TEXT (ready/processing/completed/failed)
- timestamp TEXT NOT NULL
- details TEXT
- UNIQUE(event, date)

**pipeline_runs**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- date TEXT NOT NULL
- phase TEXT NOT NULL
- started_at TEXT, completed_at TEXT
- duration_seconds REAL
- tickers_processed INTEGER, tickers_skipped INTEGER, tickers_failed INTEGER
- api_calls_made INTEGER
- status TEXT (success/partial/failed)
- error_summary TEXT

**alerts_log**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT
- date TEXT
- phase TEXT
- severity TEXT (info/warning/error)
- message TEXT
- notified BOOLEAN DEFAULT 0
- created_at TEXT

### Telegram Tables

**telegram_message_log** — incoming bot commands for usage analytics
- id INTEGER PRIMARY KEY AUTOINCREMENT
- chat_id TEXT NOT NULL — Telegram chat ID of the sender
- user_id TEXT — Telegram user ID (NULL if unavailable)
- username TEXT — Telegram @username (NULL if user has no username)
- command TEXT — extracted command prefix e.g. `/detail`, `/scatter`, `/help`, `/tickers` (NULL if none)
- message_text TEXT NOT NULL — full raw message text
- received_at TEXT NOT NULL — UTC ISO 8601 timestamp

Index: `idx_telegram_message_log_received_at` on `received_at`

## 5. Technical Indicators (15 total)

All computed using the `ta` library from OHLCV data. Parameters configurable in calculator.json.

| Category | Indicator | ta Function | Default Params |
|---|---|---|---|
| Trend | EMA 9 | `ta.trend.EMAIndicator(close, window=9).ema_indicator()` | window: 9 |
| Trend | EMA 21 | `ta.trend.EMAIndicator(close, window=21).ema_indicator()` | window: 21 |
| Trend | EMA 50 | `ta.trend.EMAIndicator(close, window=50).ema_indicator()` | window: 50 |
| Trend | MACD | `ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)` → `.macd()` / `.macd_signal()` / `.macd_diff()` | fast:12, slow:26, signal:9 |
| Trend | ADX | `ta.trend.ADXIndicator(high, low, close, window=14).adx()` | window: 14 |
| Momentum | RSI | `ta.momentum.RSIIndicator(close, window=14).rsi()` | window: 14 |
| Momentum | Stochastic | `ta.momentum.StochasticOscillator(high, low, close, window=14, smooth_window=3)` → `.stoch()` / `.stoch_signal()` | window:14, smooth:3 |
| Momentum | CCI | `ta.trend.CCIIndicator(high, low, close, window=20).cci()` | window: 20 |
| Momentum | Williams %R | `ta.momentum.WilliamsRIndicator(high, low, close, lbp=14).williams_r()` | lbp: 14 |
| Volume | OBV | `ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()` | — |
| Volume | CMF | `ta.volume.ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()` | window: 20 |
| Volume | A/D Line | `ta.volume.AccDistIndexIndicator(high, low, close, volume).acc_dist_index()` | — |
| Volatility | Bollinger Bands | `ta.volatility.BollingerBands(close, window=20, window_dev=2)` → `.bollinger_hband()` / `.bollinger_lband()` / `.bollinger_pband()` | window:20, std:2 |
| Volatility | ATR | `ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()` | window: 14 |
| Volatility | Keltner | `ta.volatility.KeltnerChannel(high, low, close, window=20)` → `.keltner_channel_hband()` / `.keltner_channel_lband()` | window: 20 |

## 6. Pattern Detection

### 6.1 Candlestick Patterns (7)
- Bullish Engulfing, Bearish Engulfing, Hammer, Shooting Star, Doji, Morning Star, Evening Star

### 6.2 Swing Points
- Swing High: high > N candles on BOTH sides (default N=5)
- Swing Low: low < N candles on BOTH sides (default N=5)

### 6.3 Support & Resistance
- Cluster swing points within 1.5% price tolerance
- 2+ touches = S/R level. Strength: weak(2), moderate(3), strong(4+)

### 6.4 Breakout/Breakdown
- Close beyond S/R with volume > 1.5x 20-day average
- False breakout: reversal within 2 days

### 6.5 Double Top / Double Bottom
- Two swing highs/lows within 1.5%, separated by 10-60 trading days
- Signal on neckline break

### 6.6 Bull/Bear Flag
- Pole: >2x ATR in 5-10 days. Flag: 20-50% retracement over 5-15 days
- Breakout in pole direction with volume

### 6.7 Gaps
- Gap Up: today's low > yesterday's high. Gap Down: today's high < yesterday's low
- Classify: Breakaway/Continuation/Exhaustion/Common based on volume

### 6.8 Fibonacci
- Levels: 23.6%, 38.2%, 50%, 61.8%, 78.6% from significant swing high/low
- Flag when price within 1% of any level

## 7. Divergences
- Regular Bullish: price lower low, indicator higher low
- Regular Bearish: price higher high, indicator lower high
- Hidden Bullish: price higher low, indicator lower low
- Hidden Bearish: price lower high, indicator higher high
- Apply to: RSI, MACD histogram, OBV, Stochastic

## 8. Crossovers
- EMA 9/21, EMA 21/50, MACD signal line
- Track days_ago for recency weighting

## 9. Relative Strength
- RS_market = ticker_return(20d) / SPY_return(20d)
- RS_sector = ticker_return(20d) / sector_ETF_return(20d)

## 10. Per-Stock Profiles
- 2-year rolling window percentiles (p5, p20, p50, p80, p95) + z-scores
- Blend: Effective = (α × Stock) + ((1-α) × Sector), α = min(0.85, days/756)
- Recompute weekly

## 11. Scoring Engine

### 9 Categories:
1. Trend — EMA, MACD, ADX, crossovers
2. Momentum — RSI, Stoch, CCI, Williams %R, divergences
3. Volume — OBV, CMF, A/D, OBV divergence
4. Volatility — BB, ATR, Keltner
5. Candlestick — 7 patterns
6. Structural — S/R, Double Top/Bottom, Flags, Gaps, Fibonacci
7. Sentiment — News, 8-K, short interest
8. Fundamental — P/E, EPS, Revenue, D/E
9. Macro — SPY, VIX, Sector ETF, Treasury, relative strength

### Regime Detection:
- Trending: ADX > 25
- Ranging: ADX < 20
- Volatile: ATR > 1.5x 20d avg OR VIX > 25
- Priority: Volatile > Trending > Ranging

### Adaptive Weights:
| Category | Trending | Ranging | Volatile |
|---|---|---|---|
| Trend | 30% | 10% | 20% |
| Momentum | 15% | 25% | 15% |
| Volume | 10% | 10% | 10% |
| Volatility | 5% | 10% | 15% |
| Candlestick | 5% | 10% | 10% |
| Structural | 15% | 15% | 10% |
| Sentiment | 10% | 10% | 10% |
| Fundamental | 5% | 5% | 5% |
| Macro | 5% | 5% | 5% |

### Regime-Adaptive 3-Way Timeframe Merge: Final = (Daily × w_d) + (Weekly × w_w) + (Monthly × w_m)

Weights are regime-specific (configured in `scorer.json` `timeframe_weights`):

| Regime | Daily | Weekly | Monthly |
|---|---|---|---|
| trending | 0.10 | 0.50 | 0.40 |
| ranging | 0.60 | 0.30 | 0.10 |
| volatile | 0.25 | 0.45 | 0.30 |

Weights are renormalized when a timeframe is absent. Weights selected via 5-year
backtest across 62 tickers (5,344 weekly samples with 21-day forward returns); an
earlier 2-way 0.2/0.8 (daily/weekly) configuration yielded 58.8% directional accuracy
vs 54.4% at a 0.6/0.4 split (+4.4pp), motivating the move to weekly-dominant
weighting that the regime-adaptive 3-way scheme now extends. The `/detail` command
display previously read the flat 2-way form and silently dropped monthly; this was
corrected so the displayed merge formula matches `final_score`.

The weekly score uses the same scoring pipeline as daily: all 14 indicators from
`indicators_weekly` are scored via `score_all_indicators`, rolled up into 4 categories
(trend, momentum, volume, volatility) with magnitude-weighted averaging, then combined
via `weekly_adaptive_weights` (regime-specific, 4 categories summing to 1.0) and the
same expansion factor. Profiles come from `indicator_profiles_weekly` (per-timeframe
percentiles); when that table is empty the scorer falls back to daily profiles and logs
INFO once per ticker. Sentiment, fundamentals, and macro are still not included on the
weekly path — they have no weekly data sources.

#### Weekly / monthly score modes (commit 5)

The composite definition is gated on `weekly_score_method` and `monthly_score_method`
in `config/scorer.json` (default `v1_4cat` for both):

- **`v1_4cat`** — 4 indicator-only categories. Existing behaviour, regression-test pinned.
- **`v2_8cat`** — adds 2 more applicable categories (candlestick, structural) sourced
  from `patterns_weekly` / `patterns_monthly`, and mirrors daily's category wiring:
    - **Crossovers** (from `crossovers_weekly` / `crossovers_monthly`) feed the **trend** category
      (alongside ema_alignment, MACD, ADX).
    - **Divergences** (from `divergences_weekly` / `divergences_monthly`) feed the
      **momentum** category (RSI/MACD/Stoch divergences) and the **volume** category
      (OBV divergence).
    - Each pattern_scorer SQL query aliases `week_start AS date` (or `month_start AS date`)
      so the scorers' recency-decay primitives — which read `row["date"]` — stay
      timeframe-agnostic.

**Both `compute_weekly_score_breakdown` and `compute_monthly_score_breakdown` return
the same 7-key dict** (`composite_score`, 4 main categories, `candlestick_score`,
`structural_score`). In v1 mode the candlestick/structural keys are `None`. The thin
scalar shims `compute_weekly_score` / `compute_monthly_score` continue to return just
the composite, keeping `src/scorer/main.py` call sites unchanged.

**Structural category scope reduction.** Daily's structural rollup also folds in
`gap_score` and `fibonacci_score`; neither has a per-timeframe data source today
(`gaps_weekly` / `gaps_monthly` and per-timeframe Fibonacci levels are out of scope
for commits 1–5). Weekly/monthly v2 structural therefore only includes
`structural_pattern_score` from the corresponding `patterns_*` table.

**v2 scalar differs from v1 even when cdl/struct weights are 0.0.** Because crossovers
join the trend rollup and divergences join momentum/volume, the magnitude-weighted
average within those categories shifts whenever a per-timeframe crossover or divergence
exists. The default flag is `v1_4cat` so live signals are unaffected at this commit;
flipping to `v2_8cat` requires a calibrator retrain (commit 7).

**Monthly candlestick is permanently None.** The candlestick recency-decay window is
7 days (≈5 trading days). Monthly bars are 0–30+ days behind any given `scoring_date`,
so candlestick scores would either zero out or alias on the timing of when scoring runs
relative to month-end. `compute_monthly_score_breakdown` therefore returns
`candlestick_score=None` even in v2 mode. Structural patterns (28-day window) and
divergences (42-day window) remain useful on monthly bars and ARE applied.

**Closed-period invariant.** Both `compute_weekly_score_breakdown` and
`compute_monthly_score_breakdown` load the most recent bar with `week_start <= scoring_date`
(or `month_start <= scoring_date`) — this includes the in-progress current week/month
when scoring runs mid-period. That is intentional: the live composite that feeds
`merge_timeframes` must reflect today's data. Persistence to `scores_weekly` /
`scores_monthly` (commit 6) filters to closed-period bars so historical queries see
stable values. Live and persisted weekly scores can therefore differ for the current
in-progress bar — by design.

### Signal: +30 to +100 = BULLISH, -30 to +30 = NEUTRAL, -100 to -30 = BEARISH
### Confidence: |Final Score|% + modifiers (timeframe agreement, volume confirmation, earnings proximity, VIX, etc.)

### 11.1 Contribution Payload (`key_signals_data`)

After `apply_adaptive_weights` produces category scores, `src/scorer/contribution.py::build_contributions_payload` assembles a per-indicator/per-pattern breakdown of how much each signal contributed to the daily composite. The result is serialised as JSON and persisted in `scores_daily.key_signals_data` (nullable TEXT). The column is **daily-only** — `scores_weekly` and `scores_monthly` do not carry it.

**Payload schema (`v: 1`):**

```json
{
  "v": 1,
  "expansion_factor": 1.5,
  "items": [
    {
      "name":             "rsi_14",
      "kind":             "indicator",
      "raw_value":        52.3,
      "score":            18.0,
      "category":         "momentum",
      "category_weight":  0.15,
      "contribution":     2.7
    }
  ]
}
```

- `kind` is `"indicator"` or `"pattern"`.
- `items` is sorted by `|contribution|` descending so the highest-impact signals appear first.
- `expansion_factor` echoes `config['scoring']['score_expansion_factor']` at scoring time so `/why` can render the full math chain (`share × regime_weight × expansion_factor = contribution`) and reconcile exactly with the persisted contribution number.
- The `v` field exists for forward-compatibility: if the payload schema changes in a future sitting, the `/why` formatter can detect stale data by checking `payload["v"]` and fall back gracefully.

**Approximation note.** Summing `items[*].contribution` does not reproduce `final_score` exactly. Three sources of divergence are expected:

1. **Clamping** — each category score is clamped to ±100 before weighting; the per-indicator scores that fed the average are unclamped.
2. **Expansion factor** — `score_expansion_factor` (default 1.5) is baked into each per-item `contribution` value and echoed at the payload root so `/why` can show it in the math chain. Sum of `items[*].contribution` therefore already reflects expansion; divergence from `final_score` comes from clamping and sector adjustment, not expansion.
3. **Post-rollup sector adjustment** — the `sector_adjuster` adds ±5 to ±10 points after the category scores are combined; this is not attributable to any single indicator or pattern.

These divergences are intentional: the payload is designed for relative ranking and human-readable explanation, not as a mathematical reconstruction of `final_score`.

**How the scorer produces it.** `src/scorer/main.py::score_ticker` calls `build_contributions_payload` after `apply_adaptive_weights` returns the category score dict, then passes the JSON string to `save_score_to_db` alongside the existing columns. The builder reads from `INDICATOR_CATEGORY_MAP` (in `category_scorer.py`) and `PATTERN_CATEGORY_MAP` (in `category_scorer.py`) to classify each signal, and from `PATTERN_RULE_DESCRIPTIONS` (in `pattern_scorer.py`) to annotate pattern items.

## 12. Historical Scoring (Option E)
- Last 12 months: daily scores
- Months 13-60: weekly scores

## 12b. Calibrator Acceptance Gate

When `weekly_score_method` flips between `v1_4cat` and `v2_8cat` the meaning of `scores_daily.weekly_score` (and `monthly_score`) changes, which shifts the calibrator's input distribution. The acceptance gate validates that the shift is bounded by comparing `calibrated_score` distribution at a fixed scoring date pre/post the flip.

- Module: `src/scorer/acceptance_gate.py` (pure helpers: `compute_calibrated_score_distribution`, `find_latest_scoring_date_with_calibration`, `validate_snapshot_compatibility`, `compare_distributions`).
- CLI: `scripts/check_calibrator_acceptance.py` with `snapshot` and `check` subcommands.
- Thresholds (config: `scorer.json:calibrator_acceptance`): `max_mean_delta`, `max_std_delta`, `max_ticker_delta` (informational), `min_sample_size`.
- Three-tier output: PASS, PASS-with-WARNING (any delta in [70%, 100%) of threshold), FAIL.
- Snapshots store per-ticker calibrated_score values in addition to summary stats so bipolar shifts (mean ≈ 0 but per-ticker huge) surface in the report.
- An integrated alternative — running the gate inside `scripts/run_scorer.py --historical` — was considered and rejected to keep the gate independently re-runnable for ad-hoc inspection.

The gate does not block the deploy directly — it is a runtime check the operator runs as part of the flip procedure (see OPERATIONS.md "Flipping weekly_score_method: required sequence").

## 13. Notifier

### 13.1 AI Reasoner (`src/notifier/ai_reasoner.py`)

The AI reasoning layer takes structured scoring output and generates human-readable analysis using the
Claude API (Anthropic). Claude **reasons** about the signals — identifying confluences, flagging
contradictions, and providing actionable insight — rather than just reformatting data.

All config comes from `config/notifier.json → ai_reasoner` (model, max_tokens, temperature) and
`config/notifier.json → telegram` (confidence_threshold, max_tickers_per_section).

**Latent coupling — `weekly_score` semantics in the LLM prompt.** `build_ticker_context` injects `weekly_score` (and `daily_score`) into the prompt at line 803 of `ai_reasoner.py`. When `weekly_score_method` flips between v1 and v2, the value of `weekly_score` shifts because v2 routes weekly crossovers/divergences into trend/momentum/volume. The prompt input therefore changes semantics and the LLM blurb tone/framing may shift. The flip procedure in OPERATIONS.md captures pre/post sample blurbs to surface this. `monthly_score` is NOT in the prompt, so monthly v2 changes do not affect blurbs directly.

**Public functions:**

| Function | Purpose |
|---|---|
| `build_ticker_context(db_conn, ticker, score, scoring_date)` | Queries all relevant DB data (indicators, patterns, divergences, crossovers, fundamentals, news, short interest, signal flips) and computes Fibonacci + RS on-the-fly. Returns a richly formatted string for Claude. |
| `build_market_context(db_conn, scoring_date)` | Builds overall market context: VIX level/interpretation, SPY/QQQ trend, 10Y treasury yield, sector leaders/laggards. |
| `build_prompt_for_ticker(ticker_context, market_context, is_flip)` | Assembles the full Claude prompt with system role, format instruction (1-2 sentences), and optional flip-change instruction. |
| `build_prompt_for_daily_summary(bullish, bearish, flips, market_context)` | Builds prompt for a cohesive 2-3 sentence daily briefing covering all qualifying tickers. |
| `call_claude(prompt, config)` | Calls `anthropic.Anthropic().messages.create()`; retries on `RateLimitError` (max 3 attempts, exponential backoff via tenacity); returns fallback string on any error — never crashes the pipeline. |
| `generate_ticker_reasoning(db_conn, ticker, score, market_context, config, is_flip)` | Orchestrates context → prompt → Claude for one ticker. Returns Claude's analysis string. |
| `generate_daily_summary(db_conn, bullish, bearish, flips, market_context, config)` | Calls Claude for the daily summary. Returns `"No significant signals today."` if no qualifying tickers. |
| `get_qualifying_tickers(db_conn, scoring_date, config)` | Queries scores_daily and signal_flips. Buckets into bullish (≥ confidence threshold), bearish (≥ threshold), flips (always included). Caps each bucket at `max_tickers_per_section`. |
| `reason_all_qualifying_tickers(db_conn, scoring_date, config)` | Orchestrates the full reasoning pass: market context once, per-ticker analysis for all qualifying tickers, daily summary. Returns structured dict. |

**Return value of `reason_all_qualifying_tickers`:**
```python
{
    "bullish": [{"ticker": str, "score": dict, "reasoning": str}, ...],
    "bearish": [{"ticker": str, "score": dict, "reasoning": str}, ...],
    "flips":   [{"ticker": str, "flip": dict, "score": dict, "reasoning": str}, ...],
    "daily_summary": str,
    "market_context_summary": str,
}
```

**Error handling:** `call_claude` wraps all API calls in try/except. On `anthropic.APIError`,
`anthropic.APIConnectionError`, or any other exception, it logs the error and returns
`"AI analysis unavailable — see raw scores above."` so the pipeline always continues.

**VIX interpretation thresholds (used in market context):**
- calm: VIX < 15
- normal: 15 ≤ VIX < 20
- elevated: 20 ≤ VIX < 25
- high: 25 ≤ VIX < 30
- extreme: VIX ≥ 30

### 13.2 Telegram Formatter (`src/notifier/formatter.py`)

Formats signal data into readable Telegram messages. Handles Telegram's 4096-character
limit by splitting at section boundaries. Times are displayed in the configured timezone
(default: `Europe/Amsterdam`).

AI-generated content (per-ticker reasoning, daily summary, market context) is gated by
`telegram.include_ai_reasoning` in `config/notifier.json`. When `false` (the default), the
Claude API call is skipped entirely in `main.py` and only the concise signal lines are shown.
Set to `true` to restore full AI commentary.

**Public functions:**

| Function | Purpose |
|---|---|
| `format_duration(seconds)` | Human-readable duration: "45s", "2m 15s", "1h 2m 5s". |
| `format_header(scoring_date, display_timezone)` | Report header with date and local time (e.g. "📊 Signal Report — March 16, 2026 • 01:23 CET"). |
| `format_signal_distribution(bullish, bearish, neutral)` | Distribution summary line: "🟢 11 | 🔴 5 | 🟡 43". |
| `format_daily_summary_section(daily_summary)` | "📋 Daily Summary" section; empty string if no signals. Shown only when `include_ai_reasoning=true`. |
| `format_bullish_section(tickers, include_reasoning)` | "🟢 BULLISH" section sorted by confidence DESC. When `include_reasoning=False`, per-ticker reasoning is omitted. |
| `format_bearish_section(tickers, include_reasoning)` | "🔴 BEARISH" section sorted by confidence DESC. Same reasoning gate. |
| `format_flips_section(flips, include_reasoning)` | "🔄 SIGNAL FLIPS" section. Same reasoning gate. |
| `format_market_context_section(market_context)` | "📉 Market Context" section. Shown only when `include_ai_reasoning=true`. |
| `format_heartbeat(pipeline_stats)` | Pipeline completion stats with per-phase timing and ticker counts. |
| `format_full_report(results, pipeline_stats, config)` | Assembles full report and splits into `list[str]` chunks ≤ 4096 chars each. Reads `include_ai_reasoning` from config. |
| `format_no_signals_report(market_context, pipeline_stats, config)` | Minimal report for days with no qualifying signals. |
| `format_market_closed_message(date, config)` | One-line market-closed notification. |
| `format_pipeline_error_message(phase, error, config)` | Pipeline failure alert message. |

Ticker line format: `{TICKER} — {confidence:.0f}% 📊 {final_score:+.1f}`
Flip line format: `{TICKER}: {prev} → {new} ({confidence:.0f}%)`

### 13.3 Telegram Sender (`src/notifier/telegram.py`)

Wraps `send_telegram_message` from `src/common/progress.py` with report-specific helpers.
Each chunk in a multi-message report is sent with a 0.5s delay to maintain ordering.

Chat IDs are loaded via `get_telegram_config(config)`, which checks environment variables first
and falls back to `config/notifier.json` values. `TELEGRAM_CHAT_ID` is accepted as a
backward-compatible alias for `TELEGRAM_ADMIN_CHAT_ID`.

**Message routing:**

| Message type | Recipients | Function |
|---|---|---|
| Daily signal report | All `subscriber_chat_ids` | `send_daily_report` |
| Market closed notification | All `subscriber_chat_ids` | `send_market_closed_notification` |
| Pipeline heartbeat | `admin_chat_id` only | `send_heartbeat` |
| Pipeline error alerts | `admin_chat_id` only | `send_pipeline_error_alert` |

| Function | Signature | Returns |
|---|---|---|
| `get_telegram_config(config)` | `config: dict` | `{bot_token, admin_chat_id, subscriber_chat_ids}` |
| `send_daily_report(messages, bot_token, subscriber_chat_ids)` | Sends each message to all subscribers; 0.5s delay between chunks. | `{sent, failed, total_subscribers}` |
| `send_heartbeat(heartbeat_text, bot_token, admin_chat_id)` | Sends heartbeat to admin only. | `bool` |
| `send_market_closed_notification(date, bot_token, subscriber_chat_ids, config)` | Sends to all subscribers. | `{sent, failed}` |
| `send_pipeline_error_alert(phase, error, bot_token, admin_chat_id, config)` | Sends to admin only. | `bool` |

### 13.4 Notifier Orchestrator (`src/notifier/main.py`)

Phase 4 entry point. Reads pipeline events, optionally calls the AI reasoner (skipped when
`telegram.include_ai_reasoning=false`), formats and sends the report, and records the pipeline run.

**`run_notifier(db_path, pipeline_stats) -> dict`**

Pre-flight checks: verifies `scorer_done` exists; skips if `notifier_done` already completed.
On AI failure (any exception), falls back to an empty results dict and still sends a minimal report.
On Telegram failure, logs the error but writes `notifier_done=completed` anyway.

Sends the signal report (without heartbeat) to all `subscriber_chat_ids`.
Sends the heartbeat to `admin_chat_id` only.
If no subscribers are configured, logs a warning but does not crash.

Returns: `{scoring_date, bullish_count, bearish_count, neutral_count, flips_count, tickers_reasoned, telegram_sent, subscribers_notified, duration_seconds}`.

### 13.5 Telegram Bot & Interactive Commands (`src/notifier/bot.py`, `src/notifier/detail_command.py`, `src/notifier/scatter_command.py`)

An interactive Telegram bot (`python-telegram-bot`) that responds to subscriber commands.

**Commands handled:**

| Command | Description |
|---|---|
| `/detail <TICKER> [days]` | Sends a 4-panel technical chart image + AI summary for the ticker |
| `/scatter N [TICKER] [days_back]` | Sends a predicted vs actual excess return scatter plot; X-axis is a signed confidence score (`calibrated_score` when available, otherwise `final_score/100`), Y-axis is the raw N-day excess return vs SPY; per-signal-type regression lines via `np.polyfit`; IC (Information Coefficient = Spearman rank correlation) annotated in upper-right text box |
| `/tickers` | Lists all watched tickers grouped by sector |
| `/start` | Welcome message |
| `/help` | Lists available commands |

The `/detail` flow calls `generate_chart()` to produce the image, then `cleanup_chart()` to delete
the temporary file after delivery.

The `/scatter` flow calls `fetch_signals_with_forward_returns()` (finds the Nth future trading-day
close using `LIMIT 1 OFFSET N-1` against `ohlcv_daily`, computes excess return vs SPY) and
`generate_scatter_chart()` (dark-mode matplotlib scatter, dots colored green/red/gray by signal
type, one regression line per type, IC annotation text box in upper-right corner showing Spearman
rank correlation, p-value, and sample count),
then deletes the temp PNG after delivery.

#### Chart Generator (`src/notifier/chart_generator.py`)

Generates a 4-panel PNG using `mplfinance` with a fully custom dark-mode style.
The chart uses `returnfig=True` to retrieve the matplotlib figure and axes after
mplfinance plots the candlestick data, then `_annotate_chart()` applies all
post-processing before saving with `fig.savefig()`.

**Color palette:**

| Element | Color |
|---|---|
| Figure / panel background | `#0d0d0d` / `#111111` |
| Grid lines | `#2a2a2a` dashed |
| Spines / borders | `#333333` |
| Tick / axis label | `#aaaaaa` |
| Bullish candles | `#4fc3f7` (light blue) |
| Bearish candles | `#f48fb1` (soft pink) |
| EMA 9 / 21 / 50 | `#66ff99` / `#ff6ec7` / `#ffd700` |
| Bollinger Bands | `#888888` dashed |
| RSI OB line | `#ff6666` |
| RSI OS line | `#66ff99` |
| MACD line / Signal | `#00e5ff` / `#ff9800` |

**Panel layout:**

| Panel | Height | Content |
|---|---|---|
| 0 — Price | 50% | Candlestick + EMA 9/21/50 + Bollinger Bands (dashed + 5% alpha fill) + Fibonacci levels + S/R levels + volume-spike event arrows + **candlestick pattern markers** + **structural pattern lines** |
| 1 — Volume | 12% | Bull/bear colored bars (alpha 0.75) + dotted vertical lines on spike days |
| 2 — RSI | 19% | RSI(14) white line + OB/OS colored axhlines (70/30) + subtle 50 line + red/green alpha fills + "OB 70" / "OS 30" / "50" right-edge labels + legend |
| 3 — MACD | 19% | MACD line + signal line + conditional-color histogram (positive=`#4fc3f7`, negative=`#f48fb1`) + zero axhline + crossover arrows + legend |

**Labels added by `_annotate_chart()`:**

| Location | Type | Content |
|---|---|---|
| Price panel | Legend (upper right) | EMA 9 / EMA 21 / EMA 50 / BB |
| Price panel right margin | Text | S/R: `"S $123.45"`, `"R $130.00 (strong)"`; Fib: `"Fib 38.2%"` |
| Price panel | Annotate arrows | Spike days: "Buy" (bull spike) / "Sell" (bear spike) pointing to candle |
| Volume panel | Dotted axvline | One per volume spike day (volume > 1.5× 20-day rolling average) |
| RSI panel | Text (right edge) | "OB 70", "OS 30", "50" |
| RSI panel | Legend | RSI (14) |
| RSI panel | fill_between | Red (RSI > 70) / green (RSI < 30) alpha fills |
| MACD panel | Legend | MACD (12/26), Signal (9) |
| MACD panel | Annotate arrows | ↑/↓ on all MACD/signal crossovers |

**Pattern overlays** (all on price panel, sourced from `patterns_daily`):

| Category | Rendering | Color |
|---|---|---|
| Candlestick (bullish) | `ax.annotate()` arrow below candle Low; label e.g. "Hammer", "BullEng" | `#4fc3f7` |
| Candlestick (bearish) | `ax.annotate()` arrow above candle High | `#f48fb1` |
| Candlestick (neutral) | `ax.annotate()` at candle mid-price | `#aaaaaa` |
| `double_top` | M-shape `ax.plot()` through peak1 → neckline trough → peak2; dashed neckline `axhline`; `fill_between` α=0.08; "Neckline Break ▼" arrow; right-edge "Double Top ⊗" | `#ff9944` |
| `double_bottom` | W-shape `ax.plot()` through trough1 → neckline peak → trough2; dashed neckline `axhline`; `fill_between` α=0.08; "Neckline Break ▲" arrow; right-edge "Double Bottom ⊕" | `#4fc3f7` |
| `bull_flag` | Pole arrow via `ax.annotate()`; two parallel channel `ax.plot()` lines (upper/lower flag high→end high, low→end low); `fill_between` α=0.08; "Breakout ▲ Bullish" arrow at flag end; right-edge "Bull Flag" | `#66ff99` |
| `bear_flag` | Mirror of bull_flag pointing down; "Breakdown ▼ Bearish" arrow | `#f48fb1` |
| `breakout` | Dashed `axhline` at `level_price` + "Breakout ↑" directional arrow at candle date | `#4fc3f7` |
| `breakdown` | Dashed `axhline` + "Breakdown ↓" arrow | `#f48fb1` |
| `false_breakout` | Dashed `axhline` + "False BO" arrow | `#ffd700` |

All patterns within the chart's date window are shown. Trendlines require the enriched `details` JSON
(see §6.5–6.6 for fields stored per pattern type). If date keys are absent, falls back to `axhline` only.

**Structural pattern `details` fields** (stored by the calculator):

| Pattern | Key date/position fields |
|---|---|
| `double_top` | `peak1_date`, `peak1_price`, `peak2_date`, `peak2_price`, `neckline_date`, `neckline_price`, `peak_price` (avg), `distance_days` |
| `double_bottom` | `trough1_date`, `trough1_price`, `trough2_date`, `trough2_price`, `neckline_date`, `neckline_price`, `trough_price` (avg), `distance_days` |
| `bull_flag` / `bear_flag` | `pole_start_date`, `pole_start_price`, `pole_end_date`, `pole_end_price`, `flag_start_date`, `flag_start_high`, `flag_end_high`, `flag_start_low`, `flag_end_low`, `flag_retracement_pct` |
| `breakout` / `breakdown` / `false_breakout` | `level_price`, `volume_ratio` |


**General:**
- x-tick labels hidden on all panels except the bottom (MACD)
- Bottom panel x-ticks formatted as "Mon DD" (e.g. "Feb 17"), rotated 45°
- All y-labels in `#aaaaaa`, fontsize 9

Chart config (`chart_figsize`, `sr_levels_to_show`) comes from
`config/notifier.json → detail_command`.

#### AI vs deterministic content boundary in /detail message #2

The `/detail` command sends three messages. Message #2 is a structured analysis that combines
AI-generated interpretation with deterministic data pulled from the DB.

**Content boundary:**

| Section | Source | Notes |
|---|---|---|
| Verdict header (`📊 AAPL — Detail Analysis (date)`) | Deterministic | Includes optional ⚠️ earnings warning |
| `📍 VERDICT` content | Claude (AI) | BUY / SELL / HOLD-WAIT + 2-line justification |
| `⏱️ TIMEFRAME SUMMARY` table | Deterministic | Triple-backtick code block; daily/weekly/monthly scores from DB |
| `⏱️ TIMEFRAME SUMMARY` 1-line note | Claude (AI) | Timeframe agreement interpretation |
| `🧠 REASONING` | Claude (AI) | 1 paragraph; omitted when empty |
| `📊 CONFIDENCE` | Deterministic | Agreeing/disagreeing categories + calibration flag |
| `🎯 LEVELS & TRIGGERS` | Deterministic | From existing `build_key_levels` + `build_signal_change_triggers` |

**Prefilling strategy:** The assistant turn is prefilled with `<verdict>` before calling the Claude API
(`messages=[{"role":"user", ...}, {"role":"assistant","content":"<verdict>"}]`). This guarantees the
response begins with a valid XML open tag and prevents Claude from generating preamble text. Prefilling
requires Claude 3+ models. The prefill content is NOT returned in the API response — `parse_ai_response`
prepends it before regex extraction.

**Parse fallback:** If any of the three XML tags (`<verdict>`, `<timeframe_note>`, `<reasoning>`) is
missing or malformed, `parse_ai_response` logs a WARNING and returns the raw response text in the
`verdict` slot, with empty strings for the others. The command always sends a message — it never crashes
on parse failure.

**Scoring chain and category scores removed from msg #3:** In Plan B, `build_scoring_chain` and
`build_category_scores` were removed from `build_full_breakdown` (msg #3). These views were redundant
with the new `📊 CONFIDENCE` deterministic section in msg #2. The underlying functions were deleted.
`build_confidence_modifiers_section` (a simpler view) remains in msg #3.

**MarkdownV2 escaping:** AI-generated free text (verdict, timeframe_note, reasoning) and the verdict
header are escaped via `escape_markdown_v2()` before inclusion in msg #2. The timeframe table is
rendered inside a triple-backtick code block, which passes through the escaper unchanged.
The `send_telegram_message` call for each msg #2 chunk passes `parse_mode="MarkdownV2"`.
If `send_telegram_message` returns `None` (API rejection), the handler logs an ERROR with context
and a hint that MarkdownV2 escaping may have failed.

### 13.6 Daily Pipeline Script (`scripts/run_daily.py`)

The main cron entry point. Runs all 4 phases in sequence with the following error policy:
- Fetcher or Calculator failure → stop pipeline, exit 1
- Scorer failure → run notifier anyway (to report the error), exit 1
- Notifier failure → log error, exit 1
- Any phase failure → send Telegram alert to `admin_chat_id` via `send_pipeline_error_alert`
- Market closed → send notification to all `subscriber_chat_ids`, exit 0

Timing stats are collected per phase and passed to `run_notifier` for the heartbeat message.

---

## 14. Web UI (`src/web/`)

A read-only, desktop-only signal browser for the developer and up to 3-4 trusted friends. No mobile layout, no deep links, no auto-refresh in v1.

### 14.1 Architecture

```
Browser ←→ Vite/React SPA (web/dist/ static files)
              ↕ same-origin fetch with cookie
         FastAPI JSON API (src/web/app.py)
              ↕
         SQLite DB (read-only from web tier)
```

**Stack:** FastAPI + Vite/React/TypeScript SPA. Uvicorn single worker (required — in-memory LLM debounce is process-local). Bound to `127.0.0.1:port` behind Caddy (HTTPS reverse proxy). Config: `config/web.json`.

**Auth:** Shared password (`WEB_PASSWORD` env var), constant-time compare via `secrets.compare_digest`. Cookie session via Starlette `SessionMiddleware` signed with `WEB_SECRET_KEY`. 7-day TTL. `HttpOnly`, `SameSite=Lax`. IP from `request.client.host` (Uvicorn `--proxy-headers` populates from trusted X-Forwarded-For). Login rate limit: 5 attempts/IP/60s via `web_login_attempts` SQLite table; prune rows older than 1 hour on each login attempt.

**Auth routes (JSON):**
- `POST /api/login { password }` → `200 { ok: true }` + Set-Cookie, or `401 { detail }`, or `429 { detail }`
- `POST /api/logout` → `200 { ok: true }`, clears cookie
- `GET /api/me` → `200 { authenticated: true }`, or `401 { detail }`

**API routes (auth-gated, return 401 when not logged in):**
- `GET /api/tickers` — alphabetized list of active tickers
- `GET /api/dates?ticker=X` — `{min, max}` from `scores_daily`
- `GET /api/snapshot?ticker=X&date=Y` — three-card snapshot dict (404 if no data)
- `POST /api/llm { ticker, date, timeframe }` — LLM analysis (429 on debounce, 503 on Claude failure)

**Static-serve (SPA):**
- `GET /assets/*` — `StaticFiles` mount serving Vite-hashed assets from `web/dist/assets/`
- `GET /favicon.ico` — `FileResponse(dist/favicon.ico)` or 404
- `GET /robots.txt` — `FileResponse(dist/robots.txt)` or 404
- `GET /{full_path:path}` — catch-all (registered LAST): serves `dist/index.html` with `Cache-Control: no-cache` for SPA routing; returns `503 { detail: "Frontend not built." }` when `dist/` is absent; returns `404` for unmatched `/api/*` paths

No CORS. Same-origin requests only. `dist_dir` is hardcoded to `web/dist` relative to repo root (not a config key).

### 14.2 Snapshot Data Contract

`GET /api/snapshot` returns `{ daily, weekly, monthly }`. Each section:

| Field | Daily | Weekly | Monthly |
|---|---|---|---|
| `data_available` | exact match on `scores_daily.date` | most-recent `week_start <= date` | most-recent `month_start <= date` |
| `categories` | 9 cats: trend,momentum,volume,volatility,candlestick,structural,sentiment,fundamental,macro | 6 cats: trend,momentum,volume,volatility,candlestick,structural | 5 cats: trend,momentum,volume,volatility,structural (candlestick permanently excluded) |
| `signal`, `confidence`, `calibrated_score` | present | absent | absent |
| `composite_score` | `final_score` from `scores_daily` | `composite_score` from `scores_weekly` | `composite_score` from `scores_monthly` |
| `is_fallback` | absent | True when `resolved_period < picked_date` | True when `resolved_period < picked_date` |
| `sparkline` | 15 trading days, `<= picked_date` bound | 6 weeks, `<= picked_date` bound | 6 months, `<= picked_date` bound |
| `key_signals` | top-N why-bullets from `scores_daily.key_signals` (see §14.6) | absent | absent |
| `earnings` | `{next, last_surprise}` from `earnings_calendar` (see §14.6) | absent | absent |
| `signal_flip` | most-recent flip within lookback window from `signal_flips` (see §14.6) | absent | absent |

The `categories` array is the UI rendering contract. The UI renders only bars listed in this array. The `scores` dict may contain `candlestick` for monthly (always None) — the UI ignores it because `"candlestick"` is absent from the monthly `categories` array.

### 14.3 LLM Analysis

`POST /api/llm { ticker, date, timeframe }` → `{ text }`.

- **Daily**: reuses `build_ticker_context()` from `ai_reasoner.py` (full context including news/fundamentals/macro). Prompt targets ~150 words, `max_tokens=800`, `temperature=0.3`.
- **Weekly/Monthly**: `build_timeframe_context()` (indicators + patterns only; no news/fundamentals/macro). Prompt prepends a one-line disclaimer. Same model/tokens/temperature.
- **Debounce**: per-(session_id, ticker, date, timeframe) 60s in-memory window (closure dict in `create_app()`). Returns 429 if within window. Debounce is reset on Claude failure so user can retry.
- **Claude error**: returns 503 with friendly message (never exposes a stack trace).
- **Async dispatch**: `await asyncio.to_thread(call_claude_for_web, ...)` keeps the ASGI event loop responsive.

### 14.4 New DB Table: `web_login_attempts`

```sql
CREATE TABLE IF NOT EXISTS web_login_attempts (
    ip TEXT NOT NULL,
    attempted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_web_login_attempts_ip_time ON web_login_attempts(ip, attempted_at);
```

Added to `create_all_tables()` in `src/common/db.py`. No migration script needed — `CREATE TABLE IF NOT EXISTS` is idempotent.

### 14.5 Deployment

Runs as systemd service `ticker-tide-web` (unit file: `deploy/ticker-tide-web.service`). Managed by `deploy.sh` (step 12) and the GitHub Actions deploy workflow ("Restart Web service" step). Logs to `logs/web.log`. See OPERATIONS.md §Web UI Service for management commands and Caddy restoration instructions.

### 14.6 Daily Card Enrichment (daily-only)

Three high-signal additions to the daily card only. Weekly and monthly cards are intentionally unchanged (adversarial review dropped the market-context banner as QQQ-only chrome and dropped news as sentiment ≈ 0 / headlines clickbait-quality).

**Pick A — "Why" bullets (`key_signals`)**

`_extract_key_signals(score_dict, limit)` decodes `scores_daily.key_signals` (JSON-encoded string list, 7 items in production) and returns the first `limit` items. Limit comes from `config["why_bullets"]["limit"]` (default 3). Returns `[]` on missing/None/invalid JSON/non-list. Rendered as a `<ul>` with heading "Why" above the patterns section; hidden when list is empty.

**Pick B — Earnings row (`earnings`)**

`_fetch_earnings(conn, ticker, picked_date)` returns `{"next": {...}|None, "last_surprise": {...}|None}`.

- **next**: `earnings_date > picked_date AND actual_eps IS NULL` — strict `>` boundary excludes same-day; `actual_eps IS NULL` excludes future rows already reported (stale-null past rows are excluded by the `>` boundary).
- **last_surprise**: `earnings_date <= picked_date AND actual_eps IS NOT NULL` — `<=` boundary allows same-day past earnings.

`beat` is `eps_surprise > 0`; `None` when `eps_surprise` is NULL. Both subkeys may be `None` independently. UI section hidden when both are `None`.

**Pick C — Signal flip badge (`signal_flip`)**

`_fetch_signal_flip(conn, ticker, picked_date, lookback_days)` returns the most recent flip within `[picked_date - lookback_days, picked_date]` (inclusive). Query: `ORDER BY date DESC, id DESC LIMIT 1` — mandatory `id DESC` tiebreaker defends against production duplicate/contradictory rows (ASTS ×3, LLY ×2). Lookback from `config["signal_flip_lookback_days"]` (default 14). Returns `None` when no qualifying row exists. UI renders a small badge in the daily card header using a 6-transition color/glyph lookup table (green ↑ toward BULLISH, red ↓ away from BULLISH, arrows for NEUTRAL transitions).


