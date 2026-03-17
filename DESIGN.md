# DESIGN.md — Stock Signal Engine

## 1. Overview

A signal generation engine that analyzes ~50 US stock tickers daily and produces:
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
| `src/backfiller/verify.py` | Post-backfill data verification + report | — |

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

## 3. Data Sources

### 3.1 Polygon.io (api.polygon.io)
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
- id TEXT PRIMARY KEY
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

### Pattern & Signal Tables

**swing_points**
- ticker TEXT NOT NULL, date TEXT NOT NULL
- type TEXT (high/low)
- price REAL
- strength INTEGER
- UNIQUE(ticker, date, type)

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

**divergences_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- indicator TEXT (rsi/macd/obv/stochastic)
- divergence_type TEXT (regular_bullish/regular_bearish/hidden_bullish/hidden_bearish)
- price_swing_1_date TEXT, price_swing_1_value REAL
- price_swing_2_date TEXT, price_swing_2_value REAL
- indicator_swing_1_value REAL, indicator_swing_2_value REAL
- strength INTEGER (1-5)

**crossovers_daily**
- id INTEGER PRIMARY KEY AUTOINCREMENT
- ticker TEXT NOT NULL, date TEXT NOT NULL
- crossover_type TEXT (ema_9_21/ema_21_50/macd_signal)
- direction TEXT (bullish/bearish)
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
- final_score REAL (-100 to +100)
- regime TEXT (trending/ranging/volatile)
- daily_score REAL, weekly_score REAL
- trend_score REAL, momentum_score REAL, volume_score REAL
- volatility_score REAL, candlestick_score REAL, structural_score REAL
- sentiment_score REAL, fundamental_score REAL, macro_score REAL
- data_completeness TEXT (JSON)
- key_signals TEXT (JSON array)
- UNIQUE(ticker, date)

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

### Dual Timeframe: Final = (Daily × 0.6) + (Weekly × 0.4)
### Signal: +30 to +100 = BULLISH, -30 to +30 = NEUTRAL, -100 to -30 = BEARISH
### Confidence: |Final Score|% + modifiers (timeframe agreement, volume confirmation, earnings proximity, VIX, etc.)

## 12. Historical Scoring (Option E)
- Last 12 months: daily scores
- Months 13-60: weekly scores

## 13. Notifier
- Claude API (Anthropic) for AI reasoning
- Telegram: confidence > 70% OR signal flips (always)
- Daily summary + pipeline heartbeat
