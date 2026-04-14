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
| `src/common/db.py` | `get_connection(path)` → WAL mode + `row_factory`; `create_all_tables()` → idempotent schema creation |
| `src/common/events.py` | `pipeline_events` read/write; `alerts_log` insert; `pipeline_runs` logging; trading day detection |
| `src/common/logger.py` | `setup_root_logging()` — call once per entry-point script; format: `[YYYY-MM-DD HH:MM:SS] LEVEL [module] msg` |
| `src/common/progress.py` | `ProgressTracker` class; `send_telegram_message()`; `edit_telegram_message()` |
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
| `src/backfiller/verify_pipeline.py` | 29 computed data checks (indicators, scores, patterns, profiles); `run_full_pipeline_verification()` → `VerificationReport` |
| `src/fetcher/main.py` | Daily fetch orchestrator; gated on market calendar; writes `fetcher_done` event |
| `src/fetcher/earnings.py` | Periodic earnings calendar refresh |
| `src/fetcher/market_calendar.py` | `is_market_open_today()` via Polygon market holidays endpoint |
| `src/calculator/main.py` | `run_calculator(mode, target_date)` — orchestrates all sub-modules per ticker; `target_date` is the trading date the fetcher processed (yesterday UTC in daily pipeline) and drives the `fetcher_done` pre-flight check |
| `src/calculator/indicators.py` | 15 technical indicators via `ta` library → `indicators_daily` |
| `src/calculator/weekly.py` | Weekly OHLCV candles + weekly indicators → `weekly_candles`, `indicators_weekly` |
| `src/calculator/profiles.py` | Percentile profiles (p5–p95); sector profile blending → `indicator_profiles` |
| `src/calculator/crossovers.py` | EMA and MACD crossover detection → `crossovers_daily` |
| `src/calculator/gaps.py` | Gap classification (Breakaway/Continuation/Exhaustion/Common) → `gaps_daily` |
| `src/calculator/swing_points.py` | Swing high/low detection → `swing_points` |
| `src/calculator/support_resistance.py` | Cluster swing points into S/R levels → `support_resistance` |
| `src/calculator/patterns.py` | 7 candlestick + 7 structural patterns → `patterns_daily` |
| `src/calculator/divergences.py` | Regular/Hidden Bullish/Bearish divergences → `divergences_daily` |
| `src/calculator/fibonacci.py` | Fibonacci retracement levels (on-the-fly; not stored) |
| `src/calculator/relative_strength.py` | RS vs SPY and sector ETF (on-the-fly; not stored) |
| `src/calculator/news_aggregator.py` | `news_articles` → `news_daily_summary` per ticker per day |
| `src/scorer/main.py` | `run_scorer()` and `run_historical_scoring()`; per-ticker `score_ticker()` pipeline |
| `src/scorer/regime.py` | Trending/Ranging/Volatile detection from ADX, ATR, VIX; EMA stack alignment override (close/EMA9/EMA21/EMA50 fully aligned → Trending even with low ADX) |
| `src/scorer/indicator_scorer.py` | Maps indicator values → [−100, +100] using percentile profiles; momentum oscillators (RSI, Stochastic %K, CCI, Williams %R) accept a `regime` parameter — `"trending"` flips to `higher_is_bullish=True` (trend-continuation), `"ranging"`/`"volatile"` use mean-reversion |
| `src/scorer/pattern_scorer.py` | Scores patterns, divergences, crossovers, gaps, Fibonacci, news, fundamentals, macro |
| `src/scorer/category_scorer.py` | Aggregates component scores into 9 categories; applies adaptive weights |
| `src/scorer/sector_adjuster.py` | Sector ETF trend score → ±5 to ±10 adjustment on final score |
| `src/scorer/timeframe_merger.py` | Merges daily + weekly into composite score using regime-adaptive weights (trending: 0.2d/0.8w, ranging: 0.8d/0.2w, volatile: 0.5/0.5); `compute_weekly_score()` scores all 14 indicators from `indicators_weekly`; requires `scoring_date` and `regime` |
| `src/scorer/calibrator.py` | Rolling ridge regression calibrator: trains on recent signals + realized 10-day excess returns (vs SPY), predicts expected excess return for current signal; 15 features (6 category scores + 6 raw indicators + 3 EMA spreads); cold-start fallback when < 30 samples; `calibrate_score()` is the main entry point |
| `src/scorer/confidence.py` | Signal classification; confidence modifiers; `data_completeness`; `key_signals` |
| `src/scorer/flip_detector.py` | Detects signal direction changes → `signal_flips` |
| `src/notifier/main.py` | `run_notifier()` — queries scores, calls AI reasoner, formats, sends Telegram |
| `src/notifier/ai_reasoner.py` | `reason_all_qualifying_tickers()` — Claude API calls per qualifying ticker |
| `src/notifier/sentiment_enrichment.py` | Finnhub sentiment enrichment via Claude Haiku; `run_sentiment_enrichment()` + `enrich_batch()` |
| `src/notifier/formatter.py` | Formats full report, heartbeat, and no-signals variants |
| `src/notifier/telegram.py` | Telegram send/edit helpers |
| `src/notifier/bot.py` | Long-polling bot; `/detail`, `/scatter`, `/tickers`, `/help` handlers; logs every incoming command to `telegram_message_log` |
| `src/notifier/tickers_command.py` | `/tickers` Telegram bot command handler; logs invocations to `telegram_message_log` |
| `src/notifier/scatter_command.py` | `/scatter` bot command handler; queries `scores_daily` + `ohlcv_daily` to compute N-day forward excess returns (vs SPY), plots calibrated_score (predicted) vs actual excess return scatter chart, sends PNG via Telegram |

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
```

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

6. **Write tests first (TDD)**:
    - `tests/test_calculator/test_indicators.py` — test that the value is computed and stored correctly
    - `tests/test_scorer/test_indicator_scorer.py` — test the score mapping

7. **Re-run calculator in full mode** to populate the new column:

    ```bash
    python scripts/run_calculator.py --mode full
    python scripts/run_scorer.py --historical
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

---

## Code Conventions

→ [CLAUDE.md](CLAUDE.md)

Key rules that affect most code changes:

- **TDD** — write failing tests before implementing
- **No magic numbers** — every threshold and period comes from a config file
- **Error handling** — catch specific exceptions; log with ticker + phase + date; `continue` to next ticker; never abort the full pipeline
- **Parameterized SQL** — always use `?` placeholders; never f-string SQL
- **WAL mode** — every `get_connection()` call enables WAL; never open SQLite directly without it
