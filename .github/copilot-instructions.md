# Copilot Instructions — Stock Signal Engine

## Primary Reference

**Always read `CLAUDE.md` before writing or modifying any code.** It is the authoritative source for coding standards, error handling rules, database conventions, testing requirements, and dependency choices. The sections below supplement it with architecture and project-specific context.

## Commands

```bash
# Run all tests
pytest tests/ -v

# Run a single test file
pytest tests/test_fetcher/test_something.py -v

# Run a single test by name
pytest tests/ -v -k "test_function_name"

# Install dependencies
pip install -r requirements.txt

# Run one-time historical backfill
python scripts/run_backfill.py

# Run daily pipeline
python scripts/run_daily.py
```

## Architecture

The pipeline is **event-driven via SQLite** (`pipeline_events` table). Each phase writes a `status=completed` event; the next phase polls for it before starting.

```
tickers.json → BACKFILLER (one-time)
                     ↓
              FETCHER (cron 00:00 UTC) → ohlcv_daily, fundamentals, news, etc.
                     ↓ event: fetcher_done
              CALCULATOR → indicators_daily, indicators_weekly, patterns_daily, divergences_daily
                     ↓ event: calculator_done
              SCORER → scores_daily, signal_flips
                     ↓ event: scorer_done
              NOTIFIER → Claude API (reasoning) → Telegram
```

Six modules under `src/`:
- **common/** — DB connection (WAL mode), config loader, shared logging setup
- **backfiller/** — one-time historical loader (OHLCV + fundamentals + news + macro, up to 5 years)
- **fetcher/** — daily data pull: OHLCV from Polygon, fundamentals from yfinance, earnings from Finnhub, macro from Polygon
- **calculator/** — computes 15 technical indicators (via `pandas_ta`), detects candlestick patterns, swing points, S/R levels, divergences, crossovers, gaps
- **scorer/** — regime detection (trending/ranging/volatile) → adaptive category weights → dual timeframe (daily×0.6 + weekly×0.4) → BULLISH/BEARISH/NEUTRAL
- **notifier/** — sends signals via Telegram; confidence >70% or signal flips always trigger delivery; uses Claude API for AI reasoning text

The ticker universe and sector-ETF mappings live in `config/tickers.json`. SPX is proxied via the SPY ETF (Polygon does not authorize index OHLCV on the free tier).

## Key Conventions

### TDD — tests before implementation
Write (failing) tests first, then implement. Tests live in `tests/` mirroring `src/` exactly (e.g., `src/fetcher/foo.py` → `tests/test_fetcher/test_foo.py`).

### All thresholds come from config
No magic numbers in code. Every indicator period, pattern threshold, scoring weight, and signal cutoff is read from the corresponding JSON file in `config/`. Example: `calculator.json` → indicator params; `scorer.json` → regime thresholds and adaptive weights; `notifier.json` → Telegram confidence cutoff and AI model.

### Database rules
- Enable WAL mode on every connection: `conn.execute("PRAGMA journal_mode=WAL")`
- All timestamps as UTC strings: `YYYY-MM-DD` for dates, ISO 8601 for datetimes
- All daily tables have `UNIQUE(ticker, date)`; use `INSERT OR REPLACE` for idempotent re-runs
- `conn.row_factory = sqlite3.Row` is the standard — rows are accessed by column name
- Always use parameterized queries (`?` placeholders), never string-formatted SQL

### Error handling pattern
Wrap per-ticker processing in try/except (specific exceptions only). On failure: log the error with ticker + phase + date context, write a row to `alerts_log`, then `continue` to the next ticker. Never let one ticker abort the full pipeline run.

### API clients
- HTTP calls: `httpx` (synchronous), with `tenacity` retry (exponential backoff, max 3 attempts)
- Polygon.io: `apiKey` as query param; follow `next_url` for paginated responses
- yfinance: used for financials/ratios and VIX (`^VIX`) — no auth required
- Finnhub: free tier, 60 calls/min — respect `finnhub_delay_seconds` from `fetcher.json`
- Technical indicators: `pandas_ta` only — do **not** use TA-Lib

### Logging
Use the standard library `logging` module. Log at `INFO` for normal flow, `WARNING` for recoverable issues (e.g., missing data for one ticker), `ERROR` for failures. Always include ticker symbol, pipeline phase, and date in the message.

### Test fixtures
`tests/conftest.py` provides shared fixtures — use them rather than creating local copies:
- `sample_ohlcv_dataframe` — 30 days of AAPL OHLCV as a DataFrame
- `sample_ticker_config` / `sample_tickers_list` — ticker dicts matching `tickers.json` format
- `db_connection` — temporary SQLite DB with the full schema, uses `tmp_path`, WAL mode enabled

Mock all external calls (`pytest-mock`). Never hit real APIs in tests.

### Documentation sync
When changing code, update all relevant `.md` files in the repo (README.md, DESIGN.md, and any others). The full set of docs may grow over time — check before closing any task.
