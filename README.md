# Stock Signal Engine

A daily stock signal generation engine that analyzes ~50 US stock tickers and produces BULLISH / BEARISH / NEUTRAL signals with confidence scores and AI-generated reasoning.

## What It Does

- Fetches daily OHLCV price data, fundamentals, news, and macro data
- Computes 15 technical indicators across daily and weekly timeframes; builds per-stock percentile profiles calibrated to each ticker's own history
- Aggregates news sentiment into daily summaries; computes relative strength vs SPY and sector ETFs
- Detects candlestick patterns, support/resistance levels, divergences, and chart patterns
- Scores each ticker across 9 weighted categories with adaptive regime detection
- Generates AI reasoning via Claude API and delivers signals via Telegram

## Project Structure

```
ticker-tide/
├── config/             # JSON configuration files
│   ├── tickers.json    # Ticker universe, sector ETF mappings, and ticker aliases
│   ├── backfiller.json # Historical data backfill settings
│   ├── fetcher.json    # Daily fetch schedule and API rate limits
│   ├── calculator.json # Indicator parameters and pattern detection thresholds
│   ├── scorer.json     # Scoring weights, regime detection, signal thresholds
│   ├── notifier.json   # AI model config and Telegram delivery settings
│   └── database.json   # SQLite path and maintenance settings
├── src/
│   ├── common/         # Shared utilities: DB connection, config loader, logging, validation, events, progress
│   ├── backfiller/     # One-time historical data loader
│   ├── fetcher/        # Daily data fetch (OHLCV, news, fundamentals, macro)
│   ├── calculator/     # Technical indicator computation, weekly candles, profiles, relative strength, news aggregation
│   ├── scorer/         # Signal scoring engine
│   ├── notifier/       # AI reasoning + Telegram delivery
│   └── dashboard/      # (future) Web dashboard
├── tests/              # pytest tests mirroring src/ structure
│   ├── conftest.py     # Shared fixtures
│   ├── test_common/
│   ├── test_backfiller/
│   ├── test_fetcher/
│   ├── test_calculator/
│   ├── test_scorer/
│   └── test_notifier/
├── scripts/            # Entry point scripts
├── data/               # SQLite database and backups (git-ignored)
├── .env.example        # Environment variable template
├── requirements.txt    # Python dependencies
├── CLAUDE.md           # Coding standards and conventions
└── DESIGN.md           # Architecture and schema reference
```

## Deployment

### First time setup (EC2)
```bash
git clone https://github.com/your-repo/stock-signal-engine.git
cd stock-signal-engine
chmod +x deploy.sh
./deploy.sh
```

### Subsequent deploys
```bash
./deploy.sh
```

### What deploy.sh does
1. Pulls latest code from main
2. Verifies Python 3.10+
3. Creates/activates virtual environment
4. Installs dependencies
5. Validates .env configuration
6. Creates data directories
7. Initializes database schema
8. Runs all tests
9. Prints deployment summary

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd ticker-tide
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in your API keys:
# - POLYGON_API_KEY (from polygon.io)
# - FINNHUB_API_KEY (from finnhub.io)
# - ANTHROPIC_API_KEY (from console.anthropic.com)
# - TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (from BotFather)
```

### 3. Run the backfill (one-time historical data load)

```bash
python scripts/run_backfill.py
```

This loads up to 5 years of OHLCV data, fundamentals, earnings calendar, corporate actions (dividends, splits, short interest), news, and macro data for all tickers in `config/tickers.json`.

**Ticker aliases (historical renames):** When a company changes its ticker symbol (e.g., Facebook `FB` → `META` on June 9, 2022), add `"former_symbol"` and `"symbol_since"` fields to the ticker entry in `tickers.json`. The OHLCV backfiller automatically splits the date range — fetching pre-rename history under the old symbol and storing everything under the current ticker:

```json
{
  "symbol": "META",
  "former_symbol": "FB",
  "symbol_since": "2022-06-09"
}
```

The backfill modules and their data sources:
- `src/backfiller/ohlcv.py` — daily OHLCV bars from Polygon
- `src/backfiller/macro.py` — treasury yields and VIX from Polygon/yfinance
- `src/backfiller/fundamentals.py` — quarterly financials and ratios from yfinance
- `src/backfiller/earnings.py` — earnings calendar (dates, EPS) from yfinance
- `src/backfiller/corporate_actions.py` — dividends, splits, short interest from Polygon
- `src/backfiller/news.py` — news articles + AI sentiment from Polygon (3 months) + Finnhub (1 month)
- `src/backfiller/filings.py` — 8-K SEC filings from Polygon (6 months)

**Targeted runs** — backfill a single ticker or single phase:
```bash
python scripts/run_backfill.py --ticker AAPL
python scripts/run_backfill.py --phase news
python scripts/run_backfill.py --ticker AAPL --phase ohlcv
```

**Verify API access** before running a full backfill:
```bash
python scripts/test_api_access.py
```

### 4. Run the Calculator (Phase 2b — compute all indicators, patterns, and signals)

After backfill completes, run the calculator to compute all technical indicators,
patterns, divergences, swing points, support/resistance levels, profiles, and weekly data:

```bash
python scripts/run_calculator.py
```

**Modes:**
- `full` (default) — recompute everything from scratch for all historical data
- `incremental` — compute only new data for the current day (used in the daily pipeline)

**Targeted runs:**
```bash
python scripts/run_calculator.py --mode incremental     # daily update only
python scripts/run_calculator.py --ticker AAPL           # single ticker only
python scripts/run_calculator.py --mode full --ticker AAPL  # full recompute for AAPL
```

The calculator processes tickers in this dependency order per ticker:
indicators → crossovers, swing points → support/resistance → patterns, divergences → profiles.
Weekly candles and news aggregation run independently.
Sector ETFs and market benchmarks (SPY, QQQ, XLK, etc.) also get indicators + weekly data
for sector scoring and relative strength computation.

### 5. Run the Scorer (Phase 3 — generate BULLISH/BEARISH/NEUTRAL signals)

After the calculator completes, run the scorer to generate signals with confidence scores:

```bash
python scripts/run_scorer.py
```

**Targeted runs:**
```bash
python scripts/run_scorer.py --ticker AAPL          # single ticker only
python scripts/run_scorer.py --historical            # Option E historical backfill
python scripts/run_scorer.py --historical --ticker AAPL  # historical for AAPL only
```

**Historical scoring (Option E):**
- Last 12 months: daily scores for each trading day
- Months 13-60: weekly scores only (lighter computation)

The scorer:
1. Scores each ticker across 9 weighted categories using regime-adaptive weights
2. Merges daily (×0.6) and weekly (×0.4) scores
3. Classifies the signal (BULLISH ≥ +30, BEARISH ≤ -30, otherwise NEUTRAL)
4. Computes confidence (0-100%) with modifiers for timeframe agreement, volume confirmation, earnings proximity, VIX level, and data completeness
5. Detects signal flips (direction changes) and records them in `signal_flips`
6. Saves results to `scores_daily` and sends a Telegram summary

### 6. AI Reasoning (Phase 4 — generate human-readable signal analysis)

After the scorer completes, the AI reasoner calls Claude to explain each qualifying signal. It
identifies confluences, flags contradictions, and produces actionable 2-4 sentence analyses.

**Qualifying tickers** (controlled by `config/notifier.json`):
- BULLISH or BEARISH with confidence ≥ 70% — individual per-ticker analysis
- Signal flips — always included regardless of confidence
- Each section capped at `max_tickers_per_section` (default 10) to control API costs

**Output of `reason_all_qualifying_tickers()`:**
```python
{
    "bullish": [{"ticker": str, "score": dict, "reasoning": str}, ...],
    "bearish": [{"ticker": str, "score": dict, "reasoning": str}, ...],
    "flips":   [{"ticker": str, "flip": dict, "score": dict, "reasoning": str}, ...],
    "daily_summary": str,
    "market_context_summary": str,
}
```

**Config (`config/notifier.json`):**
```json
{
  "ai_reasoner": {
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 4096,
    "temperature": 0.3
  },
  "telegram": {
    "confidence_threshold": 70,
    "max_tickers_per_section": 10,
    "always_include_flips": true
  }
}
```

**Error handling:** Claude API failures return `"AI analysis unavailable — see raw scores above."` —
the pipeline never crashes due to an AI error.

### 7. Run the daily pipeline (or set up cron)

```bash
python scripts/run_daily.py
```

Recommended cron (runs at midnight UTC):
```
0 0 * * * /path/to/.venv/bin/python /path/to/scripts/run_daily.py
```

## Cron Setup (Daily Pipeline)

The daily pipeline runs at 00:00 UTC (01:00 CET) after US market data settles.

### Set up the cron job

```bash
# Open crontab editor
crontab -e

# Add this line (adjust paths to your installation):
0 0 * * * cd /home/ec2-user/ticker-tide && /home/ec2-user/ticker-tide/.venv/bin/python scripts/run_daily.py >> logs/daily_$(date +\%Y\%m\%d).log 2>&1
```

What the cron line does:
- `0 0 * * *` — runs at 00:00 UTC every day
- `cd /home/ec2-user/ticker-tide` — changes to the project directory
- `.venv/bin/python` — uses the project's virtual environment Python
- `scripts/run_daily.py` — runs the daily pipeline
- `>> logs/daily_YYYYMMDD.log 2>&1` — appends output to a dated log file

### Verify cron is running

```bash
# List cron jobs
crontab -l

# Check cron service is active
systemctl status crond

# Check recent logs
tail -f logs/daily_$(date +%Y%m%d).log
```

### Log rotation

Add a weekly cleanup to crontab to prevent log buildup:

```bash
# Delete logs older than 30 days (runs weekly on Sunday)
0 6 * * 0 find /home/ec2-user/ticker-tide/logs -name "daily_*.log" -mtime +30 -delete
```

The `logs/` directory is created automatically by the pipeline. It is excluded from version control via `.gitignore`.

## Running Tests

```bash
pytest tests/ -v
```

Tests mock all external API calls. No real API keys required to run the test suite.

## Data Sources

| Source | Used For | Auth |
|--------|----------|------|
| Polygon.io | OHLCV, news, 8-K filings, dividends, splits, short interest, treasury yields | API key |
| yfinance | Fundamentals, financial ratios, VIX, earnings calendar | None |
| Finnhub | Supplementary news | API key |
| Anthropic Claude | AI signal reasoning | API key |
| Telegram | Signal delivery | Bot token |

## Signal Logic

Signals are generated by scoring each ticker across 9 categories with adaptive weights based on market regime:

| Signal | Score Range |
|--------|-------------|
| BULLISH | +30 to +100 |
| NEUTRAL | -30 to +30 |
| BEARISH | -100 to -30 |

Confidence is the absolute score value (0-100) plus modifiers for timeframe agreement (+10/-15), volume confirmation (+10/-10), indicator consensus (+5/-10), earnings proximity (-15 if within 7 days), VIX extremes (-10 if VIX > 30), ATR expansion (-5), and data completeness (-5/-3 for missing news/fundamentals). Final confidence is clamped to [0, 100].

Signals with confidence > 70% and all signal flips are delivered via Telegram.
