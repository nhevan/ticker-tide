# Stock Signal Engine

A daily stock signal generation engine that analyzes ~50 US stock tickers and produces BULLISH / BEARISH / NEUTRAL signals with confidence scores and AI-generated reasoning.

## What It Does

- Fetches daily OHLCV price data, fundamentals, news, and macro data
- Computes 15 technical indicators across daily and weekly timeframes
- Detects candlestick patterns, support/resistance levels, divergences, and chart patterns
- Scores each ticker across 9 weighted categories with adaptive regime detection
- Generates AI reasoning via Claude API and delivers signals via Telegram

## Project Structure

```
ticker-tide/
├── config/             # JSON configuration files
│   ├── tickers.json    # Ticker universe and sector ETF mappings
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
│   ├── calculator/     # Technical indicator computation and pattern detection
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

### 4. Run the daily pipeline (or set up cron)

```bash
python scripts/run_daily.py
```

Recommended cron (runs at midnight UTC):
```
0 0 * * * /path/to/.venv/bin/python /path/to/scripts/run_daily.py
```

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

Confidence is the absolute score value plus modifiers for timeframe agreement, volume confirmation, earnings proximity, and VIX conditions.

Signals with confidence > 70% and all signal flips are delivered via Telegram.
