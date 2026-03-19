# Stock Signal Engine

Fetches daily market data for ~50 US equities, computes 15 technical indicators across daily and weekly timeframes, and scores each ticker against regime-adaptive category weights to produce BULLISH / BEARISH / NEUTRAL signals with confidence scores. Signals meeting the confidence threshold and all direction changes are delivered via Telegram with Claude-generated reasoning. No trade execution — pure signal intelligence.

## Architecture

```
config/tickers.json
        │
        ▼
  BACKFILLER ──────────────────────────────────────── data/signals.db
  (one-time)                                                 ▲
                                                             │
  FETCHER ─── cron 00:00 UTC (daily) ──────────────────────►│
  (OHLCV · fundamentals · news · macro)    fetcher_done      │
        │                                                     │
        ▼                                                     │
  CALCULATOR ──────────────────────────────────────────────►│
  (indicators · patterns · profiles)    calculator_done      │
        │                                                     │
        ▼                                                     │
  SCORER ──────────────────────────────────────────────────►│
  (regime → weights → scores → signal)    scorer_done        │
        │
        ▼
  NOTIFIER ── Claude API ──► Telegram
```

## Quick Start

```bash
git clone <repo-url> /home/ec2-user/ticker-tide
cd /home/ec2-user/ticker-tide
./deploy.sh
```

`deploy.sh` creates `.venv`, installs dependencies, initialises the database, and runs all tests. If `.env` does not exist it is created from `.env.example`.

```bash
nano .env   # set POLYGON_API_KEY, FINNHUB_API_KEY, ANTHROPIC_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_CHAT_ID
source .venv/bin/activate
python scripts/test_api_access.py          # verify all 5 API keys
python scripts/run_backfill.py             # one-time: load 5 years of data (~30–60 min)
python scripts/verify_backfill.py          # check data completeness and gaps
python scripts/run_calculator.py           # compute all indicators, patterns, profiles
python scripts/run_scorer.py --historical  # score all historical dates
python scripts/verify_pipeline.py         # verify computed data (indicators, scores, patterns)
```

Set up cron (runs 00:00 UTC, logs to a dated file, rotates logs weekly, and runs pipeline health check Sundays 06:00 UTC):

```
0 0 * * * cd /home/ec2-user/ticker-tide && /home/ec2-user/ticker-tide/.venv/bin/python scripts/run_daily.py >> logs/daily_$(date +\%Y\%m\%d).log 2>&1
0 6 * * 0 find /home/ec2-user/ticker-tide/logs -name "daily_*.log" -mtime +30 -delete
0 6 * * 0 cd /home/ec2-user/ticker-tide && .venv/bin/python scripts/verify_pipeline.py >> logs/verify_$(date +\%Y\%m\%d).log 2>&1
```

## Daily Operations

→ [OPERATIONS.md](OPERATIONS.md)

## Config Reference

→ [CONFIG.md](CONFIG.md)

## Project Structure

```
src/
├── common/
│   ├── api_client.py          # Polygon + Finnhub HTTP clients (httpx + tenacity)
│   ├── config.py
│   ├── db.py                  # get_connection(), create_all_tables()
│   ├── events.py              # pipeline_events read/write, alerts_log, pipeline_runs
│   ├── logger.py
│   ├── progress.py            # ProgressTracker + Telegram send helpers
│   ├── validators.py
│   └── yfinance_client.py
├── backfiller/
│   ├── main.py
│   ├── ohlcv.py
│   ├── fundamentals.py
│   ├── earnings.py
│   ├── corporate_actions.py
│   ├── macro.py
│   ├── news.py
│   ├── filings.py
│   ├── utils.py
│   ├── verify.py              # post-backfill raw data quality checks (10 checks)
│   └── verify_pipeline.py     # post-calculation computed data checks (29 checks)
├── fetcher/
│   ├── main.py
│   ├── earnings.py
│   └── market_calendar.py
├── calculator/
│   ├── main.py
│   ├── indicators.py
│   ├── weekly.py
│   ├── profiles.py            # percentile profiles + sector blending
│   ├── crossovers.py
│   ├── gaps.py
│   ├── swing_points.py
│   ├── support_resistance.py
│   ├── patterns.py
│   ├── divergences.py
│   ├── fibonacci.py
│   ├── relative_strength.py
│   └── news_aggregator.py
├── scorer/
│   ├── main.py
│   ├── regime.py
│   ├── indicator_scorer.py
│   ├── pattern_scorer.py
│   ├── category_scorer.py
│   ├── sector_adjuster.py
│   ├── timeframe_merger.py
│   ├── confidence.py
│   └── flip_detector.py
└── notifier/
    ├── main.py
    ├── ai_reasoner.py
    ├── formatter.py
    ├── sentiment_enrichment.py  # Finnhub sentiment enrichment via Claude Haiku
    ├── telegram.py
    ├── chart_generator.py       # 4-panel mplfinance chart for /detail command
    ├── detail_command.py        # /detail Telegram bot command handler
    └── bot.py                   # Telegram bot long-polling listener

scripts/
├── run_daily.py               # cron entry point — all 4 phases in sequence
├── run_backfill.py            # one-time historical loader
├── run_calculator.py
├── run_scorer.py
├── run_notifier.py
├── run_bot.py                 # start the interactive Telegram bot listener
├── enrich_finnhub_sentiment.py  # backfill NULL-sentiment Finnhub articles
├── setup_db.py                # initialise schema (idempotent)
├── test_api_access.py         # verify all 5 API keys
├── verify_backfill.py         # post-backfill raw data quality report
└── verify_pipeline.py         # post-calculation computed data quality report
```

## Tech Stack

| Library | Purpose |
|---|---|
| `pandas` | DataFrames across all pipeline stages |
| `numpy` | Numerical operations |
| `ta` | Technical indicator calculation (EMA, MACD, RSI, Stochastic, etc.) |
| `httpx` | Synchronous HTTP client for all API calls |
| `tenacity` | Retry with exponential backoff (max 3 attempts) |
| `python-dotenv` | `.env` loading |
| `yfinance` | Fundamentals, financial ratios, VIX, earnings calendar |
| `finnhub-python` | Supplementary news (free tier, 60 calls/min) |
| `anthropic` | Claude API for signal reasoning |
| `python-telegram-bot` | Telegram message delivery and interactive bot |
| `mplfinance` | 4-panel technical chart generation for /detail command |
| `sqlite3` | Database (stdlib, WAL mode) |
| `pytest` / `pytest-mock` | Tests |

## CI/CD — Automated Deployment

Every push to `main` triggers `.github/workflows/deploy.yml`, which SSHes into the EC2 instance and runs `./deploy.sh`.

`deploy.sh` is idempotent and handles:
- `git pull origin main` — fetch latest code
- Python 3.9+ detection and venv setup
- `pip install -r requirements.txt`
- `.env` key validation
- Database initialisation (`scripts/setup_db.py`)
- Full test suite (`pytest tests/ -v`) — deployment aborts on test failure

### Required Repository Secrets

Configure these under **Settings → Secrets → Actions** in the GitHub repository:

| Secret | Description |
|---|---|
| `EC2_HOST` | Public IP or hostname of the EC2 instance |
| `EC2_USER` | SSH username (e.g. `ec2-user`) |
| `EC2_SSH_KEY` | PEM private key with access to the EC2 instance |

## Telegram Bot (Interactive Commands)

The pipeline sends daily reports automatically. A separate interactive bot process handles on-demand commands.

### Available Commands

| Command | Description |
|---|---|
| `/detail AAPL` | Deep analysis for AAPL with 30-day chart (default) |
| `/detail AAPL 90` | Deep analysis with 90-day chart |
| `/help` | List all available commands |

### What `/detail` Returns

Three messages sent in sequence:

1. **Technical chart** (4-panel PNG): Candlestick + EMA 9/21/50 + Bollinger Bands + Fibonacci S/R levels | Volume | RSI + divergences | MACD
2. **AI analyst take**: Claude-generated 3–4 paragraph deep analysis with specific price levels and triggers
3. **Raw data breakdown**: Scoring chain, category scores, indicators, patterns, divergences, Fibonacci, sentiment, fundamentals, macro, key levels, signal triggers, 30-day signal history, earnings warning, and sector peer comparison

### Starting the Bot

```bash
tmux new -s bot
source .venv/bin/activate
python scripts/run_bot.py
# Ctrl+B, D to detach
```

See [OPERATIONS.md](OPERATIONS.md) for systemd service setup.
