# OPERATIONS.md — Stock Signal Engine Runbook

## Daily Pipeline

### Schedule

| Item | Value |
|---|---|
| Cron schedule | `0 0 * * *` (00:00 UTC every day) |
| Local time (CET/CEST) | 01:00 CET (winter) / 02:00 CEST (summer) |
| Skips | Weekends and US market holidays (`fetcher.json schedule.skip_weekends/skip_market_holidays`) |
| Cron entry point | `scripts/run_daily.py` |

### Phase order and expected duration

| Phase | Script called | Expected duration |
|---|---|---|
| 2a — Fetcher | `run_daily_fetch()` | 3–8 min (API latency per ~65 symbols) |
| 2b — Calculator | `run_calculator(mode='incremental')` | 2–4 min |
| 3 — Scorer | `run_scorer()` | 1–3 min |
| 4 — Notifier | `run_notifier()` | 2–5 min (Claude API per qualifying ticker) |
| **Total** | | **~10–20 min** |

If the market is closed, `run_daily.py` sends a "market closed" Telegram message and exits 0 without running any phase.

### Verify pipeline ran

```bash
# Check cron is installed
crontab -l

# Check cron daemon is running (Amazon Linux / RHEL)
systemctl status crond

# Check cron daemon is running (Ubuntu / Debian)
systemctl status cron

# Tail today's log
tail -50 /home/ec2-user/ticker-tide/logs/daily_$(date +%Y%m%d).log
```

---

---

## Telegram Bot Service

The interactive bot (`/detail`, `/help`, `/tickers`) runs as a systemd service and is managed by `deploy.sh` — no manual startup needed.

### Status and control

```bash
# Check if the bot is running
sudo systemctl status ticker-tide-bot

# Stop / start / restart
sudo systemctl stop ticker-tide-bot
sudo systemctl start ticker-tide-bot
sudo systemctl restart ticker-tide-bot

# Live log tail (systemd journal)
sudo journalctl -u ticker-tide-bot -f

# File-based log
tail -f /home/ec2-user/ticker-tide/logs/bot.log
```

### How it is managed

`deploy.sh` installs `deploy/ticker-tide-bot.service` to `/etc/systemd/system/` on every deploy, then runs `systemctl enable` + `systemctl restart`. The service:
- Auto-starts on EC2 reboot
- Restarts automatically within 5 seconds on any crash or clean exit
- Reads credentials directly from `.env` via `EnvironmentFile`

Never start `run_bot.py` manually in a tmux session on EC2 — the systemd service handles it.

---

## Manual Commands

| Script | Purpose | Common flags |
|---|---|---|
| `run_daily.py` | Run all 4 phases in sequence | `--force` (bypass "already completed" check), `--date YYYY-MM-DD` (backfill a specific past date), `--db-path PATH` |
| `run_backfill.py` | One-time historical data load | `--ticker AAPL`, `--phase ohlcv`, `--force`, `--db-path PATH` |
| `run_calculator.py` | Compute indicators, patterns, profiles | `--mode full\|incremental`, `--ticker AAPL`, `--db-path PATH` |
| `run_scorer.py` | Generate BULLISH/BEARISH/NEUTRAL signals | `--ticker AAPL`, `--historical`, `--db-path PATH` |
| `run_notifier.py` | Send Telegram report from latest scores | `--db-path PATH` |
| `enrich_finnhub_sentiment.py` | Enrich Finnhub articles with Claude sentiment | `--all`, `--ticker AAPL`, `--dry-run`, `--db-path PATH` |
| `setup_db.py` | Create/migrate schema (idempotent) | `(none)` |
| `test_api_access.py` | Test all 5 API keys | `(none)` |
| `verify_backfill.py` | Post-backfill data quality checks | `--ticker AAPL`, `--quiet`, `--no-telegram`, `--db-path PATH` |
| `verify_pipeline.py` | Post-calculation computed data checks | `--date YYYY-MM-DD`, `--quiet`, `--no-telegram`, `--db-path PATH` |

Valid `--phase` values for `run_backfill.py`: `ohlcv`, `macro`, `fundamentals`, `earnings`, `corporate_actions`, `news`, `filings`.

### Finnhub Sentiment Enrichment

Finnhub articles arrive without sentiment scores. `src/notifier/sentiment_enrichment.py` uses Claude Haiku (cheapest model) to classify them as `positive`, `negative`, or `neutral`.

**Automatic (daily):** Runs as a post-processing step inside `run_daily_fetch()` after news is fetched. Processes up to `max_articles_per_run` new NULL-sentiment articles. Non-critical — a failure is logged as a warning and does not abort the fetcher.

**Manual backfill:** Run `scripts/enrich_finnhub_sentiment.py` after the initial `run_backfill.py`:

```bash
# Preview what would be processed (no API calls)
python scripts/enrich_finnhub_sentiment.py --dry-run

# Process up to max_articles_per_run (from config/notifier.json)
python scripts/enrich_finnhub_sentiment.py

# Process ALL NULL-sentiment articles (no cap — full backfill)
python scripts/enrich_finnhub_sentiment.py --all

# Enrich only a specific ticker
python scripts/enrich_finnhub_sentiment.py --ticker AAPL
```

After the backfill (`run_backfill.py`) completes, run the enrichment before the calculator to ensure `news_daily_summary` is computed with real sentiment values:

```bash
python scripts/run_backfill.py
python scripts/enrich_finnhub_sentiment.py --all   # ~15,000 articles ≈ $1.50
python scripts/run_calculator.py
python scripts/run_scorer.py --historical
```

---

## Monitoring

All queries below run against `data/signals.db` from the project root.

### Did the pipeline run today?

```bash
python -c "
import sqlite3, json
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT event, date, status, timestamp FROM pipeline_events \"
    \"WHERE date = date('now') ORDER BY timestamp\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

Expected output on a successful run day:

```
('fetcher_done', '2026-03-18', 'completed', '2026-03-18T00:07:42...')
('calculator_done', '2026-03-18', 'completed', '2026-03-18T00:11:15...')
('scorer_done', '2026-03-18', 'completed', '2026-03-18T00:13:01...')
('notifier_done', '2026-03-18', 'completed', '2026-03-18T00:16:44...')
```

### Check for errors (last 7 days)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT date, phase, severity, ticker, message FROM alerts_log \"
    \"WHERE date >= date('now', '-7 days') ORDER BY created_at DESC LIMIT 50\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

### Check signal output (latest scoring date)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT ticker, signal, confidence, final_score, regime \"
    \"FROM scores_daily \"
    \"WHERE date = (SELECT max(date) FROM scores_daily) \"
    \"ORDER BY confidence DESC\"
).fetchall()
print(f'Total: {len(rows)}')
for r in rows: print(r)
conn.close()
"
```

### Check pipeline phase durations (last 7 runs)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT date, phase, status, duration_seconds, tickers_processed, tickers_failed \"
    \"FROM pipeline_runs ORDER BY started_at DESC LIMIT 28\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

### Bot command usage analytics

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT command, COUNT(*) as count \"
    \"FROM telegram_message_log \"
    \"GROUP BY command ORDER BY count DESC\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

To see usage by user over the last 30 days:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT username, command, COUNT(*) as count \"
    \"FROM telegram_message_log \"
    \"WHERE received_at >= datetime('now', '-30 days') \"
    \"GROUP BY username, command ORDER BY count DESC\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

### Check signal flips (last 7 days)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
rows = conn.execute(
    \"SELECT ticker, date, previous_signal, new_signal, new_confidence \"
    \"FROM signal_flips WHERE date >= date('now', '-7 days') ORDER BY date DESC\"
).fetchall()
for r in rows: print(r)
conn.close()
"
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No log file for today | `crond` stopped | `sudo systemctl start crond` (RHEL/AL) or `sudo systemctl start cron` (Ubuntu); verify with `crontab -l` |
| Log file exists but is empty or shows "market closed" | Weekend or US market holiday | Expected — no action needed |
| `fetcher_done` missing from `pipeline_events` | Polygon API down, key expired, or network issue | `python scripts/test_api_access.py`; check `alerts_log` for the error message |
| `calculator_done` missing | OOM, corrupt OHLCV data, or `fetcher_done` missing | Check available RAM (`free -h`); if data issue run `python scripts/run_backfill.py --ticker <TICKER> --phase ohlcv --force` |
| Scores all NEUTRAL | Signal thresholds too tight or score compression | Check `scorer.json signal_thresholds` (default `bullish: 20, bearish: -20`); also check `scoring.score_expansion_factor`; re-run `python scripts/run_scorer.py --historical` after changing |
| No Telegram message received | Invalid bot token or wrong admin chat ID | `python scripts/test_api_access.py`; confirm `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_CHAT_ID` in `.env`; send `/start` to the bot |
| Pipeline skips with "already completed" | Phase already ran today | Add `--force` to the script: `python scripts/run_daily.py --force` |
| `database is locked` error | Another process has the DB open | `lsof data/signals.db` to find the PID; kill it; WAL mode allows concurrent reads but only one writer |
| New ticker shows no data after adding | Backfill not run for new ticker | See "Adding a Ticker" below |
| Disk space full | DB or log growth | Run VACUUM (see Database section); delete old logs: `find logs/ -name "daily_*.log" -mtime +30 -delete` |
| `fetcher_done` stuck at `processing` | Previous run crashed mid-phase | `python scripts/run_daily.py --force` — force re-runs write a fresh event |
| Scorer shows `partial` status for a ticker | One calculator module failed for that ticker | Check `alerts_log` for the specific error; re-run `python scripts/run_calculator.py --mode full --ticker <TICKER>` |

---

## Adding / Removing Tickers

### Adding a ticker

```bash
# 1. Edit config/tickers.json — add entry:
#    { "symbol": "NVDA", "sector": "Technology", "sector_etf": "XLK",
#      "added": "2026-03-18", "active": true }
#    For renamed tickers add: "former_symbol": "FB", "symbol_since": "2022-06-09"

# 2. Backfill all data for the new ticker
python scripts/run_backfill.py --ticker NVDA

# 3. Compute all indicators, patterns, and profiles
python scripts/run_calculator.py --mode full --ticker NVDA

# 4. Score all historical dates
python scripts/run_scorer.py --historical --ticker NVDA

# 5. Verify data completeness
python scripts/verify_backfill.py --ticker NVDA
```

### Removing a ticker

```bash
# Set "active": false in config/tickers.json
# The next daily run will skip the ticker.
# Data is NOT deleted — set active: true to reactivate.
```

---

## Database

| Item | Value |
|---|---|
| Location | `data/signals.db` (configured in `config/database.json path`) |
| Size | ~200 MB for 50 tickers × 5 years; grows ~1–5 MB/day during daily runs |
| Auto-backup | Before each daily run to `data/backups/` (controlled by `database.json backup_before_run: true`) |
| Auto-vacuum | Every 7 days (`database.json vacuum_frequency_days: 7`) |

### Query via sqlite3 CLI

```bash
sqlite3 data/signals.db
sqlite> SELECT ticker, signal, confidence FROM scores_daily WHERE date = date('now') ORDER BY confidence DESC;
sqlite> .quit
```

### Manual VACUUM (reclaim disk space)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/signals.db')
conn.execute('VACUUM')
conn.close()
print('VACUUM complete')
"
```

### Full reset (delete and rebuild)

```bash
# WARNING: deletes all data
rm data/signals.db data/signals.db-shm data/signals.db-wal
python scripts/setup_db.py
python scripts/run_backfill.py
python scripts/run_calculator.py
python scripts/run_scorer.py --historical
```

---

## Logs

| Item | Value |
|---|---|
| Location | `logs/` |
| Naming | `daily_YYYYMMDD.log` (cron redirects stdout+stderr) |
| Rotation | Weekly cron job deletes files older than 30 days (add line below to crontab) |

```
0 6 * * 0 find /home/ec2-user/ticker-tide/logs -name "daily_*.log" -mtime +30 -delete
```

Weekly pipeline health check (Sundays 06:00 UTC):

```
# Weekly pipeline health check (Sundays at 06:00 UTC)
0 6 * * 0 cd /home/ec2-user/ticker-tide && .venv/bin/python scripts/verify_pipeline.py >> logs/verify_$(date +\%Y\%m\%d).log 2>&1
```

Log format (set in `src/common/logger.py`):

```
[2026-03-18 00:07:42] INFO [fetcher.main] phase=fetcher date=2026-03-18 Starting daily fetch
```

---

## Migration / Moving to a New Server

### Migration checklist

| Step | Command | Verify |
|---|---|---|
| Clone repo | `git clone <repo-url> /home/ec2-user/ticker-tide` | `ls /home/ec2-user/ticker-tide/src/` |
| Run deploy | `cd /home/ec2-user/ticker-tide && ./deploy.sh` | "All tests passed" in output |
| Restore `.env` | `scp old-server:/home/ec2-user/ticker-tide/.env /home/ec2-user/ticker-tide/.env` | `python scripts/test_api_access.py` |
| Restore DB | `scp old-server:/home/ec2-user/ticker-tide/data/signals.db /home/ec2-user/ticker-tide/data/` | `python scripts/verify_backfill.py --quiet && python scripts/verify_pipeline.py --quiet` |
| Set up cron | `crontab -e` — paste cron lines from README | `crontab -l` |
| Test pipeline | `source .venv/bin/activate && python scripts/run_daily.py --force` | Telegram received |

### EC2 → EC2 (Amazon Linux 2023)

`deploy.sh` handles the full setup. The process is identical on any AL2023 instance.

```bash
# On old server — stop cron first
crontab -l > /tmp/crontab_backup.txt
crontab -r

# Transfer files (run from new server)
scp -i key.pem ec2-user@OLD_IP:/home/ec2-user/ticker-tide/.env /home/ec2-user/ticker-tide/.env
scp -i key.pem ec2-user@OLD_IP:/home/ec2-user/ticker-tide/data/signals.db /home/ec2-user/ticker-tide/data/

# Restore cron on new server
crontab /tmp/crontab_backup.txt
```

### Ubuntu / Debian differences

```bash
# Package manager
sudo apt update && sudo apt install -y python3 python3-venv python3-pip git

# Cron daemon name
sudo systemctl status cron        # NOT crond

# pyenv (if needed)
sudo apt install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
  libreadline-dev libsqlite3-dev libffi-dev
curl https://pyenv.run | bash
pyenv install 3.11.9 && pyenv global 3.11.9
```

Everything else (`deploy.sh`, cron lines, script invocations) is identical.

### Docker

Create `Dockerfile` in the project root:

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python scripts/setup_db.py

# Install cron job
RUN echo "0 0 * * * cd /app && python scripts/run_daily.py >> /app/logs/daily_\$(date +\\%Y\\%m\\%d).log 2>&1" | crontab -
RUN echo "0 6 * * 0 find /app/logs -name 'daily_*.log' -mtime +30 -delete" | crontab -a -u root -

VOLUME ["/app/data", "/app/logs"]

CMD ["cron", "-f"]
```

```bash
# Build and run
docker build -t ticker-tide .
docker run -d \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  --name ticker-tide \
  ticker-tide

# Verify
docker logs ticker-tide
docker exec ticker-tide python scripts/test_api_access.py
```

To run a manual backfill inside the container:

```bash
docker exec -it ticker-tide python scripts/run_backfill.py
```

---

## Telegram Bot

The interactive bot handles on-demand `/detail` commands. It runs as a **separate long-polling process** — independent from the daily pipeline cron job.

### Starting the Bot

```bash
tmux new -s bot
source .venv/bin/activate
python scripts/run_bot.py
# Ctrl+B, D to detach
```

### Available Commands

- `/detail AAPL` — deep analysis with 30-day chart (default)
- `/detail AAPL 90` — deep analysis with 90-day chart (max: 180 days)
- `/help` — list commands

### Running as a Service (Optional)

```bash
sudo tee /etc/systemd/system/ticker-tide-bot.service << 'SERVICE'
[Unit]
Description=Ticker Tide Telegram Bot
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/ticker-tide
ExecStart=/home/ec2-user/ticker-tide/.venv/bin/python scripts/run_bot.py
Restart=always
RestartSec=10
EnvironmentFile=/home/ec2-user/ticker-tide/.env

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl enable ticker-tide-bot
sudo systemctl start ticker-tide-bot
sudo systemctl status ticker-tide-bot
```

### Monitoring

```bash
# Check the bot process
sudo systemctl status ticker-tide-bot

# View logs
journalctl -u ticker-tide-bot -f

# Restart after a crash
sudo systemctl restart ticker-tide-bot
```

### Chart Cleanup

Charts are saved as temporary PNGs at `/tmp/ticker_tide_chart_{TICKER}_{timestamp}.png` and deleted automatically after each `/detail` command. If the process crashes mid-command, orphaned files can be cleaned with:

```bash
rm -f /tmp/ticker_tide_chart_*.png
```

### Configuration

Bot behaviour is controlled by `config/notifier.json` under the `detail_command` key:

| Key | Default | Description |
|---|---|---|
| `default_chart_days` | 30 | Days used when no day count provided |
| `max_chart_days` | 180 | Upper bound; larger values are clamped |
| `chart_style` | `nightclouds` | mplfinance dark style |
| `chart_figsize` | `[14, 10]` | Chart width × height in inches |
| `sr_levels_to_show` | 3 | Number of S/R levels drawn on chart |
| `signal_history_days` | 30 | Days of signal history shown in breakdown |
| `peer_count` | 5 | Max sector peers shown in breakdown |
