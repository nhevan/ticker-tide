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
| 2b — Calculator | `run_calculator(mode='incremental')` | 2–4 min (per ticker the calculator now also runs the weekly + monthly per-timeframe sub-pipelines: swing_points → S/R → patterns → divergences → crossovers → profiles, against `*_weekly` and `*_monthly` mirror tables; ETFs/benchmarks skip the sub-pipeline) |
| 3 — Scorer | `run_scorer()` | 1–3 min |
| 3b — Realized returns | `populate_realized_returns()` (called inline from `run_daily.py`) | <5s incremental path (only rows with NULL `realized_computed_at` and closed forward window); non-fatal — failure sends admin alert but does not block Phase 4 |
| 4 — Notifier | `run_notifier()` | 2–5 min (Claude API per qualifying ticker) |
| **Total** | | **~10–20 min** |

### One-time backfill after Migration 7

```bash
python scripts/backfill_realized_returns.py
# Expected duration: 30–90s for a full historical backfill (~100k rows)
# Add --force to recompute already-populated rows
# Add --dry-run to validate without writing
```

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

The interactive bot (`/detail`, `/scatter`, `/tickers`, `/help`) runs as a systemd service and is managed by `deploy.sh` — no manual startup needed.

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

Every push to `main` triggers the GitHub Actions deploy workflow, which:
1. SSHes into EC2 and runs `./deploy.sh` (installs/updates the systemd service, runs tests, etc.)
2. Runs a dedicated **"Restart Telegram bot"** step that explicitly calls `sudo systemctl restart ticker-tide-bot` and verifies the service is active — visible as its own step in the Actions UI.

`deploy.sh` also handles the service on **manual deploys**: it installs `deploy/ticker-tide-bot.service` to `/etc/systemd/system/`, runs `systemctl enable` + `systemctl restart`. The service:
- Auto-starts on EC2 reboot
- Restarts automatically within 5 seconds on any crash or clean exit
- Reads credentials directly from `.env` via `EnvironmentFile`

Never start `run_bot.py` manually in a tmux session on EC2 — the systemd service handles it.

---

## Web UI Service

The read-only web UI (`scripts/run_web.py`) runs as a systemd service on EC2, bound to `127.0.0.1:8765` behind Caddy (HTTPS reverse proxy). Single worker only — the in-memory LLM debounce assumes a single-worker process.

FastAPI is now a pure JSON API (`/api/*`). A Vite + React SPA (built by GitHub Actions `build-frontend` job) is served from `web/dist/` with a SPA catch-all. The frontend build is rsynced to EC2 as part of the CI `deploy` job before `deploy.sh` runs.

**Frontend build artifacts:** `web/dist/` is produced by the `build-frontend` CI job and rsynced to EC2 `web/dist_new/` then atomically renamed to `web/dist`. `deploy.sh` checks for `web/dist/index.html` at startup and aborts if missing.

### Status and control

```bash
# Check if the web UI is running
sudo systemctl status ticker-tide-web

# Stop / start / restart
sudo systemctl stop ticker-tide-web
sudo systemctl start ticker-tide-web
sudo systemctl restart ticker-tide-web

# File-based log
tail -f /home/ec2-user/ticker-tide/logs/web.log
```

### First-time deployment prerequisites

Before the first deploy, the EC2 `.env` must contain:

```
WEB_PASSWORD=<shared password>
WEB_SECRET_KEY=<long random string for cookie signing>
```

Generate a secret key with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(64))"
```

`deploy.sh` will refuse to proceed if either is missing (both are listed in `REQUIRED_KEYS`).

A Caddy block reverse-proxying `quant.nhevan.com` → `localhost:8765` must also exist before the first deploy — see "Caddy reverse proxy" below.

### How it is managed

Every push to `main` triggers the GitHub Actions deploy workflow (two-job sequence):
1. **`build-frontend`** — installs Node 20, runs `npm ci`, runs Vitest, runs `npm run build`, uploads `web/dist/` as artifact.
2. **`deploy`** — downloads the artifact, rsyncs `web/dist/` to EC2 `web/dist_new/`, atomically moves to `web/dist`, then SSHes into EC2 and runs `./deploy.sh`, and finishes by restarting both `ticker-tide-bot` and `ticker-tide-web`.

`deploy.sh` checks for `web/dist/index.html` and **aborts the deploy** if missing.

### Troubleshooting: 503 on all non-API routes

If all page loads return `{"detail": "Frontend not built."}`:
1. Check GitHub Actions — did the `build-frontend` job succeed?
2. Check that `web/dist/index.html` exists on EC2: `ls ~/ticker-tide/web/dist/`
3. Check the rsync step output in the `deploy` job for any errors.
4. If `dist/` is absent entirely, the `dist_new/` → `dist/` atomic rename may have failed — check disk space and permissions.

`deploy.sh` also handles the service on **manual deploys**: installs `deploy/ticker-tide-web.service` to `/etc/systemd/system/`, runs `systemctl enable` + `systemctl restart`. The service:
- Auto-starts on EC2 reboot
- Restarts automatically within 5 seconds on any crash or clean exit
- Reads credentials from `.env` via `EnvironmentFile` (requires `WEB_PASSWORD` and `WEB_SECRET_KEY`)
- Logs to `logs/web.log`

Never start `run_web.py` manually — the systemd service handles it.

### Updating scorer config (`config/scorer.json`)

`GET /api/scoring-rules` is **process-static**: it reads `config/scorer.json` at startup and returns the same response for the lifetime of the process. Changes to `scorer.json` (including RSI thresholds, regime weights, and `score_expansion_factor`) are not reflected until the web service is restarted:

```bash
sudo systemctl restart ticker-tide-web
```

If you change `scoring.score_expansion_factor` or RSI thresholds in `scorer.json` and want the stored contribution payloads (`scores_daily.key_signals_data`) to be consistent with the new config, also re-run the scorer before restarting the web service:

```bash
python scripts/run_scorer.py --force
sudo systemctl restart ticker-tide-web
```

### GET /api/shrinkage-path

Returns the ridge regression shrinkage path for the latest (or a specific) scoring date.
This endpoint is **not** process-static: it re-runs `compute_shrinkage_path` on the live
training window on every request, opening a fresh DB connection.

**Authentication required** (session cookie). Returns 401 without auth.

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `date` | string (optional) | ISO date YYYY-MM-DD. Defaults to latest `scores_daily` date. Returns 422 for invalid format. |

**Expected response time:** under 500 ms for typical training windows (30–300 rows, 50 lambda values). No SLA yet — contact the developer if queries consistently exceed 2 s.

**Cold-start behaviour:** returns `{"cold_start": true, ...}` (HTTP 200) when training samples are below `min_training_samples` (default 30) or when `scores_daily` is empty. No 5xx is emitted for cold-start.

To check if the endpoint is healthy after a deploy:

```bash
curl -s -b <session-cookie> https://quant.nhevan.com/api/shrinkage-path | python3 -m json.tool | head -10
```

### GET /api/price-chart

Returns OHLCV bars for the candlestick price chart on the Ticker Detail page.
This endpoint is **not** process-static: it queries `ohlcv_daily` on every request, opening a fresh DB connection.

**Authentication required** (session cookie). Returns 401 without auth.

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | string (required) | Ticker symbol. Unknown ticker returns 200 with `bars: []`. |
| `range` | string (required) | One of `1M`, `3M`, `6M`, `1Y`, `ALL`. Invalid value returns 422. |

**No new pipeline phase.** The endpoint reads `ohlcv_daily` which is populated by the existing daily fetcher. No schedule changes or backfill steps are needed.

**Range days** are controlled by `config/web.json price_chart.range_days`. Changes are picked up on the next request — no web service restart needed.

---

### Caddy reverse proxy

**Live URL:** https://quant.nhevan.com

The Caddy configuration for the web subdomain is **not stored in this repo**. The production block on EC2 is:

```caddy
quant.nhevan.com {
    encode zstd gzip
    reverse_proxy localhost:8765
}
```

Caddy auto-provisions HTTPS via Let's Encrypt (no explicit `tls` directive needed). Caddy's default `reverse_proxy` already forwards `X-Forwarded-*` headers, which Starlette's `SessionMiddleware` uses to set the `Secure` cookie correctly.

When restoring to a new EC2 instance or adding a new subdomain:
1. Add an `A` record at the DNS provider pointing the subdomain to the EC2 public IP.
2. Wait for propagation: `dig +short <subdomain>.nhevan.com` should return the EC2 IP.
3. Append the Caddy block above (substituting the subdomain) to `/etc/caddy/Caddyfile`.
4. Validate: `sudo caddy validate --config /etc/caddy/Caddyfile`.
5. Reload (zero-downtime): `sudo systemctl reload caddy`.
6. Tail the journal: `sudo journalctl -u caddy -f` — look for `certificate obtained successfully`.

Port 80 must be open in the EC2 security group for the HTTP-01 ACME challenge.

### New table: `web_login_attempts`

`create_all_tables()` now also creates a `web_login_attempts` table used for IP-based login rate limiting. The table is automatically created on first `deploy.sh` run (via `setup_db.py`). No migration script needed — `CREATE TABLE IF NOT EXISTS` is idempotent.

```sql
CREATE TABLE IF NOT EXISTS web_login_attempts (
    ip TEXT NOT NULL,
    attempted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_web_login_attempts_ip_time ON web_login_attempts(ip, attempted_at);
```

### Dev auto-reload

`scripts/run_web.py --reload` boots uvicorn in reload mode, watching `src/` and `config/`. The app is re-imported via `src.web.asgi:app` on every change. Production (`run_web.py` with no flag) still passes the constructed app instance directly. Do not use `--reload` in the systemd service — it forks subprocesses that conflict with the single-worker assumptions of the in-memory rate-limit and LLM debounce.

### New table: `dashboard_verdicts`

Caches the Claude-generated verdict shown above the three timeframe cards on the dashboard. One row per `(ticker, date)`; `POST /api/verdict` returns the cached row when present and only calls Claude on a miss.

```sql
CREATE TABLE IF NOT EXISTS dashboard_verdicts (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    verdict TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_dashboard_verdicts_ticker_date ON dashboard_verdicts(ticker, date);
```

To invalidate (force regeneration for a ticker/date), delete the row:

```sql
DELETE FROM dashboard_verdicts WHERE ticker = 'AAPL' AND date = '2026-04-25';
```

### Indicator score sidecar tables

Three sidecar tables (`indicator_scores_daily`, `indicator_scores_weekly`, `indicator_scores_monthly`) store the per-indicator signed scores (output of `score_all_indicators`) produced during every scorer run. The dashboard indicator-agreement matrix reads from these tables to show which indicators agreed or disagreed with the final signal direction for each timeframe.

**What populates them:**
- **Daily**: `persist_indicator_scores_daily` in `src/scorer/persistence.py`, called from `score_ticker` after `save_score_to_db`. Runs on every daily and historical scorer pass.
- **Weekly/Monthly**: written inside `persist_weekly_score_row` / `persist_monthly_score_row` using the same resolved `week_start` / `month_start` as the parent snapshot row.

**Partial-write caveat (daily):** The daily `save_score_to_db` and `persist_indicator_scores_daily` commit separately. A process interruption between the two leaves `scores_daily` populated but `indicator_scores_daily` empty for that ticker/date. Symptom: the matrix shows no indicator cells for an otherwise-valid signal. Remediation:

```bash
python scripts/run_scorer.py --force --ticker <SYMBOL>
```

Weekly and monthly sidecar writes are folded into their respective persistence helpers, but they also commit at the SQL level — the same caveat applies.

**One-time backfill on first deploy of this feature.** Run the full historical scorer to populate the sidecar tables across the full date range. Until backfill completes, the matrix shows empty cells for historical dates:

```bash
python scripts/run_scorer.py --historical --force
```

**Useful monitoring query:**

```sql
-- Count distinct ticker/date pairs with indicator scores (daily)
SELECT COUNT(DISTINCT ticker || date) FROM indicator_scores_daily;
```

Compare this against `SELECT COUNT(DISTINCT ticker || date) FROM scores_daily` to detect gaps.

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
| `migrate_news_articles_pk.py` | One-time migration: change `news_articles` PK from `id` to `(id, ticker)` | `(none)` |
| `migrate_add_calibration_columns.py` | One-time: add calibration columns to `scores_daily` | `--db-path PATH` |
| `migrate_add_monthly.py` | One-time: add `monthly_candles`, `indicators_monthly` tables and `monthly_score` column to `scores_daily` | `--db-path PATH` |
| `migrate_add_timeframe_parity.py` | One-time: add 14 weekly/monthly parity tables (swing_points, support_resistance, patterns, divergences, crossovers, indicator_profiles per timeframe + scores_weekly/scores_monthly) | `--db-path PATH` |
| `migrate_fix_scores_completeness_type.py` | One-time: fix `scores_weekly`/`scores_monthly`.`data_completeness` from `REAL` to `TEXT` (commit-1 schema bug). Idempotent — no-op when already TEXT. ABORTS if rows are present and type is REAL. **Must run before commit-6 persistence is exercised.** | `--db-path PATH` |
| `test_api_access.py` | Test all 5 API keys | `(none)` |
| `verify_backfill.py` | Post-backfill data quality checks | `--ticker AAPL`, `--quiet`, `--no-telegram`, `--db-path PATH` |
| `verify_pipeline.py` | Post-calculation computed data checks | `--date YYYY-MM-DD`, `--quiet`, `--no-telegram`, `--db-path PATH` |

`verify_pipeline.py` runs ~45 checks today, organised into sections:

- **Indicator** (`indicator_ranges`, `indicator_coverage`, `indicator_date_alignment`, `indicator_null_percentage`)
- **Score (daily)** (`score_ranges`, `category_score_ranges`, `confidence_range`, `signal_score_consistency`, `signal_distribution`, `confidence_distribution`, `weighted_score_math`, `regime_values`, `json_fields`)
- **Pattern / divergence / crossover (daily)** (`pattern_counts`, `pattern_duplicates`, `pattern_field_validity`, `divergence_counts`, `divergence_consistency`, `crossover_validity`)
- **Profile** (`profile_coverage`, `profile_percentile_order`, `profile_freshness`)
- **Weekly** (`weekly_candle_validity`, `weekly_indicator_coverage`, `weekly_pattern_count`, `weekly_divergence_count`, `weekly_crossover_count`, `scores_weekly_table_coverage`, `scores_weekly_score_range`, `scores_weekly_category_math`)
- **Monthly** (`monthly_candle_counts`, `monthly_score_column_coverage`, `monthly_indicator_coverage`, `monthly_pattern_count`, `monthly_divergence_count`, `monthly_crossover_count`, `scores_monthly_table_coverage`, `scores_monthly_score_range`, `scores_monthly_category_math`)
- **Period integrity** (`no_open_period_persisted` — gates `scores_weekly` / `scores_monthly` against `is_week_closed` / `is_month_closed`)
- **News + cross-table** (`news_summary_consistency`, `scores_have_indicators`, `indicators_have_ohlcv`, `sr_levels_within_range`)
- **Signal flips** (`signal_flip_validity`)

The category-math checks (`scores_weekly_category_math` / `scores_monthly_category_math`) re-derive the composite from per-category scores using both the v1 (`weekly_adaptive_weights`) and v2 (`weekly_adaptive_weights_v2`) weight sets and pass if **either** matches within `category_math_tolerance`. This dual-mode tolerance lets a `weekly_score_method` flip from v1 to v2 land mid-history without retroactively failing every old row.

Note: `monthly_score_column_coverage` (renamed from `monthly_score_coverage` in commit 8) inspects the `scores_daily.monthly_score` *column*; the standalone `scores_monthly` *table* is separately covered by `scores_monthly_table_coverage`.

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

### Database Schema Migrations

Some schema changes (e.g., modifying a PRIMARY KEY) cannot be applied with `ALTER TABLE` in SQLite and require a full table recreation.

**`migrate_news_articles_pk.py`** — Changes `news_articles` PRIMARY KEY from `id TEXT` to composite `(id, ticker)`, so the same Polygon article can be stored once per ticker that mentions it. This fixes `news_summary_consistency` warnings caused by `INSERT OR REPLACE` overwriting one ticker's article row when another ticker's fetch returned the same article ID.

**Run order (deploy then migrate):**

```bash
# 1. Run the migration (safe to re-run — skips if already on new schema)
python scripts/migrate_news_articles_pk.py

# 2. Re-backfill news so each ticker gets its own rows for shared articles
python scripts/run_backfill.py --phase news --force

# 3. Recompute news_daily_summary article counts
python scripts/run_calculator.py --force

# 4. Verify the warning is gone
python scripts/verify_pipeline.py
```

**`migrate_add_calibration_columns.py`** — Adds three columns to `scores_daily`: `calibrated_score REAL`, `raw_composite_score REAL`, `model_r2 REAL`. Uses `ALTER TABLE ADD COLUMN` (safe, no data loss). Idempotent — skips columns that already exist.

```bash
# 1. Run the migration
python scripts/migrate_add_calibration_columns.py

# 2. Re-run scorer to populate the new columns
python scripts/run_scorer.py --force

# 3. Verify
python scripts/verify_pipeline.py
```

**`migrate_add_monthly.py`** — Adds `monthly_candles` and `indicators_monthly` tables, and `monthly_score REAL` column to `scores_daily`. Uses `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ADD COLUMN`. Safe to run multiple times — skips structures that already exist.

```bash
# 1. Run the migration (safe to re-run)
python scripts/migrate_add_monthly.py

# 2. Re-run calculator to populate monthly tables
python scripts/run_calculator.py --mode full

# 3. Re-run scorer to populate monthly_score
python scripts/run_scorer.py --historical --force

# 4. Verify
python scripts/verify_pipeline.py
```

**`migrate_add_timeframe_parity.py`** — Adds 14 weekly/monthly parity tables that mirror the existing daily structures: `swing_points_weekly|monthly`, `support_resistance_weekly|monthly`, `patterns_weekly|monthly`, `divergences_weekly|monthly`, `crossovers_weekly|monthly`, `indicator_profiles_weekly|monthly`, plus two new score snapshot tables `scores_weekly` (PK `(ticker, week_start)`) and `scores_monthly` (PK `(ticker, month_start)`). Each new table also gets an `idx_<table>_ticker_<datecol>` index. Uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` throughout — fully idempotent.

```bash
# 1. Run the migration (safe to re-run)
python scripts/migrate_add_timeframe_parity.py

# 2. Re-run calculator + scorer once the corresponding write paths land
#    (subsequent commits in the parity series populate these tables).
#    From commit 4 onward, `run_calculator.py --mode full` also populates the
#    six event/profile tables for each timeframe: swing_points_weekly /
#    support_resistance_weekly / patterns_weekly / divergences_weekly /
#    crossovers_weekly / indicator_profiles_weekly (plus the monthly
#    counterparts). ETFs and benchmarks remain restricted to candles +
#    indicators on every timeframe.
python scripts/run_calculator.py --mode full
python scripts/run_scorer.py --historical --force

# 3. Verify
python scripts/verify_pipeline.py
```

**`migrate_fix_scores_completeness_type.py`** — Corrects a column-type bug introduced by commit 1's parity migration. The `scores_weekly` + `scores_monthly` tables were created with `data_completeness REAL`, but the scorer writes `json.dumps(...)` (a string) to that column — REAL would silently coerce to NULL. This migration drops + recreates each affected table when its column type is REAL. Tables must be empty (commit 6 is the first writer); the migration ABORTS rather than silently destroying data if it finds any rows in a wrong-type table.

```bash
# 1. Run the migration BEFORE deploying commit 6's scorer changes.
#    Idempotent — re-runs are no-ops once the column is TEXT.
python scripts/migrate_fix_scores_completeness_type.py

# 2. Verify both columns are TEXT
sqlite3 data/signals.db "PRAGMA table_info(scores_weekly);"   | grep data_completeness
sqlite3 data/signals.db "PRAGMA table_info(scores_monthly);"  | grep data_completeness
```

### Historical scoring modes (commit 6 expansion)

`run_scorer.py --historical --mode <MODE>` accepts five values:

| Mode | What it walks |
|---|---|
| `daily` | Trading dates in the last `daily_lookback_months`. |
| `weekly` | `week_start` rows from `weekly_candles` between `daily_lookback_months..weekly_lookback_months`. |
| `monthly` | `month_start` rows from `monthly_candles` between `daily_lookback_months..monthly_lookback_months`. **NEW in commit 6.** |
| `all` | `daily` + `weekly` + `monthly` together. **NEW in commit 6** — explicit, unambiguous. |
| `both` | Back-compat alias. **Now also includes monthly** — this is a deliberate semantic expansion in commit 6 so existing callers automatically get monthly coverage. Identical to `all` going forward. |

> **Sparse monthly coverage.** When a `month_start` lands on a non-trading day
> (e.g. 2026-01-01 holiday), `score_ticker(scoring_date=month_start)` returns
> None silently because no daily indicator row exists. This is an accepted
> sparseness in the monthly historical backfill — not every `month_start` is
> guaranteed to produce a `scores_monthly` row.

**Known re-run-to-fix failure mode.** The save functions used by the calculator/scorer commit a `DELETE FROM <table> WHERE ticker = ?` before the per-row `INSERT` loop. If the process is killed between the delete and the inserts (kill -9, OOM, sudden shutdown), the table for that ticker is left empty until the next run. Re-running the affected phase with `--force` regenerates the rows; no data is lost beyond what the calculator/scorer can recompute from raw OHLCV.

---

### Flipping weekly_score_method: required sequence

Flipping `weekly_score_method` from `v1_4cat` to `v2_8cat` (or any future scoring-method change) shifts the meaning of `weekly_score` / `monthly_score` and therefore the calibrator's input distribution. The acceptance gate validates the shift is bounded.

```bash
# 1. Snapshot baseline calibrator distribution (use today's most recent calibrated scoring date)
python scripts/check_calibrator_acceptance.py snapshot \
    --scoring-date YYYY-MM-DD --output baselines/pre-v2.json

# 1b. Snapshot 5 sample LLM blurbs BEFORE the scorer re-run wipes the v1 weekly_score values.
#     ai_reasoner.py:803 includes weekly_score in the LLM prompt, so v2 semantics will subtly
#     shift blurb tone/framing. Capture pre-flip outputs now — once step 3 runs there is no
#     way to reproduce the v1-prompt-input blurbs.
python scripts/run_notifier.py --dry-run --tickers AAPL,MSFT,NVDA,GOOG,META \
    > baselines/pre-v2-blurbs.txt

# 2. Edit config/scorer.json: flip weekly_score_method (and monthly_score_method) to "v2_8cat"

# 3. Regenerate scores with v2 semantics across the calibrator's 365-day window
python scripts/run_scorer.py --historical --force

# 4. Wait for the historical run to fully complete before snapping post.

# 5. Snapshot post calibrator distribution (same scoring date as step 1)
python scripts/check_calibrator_acceptance.py snapshot \
    --scoring-date YYYY-MM-DD --output baselines/post-v2.json

# 5b. Snapshot 5 sample LLM blurbs post-flip and manually diff against baselines/pre-v2-blurbs.txt.
#     Look for tone shifts, contradictions, or framing changes driven by the new weekly_score values.
python scripts/run_notifier.py --dry-run --tickers AAPL,MSFT,NVDA,GOOG,META \
    > baselines/post-v2-blurbs.txt
diff baselines/pre-v2-blurbs.txt baselines/post-v2-blurbs.txt

# 6. Run the gate
python scripts/check_calibrator_acceptance.py check --baseline baselines/pre-v2.json
```

Exit codes:
- `0` PASS (or PASS-with-WARNING — investigate but did not block)
- `1` FAIL — revert `weekly_score_method` to `v1_4cat`, re-run `scripts/run_scorer.py --historical --force`, document the Telegram message.
- `2` INSUFFICIENT_DATA — baseline date has no rows in current DB, or sample size below `min_sample_size`.

Mixed-semantics caveat: even on PASS, the calibrator's 365-day training window contains rows scored before the historical re-run completed. The first PASS is partially optimistic. Re-run the gate ~365 days post-flip with a fresh baseline to confirm stability (tracked in `hot.md` Next Up).

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
| `notifier_done` shows `failed` status | DB error during ticker load (e.g. transient lock) | Check logs for the propagated `sqlite3` exception; re-run with `python scripts/run_notifier.py --force`. Claude API errors are absorbed inside `call_claude` and never surface as `failed` here. |
| Scorer shows `partial` status for a ticker | One calculator module failed for that ticker | Check `alerts_log` for the specific error; re-run `python scripts/run_calculator.py --mode full --ticker <TICKER>` |

---

## Adding / Removing Tickers

### Adding a ticker

```bash
# 1. Edit config/tickers.json — add entry:
#    { "symbol": "NVDA", "sector": "Technology", "sector_etf": "XLK",
#      "added": "2026-03-18", "active": true }
#    For renamed tickers add: "former_symbol": "FB", "symbol_since": "2022-06-09"
#
#    For index ETFs (QQQ, VOO, DIA, etc.) use sector: "Index" and sector_etf: null.
#    Fundamentals/earnings/filings will be empty (expected); all price-based indicators work.
#    { "symbol": "VOO", "sector": "Index", "sector_etf": null,
#      "added": "2026-04-13", "active": true }

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

The interactive bot handles on-demand commands. It runs as a **separate long-polling process** — independent from the daily pipeline cron job.

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
- `/scatter 10` — confidence vs 10-day forward return scatter plot, all tickers, last 90 days
- `/scatter 5 AAPL` — confidence vs 5-day return for AAPL only
- `/scatter 20 AAPL 180` — 20-day return, AAPL, last 180 days of signals
- `/tickers` — list all watched tickers by sector
- `/why AAPL` — top-5 verbose math walkthrough of every contribution to the latest signal
- `/why AAPL all` — ranked table of all contributions (capped at 50)
- `/why AAPL rsi_14` — drill-down for a specific indicator or pattern by its canonical lowercase key
- `/help` — list commands

The raw-data breakdown message sent by `/detail` also includes an inline "🔍 Why this signal?" button; tapping it is equivalent to sending `/why AAPL` for that ticker.

### `/why` Deploy and Smoke Test

**Deploying the `/why` handlers** (iterative push between full `deploy.sh` runs):

```bash
git pull
sudo systemctl restart ticker-tide-bot
sudo systemctl status ticker-tide-bot   # confirm Active: running
```

The systemd service picks up the new `CommandHandler("why", ...)` and `CallbackQueryHandler(pattern="^why:")` registered in `src/notifier/bot.py` on restart. No config changes are required.

**Schema migration runs automatically.** `run_migrations(conn)` is called from `run_bot.py` at startup (and from `run_scorer.py` / `run_daily.py`), so the `scores_daily.key_signals_data` column is added to existing databases on the first restart after deploy. No manual `ALTER TABLE` is needed.

**Data backfill — required before `/why` returns useful output.** `run_migrations` only adds the column; it does not populate it. Existing rows have `key_signals_data = NULL` until the scorer writes a fresh row. Until then, every `/why TICKER` returns the null-data sentinel reply (`"Signal data for TICKER is unavailable or malformed."`).

Two ways to populate:

1. **Wait for the next daily cron run (00:00 UTC)** — the scheduled `run_scorer.py` writes today's row with `key_signals_data` for every active ticker. `/why` works immediately after that.
2. **Backfill manually now** (recommended after iterative pushes mid-day):

   ```bash
   cd /home/ec2-user/ticker-tide
   python3 scripts/run_scorer.py --force
   ```

   Re-scores every active ticker for today's date. Takes ~20–30 seconds. After this completes, `/why` returns the verbose math walkthrough as expected.

`--force` only re-scores the latest date. Historical rows (yesterday and earlier) keep their NULL `key_signals_data`. The `/why` feature is scoped to the latest signal only, so this is intentional — historical drill-down is not a supported mode.

**Behavioral note — pre-deploy `/detail` messages will not have the button.** Telegram does not retroactively patch inline keyboards on already-sent messages. Only `/detail` responses generated after this deploy will include the "🔍 Why this signal?" button. Older messages remain unchanged.

**Smoke test steps after deploy:**

1. `/why AAPL` → should return a verbose top-5 walkthrough (not an error).
2. `/why AAPL all` → should return a ranked table of all contributions.
3. `/why AAPL rsi_14` → should return a drill-down for the RSI-14 indicator. Use the canonical lowercase key (e.g. `rsi_14`, not `rsi`) to avoid an ambiguous-match reply.
4. `/why` (no ticker) → should return a usage hint, not a crash.
5. `/detail AAPL` → tap the "🔍 Why this signal?" inline button → response should be identical to step 1.

### `/scatter` Details

Plots historical signal confidence (X-axis) vs the actual % price change N trading days
after each signal (Y-axis), colored by signal type (green=BULLISH, red=BEARISH, gray=NEUTRAL).
BEARISH returns are inverted so a correct bearish call (price drop) shows as a positive value.
Signals without N future days of OHLCV data are automatically excluded.
A linear regression line is drawn per signal type.

Config keys in `config/notifier.json` under `scatter_command`:
- `default_n_days` (default 5) — forward horizon when N is omitted
- `max_n_days` (default 60) — cap on N
- `default_days_back` (default 90) — signal history window in calendar days
- `max_days_back` (default 365) — cap on days_back

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

Charts are saved as temporary PNGs and deleted automatically after each command.
- `/detail` charts: `/tmp/ticker_tide_chart_{TICKER}_{timestamp}.png`
- `/scatter` charts: `/tmp/scatter_{random}.png`

If the process crashes mid-command, orphaned files can be cleaned with:

```bash
rm -f /tmp/ticker_tide_chart_*.png /tmp/scatter_*.png
```

### Configuration

Bot behaviour is controlled by `config/notifier.json`.

**`detail_command` keys:**

| Key | Default | Description |
|---|---|---|
| `default_chart_days` | 30 | Days used when no day count provided |
| `max_chart_days` | 180 | Upper bound; larger values are clamped |
| `chart_style` | `nightclouds` | mplfinance dark style |
| `chart_figsize` | `[14, 10]` | Chart width × height in inches |
| `sr_levels_to_show` | 3 | Number of S/R levels drawn on chart |
| `signal_history_days` | 30 | Days of signal history shown in breakdown |
| `peer_count` | 5 | Max sector peers shown in breakdown |

**`scatter_command` keys:**

| Key | Default | Description |
|---|---|---|
| `default_n_days` | 5 | Default forward horizon in trading days |
| `max_n_days` | 60 | Upper bound on N; larger values are clamped |
| `default_days_back` | 90 | Default signal history window in calendar days |
| `max_days_back` | 365 | Upper bound on days_back |
