# CONFIG.md — Configuration Reference

All config files live in `config/`. All thresholds, periods, and URLs are read from these files — nothing is hardcoded in `src/`.

---

## Environment Variables (`.env`)

| Variable | Required | Where to get it |
|---|---|---|
| `POLYGON_API_KEY` | Yes | [polygon.io](https://polygon.io) → Dashboard → API Keys |
| `FINNHUB_API_KEY` | Yes | [finnhub.io](https://finnhub.io) → Dashboard → API Key |
| `ANTHROPIC_API_KEY` | Yes | [console.anthropic.com](https://console.anthropic.com) → API Keys |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram → @BotFather → `/newbot` |
| `TELEGRAM_ADMIN_CHAT_ID` | Yes | Admin chat ID — receives heartbeats, error alerts, and progress updates. Send a message to your bot, then call `https://api.telegram.org/bot<TOKEN>/getUpdates` and read `message.chat.id` |
| `TELEGRAM_SUBSCRIBER_CHAT_IDS` | No | Comma-separated list of chat IDs that receive the daily signal report and market-closed notifications (e.g. `"111,222,333"`). If omitted, only the admin receives the signal report. |
| `TELEGRAM_SUBSCRIBER_TICKERS` | No | Per-subscriber ticker watchlist. Format: `chat_id1:AAPL,MSFT;chat_id2:NVDA,AMD`. Subscribers listed here receive a daily report filtered to their watched tickers only. Subscribers absent from this variable receive the full report. Ticker symbols are case-insensitive. The `/detail TICKER` command is unaffected — subscribers can still look up any ticker. |

**Backward compatibility:** `TELEGRAM_CHAT_ID` is still accepted as a fallback for `TELEGRAM_ADMIN_CHAT_ID`. Existing `.env` files without the new variables continue to work — the admin becomes the only subscriber.

Template: `.env.example`

---

## config/tickers.json

Ticker universe. The `tickers` array is the only required field; `sector_etfs` and `market_benchmarks` are read by the scorer and fetcher for sector/benchmark comparisons.

**Index ETFs** (QQQ, VOO, DIA) are tracked as regular tickers with `sector: "Index"` and `sector_etf: null`. They receive full pipeline treatment (indicators, patterns, scoring, signals) but will not have fundamentals, earnings, or filing data — the scorer applies a -3% confidence penalty for the missing fundamentals and returns a raw score (no sector adjustment) when `sector_etf` is null.

### `tickers[]` object fields

| Key | Type | Required | Description |
|---|---|---|---|
| `symbol` | string | Yes | Ticker symbol as traded on Nasdaq/NYSE |
| `sector` | string | Yes | GICS sector name |
| `sector_etf` | string or null | Yes | SPDR sector ETF symbol (must be listed in `sector_etfs`). Set to `null` for index ETFs (QQQ, VOO, DIA) — the sector adjustment is skipped when this is null. |
| `added` | string | Yes | Date added to config (`YYYY-MM-DD`); preserved on re-insert |
| `active` | boolean | Yes | `false` disables the ticker without deleting its data |
| `former_symbol` | string | No | Pre-rename ticker (e.g. `"FB"`); triggers split-fetch in OHLCV backfiller |
| `symbol_since` | string | No | Date of rename (`YYYY-MM-DD`); required when `former_symbol` is set |

### `sector_etfs`

Array of SPDR sector ETF symbols. Must include every ETF referenced in `tickers[].sector_etf`. Indicators and weekly candles are computed for all ETFs listed here.

### `market_benchmarks`

| Key | Type | Default | Description |
|---|---|---|---|
| `market_benchmarks.spy` | string | `"SPY"` | S&P 500 proxy (Polygon does not provide `/I:SPX` on free tier) |
| `market_benchmarks.qqq` | string | `"QQQ"` | Nasdaq-100 proxy |
| `market_benchmarks.vix` | string | `"^VIX"` | VIX ticker (fetched via yfinance) |

---

## config/backfiller.json

Controls the one-time historical data load (`scripts/run_backfill.py`).

| Key | Type | Default | Description |
|---|---|---|---|
| `skip_if_fresh_days.earnings` | int | `7` | Re-fetch earnings only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.fundamentals` | int | `30` | Re-fetch fundamentals only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.dividends` | int | `7` | Re-fetch dividends only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.splits` | int | `30` | Re-fetch splits only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.short_interest` | int | `7` | Re-fetch short interest only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.news` | int | `1` | Re-fetch news only if last fetch was ≥ N days ago |
| `skip_if_fresh_days.filings` | int | `7` | Re-fetch 8-K filings only if last fetch was ≥ N days ago |
| `ohlcv.lookback_years` | int | `5` | Years of daily OHLCV history to backfill |
| `ohlcv.adjusted` | boolean | `true` | Use split/dividend-adjusted prices |
| `ohlcv.batch_size` | int | `50000` | Max bars per Polygon API request |
| `news.lookback_months` | int | `3` | Months of news to backfill from Polygon |
| `news.polygon_limit_per_request` | int | `1000` | Max articles per Polygon request (follows `next_url` pagination) |
| `news.finnhub_lookback_months` | int | `1` | Months of news to backfill from Finnhub |
| `fundamentals.lookback_years` | int | `5` | Years of quarterly financials to load from yfinance |
| `fundamentals.periods` | array | `["quarterly","annual"]` | yfinance periods to fetch |
| `filings.form_types` | array | `["8-K"]` | SEC filing types to backfill |
| `filings.lookback_months` | int | `6` | Months of filings to backfill |
| `macro.treasury_lookback_years` | int | `5` | Years of treasury yield history to backfill |
| `earnings.lookback_years` | int | `2` | Passed to yfinance `get_earnings_dates` limit calculation |
| `rate_limit.polygon_rate_limited` | boolean | `false` | Set `true` to add delays between Polygon calls (Starter tier has no rate limit) |
| `rate_limit.finnhub_calls_per_minute` | int | `60` | Finnhub free-tier limit |
| `rate_limit.finnhub_delay_seconds` | float | `1.0` | Seconds to sleep between Finnhub calls |

---

## config/fetcher.json

Controls the daily fetch phase (`scripts/run_daily.py` → `run_daily_fetch()`).

| Key | Type | Default | Description |
|---|---|---|---|
| `schedule.daily_run_utc` | string | `"00:00"` | Expected run time (informational only; actual scheduling is via cron) |
| `schedule.skip_market_holidays` | boolean | `true` | Skip fetch on NYSE holidays |
| `schedule.skip_weekends` | boolean | `true` | Skip fetch on Saturdays and Sundays |
| `polling_intervals.fundamentals_days` | int | `14` | Re-fetch fundamentals only if last fetch was ≥ N days ago |
| `polling_intervals.earnings_calendar_days` | int | `7` | Re-fetch earnings calendar only if last fetch was ≥ N days ago |
| `polling_intervals.short_interest_days` | int | `15` | Re-fetch short interest only if last fetch was ≥ N days ago |
| `rate_limit.polygon_rate_limited` | boolean | `false` | Same as backfiller — set `true` to throttle Polygon calls |
| `rate_limit.finnhub_calls_per_minute` | int | `60` | Finnhub free-tier limit |
| `rate_limit.finnhub_delay_seconds` | float | `1.0` | Seconds to sleep between Finnhub calls |

---

## config/calculator.json

Controls all indicator periods, pattern detection, and profile computation.

### Indicators

| Key | Type | Default | Description |
|---|---|---|---|
| `indicators.ema_periods` | array | `[9, 21, 50]` | EMA lookback periods computed for `ema_9`, `ema_21`, `ema_50` |
| `indicators.macd.fast` | int | `12` | MACD fast EMA period |
| `indicators.macd.slow` | int | `26` | MACD slow EMA period |
| `indicators.macd.signal` | int | `9` | MACD signal line period |
| `indicators.adx_period` | int | `14` | ADX and directional indicator period |
| `indicators.rsi_period` | int | `14` | RSI lookback period |
| `indicators.stochastic.k` | int | `14` | Stochastic %K period |
| `indicators.stochastic.d` | int | `3` | Stochastic %D smoothing period |
| `indicators.stochastic.smooth_k` | int | `3` | Stochastic %K smoothing |
| `indicators.cci_period` | int | `20` | CCI period |
| `indicators.williams_r_period` | int | `14` | Williams %R period |
| `indicators.bollinger.period` | int | `20` | Bollinger Bands SMA period |
| `indicators.bollinger.std_dev` | int | `2` | Bollinger Bands standard deviation multiplier |
| `indicators.atr_period` | int | `14` | ATR period |
| `indicators.keltner_period` | int | `20` | Keltner Channel EMA period |
| `indicators.cmf_period` | int | `20` | Chaikin Money Flow period |

### Swing points and support/resistance

| Key | Type | Default | Description |
|---|---|---|---|
| `swing_points.lookback_candles` | int | `5` | A swing high/low requires N candles dominant on both sides |
| `support_resistance.price_tolerance_pct` | float | `1.5` | Max % difference to cluster swing points into one S/R level |
| `support_resistance.min_touches` | int | `2` | Minimum touches for a valid S/R level |
| `support_resistance.lookback_days` | int | `120` | Days of swing points to consider when building S/R levels |

### Patterns

| Key | Type | Default | Description |
|---|---|---|---|
| `patterns.double_top_bottom.price_tolerance_pct` | float | `1.5` | Max % difference between the two peaks/troughs |
| `patterns.double_top_bottom.min_days_between` | int | `10` | Minimum trading days between the two peaks/troughs |
| `patterns.double_top_bottom.max_days_between` | int | `60` | Maximum trading days between the two peaks/troughs |
| `patterns.flag.pole_min_atr_multiple` | float | `2.0` | Pole must move at least N × ATR |
| `patterns.flag.pole_max_days` | int | `10` | Maximum candles in the pole |
| `patterns.flag.flag_min_days` | int | `5` | Minimum candles in the consolidation |
| `patterns.flag.flag_max_days` | int | `15` | Maximum candles in the consolidation |
| `patterns.flag.flag_retracement_min_pct` | float | `20` | Minimum retracement % during consolidation |
| `patterns.flag.flag_retracement_max_pct` | float | `50` | Maximum retracement % during consolidation |
| `patterns.breakout_volume_threshold` | float | `1.5` | Volume must be N × 20-day average to confirm a breakout/breakdown |

### Divergences

| Key | Type | Default | Description |
|---|---|---|---|
| `divergences.indicators` | array | `["rsi","macd_histogram","obv","stochastic"]` | Indicators to check for divergence |
| `divergences.min_swing_distance_days` | int | `5` | Minimum days between swing points for a valid divergence pair |
| `divergences.max_swing_distance_days` | int | `60` | Maximum days between swing points for a valid divergence pair |

### Gaps

| Key | Type | Default | Description |
|---|---|---|---|
| `gaps.volume_breakaway_threshold` | float | `2.0` | Volume must be N × average to classify a gap as Breakaway |
| `gaps.volume_average_period` | int | `20` | Lookback period for average volume calculation |

### Fibonacci

| Key | Type | Default | Description |
|---|---|---|---|
| `fibonacci.levels` | array | `[0.236, 0.382, 0.5, 0.618, 0.786]` | Retracement ratios to compute |
| `fibonacci.proximity_pct` | float | `1.0` | Max % distance from a Fibonacci level to be considered "at the level" |

### Relative strength

| Key | Type | Default | Description |
|---|---|---|---|
| `relative_strength.period_days` | int | `20` | Lookback period for RS_market and RS_sector computation |

### Indicator profiles

| Key | Type | Default | Description |
|---|---|---|---|
| `profiles.rolling_window_days` | int | `504` | Days of history used to compute percentile profiles (~2 years) |
| `profiles.recompute_frequency` | string | `"weekly"` | How often to recompute profiles in incremental mode |
| `profiles.blend_alpha_max` | float | `0.85` | Maximum weight of the stock's own profile vs. sector profile |
| `profiles.blend_alpha_denominator` | int | `756` | Days used in blend formula: `α = min(blend_alpha_max, days/756)` |

### Weekly candles

| Key | Type | Default | Description |
|---|---|---|---|
| `weekly.week_start_day` | string | `"Monday"` | Day each weekly candle opens |

---

## config/scorer.json

Controls regime detection, adaptive weights, signal thresholds, and confidence modifiers.

### Regime detection

| Key | Type | Default | Description |
|---|---|---|---|
| `regime_detection.adx_trending_threshold` | int | `25` | ADX ≥ this → Trending regime |
| `regime_detection.adx_ranging_threshold` | int | `20` | ADX < this → Ranging regime |
| `regime_detection.atr_volatile_multiplier` | float | `1.5` | ATR > N × 20-day ATR SMA → Volatile regime |
| `regime_detection.atr_volatile_lookback` | int | `20` | Lookback period for ATR SMA in volatile detection |
| `regime_detection.vix_volatile_threshold` | int | `25` | VIX ≥ this also triggers Volatile regime |
| `regime_detection.ema_trend_override` | bool | `true` | When enabled, a fully aligned EMA stack (close > EMA9 > EMA21 > EMA50 or reverse) overrides regime to Trending even if ADX is below the trending threshold. Prevents mean-reversion oscillator interpretation on stocks in clear directional trends with low ADX |

### Adaptive category weights

Weights for each of 9 categories per regime. All 9 weights in a regime must sum to 1.0.

Categories: `trend`, `momentum`, `volume`, `volatility`, `candlestick`, `structural`, `sentiment`, `fundamental`, `macro`.

| Key | Trending | Ranging | Volatile |
|---|---|---|---|
| `adaptive_weights.{regime}.trend` | `0.30` | `0.10` | `0.20` |
| `adaptive_weights.{regime}.momentum` | `0.15` | `0.25` | `0.15` |
| `adaptive_weights.{regime}.volume` | `0.10` | `0.10` | `0.10` |
| `adaptive_weights.{regime}.volatility` | `0.05` | `0.10` | `0.15` |
| `adaptive_weights.{regime}.candlestick` | `0.05` | `0.10` | `0.10` |
| `adaptive_weights.{regime}.structural` | `0.15` | `0.15` | `0.10` |
| `adaptive_weights.{regime}.sentiment` | `0.10` | `0.10` | `0.10` |
| `adaptive_weights.{regime}.fundamental` | `0.05` | `0.05` | `0.05` |
| `adaptive_weights.{regime}.macro` | `0.05` | `0.05` | `0.05` |

### Sector adjustment

| Key | Type | Default | Description |
|---|---|---|---|
| `sector_adjustment.bullish_sector_threshold` | int | `30` | Sector ETF score ≥ this adds a bullish adjustment |
| `sector_adjustment.bearish_sector_threshold` | int | `-30` | Sector ETF score ≤ this adds a bearish adjustment |
| `sector_adjustment.max_adjustment` | int | `10` | Maximum points added or subtracted by sector adjustment |

### Timeframe weights

| Key | Type | Default | Description |
|---|---|---|---|
| `timeframe_weights.daily` | float | `0.2` | Weight of daily score in final merged score |
| `timeframe_weights.weekly` | float | `0.8` | Weight of weekly score in final merged score |

### Signal thresholds

| Key | Type | Default | Description |
|---|---|---|---|
| `signal_thresholds.bullish` | int | `20` | `final_score ≥` this → BULLISH |
| `signal_thresholds.bearish` | int | `-20` | `final_score ≤` this → BEARISH |

### Confidence modifiers

Applied to the base confidence value (`|final_score|`). Final confidence is clamped to [0, 100].

| Key | Type | Default | Description |
|---|---|---|---|
| `confidence_modifiers.timeframe_agree` | int | `+10` | Daily and weekly both in same direction |
| `confidence_modifiers.timeframe_disagree` | int | `-15` | Daily and weekly in opposite directions |
| `confidence_modifiers.volume_confirms` | int | `+10` | Volume category score agrees with signal direction |
| `confidence_modifiers.volume_diverges` | int | `-10` | Volume category score opposes signal direction |
| `confidence_modifiers.indicator_consensus` | int | `+5` | > 60% of indicators agree with signal direction |
| `confidence_modifiers.indicator_mixed` | int | `-10` | < 50% of indicators agree with signal direction |
| `confidence_modifiers.earnings_within_days` | int | `7` | Window in days that triggers earnings penalty |
| `confidence_modifiers.earnings_penalty` | int | `-15` | Applied when next earnings is within `earnings_within_days` |
| `confidence_modifiers.vix_extreme_threshold` | int | `30` | VIX ≥ this triggers VIX penalty |
| `confidence_modifiers.vix_extreme_penalty` | int | `-10` | Applied when VIX ≥ `vix_extreme_threshold` |
| `confidence_modifiers.atr_expanding_penalty` | int | `-5` | Applied when ATR > 1.5 × its 20-day SMA |
| `confidence_modifiers.missing_news_penalty` | int | `-5` | Applied when no news data is available for the ticker |
| `confidence_modifiers.missing_fundamentals_penalty` | int | `-3` | Applied when no fundamentals data is available |

### Historical scoring

| Key | Type | Default | Description |
|---|---|---|---|
| `historical_scoring.daily_lookback_months` | int | `12` | Months of daily scores computed in `--historical` mode |
| `historical_scoring.weekly_lookback_months` | int | `60` | Months of weekly scores computed in `--historical` mode (months 13–60) |

### Scoring

| Key | Type | Default | Description |
|---|---|---|---|
| `scoring.score_expansion_factor` | float | `1.5` | Multiplier applied to spread indicator scores before category aggregation (used by both daily and weekly pipelines) |

### Weekly adaptive weights

Regime-specific weights for the 4 indicator-based categories used in weekly scoring.
Each regime's weights must sum to 1.0. Weekly scoring only uses trend, momentum, volume,
and volatility (patterns, sentiment, fundamental, macro have no weekly data sources).
If this section is missing, daily `adaptive_weights` are re-normalized to these 4 categories.

| Key | Type | Default | Description |
|---|---|---|---|
| `weekly_adaptive_weights.trending.trend` | float | `0.45` | Trend category weight in trending regime |
| `weekly_adaptive_weights.trending.momentum` | float | `0.25` | Momentum category weight in trending regime |
| `weekly_adaptive_weights.trending.volume` | float | `0.15` | Volume category weight in trending regime |
| `weekly_adaptive_weights.trending.volatility` | float | `0.15` | Volatility category weight in trending regime |
| `weekly_adaptive_weights.ranging.trend` | float | `0.20` | Trend category weight in ranging regime |
| `weekly_adaptive_weights.ranging.momentum` | float | `0.40` | Momentum category weight in ranging regime |
| `weekly_adaptive_weights.ranging.volume` | float | `0.20` | Volume category weight in ranging regime |
| `weekly_adaptive_weights.ranging.volatility` | float | `0.20` | Volatility category weight in ranging regime |
| `weekly_adaptive_weights.volatile.trend` | float | `0.30` | Trend category weight in volatile regime |
| `weekly_adaptive_weights.volatile.momentum` | float | `0.25` | Momentum category weight in volatile regime |
| `weekly_adaptive_weights.volatile.volume` | float | `0.15` | Volume category weight in volatile regime |
| `weekly_adaptive_weights.volatile.volatility` | float | `0.30` | Volatility category weight in volatile regime |

---

## config/notifier.json

| Key | Type | Default | Description |
|---|---|---|---|
| `ai_reasoner.model` | string | `"claude-sonnet-4-20250514"` | Anthropic model ID |
| `ai_reasoner.max_tokens` | int | `4096` | Max tokens per Claude response |
| `ai_reasoner.temperature` | float | `0.3` | Sampling temperature (lower = more deterministic) |
| `sentiment_enrichment.enabled` | boolean | `true` | Master switch — set `false` to skip Finnhub sentiment enrichment entirely |
| `sentiment_enrichment.model` | string | `"claude-haiku-4-5-20251001"` | Anthropic model used for classification (Haiku — cheapest, fastest) |
| `sentiment_enrichment.max_tokens` | int | `512` | Max tokens per Claude response (20 articles × ~12 tokens/line; 512 gives safe headroom) |
| `sentiment_enrichment.temperature` | float | `0.0` | Deterministic temperature — must stay at 0.0 for consistent results |
| `sentiment_enrichment.batch_size` | int | `20` | Articles per Claude API call (batched prompt) |
| `sentiment_enrichment.max_articles_per_run` | int | `500` | Safety cap: max articles processed per run (controls daily/backfill cost) |
| `sentiment_enrichment.retry_failed` | boolean | `true` | Reserved for future retry logic — has no effect in current implementation |
| `telegram.admin_chat_id` | string | `""` | Chat ID that receives heartbeats, error alerts, and pipeline progress. Overridden by `TELEGRAM_ADMIN_CHAT_ID` env var. |
| `telegram.subscriber_chat_ids` | array | `[]` | List of chat IDs that receive the daily signal report and market-closed notifications. Overridden by `TELEGRAM_SUBSCRIBER_CHAT_IDS` env var (comma-separated). If empty, `admin_chat_id` is used as the sole subscriber. |
| `telegram.confidence_threshold` | int | `40` | Minimum confidence to include a ticker in the Telegram report |
| `telegram.always_include_flips` | boolean | `true` | Always include signal flips regardless of confidence |
| `telegram.max_tickers_per_section` | int | `10` | Max tickers per BULLISH/BEARISH/Flips section (controls API cost) |
| `telegram.include_heartbeat` | boolean | `true` | Send a pipeline summary heartbeat to `admin_chat_id` after each run |
| `telegram.display_timezone` | string | `"Europe/Amsterdam"` | Timezone for timestamps shown in Telegram messages |
| `telegram.max_message_chars` | int | `4000` | Maximum characters per Telegram message before splitting. Messages that exceed this limit are split at line boundaries and annotated with `(N/M)` page indicators in the header and footer. Keep below Telegram's hard limit of 4096. |

**`scatter_command` keys** (used by the `/scatter` bot command):

| Key | Type | Default | Description |
|---|---|---|---|
| `scatter_command.default_n_days` | int | `5` | Default forward horizon in trading days when N is not specified |
| `scatter_command.max_n_days` | int | `60` | Maximum allowed value for N; larger values are clamped to this |
| `scatter_command.default_days_back` | int | `90` | Default signal history window in calendar days |
| `scatter_command.max_days_back` | int | `365` | Maximum allowed days_back; larger values are clamped to this |

**Message routing:**

| Message type | Recipients |
|---|---|
| Daily signal report | All `subscriber_chat_ids` (without heartbeat section) |
| Market closed notification | All `subscriber_chat_ids` |
| Pipeline heartbeat | `admin_chat_id` only |
| Progress updates (backfill, calculator) | `admin_chat_id` only |
| Error alerts | `admin_chat_id` only |
| Verification reports | `admin_chat_id` only |

Note: include `admin_chat_id` in `subscriber_chat_ids` if the admin also wants to receive the signal report.

---

## config/database.json

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | `"data/signals.db"` | SQLite database file path (relative to project root) |
| `wal_mode` | boolean | `true` | Enable WAL journal mode on every connection |
| `backup_before_run` | boolean | `true` | Copy DB to `backup_dir` before each daily run |
| `backup_dir` | string | `"data/backups"` | Directory for automatic pre-run backups |
| `vacuum_frequency_days` | int | `7` | Run `VACUUM` every N days to reclaim disk space |

---

## config/verify_pipeline.json

Thresholds for `scripts/verify_pipeline.py`. All keys are optional — the script falls back to built-in defaults if the file is absent.

| Key | Type | Default | Description |
|---|---|---|---|
| `indicator_ranges.bb_pctb.min` | float | `-1.0` | Minimum acceptable BB %B value; values outside 0–1 are normal during breakouts |
| `indicator_ranges.bb_pctb.max` | float | `2.0` | Maximum acceptable BB %B value |
| `ema_stuck_days_threshold` | int | `10` | Flag an EMA as stuck (computation error) if it has the same value for this many consecutive trading days; EMA distance from price is never flagged as a warning (logged at INFO only) |
| `weekly_volume_min_ratio` | float | `0.30` | Flag a week only if its volume is below this fraction of `(trading_days × local_avg_daily_volume)`; reference avg uses a rolling ±42-day window (~60 trading days) so early low-volume tickers aren't compared against inflated all-time averages |
| `warmup_rows` | int | `50` | Rows per ticker (ordered by date) to skip in range checks; covers the `ta` library's indicator warm-up period |

## Config Changes Requiring Re-runs

| Config change | Required action |
|---|---|
| Any key in `indicators.*` | `python scripts/run_calculator.py --mode full` |
| `swing_points.*` | `python scripts/run_calculator.py --mode full` |
| `support_resistance.*` | `python scripts/run_calculator.py --mode full` |
| `patterns.*` | `python scripts/run_calculator.py --mode full` |
| `divergences.*` | `python scripts/run_calculator.py --mode full` |
| `gaps.*` | `python scripts/run_calculator.py --mode full` |
| `fibonacci.*` | No stored output — computed on-the-fly by scorer |
| `profiles.*` | `python scripts/run_calculator.py --mode full` |
| `relative_strength.*` | No stored output — computed on-the-fly by scorer |
| `adaptive_weights.*` | `python scripts/run_scorer.py --historical` |
| `signal_thresholds.*` | `python scripts/run_scorer.py --historical` |
| `regime_detection.*` | `python scripts/run_scorer.py --historical` |
| `confidence_modifiers.*` | `python scripts/run_scorer.py --historical` |
| `scoring.score_expansion_factor` | `python scripts/run_scorer.py --historical` |
| `ohlcv.lookback_years` | `python scripts/run_backfill.py --phase ohlcv --force` |
| `news.lookback_months` | `python scripts/run_backfill.py --phase news --force` |
| `fundamentals.lookback_years` | `python scripts/run_backfill.py --phase fundamentals --force` |
| `filings.lookback_months` | `python scripts/run_backfill.py --phase filings --force` |
| `ai_reasoner.*` | None — applies on next run |
| `sentiment_enrichment.enabled` | None — applies on next run |
| `sentiment_enrichment.model` | None — applies on next run |
| `sentiment_enrichment.batch_size` | None — applies on next run |
| `sentiment_enrichment.max_articles_per_run` | None — applies on next run (run `enrich_finnhub_sentiment.py --all` to reprocess with new cap) |
| `telegram.*` | None — applies on next run |
| Adding ticker to `tickers.json` | See OPERATIONS.md → Adding a Ticker |
| Setting `active: false` | None — ticker skipped on next run |
