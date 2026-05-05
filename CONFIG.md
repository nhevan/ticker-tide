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
| `WEB_PASSWORD` | Yes (web UI) | Shared password for the web UI login form. Must be set before starting `ticker-tide-web`. |
| `WEB_SECRET_KEY` | Yes (web UI) | Secret key for Starlette `SessionMiddleware` (signs the session cookie). Use a cryptographically random string of at least 32 characters. Generate with: `python3 -c "import secrets; print(secrets.token_hex(32))"` |

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
| `patterns.trend_context_candles` | int | `5` | Number of preceding candles required to establish trend context for hammer / shooting-star detection. Re-run the calculator (`python scripts/run_calculator.py --mode full`) after changing. |

### Divergences

| Key | Type | Default | Description |
|---|---|---|---|
| `divergences.indicators` | array | `["rsi","macd_histogram","obv","stochastic"]` | Indicators to check for divergence. Note: the ``"rsi"`` entry refers to the indicator family — the calculator persists divergence rows under ``indicator='rsi_14'`` so the column name matches the value used by the scorer's filter. |
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
| `monthly` | object | `{}` | Monthly aggregation config (no tunable params currently) |

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

Regime-specific blending of daily, weekly, and monthly composite scores. Weights are automatically
renormalized when monthly data is absent (e.g., during pipeline cold-start).

| Key | Type | Default | Description |
|---|---|---|---|
| `timeframe_weights.trending.daily` | float | `0.10` | Daily weight in trending regime |
| `timeframe_weights.trending.weekly` | float | `0.50` | Weekly weight in trending regime |
| `timeframe_weights.trending.monthly` | float | `0.40` | Monthly weight in trending regime |
| `timeframe_weights.ranging.daily` | float | `0.60` | Daily weight in ranging regime |
| `timeframe_weights.ranging.weekly` | float | `0.30` | Weekly weight in ranging regime |
| `timeframe_weights.ranging.monthly` | float | `0.10` | Monthly weight in ranging regime |
| `timeframe_weights.volatile.daily` | float | `0.25` | Daily weight in volatile regime |
| `timeframe_weights.volatile.weekly` | float | `0.45` | Weekly weight in volatile regime |
| `timeframe_weights.volatile.monthly` | float | `0.30` | Monthly weight in volatile regime |

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
| `historical_scoring.monthly_lookback_months` | int | `60` | Months of monthly scores computed in `--historical --mode {monthly,both,all}` (months 13–60). Added in commit 6 of the weekly/monthly parity series. |

> **Note on `data_completeness` storage.** `scores_daily.data_completeness`,
> `scores_weekly.data_completeness`, and `scores_monthly.data_completeness` are all
> `TEXT` columns that hold a `json.dumps(...)` blob. Commit 1's parity migration
> incorrectly created the weekly + monthly columns as `REAL`; the
> `scripts/migrate_fix_scores_completeness_type.py` migration corrects this
> in-place (idempotent — see OPERATIONS.md).

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

### Monthly adaptive weights

Regime-specific weights for the 4 indicator-based categories used in monthly scoring.
Same 4 categories as weekly (trend, momentum, volume, volatility). Monthly scoring gives
more weight to trend since longer timeframes carry more directional persistence.

| Key | Type | Default | Description |
|---|---|---|---|
| `monthly_adaptive_weights.trending.trend` | float | `0.50` | Trend category weight in trending regime |
| `monthly_adaptive_weights.trending.momentum` | float | `0.20` | Momentum category weight in trending regime |
| `monthly_adaptive_weights.trending.volume` | float | `0.15` | Volume category weight in trending regime |
| `monthly_adaptive_weights.trending.volatility` | float | `0.15` | Volatility category weight in trending regime |
| `monthly_adaptive_weights.ranging.trend` | float | `0.25` | Trend category weight in ranging regime |
| `monthly_adaptive_weights.ranging.momentum` | float | `0.35` | Momentum category weight in ranging regime |
| `monthly_adaptive_weights.ranging.volume` | float | `0.20` | Volume category weight in ranging regime |
| `monthly_adaptive_weights.ranging.volatility` | float | `0.20` | Volatility category weight in ranging regime |
| `monthly_adaptive_weights.volatile.trend` | float | `0.35` | Trend category weight in volatile regime |
| `monthly_adaptive_weights.volatile.momentum` | float | `0.20` | Momentum category weight in volatile regime |
| `monthly_adaptive_weights.volatile.volume` | float | `0.15` | Volume category weight in volatile regime |
| `monthly_adaptive_weights.volatile.volatility` | float | `0.30` | Volatility category weight in volatile regime |

### Weekly / monthly score method (v1 vs v2)

`weekly_score_method` and `monthly_score_method` switch the composite definition between
two algorithms. Default is `v1_4cat` for both — live scoring behaviour is **unchanged**
until commit 7 retrains the calibrator and validates the v2 distribution.

| Key | Type | Default | Description |
|---|---|---|---|
| `weekly_score_method` | string | `"v1_4cat"` | Weekly composite mode. `v1_4cat` = trend/momentum/volume/volatility from indicators only (existing behaviour). `v2_8cat` = adds candlestick + structural categories from `patterns_weekly`, and adds crossovers→trend / divergences→momentum/volume (mirroring daily's category wiring). |
| `monthly_score_method` | string | `"v1_4cat"` | Monthly composite mode. Same semantics as `weekly_score_method`. **Monthly candlestick is permanently disabled** in v2 — the candlestick-pattern decay window is 7 days, far shorter than monthly bar cadence, so candlestick scores would alias on scoring-vs-month-start timing and were judged unreliable. Structural patterns (28-day window) and divergences (42-day window) are still applied on monthly bars. |

**v2 is not a drop-in replacement.** Even with `candlestick = 0.0` and `structural = 0.0` weights,
the v2 scalar differs from v1 because the trend / momentum / volume categories now include
crossover and divergence contributions that v1 ignored. Calibrator retrain (commit 7) is
**mandatory** before flipping the default to v2 — flipping without retraining will silently
shift the score distribution and break signal-classification thresholds.

Re-run phase: `scorer` (with `--force` to recompute existing dates).

| Key | Type | Default | Description |
|---|---|---|---|
| `weekly_adaptive_weights_v2.<regime>.trend` | float | regime-specific | Trend weight when `weekly_score_method = v2_8cat` |
| `weekly_adaptive_weights_v2.<regime>.momentum` | float | regime-specific | Momentum weight |
| `weekly_adaptive_weights_v2.<regime>.volume` | float | regime-specific | Volume weight |
| `weekly_adaptive_weights_v2.<regime>.volatility` | float | regime-specific | Volatility weight |
| `weekly_adaptive_weights_v2.<regime>.candlestick` | float | `0.0` | Candlestick weight. Defaults to 0.0 — anti-predictive on daily; not yet validated on weekly. |
| `weekly_adaptive_weights_v2.<regime>.structural` | float | `0.0` | Structural-pattern weight. Defaults to 0.0 for the same reason. |
| `monthly_adaptive_weights_v2.<regime>.trend` | float | regime-specific | Trend weight when `monthly_score_method = v2_8cat` |
| `monthly_adaptive_weights_v2.<regime>.momentum` | float | regime-specific | Momentum weight |
| `monthly_adaptive_weights_v2.<regime>.volume` | float | regime-specific | Volume weight |
| `monthly_adaptive_weights_v2.<regime>.volatility` | float | regime-specific | Volatility weight |
| `monthly_adaptive_weights_v2.<regime>.candlestick` | float | `0.0` | Always-zero in v2 — monthly candlestick is permanently disabled regardless of weight. |
| `monthly_adaptive_weights_v2.<regime>.structural` | float | `0.0` | Structural-pattern weight. |

If `*_adaptive_weights_v2` is absent, the v2 path falls back to the v1 4-category weights
plus `candlestick = 0.0` and `structural = 0.0` (same effect as the explicit defaults).

### Calibration (rolling ridge regression)

The calibrator trains a ridge regression on recent historical signals and their realized
excess returns (vs SPY), then predicts the expected excess return for the current signal.
When enabled and sufficient training data exists, `calibrated_score` replaces the static
composite as the primary signal classification input.

| Key | Type | Default | Description |
|---|---|---|---|
| `calibration.enabled` | bool | `true` | Master switch for rolling ridge calibration |
| `calibration.window_size` | int | `365` | Calendar days to look back for training signals |
| `calibration.ridge_lambda` | float | `0.1` | L2 regularisation strength (higher = more conservative) |
| `calibration.min_training_samples` | int | `30` | Minimum samples required; fewer triggers cold-start fallback to static composite |
| `calibration.benchmark_ticker` | string | `"SPY"` | Ticker whose return is subtracted from each signal's return to compute excess return |
| `calibration.forward_days` | int | `10` | Trading-day horizon for measuring forward returns in training data |

### Calibrator acceptance gate

Distribution-level guardrail used when flipping `weekly_score_method` /
`monthly_score_method` between v1 and v2. After re-running
`scripts/run_scorer.py --historical --force`, the operator runs
`scripts/check_calibrator_acceptance.py check` to confirm that the new
calibrated_score distribution did not catastrophically drift from the pre-flip
baseline. See OPERATIONS.md "Flipping weekly_score_method" for the full
procedure.

| Key | Type | Default | Description |
|---|---|---|---|
| `calibrator_acceptance.max_mean_delta` | float | `5.0` | Maximum allowed shift in the cross-ticker mean of `calibrated_score`. Derivation: ≈ 2× the 95% CI width on a 50-ticker sample with std ≈ 9; values within this range are likely noise, not systematic shift. |
| `calibrator_acceptance.max_std_delta` | float | `8.0` | Maximum allowed shift in the cross-ticker std. Derivation: tolerates ~1 std-deviation shift before flagging — std movement larger than this implies the calibrator is reweighting the population, not just translating it. |
| `calibrator_acceptance.max_ticker_delta` | float | `15.0` | Per-ticker delta threshold. **Informational** only — counts how many individual tickers shifted by more than this between snapshots; does not by itself trigger FAIL. Surfaces bipolar shifts that mean/std deltas miss (half the tickers swing +X while half swing -X). |
| `calibrator_acceptance.min_sample_size` | int | `30` | Minimum count of non-NULL `calibrated_score` rows required in both the baseline and current snapshot. Smaller samples produce noisy mean/std; gate refuses to compare below this. |

WARNING tier (non-blocking) fires when `|Δ mean| / max_mean_delta` or
`|Δ std| / max_std_delta` falls in `[0.70, 1.00)`. FAIL fires above 1.00.

**Re-run requirement**: changing any threshold here does not require
re-running the calibrator — only the acceptance check. Adding/removing keys
in this block does, however, require a redeploy because
`scripts/check_calibrator_acceptance.py` reads the block on every invocation.

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
| `telegram.include_ai_reasoning` | boolean | `false` | When `false`, skips the Claude API call entirely and omits per-ticker reasoning, the daily summary, and the market context section from the signal report. Set to `true` to restore full AI-generated commentary. |
| `telegram.display_timezone` | string | `"Europe/Amsterdam"` | Timezone for timestamps shown in Telegram messages |
| `telegram.max_message_chars` | int | `4000` | Maximum characters per Telegram message before splitting. Messages that exceed this limit are split at line boundaries and annotated with `(N/M)` page indicators in the header and footer. Keep below Telegram's hard limit of 4096. |

**`detail_command` keys** (used by the `/detail` bot command):

| Key | Type | Default | Description |
|---|---|---|---|
| `detail_command.default_chart_days` | int | `30` | Default lookback days for the chart image |
| `detail_command.max_chart_days` | int | `180` | Maximum allowed chart days; larger values are clamped |
| `detail_command.chart_style` | string | `"nightclouds"` | mplfinance chart style name |
| `detail_command.chart_figsize` | array | `[14, 10]` | Chart figure size `[width, height]` in inches |
| `detail_command.sr_levels_to_show` | int | `3` | Maximum S/R levels shown per direction (resistance / support) |
| `detail_command.signal_history_days` | int | `30` | Days of signal history to include in msg #3 breakdown |
| `detail_command.peer_count` | int | `5` | Maximum sector peers to show |
| `detail_command.category_agreement_min_score` | float | `10.0` | A category is counted as "agreeing" or "disagreeing" with the signal direction when `abs(category_score) ≥` this threshold. Lower values include weaker signals; higher values surface only strong category alignment. Display-only — no pipeline re-run needed. |
| `detail_command.calibration_divergence_min_abs` | float | `0.3` | Minimum `abs(calibrated_score)` required before the raw-vs-calibrated sign-flip check is applied. Suppresses the ⚠️ flag when calibrated_score is near-zero (directionally meaningless). Display-only — no pipeline re-run needed. |
| `detail_command.earnings_warning_days` | int | `7` | Prepend an earnings ⚠️ warning line to the verdict header when the next earnings date is within this many calendar days (inclusive boundary). Display-only — no pipeline re-run needed. |
| `detail_command.timeframe_direction_threshold` | float | `15.0` | Score > +threshold renders ▲ (bullish); score < -threshold renders ▼ (bearish); otherwise ▬ (neutral) in the timeframe summary table. Display-only — no pipeline re-run needed. |

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
| `weekly_pattern_warn_zero_window_weeks` | int | `30` | Warn if no `patterns_weekly` rows exist across all active tickers within the most recent N closed weeks (zero-detection signal). Used by `check_weekly_pattern_count`. |
| `weekly_pattern_structural_warn_high` | int | `400` | Warn if any single ticker has more than this many `patterns_weekly` structural rows over its full history. Distinct from the daily 2000 constant — weeks compress signal so the threshold is lower. |
| `monthly_pattern_warn_zero_window_months` | int | `24` | Warn if no `patterns_monthly` rows exist across all active tickers within the most recent N closed months. Used by `check_monthly_pattern_count`. |
| `weekly_divergence_warn_zero_window_days` | int | `90` | Warn if zero `divergences_weekly` rows exist within this many days. **Lower bound only** — adversarial review dropped the upper-bound rule because divergence detection naturally produces variable counts. |
| `monthly_divergence_warn_zero_window_months` | int | `12` | Same as `weekly_divergence_warn_zero_window_days` but for `divergences_monthly`. |
| `weekly_crossover_warn_zero_window_days` | int | `90` | Warn if zero `crossovers_weekly` rows exist within this many days. Used by `check_weekly_crossover_count`. |
| `monthly_crossover_warn_zero_window_months` | int | `24` | Same as `weekly_crossover_warn_zero_window_days` but for `crossovers_monthly`. |
| `weekly_score_coverage_window_weeks` | int | `12` | Lookback window for `check_scores_weekly_table_coverage`. A ticker is considered "should be covered" only if it has at least one `indicators_weekly` row in this window — warm-up tickers (candles but no indicators) are NOT counted as gaps. |
| `monthly_score_coverage_window_months` | int | `6` | Same as `weekly_score_coverage_window_weeks` but for `scores_monthly` / `indicators_monthly`. |
| `category_math_window_days` | int | `365` | How far back `check_scores_weekly_category_math` and `check_scores_monthly_category_math` validate `composite ≈ clamp(sum(category × weight) × score_expansion_factor)`. Bounds the cost of the deterministic math check. |
| `category_math_tolerance` | float | `0.01` | Absolute tolerance (in score points) for the category-math equality. Tight (0.01) because the formula is deterministic; the check tries v1 weights then v2 (`weekly_adaptive_weights_v2` / `monthly_adaptive_weights_v2`) and passes if either matches. |

The `_*_warn_zero_*` keys are diagnostic ("nobody is producing data") and only ever produce warnings — never failures. The score-range and category-math checks fail on out-of-bound composites and warn on math drift.

## config/web.json

Configuration for the read-only web UI (`scripts/run_web.py` + `src/web/`).

| Key | Type | Default | Description |
|---|---|---|---|
| `port` | int | `8765` | Uvicorn listen port (binds to `127.0.0.1:port` — local Caddy proxy only) |
| `session_ttl_hours` | int | `168` | Session cookie lifetime in hours (168 = 7 days). Applies to `SessionMiddleware.max_age`. |
| `login_rate_limit.max_attempts` | int | `5` | Maximum login attempts allowed per IP within `window_seconds` before returning 429 |
| `login_rate_limit.window_seconds` | int | `60` | Sliding window duration in seconds for login rate limit |
| `llm_rate_limit.window_seconds` | int | `60` | In-memory per-(session, ticker, date, timeframe) debounce window in seconds for `/api/llm` |
| `sparkline.daily_days` | int | `15` | Number of trading-day OHLCV rows to include in the daily sparkline (bounded by `<= picked_date`) |
| `sparkline.weekly_weeks` | int | `6` | Number of weekly candle rows to include in the weekly sparkline |
| `sparkline.monthly_months` | int | `6` | Number of monthly candle rows to include in the monthly sparkline |
| `ai_reasoner.model` | string | `claude-sonnet-4-20250514` | Anthropic model to use for web LLM analysis |
| `ai_reasoner.max_tokens` | int | `800` | Maximum tokens in Claude's response |
| `ai_reasoner.temperature` | float | `0.3` | Sampling temperature for Claude |
| `ai_reasoner.target_words` | int | `150` | Target word count in the prompt instruction to Claude |
| `why_bullets.limit` | int | `3` | Maximum number of key_signals items to show in the "Why" section of the daily card. Items come from `scores_daily.key_signals` (JSON list, 7 items in production). |
| `signal_flip_lookback_days` | int | `14` | Number of calendar days to look back from the picked date when searching for a recent signal flip. The badge is shown only when a flip exists within this window. |

**Re-run required after change:**
- `login_rate_limit.*`, `llm_rate_limit.*`, `sparkline.*`, `ai_reasoner.*` — None; applies on next web UI request.
- `why_bullets.*`, `signal_flip_lookback_days` — None; web-only read layer, applies on next snapshot load. No pipeline phase re-run needed.
- `port` — `sudo systemctl restart ticker-tide-web`

---

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
| `web.json port` | `sudo systemctl restart ticker-tide-web` |
| `web.json login_rate_limit.*` | None — applies on next login attempt |
| `web.json llm_rate_limit.*` | None — applies on next LLM request |
| `web.json sparkline.*` | None — applies on next snapshot load |
| `web.json ai_reasoner.*` | None — applies on next LLM request |
| `web.json why_bullets.*` | None — applies on next snapshot load |
| `web.json signal_flip_lookback_days` | None — applies on next snapshot load |
