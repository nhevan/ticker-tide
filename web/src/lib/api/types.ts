/**
 * Type definitions for the Ticker Tide API.
 *
 * These shapes mirror the return values of src/web/queries.py. All date strings
 * are YYYY-MM-DD. Nullable fields use null (not undefined) to match JSON.
 */

/** Category names for daily timeframe (9 categories). */
export type DailyCategory =
  | 'trend'
  | 'momentum'
  | 'volume'
  | 'volatility'
  | 'candlestick'
  | 'structural'
  | 'sentiment'
  | 'fundamental'
  | 'macro';

/** Category names for weekly timeframe (6 categories). */
export type WeeklyCategory =
  | 'trend'
  | 'momentum'
  | 'volume'
  | 'volatility'
  | 'candlestick'
  | 'structural';

/** Category names for monthly timeframe (5 categories, no candlestick). */
export type MonthlyCategory =
  | 'trend'
  | 'momentum'
  | 'volume'
  | 'volatility'
  | 'structural';

/** Per-category score values (float or null). */
export type CategoryScores = Record<string, number | null>;

/** A single sparkline data point. */
export interface SparklinePoint {
  date: string;
  close: number;
}

/** A single detected pattern. */
export interface Pattern {
  pattern_name: string;
  pattern_category: string;
  direction: string;
  strength: number;
  confirmed: boolean;
  days_ago?: number;
}

/** Next upcoming earnings data. */
export interface NextEarnings {
  date: string;
  days_until: number | null;
  estimated_eps: number | null;
}

/** Last reported earnings surprise data. */
export interface LastSurprise {
  date: string;
  actual_eps: number;
  surprise: number | null;
  beat: boolean | null;
}

/** Earnings section in the daily snapshot card. */
export interface EarningsData {
  next: NextEarnings | null;
  last_surprise: LastSurprise | null;
}

/** Most recent signal flip within the lookback window. */
export interface SignalFlip {
  date: string;
  previous_signal: string;
  new_signal: string;
  days_ago: number | null;
}

/** RSI percentile profile from indicator_profiles. */
export interface RsiProfile {
  p5: number;
  p20: number;
  p50: number;
  p80: number;
  p95: number;
  mean: number;
  std: number;
}

/** A single indicator contribution item inside ContributionsPayload. */
export interface ContributionItem {
  name: string;
  category: string;
  /** Distinguishes indicator, pattern, and aggregate contributions. Python emits all three values. */
  kind: 'indicator' | 'pattern' | 'aggregate';
  /** score is the INDICATOR SCORE (−100 to +100), NOT the raw measurement. */
  score: number;
  /**
   * Indicators: stores the indicator score, not the raw measurement (see
   * IndicatorExplainerPanel). Patterns and aggregates: always `null` —
   * those item kinds have no single scalar raw measurement.
   */
  raw_value: number | null;
  category_weight: number;
  contribution: number;
}

/** The contributions payload stored in scores_daily.key_signals_data. */
export interface ContributionsPayload {
  expansion_factor: number;
  items: ContributionItem[];
}

/** Response shape for GET /api/scoring-rules. */
export interface ScoringRules {
  rsi: {
    thresholds: { oversold: number; overbought: number };
    scoring_method: string;
    fallback_zones: string[];
    profile_zones: string[];
  };
  regime_weights: Record<string, Record<string, number>>;
  score_expansion_factor: number;
  approximation_caveat: string;
}

/** Daily snapshot card data. */
export interface DailySection {
  data_available: boolean;
  categories: DailyCategory[];
  resolved_period: string;
  scores?: CategoryScores;
  indicators?: Record<string, number | string | null>;
  indicator_scores?: Record<string, number | null>;
  patterns?: Pattern[];
  recent_patterns?: Pattern[];
  sparkline?: SparklinePoint[];
  signal?: string | null;
  confidence?: number | null;
  calibrated_score?: number | null;
  composite_score?: number | null;
  key_signals?: string[];
  earnings?: EarningsData;
  signal_flip?: SignalFlip | null;
  /** Market regime from scores_daily (e.g. "trending", "ranging", "volatile"). */
  regime?: string | null;
  /** Per-ticker RSI percentile profile, or null if no profile exists. */
  rsi_profile?: RsiProfile | null;
  /** Zone label string from zone_label_for_rsi(), or null if RSI unavailable. */
  rsi_zone_label?: string | null;
  /** Parsed key_signals_data payload, or null for legacy rows. */
  contributions_payload?: ContributionsPayload | null;
  /**
   * Last N working days of RSI(14) values for this ticker, ordered ascending by date,
   * bounded by the picked date. Rows with rsi_14 IS NULL are excluded.
   * Always present when data_available is true — empty array when no RSI data exists,
   * never null, never absent. Configured via web.json sparkline.rsi_sparkline_days (default 100).
   */
  rsi_sparkline?: { date: string; value: number }[];
}

/** Weekly or monthly snapshot card data. */
export interface TimeframeSection {
  data_available: boolean;
  categories: WeeklyCategory[] | MonthlyCategory[];
  resolved_period: string | null;
  resolved_period_label: string | null;
  is_fallback: boolean;
  scores?: CategoryScores;
  indicators?: Record<string, number | string | null>;
  indicator_scores?: Record<string, number | null>;
  patterns?: Pattern[];
  recent_patterns?: Pattern[];
  sparkline?: SparklinePoint[];
  composite_score?: number | null;
  /** Parsed key_signals_data payload, or null/absent for legacy rows. */
  contributions_payload?: ContributionsPayload | null;
}

/** Full snapshot response from GET /api/snapshot. */
export interface Snapshot {
  daily: DailySection;
  weekly: TimeframeSection;
  monthly: TimeframeSection;
}

/** Response from POST /api/llm. */
export interface LlmResponse {
  text: string;
}

/** Response from GET/POST /api/verdict. */
export interface VerdictResponse {
  verdict: string;
  generated_at: string;
}

/** Response from GET /api/me. */
export interface MeResponse {
  authenticated: boolean;
}

/** Response from GET /api/dates. */
export interface DateRange {
  min: string | null;
  max: string | null;
}
