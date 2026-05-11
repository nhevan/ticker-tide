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

/** Daily snapshot card data. */
export interface DailySection {
  data_available: boolean;
  categories: DailyCategory[];
  resolved_period: string;
  scores?: CategoryScores;
  indicators?: Record<string, number | string | null>;
  indicator_scores?: Record<string, number | null>;
  patterns?: Pattern[];
  sparkline?: SparklinePoint[];
  signal?: string | null;
  confidence?: number | null;
  calibrated_score?: number | null;
  composite_score?: number | null;
  key_signals?: string[];
  earnings?: EarningsData;
  signal_flip?: SignalFlip | null;
}

/** Weekly or monthly snapshot card data. */
export interface TimeframeSection {
  data_available: boolean;
  categories: string[];
  resolved_period: string | null;
  resolved_period_label: string | null;
  is_fallback: boolean;
  scores?: CategoryScores;
  indicators?: Record<string, number | string | null>;
  indicator_scores?: Record<string, number | null>;
  patterns?: Pattern[];
  sparkline?: SparklinePoint[];
  composite_score?: number | null;
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
