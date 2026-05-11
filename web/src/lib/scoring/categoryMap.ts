/**
 * Category map mirroring INDICATOR_CATEGORY_MAP in src/scorer/category_scorer.py.
 *
 * WARNING: This file must stay in sync with the Python source of truth.
 * The drift-guard test at tests/web/test_category_map_sync.py enforces this.
 */

export const CATEGORIES = [
  'trend',
  'momentum',
  'volume',
  'volatility',
  'candlestick',
  'structural',
  'sentiment',
  'fundamental',
  'macro',
] as const;

export type Category = typeof CATEGORIES[number];

/** Maps each indicator key to its scoring category. Mirrors Python's INDICATOR_CATEGORY_MAP. */
export const INDICATOR_CATEGORY_MAP: Record<string, Category> = {
  // --- trend ---
  ema_alignment: 'trend',
  macd_line: 'trend',
  macd_histogram: 'trend',
  adx: 'trend',
  // --- momentum ---
  rsi_14: 'momentum',
  stoch_k: 'momentum',
  cci_20: 'momentum',
  williams_r: 'momentum',
  // --- volume ---
  obv: 'volume',
  cmf_20: 'volume',
  ad_line: 'volume',
  // --- volatility ---
  bb_pctb: 'volatility',
};

/** Human-readable display labels for each indicator key. */
export const INDICATOR_DISPLAY_LABELS: Record<string, string> = {
  ema_alignment: 'EMA align',
  macd_line: 'MACD line',
  macd_histogram: 'MACD hist',
  adx: 'ADX',
  rsi_14: 'RSI 14',
  stoch_k: 'Stoch K',
  cci_20: 'CCI 20',
  williams_r: 'Williams R',
  obv: 'OBV',
  cmf_20: 'CMF 20',
  ad_line: 'AD Line',
  bb_pctb: 'BB %B',
};
