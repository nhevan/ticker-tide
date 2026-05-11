/**
 * Pattern display labels for the indicator agreement matrix.
 *
 * PATTERN_DISPLAY_LABELS maps pattern_name keys (as stored in the database
 * by src/calculator/patterns.py) to human-readable labels for the matrix
 * pattern rows.
 *
 * Candlestick names: doji, hammer, shooting_star, bullish_engulfing,
 * bearish_engulfing, morning_star, evening_star
 *
 * Structural names from _STRUCTURAL_BASE in src/scorer/pattern_scorer.py:
 * double_top, double_bottom, breakout, breakdown, bull_flag, bear_flag,
 * false_breakout
 */

/** Map of snake_case pattern_name → human-readable display label. */
export const PATTERN_DISPLAY_LABELS: Record<string, string> = {
  // Candlestick patterns (from src/calculator/patterns.py detectors)
  doji: 'Doji',
  hammer: 'Hammer',
  shooting_star: 'Shooting Star',
  bullish_engulfing: 'Bullish Engulfing',
  bearish_engulfing: 'Bearish Engulfing',
  morning_star: 'Morning Star',
  evening_star: 'Evening Star',
  // Structural patterns (from _STRUCTURAL_BASE in src/scorer/pattern_scorer.py)
  double_top: 'Double Top',
  double_bottom: 'Double Bottom',
  breakout: 'Breakout',
  breakdown: 'Breakdown',
  bull_flag: 'Bull Flag',
  bear_flag: 'Bear Flag',
  false_breakout: 'False Breakout',
};

/**
 * Convert a snake_case pattern_name into a human-readable label.
 *
 * Returns PATTERN_DISPLAY_LABELS[name] if present, otherwise splits on
 * underscores and title-cases each word (e.g. "some_made_up_pattern"
 * → "Some Made Up Pattern").
 *
 * @param name - The snake_case pattern_name from the database.
 * @returns A human-readable label string.
 */
export function humanizePatternName(name: string): string {
  if (name in PATTERN_DISPLAY_LABELS) {
    return PATTERN_DISPLAY_LABELS[name];
  }
  return name
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}
