/**
 * Indicator agreement matrix table.
 *
 * Renders one row per indicator (from INDICATOR_CATEGORY_MAP) and one column
 * per scoring category. The own-category cell is coloured green/red/grey based
 * on whether the indicator score agrees with the signal direction. All
 * off-category cells are always grey.
 *
 * Below the indicator rows, optional pattern rows are rendered for each item in
 * recentPatterns. The categories prop drives which columns are displayed, so
 * monthly (5 columns, no candlestick) renders correctly without a code change.
 */

import { INDICATOR_CATEGORY_MAP, INDICATOR_DISPLAY_LABELS } from '@/lib/scoring/categoryMap';
import type { Category } from '@/lib/scoring/categoryMap';
import { humanizePatternName } from '@/lib/scoring/patternLabels';
import type { Pattern } from '@/lib/api/types';
import type { DailyCategory, WeeklyCategory, MonthlyCategory } from '@/lib/api/types';

interface MatrixTableProps {
  /** Section title displayed above the table. */
  title: string;
  /** Raw indicator values keyed by indicator name. */
  indicators: Record<string, number | string | null> | undefined;
  /** Per-indicator scores keyed by indicator name. */
  indicatorScores: Record<string, number | null> | undefined;
  /** Direction derived from the signal: 1 = bullish, -1 = bearish, 0 = neutral. */
  signalDirection: 1 | -1 | 0;
  /**
   * Category columns to render. Drives both the column headers and the per-row
   * cell iteration. Pass DailyCategory[] for daily (9 cols), WeeklyCategory[] for
   * weekly (6 cols), or MonthlyCategory[] for monthly (5 cols, no candlestick).
   * Defaults to the full 9-category set for backward compatibility.
   */
  categories?: DailyCategory[] | WeeklyCategory[] | MonthlyCategory[];
  /** Timeframe label — controls pattern-cell text format. */
  timeframe?: 'daily' | 'weekly' | 'monthly';
  /** Optional recent pattern rows rendered below indicator rows. */
  recentPatterns?: Pattern[];
}

/** Default full category list (daily). Used when categories prop is omitted. */
const DEFAULT_CATEGORIES: DailyCategory[] = [
  'trend', 'momentum', 'volume', 'volatility',
  'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
];

/** Column header labels for each category. */
const CATEGORY_HEADERS: Record<Category, string> = {
  trend: 'Trend',
  momentum: 'Momentum',
  volume: 'Volume',
  volatility: 'Volatility',
  candlestick: 'Candlestick',
  structural: 'Structural',
  sentiment: 'Sentiment',
  fundamental: 'Fundamental',
  macro: 'Macro',
};

type CellTone = 'green' | 'red' | 'grey';

/**
 * Determine the colour tone for a cell at the intersection of an indicator row
 * and a category column.
 *
 * @param indicatorKey - The indicator's map key (e.g. "rsi_14").
 * @param columnCategory - The category this column represents.
 * @param score - The indicator's numeric score, or null if unavailable.
 * @param signalDirection - The signal direction: 1, -1, or 0.
 * @returns 'green', 'red', or 'grey'.
 */
function resolveCellTone(
  indicatorKey: string,
  columnCategory: Category,
  score: number | null,
  signalDirection: 1 | -1 | 0,
): CellTone {
  const indicatorCategory = INDICATOR_CATEGORY_MAP[indicatorKey];
  if (indicatorCategory !== columnCategory) {
    return 'grey';
  }
  if (score === null || score === 0 || signalDirection === 0) {
    return 'grey';
  }
  return Math.sign(score) === signalDirection ? 'green' : 'red';
}

/**
 * Determine the colour tone for a pattern row cell.
 *
 * @param patternCategory - The pattern's own category (e.g. "candlestick").
 * @param columnCategory - The category this column represents.
 * @param direction - The pattern's direction ("bullish", "bearish", "neutral").
 * @param signalDirection - The overall signal direction: 1, -1, or 0.
 * @returns 'green', 'red', or 'grey'.
 */
function resolvePatternCellTone(
  patternCategory: string,
  columnCategory: Category,
  direction: string,
  signalDirection: 1 | -1 | 0,
): CellTone {
  if (patternCategory !== columnCategory) {
    return 'grey';
  }
  if (signalDirection === 0) {
    return 'grey';
  }
  if (direction !== 'bullish' && direction !== 'bearish') {
    return 'grey';
  }
  const directionSign = direction === 'bullish' ? 1 : -1;
  return directionSign === signalDirection ? 'green' : 'red';
}

/**
 * Build the text content for a pattern own-category cell.
 *
 * @param timeframe - Timeframe determines the label format.
 * @param daysAgo - Number of days ago the pattern occurred (daily only).
 * @param confirmed - Whether the pattern is confirmed.
 * @returns Display string for the cell.
 */
function patternCellText(
  timeframe: 'daily' | 'weekly' | 'monthly',
  daysAgo: number | undefined,
  confirmed: boolean,
): string {
  if (timeframe === 'daily') {
    if (daysAgo === undefined) {
      return confirmed ? '✓' : '';
    }
    const prefix = daysAgo === 0 ? 'today' : `${daysAgo}d`;
    return confirmed ? `${prefix} ✓` : prefix;
  }
  return confirmed ? '✓' : '';
}

/** Tailwind background class for each tone. */
const TONE_CLASS: Record<CellTone, string> = {
  green: 'bg-emerald-500/20',
  red: 'bg-rose-500/20',
  grey: 'bg-muted',
};

/**
 * Format a raw indicator value for display.
 *
 * @param value - Raw value from the indicators map.
 * @returns Two-decimal string for numbers, the string itself, or "—" if absent.
 */
function formatValue(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return '—';
  if (typeof value === 'number') return value.toFixed(2);
  return value;
}

const INDICATOR_KEYS = Object.keys(INDICATOR_CATEGORY_MAP);

/**
 * Render an indicator agreement matrix for a single timeframe.
 *
 * Shows an empty-state message when both indicators and indicatorScores are
 * absent or empty. Below indicator rows, renders optional pattern rows from
 * recentPatterns.
 *
 * @param props - See MatrixTableProps.
 */
export function MatrixTable({
  title,
  indicators,
  indicatorScores,
  signalDirection,
  categories = DEFAULT_CATEGORIES,
  timeframe = 'daily',
  recentPatterns = [],
}: MatrixTableProps) {
  const hasData =
    (indicators !== undefined && Object.keys(indicators).length > 0) ||
    (indicatorScores !== undefined && Object.keys(indicatorScores).length > 0);

  return (
    <div className="rounded-lg border p-4">
      <h3 className="mb-3 text-sm font-semibold text-foreground">{title}</h3>

      {!hasData ? (
        <p className="text-sm text-muted-foreground">Indicator scores not available</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-muted-foreground">
                <th className="py-1 pr-3 text-left font-normal">Indicator</th>
                <th className="py-1 pr-3 text-right font-normal">Value</th>
                {categories.map((cat) => (
                  <th key={cat} className="py-1 px-1 text-center font-normal">
                    {CATEGORY_HEADERS[cat as Category]}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {INDICATOR_KEYS.map((indicatorKey) => {
                const score = indicatorScores?.[indicatorKey] ?? null;
                const rawValue = indicators?.[indicatorKey];
                return (
                  <tr key={indicatorKey} className="border-t border-border/40">
                    <td className="py-1 pr-3 text-left text-foreground">
                      {INDICATOR_DISPLAY_LABELS[indicatorKey] ?? indicatorKey}
                    </td>
                    <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">
                      {formatValue(rawValue)}
                    </td>
                    {categories.map((cat) => {
                      const tone = resolveCellTone(indicatorKey, cat as Category, score, signalDirection);
                      return (
                        <td
                          key={cat}
                          className={`py-1 px-1 text-center ${TONE_CLASS[tone]}`}
                          data-testid={`cell-${indicatorKey}-${cat}`}
                          data-tone={tone}
                        >
                          &nbsp;
                        </td>
                      );
                    })}
                  </tr>
                );
              })}

              {recentPatterns.map((pattern, index) => (
                <tr key={`${pattern.pattern_name}-${index}`} className="border-t border-border/40">
                  <td className="py-1 pr-3 text-left text-foreground">
                    {humanizePatternName(pattern.pattern_name)}
                  </td>
                  <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">
                    {pattern.strength.toFixed(2)}
                  </td>
                  {categories.map((cat) => {
                    const isOwnCategory = cat === pattern.pattern_category;
                    const tone = resolvePatternCellTone(
                      pattern.pattern_category,
                      cat as Category,
                      pattern.direction,
                      signalDirection,
                    );
                    const cellText = isOwnCategory
                      ? patternCellText(timeframe, pattern.days_ago, pattern.confirmed)
                      : '';
                    return (
                      <td
                        key={cat}
                        className={`py-1 px-1 text-center ${TONE_CLASS[tone]}`}
                        data-testid={`pattern-cell-${pattern.pattern_name}-${index}-${cat}`}
                        data-tone={tone}
                      >
                        {cellText || <>&nbsp;</>}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
