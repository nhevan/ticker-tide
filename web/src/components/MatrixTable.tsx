/**
 * Indicator agreement matrix table.
 *
 * Renders one row per indicator (from INDICATOR_CATEGORY_MAP) and one column
 * per scoring category. The own-category cell is coloured green/red/grey based
 * on whether the indicator score agrees with the signal direction. All
 * off-category cells are always grey.
 */

import { CATEGORIES, INDICATOR_CATEGORY_MAP, INDICATOR_DISPLAY_LABELS } from '@/lib/scoring/categoryMap';
import type { Category } from '@/lib/scoring/categoryMap';

interface MatrixTableProps {
  /** Section title displayed above the table. */
  title: string;
  /** Raw indicator values keyed by indicator name. */
  indicators: Record<string, number | string | null> | undefined;
  /** Per-indicator scores keyed by indicator name. */
  indicatorScores: Record<string, number | null> | undefined;
  /** Direction derived from the signal: 1 = bullish, -1 = bearish, 0 = neutral. */
  signalDirection: 1 | -1 | 0;
}

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
 * absent or empty.
 *
 * @param props - See MatrixTableProps.
 */
export function MatrixTable({
  title,
  indicators,
  indicatorScores,
  signalDirection,
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
                {CATEGORIES.map((cat) => (
                  <th key={cat} className="py-1 px-1 text-center font-normal">
                    {CATEGORY_HEADERS[cat]}
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
                    {CATEGORIES.map((cat) => {
                      const tone = resolveCellTone(indicatorKey, cat, score, signalDirection);
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
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
