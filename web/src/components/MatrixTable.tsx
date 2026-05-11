/**
 * Indicator agreement matrix table.
 *
 * Always renders all 9 category columns regardless of timeframe so daily,
 * weekly, and monthly matrices stay structurally consistent. Cells that
 * aren't applicable show a generic `—` label with a hover tooltip explaining
 * why (off-timeframe category, missing score, or no patterns in window).
 *
 * The `categories` prop now means "categories actually scored at this
 * timeframe" — it drives the off-timeframe tooltip path, not the column set.
 *
 * Below the indicator rows, pattern rows render for each item in
 * recentPatterns. Two placeholder rows (Candlestick, Structural) appear when
 * there are zero patterns for that category.
 */

import { INDICATOR_CATEGORY_MAP, INDICATOR_DISPLAY_LABELS } from '@/lib/scoring/categoryMap';
import type { Category } from '@/lib/scoring/categoryMap';
import { humanizePatternName } from '@/lib/scoring/patternLabels';
import type { CategoryScores, Pattern } from '@/lib/api/types';
import type { DailyCategory, WeeklyCategory, MonthlyCategory } from '@/lib/api/types';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

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
   * Categories actually scored at this timeframe (from section.categories).
   * Columns outside this set render as off-timeframe (`—` with tooltip).
   * Defaults to the full 9-category daily set for backward compatibility.
   */
  categories?: DailyCategory[] | WeeklyCategory[] | MonthlyCategory[];
  /** Timeframe label — controls pattern-cell text format. */
  timeframe?: 'daily' | 'weekly' | 'monthly';
  /** Optional recent pattern rows rendered below indicator rows. */
  recentPatterns?: Pattern[];
  /**
   * Per-category aggregate scores from the snapshot (section.scores). Used to
   * render the Sentiment, Fundamental, and Macro aggregate rows — categories
   * that have no indicator/pattern constituents in the matrix.
   */
  categoryScores?: CategoryScores;
}

/** All 9 categories. Always rendered as columns; off-timeframe cells get a tooltip. */
const ALL_CATEGORIES: Category[] = [
  'trend', 'momentum', 'volume', 'volatility',
  'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
];

/** Default scored-categories list (daily — all 9). Used when prop is omitted. */
const DEFAULT_SCORED_CATEGORIES: DailyCategory[] = ALL_CATEGORIES as DailyCategory[];

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

/** Pattern categories that get a placeholder row when no real patterns exist. */
const PATTERN_CATEGORIES: Array<'candlestick' | 'structural'> = ['candlestick', 'structural'];

/**
 * Categories with no indicator/pattern constituents in the matrix. Rendered
 * as aggregate rows showing the category's rollup score directly.
 */
const AGGREGATE_CATEGORIES: Array<'sentiment' | 'fundamental' | 'macro'> = [
  'sentiment', 'fundamental', 'macro',
];

type CellTone = 'green' | 'red' | 'grey';

/** Tailwind background class for each tone. */
const TONE_CLASS: Record<CellTone, string> = {
  green: 'bg-emerald-500/20',
  red: 'bg-rose-500/20',
  grey: 'bg-muted',
};

/**
 * Tooltip text for an off-timeframe column at this timeframe.
 *
 * @param category - The category whose own-category cell is off-timeframe.
 * @returns Short reason string for the hover tooltip.
 */
function offTimeframeReason(category: Category): string {
  if (category === 'sentiment' || category === 'fundamental' || category === 'macro') {
    return 'Daily only';
  }
  if (category === 'candlestick') {
    return 'Daily and weekly only';
  }
  return 'Not scored at this timeframe';
}

/** Discriminated state for a single matrix cell. */
type CellState =
  | { kind: 'off-category' }
  | { kind: 'off-timeframe'; tooltip: string }
  | { kind: 'missing'; tooltip: string }
  | { kind: 'valid'; tone: CellTone; text: string };

/**
 * Resolve the cell state for an indicator row × category column intersection.
 *
 * @param indicatorKey - The indicator's map key (e.g. "rsi_14").
 * @param columnCategory - The category this column represents.
 * @param score - The indicator's numeric score, or null if unavailable.
 * @param signalDirection - The signal direction: 1, -1, or 0.
 * @param scored - Set of categories actually scored at this timeframe.
 * @returns CellState describing how to render the cell.
 */
function resolveIndicatorCellState(
  indicatorKey: string,
  columnCategory: Category,
  score: number | null,
  signalDirection: 1 | -1 | 0,
  scored: Set<string>,
): CellState {
  const indicatorCategory = INDICATOR_CATEGORY_MAP[indicatorKey];
  if (indicatorCategory !== columnCategory) {
    return { kind: 'off-category' };
  }
  if (!scored.has(columnCategory)) {
    return { kind: 'off-timeframe', tooltip: offTimeframeReason(columnCategory) };
  }
  if (score === null) {
    return { kind: 'missing', tooltip: 'Score not available' };
  }
  if (score === 0 || signalDirection === 0) {
    return { kind: 'valid', tone: 'grey', text: '' };
  }
  return {
    kind: 'valid',
    tone: Math.sign(score) === signalDirection ? 'green' : 'red',
    text: '',
  };
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

/**
 * Resolve the cell state for a real pattern row × category column intersection.
 */
function resolvePatternCellState(
  pattern: Pattern,
  columnCategory: Category,
  signalDirection: 1 | -1 | 0,
  scored: Set<string>,
  timeframe: 'daily' | 'weekly' | 'monthly',
): CellState {
  if (pattern.pattern_category !== columnCategory) {
    return { kind: 'off-category' };
  }
  if (!scored.has(columnCategory)) {
    return { kind: 'off-timeframe', tooltip: offTimeframeReason(columnCategory) };
  }
  if (signalDirection === 0) {
    return { kind: 'valid', tone: 'grey', text: patternCellText(timeframe, pattern.days_ago, pattern.confirmed) };
  }
  if (pattern.direction !== 'bullish' && pattern.direction !== 'bearish') {
    return { kind: 'valid', tone: 'grey', text: patternCellText(timeframe, pattern.days_ago, pattern.confirmed) };
  }
  const directionSign = pattern.direction === 'bullish' ? 1 : -1;
  return {
    kind: 'valid',
    tone: directionSign === signalDirection ? 'green' : 'red',
    text: patternCellText(timeframe, pattern.days_ago, pattern.confirmed),
  };
}

/**
 * Resolve the cell state for an aggregate-category row × column intersection.
 *
 * Aggregate rows (sentiment, fundamental, macro) render the category's
 * rollup score directly — no constituent indicator or pattern.
 */
function resolveAggregateCellState(
  aggregateCategory: Category,
  columnCategory: Category,
  score: number | null | undefined,
  signalDirection: 1 | -1 | 0,
  scored: Set<string>,
): CellState {
  if (aggregateCategory !== columnCategory) {
    return { kind: 'off-category' };
  }
  if (!scored.has(columnCategory)) {
    return { kind: 'off-timeframe', tooltip: offTimeframeReason(columnCategory) };
  }
  if (score === null || score === undefined) {
    return { kind: 'missing', tooltip: 'Score not available' };
  }
  if (score === 0 || signalDirection === 0) {
    return { kind: 'valid', tone: 'grey', text: '' };
  }
  return {
    kind: 'valid',
    tone: Math.sign(score) === signalDirection ? 'green' : 'red',
    text: '',
  };
}

/**
 * Resolve the cell state for a placeholder pattern row × column intersection.
 */
function resolvePlaceholderCellState(
  placeholderCategory: 'candlestick' | 'structural',
  columnCategory: Category,
  scored: Set<string>,
): CellState {
  if (placeholderCategory !== columnCategory) {
    return { kind: 'off-category' };
  }
  if (!scored.has(columnCategory)) {
    return { kind: 'off-timeframe', tooltip: offTimeframeReason(columnCategory) };
  }
  return { kind: 'missing', tooltip: 'No patterns detected in window' };
}

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

interface CellViewProps {
  state: CellState;
  testid: string;
}

/** Render a single matrix cell from its resolved state. */
function CellView({ state, testid }: CellViewProps) {
  if (state.kind === 'off-category') {
    return (
      <td
        className={`py-1 px-1 text-center ${TONE_CLASS.grey}`}
        data-testid={testid}
        data-tone="grey"
      >
        &nbsp;
      </td>
    );
  }
  if (state.kind === 'off-timeframe' || state.kind === 'missing') {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <td
            className={`py-1 px-1 text-center ${TONE_CLASS.grey} text-muted-foreground cursor-help`}
            data-testid={testid}
            data-tone="grey"
            title={state.tooltip}
          >
            —
          </td>
        </TooltipTrigger>
        <TooltipContent>{state.tooltip}</TooltipContent>
      </Tooltip>
    );
  }
  return (
    <td
      className={`py-1 px-1 text-center ${TONE_CLASS[state.tone]}`}
      data-testid={testid}
      data-tone={state.tone}
    >
      {state.text || <>&nbsp;</>}
    </td>
  );
}

/**
 * Render an indicator agreement matrix for a single timeframe.
 *
 * Shows an empty-state message when both indicators and indicatorScores are
 * absent or empty. Below indicator rows, renders pattern rows from
 * recentPatterns. Always renders placeholder rows for any pattern category
 * (candlestick, structural) that has zero real patterns.
 *
 * @param props - See MatrixTableProps.
 */
export function MatrixTable({
  title,
  indicators,
  indicatorScores,
  signalDirection,
  categories = DEFAULT_SCORED_CATEGORIES,
  timeframe = 'daily',
  recentPatterns = [],
  categoryScores,
}: MatrixTableProps) {
  const hasData =
    (indicators !== undefined && Object.keys(indicators).length > 0) ||
    (indicatorScores !== undefined && Object.keys(indicatorScores).length > 0);

  const scored = new Set<string>(categories as readonly string[]);

  return (
    <TooltipProvider delayDuration={150}>
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
                {ALL_CATEGORIES.map((cat) => (
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
                    {ALL_CATEGORIES.map((cat) => (
                      <CellView
                        key={cat}
                        testid={`cell-${indicatorKey}-${cat}`}
                        state={resolveIndicatorCellState(indicatorKey, cat, score, signalDirection, scored)}
                      />
                    ))}
                  </tr>
                );
              })}

              {PATTERN_CATEGORIES.flatMap((patternCat) => {
                const reals = recentPatterns
                  .map((pattern, index) => ({ pattern, index }))
                  .filter(({ pattern }) => pattern.pattern_category === patternCat);

                if (reals.length > 0) {
                  return reals.map(({ pattern, index }) => (
                    <tr key={`pattern-${patternCat}-${pattern.pattern_name}-${index}`} className="border-t border-border/40">
                      <td className="py-1 pr-3 text-left text-foreground">
                        {humanizePatternName(pattern.pattern_name)}
                      </td>
                      <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">
                        {pattern.strength.toFixed(2)}
                      </td>
                      {ALL_CATEGORIES.map((cat) => (
                        <CellView
                          key={cat}
                          testid={`pattern-cell-${pattern.pattern_name}-${index}-${cat}`}
                          state={resolvePatternCellState(pattern, cat, signalDirection, scored, timeframe)}
                        />
                      ))}
                    </tr>
                  ));
                }

                return [
                  <tr key={`pattern-placeholder-${patternCat}`} className="border-t border-border/40">
                    <td className="py-1 pr-3 text-left text-muted-foreground italic">
                      {CATEGORY_HEADERS[patternCat]}
                    </td>
                    <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">—</td>
                    {ALL_CATEGORIES.map((cat) => (
                      <CellView
                        key={cat}
                        testid={`pattern-placeholder-cell-${patternCat}-${cat}`}
                        state={resolvePlaceholderCellState(patternCat, cat, scored)}
                      />
                    ))}
                  </tr>,
                ];
              })}

              {AGGREGATE_CATEGORIES.map((aggCat) => {
                const score = categoryScores?.[aggCat] ?? null;
                const valueText =
                  score === null || score === undefined ? '—' : score.toFixed(2);
                return (
                  <tr key={`aggregate-${aggCat}`} className="border-t border-border/40">
                    <td className="py-1 pr-3 text-left text-muted-foreground italic">
                      {CATEGORY_HEADERS[aggCat]}
                    </td>
                    <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">
                      {valueText}
                    </td>
                    {ALL_CATEGORIES.map((cat) => (
                      <CellView
                        key={cat}
                        testid={`aggregate-cell-${aggCat}-${cat}`}
                        state={resolveAggregateCellState(aggCat, cat, score, signalDirection, scored)}
                      />
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
    </TooltipProvider>
  );
}
