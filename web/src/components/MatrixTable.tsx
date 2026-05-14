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

import { useState, useMemo, Fragment, type ReactNode } from 'react';
import { summarizeSectionContributions } from '@/lib/scoring/equationSummary';
import { INDICATOR_CATEGORY_MAP, INDICATOR_DISPLAY_LABELS } from '@/lib/scoring/categoryMap';
import type { Category } from '@/lib/scoring/categoryMap';
import { humanizePatternName } from '@/lib/scoring/patternLabels';
import type { CategoryScores, Pattern, Snapshot, ScoringRules } from '@/lib/api/types';
import type { DailyCategory, WeeklyCategory, MonthlyCategory } from '@/lib/api/types';
import {
  IndicatorExplainerPanel,
  INDICATORS_WITH_EXPLAINER,
} from '@/components/IndicatorExplainerPanel';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface MatrixTableProps {
  /** Section title displayed above the table. */
  title: string;
  /**
   * Optional pre-computed contribution for this timeframe section header.
   * When provided and both weight and score are finite, renders the Variant C
   * math chain: "60% × +20.5 = ▲ 12.3" next to the title.
   * Hidden when null, undefined, or when score is non-finite.
   */
  headerContribution?: { weight: number; score: number } | null;
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
  /**
   * Full snapshot object — passed to IndicatorExplainerPanel when a row is
   * expanded. Optional; explainer panel is suppressed when not provided.
   */
  snapshot?: Snapshot;
  /**
   * Scoring rules from /api/scoring-rules — passed to IndicatorExplainerPanel.
   * Optional; panel renders with limited info when undefined.
   */
  scoringRules?: ScoringRules;
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
 * Resolve a humanized label for a contribution-payload item name.
 * Indicators use INDICATOR_DISPLAY_LABELS; aggregates (sentiment/fundamental/macro)
 * are capitalized; everything else is treated as a pattern key and humanized.
 */
function prettyContributionLabel(name: string): string {
  if (name in INDICATOR_DISPLAY_LABELS) return INDICATOR_DISPLAY_LABELS[name];
  if (name === 'sentiment' || name === 'fundamental' || name === 'macro') {
    return name.charAt(0).toUpperCase() + name.slice(1);
  }
  return humanizePatternName(name);
}

/**
 * Categories with no indicator/pattern constituents in the matrix. Rendered
 * as aggregate rows showing the category's rollup score directly.
 */
const AGGREGATE_CATEGORIES: Array<'sentiment' | 'fundamental' | 'macro'> = [
  'sentiment', 'fundamental', 'macro',
];


/**
 * Render the Variant C header math chain for a timeframe section title.
 *
 * Format: "60% × +20.5 = ▲ 12.3"
 * Zero contribution renders muted "0.0" with no directional glyph.
 * Returns null when contribution is null/undefined or when score is non-finite.
 *
 * @param contribution - Weight/score pair from computeTimeframeHeaderContributions.
 * @returns ReactNode with the formatted math chain, or null.
 */
function renderHeaderContribution(
  contribution: { weight: number; score: number } | null | undefined,
): ReactNode {
  if (!contribution) return null;
  const { weight, score } = contribution;
  if (!Number.isFinite(score) || !Number.isFinite(weight)) return null;

  const value = weight * score;
  const weightPct = `${Math.round(weight * 100)}%`;
  const scoreSign = score >= 0 ? '+' : '−';
  const scoreAbs = Math.abs(score).toFixed(1);

  if (value === 0) {
    return (
      <span className="ml-2 text-[11px] font-normal text-muted-foreground tabular-nums">
        {weightPct} × <span className="text-foreground">{scoreSign}{scoreAbs}</span> ={' '}
        <span className="text-muted-foreground">0.0</span>
      </span>
    );
  }

  const glyph = value > 0 ? '▲' : '▼';
  const tone = value > 0 ? 'text-[hsl(var(--up))]' : 'text-[hsl(var(--down))]';
  const mag = Math.abs(value).toFixed(1);

  return (
    <span className="ml-2 text-[11px] font-normal text-muted-foreground tabular-nums">
      {weightPct} × <span className="text-foreground">{scoreSign}{scoreAbs}</span> ={' '}
      <span className={`${tone} font-semibold`}>{glyph} {mag}</span>
    </span>
  );
}

/**
 * Format a signed composite-point contribution for display in a matrix cell.
 *
 * Renders Variant C format: directional glyph (▲/▼) + unsigned magnitude to
 * 1 decimal place. Zero renders as muted "0.0" with no glyph. Called for
 * indicator and aggregate rows at the daily timeframe; the caller is responsible
 * for not invoking this on pattern rows.
 *
 * @param value - The contribution value from contributions_payload.items[].contribution.
 * @returns A ReactNode with the formatted contribution.
 */
function formatContribution(value: number): ReactNode {
  if (value === 0) return <span className="text-muted-foreground">0.0</span>;
  const glyph = value > 0 ? '▲' : '▼';
  const magnitude = Math.abs(value).toFixed(1);
  return (
    <span className="tabular-nums">
      <span className={value > 0 ? 'text-[hsl(var(--up))]' : 'text-[hsl(var(--down))]'}>{glyph}</span>
      {' '}
      <span className="font-semibold">{magnitude}</span>
    </span>
  );
}

type CellTone = 'green' | 'red' | 'grey';

/** Tailwind background class for each tone. */
const TONE_CLASS: Record<CellTone, string> = {
  green: 'bg-[hsl(var(--up)/0.18)]',
  red: 'bg-[hsl(var(--down)/0.18)]',
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
  /**
   * Optional composite-point contribution from contributions_payload.items[].
   * Passed for indicator and aggregate rows at the daily timeframe. When present
   * and the cell state is valid with no text, the formatted contribution is shown
   * in place of the empty cell. Pattern rows never receive this prop.
   */
  contribution?: number;
}

/** Render a single matrix cell from its resolved state. */
function CellView({ state, testid, contribution }: CellViewProps) {
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
  const showContribution = contribution !== undefined && !state.text;
  return (
    <td
      className={`py-1 px-1 text-center ${TONE_CLASS[state.tone]}`}
      data-testid={testid}
      data-tone={state.tone}
    >
      {showContribution
        ? formatContribution(contribution)
        : state.text || <>&nbsp;</>}
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
  snapshot,
  scoringRules,
  headerContribution,
}: MatrixTableProps) {
  const [expandedIndicator, setExpandedIndicator] = useState<string | null>(null);

  const hasData =
    (indicators !== undefined && Object.keys(indicators).length > 0) ||
    (indicatorScores !== undefined && Object.keys(indicatorScores).length > 0);

  const scored = new Set<string>(categories as readonly string[]);

  /**
   * Map of name → composite-point contribution, built from the timeframe's
   * contributions_payload.items. Dispatches on timeframe:
   *   'daily'  → snapshot.daily.contributions_payload
   *   'weekly' → snapshot.weekly.contributions_payload
   *   else     → empty Map (monthly stays gated — no payload wired yet)
   * Accepts items with kind === 'indicator' or kind === 'aggregate'; both land
   * in the same Map keyed by item.name. Aggregate names (sentiment/fundamental/
   * macro) are guaranteed not to collide with indicator names — categoryMap is
   * the source of truth.
   * Returns an empty Map when the payload is absent or for un-wired timeframes.
   */
  const contributionsByName = useMemo(() => {
    let items;
    if (timeframe === 'daily') {
      items = snapshot?.daily?.contributions_payload?.items ?? [];
    } else if (timeframe === 'weekly') {
      items = snapshot?.weekly?.contributions_payload?.items ?? [];
    } else {
      return new Map<string, number>();
    }
    const map = new Map<string, number>();
    for (const item of items) {
      if (item.kind !== 'indicator' && item.kind !== 'aggregate') continue;
      if (!Number.isFinite(item.contribution)) continue;
      map.set(item.name, item.contribution);
    }
    return map;
  }, [snapshot?.daily?.contributions_payload, snapshot?.weekly?.contributions_payload, timeframe]);

  /**
   * Section equation data: all contribution items + total.
   * Dispatches on timeframe — does NOT use bracket notation on snapshot.
   */
  const sectionEquationData = useMemo(() => {
    if (!Number.isFinite(headerContribution?.score ?? null)) return null;
    const target = headerContribution?.score ?? null;
    let items;
    if (timeframe === 'daily') {
      items = snapshot?.daily?.contributions_payload?.items;
    } else if (timeframe === 'weekly') {
      items = snapshot?.weekly?.contributions_payload?.items;
    } else {
      items = snapshot?.monthly?.contributions_payload?.items;
    }
    return summarizeSectionContributions(items, target);
  }, [
    snapshot?.daily?.contributions_payload,
    snapshot?.weekly?.contributions_payload,
    snapshot?.monthly?.contributions_payload,
    headerContribution,
    timeframe,
  ]);

  /** Toggle the explainer panel for an indicator row. Same row collapses; different row replaces.
   *  Indicators without a real explainer (i.e. not in INDICATORS_WITH_EXPLAINER) are no-ops at
   *  the function boundary — belt-and-suspenders even though the JSX also gates the onClick. */
  function handleIndicatorClick(indicatorKey: string): void {
    if (!INDICATORS_WITH_EXPLAINER.has(indicatorKey)) return;
    setExpandedIndicator((prev) => (prev === indicatorKey ? null : indicatorKey));
  }

  /** Total column count (label + value + 9 category columns). */
  const totalColSpan = 2 + ALL_CATEGORIES.length;

  return (
    <TooltipProvider delayDuration={150}>
    <details className="group rounded-lg border">
      <summary className="flex cursor-pointer items-center justify-between gap-3 px-4 py-3 select-none [&::-webkit-details-marker]:hidden">
        <h3 className="flex-1 text-sm font-semibold text-foreground m-0">
          {title}
          {renderHeaderContribution(headerContribution)}
        </h3>
        <span className="text-muted-foreground transition-transform group-open:rotate-90">›</span>
      </summary>
      <div className="border-t p-4">
      {sectionEquationData && (
        <div className="mb-3 text-[11px] text-muted-foreground leading-relaxed tabular-nums">
          {sectionEquationData.items.map((item, idx) => {
            const tone =
              item.value > 0
                ? 'text-[hsl(var(--up))]'
                : item.value < 0
                  ? 'text-[hsl(var(--down))]'
                  : 'text-muted-foreground';
            const mag = Math.abs(item.value).toFixed(1);
            const sign = item.value < 0 ? '−' : '+';
            const prettyLabel = prettyContributionLabel(item.label);
            return (
              <Fragment key={item.label}>
                {idx === 0 ? (
                  <span className={`${tone} font-semibold`}>
                    {sign}
                    {mag}
                  </span>
                ) : (
                  <>
                    <span className="mx-1 text-muted-foreground">{sign}</span>
                    <span className={`${tone} font-semibold`}>{mag}</span>
                  </>
                )}
                <span className="ml-1 text-[10px] text-muted-foreground">{prettyLabel}</span>
              </Fragment>
            );
          })}
          <span className="mx-2">≈</span>
          <span className="font-semibold">
            {sectionEquationData.total >= 0 ? '+' : '−'}
            {Math.abs(sectionEquationData.total).toFixed(1)}
          </span>
        </div>
      )}

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
                const isExpanded = expandedIndicator === indicatorKey;
                const hasExplainer = INDICATORS_WITH_EXPLAINER.has(indicatorKey);
                const labelText = INDICATOR_DISPLAY_LABELS[indicatorKey] ?? indicatorKey;
                return (
                  <Fragment key={indicatorKey}>
                    <tr className="border-t border-border/40 hover:bg-muted/40">
                      {hasExplainer ? (
                        <td
                          className="py-1 pr-3 text-left text-foreground cursor-pointer underline-offset-4 hover:underline"
                          onClick={() => handleIndicatorClick(indicatorKey)}
                          role="button"
                          tabIndex={0}
                          aria-expanded={isExpanded}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              handleIndicatorClick(indicatorKey);
                            }
                          }}
                        >
                          {labelText}
                          <svg
                            aria-hidden="true"
                            viewBox="0 0 24 24"
                            width="10"
                            height="10"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2.5"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className={`inline-block flex-shrink-0 ml-1.5 align-[-1px] text-muted-foreground transition-transform duration-200 ${isExpanded ? 'rotate-90' : ''}`}
                          >
                            <polyline points="9 6 15 12 9 18" />
                          </svg>
                        </td>
                      ) : (
                        <td className="py-1 pr-3 text-left text-foreground">
                          {labelText}
                        </td>
                      )}
                      <td className="py-1 pr-3 text-right tabular-nums text-muted-foreground">
                        {formatValue(rawValue)}
                      </td>
                      {ALL_CATEGORIES.map((cat) => (
                        <CellView
                          key={cat}
                          testid={`cell-${indicatorKey}-${cat}`}
                          state={resolveIndicatorCellState(indicatorKey, cat, score, signalDirection, scored)}
                          contribution={contributionsByName.get(indicatorKey)}
                        />
                      ))}
                    </tr>
                    {isExpanded && snapshot && (
                      <tr>
                        <td colSpan={totalColSpan} className="p-0">
                          <IndicatorExplainerPanel
                            indicator={indicatorKey}
                            snapshot={snapshot}
                            rules={scoringRules}
                          />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}

              {PATTERN_CATEGORIES.flatMap((patternCat) => {
                if (!scored.has(patternCat)) return [];
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

              {AGGREGATE_CATEGORIES.filter((aggCat) => scored.has(aggCat)).map((aggCat) => {
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
                        contribution={contributionsByName.get(aggCat)}
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
    </details>
    </TooltipProvider>
  );
}
