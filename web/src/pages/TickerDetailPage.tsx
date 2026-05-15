/**
 * Ticker Detail page — page-scoped ticker/date controls row and three
 * timeframe cards. The global Header (brand, nav, theme, sign-out) is
 * rendered above; ticker/date pickers live in a page-local sub-bar
 * because future pages will not need them.
 *
 * State is driven by URL search params (ticker, date) so the page is
 * bookmarkable. Loads /api/snapshot when the user clicks Load.
 * Fetches /api/dates when ticker changes to constrain the date picker.
 */

import React, { useEffect, useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Header } from '@/components/Header';
import { Button } from '@/components/ui/button';
import { TickerPicker } from '@/components/TickerPicker';
import { DatePicker } from '@/components/DatePicker';
import { ErrorBanner } from '@/components/ErrorBanner';
import { VerdictBlock } from '@/components/VerdictBlock';
import { PriceChart } from '@/components/PriceChart';
import { MatrixTable } from '@/components/MatrixTable';
import { ModelInputsTable } from '@/components/ModelInputsTable';
import { TickerTape } from '@/components/TickerTape';
import { Skeleton } from '@/components/ui/skeleton';
import { useTickers } from '@/lib/hooks/useTickers';
import { useDateRange } from '@/lib/hooks/useDateRange';
import { useSnapshot } from '@/lib/hooks/useSnapshot';
import { useScoringRules } from '@/lib/hooks/useScoringRules';
import { ApiError } from '@/lib/api/client';
import { computeTimeframeHeaderContributions } from '@/lib/scoring/timeframeHeaderContribution';
import { summarizeCrossSection } from '@/lib/scoring/equationSummary';

/**
 * Map a numeric composite score to a numeric direction for the indicator matrix.
 *
 * @param score - Composite score from a TimeframeSection.
 * @returns 1 if positive, -1 if negative, 0 if zero or absent.
 */
function scoreToDirection(score: number | null | undefined): 1 | -1 | 0 {
  const direction = Math.sign(score ?? 0);
  return direction as 1 | -1 | 0;
}

/**
 * Render the main dashboard with ticker/date controls and three timeframe cards.
 *
 * URL search params:
 *   ?ticker=AAPL&date=2026-04-25
 *
 * The snapshot is fetched only after the user clicks Load.
 */
export function TickerDetailPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [inputTicker, setInputTicker] = useState(
    searchParams.get('ticker') ?? '',
  );
  const [inputDate, setInputDate] = useState(searchParams.get('date') ?? '');

  // Committed ticker/date drive the actual query (set on Load click).
  const [loadedTicker, setLoadedTicker] = useState(
    searchParams.get('ticker') ?? '',
  );
  const [loadedDate, setLoadedDate] = useState(searchParams.get('date') ?? '');

  const { data: tickers = [] } = useTickers();
  const { data: dateRange } = useDateRange(inputTicker);

  // When date range loads, auto-fill date to max if current date is outside bounds.
  useEffect(() => {
    if (!dateRange?.max) return;
    if (!inputDate || inputDate < (dateRange.min ?? '') || inputDate > dateRange.max) {
      setInputDate(dateRange.max);
    }
  }, [dateRange, inputTicker]);

  const {
    data: snapshot,
    isLoading: snapshotLoading,
    error: snapshotError,
  } = useSnapshot(loadedTicker, loadedDate);

  const { data: scoringRules } = useScoringRules();

  /**
   * Compute redistributed per-timeframe header contributions once per
   * render whenever snapshot or scoringRules changes. Each entry holds
   * the weight and pre-blend score for that timeframe's section header.
   * All nulls when regime is absent, scoringRules is loading, or a
   * timeframe has no finite score.
   */
  const headerContributions = useMemo(
    () =>
      snapshot
        ? computeTimeframeHeaderContributions(snapshot, scoringRules)
        : { daily: null, weekly: null, monthly: null },
    [snapshot, scoringRules],
  );

  /**
   * Cross-section banner data: per-timeframe (weight × score) parts and total.
   * Uses ≈ because the sum of parts does not exactly equal final_score due to
   * Python-side clamping at scoring time.
   * Null when all headerContributions entries are null/non-finite.
   */
  const crossSectionData = useMemo(
    () => summarizeCrossSection(headerContributions),
    [headerContributions],
  );

  function handleLoad() {
    if (!inputTicker || !inputDate) return;
    setLoadedTicker(inputTicker);
    setLoadedDate(inputDate);
    setSearchParams({ ticker: inputTicker, date: inputDate });
  }

  const errorMessage = snapshotError
    ? snapshotError instanceof ApiError
      ? snapshotError.detail
      : snapshotError.message
    : null;

  const showCards = Boolean(snapshot);

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <div className="border-b bg-background px-4 py-3">
        <div className="flex flex-wrap items-end gap-3">
          <TickerPicker
            value={inputTicker}
            onChange={setInputTicker}
            tickers={tickers}
          />
          <DatePicker
            value={inputDate}
            onChange={setInputDate}
            min={dateRange?.min}
            max={dateRange?.max}
          />
          <Button
            onClick={handleLoad}
            disabled={snapshotLoading || !inputTicker || !inputDate}
            size="sm"
          >
            {snapshotLoading ? 'Loading…' : 'Load'}
          </Button>
        </div>
      </div>
      <TickerTape
        ticker={loadedTicker}
        snapshot={snapshot}
        isLoading={snapshotLoading}
        error={errorMessage}
      />

      <main className="mx-auto max-w-6xl px-4 py-6">
        {errorMessage && (
          <div className="mb-4">
            <ErrorBanner message={errorMessage} />
          </div>
        )}

        {snapshotLoading && (
          <div className="grid gap-4 md:grid-cols-3">
            {[0, 1, 2].map((i) => (
              <div key={i} className="space-y-3 rounded-lg border p-4">
                <Skeleton className="h-5 w-20" />
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-3/4" />
                <Skeleton className="h-16 w-full" />
              </div>
            ))}
          </div>
        )}

        {showCards && snapshot && !snapshotLoading && (
          <>
            <VerdictBlock
              ticker={loadedTicker}
              date={loadedDate}
              snapshot={snapshot}
            />
            <div className="mb-4">
              <PriceChart ticker={loadedTicker} />
            </div>
            <div className="mb-4">
              <ModelInputsTable
                payload={snapshot.daily.calibrator_payload}
                signal={snapshot.daily.signal}
              />
            </div>
            <div className="mb-4 space-y-4">
              {crossSectionData && (
                <details className="group rounded-lg border" open>
                  <summary className="flex cursor-pointer items-center justify-between gap-3 px-4 py-3 select-none [&::-webkit-details-marker]:hidden">
                    <h3 className="flex-1 text-sm font-semibold text-foreground m-0">
                      Raw Data Input and Decision
                    </h3>
                    <span className="text-muted-foreground transition-transform group-open:rotate-90">›</span>
                  </summary>
                  <div className="border-t p-4 text-[11px] text-muted-foreground tabular-nums">
                  {crossSectionData.parts.map((part, idx) => {
                    const tone =
                      part.value > 0
                        ? 'text-[hsl(var(--up))]'
                        : part.value < 0
                          ? 'text-[hsl(var(--down))]'
                          : 'text-muted-foreground';
                    const mag = Math.abs(part.value).toFixed(1);
                    const sign = part.value < 0 ? '−' : '+';
                    return (
                      <span key={part.label}>
                        {idx === 0 ? (
                          <span className={`${tone} font-semibold`}>
                            {sign}
                            {mag}
                          </span>
                        ) : (
                          <>
                            <span className="mx-1.5">{sign}</span>
                            <span className={`${tone} font-semibold`}>{mag}</span>
                          </>
                        )}
                        <span className="text-[10px] text-muted-foreground ml-0.5">
                          ({part.label})
                        </span>
                      </span>
                    );
                  })}
                  <span className="mx-2">≈</span>
                  <span className="font-semibold">
                    {crossSectionData.total >= 0 ? '+' : '−'}
                    {Math.abs(crossSectionData.total).toFixed(1)}
                  </span>
                  <span className="ml-2 text-[10px] text-muted-foreground">(final blended)</span>

                  <DirectionBreakdown
                    compositeScore={snapshot.daily.composite_score ?? null}
                    regime={snapshot.daily.regime ?? null}
                    rawThresholds={scoringRules?.signal_thresholds_raw}
                  />

                  <ConfidenceBreakdown
                    compositeScore={snapshot.daily.composite_score ?? null}
                    confidenceModifiers={snapshot.daily.confidence_modifiers ?? null}
                    coldStartMultiplier={scoringRules?.cold_start_base_multiplier}
                    coldStartMax={scoringRules?.cold_start_max}
                  />
                  </div>
                </details>
              )}
              <MatrixTable
                title="Daily — Indicator Agreement"
                indicators={snapshot.daily.indicators}
                indicatorScores={snapshot.daily.indicator_scores}
                signalDirection={scoreToDirection(snapshot.daily.composite_score)}
                categories={snapshot.daily.categories}
                timeframe="daily"
                recentPatterns={snapshot.daily.recent_patterns}
                categoryScores={snapshot.daily.scores}
                snapshot={snapshot}
                scoringRules={scoringRules}
                headerContribution={headerContributions.daily}
              />
              <MatrixTable
                title="Weekly — Indicator Agreement"
                indicators={snapshot.weekly.indicators}
                indicatorScores={snapshot.weekly.indicator_scores}
                signalDirection={scoreToDirection(snapshot.weekly.composite_score)}
                categories={snapshot.weekly.categories}
                timeframe="weekly"
                recentPatterns={snapshot.weekly.recent_patterns}
                categoryScores={snapshot.weekly.scores}
                snapshot={snapshot}
                headerContribution={headerContributions.weekly}
              />
              <MatrixTable
                title="Monthly — Indicator Agreement"
                indicators={snapshot.monthly.indicators}
                indicatorScores={snapshot.monthly.indicator_scores}
                signalDirection={scoreToDirection(snapshot.monthly.composite_score)}
                categories={snapshot.monthly.categories}
                timeframe="monthly"
                recentPatterns={snapshot.monthly.recent_patterns}
                categoryScores={snapshot.monthly.scores}
                snapshot={snapshot}
                headerContribution={headerContributions.monthly}
              />
            </div>
          </>
        )}

        {!snapshotLoading && !showCards && !errorMessage && (
          <p className="text-center text-sm text-muted-foreground">
            Select a ticker and date, then click Load.
          </p>
        )}
      </main>
    </div>
  );
}

const MODIFIER_LABELS: Record<string, string> = {
  timeframe_agreement: 'Timeframe agreement',
  volume_confirmation: 'Volume confirmation',
  indicator_consensus: 'Indicator consensus',
  earnings_proximity: 'Earnings proximity',
  vix_extreme: 'VIX extreme',
  atr_expanding: 'ATR expanding',
  missing_data: 'Missing data',
};

function formatSigned(value: number, digits = 1): string {
  const sign = value < 0 ? '−' : '+';
  return `${sign}${Math.abs(value).toFixed(digits)}`;
}

function toneFor(value: number): string {
  if (value > 0) return 'text-[hsl(var(--up))]';
  if (value < 0) return 'text-[hsl(var(--down))]';
  return 'text-muted-foreground';
}

// exported for tests
export function DirectionBreakdown(props: {
  compositeScore: number | null;
  regime: string | null | undefined;
  rawThresholds: Record<string, { bullish: number; bearish: number; n: number }> | undefined;
}) {
  const { compositeScore, regime, rawThresholds } = props;

  if (!Number.isFinite(compositeScore) || compositeScore === null) {
    return null;
  }

  // Priority chain:
  // 1. regime-keyed entry when regime is known and the key exists
  // 2. "all" cross-regime fallback
  // 3. no display (return null)
  let resolvedEntry: { bullish: number; bearish: number; n: number } | null = null;
  let resolvedKey: string | null = null;

  if (regime != null && rawThresholds?.[regime] != null) {
    resolvedEntry = rawThresholds[regime];
    resolvedKey = regime;
  } else if (rawThresholds?.["all"] != null) {
    resolvedEntry = rawThresholds["all"];
    resolvedKey = "all";
  }

  if (resolvedEntry === null || resolvedKey === null) {
    return null;
  }

  const { bullish: bullishThreshold, bearish: bearishThreshold, n: sampleSize } = resolvedEntry;

  const caption =
    resolvedKey === "all"
      ? `Regime IQR thresholds — approx. p25/p75 of final_score (all rows, n=${sampleSize.toLocaleString()})`
      : `Regime IQR thresholds — approx. p25/p75 of final_score (${resolvedKey}, n=${sampleSize.toLocaleString()})`;

  const tone = toneFor(compositeScore);
  const rawSignal =
    compositeScore >= bullishThreshold
      ? 'BULLISH'
      : compositeScore <= bearishThreshold
        ? 'BEARISH'
        : 'NEUTRAL';
  const comparator =
    compositeScore >= bullishThreshold ? '≥' : compositeScore <= bearishThreshold ? '≤' : 'in';
  const threshold =
    compositeScore >= bullishThreshold
      ? formatSigned(bullishThreshold)
      : compositeScore <= bearishThreshold
        ? formatSigned(bearishThreshold)
        : `[${formatSigned(bearishThreshold)}, ${formatSigned(bullishThreshold)}]`;

  // Per-regime threshold rows displayed in the right-hand table. "all" sits
  // last so it visually reads as the cross-regime fallback.
  const regimeOrder = ['ranging', 'trending', 'volatile', 'all'];
  const regimeEntries = regimeOrder
    .map((key) => ({ key, entry: rawThresholds?.[key] }))
    .filter((row): row is { key: string; entry: { bullish: number; bearish: number; n: number } } => row.entry != null);

  return (
    <div className="mt-4 pt-3 border-t border-border/60">
      <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-foreground">
        Direction decision
      </div>
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        {/* Left: math chain */}
        <div className="flex-1 text-[11px] leading-relaxed">
          <div>
            <span className="text-muted-foreground">Raw composite score</span>
            <span className="mx-1.5">=</span>
            <span className={`${tone} font-semibold`}>{formatSigned(compositeScore)}</span>
          </div>
          <div className="mt-0.5">
            <span className="text-muted-foreground">Thresholds</span>
            <span className="mx-1.5">·</span>
            <span className="text-[hsl(var(--up))] font-semibold">{formatSigned(bullishThreshold)}</span>
            <span className="mx-1 text-muted-foreground">bullish</span>
            <span className="mx-1.5 text-muted-foreground">/</span>
            <span className="text-[hsl(var(--down))] font-semibold">{formatSigned(bearishThreshold)}</span>
            <span className="mx-1 text-muted-foreground">bearish</span>
          </div>
          <div className="mt-1">
            <span className={`${tone} font-semibold`}>{formatSigned(compositeScore)}</span>
            <span className="mx-1.5">{comparator}</span>
            <span className="font-semibold">{threshold}</span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="text-muted-foreground">Raw-data signal:</span>
            <span className={`ml-1 ${tone} font-semibold uppercase`}>{rawSignal}</span>
          </div>
          <div className="mt-1 text-[10px] text-muted-foreground italic">
            {caption}
          </div>
        </div>

        {/* Right: regime threshold table — active regime highlighted with muted bg + ▸ marker */}
        <div className="md:w-[260px] md:shrink-0">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Thresholds by regime
          </div>
          <div className="overflow-hidden rounded-md border border-border/60 text-[10px] tabular-nums">
            <div className="grid grid-cols-[1fr_auto_auto] gap-x-3 bg-muted/40 px-2 py-1 font-semibold text-muted-foreground">
              <span>regime</span>
              <span className="text-right">bullish</span>
              <span className="text-right">bearish</span>
            </div>
            {regimeEntries.map(({ key, entry }) => {
              const active = key === resolvedKey;
              return (
                <div
                  key={key}
                  className={`grid grid-cols-[1fr_auto_auto] gap-x-3 px-2 py-1 ${
                    active ? 'bg-muted/60 font-semibold text-foreground' : 'text-muted-foreground'
                  }`}
                >
                  <span>
                    {active && <span className="mr-1 text-[hsl(var(--up))]">▸</span>}
                    {key}
                    <span className="ml-1 text-[9px] opacity-60">n={entry.n.toLocaleString()}</span>
                  </span>
                  <span className="text-right text-[hsl(var(--up))]">{formatSigned(entry.bullish)}</span>
                  <span className="text-right text-[hsl(var(--down))]">{formatSigned(entry.bearish)}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

// exported for tests
export function ConfidenceBreakdown(props: {
  compositeScore: number | null;
  confidenceModifiers: Record<string, number> | null;
  coldStartMultiplier?: number;
  coldStartMax?: number;
}) {
  const { compositeScore, confidenceModifiers, coldStartMultiplier, coldStartMax } = props;

  if (
    compositeScore === null ||
    compositeScore === undefined ||
    !Number.isFinite(compositeScore)
  ) {
    return null;
  }

  const multiplier = coldStartMultiplier ?? 0.3;
  const base = Math.abs(compositeScore) * multiplier;
  const modifierEntries = Object.entries(confidenceModifiers ?? {}).filter(
    ([, value]) => Number.isFinite(value) && value !== 0,
  );
  const modifierSum = modifierEntries.reduce((acc, [, value]) => acc + value, 0);
  const preClamp = base + modifierSum;
  const rawConfidence = Math.max(0, Math.min(100, preClamp));

  return (
    <div className="mt-4 pt-3 border-t border-border/60">
      <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-foreground">
        Confidence decision
      </div>
      <div className="text-[11px] leading-relaxed">
        <div>
          <span className="text-muted-foreground">Raw-data base</span>
          <span className="mx-1.5">=</span>
          <span className="text-muted-foreground">
            |{formatSigned(compositeScore)}| × {multiplier}
          </span>
          <span className="mx-1.5">=</span>
          <span className="font-semibold">{base.toFixed(1)}</span>
        </div>

        {modifierEntries.length === 0 ? (
          <div className="mt-1 text-muted-foreground">No modifiers applied.</div>
        ) : (
          <div className="mt-1.5">
            <div className="mb-0.5 text-muted-foreground">Modifiers</div>
            <ul className="ml-3 space-y-0.5">
              {modifierEntries.map(([key, value]) => (
                <li key={key} className="flex items-baseline gap-2">
                  <span className="text-muted-foreground">
                    {MODIFIER_LABELS[key] ?? key}
                  </span>
                  <span className={`${toneFor(value)} font-semibold`}>
                    {formatSigned(value)}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}

        <div className="mt-1.5">
          <span className="text-muted-foreground">Total</span>
          <span className="mx-1.5">=</span>
          <span className="font-semibold">{base.toFixed(1)}</span>
          <span className="mx-1">+</span>
          <span className={`${toneFor(modifierSum)} font-semibold`}>
            {formatSigned(modifierSum)}
          </span>
          <span className="mx-1.5">=</span>
          <span className="font-semibold">{preClamp.toFixed(1)}</span>
          <span className="mx-2 text-muted-foreground">→ clamp[0, 100] →</span>
          <span className="text-muted-foreground">Raw-data confidence:</span>
          <span className="ml-1 font-semibold">{Math.round(rawConfidence)}%</span>
        </div>

        {Number.isFinite(coldStartMax) && coldStartMax !== undefined && (
          <div className="mt-1 text-[10px] text-muted-foreground">
            Theoretical maximum ≈ {coldStartMax}%
          </div>
        )}

        <div className="mt-2 text-[10px] text-muted-foreground italic">
          What raw data alone would decide. The live verdict may differ when
          the calibrator is active.
        </div>
      </div>
    </div>
  );
}
