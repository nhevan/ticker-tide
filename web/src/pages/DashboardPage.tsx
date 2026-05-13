/**
 * Dashboard page — ticker/date selector and three timeframe cards.
 *
 * State is driven by URL search params (ticker, date) so the page is
 * bookmarkable. Loads /api/snapshot when the user clicks Load.
 * Fetches /api/dates when ticker changes to constrain the date picker.
 */

import React, { useEffect, useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Header } from '@/components/Header';
import { TimeframeCard } from '@/components/TimeframeCard';
import { ErrorBanner } from '@/components/ErrorBanner';
import { VerdictBlock } from '@/components/VerdictBlock';
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
export function DashboardPage() {
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
      <Header
        ticker={inputTicker}
        onTickerChange={setInputTicker}
        date={inputDate}
        onDateChange={setInputDate}
        onLoad={handleLoad}
        isLoading={snapshotLoading}
        tickers={tickers}
        minDate={dateRange?.min}
        maxDate={dateRange?.max}
      />
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
              <ModelInputsTable payload={snapshot.daily.calibrator_payload} />
            </div>
            <div className="mb-4 space-y-4">
              {crossSectionData && (
                <div className="rounded-lg border border-border/60 bg-muted/20 px-3 py-2 text-[11px] text-muted-foreground tabular-nums">
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
                </div>
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
                headerContribution={headerContributions.monthly}
              />
            </div>
            <div className="grid gap-4 md:grid-cols-3">
                <TimeframeCard
                title="Daily"
                timeframe="daily"
                section={snapshot.daily}
                ticker={loadedTicker}
                date={loadedDate}
                isLoading={false}
              />
              <TimeframeCard
                title="Weekly"
                timeframe="weekly"
                section={snapshot.weekly}
                ticker={loadedTicker}
                date={loadedDate}
                isLoading={false}
              />
              <TimeframeCard
                title="Monthly"
                timeframe="monthly"
                section={snapshot.monthly}
                ticker={loadedTicker}
                date={loadedDate}
                isLoading={false}
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
