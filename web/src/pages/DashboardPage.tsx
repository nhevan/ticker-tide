/**
 * Dashboard page — ticker/date selector and three timeframe cards.
 *
 * State is driven by URL search params (ticker, date) so the page is
 * bookmarkable. Loads /api/snapshot when the user clicks Load.
 * Fetches /api/dates when ticker changes to constrain the date picker.
 */

import React, { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Header } from '@/components/Header';
import { TimeframeCard } from '@/components/TimeframeCard';
import { ErrorBanner } from '@/components/ErrorBanner';
import { VerdictBlock } from '@/components/VerdictBlock';
import { Skeleton } from '@/components/ui/skeleton';
import { useTickers } from '@/lib/hooks/useTickers';
import { useDateRange } from '@/lib/hooks/useDateRange';
import { useSnapshot } from '@/lib/hooks/useSnapshot';
import { ApiError } from '@/lib/api/client';

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
            <VerdictBlock ticker={loadedTicker} date={loadedDate} />
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
