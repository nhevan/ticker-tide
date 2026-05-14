/**
 * Tickers page — renders a dense, sortable, paginated table of all
 * active tickers, fed by GET /api/tickers-list via useTickersList().
 */

import React from 'react';
import { Header } from '@/components/Header';
import { TickersTable } from '@/components/TickersTable';
import { ErrorBanner } from '@/components/ErrorBanner';
import { Skeleton } from '@/components/ui/skeleton';
import { useTickersList } from '@/lib/hooks/useTickersList';

/**
 * Render the Tickers page: data fetch + loading/error states + the
 * sortable, paginated table.
 */
export function TickersPage() {
  const { data, isLoading, isError, error } = useTickersList();

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Header />
      <main className="space-y-4 px-4 py-6">
        {isLoading && (
          <section className="rounded-lg border bg-card p-4 space-y-2">
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-3 w-full" />
            <Skeleton className="h-3 w-full" />
            <Skeleton className="h-3 w-full" />
            <Skeleton className="h-3 w-2/3" />
          </section>
        )}
        {isError && (
          <ErrorBanner
            message={
              error instanceof Error
                ? error.message
                : 'Failed to load tickers list.'
            }
          />
        )}
        {data && (
          <section className="rounded-lg border bg-card">
            <TickersTable rows={data} />
          </section>
        )}
      </main>
    </div>
  );
}
