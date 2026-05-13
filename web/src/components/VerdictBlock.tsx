/**
 * Top-of-dashboard verdict block.
 *
 * Two-column layout: verdict text/button on the left, timeframe summary
 * table on the right. The table is always present (deterministic from the
 * snapshot); the verdict region has three states for a given ticker/date:
 *   - cached: render the stored verdict text (preserved line breaks).
 *   - uncached idle: a "Generate verdict" button that triggers a Claude call.
 *   - generating: a Skeleton placeholder while the POST is in flight.
 *
 * Date/ticker changes are handled by the parent passing new props — the
 * underlying useVerdict query key changes and TanStack automatically
 * swaps to whatever (if anything) is cached for the new key.
 */

import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { TimeframeSummaryTable } from '@/components/TimeframeSummaryTable';
import { ConfidenceBreakdown } from '@/components/ConfidenceBreakdown';
import { useGenerateVerdict, useVerdict } from '@/lib/hooks/useVerdict';
import { ApiError } from '@/lib/api/client';
import type { Snapshot } from '@/lib/api/types';

interface VerdictBlockProps {
  ticker: string;
  date: string;
  snapshot: Snapshot;
}

/**
 * Render the dashboard verdict block for a ticker and date.
 *
 * @param ticker - The currently loaded ticker symbol.
 * @param date - The currently loaded date (YYYY-MM-DD).
 * @param snapshot - Full snapshot used to render the right-side timeframe table.
 */
export function VerdictBlock({ ticker, date, snapshot }: VerdictBlockProps) {
  const { data: cached, isLoading: lookupLoading } = useVerdict(ticker, date);
  const generate = useGenerateVerdict();

  const verdict = cached ?? generate.data ?? null;
  const isGenerating = generate.isPending;
  const errorMessage =
    generate.error instanceof ApiError
      ? generate.error.detail
      : generate.error?.message ?? null;

  const daily = snapshot.daily;
  const showBreakdown =
    daily.confidence_modifiers != null &&
    Number.isFinite(daily.confidence_base) &&
    Number.isFinite(daily.confidence);

  return (
    <div className="mb-4 rounded-lg border bg-card p-4">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:gap-6">
        <div className="min-w-0 flex-1">
          <div className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Verdict
          </div>

          {lookupLoading && (
            <div className="space-y-2">
              <Skeleton className="h-4 w-24" />
              <Skeleton className="h-4 w-3/4" />
            </div>
          )}

          {!lookupLoading && verdict && (
            <pre className="whitespace-pre-wrap break-words font-sans text-sm leading-relaxed text-foreground">
              {verdict.verdict}
            </pre>
          )}

          {!lookupLoading && !verdict && !isGenerating && (
            <Button
              onClick={() => generate.mutate({ ticker, date })}
              disabled={!ticker || !date}
            >
              Generate verdict
            </Button>
          )}

          {isGenerating && (
            <div className="space-y-2">
              <Skeleton className="h-4 w-32" />
              <Skeleton className="h-4 w-2/3" />
              <Skeleton className="h-4 w-1/2" />
            </div>
          )}

          {errorMessage && !isGenerating && !verdict && (
            <p className="mt-2 text-sm text-destructive">{errorMessage}</p>
          )}
        </div>

        <div className="w-full shrink-0 md:w-72">
          <TimeframeSummaryTable snapshot={snapshot} />
        </div>
      </div>

      {showBreakdown && (
        <ConfidenceBreakdown
          confidence={daily.confidence as number}
          base={daily.confidence_base as number}
          modifiers={daily.confidence_modifiers as Record<string, number>}
          dailyScore={daily.daily_score ?? null}
          weeklyScore={daily.weekly_score ?? null}
          trendScore={daily.scores?.trend ?? null}
          volumeScore={daily.scores?.volume ?? null}
          earningsDate={daily.earnings?.next?.date ?? null}
          scoringDate={daily.resolved_period}
          calibratedScore={daily.calibrated_score ?? null}
          indicatorScores={daily.indicator_scores ?? null}
        />
      )}
    </div>
  );
}
