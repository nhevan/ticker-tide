/**
 * Top-of-dashboard verdict block.
 *
 * Shows one of three states for a given ticker/date:
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
import { useGenerateVerdict, useVerdict } from '@/lib/hooks/useVerdict';
import { ApiError } from '@/lib/api/client';

interface VerdictBlockProps {
  ticker: string;
  date: string;
}

/**
 * Render the dashboard verdict block for a ticker and date.
 *
 * @param ticker - The currently loaded ticker symbol.
 * @param date - The currently loaded date (YYYY-MM-DD).
 */
export function VerdictBlock({ ticker, date }: VerdictBlockProps) {
  const { data: cached, isLoading: lookupLoading } = useVerdict(ticker, date);
  const generate = useGenerateVerdict();

  const verdict = cached ?? generate.data ?? null;
  const isGenerating = generate.isPending;
  const errorMessage =
    generate.error instanceof ApiError
      ? generate.error.detail
      : generate.error?.message ?? null;

  return (
    <div className="mb-4 rounded-lg border bg-card p-4">
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
  );
}
