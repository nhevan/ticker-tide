/**
 * "Ask AI" button and response panel for a single timeframe card.
 */

import React from 'react';
import { Button } from '@/components/ui/button';
import { useLlm } from '@/lib/hooks/useLlm';
import { ApiError } from '@/lib/api/client';

interface AskAIProps {
  /** Ticker symbol currently loaded. */
  ticker: string;
  /** Picked date in YYYY-MM-DD format. */
  date: string;
  /** Timeframe for this card. */
  timeframe: 'daily' | 'weekly' | 'monthly';
}

/**
 * Render an "Ask AI" button. On click, fires POST /api/llm and renders the
 * returned analysis text below the button.
 *
 * Shows a friendly error message on 429 (debounce) or 503 (Claude unavailable).
 *
 * @param ticker - Ticker symbol to analyze.
 * @param date - Picked date string.
 * @param timeframe - One of daily/weekly/monthly.
 */
export function AskAI({ ticker, date, timeframe }: AskAIProps) {
  const { mutate, data, error, isPending, reset } = useLlm();

  function handleClick() {
    reset();
    mutate({ ticker, date, timeframe });
  }

  const errorMessage = error
    ? error instanceof ApiError
      ? error.detail
      : error.message
    : null;

  return (
    <div className="mt-3">
      <Button
        variant="outline"
        size="sm"
        onClick={handleClick}
        disabled={isPending}
        className="text-xs"
      >
        {isPending ? 'Analyzing…' : 'Ask AI'}
      </Button>
      {errorMessage && (
        <p className="mt-1 text-xs text-red-600">{errorMessage}</p>
      )}
      {data && (
        <p className="mt-2 whitespace-pre-wrap text-xs leading-relaxed">{data.text}</p>
      )}
    </div>
  );
}
