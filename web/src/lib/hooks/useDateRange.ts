/**
 * TanStack Query hook for GET /api/dates.
 *
 * Fetches min/max available dates for a given ticker.
 * The query is disabled when ticker is empty.
 */

import { useQuery } from '@tanstack/react-query';
import { getDateRange } from '@/lib/api/endpoints';
import type { DateRange } from '@/lib/api/types';

/**
 * Return the min/max available date range for a ticker.
 *
 * The query is disabled (idle) when ticker is an empty string.
 *
 * @param ticker - Ticker symbol (e.g. "AAPL").
 */
export function useDateRange(ticker: string) {
  return useQuery<DateRange>({
    queryKey: ['dates', ticker],
    queryFn: () => getDateRange(ticker),
    enabled: ticker.length > 0,
  });
}
