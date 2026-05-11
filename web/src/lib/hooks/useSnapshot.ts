/**
 * TanStack Query hook for GET /api/snapshot.
 *
 * Fetches the three-card snapshot for a given ticker and date.
 * The query is disabled when ticker or date is empty.
 */

import { useQuery } from '@tanstack/react-query';
import { getSnapshot } from '@/lib/api/endpoints';
import type { Snapshot } from '@/lib/api/types';

/**
 * Return the snapshot for the given ticker and date.
 *
 * The query is disabled (idle) when ticker or date is an empty string.
 *
 * @param ticker - Ticker symbol (e.g. "AAPL").
 * @param date - ISO date string (YYYY-MM-DD).
 */
export function useSnapshot(ticker: string, date: string) {
  return useQuery<Snapshot>({
    queryKey: ['snapshot', ticker, date],
    queryFn: () => getSnapshot(ticker, date),
    enabled: ticker.length > 0 && date.length > 0,
  });
}
