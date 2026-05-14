/**
 * TanStack Query hook for GET /api/tickers-list.
 *
 * Returns one summary row per active ticker for the Tickers listing page.
 */

import { useQuery } from '@tanstack/react-query';
import { getTickersList } from '@/lib/api/endpoints';
import type { TickerRow } from '@/lib/api/types';

/**
 * Fetch the list of ticker summary rows from the API.
 */
export function useTickersList() {
  return useQuery<TickerRow[]>({
    queryKey: ['tickers-list'],
    queryFn: getTickersList,
  });
}
