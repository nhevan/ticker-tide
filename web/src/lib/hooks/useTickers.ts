/**
 * TanStack Query hook for GET /api/tickers.
 *
 * Fetches the alphabetized list of active ticker symbols.
 */

import { useQuery } from '@tanstack/react-query';
import { getTickers } from '@/lib/api/endpoints';

/**
 * Return the list of active ticker symbols from the API.
 */
export function useTickers() {
  return useQuery<string[]>({
    queryKey: ['tickers'],
    queryFn: getTickers,
  });
}
