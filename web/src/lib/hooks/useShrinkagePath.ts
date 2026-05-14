/**
 * TanStack Query hook for GET /api/shrinkage-path.
 *
 * Fetches the ridge regression shrinkage path for the given (or latest) scoring
 * date. Data is considered fresh for 5 minutes — path computation is
 * deterministic for a given date and training window, so aggressive re-fetching
 * is unnecessary.
 */

import { useQuery } from '@tanstack/react-query';
import { getShrinkagePath } from '@/lib/api/endpoints';
import type { ShrinkagePathResponse } from '@/lib/api/types';

/**
 * Return the ridge regression shrinkage path from the server.
 *
 * Data is re-fetched automatically after 5 minutes (staleTime: 5 * 60 * 1000).
 * The query is always enabled — shrinkage path data is not user-specific.
 *
 * @param date - Optional ISO date string (YYYY-MM-DD). When omitted, the server
 *               resolves to the latest scoring date in scores_daily.
 */
export function useShrinkagePath(date?: string) {
  return useQuery<ShrinkagePathResponse>({
    queryKey: ['shrinkagePath', date ?? null],
    queryFn: () => getShrinkagePath(date),
    staleTime: 5 * 60 * 1000,
  });
}
