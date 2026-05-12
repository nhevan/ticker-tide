/**
 * TanStack Query hook for GET /api/scoring-rules.
 *
 * The scoring rules are process-static (constant for the server's lifetime),
 * so staleTime is set to Infinity — the data is fetched once and never
 * re-fetched automatically.
 */

import { useQuery } from '@tanstack/react-query';
import { fetchScoringRules } from '@/lib/api/endpoints';
import type { ScoringRules } from '@/lib/api/types';

/**
 * Return the static scoring rules from the server.
 *
 * Data is never automatically re-fetched (staleTime: Infinity).
 * The query is always enabled — scoring rules are not user-specific.
 */
export function useScoringRules() {
  return useQuery<ScoringRules>({
    queryKey: ['scoringRules'],
    queryFn: fetchScoringRules,
    staleTime: Infinity,
  });
}
