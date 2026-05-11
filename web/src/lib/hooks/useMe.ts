/**
 * TanStack Query hook for GET /api/me.
 *
 * Used by RequireAuth to gate protected routes. staleTime: Infinity avoids
 * repeated polling — the session state only changes on login/logout actions.
 */

import { useQuery } from '@tanstack/react-query';
import { getMe } from '@/lib/api/endpoints';
import type { MeResponse } from '@/lib/api/types';

/** Cache key for the /api/me query. */
export const ME_QUERY_KEY = ['me'] as const;

/**
 * Return the current authentication state from /api/me.
 *
 * Returns a TanStack Query result. The query will not retry on 401 to avoid
 * a retry loop on unauthenticated sessions.
 */
export function useMe() {
  return useQuery<MeResponse>({
    queryKey: ME_QUERY_KEY,
    queryFn: getMe,
    staleTime: Infinity,
    retry: 0,
  });
}
