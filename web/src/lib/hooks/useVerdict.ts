/**
 * TanStack Query hooks for the dashboard verdict block.
 *
 * useVerdict: GET /api/verdict — returns the cached verdict (or null on 404).
 * useGenerateVerdict: POST /api/verdict — generates (or returns cached) a
 *   verdict and primes the GET query cache via setQueryData so the UI
 *   transitions from "no verdict" to "show verdict" without a refetch.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { generateVerdict, getVerdict } from '@/lib/api/endpoints';
import type { VerdictArgs } from '@/lib/api/endpoints';
import type { VerdictResponse } from '@/lib/api/types';

const verdictQueryKey = (ticker: string, date: string) => ['verdict', ticker, date];

/**
 * Return the cached verdict for the given ticker/date, or null when uncached.
 *
 * Query is disabled when ticker or date is empty.
 *
 * @param ticker - Ticker symbol (e.g. "AAPL").
 * @param date - ISO date string (YYYY-MM-DD).
 */
export function useVerdict(ticker: string, date: string) {
  return useQuery<VerdictResponse | null>({
    queryKey: verdictQueryKey(ticker, date),
    queryFn: () => getVerdict({ ticker, date }),
    enabled: ticker.length > 0 && date.length > 0,
    // Verdicts are immutable per (ticker, date) — once fetched they never go
    // stale within a session, so serve from cache on revisit without a GET.
    staleTime: Infinity,
    gcTime: Infinity,
  });
}

/**
 * Return a mutation that generates a verdict for the given ticker/date.
 *
 * On success, primes the matching useVerdict query cache so the UI
 * immediately renders the verdict without a follow-up GET roundtrip.
 */
export function useGenerateVerdict() {
  const queryClient = useQueryClient();
  return useMutation<VerdictResponse, Error, VerdictArgs>({
    mutationFn: generateVerdict,
    retry: 0,
    onSuccess: (data, variables) => {
      queryClient.setQueryData(
        verdictQueryKey(variables.ticker, variables.date),
        data,
      );
    },
  });
}
