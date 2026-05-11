/**
 * TanStack Query mutation hook for POST /api/llm.
 *
 * Uses useMutation with retry: 0 since the server debounces duplicate
 * requests — retrying on 429 would always fail within the window.
 */

import { useMutation } from '@tanstack/react-query';
import { askAI } from '@/lib/api/endpoints';
import type { AskAIArgs } from '@/lib/api/endpoints';
import type { LlmResponse } from '@/lib/api/types';

/**
 * Return a TanStack Query mutation for requesting AI analysis.
 *
 * retry is explicitly set to 0 so that 429 debounce responses are not
 * automatically retried by the query client.
 */
export function useLlm() {
  return useMutation<LlmResponse, Error, AskAIArgs>({
    mutationFn: askAI,
    retry: 0,
  });
}
