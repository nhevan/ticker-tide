/**
 * Tests for AskAI.tsx
 *
 * Verifies: Ask AI button calls mutation with correct args, shows response text,
 * shows error on failure.
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AskAI } from '@/components/AskAI';

vi.mock('@/lib/hooks/useLlm', () => ({
  useLlm: vi.fn(),
}));

import { useLlm } from '@/lib/hooks/useLlm';
import type { AskAIArgs } from '@/lib/api/endpoints';
import type { LlmResponse } from '@/lib/api/types';
import { ApiError } from '@/lib/api/client';

function renderAskAI(ticker = 'AAPL', date = '2026-04-25', timeframe: AskAIArgs['timeframe'] = 'daily') {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 0 } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <AskAI ticker={ticker} date={date} timeframe={timeframe} />
    </QueryClientProvider>,
  );
}

describe('AskAI', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders Ask AI button', () => {
    const mockMutate = vi.fn();
    vi.mocked(useLlm).mockReturnValue({
      mutate: mockMutate,
      data: undefined,
      error: null,
      isPending: false,
      reset: vi.fn(),
    } as unknown as ReturnType<typeof useLlm>);

    renderAskAI();
    expect(screen.getByRole('button', { name: /ask ai/i })).toBeInTheDocument();
  });

  it('calls mutate with correct ticker, date, and timeframe on click', () => {
    const mockMutate = vi.fn();
    vi.mocked(useLlm).mockReturnValue({
      mutate: mockMutate,
      data: undefined,
      error: null,
      isPending: false,
      reset: vi.fn(),
    } as unknown as ReturnType<typeof useLlm>);

    renderAskAI('TSLA', '2026-04-20', 'weekly');
    fireEvent.click(screen.getByRole('button', { name: /ask ai/i }));

    expect(mockMutate).toHaveBeenCalledWith({
      ticker: 'TSLA',
      date: '2026-04-20',
      timeframe: 'weekly',
    });
  });

  it('shows loading state while pending', () => {
    vi.mocked(useLlm).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: null,
      isPending: true,
      reset: vi.fn(),
    } as unknown as ReturnType<typeof useLlm>);

    renderAskAI();
    expect(screen.getByRole('button', { name: /analyzing/i })).toBeDisabled();
  });

  it('renders analysis text when data is returned', () => {
    vi.mocked(useLlm).mockReturnValue({
      mutate: vi.fn(),
      data: { text: 'AAPL looks bullish on RSI divergence.' } as LlmResponse,
      error: null,
      isPending: false,
      reset: vi.fn(),
    } as unknown as ReturnType<typeof useLlm>);

    renderAskAI();
    expect(screen.getByText('AAPL looks bullish on RSI divergence.')).toBeInTheDocument();
  });

  it('shows error detail on API failure', () => {
    vi.mocked(useLlm).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: new ApiError(503, 'AI analysis is temporarily unavailable.'),
      isPending: false,
      reset: vi.fn(),
    } as unknown as ReturnType<typeof useLlm>);

    renderAskAI();
    expect(screen.getByText('AI analysis is temporarily unavailable.')).toBeInTheDocument();
  });
});
