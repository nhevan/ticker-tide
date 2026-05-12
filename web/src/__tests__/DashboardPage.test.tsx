/**
 * Tests for DashboardPage.tsx
 *
 * Mocks useSnapshot to verify loading, error, and success render states.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { DashboardPage } from '@/pages/DashboardPage';
import { ApiError } from '@/lib/api/client';

vi.mock('@/lib/hooks/useSnapshot', () => ({
  useSnapshot: vi.fn(),
}));
vi.mock('@/lib/hooks/useTickers', () => ({
  useTickers: vi.fn().mockReturnValue({ data: ['AAPL', 'TSLA'] }),
}));
vi.mock('@/lib/hooks/useDateRange', () => ({
  useDateRange: vi.fn().mockReturnValue({ data: { min: '2026-01-01', max: '2026-04-25' } }),
}));
vi.mock('@/lib/api/endpoints', () => ({
  logout: vi.fn(),
  askAI: vi.fn(),
  generateVerdict: vi.fn(),
  getVerdict: vi.fn().mockResolvedValue(null),
  fetchScoringRules: vi.fn().mockResolvedValue(null),
}));
vi.mock('@/lib/hooks/useLlm', () => ({
  useLlm: vi.fn().mockReturnValue({
    mutate: vi.fn(),
    data: undefined,
    error: null,
    isPending: false,
    reset: vi.fn(),
  }),
}));
vi.mock('@/lib/hooks/useVerdict', () => ({
  useVerdict: vi.fn().mockReturnValue({ data: null, isLoading: false }),
  useGenerateVerdict: vi.fn().mockReturnValue({
    mutate: vi.fn(),
    data: undefined,
    error: null,
    isPending: false,
  }),
}));

import { useSnapshot } from '@/lib/hooks/useSnapshot';

const mockSnapshot = {
  daily: {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro'],
    resolved_period: '2026-04-25',
    signal: 'BULLISH',
    confidence: 72.5,
    calibrated_score: 1.42,
    composite_score: 55.0,
    scores: { trend: 40, momentum: 30, volume: 20, volatility: -10, candlestick: 25, structural: 15, sentiment: 5, fundamental: 8, macro: -3 },
    patterns: [],
    sparkline: [],
    key_signals: ['RSI above 60', 'Price above 50MA'],
    earnings: { next: null, last_surprise: null },
    signal_flip: null,
  },
  weekly: {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
    resolved_period: '2026-04-21',
    resolved_period_label: 'Week ending Apr 25',
    is_fallback: false,
    composite_score: 48.0,
    scores: { trend: 35, momentum: 20, volume: 15, volatility: -5, candlestick: 10, structural: 12 },
    patterns: [],
    sparkline: [],
  },
  monthly: {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
    resolved_period: '2026-04-01',
    resolved_period_label: 'Apr 2026',
    is_fallback: false,
    composite_score: 38.0,
    scores: { trend: 30, momentum: 15, volume: 10, volatility: -8, structural: 11 },
    patterns: [],
    sparkline: [],
  },
};

function renderDashboard() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 0 } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <DashboardPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('DashboardPage', () => {
  it('renders placeholder text before any load', () => {
    vi.mocked(useSnapshot).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSnapshot>);

    renderDashboard();
    expect(screen.getByText(/select a ticker/i)).toBeInTheDocument();
  });

  it('renders skeleton loading state while snapshot is loading', () => {
    vi.mocked(useSnapshot).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as unknown as ReturnType<typeof useSnapshot>);

    const { container } = renderDashboard();
    // Skeleton divs should be present
    const skeletons = container.querySelectorAll('.animate-pulse');
    expect(skeletons.length).toBeGreaterThan(0);
  });

  it('renders error banner when snapshot fails', () => {
    vi.mocked(useSnapshot).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new ApiError(404, 'No data found for ticker "ZZZZ".'),
    } as unknown as ReturnType<typeof useSnapshot>);

    renderDashboard();
    expect(screen.getByRole('alert')).toBeInTheDocument();
  });

  it('renders three timeframe cards on success', () => {
    vi.mocked(useSnapshot).mockReturnValue({
      data: mockSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSnapshot>);

    renderDashboard();
    expect(screen.getAllByText('Daily').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Weekly').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Monthly').length).toBeGreaterThan(0);
  });

  it('renders signal badge for BULLISH signal', () => {
    vi.mocked(useSnapshot).mockReturnValue({
      data: mockSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSnapshot>);

    renderDashboard();
    expect(screen.getByText('BULLISH')).toBeInTheDocument();
  });
});
