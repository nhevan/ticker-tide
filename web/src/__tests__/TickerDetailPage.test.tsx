/**
 * Tests for TickerDetailPage.tsx
 *
 * Mocks useSnapshot to verify loading, error, and success render states.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { TickerDetailPage } from '@/pages/TickerDetailPage';
import { ApiError } from '@/lib/api/client';

// PriceChart renders lightweight-charts, which requires Canvas APIs not
// available under jsdom. Stub it out — chart rendering is covered by
// manual verification, not unit tests.
vi.mock('@/components/PriceChart', () => ({
  PriceChart: () => null,
}));

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
vi.mock('@/lib/hooks/useScoringRules', () => ({
  useScoringRules: vi.fn().mockReturnValue({ data: null }),
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
import { useScoringRules } from '@/lib/hooks/useScoringRules';
import { DirectionBreakdown, ConfidenceBreakdown } from '@/pages/TickerDetailPage';

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
        <TickerDetailPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('TickerDetailPage', () => {
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
    expect(screen.getAllByText('BULLISH').length).toBeGreaterThanOrEqual(1);
  });

  describe('cross-section banner', () => {
    const SCORING_RULES_WITH_TIMEFRAME_WEIGHTS = {
      rsi: {
        thresholds: { oversold: 30, overbought: 70 },
        scoring_method: 'percentile_blended_with_fallback',
        fallback_zones: [],
        profile_zones: [],
      },
      regime_weights: {},
      score_expansion_factor: 1.5,
      approximation_caveat: '',
      timeframe_weights: {
        trending: { daily: 0.10, weekly: 0.50, monthly: 0.40 },
        ranging: { daily: 0.60, weekly: 0.30, monthly: 0.10 },
        volatile: { daily: 0.25, weekly: 0.45, monthly: 0.30 },
      },
    };

    /** Snapshot with regime + daily_score so computeTimeframeHeaderContributions returns non-null entries. */
    const snapshotWithRegime = {
      ...mockSnapshot,
      daily: {
        ...mockSnapshot.daily,
        regime: 'ranging',
        daily_score: 20.0,
      },
    };

    it('banner renders when headerContributions has at least one non-null entry', () => {
      vi.mocked(useSnapshot).mockReturnValue({
        data: snapshotWithRegime,
        isLoading: false,
        error: null,
      } as unknown as ReturnType<typeof useSnapshot>);
      vi.mocked(useScoringRules).mockReturnValue({
        data: SCORING_RULES_WITH_TIMEFRAME_WEIGHTS,
      } as unknown as ReturnType<typeof useScoringRules>);

      renderDashboard();
      // Banner should render with ≈ symbol
      expect(document.body.textContent).toContain('≈');
    });

    it('banner hidden when all headerContributions entries are null (no regime)', () => {
      const snapshotNoRegime = {
        ...mockSnapshot,
        daily: {
          ...mockSnapshot.daily,
          regime: null,
          daily_score: null,
        },
      };
      vi.mocked(useSnapshot).mockReturnValue({
        data: snapshotNoRegime,
        isLoading: false,
        error: null,
      } as unknown as ReturnType<typeof useSnapshot>);
      vi.mocked(useScoringRules).mockReturnValue({
        data: SCORING_RULES_WITH_TIMEFRAME_WEIGHTS,
      } as unknown as ReturnType<typeof useScoringRules>);

      const { container } = renderDashboard();
      // The cross-section banner div has class "rounded-lg border border-border/60"
      // which is distinct from the MatrixTable divs. No ≈ should appear.
      // Note: MatrixTable ≈ symbols may appear if snapshots have contributions_payload
      // but mockSnapshot does not have contributions_payload, so no ≈ should appear at all.
      expect(document.body.textContent).not.toContain('≈');
    });

    it('banner uses ≈ not = for the cross-section total', () => {
      vi.mocked(useSnapshot).mockReturnValue({
        data: snapshotWithRegime,
        isLoading: false,
        error: null,
      } as unknown as ReturnType<typeof useSnapshot>);
      vi.mocked(useScoringRules).mockReturnValue({
        data: SCORING_RULES_WITH_TIMEFRAME_WEIGHTS,
      } as unknown as ReturnType<typeof useScoringRules>);

      renderDashboard();
      expect(document.body.textContent).toContain('≈');
      // The banner should NOT use bare = (it uses ≈)
      // We assert ≈ is present — the prototype used = but the real implementation uses ≈
    });
  });
});

const RAW_THRESHOLDS = {
  all:      { bullish: 39, bearish: -11, n: 30974 },
  ranging:  { bullish: 23, bearish: -4,  n: 9326 },
  trending: { bullish: 59, bearish: -20, n: 18964 },
  volatile: { bullish: 27, bearish: -1,  n: 2684 },
};

describe('DirectionBreakdown', () => {
  it('F1: renders trending thresholds and caption when regime="trending"', () => {
    const { container } = render(
      <DirectionBreakdown
        compositeScore={65}
        regime="trending"
        rawThresholds={RAW_THRESHOLDS}
      />,
    );
    const text = container.textContent ?? '';
    // Bullish threshold for trending is +59.0
    expect(text).toContain('+59.0');
    // Bearish threshold for trending is −20.0
    expect(text).toContain('−20.0');
    // Caption must contain regime name and sample size
    expect(text).toContain('trending');
    expect(text).toContain('18,964');
  });

  it('F2: falls back to "all" thresholds and caption when regime is unknown', () => {
    const { container } = render(
      <DirectionBreakdown
        compositeScore={10}
        regime="unknown_regime_xyz"
        rawThresholds={RAW_THRESHOLDS}
      />,
    );
    const text = container.textContent ?? '';
    // "all" bullish threshold is +39.0
    expect(text).toContain('+39.0');
    // "all" bearish threshold is −11.0
    expect(text).toContain('−11.0');
    // Caption must indicate all rows and n=30,974
    expect(text).toContain('all rows');
    expect(text).toContain('30,974');
  });

  it('F3: falls back to "all" thresholds and caption when regime is null', () => {
    const { container } = render(
      <DirectionBreakdown
        compositeScore={10}
        regime={null}
        rawThresholds={RAW_THRESHOLDS}
      />,
    );
    const text = container.textContent ?? '';
    // Same "all" fallback as F2
    expect(text).toContain('+39.0');
    expect(text).toContain('−11.0');
    expect(text).toContain('all rows');
    expect(text).toContain('30,974');
  });
});

describe('ConfidenceBreakdown', () => {
  it('uses configured multiplier from props', () => {
    const { container } = render(
      <ConfidenceBreakdown
        compositeScore={50}
        confidenceModifiers={{}}
        coldStartMultiplier={0.65}
        coldStartMax={90}
      />,
    );
    const text = container.textContent ?? '';
    // base = abs(50) * 0.65 = 32.5
    expect(text).toContain('32.5');
  });

  it('falls back to 0.3 when multiplier undefined', () => {
    const { container } = render(
      <ConfidenceBreakdown
        compositeScore={50}
        confidenceModifiers={{}}
        coldStartMultiplier={undefined}
        coldStartMax={undefined}
      />,
    );
    const text = container.textContent ?? '';
    // base = abs(50) * 0.3 = 15.0
    expect(text).toContain('15.0');
  });

  it('displays the theoretical maximum line', () => {
    const { container } = render(
      <ConfidenceBreakdown
        compositeScore={50}
        confidenceModifiers={{}}
        coldStartMultiplier={0.65}
        coldStartMax={90}
      />,
    );
    const text = container.textContent ?? '';
    expect(text).toContain('Theoretical maximum');
    expect(text).toContain('90%');
  });
});
