/**
 * Tests for SignalClassificationTooltip.tsx.
 *
 * Verifies:
 * - Happy path: all three steps render with calibrated_score and sector ETF mapped.
 * - Cold start: Step 2 shows cold-start state, Step 3 compares composite_score.
 *
 * Frontend tests for pure UI components are deferred per project policy (CLAUDE.md
 * explainer recipe §7). These two tests cover the wiring contract only.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { SignalClassificationTooltip } from '@/components/SignalClassificationTooltip';
import type { DailySection, TimeframeSection, ScoringRules } from '@/lib/api/types';

// ── Mock useScoringRules ───────────────────────────────────────────────────────

vi.mock('@/lib/hooks/useScoringRules', () => ({
  useScoringRules: vi.fn(),
}));

import { useScoringRules } from '@/lib/hooks/useScoringRules';

// ── Fixtures ──────────────────────────────────────────────────────────────────

const MOCK_SCORING_RULES: ScoringRules = {
  rsi: {
    thresholds: { oversold: 30, overbought: 70 },
    scoring_method: 'percentile_blended_with_fallback',
    fallback_zones: ['oversold', 'below_mid', 'above_mid', 'overbought'],
    profile_zones: ['extreme_oversold', 'oversold', 'below_mid', 'above_mid', 'overbought', 'extreme_overbought'],
  },
  regime_weights: {
    trending: { trend: 0.30, momentum: 0.20, volume: 0.10, volatility: 0.05,
                candlestick: 0.0, structural: 0.0, sentiment: 0.0, fundamental: 0.05, macro: 0.30 },
    ranging: { trend: 0.15, momentum: 0.25, volume: 0.15, volatility: 0.10,
               candlestick: 0.0, structural: 0.0, sentiment: 0.0, fundamental: 0.10, macro: 0.25 },
    volatile: { trend: 0.20, momentum: 0.20, volume: 0.10, volatility: 0.15,
                candlestick: 0.0, structural: 0.0, sentiment: 0.0, fundamental: 0.05, macro: 0.30 },
  },
  score_expansion_factor: 1.5,
  timeframe_weights: {
    trending: { daily: 0.10, weekly: 0.50, monthly: 0.40 },
    ranging:  { daily: 0.60, weekly: 0.30, monthly: 0.10 },
    volatile: { daily: 0.25, weekly: 0.45, monthly: 0.30 },
  },
  signal_thresholds: { bullish: 2, bearish: -2 },
  approximation_caveat: 'Item-level contributions do not sum to the final composite score.',
};

function makeDaily(overrides: Partial<DailySection> = {}): DailySection {
  return {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro'],
    resolved_period: '2026-04-27',
    signal: 'BULLISH',
    confidence: 80,
    composite_score: 94.8,
    daily_score: 94.8,
    calibrated_score: 4.7,
    regime: 'trending',
    raw_daily_score: 92.0,
    sector_etf_score: 58.0,
    sector_etf: 'SMH',
    ...overrides,
  };
}

function makeWeekly(overrides: Partial<TimeframeSection> = {}): TimeframeSection {
  return {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
    resolved_period: '2026-04-21',
    resolved_period_label: 'Week ending Apr 27',
    is_fallback: false,
    composite_score: 89.7,
    ...overrides,
  };
}

function makeMonthly(overrides: Partial<TimeframeSection> = {}): TimeframeSection {
  return {
    data_available: true,
    categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
    resolved_period: '2026-04-01',
    resolved_period_label: 'Apr 2026',
    is_fallback: false,
    composite_score: 99.9,
    ...overrides,
  };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('SignalClassificationTooltip', () => {
  it('happy path: renders all three step headings with calibrated_score and sector ETF mapped', () => {
    vi.mocked(useScoringRules).mockReturnValue({
      data: MOCK_SCORING_RULES,
    } as ReturnType<typeof useScoringRules>);

    const daily = makeDaily();
    const weekly = makeWeekly();
    const monthly = makeMonthly();

    render(
      <SignalClassificationTooltip daily={daily} weekly={weekly} monthly={monthly} />,
    );

    // All three step headings must be visible.
    expect(screen.getByText(/Step 1a —/i)).toBeInTheDocument();
    expect(screen.getByText(/Step 1b —/i)).toBeInTheDocument();
    expect(screen.getByText(/Step 2 —/i)).toBeInTheDocument();
    expect(screen.getByText(/Step 3 —/i)).toBeInTheDocument();

    // Sector ETF symbol must appear in Step 1a.
    expect(screen.getByText(/SMH/)).toBeInTheDocument();

    // Calibrator state should show "available" (exact span text, not the formula prose).
    expect(screen.getByText('available')).toBeInTheDocument();

    // Regime label should appear.
    expect(screen.getByText('trending')).toBeInTheDocument();

    // Final classification — effective=4.7 >= bullish=2 → BULLISH
    // Appears in both the formula row and the result row.
    expect(screen.getAllByText('BULLISH').length).toBeGreaterThanOrEqual(1);
  });

  it('cold start: shows cold-start label in Step 2, classification uses composite_score', () => {
    vi.mocked(useScoringRules).mockReturnValue({
      data: MOCK_SCORING_RULES,
    } as ReturnType<typeof useScoringRules>);

    // calibrated_score = null → cold start
    const daily = makeDaily({ calibrated_score: null, composite_score: 94.8 });
    const weekly = makeWeekly();
    const monthly = makeMonthly();

    render(
      <SignalClassificationTooltip daily={daily} weekly={weekly} monthly={monthly} />,
    );

    // Step 2 must show the cold-start indicator (header status span exact text).
    expect(screen.getByText('cold start')).toBeInTheDocument();

    // Fallback caption must appear.
    expect(screen.getByText(/calibrator has too little history/i)).toBeInTheDocument();

    // With effective = composite_score ≈ 94.8 >= bullish threshold 2 → BULLISH
    // Appears in both the formula row and the result row.
    expect(screen.getAllByText('BULLISH').length).toBeGreaterThanOrEqual(1);
  });
});
