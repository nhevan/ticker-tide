/**
 * Unit tests for computeTimeframeHeaderContributions.
 *
 * Covers normalization, partial-availability, edge cases, and
 * guard clauses (null regime, undefined scoringRules, totalAvailable === 0).
 */

import { describe, it, expect } from 'vitest';
import { computeTimeframeHeaderContributions } from './timeframeHeaderContribution';
import type { Snapshot, ScoringRules } from '@/lib/api/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeSnapshot(opts: {
  regime?: string | null;
  dailyScore?: number | null;
  weeklyScore?: number | null;
  monthlyScore?: number | null;
}): Snapshot {
  return {
    daily: {
      data_available: true,
      categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro'],
      resolved_period: '2026-05-12',
      regime: opts.regime ?? null,
      daily_score: opts.dailyScore ?? null,
      rsi_sparkline: [],
    },
    weekly: {
      data_available: opts.weeklyScore !== undefined && opts.weeklyScore !== null,
      categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
      resolved_period: '2026-05-05',
      resolved_period_label: 'Week ending May 11',
      is_fallback: false,
      composite_score: opts.weeklyScore ?? null,
    },
    monthly: {
      data_available: opts.monthlyScore !== undefined && opts.monthlyScore !== null,
      categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
      resolved_period: '2026-05-01',
      resolved_period_label: 'May 2026',
      is_fallback: false,
      composite_score: opts.monthlyScore ?? null,
    },
  } as Snapshot;
}

function makeScoringRules(opts?: {
  trending?: { daily: number; weekly: number; monthly: number };
  ranging?: { daily: number; weekly: number; monthly: number };
  volatile?: { daily: number; weekly: number; monthly: number };
}): ScoringRules {
  return {
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
      trending: opts?.trending ?? { daily: 0.10, weekly: 0.50, monthly: 0.40 },
      ranging: opts?.ranging ?? { daily: 0.60, weekly: 0.30, monthly: 0.10 },
      volatile: opts?.volatile ?? { daily: 0.25, weekly: 0.45, monthly: 0.30 },
    },
    equation_summary_top_n: 5,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('computeTimeframeHeaderContributions', () => {
  describe('all three timeframes available', () => {
    it('returns weight and score for each timeframe when all scores are finite', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).not.toBeNull();
      expect(result.weekly).not.toBeNull();
      expect(result.monthly).not.toBeNull();
    });

    it('weights normalize to sum ≈ 1.0 when config weights already sum to 1.0', () => {
      // ranging: 0.60 + 0.30 + 0.10 = 1.00
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      const totalWeight =
        (result.daily?.weight ?? 0) +
        (result.weekly?.weight ?? 0) +
        (result.monthly?.weight ?? 0);
      expect(Math.abs(totalWeight - 1.0)).toBeLessThan(1e-9);
    });

    it('daily weight equals configWeight / totalAvailable when all available', () => {
      // ranging: daily=0.60, totalAvailable=1.00, so weight=0.60
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(Math.abs((result.daily?.weight ?? -1) - 0.60)).toBeLessThan(1e-9);
    });

    it('score fields match the pre-blend scores from the snapshot', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily?.score).toBe(20.5);
      expect(result.weekly?.score).toBe(41.0);
      expect(result.monthly?.score).toBe(-8.3);
    });
  });

  describe('weekly unavailable', () => {
    it('returns null for weekly when weekly composite_score is null', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: null,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.weekly).toBeNull();
    });

    it('daily and monthly weights inflate proportionally; their sum ≈ 1.0', () => {
      // ranging: daily=0.60, monthly=0.10, totalAvailable=0.70
      // daily weight = 0.60/0.70, monthly weight = 0.10/0.70
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: null,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      const sum = (result.daily?.weight ?? 0) + (result.monthly?.weight ?? 0);
      expect(Math.abs(sum - 1.0)).toBeLessThan(1e-9);
    });

    it('daily weight = 0.60/0.70 when weekly is unavailable (ranging)', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: null,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      const expected = 0.60 / 0.70;
      expect(Math.abs((result.daily?.weight ?? -1) - expected)).toBeLessThan(1e-9);
    });
  });

  describe('only daily available', () => {
    it('daily weight equals 1.0; weekly and monthly are null', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: null,
        monthlyScore: null,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
      expect(Math.abs((result.daily?.weight ?? -1) - 1.0)).toBeLessThan(1e-9);
    });
  });

  describe('all three unavailable', () => {
    it('returns all nulls when all scores are null', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: null,
        weeklyScore: null,
        monthlyScore: null,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });
  });

  describe('guard clauses', () => {
    it('returns all nulls when regime is null', () => {
      const snapshot = makeSnapshot({
        regime: null,
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });

    it('returns all nulls when regime is undefined', () => {
      const snapshot = makeSnapshot({
        regime: undefined as unknown as null,
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });

    it('returns all nulls when scoringRules is undefined', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const result = computeTimeframeHeaderContributions(snapshot, undefined);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });

    it('returns all nulls when regime has no entry in timeframe_weights', () => {
      const snapshot = makeSnapshot({
        regime: 'unknown_regime',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });

    it('returns all nulls when totalAvailable is 0 (all config weights are 0 for regime)', () => {
      // Synthetic: all three timeframe weights for regime are 0.
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules({
        ranging: { daily: 0, weekly: 0, monthly: 0 },
      });
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
      expect(result.weekly).toBeNull();
      expect(result.monthly).toBeNull();
    });
  });

  describe('NaN / Infinity handling', () => {
    it('treats NaN daily_score as unavailable (returns null for daily)', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: NaN,
        weeklyScore: 41.0,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.daily).toBeNull();
    });

    it('treats Infinity weekly score as unavailable (returns null for weekly)', () => {
      const snapshot = makeSnapshot({
        regime: 'ranging',
        dailyScore: 20.5,
        weeklyScore: Infinity,
        monthlyScore: -8.3,
      });
      const rules = makeScoringRules();
      const result = computeTimeframeHeaderContributions(snapshot, rules);

      expect(result.weekly).toBeNull();
    });
  });
});
