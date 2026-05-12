/**
 * Tests for equationSummary.ts — pure helper functions for building
 * equation row data from contributions_payload items.
 */

import { describe, it, expect } from 'vitest';
import { summarizeSectionContributions, summarizeCrossSection } from './equationSummary';
import type { ContributionItem } from '@/lib/api/types';

function makeItem(
  name: string,
  contribution: number,
  kind: 'indicator' | 'pattern' | 'aggregate' = 'indicator',
): ContributionItem {
  return {
    name,
    category: 'momentum',
    kind,
    score: 50,
    raw_value: null,
    category_weight: 0.2,
    contribution,
  };
}

describe('summarizeSectionContributions', () => {
  describe('basic shape', () => {
    it('returns all finite non-zero items sorted by abs(contribution) desc', () => {
      const items = [makeItem('a', 10), makeItem('b', -3), makeItem('c', 8)];
      const result = summarizeSectionContributions(items, 50);
      expect(result).not.toBeNull();
      expect(result!.items.map((i) => i.label)).toEqual(['a', 'c', 'b']);
      expect(result!.items.map((i) => i.value)).toEqual([10, 8, -3]);
    });

    it('items.length equals the number of non-zero finite contributions', () => {
      const items = [
        makeItem('a', 10),
        makeItem('b', 9),
        makeItem('c', 8),
        makeItem('d', 7),
        makeItem('e', 6),
        makeItem('f', 5),
      ];
      const result = summarizeSectionContributions(items, 50);
      expect(result!.items).toHaveLength(6);
    });
  });

  describe('zero contributions hidden', () => {
    it('items with contribution = 0 are filtered out', () => {
      const items = [
        makeItem('zero1', 0),
        makeItem('nonzero', 5),
        makeItem('zero2', -0),
      ];
      const result = summarizeSectionContributions(items, 5);
      expect(result!.items.map((i) => i.label)).toEqual(['nonzero']);
    });

    it('returns null when every item has zero contribution', () => {
      const items = [makeItem('z1', 0), makeItem('z2', 0)];
      expect(summarizeSectionContributions(items, 10)).toBeNull();
    });
  });

  describe('non-finite contributions skipped', () => {
    it('skips NaN contribution items', () => {
      const items = [makeItem('nan', NaN), makeItem('valid', 5)];
      const result = summarizeSectionContributions(items, 10);
      expect(result!.items.map((i) => i.label)).toEqual(['valid']);
    });

    it('skips Infinity contribution items', () => {
      const items = [makeItem('inf', Infinity), makeItem('valid', 5)];
      const result = summarizeSectionContributions(items, 10);
      expect(result!.items.map((i) => i.label)).toEqual(['valid']);
    });
  });

  describe('null / undefined / empty inputs', () => {
    it('returns null when items is undefined', () => {
      expect(summarizeSectionContributions(undefined, 50)).toBeNull();
    });

    it('returns null when items is empty', () => {
      expect(summarizeSectionContributions([], 50)).toBeNull();
    });

    it('returns null when target is null', () => {
      expect(summarizeSectionContributions([makeItem('a', 5)], null)).toBeNull();
    });

    it('returns null when target is undefined', () => {
      expect(summarizeSectionContributions([makeItem('a', 5)], undefined)).toBeNull();
    });

    it('returns null when target is NaN', () => {
      expect(summarizeSectionContributions([makeItem('a', 5)], NaN)).toBeNull();
    });

    it('returns null when target is Infinity', () => {
      expect(summarizeSectionContributions([makeItem('a', 5)], Infinity)).toBeNull();
    });

    it('finite zero target is accepted', () => {
      const result = summarizeSectionContributions([makeItem('a', 5)], 0);
      expect(result!.total).toBe(0);
    });
  });

  describe('total field', () => {
    it('total equals target, not the sum of items (load-bearing divergence)', () => {
      const result = summarizeSectionContributions(
        [makeItem('a', 5), makeItem('b', 3)],
        12,
      );
      expect(result!.total).toBe(12);
    });
  });

  describe('kind inclusion', () => {
    it('includes pattern items with raw name as label', () => {
      const result = summarizeSectionContributions(
        [makeItem('candlestick_pattern_score', 4.2, 'pattern')],
        10,
      );
      expect(result!.items[0].label).toBe('candlestick_pattern_score');
    });

    it('includes aggregate items with raw name as label', () => {
      const result = summarizeSectionContributions(
        [makeItem('sentiment', 3.1, 'aggregate')],
        10,
      );
      expect(result!.items[0].label).toBe('sentiment');
    });
  });
});

describe('summarizeCrossSection', () => {
  it('returns parts in [daily, weekly, monthly] order when all present', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0.1, score: 100 },
      weekly: { weight: 0.5, score: 90 },
      monthly: { weight: 0.4, score: 100 },
    });
    expect(result!.parts.map((p) => p.label)).toEqual(['daily', 'weekly', 'monthly']);
    expect(result!.parts[0].value).toBeCloseTo(10, 6);
    expect(result!.parts[1].value).toBeCloseTo(45, 6);
    expect(result!.parts[2].value).toBeCloseTo(40, 6);
    expect(result!.total).toBeCloseTo(95, 6);
  });

  it('skips null entries; total is sum of present parts', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0.2, score: 50 },
      weekly: null,
      monthly: { weight: 0.8, score: 25 },
    });
    expect(result!.parts.map((p) => p.label)).toEqual(['daily', 'monthly']);
    expect(result!.total).toBeCloseTo(30, 6);
  });

  it('returns null when all entries are null', () => {
    expect(
      summarizeCrossSection({ daily: null, weekly: null, monthly: null }),
    ).toBeNull();
  });

  it('weight=0 still produces a part with value=0', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0, score: 100 },
      weekly: null,
      monthly: null,
    });
    expect(result!.parts[0].value).toBe(0);
  });

  it('score=0 still produces a part with value=0', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0.5, score: 0 },
      weekly: null,
      monthly: null,
    });
    expect(result!.parts[0].value).toBe(0);
  });

  it('skips entries with non-finite weight', () => {
    const result = summarizeCrossSection({
      daily: { weight: NaN, score: 50 },
      weekly: { weight: 0.5, score: 20 },
      monthly: null,
    });
    expect(result!.parts.map((p) => p.label)).toEqual(['weekly']);
  });

  it('skips entries with non-finite score', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0.1, score: Infinity },
      weekly: { weight: 0.5, score: 20 },
      monthly: null,
    });
    expect(result!.parts.map((p) => p.label)).toEqual(['weekly']);
  });

  it('handles negative contributions', () => {
    const result = summarizeCrossSection({
      daily: { weight: 0.5, score: -40 },
      weekly: { weight: 0.5, score: 30 },
      monthly: null,
    });
    expect(result!.parts[0].value).toBeCloseTo(-20, 6);
    expect(result!.parts[1].value).toBeCloseTo(15, 6);
    expect(result!.total).toBeCloseTo(-5, 6);
  });
});
