/**
 * Tests for equationSummary.ts — pure helper functions for building
 * equation row data from contributions_payload items.
 *
 * TDD: these tests were written before the implementation.
 */

import { describe, it, expect } from 'vitest';
import { summarizeSectionContributions, summarizeCrossSection } from './equationSummary';
import type { ContributionItem } from '@/lib/api/types';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// summarizeSectionContributions
// ---------------------------------------------------------------------------

describe('summarizeSectionContributions', () => {
  describe('top-N truncation', () => {
    it('returns topItems.length == topN when items.length > topN', () => {
      const items = [
        makeItem('a', 10),
        makeItem('b', 9),
        makeItem('c', 8),
        makeItem('d', 7),
        makeItem('e', 6),
        makeItem('f', 5),
        makeItem('g', 4),
        makeItem('h', 3),
      ];
      const result = summarizeSectionContributions(items, 50, 5);
      expect(result).not.toBeNull();
      expect(result!.topItems).toHaveLength(5);
    });

    it('othersCount equals items.length - topN when items.length > topN', () => {
      const items = [
        makeItem('a', 10),
        makeItem('b', 9),
        makeItem('c', 8),
        makeItem('d', 7),
        makeItem('e', 6),
        makeItem('f', 5),
        makeItem('g', 4),
        makeItem('h', 3),
      ];
      const result = summarizeSectionContributions(items, 50, 5);
      expect(result).not.toBeNull();
      expect(result!.othersCount).toBe(3);
    });

    it('othersSum equals sum of contributions beyond topN', () => {
      const items = [
        makeItem('a', 10),
        makeItem('b', 9),
        makeItem('c', 8),
        makeItem('d', 7),
        makeItem('e', 6),
        makeItem('f', 5),
        makeItem('g', 4),
        makeItem('h', 3),
      ];
      const result = summarizeSectionContributions(items, 50, 5);
      expect(result).not.toBeNull();
      // Items at positions 5,6,7 (0-indexed) sorted desc by abs: f=5, g=4, h=3 → sum=12
      expect(result!.othersSum).toBeCloseTo(12, 6);
    });
  });

  describe('items.length <= topN', () => {
    it('all items in topItems when count equals topN', () => {
      const items = [makeItem('a', 5), makeItem('b', 3), makeItem('c', 1)];
      const result = summarizeSectionContributions(items, 9, 3);
      expect(result).not.toBeNull();
      expect(result!.topItems).toHaveLength(3);
      expect(result!.othersCount).toBe(0);
      expect(result!.othersSum).toBe(0);
    });

    it('all items in topItems when count is less than topN', () => {
      const items = [makeItem('a', 5), makeItem('b', 3)];
      const result = summarizeSectionContributions(items, 8, 5);
      expect(result).not.toBeNull();
      expect(result!.topItems).toHaveLength(2);
      expect(result!.othersCount).toBe(0);
      expect(result!.othersSum).toBe(0);
    });
  });

  describe('sort by abs(contribution) desc', () => {
    it('first topItem has the largest abs contribution', () => {
      const items = [
        makeItem('small', 2),
        makeItem('large', -10),
        makeItem('medium', 5),
      ];
      const result = summarizeSectionContributions(items, 20, 5);
      expect(result).not.toBeNull();
      expect(result!.topItems[0].label).toBe('large');
      expect(result!.topItems[0].value).toBeCloseTo(-10, 6);
    });

    it('sorts items in descending abs order throughout topItems', () => {
      const items = [
        makeItem('c', 3),
        makeItem('a', -15),
        makeItem('b', 8),
      ];
      const result = summarizeSectionContributions(items, 20, 5);
      expect(result).not.toBeNull();
      const labels = result!.topItems.map((i) => i.label);
      expect(labels).toEqual(['a', 'b', 'c']);
    });
  });

  describe('null/undefined/empty inputs', () => {
    it('returns null when items is undefined', () => {
      expect(summarizeSectionContributions(undefined, 50, 5)).toBeNull();
    });

    it('returns null when items is empty array', () => {
      expect(summarizeSectionContributions([], 50, 5)).toBeNull();
    });

    it('returns null when target is null', () => {
      const items = [makeItem('a', 5)];
      expect(summarizeSectionContributions(items, null, 5)).toBeNull();
    });

    it('returns null when target is undefined', () => {
      const items = [makeItem('a', 5)];
      expect(summarizeSectionContributions(items, undefined, 5)).toBeNull();
    });

    it('returns null when target is NaN', () => {
      const items = [makeItem('a', 5)];
      expect(summarizeSectionContributions(items, NaN, 5)).toBeNull();
    });

    it('returns null when target is Infinity', () => {
      const items = [makeItem('a', 5)];
      expect(summarizeSectionContributions(items, Infinity, 5)).toBeNull();
    });

    it('returns valid result when target is finite zero', () => {
      const items = [makeItem('a', 3), makeItem('b', -3)];
      const result = summarizeSectionContributions(items, 0, 5);
      expect(result).not.toBeNull();
      expect(result!.total).toBe(0);
    });
  });

  describe('total field', () => {
    it('total equals the target value, not the sum of items', () => {
      // sum of items = 5 + 3 = 8, but target = 12 (diverges by 4 to be load-bearing)
      const items = [makeItem('a', 5), makeItem('b', 3)];
      const result = summarizeSectionContributions(items, 12, 5);
      expect(result).not.toBeNull();
      expect(result!.total).toBe(12);
    });
  });

  describe('items with zero contribution', () => {
    it('items with contribution = 0 appear in topItems normally', () => {
      const items = [makeItem('zero', 0), makeItem('nonzero', 5)];
      const result = summarizeSectionContributions(items, 5, 5);
      expect(result).not.toBeNull();
      const labels = result!.topItems.map((i) => i.label);
      expect(labels).toContain('zero');
      expect(labels).toContain('nonzero');
    });
  });

  describe('items with non-finite contribution', () => {
    it('skips items with NaN contribution', () => {
      const items = [makeItem('nan', NaN), makeItem('valid', 5)];
      const result = summarizeSectionContributions(items, 10, 5);
      expect(result).not.toBeNull();
      const labels = result!.topItems.map((i) => i.label);
      expect(labels).not.toContain('nan');
      expect(labels).toContain('valid');
    });

    it('skips items with Infinity contribution', () => {
      const items = [makeItem('inf', Infinity), makeItem('valid', 5)];
      const result = summarizeSectionContributions(items, 10, 5);
      expect(result).not.toBeNull();
      const labels = result!.topItems.map((i) => i.label);
      expect(labels).not.toContain('inf');
    });
  });

  describe('kind inclusion', () => {
    it('includes pattern items (kind=pattern) with raw name as label', () => {
      const items = [makeItem('candlestick_pattern_score', 4.2, 'pattern')];
      const result = summarizeSectionContributions(items, 10, 5);
      expect(result).not.toBeNull();
      expect(result!.topItems[0].label).toBe('candlestick_pattern_score');
    });

    it('includes aggregate items (kind=aggregate) with raw name as label', () => {
      const items = [makeItem('sentiment', 3.1, 'aggregate')];
      const result = summarizeSectionContributions(items, 10, 5);
      expect(result).not.toBeNull();
      expect(result!.topItems[0].label).toBe('sentiment');
    });
  });

  describe('negative contributions', () => {
    it('negative items appear in topItems with their negative value', () => {
      const items = [makeItem('bearish', -7.5), makeItem('bullish', 4.0)];
      const result = summarizeSectionContributions(items, 50, 5);
      expect(result).not.toBeNull();
      const bearish = result!.topItems.find((i) => i.label === 'bearish');
      expect(bearish).toBeDefined();
      expect(bearish!.value).toBeCloseTo(-7.5, 6);
    });

    it('negative items contribute to othersSum with their sign', () => {
      const items = [
        makeItem('a', 10),
        makeItem('b', 9),
        makeItem('c', 8),
        makeItem('d', 7),
        makeItem('e', 6),
        makeItem('neg', -5),
      ];
      // topN=5 → tops are a,b,c,d,e; others = [neg] → othersSum = -5
      const result = summarizeSectionContributions(items, 50, 5);
      expect(result).not.toBeNull();
      expect(result!.othersSum).toBeCloseTo(-5, 6);
    });
  });
});

// ---------------------------------------------------------------------------
// summarizeCrossSection
// ---------------------------------------------------------------------------

describe('summarizeCrossSection', () => {
  function makeHC(weight: number, score: number) {
    return { weight, score };
  }

  describe('all three present', () => {
    it('parts has 3 entries in order [daily, weekly, monthly]', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, 20.0),
        weekly: makeHC(0.30, 45.0),
        monthly: makeHC(0.10, 30.0),
      });
      expect(result).not.toBeNull();
      expect(result!.parts).toHaveLength(3);
      expect(result!.parts[0].label).toBe('daily');
      expect(result!.parts[1].label).toBe('weekly');
      expect(result!.parts[2].label).toBe('monthly');
    });

    it('total equals sum of weight × score across parts', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, 20.0),
        weekly: makeHC(0.30, 45.0),
        monthly: makeHC(0.10, 30.0),
      });
      expect(result).not.toBeNull();
      // 0.60×20 + 0.30×45 + 0.10×30 = 12 + 13.5 + 3 = 28.5
      expect(result!.total).toBeCloseTo(28.5, 6);
    });

    it('each part value equals weight × score', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, 20.0),
        weekly: makeHC(0.30, 45.0),
        monthly: makeHC(0.10, 30.0),
      });
      expect(result).not.toBeNull();
      expect(result!.parts[0].value).toBeCloseTo(12.0, 6);
      expect(result!.parts[1].value).toBeCloseTo(13.5, 6);
      expect(result!.parts[2].value).toBeCloseTo(3.0, 6);
    });
  });

  describe('only daily present', () => {
    it('parts has 1 entry when only daily is non-null', () => {
      const result = summarizeCrossSection({
        daily: makeHC(1.0, 25.0),
        weekly: null,
        monthly: null,
      });
      expect(result).not.toBeNull();
      expect(result!.parts).toHaveLength(1);
      expect(result!.parts[0].label).toBe('daily');
    });

    it('total equals daily.weight × daily.score', () => {
      const result = summarizeCrossSection({
        daily: makeHC(1.0, 25.0),
        weekly: null,
        monthly: null,
      });
      expect(result).not.toBeNull();
      expect(result!.total).toBeCloseTo(25.0, 6);
    });
  });

  describe('all null', () => {
    it('returns null when all three are null', () => {
      const result = summarizeCrossSection({
        daily: null,
        weekly: null,
        monthly: null,
      });
      expect(result).toBeNull();
    });
  });

  describe('score = 0 entries', () => {
    it('entry with score=0 is still included in parts with value=0', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, 0),
        weekly: makeHC(0.40, 20.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      const daily = result!.parts.find((p) => p.label === 'daily');
      expect(daily).toBeDefined();
      expect(daily!.value).toBeCloseTo(0, 6);
    });
  });

  describe('weight = 0 entries', () => {
    it('entry with weight=0 is still included in parts with value=0', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0, 20.0),
        weekly: makeHC(1.0, 30.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      const daily = result!.parts.find((p) => p.label === 'daily');
      expect(daily).toBeDefined();
      expect(daily!.value).toBeCloseTo(0, 6);
    });
  });

  describe('non-finite weight or score', () => {
    it('entry with non-finite weight is skipped', () => {
      const result = summarizeCrossSection({
        daily: makeHC(NaN, 20.0),
        weekly: makeHC(0.40, 30.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      const labels = result!.parts.map((p) => p.label);
      expect(labels).not.toContain('daily');
      expect(labels).toContain('weekly');
    });

    it('entry with non-finite score is skipped', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, Infinity),
        weekly: makeHC(0.40, 30.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      const labels = result!.parts.map((p) => p.label);
      expect(labels).not.toContain('daily');
      expect(labels).toContain('weekly');
    });

    it('returns null when all entries are non-finite and skipped', () => {
      const result = summarizeCrossSection({
        daily: makeHC(NaN, 20.0),
        weekly: null,
        monthly: null,
      });
      expect(result).toBeNull();
    });
  });

  describe('negative contributions', () => {
    it('negative weight × score values are preserved in parts', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, -10.0),
        weekly: makeHC(0.40, 20.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      const daily = result!.parts.find((p) => p.label === 'daily');
      expect(daily).toBeDefined();
      expect(daily!.value).toBeCloseTo(-6.0, 6);
    });

    it('total reflects negative parts', () => {
      const result = summarizeCrossSection({
        daily: makeHC(0.60, -10.0),
        weekly: makeHC(0.40, 20.0),
        monthly: null,
      });
      expect(result).not.toBeNull();
      // -6 + 8 = 2
      expect(result!.total).toBeCloseTo(2.0, 6);
    });
  });
});
