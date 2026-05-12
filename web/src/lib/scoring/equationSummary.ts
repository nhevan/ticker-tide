/**
 * Pure helper functions for building equation summary data from
 * contributions_payload items.
 *
 * No React imports. All numeric guards use Number.isFinite per CLAUDE.md gotcha #4.
 */

import type { ContributionItem } from '@/lib/api/types';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Data for a per-section equation row rendered below the matrix table header. */
export interface SectionEquationData {
  /** All items sorted by abs(contribution) descending, each with raw name and value. */
  items: { label: string; value: number }[];
  /** The timeframe's authoritative pre-blend score (headerContribution.score). */
  total: number;
}

/** Data for the cross-section banner rendered above the three matrix tables. */
export interface CrossSectionData {
  /** Each available timeframe's contribution (weight × score). */
  parts: { label: 'daily' | 'weekly' | 'monthly'; value: number }[];
  /** Sum of all part values. */
  total: number;
}

// ---------------------------------------------------------------------------
// summarizeSectionContributions
// ---------------------------------------------------------------------------

/**
 * Build section equation data from a contributions_payload items array.
 *
 * Items are sorted by abs(contribution) descending. Items with non-finite
 * contributions are skipped. All remaining items are included. The total
 * comes from the target parameter (the timeframe's authoritative pre-blend
 * score), NOT from summing items — the ≈ symbol acknowledges that gap.
 *
 * Parameters:
 *   items - ContributionItem array from contributions_payload.items, or undefined.
 *   target - The timeframe's pre-blend score (headerContribution.score). Must be
 *            a finite number; null/undefined/NaN/Infinity causes null return.
 *
 * Returns:
 *   SectionEquationData when inputs are valid and items is non-empty after filtering,
 *   null otherwise.
 */
export function summarizeSectionContributions(
  items: ContributionItem[] | undefined,
  target: number | null | undefined,
): SectionEquationData | null {
  if (!Number.isFinite(target)) return null;
  if (!items || items.length === 0) return null;

  // Filter out non-finite and zero contributions: zero items are visual noise
  // (e.g. divergence patterns that didn't fire under the current regime weights).
  const finite = items.filter(
    (item) => Number.isFinite(item.contribution) && item.contribution !== 0,
  );
  if (finite.length === 0) return null;

  const sorted = finite.slice().sort(
    (itemA, itemB) => Math.abs(itemB.contribution) - Math.abs(itemA.contribution),
  );

  return {
    items: sorted.map((item) => ({ label: item.name, value: item.contribution })),
    total: target as number,
  };
}

// ---------------------------------------------------------------------------
// summarizeCrossSection
// ---------------------------------------------------------------------------

/**
 * Build cross-section banner data from the per-timeframe header contributions.
 *
 * Each entry with both finite weight and finite score produces a part with
 * value = weight × score. Entries that are null or have non-finite weight/score
 * are skipped. Returns null when no valid entries remain.
 */
export function summarizeCrossSection(
  headerContributions: {
    daily: { weight: number; score: number } | null;
    weekly: { weight: number; score: number } | null;
    monthly: { weight: number; score: number } | null;
  },
): CrossSectionData | null {
  const labels: ('daily' | 'weekly' | 'monthly')[] = ['daily', 'weekly', 'monthly'];
  const parts: { label: 'daily' | 'weekly' | 'monthly'; value: number }[] = [];

  for (const label of labels) {
    const hc = headerContributions[label];
    if (!hc) continue;
    if (!Number.isFinite(hc.weight) || !Number.isFinite(hc.score)) continue;
    parts.push({ label, value: hc.weight * hc.score });
  }

  if (parts.length === 0) return null;

  const total = parts.reduce((acc, part) => acc + part.value, 0);
  return { parts, total };
}
