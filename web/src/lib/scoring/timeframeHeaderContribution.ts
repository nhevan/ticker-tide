/**
 * Compute per-timeframe header contributions for the matrix section titles.
 *
 * Each MatrixTable header shows a mini math chain:
 *   weight × score = ▲/▼ contribution
 *
 * The weights are redistributed based on which timeframes have a finite
 * pre-blend score. When a timeframe is unavailable, its config weight is
 * reallocated proportionally to the remaining available timeframes.
 *
 * Pure, deterministic — no React imports.
 */

import type { Snapshot, ScoringRules } from '@/lib/api/types';

/** The weight/score pair emitted for a single available timeframe. */
export interface HeaderContribution {
  /** Redistributed weight (0–1) for this timeframe after availability filtering. */
  weight: number;
  /** Pre-blend score for this timeframe (daily_score, weekly/monthly composite_score). */
  score: number;
}

/** Return type: one entry per timeframe, null when unavailable or guards triggered. */
export interface TimeframeHeaderContributions {
  daily: HeaderContribution | null;
  weekly: HeaderContribution | null;
  monthly: HeaderContribution | null;
}

const NULL_RESULT: TimeframeHeaderContributions = {
  daily: null,
  weekly: null,
  monthly: null,
};

/**
 * Compute redistributed weight × score entries for each of the three
 * timeframe matrix section headers.
 *
 * Returns all nulls when:
 *   - snapshot.daily.regime is null/undefined
 *   - scoringRules is undefined
 *   - scoringRules.timeframe_weights has no entry for the regime
 *   - totalAvailable weight across finite-scored timeframes is <= 0
 *
 * For each available timeframe:
 *   weight = configWeights[tf] / totalAvailable
 *   score  = the pre-blend score for that timeframe
 *
 * Unavailable timeframes (non-finite score) return null.
 *
 * Parameters:
 *   snapshot     - Full three-card snapshot from /api/snapshot.
 *   scoringRules - Response from /api/scoring-rules. May be undefined while loading.
 *
 * Returns:
 *   TimeframeHeaderContributions with per-timeframe HC | null entries.
 */
export function computeTimeframeHeaderContributions(
  snapshot: Snapshot,
  scoringRules: ScoringRules | undefined,
): TimeframeHeaderContributions {
  const regime = snapshot.daily?.regime;
  if (!regime) {
    return NULL_RESULT;
  }

  const configWeights = scoringRules?.timeframe_weights?.[regime];
  if (!configWeights) {
    return NULL_RESULT;
  }

  // Determine which timeframes have a finite pre-blend score.
  const dailyScore = snapshot.daily?.daily_score ?? null;
  const weeklyScore = snapshot.weekly?.composite_score ?? null;
  const monthlyScore = snapshot.monthly?.composite_score ?? null;

  const dailyAvailable = Number.isFinite(dailyScore as number);
  const weeklyAvailable = Number.isFinite(weeklyScore as number);
  const monthlyAvailable = Number.isFinite(monthlyScore as number);

  // Sum config weights for available timeframes only.
  const totalAvailable =
    (dailyAvailable ? configWeights.daily : 0) +
    (weeklyAvailable ? configWeights.weekly : 0) +
    (monthlyAvailable ? configWeights.monthly : 0);

  if (totalAvailable <= 0) {
    return NULL_RESULT;
  }

  const daily: HeaderContribution | null = dailyAvailable
    ? { weight: configWeights.daily / totalAvailable, score: dailyScore as number }
    : null;

  const weekly: HeaderContribution | null = weeklyAvailable
    ? { weight: configWeights.weekly / totalAvailable, score: weeklyScore as number }
    : null;

  const monthly: HeaderContribution | null = monthlyAvailable
    ? { weight: configWeights.monthly / totalAvailable, score: monthlyScore as number }
    : null;

  return { daily, weekly, monthly };
}
