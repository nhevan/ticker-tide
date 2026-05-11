/**
 * Horizontal bar chart for per-category scores (-100 to +100).
 */

import React from 'react';
import type { CategoryScores } from '@/lib/api/types';

interface CategoryBarsProps {
  /** Ordered list of category names to display. */
  categories: string[];
  /** Map of category name to score value (float or null). */
  scores: CategoryScores;
}

/**
 * Render horizontal bars for each category score.
 *
 * Scores range from -100 to +100. Positive scores are rendered green,
 * negative scores red. Null scores are shown as a dash.
 *
 * @param categories - Category names in display order.
 * @param scores - Score values keyed by category name.
 */
export function CategoryBars({ categories, scores }: CategoryBarsProps) {
  return (
    <div className="space-y-1">
      {categories.map((category) => {
        const score = scores[category];
        const hasScore = score !== null && score !== undefined;
        const pct = hasScore ? Math.round(((score + 100) / 200) * 100) : 50;
        const isPositive = hasScore && score >= 0;

        return (
          <div key={category} className="flex items-center gap-2 text-xs">
            <span className="w-24 text-right capitalize text-muted-foreground">{category}</span>
            <div className="relative flex-1 h-3 rounded-full bg-muted overflow-hidden">
              <div
                className={`absolute top-0 h-full rounded-full ${
                  isPositive ? 'bg-green-500' : hasScore ? 'bg-red-400' : 'bg-gray-300'
                }`}
                style={{
                  left: isPositive ? '50%' : `${pct}%`,
                  width: hasScore ? `${Math.abs(score) / 2}%` : '0%',
                }}
              />
              {/* Center line */}
              <div className="absolute top-0 left-1/2 h-full w-px bg-border" />
            </div>
            <span className="w-10 font-mono text-xs">
              {hasScore ? score.toFixed(0) : '—'}
            </span>
          </div>
        );
      })}
    </div>
  );
}
