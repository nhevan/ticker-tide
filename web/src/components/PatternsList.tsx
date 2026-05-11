/**
 * List of detected patterns for a timeframe card.
 */

import React from 'react';
import type { Pattern } from '@/lib/api/types';

interface PatternsListProps {
  /** Patterns array from the snapshot section. */
  patterns: Pattern[] | undefined;
}

/**
 * Render a compact list of detected patterns.
 *
 * Shows "None detected" when the patterns array is empty or undefined.
 *
 * @param patterns - Array of pattern objects from the snapshot API.
 */
export function PatternsList({ patterns }: PatternsListProps) {
  if (!patterns || patterns.length === 0) {
    return <p className="text-xs text-muted-foreground">None detected.</p>;
  }

  return (
    <ul className="space-y-0.5 text-xs">
      {patterns.map((pattern, index) => (
        <li key={index} className="flex items-center gap-1.5">
          <span
            className={`inline-block h-1.5 w-1.5 rounded-full ${
              pattern.direction === 'bullish'
                ? 'bg-green-500'
                : pattern.direction === 'bearish'
                  ? 'bg-red-400'
                  : 'bg-gray-400'
            }`}
          />
          <span className="font-medium">{pattern.pattern_name}</span>
          <span className="text-muted-foreground">
            ({pattern.direction}, str={pattern.strength})
          </span>
        </li>
      ))}
    </ul>
  );
}
