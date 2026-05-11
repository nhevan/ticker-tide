/**
 * Compact earnings row for the daily card: next earnings date + last surprise.
 */

import React from 'react';
import type { EarningsData } from '@/lib/api/types';

interface EarningsRowProps {
  /** Earnings data from the daily snapshot. */
  earnings: EarningsData | undefined;
}

/**
 * Render a two-line earnings section showing next date and last surprise.
 *
 * Renders nothing when both next and last_surprise are null.
 *
 * @param earnings - Earnings data from the daily snapshot section.
 */
export function EarningsRow({ earnings }: EarningsRowProps) {
  if (!earnings) return null;
  if (!earnings.next && !earnings.last_surprise) return null;

  return (
    <div className="mt-1 text-xs text-muted-foreground">
      {earnings.next && (
        <div>
          <span className="font-medium text-foreground">Next earnings:</span>{' '}
          {earnings.next.date}
          {earnings.next.days_until !== null && ` (in ${earnings.next.days_until}d)`}
          {earnings.next.estimated_eps !== null && ` — est. EPS $${earnings.next.estimated_eps}`}
        </div>
      )}
      {earnings.last_surprise && (
        <div>
          <span className="font-medium text-foreground">Last surprise:</span>{' '}
          {earnings.last_surprise.date} —{' '}
          {earnings.last_surprise.beat === true ? (
            <span className="text-green-600">beat</span>
          ) : earnings.last_surprise.beat === false ? (
            <span className="text-red-500">miss</span>
          ) : null}
          {earnings.last_surprise.surprise !== null &&
            ` ($${earnings.last_surprise.surprise > 0 ? '+' : ''}${earnings.last_surprise.surprise.toFixed(2)})`}
        </div>
      )}
    </div>
  );
}
