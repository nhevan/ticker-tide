/**
 * Badge displayed when a signal flip occurred within the lookback window.
 */

import React from 'react';
import type { SignalFlip } from '@/lib/api/types';

interface SignalFlipBadgeProps {
  /** Signal flip data from the daily snapshot, or null if none. */
  signalFlip: SignalFlip | null | undefined;
}

/**
 * Display a one-line flip summary when a recent signal flip is present.
 *
 * Renders nothing when signalFlip is null or undefined.
 *
 * @param signalFlip - The signal flip data from the daily section.
 */
export function SignalFlipBadge({ signalFlip }: SignalFlipBadgeProps) {
  if (!signalFlip) return null;

  const daysText =
    signalFlip.days_ago === 0
      ? 'today'
      : signalFlip.days_ago === 1
        ? '1 day ago'
        : `${signalFlip.days_ago} days ago`;

  return (
    <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
      Flip: {signalFlip.previous_signal} → {signalFlip.new_signal} ({daysText})
    </span>
  );
}
