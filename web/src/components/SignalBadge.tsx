/**
 * Colored badge for BULLISH / BEARISH / NEUTRAL signal values.
 */

import React from 'react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

interface SignalBadgeProps {
  /** Signal string from the API (e.g. "BULLISH", "BEARISH", "NEUTRAL"). */
  signal: string | null | undefined;
}

/**
 * Render a colored badge for a signal value.
 *
 * BULLISH → green, BEARISH → red, NEUTRAL / other → muted.
 *
 * @param signal - The signal string from the daily snapshot.
 */
export function SignalBadge({ signal }: SignalBadgeProps) {
  if (!signal) return null;

  const colorClass =
    signal === 'BULLISH'
      ? 'bg-green-100 text-green-800 border-green-200'
      : signal === 'BEARISH'
        ? 'bg-red-100 text-red-800 border-red-200'
        : 'bg-gray-100 text-gray-700 border-gray-200';

  return <Badge className={cn('font-mono text-xs', colorClass)}>{signal}</Badge>;
}
