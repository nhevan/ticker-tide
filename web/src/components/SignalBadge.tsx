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

  const normalized = signal.toUpperCase();
  const colorClass =
    normalized === 'BULLISH'
      ? 'border-transparent bg-[hsl(var(--up)/0.18)] text-[hsl(var(--up))]'
      : normalized === 'BEARISH'
        ? 'border-transparent bg-[hsl(var(--down)/0.18)] text-[hsl(var(--down))]'
        : 'border-transparent bg-muted text-muted-foreground';

  return <Badge className={cn('font-mono text-xs', colorClass)}>{signal}</Badge>;
}
