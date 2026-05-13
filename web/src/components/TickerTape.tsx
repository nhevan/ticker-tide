/**
 * Render the static ticker tape strip showing the currently loaded ticker's
 * price, day-over-day change, and signal pill. Derives price and % change from
 * the last two sparkline points in snapshot.daily.sparkline. Renders skeleton
 * during loading, ERR on error, and em-dash placeholders before any ticker is
 * loaded.
 *
 * @param props - TickerTapeProps with ticker, snapshot, isLoading, and error.
 */

import { useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import type { Snapshot } from '@/lib/api/types';
import { SignalClassificationTooltip } from '@/components/SignalClassificationTooltip';

export interface TickerTapeProps {
  /** Currently loaded ticker symbol. Empty string = placeholder mode. */
  ticker: string;
  /** Full snapshot from the API. */
  snapshot: Snapshot | undefined;
  /** Whether the snapshot is currently being fetched. */
  isLoading: boolean;
  /** Error message string if the snapshot fetch failed, or null. */
  error: string | null;
}

/** Known signal values (compared case-insensitively). */
const KNOWN_SIGNALS = new Set(['BULLISH', 'BEARISH', 'NEUTRAL']);

/**
 * Derive price and percent change from the last two sparkline close values.
 *
 * @param snapshot - The full snapshot, or undefined.
 * @returns Object with price string, changePct number or null, and asOfDate string or null.
 */
function deriveSparklineValues(snapshot: Snapshot | undefined): {
  price: string | null;
  changePct: number | null;
  asOfDate: string | null;
} {
  const points = snapshot?.daily?.sparkline;
  if (!points || points.length === 0) {
    return { price: null, changePct: null, asOfDate: null };
  }
  const last = points[points.length - 1];
  const price = last.close;
  const asOfDate = last.date ?? null;

  if (points.length === 1) {
    return { price: price.toFixed(2), changePct: null, asOfDate };
  }

  const prior = points[points.length - 2].close;
  if (prior === 0) {
    return { price: price.toFixed(2), changePct: null, asOfDate };
  }

  const changePct = ((price - prior) / prior) * 100;
  return { price: price.toFixed(2), changePct, asOfDate };
}

/**
 * Format the percent change for display, with sign prefix.
 *
 * @param changePct - Numeric percent change, or null for unavailable.
 * @returns Formatted string like "+5.00%" or "-3.12%" or "—%".
 */
function formatChangePct(changePct: number | null): string {
  if (changePct === null) return '—%';
  const sign = changePct >= 0 ? '+' : '';
  return `${sign}${changePct.toFixed(2)}%`;
}

/**
 * Resolve the pill className for a signal string, using --up/--down theme vars
 * so bullish reads as green and bearish as red regardless of palette.
 *
 * @param signal - Signal string from snapshot.daily.signal, or null.
 * @returns Tailwind className string for the Badge.
 */
function signalPillClass(signal: string | null | undefined): string {
  if (!signal) return '';
  const normalized = signal.toUpperCase();
  if (normalized === 'BULLISH') {
    return 'border-transparent bg-[hsl(var(--up)/0.18)] text-[hsl(var(--up))] hover:bg-[hsl(var(--up)/0.25)]';
  }
  if (normalized === 'BEARISH') {
    return 'border-transparent bg-[hsl(var(--down)/0.18)] text-[hsl(var(--down))] hover:bg-[hsl(var(--down)/0.25)]';
  }
  // neutral or unknown: muted
  return 'border-transparent bg-muted text-muted-foreground';
}

/**
 * Static ticker tape strip mounted above main content.
 *
 * Shows SYMBOL · PRICE · ±CHANGE% · [SIGNAL PILL] for the currently loaded ticker.
 * Renders placeholders (em-dashes) before any ticker is loaded, a skeleton during
 * loading, and ERR when an error is present.
 *
 * @param props - See TickerTapeProps.
 */
export function TickerTape({ ticker, snapshot, isLoading, error }: TickerTapeProps) {
  const { price, changePct, asOfDate } = deriveSparklineValues(snapshot);
  const [pillOpen, setPillOpen] = useState(false);

  const signal = snapshot?.daily?.signal ?? null;
  const confidence = snapshot?.daily?.confidence ?? null;

  const isPlaceholder = !ticker && !snapshot && !error && !isLoading;
  const hasData = Boolean(snapshot && price !== null);

  // Build pill label: signal string + optional confidence suffix
  function pillLabel(): string {
    if (!signal) return '—';
    const suffix = confidence !== null ? ` · ${Math.round(confidence)}%` : '';
    return `${signal}${suffix}`;
  }

  return (
    <div
      className="w-full border-b border-border bg-card"
      role="status"
      aria-live="polite"
    >
      <div
        className={`mx-auto max-w-6xl px-4 h-10 flex items-center gap-4 ${isPlaceholder ? 'opacity-70' : ''}`}
      >
        {isLoading ? (
          <div data-testid="tape-skeleton" className="flex items-center gap-4">
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-4 w-20" />
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-6 w-24" />
          </div>
        ) : error ? (
          <>
            {ticker && (
              <span className="font-mono font-bold tracking-wider text-foreground">
                {ticker}
              </span>
            )}
            <span className="mx-1 text-muted-foreground">·</span>
            <span className="font-mono tabular-nums text-[hsl(var(--down))]">ERR</span>
            <span className="mx-1 text-muted-foreground">·</span>
            <Badge variant="outline">—</Badge>
          </>
        ) : (
          <>
            <span className="font-mono font-bold tracking-wider text-foreground">
              {ticker || '—'}
            </span>
            <span className="mx-1 text-muted-foreground">·</span>
            <span className="font-mono tabular-nums text-foreground">
              {hasData ? price : '—'}
            </span>
            <span
              data-direction={
                changePct === null || !hasData ? 'flat' : changePct >= 0 ? 'up' : 'down'
              }
              className={
                changePct === null || !hasData
                  ? 'font-mono tabular-nums text-muted-foreground'
                  : changePct >= 0
                  ? 'font-mono tabular-nums text-[hsl(var(--up))]'
                  : 'font-mono tabular-nums text-[hsl(var(--down))]'
              }
            >
              {hasData ? formatChangePct(changePct) : '—'}
            </span>
            <span className="mx-1 text-muted-foreground">·</span>
            {hasData && signal ? (
              <TooltipProvider delayDuration={0}>
                <Tooltip open={pillOpen} onOpenChange={setPillOpen}>
                  <TooltipTrigger asChild>
                    <button
                      type="button"
                      onClick={() => setPillOpen((v) => !v)}
                      className="focus:outline-none focus:ring-2 focus:ring-ring rounded-md"
                    >
                      <Badge
                        data-signal={signal}
                        className={`${signalPillClass(signal)} cursor-pointer`}
                      >
                        {KNOWN_SIGNALS.has(signal.toUpperCase()) ? pillLabel() : signal}
                      </Badge>
                    </button>
                  </TooltipTrigger>
                  <TooltipContent
                    side="bottom"
                    align="start"
                    className="max-w-none p-0 bg-card text-card-foreground border-border shadow-xl backdrop-blur-none opacity-100"
                    onPointerDownOutside={() => setPillOpen(false)}
                  >
                    <SignalClassificationTooltip
                      daily={snapshot!.daily}
                      weekly={snapshot!.weekly?.data_available ? snapshot!.weekly : null}
                      monthly={snapshot!.monthly?.data_available ? snapshot!.monthly : null}
                    />
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            ) : (
              <Badge variant="outline">—</Badge>
            )}
            {asOfDate && (
              <span className="text-xs text-muted-foreground">as of {asOfDate}</span>
            )}
          </>
        )}
      </div>
    </div>
  );
}
