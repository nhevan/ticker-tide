/**
 * Renders one of the three timeframe cards (daily / weekly / monthly).
 *
 * Daily cards include the extra enrichments: key_signals "Why" bullets,
 * earnings row, signal/confidence, calibrated score, and signal flip badge.
 * All cards include: category bars, sparkline, patterns list, and Ask AI.
 */

import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { CategoryBars } from '@/components/CategoryBars';
import { Sparkline } from '@/components/Sparkline';
import { PatternsList } from '@/components/PatternsList';
import { WhyBullets } from '@/components/WhyBullets';
import { EarningsRow } from '@/components/EarningsRow';
import { SignalBadge } from '@/components/SignalBadge';
import { SignalFlipBadge } from '@/components/SignalFlipBadge';
import { AskAI } from '@/components/AskAI';
import type { DailySection, TimeframeSection } from '@/lib/api/types';

interface TimeframeCardProps {
  /** Card title: "Daily", "Weekly", or "Monthly". */
  title: string;
  /** Timeframe identifier for Ask AI requests. */
  timeframe: 'daily' | 'weekly' | 'monthly';
  /** Section data from the snapshot. */
  section: DailySection | TimeframeSection;
  /** Currently loaded ticker symbol. */
  ticker: string;
  /** Currently loaded date. */
  date: string;
  /** Whether the snapshot is still loading. */
  isLoading: boolean;
}

/**
 * Render a single timeframe card with all its sub-sections.
 *
 * @param title - Card heading.
 * @param timeframe - Timeframe passed to Ask AI.
 * @param section - Snapshot section data (daily, weekly, or monthly).
 * @param ticker - Ticker for Ask AI.
 * @param date - Date for Ask AI.
 * @param isLoading - Show skeleton placeholders while loading.
 */
export function TimeframeCard({
  title,
  timeframe,
  section,
  ticker,
  date,
  isLoading,
}: TimeframeCardProps) {
  const dailySection = timeframe === 'daily' ? (section as DailySection) : null;
  const resolvedLabel =
    'resolved_period_label' in section
      ? (section as TimeframeSection).resolved_period_label
      : null;

  return (
    <Card className="w-full">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">{title}</CardTitle>
          {resolvedLabel && (
            <span className="text-xs text-muted-foreground">{resolvedLabel}</span>
          )}
        </div>
        {dailySection && (
          <div className="flex flex-wrap items-center gap-2">
            <SignalBadge signal={dailySection.signal} />
            {dailySection.confidence !== null && dailySection.confidence !== undefined && (
              <span className="text-xs text-muted-foreground">
                {dailySection.confidence.toFixed(1)}% confidence
              </span>
            )}
            <SignalFlipBadge signalFlip={dailySection.signal_flip} />
          </div>
        )}
      </CardHeader>

      <CardContent className="space-y-3">
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
          </div>
        ) : !section.data_available ? (
          <p className="text-sm text-muted-foreground">No data for this period.</p>
        ) : (
          <>
            {dailySection && (
              <>
                <EarningsRow earnings={dailySection.earnings} />
                <WhyBullets keySignals={dailySection.key_signals} />
              </>
            )}

            {section.scores && (
              <CategoryBars
                categories={section.categories}
                scores={section.scores}
              />
            )}

            <Sparkline data={section.sparkline} />

            <div>
              <h4 className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Patterns
              </h4>
              <PatternsList patterns={section.patterns} />
            </div>

            <AskAI ticker={ticker} date={date} timeframe={timeframe} />
          </>
        )}
      </CardContent>
    </Card>
  );
}
