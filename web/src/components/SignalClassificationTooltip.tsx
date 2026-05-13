/**
 * SignalClassificationTooltip — tooltip body for the signal pill.
 *
 * Shows the effective signal score (calibrated if the ridge model has enough
 * history, otherwise the formula composite), the bullish/bearish thresholds
 * from /api/scoring-rules, and the resulting BULLISH / NEUTRAL / BEARISH
 * classification.  The regime and calibrator-availability status are shown in
 * the header so the user understands which score path was taken.
 *
 * All numeric props are guarded with Number.isFinite before formatting.
 * While scoringRules is loading or absent the component renders a loading state.
 */

import { useScoringRules } from '@/lib/hooks/useScoringRules';
import type { DailySection } from '@/lib/api/types';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Format a number with sign prefix, 1 decimal place. */
function fmt1(n: number): string {
  return n >= 0 ? `+${n.toFixed(1)}` : n.toFixed(1);
}

// ── Props ─────────────────────────────────────────────────────────────────────

export interface SignalClassificationTooltipProps {
  /** Daily section from the snapshot. */
  daily: DailySection;
}

// ── Component ─────────────────────────────────────────────────────────────────

/**
 * Tooltip body for the signal pill showing the effective score vs thresholds
 * and the resulting signal classification.
 *
 * @param props - daily snapshot section.
 */
export function SignalClassificationTooltip({
  daily,
}: SignalClassificationTooltipProps) {
  const { data: scoringRules } = useScoringRules();

  // While scoringRules has not loaded yet, render a minimal loading state.
  if (!scoringRules) {
    return (
      <div className="min-w-[280px] w-auto p-4 font-sans text-xs text-muted-foreground">
        Loading classification rules…
      </div>
    );
  }

  // ── Extract values ──────────────────────────────────────────────────────────

  const regime = daily.regime ?? 'trending';

  // Pick effective score: calibrated if available, else composite.
  const calibratedScore = Number.isFinite(daily.calibrated_score as number)
    ? (daily.calibrated_score as number)
    : null;
  const compositeScore = Number.isFinite(daily.composite_score as number)
    ? (daily.composite_score as number)
    : null;

  const calibratorAvailable = calibratedScore !== null;
  const effective = calibratedScore !== null ? calibratedScore : compositeScore;

  // Step 3 — classify against thresholds.
  const tBull = scoringRules.signal_thresholds.bullish;
  const tBear = scoringRules.signal_thresholds.bearish;
  const signal: string | null =
    effective !== null
      ? effective >= tBull
        ? 'BULLISH'
        : effective <= tBear
        ? 'BEARISH'
        : 'NEUTRAL'
      : null;

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div className="min-w-[280px] w-auto p-4 font-mono text-xs leading-relaxed">
      {/* Header */}
      <div className="mb-2 flex items-baseline justify-between">
        <span className="font-sans text-sm font-semibold text-foreground">
          Signal classification
        </span>
      </div>
      <div className="mb-3 font-sans text-[11px] text-muted-foreground">
        Regime: <span className="text-foreground">{regime}</span>
        {'  ·  '}
        Calibrator:{' '}
        <span className="text-foreground">
          {calibratorAvailable ? 'available' : 'cold start'}
        </span>
      </div>

      {/* Plain-prose classification */}
      {effective !== null && signal !== null ? (
        <div className="font-sans text-[12px] leading-relaxed text-foreground">
          The model's prediction value is{' '}
          <span className="font-semibold text-foreground">{fmt1(effective)}</span>{' '}
          <span className="text-[10px] text-muted-foreground">
            {calibratorAvailable
              ? '(calibrated)'
              : '(formula composite — cold start)'}
          </span>
          .{' '}
          {signal === 'BULLISH' ? (
            <>
              Since it is at or above the bullish threshold ({fmt1(tBull)}),
              this means{' '}
              <span className="font-semibold text-[hsl(var(--up))]">BULLISH</span>.
            </>
          ) : signal === 'BEARISH' ? (
            <>
              Since it is at or below the bearish threshold ({fmt1(tBear)}),
              this means{' '}
              <span className="font-semibold text-[hsl(var(--down))]">BEARISH</span>.
            </>
          ) : (
            <>
              Since it sits between the bearish ({fmt1(tBear)}) and bullish (
              {fmt1(tBull)}) thresholds, this means{' '}
              <span className="font-semibold text-foreground">NEUTRAL</span>.
            </>
          )}
        </div>
      ) : (
        <div className="font-sans text-[11px] text-muted-foreground">
          Effective score unavailable — classification cannot be shown.
        </div>
      )}
    </div>
  );
}
