/**
 * SignalClassificationTooltip — real-data tooltip body for the signal pill.
 *
 * Renders the 3-step math chain:
 *   Step 1a — sector adjustment (raw_daily + sector_adj → daily_score)
 *   Step 1b — timeframe merge  (daily × w_d + weekly × w_w + monthly × w_m → composite)
 *   Step 2  — pick effective score (calibrated if available, else composite)
 *   Step 3  — classify against signal_thresholds from /api/scoring-rules
 *
 * All numeric props are guarded with Number.isFinite before formatting.
 * While scoringRules is loading or absent the component renders a loading state.
 */

import { useScoringRules } from '@/lib/hooks/useScoringRules';
import type { DailySection, TimeframeSection } from '@/lib/api/types';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Format a number with sign prefix, 1 decimal place. */
function fmt1(n: number): string {
  return n >= 0 ? `+${n.toFixed(1)}` : n.toFixed(1);
}

/** Format a number with sign prefix, 2 decimal places. */
function fmt2(n: number): string {
  return n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2);
}

// ── Props ─────────────────────────────────────────────────────────────────────

export interface SignalClassificationTooltipProps {
  /** Daily section from the snapshot. */
  daily: DailySection;
  /** Weekly section from the snapshot, or null when unavailable. */
  weekly: TimeframeSection | null;
  /** Monthly section from the snapshot, or null when unavailable. */
  monthly: TimeframeSection | null;
}

// ── Component ─────────────────────────────────────────────────────────────────

/**
 * Tooltip body for the signal pill showing the full signal-classification math chain.
 *
 * @param props - daily, weekly, monthly snapshot sections.
 */
export function SignalClassificationTooltip({
  daily,
  weekly,
  monthly,
}: SignalClassificationTooltipProps) {
  const { data: scoringRules } = useScoringRules();

  // While scoringRules has not loaded yet, render a minimal loading state.
  if (!scoringRules) {
    return (
      <div className="w-[460px] p-4 font-sans text-xs text-muted-foreground">
        Loading classification rules…
      </div>
    );
  }

  // ── Extract values ──────────────────────────────────────────────────────────

  const regime = daily.regime ?? 'trending';

  // Step 1a — sector adjustment
  const rawDaily = Number.isFinite(daily.raw_daily_score as number)
    ? (daily.raw_daily_score as number)
    : null;
  const dailyScore = Number.isFinite(daily.daily_score as number)
    ? (daily.daily_score as number)
    : null;
  const sectorEtfScore = Number.isFinite(daily.sector_etf_score as number)
    ? (daily.sector_etf_score as number)
    : null;
  const sectorEtf = daily.sector_etf ?? null;

  // Sector adjustment is derived as the effective (clamp-aware) difference.
  const sectorAdj =
    rawDaily !== null && dailyScore !== null ? dailyScore - rawDaily : null;

  // Whether we have enough data for the sector sub-step.
  const hasSectorData =
    rawDaily !== null && dailyScore !== null && sectorEtf !== null && sectorEtfScore !== null;

  // Step 1b — timeframe merge
  const compositeScore = Number.isFinite(daily.composite_score as number)
    ? (daily.composite_score as number)
    : null;

  const weeklyScore = Number.isFinite(weekly?.composite_score as number)
    ? (weekly!.composite_score as number)
    : null;
  const monthlyScore = Number.isFinite(monthly?.composite_score as number)
    ? (monthly!.composite_score as number)
    : null;

  const baseWeights = scoringRules.timeframe_weights[regime] ?? {
    daily: 0.34,
    weekly: 0.33,
    monthly: 0.33,
  };

  const availDaily = dailyScore !== null;
  const availWeekly = weeklyScore !== null;
  const availMonthly = monthlyScore !== null;

  const totalAvailWeight =
    (availDaily ? baseWeights.daily : 0) +
    (availWeekly ? baseWeights.weekly : 0) +
    (availMonthly ? baseWeights.monthly : 0);

  const effectiveWeights =
    totalAvailWeight > 0
      ? {
          daily: availDaily ? baseWeights.daily / totalAvailWeight : 0,
          weekly: availWeekly ? baseWeights.weekly / totalAvailWeight : 0,
          monthly: availMonthly ? baseWeights.monthly / totalAvailWeight : 0,
        }
      : { daily: 0, weekly: 0, monthly: 0 };

  // Reconstruct final score for display (may differ from persisted by rounding).
  const reconstructed =
    (availDaily ? (dailyScore as number) * effectiveWeights.daily : 0) +
    (availWeekly ? (weeklyScore as number) * effectiveWeights.weekly : 0) +
    (availMonthly ? (monthlyScore as number) * effectiveWeights.monthly : 0);

  // Step 2 — pick effective score
  const calibratedScore = Number.isFinite(daily.calibrated_score as number)
    ? (daily.calibrated_score as number)
    : null;
  const calibratorAvailable = calibratedScore !== null;
  const effective = calibratedScore !== null ? calibratedScore : compositeScore;

  // Step 3 — classify
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
    <div className="w-[460px] p-4 font-mono text-xs leading-relaxed">
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

      {/* Step 1a — sector adjustment */}
      <div className="mb-3">
        <div className="mb-1 font-sans text-[11px] font-semibold text-foreground">
          Step 1a — sector adjustment
        </div>
        <div className="space-y-0.5 pl-2">
          {hasSectorData ? (
            <>
              <div>
                <span className="text-muted-foreground">daily</span> ={' '}
                <span className="text-muted-foreground">raw_daily + sector_adj</span>
              </div>
              <div>
                {'     '}= {fmt1(rawDaily as number)} + {fmt1(sectorAdj as number)}
              </div>
              <div className="text-foreground">
                {'     '}= <span className="font-semibold">{fmt1(dailyScore as number)}</span>
              </div>
              <div className="pt-1 font-sans text-[10px] text-muted-foreground">
                sector ETF ({sectorEtf}) score {fmt1(sectorEtfScore as number)} → {fmt1(sectorAdj as number)} bump
                {' '}
                <span className="italic">(effective adj; apply_sector_adjustment clamps to ±100)</span>
              </div>
            </>
          ) : rawDaily !== null && dailyScore !== null ? (
            <>
              <div>
                <span className="text-muted-foreground">daily</span> ={' '}
                <span className="font-semibold">{fmt1(dailyScore)}</span>
              </div>
              <div className="pt-1 font-sans text-[10px] text-muted-foreground">
                No sector ETF mapped for this ticker — sector adjustment is 0.
              </div>
            </>
          ) : dailyScore !== null ? (
            <>
              <div>
                <span className="text-muted-foreground">daily</span> ={' '}
                <span className="font-semibold">{fmt1(dailyScore)}</span>
              </div>
              <div className="pt-1 font-sans text-[10px] text-muted-foreground">
                Sector adjustment breakdown unavailable for this row.
              </div>
            </>
          ) : (
            <div className="font-sans text-[10px] text-muted-foreground">
              Daily score unavailable.
            </div>
          )}
        </div>
      </div>

      {/* Step 1b — timeframe merge */}
      <div className="mb-3">
        <div className="mb-1 font-sans text-[11px] font-semibold text-foreground">
          Step 1b — timeframe merge (weights for {regime})
        </div>
        <div className="space-y-0.5 pl-2">
          <div>
            <span className="text-muted-foreground">final</span> ={' '}
            <span className="text-muted-foreground">
              {[
                availDaily && 'daily·w_d',
                availWeekly && 'weekly·w_w',
                availMonthly && 'monthly·w_m',
              ]
                .filter(Boolean)
                .join(' + ')}
            </span>
          </div>
          <div>
            {'      '}={' '}
            {[
              availDaily && `${fmt1(dailyScore as number)}·${effectiveWeights.daily.toFixed(2)}`,
              availWeekly && `${fmt1(weeklyScore as number)}·${effectiveWeights.weekly.toFixed(2)}`,
              availMonthly && `${fmt1(monthlyScore as number)}·${effectiveWeights.monthly.toFixed(2)}`,
            ]
              .filter(Boolean)
              .join(' + ')}
          </div>
          <div>
            {'      '}={' '}
            {[
              availDaily &&
                fmt2((dailyScore as number) * effectiveWeights.daily),
              availWeekly &&
                fmt2((weeklyScore as number) * effectiveWeights.weekly),
              availMonthly &&
                fmt2((monthlyScore as number) * effectiveWeights.monthly),
            ]
              .filter(Boolean)
              .join(' + ')}
          </div>
          {compositeScore !== null ? (
            <div className="text-foreground">
              {'      '}={' '}
              <span className="font-semibold">{fmt1(compositeScore)}</span>{' '}
              <span className="font-sans text-[10px] text-muted-foreground">
                (persisted; reconstructed ≈ {fmt1(reconstructed)}, clamped to [−100, +100])
              </span>
            </div>
          ) : (
            <div className="text-foreground">
              {'      '}= <span className="font-semibold">{fmt1(reconstructed)}</span>{' '}
              <span className="font-sans text-[10px] text-muted-foreground">
                (clamped to [−100, +100])
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Step 2 — pick effective score */}
      <div className="mb-3">
        <div className="mb-1 font-sans text-[11px] font-semibold text-foreground">
          Step 2 — pick effective score
        </div>
        <div className="pl-2">
          <div className="mb-2 font-sans text-[10px] text-muted-foreground">
            Two candidate scores exist; the threshold compare in Step 3 uses one of them.
          </div>
          <div className="mb-2 space-y-1">
            <div className="grid grid-cols-[80px_70px_1fr] items-baseline gap-x-2">
              <span className="text-muted-foreground">final</span>
              <span className="text-foreground">
                = {compositeScore !== null ? fmt1(compositeScore) : '—'}
              </span>
              <span className="font-sans text-[10px] text-muted-foreground">
                formula composite, scale [−100, +100]
              </span>
            </div>
            <div className="grid grid-cols-[80px_70px_1fr] items-baseline gap-x-2">
              <span className="text-muted-foreground">calibrated</span>
              <span className="text-foreground">
                ={' '}
                {calibratorAvailable ? (
                  fmt1(calibratedScore as number)
                ) : (
                  <span className="text-muted-foreground">— (cold start)</span>
                )}
              </span>
              <span className="font-sans text-[10px] text-muted-foreground">
                model prediction, % excess return vs SPY over 10d
              </span>
            </div>
          </div>
          <div className="space-y-0.5">
            <div>
              <span className="text-muted-foreground">effective</span> ={' '}
              <span className="text-muted-foreground">
                calibrated  if available  else  final
              </span>
            </div>
            <div className="text-foreground">
              {'          '}={' '}
              <span className="font-semibold">
                {effective !== null ? fmt1(effective) : '—'}
              </span>{' '}
              <span className="font-sans text-[10px] text-muted-foreground">
                (
                {calibratorAvailable
                  ? 'calibrator trained — using its prediction'
                  : 'calibrator has too little history — falling back to formula'}
                )
              </span>
            </div>
          </div>
          <div className="mt-2 font-sans text-[10px] leading-relaxed text-muted-foreground">
            The calibrator is a ridge regression trained nightly on the trailing 365 days of
            signals and their realized 10-day returns vs SPY. It maps a 17-feature vector
            (category scores, raw indicators, EMA spreads, weekly + monthly composites) to a
            forecasted excess return. Preferred over <span className="font-mono">final</span>{' '}
            because it&apos;s grounded in actual outcomes, not a hand-tuned formula.
          </div>
        </div>
      </div>

      {/* Step 3 — classify against thresholds */}
      <div>
        <div className="mb-1 font-sans text-[11px] font-semibold text-foreground">
          Step 3 — classify against thresholds
        </div>
        <div className="space-y-0.5 pl-2">
          <div>
            <span className="text-muted-foreground">signal</span> ={' '}
            <span className="text-muted-foreground">BULLISH</span> if effective ≥ t_bull (
            {fmt1(tBull)})
          </div>
          <div>
            {'       '}= <span className="text-muted-foreground">BEARISH</span> if effective ≤
            t_bear ({fmt1(tBear)})
          </div>
          <div>{'       '}= NEUTRAL otherwise</div>
          {effective !== null && signal !== null ? (
            <>
              <div>
                {'       '}= {signal} if{' '}
                {fmt1(effective)}{' '}
                {signal === 'BULLISH' ? '≥' : signal === 'BEARISH' ? '≤' : '∈'}{' '}
                {signal === 'BULLISH'
                  ? fmt1(tBull)
                  : signal === 'BEARISH'
                  ? fmt1(tBear)
                  : `(${fmt1(tBear)}, ${fmt1(tBull)})`}{' '}
                ✓
              </div>
              <div className="text-foreground">
                {'       '}={' '}
                <span
                  className={`font-semibold ${
                    signal === 'BULLISH'
                      ? 'text-[hsl(var(--up))]'
                      : signal === 'BEARISH'
                      ? 'text-[hsl(var(--down))]'
                      : ''
                  }`}
                >
                  {signal}
                </span>
              </div>
            </>
          ) : (
            <div className="font-sans text-[10px] text-muted-foreground">
              Effective score unavailable — classification cannot be shown.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
