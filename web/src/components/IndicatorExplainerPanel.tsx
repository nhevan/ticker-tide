/**
 * IndicatorExplainerPanel — click-to-expand detail panel for indicator rows.
 *
 * For indicator === "rsi_14", renders a faithful 7-step trace using only
 * server-computed data. All other indicators show a placeholder.
 *
 * IMPORTANT: payload.items[*].raw_value for indicators stores the SCORE
 * (−100 to +100), NOT the raw measurement. Step 1 of the RSI trace reads
 * snapshot.daily.indicators.rsi_14 (the true RSI reading), never raw_value.
 */

import { RsiTrendChart } from '@/components/RsiTrendChart';
import { RsiPercentileStrip } from '@/components/RsiPercentileStrip';
import type { Snapshot, ScoringRules, ContributionItem } from '@/lib/api/types';

/** Human-friendly prose fragments for zone label strings (profile path). */
const ZONE_LABEL_DESCRIPTIONS: Record<string, string> = {
  extreme_oversold: "Below p5 of this ticker's historical RSI",
  oversold: "Between p5 and p20 of this ticker's historical RSI",
  below_mid: "Between p20 and p50 of this ticker's historical RSI",
  above_mid: "Between p50 and p80 of this ticker's historical RSI",
  overbought: "Between p80 and p95 of this ticker's historical RSI",
  extreme_overbought: "Above p95 of this ticker's historical RSI",
};

/** Human-friendly prose fragments for zone label strings (fallback path). */
const FALLBACK_ZONE_DESCRIPTIONS: Record<string, string> = {
  oversold: 'Below the fixed oversold threshold',
  below_mid: 'Between oversold and the midpoint',
  above_mid: 'Between the midpoint and overbought',
  overbought: 'Above the fixed overbought threshold',
};

interface IndicatorExplainerPanelProps {
  indicator: string;
  snapshot: Snapshot;
  rules: ScoringRules | undefined;
}

/** Wrapper for a single explainer step card. */
function StepCard({
  stepNumber,
  heading,
  children,
  unavailable = false,
}: {
  stepNumber: number;
  heading: string;
  children?: React.ReactNode;
  unavailable?: boolean;
}) {
  return (
    <div className="mb-2 rounded border border-border bg-card p-3">
      <div className="mb-1 text-xs font-semibold text-muted-foreground">
        Step {stepNumber}: {heading}
      </div>
      {unavailable ? (
        <p className="text-xs text-muted-foreground italic">Not available.</p>
      ) : (
        <div className="text-xs text-foreground">{children}</div>
      )}
    </div>
  );
}

/** Placeholder shown for indicators other than rsi_14. */
function PlaceholderPanel({ indicator }: { indicator: string }) {
  return (
    <div className="border-t border-border/40 bg-card px-4 py-3 text-xs text-muted-foreground">
      <span className="font-semibold text-foreground">{indicator}</span>
      {' — '}Detailed explanation coming soon.
    </div>
  );
}

/** Render the full 7-step RSI trace. */
function RsiPanel({ snapshot, rules }: { snapshot: Snapshot; rules: ScoringRules | undefined }) {
  const daily = snapshot.daily;

  // Step 1 — Raw value.
  // IMPORTANT: read from indicators.rsi_14 (the actual RSI reading), NOT from
  // contributions_payload items' raw_value (which stores the indicator score −100..+100).
  const rsiRaw = daily.indicators?.rsi_14;
  const rsiValue = typeof rsiRaw === 'number' ? rsiRaw : null;

  if (rsiValue === null) {
    return (
      <div className="border-t border-border/40 bg-card px-4 py-3">
        <StepCard stepNumber={1} heading="RSI(14) reading">
          RSI not available for this date.
        </StepCard>
      </div>
    );
  }

  const rsiProfile = daily.rsi_profile ?? null;
  const zoneLabel = daily.rsi_zone_label ?? null;
  const rsiScore = daily.indicator_scores?.['rsi_14'] ?? null;
  const regime = daily.regime ?? 'ranging';
  const contributions = daily.contributions_payload ?? null;

  // Step 5–7 require the RSI item from the contributions payload.
  const rsiItem: ContributionItem | undefined = contributions?.items.find(
    (item) => item.name === 'rsi_14',
  );

  return (
    <div className="border-t border-border/40 bg-card px-4 py-3 space-y-0">
      {/* Step 1 — Raw value */}
      <StepCard stepNumber={1} heading="RSI(14) reading">
        RSI is {rsiValue.toFixed(1)}.
      </StepCard>

      {/* Step 2 — Zone label with RSI trend chart */}
      <StepCard stepNumber={2} heading="Zone">
        <RsiTrendChart data={daily.rsi_sparkline ?? []} />
        {zoneLabel ? (
          <div className="flex items-center gap-2 mt-2">
            <span className="font-mono text-[10px] uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
              {zoneLabel}
            </span>
            <span>
              {rsiProfile
                ? (ZONE_LABEL_DESCRIPTIONS[zoneLabel] ?? '')
                : (FALLBACK_ZONE_DESCRIPTIONS[zoneLabel] ?? '')}
            </span>
            {!rsiProfile && (
              <span className="text-muted-foreground italic">
                (fallback — no per-ticker profile yet)
              </span>
            )}
          </div>
        ) : (
          <span className="text-muted-foreground italic">Zone label unavailable.</span>
        )}
      </StepCard>

      {/* Step 3 — Scoring path */}
      <StepCard stepNumber={3} heading="Scoring path">
        {rsiProfile ? (
          <>
            <RsiPercentileStrip
              profile={rsiProfile}
              today={rsiValue}
              zoneLabel={zoneLabel}
              zoneDescription={zoneLabel ? (ZONE_LABEL_DESCRIPTIONS[zoneLabel] ?? '') : ''}
            />
            <p className="mt-2">
              <span className="font-medium">Percentile profile path.</span> This ticker has enough
              history for the engine to use its own RSI distribution rather than textbook 30/70
              thresholds.
            </p>
          </>
        ) : (
          <>
            <span className="font-medium">Fixed threshold fallback path</span> (no per-ticker
            profile available). Thresholds: oversold={rules?.rsi.thresholds.oversold ?? 30},{' '}
            overbought={rules?.rsi.thresholds.overbought ?? 70}.
          </>
        )}
      </StepCard>

      {/* Step 4 — Indicator score */}
      {rsiScore === null ? (
        <>
          <StepCard stepNumber={4} heading="RSI score" unavailable />
          <StepCard stepNumber={5} heading="Magnitude share in momentum" unavailable />
          <StepCard stepNumber={6} heading="Category weight × expansion" unavailable />
          <StepCard stepNumber={7} heading="Net contribution to composite" unavailable />
        </>
      ) : (
        <>
          <StepCard stepNumber={4} heading="RSI score">
            Score = {rsiScore.toFixed(1)} (range −100 to +100).
          </StepCard>

          {/* Steps 5–7 require contributions payload */}
          {!contributions ? (
            <div className="rounded border border-border bg-card p-3 text-xs text-muted-foreground italic">
              Contribution breakdown not available for this date (legacy data).
            </div>
          ) : !rsiItem ? (
            <>
              <StepCard stepNumber={5} heading="Magnitude share in momentum">
                RSI not found in contributions payload.
              </StepCard>
              <StepCard stepNumber={6} heading="Category weight × expansion" unavailable />
              <StepCard stepNumber={7} heading="Net contribution to composite" unavailable />
            </>
          ) : (
            <>
              {/* Step 5 — Magnitude share in momentum */}
              <StepCard stepNumber={5} heading="Magnitude share in momentum">
                {(() => {
                  const momentumItems = contributions.items.filter(
                    (item) => item.category === 'momentum',
                  );
                  const denom = momentumItems.reduce(
                    (acc, item) => acc + Math.abs(item.score),
                    0,
                  );
                  if (denom === 0) {
                    return 'Share undefined (all momentum components zero).';
                  }
                  const share = Math.abs(rsiItem.score) / denom;
                  const siblings = momentumItems.filter((item) => item.name !== 'rsi_14');
                  return (
                    <>
                      RSI accounts for{' '}
                      <span className="font-medium">{(share * 100).toFixed(1)}%</span> of the
                      absolute momentum signal.
                      {siblings.length > 0 && (
                        <span className="text-muted-foreground">
                          {' '}Momentum siblings:{' '}
                          {siblings.map((s) => `${s.name} (${s.score.toFixed(1)})`).join(', ')}.
                        </span>
                      )}
                    </>
                  );
                })()}
              </StepCard>

              {/* Step 6 — Category weight × expansion */}
              <StepCard stepNumber={6} heading="Category weight × expansion">
                Momentum weight in{' '}
                <span className="font-medium">{regime}</span> regime ={' '}
                {rsiItem.category_weight.toFixed(3)} × expansion{' '}
                {contributions.expansion_factor.toFixed(2)}.
              </StepCard>

              {/* Step 7 — Net contribution */}
              <StepCard stepNumber={7} heading="Net contribution to composite">
                Approximately{' '}
                <span className="font-medium">{rsiItem.contribution.toFixed(2)} points</span>.
                <p className="mt-1 text-muted-foreground italic text-[10px]">
                  {rules?.approximation_caveat ??
                    'Item-level contributions do not sum to the final composite score due to clamping, sector adjustment, and timeframe merging.'}
                </p>
              </StepCard>
            </>
          )}
        </>
      )}
    </div>
  );
}

/**
 * Click-to-expand explainer panel for a single indicator row.
 *
 * @param indicator - Indicator key (e.g. "rsi_14").
 * @param snapshot - Full snapshot object from the server.
 * @param rules - Scoring rules from /api/scoring-rules, or undefined if not yet loaded.
 */
export function IndicatorExplainerPanel({
  indicator,
  snapshot,
  rules,
}: IndicatorExplainerPanelProps) {
  if (indicator === 'rsi_14') {
    return <RsiPanel snapshot={snapshot} rules={rules} />;
  }
  return <PlaceholderPanel indicator={indicator} />;
}
