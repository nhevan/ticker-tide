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
import { RsiMappingChart } from '@/components/RsiMappingChart';
import { CategoryShareBar } from '@/components/CategoryShareBar';
import { MacdTrendChart } from '@/components/MacdTrendChart';
import { CategoryWeightBar } from '@/components/CategoryWeightBar';
import { ContributionMathChain } from '@/components/ContributionMathChain';
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

/**
 * Render the 7-step MACD line trace.
 *
 * Scaffold only — Step 1 renders the real raw reading + interpretation;
 * steps 2–7 are placeholder cards to be filled in per-step follow-up branches.
 *
 * Step 1 reads three raw values from `snapshot.daily.indicators`:
 *   - macd_line, macd_signal, macd_histogram.
 * Each is independently guarded with `Number.isFinite` because the DB columns
 * are nullable (NaN → NULL is applied during indicator persistence).
 */
function MacdLinePanel({ snapshot, rules }: { snapshot: Snapshot; rules: ScoringRules | undefined }) {
  const daily = snapshot.daily;
  const regime = daily.regime ?? 'ranging';
  const macdRaw = daily.indicators?.macd_line;
  const signalRaw = daily.indicators?.macd_signal;
  const histRaw = daily.indicators?.macd_histogram;

  const macdValue = typeof macdRaw === 'number' && Number.isFinite(macdRaw) ? macdRaw : null;
  const signalValue = typeof signalRaw === 'number' && Number.isFinite(signalRaw) ? signalRaw : null;
  const histValue = typeof histRaw === 'number' && Number.isFinite(histRaw) ? histRaw : null;

  if (macdValue === null || signalValue === null || histValue === null) {
    return (
      <div className="border-t border-border/40 bg-card px-4 py-3">
        <StepCard stepNumber={1} heading="MACD line reading">
          MACD not fully available for this date.
        </StepCard>
      </div>
    );
  }

  const lineAboveSignal = macdValue > signalValue;
  const histPositive = histValue > 0;
  const bullish = lineAboveSignal && histPositive;
  const bearish = !lineAboveSignal && !histPositive;
  const verdict = bullish
    ? 'a bullish MACD configuration'
    : bearish
      ? 'a bearish MACD configuration'
      : 'a mixed MACD configuration';
  const positionPhrase = lineAboveSignal ? 'above signal' : 'below signal';
  const histPhrase = histPositive ? 'histogram is positive' : 'histogram is negative';

  return (
    <div className="border-t border-border/40 bg-card px-4 py-3 space-y-0">
      <StepCard stepNumber={1} heading="MACD line reading">
        <div>
          <span className="font-medium">Reading.</span> MACD line{' '}
          <span className="font-mono">{macdValue.toFixed(2)}</span>, signal{' '}
          <span className="font-mono">{signalValue.toFixed(2)}</span>, histogram{' '}
          <span className="font-mono">{histValue.toFixed(2)}</span>.
        </div>
        <div className="mt-1">
          <span className="font-medium">Interpretation.</span> Line sits {positionPhrase} and{' '}
          {histPhrase} — {verdict}.
        </div>
      </StepCard>
      {/* Step 2 — Trend chart.
          Classic MACD: histogram bars (green ≥0, red <0) overlaid with
          MACD line (solid) + signal line (dashed) on a zero baseline. */}
      <StepCard stepNumber={2} heading="MACD trend">
        <MacdTrendChart data={daily.macd_sparkline ?? []} />
      </StepCard>
      {/* Step 3 — Scoring path.
          MACD uses z-score normalisation (when a per-ticker profile exists)
          or a linear fallback (score = clamp(value × 20, ±100)) otherwise.
          We do not generalise RsiPercentileStrip here because MACD doesn't
          map onto a percentile metaphor — the scoring shape is fundamentally
          different from RSI's. */}
      {(() => {
        const profile = daily.macd_line_profile ?? null;
        if (profile && profile.std > 0) {
          const z = (macdValue - profile.mean) / profile.std;
          const zoneLabel =
            z > 2
              ? 'strongly bullish (z > 2)'
              : z > 1
                ? 'moderately bullish (1 < z ≤ 2)'
                : z >= -1
                  ? 'neutral band (−1 ≤ z ≤ 1)'
                  : z >= -2
                    ? 'moderately bearish (−2 ≤ z < −1)'
                    : 'strongly bearish (z < −2)';
          return (
            <StepCard stepNumber={3} heading="Scoring path">
              <p className="mb-2">
                <span className="font-medium">Profile path (z-score).</span> This ticker has enough
                MACD history for the engine to use its own distribution rather than the linear
                fallback.
              </p>
              <div className="font-mono text-[10px] space-y-0.5">
                <div className="text-muted-foreground">z = (MACD − mean) ÷ std</div>
                <div className="text-foreground">
                  = ({macdValue.toFixed(2)} − {profile.mean.toFixed(2)}) ÷ {profile.std.toFixed(2)}
                </div>
                <div className="text-foreground">
                  = {(macdValue - profile.mean).toFixed(2)} ÷ {profile.std.toFixed(2)}
                </div>
                <div>
                  = <span className="text-foreground font-semibold">{z.toFixed(2)}</span>
                </div>
              </div>
              <p className="mt-2">
                z falls in the <span className="text-primary font-medium">{zoneLabel}</span> band.
              </p>
            </StepCard>
          );
        }
        return (
          <StepCard stepNumber={3} heading="Scoring path">
            <p className="mb-2">
              <span className="font-medium">Fallback path (linear).</span> No per-ticker MACD
              profile is available, so the engine falls back to a magnitude-scaled mapping.
            </p>
            <div className="font-mono text-[10px] space-y-0.5">
              <div className="text-muted-foreground">score = clamp(MACD × 20, −100, +100)</div>
              <div className="text-foreground">
                = clamp({macdValue.toFixed(2)} × 20, −100, +100)
              </div>
              <div>
                ={' '}
                <span className="text-foreground font-semibold">
                  {Math.max(-100, Math.min(100, macdValue * 20)).toFixed(1)}
                </span>
              </div>
            </div>
          </StepCard>
        );
      })()}
      <StepCard stepNumber={4} heading="MACD line score" unavailable />
      {/* Step 5 — Magnitude share in trend.
          Uses the generalised CategoryShareBar (renamed from MomentumShareBar);
          caller filters items to category === 'trend'. Falls back to unavailable
          when contributions_payload is missing. */}
      {(() => {
        const contributions = daily.contributions_payload ?? null;
        if (!contributions) {
          return <StepCard stepNumber={5} heading="Magnitude share in trend" unavailable />;
        }
        return (
          <StepCard stepNumber={5} heading="Magnitude share in trend">
            <CategoryShareBar
              items={contributions.items.filter((item) => item.category === 'trend')}
              activeName="macd_line"
              category="trend"
            />
          </StepCard>
        );
      })()}

      {/* Step 6 — Category weight × expansion.
          Reuses CategoryWeightBar verbatim (already generic over any category).
          activeName="trend" because macd_line's home category is trend. */}
      {rules?.regime_weights && rules.regime_weights[regime] ? (
        <StepCard stepNumber={6} heading="Category weight × expansion">
          <CategoryWeightBar
            weights={rules.regime_weights[regime]}
            regime={regime}
            expansion={rules.score_expansion_factor}
            activeName="trend"
          />
        </StepCard>
      ) : (
        <StepCard stepNumber={6} heading="Category weight × expansion" unavailable />
      )}

      {/* Step 7 — Net contribution.
          Reuses ContributionMathChain verbatim (already generic over any indicator).
          Requires the macd_line item from the persisted contributions payload;
          falls back to unavailable when contributions or the item is missing. */}
      {(() => {
        const contributions = daily.contributions_payload ?? null;
        const macdItem: ContributionItem | undefined = contributions?.items.find(
          (item) => item.name === 'macd_line',
        );
        if (!contributions || !macdItem) {
          return <StepCard stepNumber={7} heading="Net contribution to composite" unavailable />;
        }
        const trendItems = contributions.items.filter((item) => item.category === 'trend');
        const denom = trendItems.reduce((acc, item) => acc + Math.abs(item.score), 0);
        return (
          <StepCard stepNumber={7} heading="Net contribution to composite">
            <ContributionMathChain
              score={macdItem.score}
              denom={denom}
              regimeWeight={macdItem.category_weight}
              expansion={contributions.expansion_factor}
              finalContribution={macdItem.contribution}
              activeName="macd_line"
            />
            <p className="mt-2 text-muted-foreground italic text-[10px]">
              {rules?.approximation_caveat ??
                'Item-level contributions do not sum to the final composite score due to clamping, sector adjustment, and timeframe merging.'}
            </p>
          </StepCard>
        );
      })()}
    </div>
  );
}

/** Placeholder shown for indicators other than rsi_14 and macd_line. */
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
            {rsiProfile && (regime === 'trending' || regime === 'ranging' || regime === 'volatile') ? (
              <RsiMappingChart
                profile={rsiProfile}
                today={rsiValue}
                score={rsiScore}
                regime={regime}
              />
            ) : (
              <>
                Score = {rsiScore.toFixed(1)} (range −100 to +100).{' '}
                <span className="text-muted-foreground italic">
                  (Mapping chart unavailable without per-ticker profile or recognised regime.)
                </span>
              </>
            )}
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
                {/* activeName hardcoded here because this panel only renders for rsi_14;
                    the component itself supports any active indicator. */}
                <CategoryShareBar
                  items={contributions.items.filter((item) => item.category === 'momentum')}
                  activeName="rsi_14"
                  category="momentum"
                />
              </StepCard>

              {/* Step 6 — Category weight × expansion */}
              <StepCard stepNumber={6} heading="Category weight × expansion">
                {/* activeName hardcoded here because this panel only renders for rsi_14,
                    whose home category is momentum; the component itself supports any
                    active category. expansion comes from current config (rules) which
                    may differ slightly from the persisted contributions.expansion_factor
                    used in step 7 if config has drifted since the last scoring run. */}
                {rules?.regime_weights && rules.regime_weights[regime] ? (
                  <CategoryWeightBar
                    weights={rules.regime_weights[regime]}
                    regime={regime}
                    expansion={rules.score_expansion_factor}
                    activeName="momentum"
                  />
                ) : (
                  <>
                    Momentum weight in <span className="font-medium">{regime}</span> regime ={' '}
                    {rsiItem.category_weight.toFixed(3)} × expansion{' '}
                    {contributions.expansion_factor.toFixed(2)}.
                  </>
                )}
              </StepCard>

              {/* Step 7 — Net contribution */}
              <StepCard stepNumber={7} heading="Net contribution to composite">
                {(() => {
                  const momentumItems = contributions.items.filter((item) => item.category === 'momentum');
                  const denom = momentumItems.reduce((acc, item) => acc + Math.abs(item.score), 0);
                  return (
                    <ContributionMathChain
                      score={rsiItem.score}
                      denom={denom}
                      regimeWeight={rsiItem.category_weight}
                      expansion={contributions.expansion_factor}
                      finalContribution={rsiItem.contribution}
                      activeName="rsi_14"
                    />
                  );
                })()}
                <p className="mt-2 text-muted-foreground italic text-[10px]">
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
  if (indicator === 'macd_line') {
    return <MacdLinePanel snapshot={snapshot} rules={rules} />;
  }
  return <PlaceholderPanel indicator={indicator} />;
}
