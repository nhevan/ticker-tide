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
import { PercentileStrip } from '@/components/PercentileStrip';
import { PercentileMappingChart } from '@/components/PercentileMappingChart';
import { CategoryShareBar } from '@/components/CategoryShareBar';
import { MacdTrendChart } from '@/components/MacdTrendChart';
import { MacdMappingChart } from '@/components/MacdMappingChart';
import { CategoryWeightBar } from '@/components/CategoryWeightBar';
import { ContributionMathChain } from '@/components/ContributionMathChain';
import { StochTrendChart } from '@/components/StochTrendChart';
import { AdxTrendChart } from '@/components/AdxTrendChart';
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

/** Human-friendly prose fragments for Stoch %K zone label strings (profile path). */
const STOCH_ZONE_LABEL_DESCRIPTIONS: Record<string, string> = {
  extreme_oversold: "Below p5 of this ticker's historical %K",
  oversold: "Between p5 and p20 of this ticker's historical %K",
  below_mid: "Between p20 and p50 of this ticker's historical %K",
  above_mid: "Between p50 and p80 of this ticker's historical %K",
  overbought: "Between p80 and p95 of this ticker's historical %K",
  extreme_overbought: "Above p95 of this ticker's historical %K",
};

/** Human-friendly prose fragments for Stoch %K zone label strings (fallback path). */
const STOCH_FALLBACK_ZONE_DESCRIPTIONS: Record<string, string> = {
  oversold: 'Below the fixed oversold threshold (20)',
  below_mid: 'Between oversold and the midpoint',
  above_mid: 'Between the midpoint and overbought',
  overbought: 'Above the fixed overbought threshold (80)',
};

/** Human-friendly prose fragments for ADX zone label strings.
 *
 * ADX has no profile/fallback path split — score_adx is always fixed-band,
 * so a single descriptions dict suffices. Boundary semantics match
 * zone_label_for_adx's `>=` checks: at exactly 20 the label is
 * weak_trend_developing, at exactly 25 it is developing_trend, at exactly
 * 40 it is strong_trend.
 */
const ADX_ZONE_LABEL_DESCRIPTIONS: Record<string, string> = {
  ranging: 'ADX below 20 — no clear trend; mean-reversion regime.',
  weak_trend_developing: 'ADX from 20 to below 25 — early trend signal.',
  developing_trend: 'ADX from 25 to below 40 — established trend.',
  strong_trend: 'ADX at or above 40 — strong trend in progress.',
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
    <div className="border-t border-border/40 bg-card px-4 py-3 space-y-2">
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
          PercentileStrip applies to percentile-profile indicators only (RSI,
          Stoch %K); MACD does not map onto a percentile metaphor — the
          scoring shape is fundamentally different. */}
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
              <p className="mt-2 text-[9px] text-muted-foreground italic">
                <span className="not-italic font-mono">mean</span> and{' '}
                <span className="not-italic font-mono">std</span> are this ticker's own MACD line
                distribution over a rolling 504 trading-day (~2-year) window, sourced from{' '}
                <span className="not-italic font-mono">indicator_profiles</span>. Z-scoring against
                the ticker's own history (rather than a fixed cross-stock threshold) accounts for
                MACD's price-dependent scale.
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
      {/* Step 4 — MACD line score (mapping chart).
          Renders the active scoring function (z-score curve with profile, or
          linear fallback) with today's value marked. Requires the persisted
          score; falls back to unavailable when absent. */}
      {(() => {
        const score = daily.indicator_scores?.['macd_line'];
        if (typeof score !== 'number' || !Number.isFinite(score)) {
          return <StepCard stepNumber={4} heading="MACD line score" unavailable />;
        }
        return (
          <StepCard stepNumber={4} heading="MACD line score">
            <MacdMappingChart
              profile={daily.macd_line_profile ?? null}
              today={macdValue}
              score={score}
            />
          </StepCard>
        );
      })()}
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

// =====================================================================
// StochKPanel — 7-step Stoch %K explainer.
// All 7 steps wired to real backend data.
// =====================================================================

/**
 * Stoch %K panel — 7-step trace mirroring RsiPanel.
 *
 * @param snapshot - Full snapshot object from the server.
 * @param rules - Scoring rules from /api/scoring-rules, or undefined if not yet loaded.
 *   Used by Step 6 to render CURRENT regime weights + expansion factor (vs. the persisted
 *   contributions data used by Step 7). If config has drifted since the last scoring run,
 *   Step 6 (current config) and Step 7 (persisted payload) may differ slightly.
 *
 * All 7 steps use real backend data.
 */
function StochKPanel({ snapshot, rules }: { snapshot: Snapshot; rules: ScoringRules | undefined }) {
  const daily = snapshot.daily;
  const liveK = daily.indicators?.stoch_k;
  const liveD = daily.indicators?.stoch_d;
  const kValue: number | null = Number.isFinite(liveK as number) ? (liveK as number) : null;
  const dValue: number | null = Number.isFinite(liveD as number) ? (liveD as number) : null;
  const spread = Number.isFinite(kValue) && Number.isFinite(dValue) ? (kValue as number) - (dValue as number) : null;

  // Real backend fields — now typed via DailySection.
  const sparkline = daily.stoch_sparkline ?? [];
  const stochProfile = daily.stoch_k_profile ?? null;
  const zoneLabel = daily.stoch_zone_label ?? null;

  // Step 4: real persisted indicator score for stoch_k.
  const stochScore: number | null = (() => {
    const raw = daily.indicator_scores?.stoch_k;
    return typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
  })();

  // Step 5: real contributions payload and stoch_k item.
  const contributions = daily.contributions_payload ?? null;
  const stochItem: ContributionItem | undefined = contributions?.items.find(
    (item) => item.name === 'stoch_k',
  );

  const regime = (daily.regime ?? 'ranging') as string;

  return (
    <div className="border-t border-border/40 bg-card px-4 py-3 text-xs text-foreground">
      {/* Step 1 — Raw value */}
      <StepCard stepNumber={1} heading="Stochastic %K reading">
        {kValue !== null && dValue !== null && spread !== null ? (
          <>
            <p>
              %K = <span className="font-mono font-semibold">{kValue.toFixed(2)}</span>, %D ={' '}
              <span className="font-mono font-semibold">{dValue.toFixed(2)}</span> (3-period SMA of %K). Spread %K − %D ={' '}
              <span className={`font-mono font-semibold ${spread >= 0 ? 'text-up' : 'text-down'}`}>{spread >= 0 ? '+' : ''}{spread.toFixed(2)}</span>.
            </p>
            <p className="mt-1 text-muted-foreground">
              %K measures where today's close sits within the highest-high / lowest-low range over the last 14 sessions. 0 = at the period low, 100 = at the period high.
            </p>
          </>
        ) : (
          <p className="text-muted-foreground italic">Stochastic reading unavailable for this date.</p>
        )}
      </StepCard>

      {/* Step 2 — Zone + trend chart (real backend data) */}
      <StepCard stepNumber={2} heading="Zone">
        {sparkline.length > 0 ? (
          <StochTrendChart
            data={sparkline.map((row) => ({ date: row.date, k: row.stoch_k, d: row.stoch_d }))}
          />
        ) : (
          <p className="text-muted-foreground italic">Sparkline unavailable for this date.</p>
        )}
        {zoneLabel ? (
          <div className="flex items-center gap-2 mt-2">
            <span className="font-mono text-[10px] uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
              {zoneLabel}
            </span>
            <span>
              {stochProfile
                ? (STOCH_ZONE_LABEL_DESCRIPTIONS[zoneLabel] ?? zoneLabel)
                : (STOCH_FALLBACK_ZONE_DESCRIPTIONS[zoneLabel] ?? zoneLabel)}
            </span>
            {!stochProfile && (
              <span className="text-muted-foreground italic">
                (fallback — no per-ticker profile yet)
              </span>
            )}
          </div>
        ) : (
          <p className="text-muted-foreground italic mt-2">Zone unavailable for this date.</p>
        )}
      </StepCard>

      {/* Step 3 — Scoring path */}
      <StepCard stepNumber={3} heading="Scoring path">
        {stochProfile &&
        Number.isFinite(stochProfile.p5) &&
        Number.isFinite(stochProfile.p20) &&
        Number.isFinite(stochProfile.p50) &&
        Number.isFinite(stochProfile.p80) &&
        Number.isFinite(stochProfile.p95) &&
        kValue !== null ? (
          <PercentileStrip
            profile={stochProfile}
            today={kValue}
            zoneLabel={zoneLabel}
            zoneDescription={STOCH_ZONE_LABEL_DESCRIPTIONS[zoneLabel ?? ''] ?? ''}
            label="Stoch %K"
          />
        ) : null}
        <p className={stochProfile && kValue !== null ? 'mt-2' : ''}>
          {stochProfile
            ? 'This ticker has a persisted percentile profile for %K, so scoring uses the '
            : 'This ticker has no persisted %K profile, so scoring falls back to the '}
          <span className="font-semibold">{stochProfile ? 'profile path' : 'fallback path'}</span>
          {stochProfile
            ? ' (six zones around p5/p20/p50/p80/p95).'
            : ' (fixed 80/20 thresholds).'}{' '}
          {stochProfile
            ? 'Without a profile, scoring would fall back to fixed 80/20 thresholds.'
            : 'With a profile, scoring would use six zones around p5/p20/p50/p80/p95.'}
        </p>
        <p className="mt-1 text-muted-foreground">
          In a <span className="font-mono">{regime}</span> regime, the oscillator's sign{' '}
          {regime === 'trending'
            ? 'flips: in trending regimes, overbought is bullish (momentum continuation).'
            : 'is treated as mean-reverting (overbought → bearish, oversold → bullish).'}
        </p>
      </StepCard>

      {/* Step 4 — Stoch %K score (real data) */}
      {stochScore === null ? (
        <StepCard stepNumber={4} heading="Stochastic %K score" unavailable />
      ) : stochProfile &&
        kValue !== null &&
        (regime === 'trending' || regime === 'ranging' || regime === 'volatile') &&
        Number.isFinite(stochProfile.p5) &&
        Number.isFinite(stochProfile.p20) &&
        Number.isFinite(stochProfile.p50) &&
        Number.isFinite(stochProfile.p80) &&
        Number.isFinite(stochProfile.p95) ? (
        <StepCard stepNumber={4} heading="Stochastic %K score">
          <PercentileMappingChart
            profile={stochProfile}
            today={kValue}
            score={stochScore}
            regime={regime as 'trending' | 'ranging' | 'volatile'}
            label="Stoch %K"
          />
        </StepCard>
      ) : (
        <StepCard stepNumber={4} heading="Stochastic %K score">
          <p>
            No per-ticker profile, so scoring uses a piecewise fallback: %K ≤ 20 caps at +60
            (oversold→bullish), %K ≥ 80 caps at −60 (overbought→bearish), and the middle
            (20 &lt; %K &lt; 80) is a linear gradient from +18 down to −18 around the midpoint
            (score = (50 − %K) ÷ 50 × 30). In a trending regime the sign flips. Today's score
            = {stochScore.toFixed(1)} (range −100 to +100).
          </p>
        </StepCard>
      )}

      {!contributions || !stochItem ? (
        <>
          <StepCard stepNumber={5} heading="Magnitude share in momentum">
            {!contributions
              ? 'Contribution breakdown not available for this date (legacy data).'
              : 'Stoch %K not found in contributions payload.'}
          </StepCard>
          <StepCard stepNumber={6} heading="Category weight × expansion" unavailable />
          <StepCard stepNumber={7} heading="Net contribution to composite" unavailable />
        </>
      ) : (
        <>
          {/* Step 5 — Magnitude share in momentum (REAL) */}
          <StepCard stepNumber={5} heading="Magnitude share in momentum">
            <CategoryShareBar
              items={contributions.items.filter((item) => item.category === 'momentum')}
              activeName="stoch_k"
              category="momentum"
            />
          </StepCard>

          {/* Step 6 — Category weight × expansion (REAL) */}
          <StepCard stepNumber={6} heading="Category weight × expansion">
            {/* activeName hardcoded because this panel is stoch_k whose home category is
                momentum; the component supports any active category. expansion comes from
                CURRENT config (rules) which may differ slightly from the persisted
                contributions.expansion_factor used in Step 7 if config has drifted since
                the last scoring run. */}
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
                {stochItem.category_weight.toFixed(3)} × expansion{' '}
                {contributions.expansion_factor.toFixed(2)}.
              </>
            )}
          </StepCard>

          {/* Step 7 — Net contribution (REAL) */}
          <StepCard stepNumber={7} heading="Net contribution to composite">
            {(() => {
              const momentumItems = contributions.items.filter((item) => item.category === 'momentum');
              const denom = momentumItems.reduce((acc, item) => acc + Math.abs(item.score), 0);
              return (
                <ContributionMathChain
                  score={stochItem.score}
                  denom={denom}
                  regimeWeight={stochItem.category_weight}
                  expansion={contributions.expansion_factor}
                  finalContribution={stochItem.contribution}
                  activeName="stoch_k"
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
    </div>
  );
}

// =====================================================================
// End StochKPanel block.
// =====================================================================

// =====================================================================
// [PROTOTYPE] AdxPanel — mockup scaffolding for the visual pick.
// Dummy data, inline SVG. Removed/promoted per task plan.
// =====================================================================

/** [PROTOTYPE] Generate dummy 100-day ADX series for mockups. */
function buildDummyAdxSeries(): { date: string; adx: number }[] {
  const out: { date: string; adx: number }[] = [];
  let v = 22;
  for (let i = 0; i < 100; i++) {
    v += (Math.sin(i / 7) + Math.sin(i / 17)) * 3 + (Math.random() - 0.5) * 3;
    v = Math.max(8, Math.min(55, v));
    const date = `2026-${String(Math.floor(i / 30) + 1).padStart(2, '0')}-${String((i % 30) + 1).padStart(2, '0')}`;
    out.push({ date, adx: v });
  }
  return out;
}


/** Compute ADX → score per src/scorer/indicator_scorer.py score_adx (4 piecewise segments). */
function scoreAdxLocal(v: number): number {
  if (v >= 40) return 80.0;
  if (v >= 25) return 40.0 + ((v - 25) / 15) * 40.0;
  if (v >= 20) return ((v - 20) / 5) * 20.0;
  return -20.0 + (v / 20) * 20.0;
}

/** [PROTOTYPE A] Mapping chart: piecewise-linear curve mirroring score_adx, asymmetric y-range. */
function AdxMappingChartProtoA({ value }: { value: number }) {
  const w = 560;
  const h = 180;
  const pad = { l: 32, r: 16, t: 14, b: 22 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const xFor = (v: number) => pad.l + (v / 100) * innerW;
  // Y range: -20 (bottom) to +80 (top) — asymmetric.
  const yFor = (s: number) => pad.t + ((80 - s) / 100) * innerH;
  // Path segments — note discontinuity at x=25 (jumps 20 → 40).
  const segLow = `M${xFor(0).toFixed(1)},${yFor(-20).toFixed(1)} L${xFor(20).toFixed(1)},${yFor(0).toFixed(1)}`;
  const segWeak = `M${xFor(20).toFixed(1)},${yFor(0).toFixed(1)} L${xFor(25).toFixed(1)},${yFor(20).toFixed(1)}`;
  const segDev = `M${xFor(25).toFixed(1)},${yFor(40).toFixed(1)} L${xFor(40).toFixed(1)},${yFor(80).toFixed(1)}`;
  const segStrong = `M${xFor(40).toFixed(1)},${yFor(80).toFixed(1)} L${xFor(100).toFixed(1)},${yFor(80).toFixed(1)}`;
  const score = scoreAdxLocal(value);
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" style={{ height: h }} preserveAspectRatio="none">
      <line x1={pad.l} x2={pad.l + innerW} y1={yFor(0)} y2={yFor(0)} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.3} />
      <path d={segLow} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
      <path d={segWeak} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
      <path d={segDev} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
      <path d={segStrong} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
      {/* Discontinuity markers at x=25 */}
      <circle cx={xFor(25)} cy={yFor(20)} r={2.5} fill="hsl(var(--background))" stroke="hsl(var(--foreground))" strokeWidth={1.2} />
      <circle cx={xFor(25)} cy={yFor(40)} r={2.5} fill="hsl(var(--foreground))" />
      {[20, 25, 40].map((p) => (
        <line key={p} x1={xFor(p)} x2={xFor(p)} y1={pad.t} y2={pad.t + innerH} stroke="hsl(var(--muted-foreground))" strokeDasharray="2 3" strokeOpacity={0.25} />
      ))}
      <circle cx={xFor(value)} cy={yFor(score)} r={4.5} fill="hsl(var(--primary))" stroke="hsl(var(--card))" strokeWidth={1.5} />
      <text x={xFor(value) + 6} y={yFor(score) - 6} fontSize={10} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--primary))">
        ADX {value.toFixed(1)} → {score.toFixed(1)}
      </text>
      <text x={pad.l - 4} y={yFor(80) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--up))" textAnchor="end">+80</text>
      <text x={pad.l - 4} y={yFor(40) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--up))" textAnchor="end">+40</text>
      <text x={pad.l - 4} y={yFor(0) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="end">0</text>
      <text x={pad.l - 4} y={yFor(-20) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--down))" textAnchor="end">−20</text>
      {[0, 20, 25, 40, 60, 80, 100].map((v) => (
        <text key={v} x={xFor(v)} y={pad.t + innerH + 12} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="middle">{v}</text>
      ))}
    </svg>
  );
}

/** [PROTOTYPE B] Mapping chart: zone-tinted bands with the curve overlaid. */
function AdxMappingChartProtoB({ value }: { value: number }) {
  const w = 560;
  const h = 180;
  const pad = { l: 32, r: 16, t: 14, b: 22 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const xFor = (v: number) => pad.l + (v / 100) * innerW;
  const yFor = (s: number) => pad.t + ((80 - s) / 100) * innerH;
  const score = scoreAdxLocal(value);
  const zones: { x0: number; x1: number; fill: string; opacity: number; label: string }[] = [
    { x0: 0, x1: 20, fill: 'hsl(var(--muted-foreground))', opacity: 0.10, label: 'ranging' },
    { x0: 20, x1: 25, fill: 'hsl(var(--up))', opacity: 0.06, label: 'weak' },
    { x0: 25, x1: 40, fill: 'hsl(var(--up))', opacity: 0.12, label: 'developing' },
    { x0: 40, x1: 100, fill: 'hsl(var(--up))', opacity: 0.20, label: 'strong' },
  ];
  const segLow = `M${xFor(0).toFixed(1)},${yFor(-20).toFixed(1)} L${xFor(20).toFixed(1)},${yFor(0).toFixed(1)}`;
  const segWeak = `M${xFor(20).toFixed(1)},${yFor(0).toFixed(1)} L${xFor(25).toFixed(1)},${yFor(20).toFixed(1)}`;
  const segDev = `M${xFor(25).toFixed(1)},${yFor(40).toFixed(1)} L${xFor(40).toFixed(1)},${yFor(80).toFixed(1)}`;
  const segStrong = `M${xFor(40).toFixed(1)},${yFor(80).toFixed(1)} L${xFor(100).toFixed(1)},${yFor(80).toFixed(1)}`;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" style={{ height: h }} preserveAspectRatio="none">
      {zones.map((z) => (
        <g key={z.x0}>
          <rect x={xFor(z.x0)} y={pad.t} width={xFor(z.x1) - xFor(z.x0)} height={innerH} fill={z.fill} fillOpacity={z.opacity} />
          <text x={(xFor(z.x0) + xFor(z.x1)) / 2} y={pad.t + 10} fontSize={8} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="middle">{z.label}</text>
        </g>
      ))}
      <line x1={pad.l} x2={pad.l + innerW} y1={yFor(0)} y2={yFor(0)} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.4} />
      <path d={segLow} fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segWeak} fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segDev} fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segStrong} fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <circle cx={xFor(25)} cy={yFor(20)} r={2.5} fill="hsl(var(--background))" stroke="hsl(var(--foreground))" strokeWidth={1.2} />
      <circle cx={xFor(25)} cy={yFor(40)} r={2.5} fill="hsl(var(--foreground))" />
      <circle cx={xFor(value)} cy={yFor(score)} r={4.5} fill="hsl(var(--primary))" stroke="hsl(var(--card))" strokeWidth={1.5} />
      <text x={xFor(value) + 6} y={yFor(score) - 6} fontSize={10} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--primary))">
        ADX {value.toFixed(1)} → {score.toFixed(1)}
      </text>
      <text x={pad.l - 4} y={yFor(80) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--up))" textAnchor="end">+80</text>
      <text x={pad.l - 4} y={yFor(0) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="end">0</text>
      <text x={pad.l - 4} y={yFor(-20) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--down))" textAnchor="end">−20</text>
      {[20, 25, 40].map((v) => (
        <text key={v} x={xFor(v)} y={pad.t + innerH + 12} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="middle">{v}</text>
      ))}
    </svg>
  );
}

/** [PROTOTYPE] ADX panel — full 7-step skeleton mirroring StochKPanel.
 * Steps 1 & 2 use real backend data. Steps 3–7 use dummy values —
 * wired per task plan. */
function AdxPanelPrototype({ snapshot }: { snapshot: Snapshot }) {
  const daily = snapshot.daily;
  const series = buildDummyAdxSeries();
  const liveAdx = daily.indicators?.adx;
  const adxValue: number | null = Number.isFinite(liveAdx as number) ? (liveAdx as number) : null;
  const valueForMapping = adxValue ?? series[series.length - 1].adx;

  // Real backend fields — wired for Steps 1 & 2.
  // Filter non-finite adx values defensively: DB columns are nullable even
  // when TS types say `number`, and the chart's path math would NaN-out on
  // a single bad row (recipe gotcha #4).
  const sparkline = (daily.adx_sparkline ?? []).filter((p) => Number.isFinite(p.adx));
  const zoneLabel = daily.adx_zone_label ?? null;

  const dummyScore = adxValue !== null ? scoreAdxLocal(adxValue) : 32.0;
  const regime = (daily.regime ?? 'ranging') as string;
  const dummyDenom = 142.0;
  const dummyExpansion = 1.08;
  const dummyTrendWeight = 0.34;
  const dummyFinalContribution = (dummyScore * Math.abs(dummyScore) / dummyDenom) * dummyTrendWeight * dummyExpansion;
  const dummyTrendItems: ContributionItem[] = [
    { name: 'adx', category: 'trend', kind: 'indicator', score: dummyScore, raw_value: dummyScore, category_weight: dummyTrendWeight, contribution: 0 },
    { name: 'macd_line', category: 'trend', kind: 'indicator', score: 28, raw_value: 28, category_weight: dummyTrendWeight, contribution: 0 },
    { name: 'sma_50_200', category: 'trend', kind: 'indicator', score: -15, raw_value: -15, category_weight: dummyTrendWeight, contribution: 0 },
    { name: 'ema_spread', category: 'trend', kind: 'indicator', score: 12, raw_value: 12, category_weight: dummyTrendWeight, contribution: 0 },
  ];
  const dummyCategoryWeights = { momentum: 0.28, trend: dummyTrendWeight, volatility: 0.14, volume: 0.10, breadth: 0.08, valuation: 0.06 };

  return (
    <div className="border-t border-border/40 bg-card px-4 py-3 text-xs text-foreground">
      <div className="mb-3 rounded border border-amber-500/60 bg-amber-500/10 px-3 py-2 text-amber-700 dark:text-amber-300">
        [PROTOTYPE] Steps 3-7 use dummy values — wired in order per task plan. Steps 1 &amp; 2 now use real backend data.
      </div>

      {/* Step 1 — Raw value */}
      <StepCard stepNumber={1} heading="ADX reading">
        {adxValue !== null && zoneLabel !== null ? (
          <>
            <p>
              ADX = <span className="font-mono font-semibold">{adxValue.toFixed(2)}</span>.
            </p>
            <div className="flex items-center gap-2 mt-2">
              <span className="font-mono text-[10px] uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
                {zoneLabel}
              </span>
              <span>{ADX_ZONE_LABEL_DESCRIPTIONS[zoneLabel] ?? zoneLabel}</span>
            </div>
            <p className="mt-2 text-muted-foreground">
              ADX measures trend strength over the last 14 sessions on a 0–100 scale (direction-agnostic — it does not say whether the trend is up or down).
            </p>
          </>
        ) : adxValue !== null ? (
          <p>
            ADX = <span className="font-mono font-semibold">{adxValue.toFixed(2)}</span>.{' '}
            <span className="text-muted-foreground italic">Zone label unavailable.</span>
          </p>
        ) : (
          <p className="text-muted-foreground italic">ADX reading unavailable for this date.</p>
        )}
      </StepCard>

      {/* Step 2 — Trend chart (REAL) */}
      <StepCard stepNumber={2} heading="Trend strength over time">
        {sparkline.length > 1 ? (
          <AdxTrendChart data={sparkline} />
        ) : (
          <p className="text-muted-foreground italic">Sparkline unavailable for this date.</p>
        )}
        {zoneLabel && adxValue !== null ? (
          <div className="flex items-center gap-2 mt-2">
            <span className="font-mono text-[10px] uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
              {zoneLabel}
            </span>
            <span>{ADX_ZONE_LABEL_DESCRIPTIONS[zoneLabel] ?? zoneLabel}</span>
          </div>
        ) : (
          <p className="text-muted-foreground italic mt-2">Zone unavailable for this date.</p>
        )}
      </StepCard>

      {/* Step 3 — Scoring path (prose + reference table, no percentile strip) */}
      <StepCard stepNumber={3} heading="Scoring path">
        <p>
          ADX is scored by a <span className="font-semibold">fixed-band piecewise function</span>, not by a per-ticker percentile profile. The same four bands apply to every ticker and the score is always direction-agnostic (no regime sign-flip).
        </p>
        <div className="mt-3 overflow-hidden rounded border border-border">
          <table className="w-full text-[11px]">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="px-2 py-1 text-left font-semibold">Band</th>
                <th className="px-2 py-1 text-left font-semibold">ADX range</th>
                <th className="px-2 py-1 text-left font-semibold">Score range</th>
              </tr>
            </thead>
            <tbody className="font-mono">
              <tr className="border-t border-border"><td className="px-2 py-1">ranging / no trend</td><td className="px-2 py-1">0 – 20</td><td className="px-2 py-1">−20 → 0</td></tr>
              <tr className="border-t border-border"><td className="px-2 py-1">weak trend developing</td><td className="px-2 py-1">20 – 25</td><td className="px-2 py-1">0 → +20</td></tr>
              <tr className="border-t border-border"><td className="px-2 py-1">developing trend</td><td className="px-2 py-1">25 – 40</td><td className="px-2 py-1">+40 → +80</td></tr>
              <tr className="border-t border-border"><td className="px-2 py-1">strong trend</td><td className="px-2 py-1">≥ 40</td><td className="px-2 py-1">+80 (cap)</td></tr>
            </tbody>
          </table>
        </div>
        <p className="mt-2 text-muted-foreground text-[10px]">
          Note the score jump at ADX = 25 (from +20 to +40). The directional pair (DI+/DI−) is not stored in this codebase, so ADX scores trend strength only.
        </p>
      </StepCard>

      {/* Step 4 — Mapping chart (picked: Variant B — zone-tinted bands with curve overlay) */}
      <StepCard stepNumber={4} heading="ADX score">
        <p>
          ADX {valueForMapping.toFixed(1)} maps to a score of{' '}
          <span className={`font-mono font-semibold ${dummyScore >= 0 ? 'text-up' : 'text-down'}`}>{dummyScore >= 0 ? '+' : ''}{dummyScore.toFixed(1)}</span>{' '}
          (range −20 to +80; ADX never strongly bearish):
        </p>
        <div className="mt-3">
          <AdxMappingChartProtoB value={valueForMapping} />
        </div>
      </StepCard>

      {/* Step 5 — Magnitude share in trend */}
      <StepCard stepNumber={5} heading="Magnitude share in trend">
        <p className="text-muted-foreground">
          ADX's |score| within the trend category{' '}
          <span>[PROTOTYPE — dummy items]</span>:
        </p>
        <div className="mt-3">
          <CategoryShareBar items={dummyTrendItems} activeName="adx" category="trend" />
        </div>
      </StepCard>

      {/* Step 6 — Category weight × expansion */}
      <StepCard stepNumber={6} heading="Category weight × expansion">
        <p>
          Trend's base weight in the <span className="font-mono">{regime}</span> regime is{' '}
          <span className="font-mono font-semibold">{(dummyTrendWeight * 100).toFixed(0)}%</span>, scaled by the cross-section expansion factor{' '}
          <span className="font-mono font-semibold">×{dummyExpansion.toFixed(2)}</span>{' '}
          <span className="text-muted-foreground">[PROTOTYPE — dummy]</span>.
        </p>
        <div className="mt-3">
          <CategoryWeightBar weights={dummyCategoryWeights} regime={regime} expansion={dummyExpansion} activeName="trend" />
        </div>
      </StepCard>

      {/* Step 7 — Net contribution */}
      <StepCard stepNumber={7} heading="Net contribution to composite">
        <p>
          Final contribution of ADX to today's composite score{' '}
          <span className="text-muted-foreground">[PROTOTYPE — dummy]</span>:
        </p>
        <div className="mt-3">
          <ContributionMathChain
            score={dummyScore}
            denom={dummyDenom}
            regimeWeight={dummyTrendWeight}
            expansion={dummyExpansion}
            finalContribution={dummyFinalContribution}
            activeName="adx"
          />
        </div>
        <p className="mt-2 text-muted-foreground italic text-[10px]">
          Item-level contributions do not sum to the final composite score due to clamping, sector adjustment, and timeframe merging.
        </p>
      </StepCard>
    </div>
  );
}

// =====================================================================
// End [PROTOTYPE] AdxPanel block.
// =====================================================================

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
    <div className="border-t border-border/40 bg-card px-4 py-3 space-y-2">
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
            <PercentileStrip
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
              <PercentileMappingChart
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
  // NOTE: keep the dispatch branches below in sync with INDICATORS_WITH_EXPLAINER.
  // The Set gates whether the matrix row is clickable; the dispatch decides which
  // panel actually renders. Add a new indicator to BOTH or the UI silently breaks
  // in one direction (clickable row that shows the placeholder, or invisible panel).
  if (indicator === 'rsi_14') {
    return <RsiPanel snapshot={snapshot} rules={rules} />;
  }
  if (indicator === 'macd_line') {
    return <MacdLinePanel snapshot={snapshot} rules={rules} />;
  }
  if (indicator === 'stoch_k') {
    return <StochKPanel snapshot={snapshot} rules={rules} />;
  }
  if (indicator === 'adx') {
    // [PROTOTYPE] — temporary dispatch for visual variant pick; promoted per task plan.
    return <AdxPanelPrototype snapshot={snapshot} />;
  }
  return <PlaceholderPanel indicator={indicator} />;
}

/**
 * Set of indicator keys that have a real explainer panel (i.e. not the
 * "coming soon" PlaceholderPanel). Consumers use this to gate visual
 * affordances and click handlers — only rows for these indicators should
 * appear clickable in the matrix.
 *
 * Keep in sync with the dispatch in IndicatorExplainerPanel above.
 */
export const INDICATORS_WITH_EXPLAINER: ReadonlySet<string> = new Set([
  'rsi_14',
  'macd_line',
  'stoch_k',
  'adx', // Steps 1-2 real; Steps 3-7 still prototype-wired (in progress per task plan).
]);
