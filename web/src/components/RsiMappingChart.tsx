/**
 * RsiMappingChart — visualises how an indicator value (default: RSI(14)) becomes its scored output.
 *
 * Renders the piecewise mapping function defined by `score_with_percentile`
 * in src/scorer/indicator_scorer.py:158-184, using the loaded ticker's
 * percentile profile (p5/p20/p50/p80/p95) as zone anchors. The active curve
 * matches the current regime; the inactive curve is shown dashed so users
 * can see the regime flip would invert the score for high/low indicator values.
 *
 * Beneath the chart, a math breakdown table walks through the calculation
 * step-by-step: zone → position in zone (with the arithmetic shown in
 * brackets) → zone score range → base score → regime → final score.
 *
 * Theme-aware via CSS custom properties. SVG-only, no chart library.
 *
 * @param profile - The ticker's percentile bands from the indicator profile (p5/p20/p50/p80/p95).
 * @param today - The raw indicator reading (e.g. snapshot.daily.indicators.rsi_14 or stoch_k).
 * @param score - The persisted indicator score from snapshot.daily.indicator_scores.
 * @param regime - Market regime from snapshot.daily.regime. Sign flips only in 'trending'.
 * @param label - Human-readable indicator name shown in chart prose (default: 'RSI').
 * @returns The chart + math table, or null if any required numeric input is non-finite.
 */
interface RsiMappingChartProps {
  profile: { p5: number; p20: number; p50: number; p80: number; p95: number };
  today: number;
  score: number;
  regime: 'trending' | 'ranging' | 'volatile';
  label?: string;
}

/** Zone names ordered to match the 6 segments of the corner-point curve (applies to any percentile-profile indicator). */
const ZONE_NAMES = [
  'extreme_oversold',  // 0 -> p5
  'oversold',          // p5 -> p20
  'below_mid',         // p20 -> p50
  'above_mid',         // p50 -> p80
  'overbought',        // p80 -> p95
  'extreme_overbought',// p95 -> 100
] as const;

/** Build the 7 corner points of the piecewise indicator→score curve (shared by RSI, Stoch %K, and any other percentile-profile indicator). */
function mappingCornerPoints(
  profile: { p5: number; p20: number; p50: number; p80: number; p95: number },
  trending: boolean,
): { x: number; y: number }[] {
  // Ranging/volatile (higher_is_bullish=false): high value → bearish (negative scores).
  // Trending (higher_is_bullish=true): high value → bullish (positive scores). Sign flipped.
  const m = trending ? -1 : 1;
  return [
    { x: 0,           y: 100 * m  },
    { x: profile.p5,  y: 80 * m   },
    { x: profile.p20, y: 40 * m   },
    { x: profile.p50, y: 0        },
    { x: profile.p80, y: -40 * m  },
    { x: profile.p95, y: -80 * m  },
    { x: 100,         y: -100 * m },
  ];
}

export function RsiMappingChart({ profile, today, score, regime, label = 'RSI' }: RsiMappingChartProps) {
  // REQUIRED guard: Number.isFinite catches both NaN and null/undefined coerced to NaN.
  if (
    !Number.isFinite(today) ||
    !Number.isFinite(score) ||
    !Number.isFinite(profile.p5) ||
    !Number.isFinite(profile.p20) ||
    !Number.isFinite(profile.p50) ||
    !Number.isFinite(profile.p80) ||
    !Number.isFinite(profile.p95)
  ) {
    return null;
  }

  // Warn (but render) if regime is unrecognised — guards future backend drift.
  if (regime !== 'trending' && regime !== 'ranging' && regime !== 'volatile') {
    // eslint-disable-next-line no-console
    console.warn(`RsiMappingChart(${label}): unrecognised regime "${regime}", treating as ranging.`);
  }

  const trending = regime === 'trending';
  const activePts = mappingCornerPoints(profile, trending);
  const inactivePts = mappingCornerPoints(profile, !trending);

  // Clamp today's X to [0, 100] so a glitched value never escapes the viewBox.
  const clampedToday = Math.max(0, Math.min(100, today));

  // REQUIRED: walk the corner-point array to find which zone `today` falls in.
  // Use the bracketing corner points as the zone anchors for the math table.
  let bracketIndex = 0;
  for (let i = 0; i < activePts.length - 1; i++) {
    if (clampedToday >= activePts[i].x && clampedToday <= activePts[i + 1].x) {
      bracketIndex = i;
      break;
    }
  }
  const zoneLo = activePts[bracketIndex];
  const zoneHi = activePts[bracketIndex + 1];
  const zoneSpan = zoneHi.x - zoneLo.x;
  const t = zoneSpan === 0 ? 0 : (clampedToday - zoneLo.x) / zoneSpan;
  const yToday = zoneLo.y + t * (zoneHi.y - zoneLo.y);
  const zoneName = ZONE_NAMES[bracketIndex] ?? 'unknown';

  // SVG geometry
  const pad = 28;
  const padY = 14;
  const W = 400;
  const H = 160;
  const xAt = (x: number) => pad + ((W - 2 * pad) * x) / 100;
  const yAt = (y: number) => padY + ((H - 2 * padY) * (1 - (y + 100) / 200));
  const activeLine = activePts.map((p) => `${xAt(p.x)},${yAt(p.y)}`).join(' ');
  const inactiveLine = inactivePts.map((p) => `${xAt(p.x)},${yAt(p.y)}`).join(' ');

  // Regime explanation for the chip
  const regimeNote =
    regime === 'trending'
      ? `sign FLIPPED (continuation — high ${label} is bullish)`
      : regime === 'volatile'
        ? `mean-reversion (same as ranging — high ${label} is bearish)`
        : `mean-reversion (high ${label} is bearish)`;
  const counterfactualLabel = trending ? 'if ranging' : 'if trending';

  // Number formatting helpers — keep signed strings consistent.
  const sgn = (n: number) => (n > 0 ? `+${n.toFixed(0)}` : n.toFixed(0));


  return (
    <div className="w-full">
      <div className="mb-2 flex items-center gap-3 text-[10px] flex-wrap">
        <span className="font-mono uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
          regime: {regime}
        </span>
        <span className="text-muted-foreground">{regimeNote}</span>
        <span className="flex items-center gap-1 text-muted-foreground">
          <svg width={20} height={6}><line x1={0} x2={20} y1={3} y2={3} stroke="hsl(var(--foreground))" strokeWidth={1.5} /></svg>
          active
        </span>
        <span className="flex items-center gap-1 text-muted-foreground">
          <svg width={20} height={6}><line x1={0} x2={20} y1={3} y2={3} stroke="hsl(var(--muted-foreground))" strokeWidth={1} strokeDasharray="3 2" strokeOpacity={0.5} /></svg>
          {counterfactualLabel}
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: H }}>
        {[0, 40, -40, 80, -80].map((s) => (
          <line key={s} x1={pad} x2={W - pad} y1={yAt(s)} y2={yAt(s)} stroke="hsl(var(--border))" strokeOpacity={0.4} strokeWidth={1} strokeDasharray={s === 0 ? undefined : '2 4'} />
        ))}
        <line x1={pad} x2={W - pad} y1={yAt(0)} y2={yAt(0)} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.6} strokeWidth={1} />
        {[
          { label: 'p5', v: profile.p5 },
          { label: 'p20', v: profile.p20 },
          { label: 'p50', v: profile.p50 },
          { label: 'p80', v: profile.p80 },
          { label: 'p95', v: profile.p95 },
        ].map((tt) => (
          <g key={tt.label}>
            <line x1={xAt(tt.v)} x2={xAt(tt.v)} y1={padY} y2={H - padY} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.15} strokeWidth={1} strokeDasharray="1 3" />
            <text x={xAt(tt.v)} y={H - 2} fill="hsl(var(--muted-foreground))" fontSize={8} textAnchor="middle" fontFamily="JetBrains Mono, monospace">{tt.label}</text>
          </g>
        ))}
        {[100, 50, 0, -50, -100].map((s) => (
          <text key={s} x={pad - 4} y={yAt(s) + 3} fill="hsl(var(--muted-foreground))" fontSize={8} textAnchor="end" fontFamily="JetBrains Mono, monospace">{s > 0 ? `+${s}` : s}</text>
        ))}
        <polyline points={inactiveLine} fill="none" stroke="hsl(var(--muted-foreground))" strokeWidth={1} strokeDasharray="3 2" strokeOpacity={0.45} />
        <polyline points={activeLine} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
        <line x1={xAt(clampedToday)} x2={xAt(clampedToday)} y1={padY} y2={yAt(yToday)} stroke="hsl(var(--primary))" strokeOpacity={0.4} strokeDasharray="2 2" strokeWidth={1} />
        <line x1={pad} x2={xAt(clampedToday)} y1={yAt(yToday)} y2={yAt(yToday)} stroke="hsl(var(--primary))" strokeOpacity={0.4} strokeDasharray="2 2" strokeWidth={1} />
        <circle cx={xAt(clampedToday)} cy={yAt(yToday)} r={4} fill="hsl(var(--primary))" stroke="hsl(var(--card))" strokeWidth={1.5} />
        <text x={xAt(clampedToday) + 8} y={yAt(yToday) + 3} fill="hsl(var(--primary))" fontSize={10} fontWeight={600} fontFamily="JetBrains Mono, monospace">
          {yToday.toFixed(1)}
        </text>
      </svg>
      <table className="mt-2 w-full text-[10px] font-mono">
        <tbody>
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">{label} value</td>
            <td className="text-foreground">{today.toFixed(1)}</td>
          </tr>
          {(() => {
            // Symbolic labels for zone endpoints. Bracket 0's lower bound is the
            // absolute floor (0), and bracket 5's upper bound is the absolute
            // ceiling (100); the four interior boundaries are named percentiles.
            const PERCENTILE_LABELS = [5, 20, 50, 80, 95];
            const loLabel = bracketIndex === 0 ? '0' : `p${PERCENTILE_LABELS[bracketIndex - 1]}`;
            const hiLabel = bracketIndex === 5 ? '100' : `p${PERCENTILE_LABELS[bracketIndex]}`;
            const loDisplay = bracketIndex === 0 ? '0' : `${loLabel} (${zoneLo.x.toFixed(1)})`;
            const hiDisplay = bracketIndex === 5 ? '100' : `${hiLabel} (${zoneHi.x.toFixed(1)})`;
            return (
              <>
                <tr>
                  <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Zone</td>
                  <td className="text-foreground">
                    {loDisplay} – {hiDisplay} — {zoneName}
                  </td>
                </tr>
                <tr>
                  <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Position in zone</td>
                  <td className="text-foreground">
                    {(t * 100).toFixed(0)}%{' '}
                    <span className="text-muted-foreground">
                      (= ({label} − {loLabel}) ÷ ({hiLabel} − {loLabel}) = ({today.toFixed(1)} − {zoneLo.x.toFixed(1)}) ÷ ({zoneHi.x.toFixed(1)} − {zoneLo.x.toFixed(1)}) = {(today - zoneLo.x).toFixed(1)} ÷ {(zoneHi.x - zoneLo.x).toFixed(1)})
                    </span>
                  </td>
                </tr>
                <tr>
                  <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Zone score range</td>
                  <td className="text-foreground">{sgn(zoneLo.y)} to {sgn(zoneHi.y)}</td>
                </tr>
                <tr>
                  <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Base score</td>
                  <td className="text-foreground">
                    {yToday.toFixed(1)}{' '}
                    <span className="text-muted-foreground">
                      (= score_lo + t × (score_hi − score_lo) = {sgn(zoneLo.y)} + {t.toFixed(2)} × ({sgn(zoneHi.y - zoneLo.y)}))
                    </span>
                  </td>
                </tr>
              </>
            );
          })()}
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Regime</td>
            <td className="text-foreground">{regime} ({regimeNote})</td>
          </tr>
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap font-semibold">Final score</td>
            <td className="text-primary font-semibold">{score > 0 ? '+' : ''}{score.toFixed(1)}</td>
          </tr>
        </tbody>
      </table>
      <p className="mt-1 text-[9px] text-muted-foreground italic">
        Base score is the chart's interpolated value; final score is the system's persisted value. They normally match; minor differences (≤ 1–2 pts) can occur near zone boundaries or in the extreme tails where the scorer uses mirrored extrapolation.
      </p>
    </div>
  );
}
