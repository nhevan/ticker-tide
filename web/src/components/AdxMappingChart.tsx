/**
 * AdxMappingChart — visualises score_adx as a piecewise-linear function
 * over a 4-band partition of the 0-100 ADX axis, with the curve, zone
 * bands, and discontinuity location all derived from the bands prop
 * (i.e., from /api/scoring-rules.adx). The y-axis is hardcoded at -20/+80,
 * which is score_adx's stable mathematical range; only the band edges
 * vary with config.
 *
 * Raw SVG (no Recharts). Zone bands rendered as tinted backgrounds; curve
 * overlay shows the score output with a marker dot at the current
 * (value, score) point. Discontinuity at `discontinuityAt` is rendered as
 * open + filled circle markers showing the +20 → +40 score jump.
 *
 * gotcha #3 (useId for SVG defs) — not applicable; no <defs> used.
 *
 * Score displayed alongside the value is the persisted Python scorer
 * value. Minor floating-point divergence from a JS replica is expected
 * and acceptable.
 *
 * @param value - Current ADX reading (0-100). Must be finite.
 * @param score - Persisted score from daily.indicator_scores.adx. Must be finite.
 * @param bands - 4-band partition from rules.adx.bands.
 * @param discontinuityAt - X-coordinate of the score jump (25.0 in current config).
 */

import type { AdxBand } from '@/lib/api/types';

interface AdxMappingChartProps {
  value: number;
  score: number;
  bands: AdxBand[];
  discontinuityAt: number;
}

export function AdxMappingChart({ value, score, bands, discontinuityAt }: AdxMappingChartProps) {
  // Defensive guard — recipe gotcha #6: never return null into a StepCard.
  if (
    bands.length < 4 ||
    !Number.isFinite(value) ||
    !Number.isFinite(score) ||
    !Number.isFinite(discontinuityAt)
  ) {
    return (
      <p className="text-muted-foreground italic">
        Mapping chart unavailable (incomplete config).
      </p>
    );
  }

  const w = 560;
  const h = 180;
  const pad = { l: 32, r: 16, t: 14, b: 22 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;

  // X maps ADX 0–100; Y maps score -20 (bottom) to +80 (top) — stable mathematical range.
  const xFor = (v: number) => pad.l + (v / 100) * innerW;
  const yFor = (s: number) => pad.t + ((80 - s) / 100) * innerH;

  // bands is guaranteed by the caller's guard to have 4 entries in order:
  // [ranging, weak_trend_developing, developing_trend, strong_trend].
  const [ranging, weak, dev, strong] = bands;

  // Curve segments derived from band min/max/score_min/score_max (Finding 4).
  const segLow    = `M${xFor(ranging.min).toFixed(1)},${yFor(ranging.score_min).toFixed(1)} L${xFor(ranging.max).toFixed(1)},${yFor(ranging.score_max).toFixed(1)}`;
  const segWeak   = `M${xFor(weak.min).toFixed(1)},${yFor(weak.score_min).toFixed(1)} L${xFor(weak.max).toFixed(1)},${yFor(weak.score_max).toFixed(1)}`;
  const segDev    = `M${xFor(dev.min).toFixed(1)},${yFor(dev.score_min).toFixed(1)} L${xFor(dev.max).toFixed(1)},${yFor(dev.score_max).toFixed(1)}`;
  const segStrong = `M${xFor(strong.min).toFixed(1)},${yFor(strong.score_min).toFixed(1)} L${xFor(strong.max).toFixed(1)},${yFor(strong.score_max).toFixed(1)}`;

  // Zone band fill config — opacity increases with trend strength.
  const zoneFills: { band: AdxBand; fill: string; opacity: number }[] = [
    { band: ranging, fill: 'hsl(var(--muted-foreground))', opacity: 0.10 },
    { band: weak,    fill: 'hsl(var(--up))',               opacity: 0.06 },
    { band: dev,     fill: 'hsl(var(--up))',               opacity: 0.12 },
    { band: strong,  fill: 'hsl(var(--up))',               opacity: 0.20 },
  ];

  return (
    <svg
      viewBox={`0 0 ${w} ${h}`}
      className="w-full"
      style={{ height: h }}
      preserveAspectRatio="none"
    >
      {/* Zone band backgrounds with labels derived from band.name */}
      {zoneFills.map(({ band, fill, opacity }) => (
        <g key={band.name}>
          <rect
            x={xFor(band.min)}
            y={pad.t}
            width={xFor(band.max) - xFor(band.min)}
            height={innerH}
            fill={fill}
            fillOpacity={opacity}
          />
          <text
            x={(xFor(band.min) + xFor(band.max)) / 2}
            y={pad.t + 10}
            fontSize={8}
            fontFamily="JetBrains Mono, monospace"
            fill="hsl(var(--muted-foreground))"
            textAnchor="middle"
          >
            {band.name.replace(/_/g, ' ')}
          </text>
        </g>
      ))}

      {/* Zero baseline */}
      <line
        x1={pad.l}
        x2={pad.l + innerW}
        y1={yFor(0)}
        y2={yFor(0)}
        stroke="hsl(var(--muted-foreground))"
        strokeOpacity={0.4}
      />

      {/* Curve segments */}
      <path d={segLow}    fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segWeak}   fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segDev}    fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />
      <path d={segStrong} fill="none" stroke="hsl(var(--foreground))" strokeWidth={2} />

      {/* Discontinuity markers at discontinuityAt (open circle = below, filled = above) */}
      <circle
        cx={xFor(discontinuityAt)}
        cy={yFor(weak.score_max)}
        r={2.5}
        fill="hsl(var(--background))"
        stroke="hsl(var(--foreground))"
        strokeWidth={1.2}
      />
      <circle
        cx={xFor(discontinuityAt)}
        cy={yFor(dev.score_min)}
        r={2.5}
        fill="hsl(var(--foreground))"
      />

      {/* Value + score dot */}
      <circle
        cx={xFor(value)}
        cy={yFor(score)}
        r={4.5}
        fill="hsl(var(--primary))"
        stroke="hsl(var(--card))"
        strokeWidth={1.5}
      />
      <text
        x={xFor(value) + 6}
        y={yFor(score) - 6}
        fontSize={10}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--primary))"
      >
        ADX {value.toFixed(1)} → {score >= 0 ? '+' : ''}{score.toFixed(1)}
      </text>

      {/* Y-axis labels */}
      <text x={pad.l - 4} y={yFor(80) + 3}  fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--up))"               textAnchor="end">+80</text>
      <text x={pad.l - 4} y={yFor(40) + 3}  fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--up))"               textAnchor="end">+40</text>
      <text x={pad.l - 4} y={yFor(0)  + 3}  fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--muted-foreground))" textAnchor="end">0</text>
      <text x={pad.l - 4} y={yFor(-20) + 3} fontSize={9} fontFamily="JetBrains Mono, monospace" fill="hsl(var(--down))"             textAnchor="end">−20</text>

      {/* X-axis labels at band boundaries */}
      {[0, ...bands.map((b) => b.max)].map((v) => (
        <text
          key={v}
          x={xFor(v)}
          y={pad.t + innerH + 12}
          fontSize={9}
          fontFamily="JetBrains Mono, monospace"
          fill="hsl(var(--muted-foreground))"
          textAnchor="middle"
        >
          {v}
        </text>
      ))}
    </svg>
  );
}
