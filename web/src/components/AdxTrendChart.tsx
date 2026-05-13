/**
 * AdxTrendChart — single-series ADX value over a date window, with shaded
 * trend-strength zones (ranging / weak / developing / strong) and an
 * end-of-series marker.
 *
 * Raw SVG (no Recharts) for pixel-perfect end-of-line label placement;
 * matches the RsiTrendChart / StochTrendChart precedent.
 *
 * REQUIRED: assumes `data.length >= 2`. Single-point and empty arrays
 * should be guarded at the call site — the chart will render incorrectly
 * (NaN coordinates from divide-by-zero) on a one-point series.
 *
 * Y-axis is fixed 0–80. ADX values above 80 are extremely rare in
 * practice; values that high would clip out the top of the chart. The
 * scoring function (score_adx in indicator_scorer.py) caps the +80 score
 * region at ADX >= 40, so the 80 ceiling is appropriate display range.
 *
 * @param data - Ascending-date sparkline; each entry must have a finite `adx`.
 */

interface AdxTrendChartProps {
  data: { date: string; adx: number }[];
}

export function AdxTrendChart({ data }: AdxTrendChartProps) {
  const w = 560;
  const h = 160;
  const pad = { l: 8, r: 36, t: 6, b: 16 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const xFor = (i: number) => pad.l + (i / (data.length - 1)) * innerW;
  const yFor = (v: number) => pad.t + ((80 - v) / 80) * innerH;
  const path = data
    .map((p, i) => `${i === 0 ? 'M' : 'L'}${xFor(i).toFixed(1)},${yFor(p.adx).toFixed(1)}`)
    .join(' ');
  const last = data[data.length - 1];

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-40" preserveAspectRatio="none">
      {/* Ranging zone: 0–20 */}
      <rect
        x={pad.l}
        y={yFor(20)}
        width={innerW}
        height={yFor(0) - yFor(20)}
        fill="hsl(var(--muted-foreground))"
        fillOpacity={0.08}
      />
      {/* Weak trend zone: 20–25 */}
      <rect
        x={pad.l}
        y={yFor(25)}
        width={innerW}
        height={yFor(20) - yFor(25)}
        fill="hsl(var(--up))"
        fillOpacity={0.05}
      />
      {/* Developing trend zone: 25–40 */}
      <rect
        x={pad.l}
        y={yFor(40)}
        width={innerW}
        height={yFor(25) - yFor(40)}
        fill="hsl(var(--up))"
        fillOpacity={0.10}
      />
      {/* Strong trend zone: 40–80 */}
      <rect
        x={pad.l}
        y={yFor(80)}
        width={innerW}
        height={yFor(40) - yFor(80)}
        fill="hsl(var(--up))"
        fillOpacity={0.18}
      />
      {/* ADX line */}
      <path d={path} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
      {/* End-of-series dot */}
      <circle
        cx={xFor(data.length - 1)}
        cy={yFor(last.adx)}
        r={3.5}
        fill="hsl(var(--foreground))"
      />
      {/* End-of-series label */}
      <text
        x={pad.l + innerW + 4}
        y={yFor(last.adx) + 3}
        fontSize={9}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--foreground))"
      >
        ADX {last.adx.toFixed(1)}
      </text>
      {/* Zone band labels */}
      <text
        x={pad.l + 4}
        y={yFor(10) - 2}
        fontSize={8}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--muted-foreground))"
        opacity={0.7}
      >
        ranging
      </text>
      <text
        x={pad.l + 4}
        y={yFor(22) - 2}
        fontSize={8}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--up))"
        opacity={0.7}
      >
        weak
      </text>
      <text
        x={pad.l + 4}
        y={yFor(32) - 2}
        fontSize={8}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--up))"
        opacity={0.8}
      >
        developing
      </text>
      <text
        x={pad.l + 4}
        y={yFor(60) - 2}
        fontSize={8}
        fontFamily="JetBrains Mono, monospace"
        fill="hsl(var(--up))"
        opacity={0.9}
      >
        strong
      </text>
    </svg>
  );
}
