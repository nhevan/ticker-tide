/**
 * Stochastic %K/%D trend chart — last N working days of %K and %D for the
 * loaded ticker. Two distinct hues, equal stroke weight (Variant A, picked).
 * Theme-aware via CSS vars. Expects data ordered ascending by date.
 *
 * Raw SVG implementation (not Recharts). Recharts was not used here because
 * the end-of-line label placement for two series (%K and %D on the same right
 * margin) requires pixel-precise manual positioning that Recharts complicates
 * without a custom label renderer. Raw SVG keeps xFor/yFor trivial.
 *
 * ASSUMPTION: data is non-empty. The empty-data guard lives at the call site
 * (Step 2 in StochKPanelPrototype) — this component renders nothing useful
 * with zero points so callers must check before rendering.
 */

interface StochTrendChartProps {
  /**
   * Ordered ascending by date. k is always a finite number (server-side
   * filters stoch_k IS NULL rows). d is nullable — null during the 3-period
   * SMA warm-up. Null rows create a visual gap in the %D path.
   */
  data: { date: string; k: number; d: number | null }[];
}

export function StochTrendChart({ data }: StochTrendChartProps) {
  const w = 560;
  const h = 160;
  const pad = { l: 8, r: 36, t: 6, b: 16 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const xFor = (i: number) => pad.l + (i / (data.length - 1)) * innerW;
  const yFor = (v: number) => pad.t + ((100 - v) / 100) * innerH;

  // %K path — always drawn; k is never null.
  const pathK = data
    .map((p, i) => `${i === 0 ? 'M' : 'L'}${xFor(i).toFixed(1)},${yFor(p.k).toFixed(1)}`)
    .join(' ');

  // %D path — split into sub-paths at null gaps.
  // Strategy: iterate rows and start a new 'M' segment after any null row so
  // the null produces a visual gap rather than an interpolated line. This is
  // simpler than rendering multiple <path> elements (single string concat).
  const pathDSegments: string[] = [];
  let inSegment = false;
  for (let i = 0; i < data.length; i++) {
    const d = data[i].d;
    if (d === null) {
      inSegment = false;
    } else {
      const cmd = inSegment ? 'L' : 'M';
      pathDSegments.push(`${cmd}${xFor(i).toFixed(1)},${yFor(d).toFixed(1)}`);
      inSegment = true;
    }
  }
  const pathD = pathDSegments.join(' ');

  const last = data[data.length - 1];
  const lastD = last.d;

  return (
    <div className="w-full">
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-40" preserveAspectRatio="none">
        {/* Overbought zone tint (80–100) */}
        <rect
          x={pad.l}
          y={yFor(100)}
          width={innerW}
          height={yFor(80) - yFor(100)}
          fill="hsl(var(--down))"
          fillOpacity={0.07}
        />
        {/* Oversold zone tint (0–20) */}
        <rect
          x={pad.l}
          y={yFor(20)}
          width={innerW}
          height={yFor(0) - yFor(20)}
          fill="hsl(var(--up))"
          fillOpacity={0.07}
        />
        {/* Reference lines */}
        <line
          x1={pad.l} x2={pad.l + innerW}
          y1={yFor(80)} y2={yFor(80)}
          stroke="hsl(var(--down))"
          strokeDasharray="3 3"
          strokeOpacity={0.6}
        />
        <line
          x1={pad.l} x2={pad.l + innerW}
          y1={yFor(50)} y2={yFor(50)}
          stroke="hsl(var(--muted-foreground))"
          strokeDasharray="2 4"
          strokeOpacity={0.3}
        />
        <line
          x1={pad.l} x2={pad.l + innerW}
          y1={yFor(20)} y2={yFor(20)}
          stroke="hsl(var(--up))"
          strokeDasharray="3 3"
          strokeOpacity={0.6}
        />
        {/* %D line (primary hue) — rendered first so %K sits on top */}
        {pathD && (
          <path d={pathD} fill="none" stroke="hsl(var(--primary))" strokeWidth={1.75} strokeOpacity={0.85} />
        )}
        {/* %K line (foreground hue) */}
        <path d={pathK} fill="none" stroke="hsl(var(--foreground))" strokeWidth={1.75} />
        {/* End-of-line dots */}
        <circle cx={xFor(data.length - 1)} cy={yFor(last.k)} r={3.5} fill="hsl(var(--foreground))" />
        {lastD !== null && (
          <circle cx={xFor(data.length - 1)} cy={yFor(lastD)} r={3.5} fill="hsl(var(--primary))" />
        )}
        {/* End-of-line labels — right margin */}
        <text
          x={pad.l + innerW + 4}
          y={yFor(last.k) + 3}
          fontSize={9}
          fontFamily="JetBrains Mono, monospace"
          fill="hsl(var(--foreground))"
        >
          %K {last.k.toFixed(1)}
        </text>
        {lastD !== null && (
          <text
            x={pad.l + innerW + 4}
            y={yFor(lastD) + 3}
            fontSize={9}
            fontFamily="JetBrains Mono, monospace"
            fill="hsl(var(--primary))"
          >
            %D {lastD.toFixed(1)}
          </text>
        )}
        {/* OB/OS level labels */}
        <text
          x={pad.l + innerW + 4}
          y={yFor(80) + 3}
          fontSize={9}
          fontFamily="JetBrains Mono, monospace"
          fill="hsl(var(--down))"
        >
          80
        </text>
        <text
          x={pad.l + innerW + 4}
          y={yFor(20) + 3}
          fontSize={9}
          fontFamily="JetBrains Mono, monospace"
          fill="hsl(var(--up))"
        >
          20
        </text>
      </svg>
      {/* Legend */}
      <div className="mt-1 flex justify-center gap-4 text-[10px] font-mono text-muted-foreground">
        <span>
          <span
            className="inline-block w-3 border-t-2 align-middle"
            style={{ borderColor: 'hsl(var(--foreground))' }}
          />{' '}
          %K
        </span>
        <span>
          <span
            className="inline-block w-3 border-t-2 align-middle"
            style={{ borderColor: 'hsl(var(--primary))' }}
          />{' '}
          %D
        </span>
      </div>
    </div>
  );
}
