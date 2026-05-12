/**
 * MacdMappingChart — visualises how a MACD line value becomes its scored output.
 *
 * MACD scoring uses z-score normalisation when a per-ticker profile is
 * available (score_with_zscore in src/scorer/indicator_scorer.py), with a
 * linear fallback `clamp(value × 20, ±100)` when not. This chart renders
 * the active mapping function as a piecewise polyline on a value (x) vs
 * score (y) plane, with today's point marked.
 *
 * Profile (z-score) path corner points:
 *   z = -3 → -100, z = -2 → -80, z = -1 → -40, z = 0 → 0,
 *   z = +1 → +40, z = +2 → +80, z = +3 → +100
 *   The x positions are mean + z·std.
 *
 * Fallback (linear) path corner points:
 *   x = -5 → -100, x = 0 → 0, x = +5 → +100 (slope 20, clamped)
 *
 * @param profile - {mean, std} from snapshot.daily.macd_line_profile, or null.
 * @param today   - The raw macd_line reading.
 * @param score   - The persisted indicator score from indicator_scores.macd_line.
 * @returns The chart + math table, or null if any required numeric input is non-finite.
 */
interface MacdMappingChartProps {
  profile: { mean: number; std: number } | null;
  today: number;
  score: number;
}

function zScoreCornerPoints(mean: number, std: number): { x: number; y: number }[] {
  return [
    { x: mean - 3 * std, y: -100 },
    { x: mean - 2 * std, y: -80 },
    { x: mean - 1 * std, y: -40 },
    { x: mean, y: 0 },
    { x: mean + 1 * std, y: 40 },
    { x: mean + 2 * std, y: 80 },
    { x: mean + 3 * std, y: 100 },
  ];
}

function linearCornerPoints(): { x: number; y: number }[] {
  return [
    { x: -5, y: -100 },
    { x: 0, y: 0 },
    { x: 5, y: 100 },
  ];
}

export function MacdMappingChart({ profile, today, score }: MacdMappingChartProps) {
  if (!Number.isFinite(today) || !Number.isFinite(score)) return null;
  const usingProfile = profile !== null && Number.isFinite(profile.mean) && Number.isFinite(profile.std) && profile.std > 0;

  const pts = usingProfile
    ? zScoreCornerPoints(profile.mean, profile.std)
    : linearCornerPoints();

  // X-domain: stretch slightly past the corner points so today's marker has room.
  const xMin = pts[0].x;
  const xMax = pts[pts.length - 1].x;
  const xRange = xMax - xMin;
  // Clamp today within the visible range; mark visually with a special tail badge
  // if the raw value escapes the curve domain.
  const clampedToday = Math.max(xMin, Math.min(xMax, today));

  // Find the bracketing corner-point segment and interpolate today's y for the line marker.
  let bracket = 0;
  for (let i = 0; i < pts.length - 1; i++) {
    if (clampedToday >= pts[i].x && clampedToday <= pts[i + 1].x) {
      bracket = i;
      break;
    }
  }
  const segLo = pts[bracket];
  const segHi = pts[bracket + 1];
  const segSpan = segHi.x - segLo.x;
  const t = segSpan === 0 ? 0 : (clampedToday - segLo.x) / segSpan;
  const yToday = segLo.y + t * (segHi.y - segLo.y);

  // SVG geometry
  const pad = 32;
  const padY = 14;
  const W = 400;
  const H = 160;
  const xAt = (x: number) => pad + ((W - 2 * pad) * (x - xMin)) / xRange;
  const yAt = (y: number) => padY + ((H - 2 * padY) * (1 - (y + 100) / 200));
  const linePath = pts.map((p) => `${xAt(p.x)},${yAt(p.y)}`).join(' ');

  const sgn = (n: number) => (n > 0 ? `+${n.toFixed(0)}` : n.toFixed(0));
  const z = usingProfile && profile ? (today - profile.mean) / profile.std : null;

  return (
    <div className="w-full">
      <div className="mb-2 flex items-center gap-3 text-[10px] flex-wrap">
        <span className="font-mono uppercase tracking-wider px-1.5 py-0.5 bg-muted text-foreground rounded-sm">
          {usingProfile ? 'profile (z-score)' : 'fallback (linear)'}
        </span>
        <span className="text-muted-foreground">
          {usingProfile
            ? 'piecewise z-score mapping — higher z = bullish'
            : 'no profile available — linear score = clamp(value × 20, ±100)'}
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: H }}>
        {/* Horizontal score-axis grid */}
        {[0, 40, -40, 80, -80].map((s) => (
          <line
            key={s}
            x1={pad}
            x2={W - pad}
            y1={yAt(s)}
            y2={yAt(s)}
            stroke="hsl(var(--border))"
            strokeOpacity={0.4}
            strokeWidth={1}
            strokeDasharray={s === 0 ? undefined : '2 4'}
          />
        ))}
        <line
          x1={pad}
          x2={W - pad}
          y1={yAt(0)}
          y2={yAt(0)}
          stroke="hsl(var(--muted-foreground))"
          strokeOpacity={0.6}
          strokeWidth={1}
        />
        {/* Vertical x-axis tick lines at corner-point x positions */}
        {pts.map((p, i) => (
          <g key={i}>
            <line
              x1={xAt(p.x)}
              x2={xAt(p.x)}
              y1={padY}
              y2={H - padY}
              stroke="hsl(var(--muted-foreground))"
              strokeOpacity={0.12}
              strokeWidth={1}
              strokeDasharray="1 3"
            />
            <text
              x={xAt(p.x)}
              y={H - 2}
              fill="hsl(var(--muted-foreground))"
              fontSize={8}
              textAnchor="middle"
              fontFamily="JetBrains Mono, monospace"
            >
              {p.x.toFixed(2)}
            </text>
          </g>
        ))}
        {/* Y-axis labels */}
        {[100, 50, 0, -50, -100].map((s) => (
          <text
            key={s}
            x={pad - 4}
            y={yAt(s) + 3}
            fill="hsl(var(--muted-foreground))"
            fontSize={8}
            textAnchor="end"
            fontFamily="JetBrains Mono, monospace"
          >
            {s > 0 ? `+${s}` : s}
          </text>
        ))}
        {/* Mapping curve */}
        <polyline
          points={linePath}
          fill="none"
          stroke="hsl(var(--foreground))"
          strokeWidth={1.75}
        />
        {/* Today's marker — dropped lines + dot + label */}
        <line
          x1={xAt(clampedToday)}
          x2={xAt(clampedToday)}
          y1={padY}
          y2={yAt(yToday)}
          stroke="hsl(var(--primary))"
          strokeOpacity={0.4}
          strokeDasharray="2 2"
          strokeWidth={1}
        />
        <line
          x1={pad}
          x2={xAt(clampedToday)}
          y1={yAt(yToday)}
          y2={yAt(yToday)}
          stroke="hsl(var(--primary))"
          strokeOpacity={0.4}
          strokeDasharray="2 2"
          strokeWidth={1}
        />
        <circle
          cx={xAt(clampedToday)}
          cy={yAt(yToday)}
          r={4}
          fill="hsl(var(--primary))"
          stroke="hsl(var(--card))"
          strokeWidth={1.5}
        />
        <text
          x={xAt(clampedToday) + 8}
          y={yAt(yToday) + 3}
          fill="hsl(var(--primary))"
          fontSize={10}
          fontWeight={600}
          fontFamily="JetBrains Mono, monospace"
        >
          {yToday.toFixed(1)}
        </text>
      </svg>
      <table className="mt-2 w-full text-[10px] font-mono">
        <tbody>
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">MACD value</td>
            <td className="text-foreground">{today.toFixed(2)}</td>
          </tr>
          {usingProfile && profile && z !== null ? (
            <>
              <tr>
                <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">Profile</td>
                <td className="text-foreground">
                  mean = {profile.mean.toFixed(2)}, std = {profile.std.toFixed(2)}
                </td>
              </tr>
              <tr>
                <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">z-score</td>
                <td className="text-foreground">
                  {z.toFixed(2)}{' '}
                  <span className="text-muted-foreground">
                    (= (MACD − mean) ÷ std = ({today.toFixed(2)} − {profile.mean.toFixed(2)}) ÷{' '}
                    {profile.std.toFixed(2)})
                  </span>
                </td>
              </tr>
              <tr>
                <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">
                  Bracket
                </td>
                <td className="text-foreground">
                  z ∈ [{((segLo.x - profile.mean) / profile.std).toFixed(1)},{' '}
                  {((segHi.x - profile.mean) / profile.std).toFixed(1)}] → score{' '}
                  {sgn(segLo.y)} to {sgn(segHi.y)}
                </td>
              </tr>
              <tr>
                <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">
                  Base score
                </td>
                <td className="text-foreground">
                  {yToday.toFixed(1)}{' '}
                  <span className="text-muted-foreground">
                    (= {sgn(segLo.y)} + {t.toFixed(2)} × ({sgn(segHi.y - segLo.y)}))
                  </span>
                </td>
              </tr>
            </>
          ) : (
            <tr>
              <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">
                Base score
              </td>
              <td className="text-foreground">
                {yToday.toFixed(1)}{' '}
                <span className="text-muted-foreground">
                  (= clamp(MACD × 20, ±100) = clamp({(today * 20).toFixed(1)}, ±100))
                </span>
              </td>
            </tr>
          )}
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap font-semibold">
              Final score
            </td>
            <td className="text-primary font-semibold">
              {score > 0 ? '+' : ''}
              {score.toFixed(1)}
            </td>
          </tr>
        </tbody>
      </table>
      <p className="mt-1 text-[9px] text-muted-foreground italic">
        Base score is the chart's interpolated value; final score is the system's persisted value.
        They normally match; minor differences can occur in the extreme tails where the scorer
        clamps past ±2σ.
      </p>
    </div>
  );
}
