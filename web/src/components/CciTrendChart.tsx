/**
 * CciTrendChart — sparkline for CCI(20) with a 9-period SMA signal line.
 *
 * Promoted from Variant A of the prototype scaffold in IndicatorExplainerPanel.
 * Renders a LineChart with dynamic auto-scale, reference lines at ±100/±200,
 * zone tinting beyond ±100, a today marker on the last data point, and a
 * dashed primary-coloured 9-SMA signal line.
 *
 * The 9-period SMA is computed client-side on the already-filtered series
 * (non-finite rows excluded first) and is NOT persisted to the database.
 *
 * @param data - Array of { date, cci } objects ordered ascending by date.
 */

import {
  LineChart,
  Line,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceArea,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts';

interface CciTrendChartProps {
  data: { date: string; cci: number }[];
}

const SMA_PERIOD = 9;

/** Compute a trailing SMA of the given period on the values array. Returns null for warm-up entries. */
function computeSma(values: number[], period: number): (number | null)[] {
  return values.map((_, i) => {
    if (i < period - 1) return null;
    let sum = 0;
    for (let j = i - period + 1; j <= i; j++) sum += values[j];
    return Math.round((sum / period) * 10) / 10;
  });
}

export function CciTrendChart({ data }: CciTrendChartProps) {
  // Filter non-finite rows first — DB columns are nullable even when TS types say number.
  const filtered = data.filter((r) => Number.isFinite(r.cci));

  if (filtered.length === 0) {
    return <p className="text-muted-foreground italic text-xs">CCI sparkline unavailable.</p>;
  }

  // Compute 9-period SMA on the already-filtered series (order matters: SMA must
  // align with filtered indices, not raw indices).
  const smaValues = computeSma(filtered.map((r) => r.cci), SMA_PERIOD);

  const chartData = filtered.map((r, i) => ({
    date: r.date,
    cci: r.cci,
    signal: smaValues[i],
  }));

  const lastIndex = chartData.length - 1;
  const last = chartData[lastIndex];

  // Dynamic auto-scale: ensure ±200 always visible; expand if CCI exceeds that.
  const maxAbs = Math.max(200, ...filtered.map((r) => Math.abs(r.cci)));
  const dom: [number, number] = [-maxAbs, maxAbs];

  return (
    <div className="h-44 w-full">
      <ResponsiveContainer>
        <LineChart data={chartData} margin={{ top: 6, right: 36, bottom: 16, left: 4 }}>
          <XAxis
            dataKey="date"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            interval={Math.max(1, Math.floor(chartData.length / 4))}
            tickFormatter={(v: string) => v.slice(5)}
            stroke="hsl(var(--border))"
          />
          <YAxis
            domain={dom}
            ticks={[-200, -100, 0, 100, 200]}
            orientation="right"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            stroke="hsl(var(--border))"
            width={32}
          />
          {/* Zone tinting: oversold (below -100) and overbought (above +100) */}
          <ReferenceArea y1={100} y2={maxAbs} fill="hsl(var(--down))" fillOpacity={0.06} />
          <ReferenceArea y1={-maxAbs} y2={-100} fill="hsl(var(--up))" fillOpacity={0.06} />
          {/* Reference lines at ±200 (hyper extremes) */}
          <ReferenceLine
            y={200}
            stroke="hsl(var(--down))"
            strokeDasharray="2 4"
            strokeOpacity={0.5}
            label={{ value: '+200', position: 'right', fill: 'hsl(var(--down))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          <ReferenceLine
            y={-200}
            stroke="hsl(var(--up))"
            strokeDasharray="2 4"
            strokeOpacity={0.5}
            label={{ value: '-200', position: 'right', fill: 'hsl(var(--up))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          {/* Reference lines at ±100 (canonical CCI oversold/overbought) */}
          <ReferenceLine
            y={100}
            stroke="hsl(var(--down))"
            strokeDasharray="3 3"
            strokeOpacity={0.6}
            label={{ value: '+100', position: 'right', fill: 'hsl(var(--down))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          <ReferenceLine
            y={-100}
            stroke="hsl(var(--up))"
            strokeDasharray="3 3"
            strokeOpacity={0.6}
            label={{ value: '-100', position: 'right', fill: 'hsl(var(--up))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          <ReferenceLine y={0} stroke="hsl(var(--muted-foreground))" strokeDasharray="2 4" strokeOpacity={0.4} />
          {/* CCI line with today marker on last point */}
          <Line
            type="monotone"
            dataKey="cci"
            stroke="hsl(var(--foreground))"
            strokeWidth={1.75}
            dot={(props: any) => {
              if (props.index !== lastIndex) return <g />;
              return (
                <g key={`today-dot-${props.index}`}>
                  <circle
                    cx={props.cx}
                    cy={props.cy}
                    r={4}
                    fill="hsl(var(--primary))"
                    stroke="hsl(var(--card))"
                    strokeWidth={1.5}
                  />
                  <text
                    x={props.cx + 8}
                    y={props.cy + 3}
                    fill="hsl(var(--primary))"
                    fontSize={9}
                    fontFamily="JetBrains Mono, monospace"
                  >
                    {last.cci.toFixed(1)}
                  </text>
                </g>
              );
            }}
            isAnimationActive={false}
          />
          {/* 9-period SMA signal line — dashed primary colour */}
          <Line
            type="monotone"
            dataKey="signal"
            stroke="hsl(var(--primary))"
            strokeWidth={1.25}
            strokeDasharray="4 3"
            dot={false}
            connectNulls={false}
            isAnimationActive={false}
          />
          <Tooltip
            contentStyle={{
              fontSize: '10px',
              padding: '4px 8px',
              backgroundColor: 'hsl(var(--card))',
              border: '1px solid hsl(var(--border))',
              fontFamily: 'JetBrains Mono, monospace',
            }}
            labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
            itemStyle={{ color: 'hsl(var(--foreground))' }}
            formatter={(value: number, name: string) => [
              value.toFixed(2),
              name === 'signal' ? 'Signal (9-SMA)' : 'CCI',
            ]}
            cursor={{ stroke: 'hsl(var(--muted-foreground))', strokeWidth: 1, strokeDasharray: '2 2' }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
