/**
 * MACD trend chart — last N working days of MACD line, signal line, and
 * histogram for the loaded ticker. Classic single-chart MACD: histogram
 * bars (green when ≥0, red when <0) overlaid with the MACD line (solid)
 * and signal line (dashed), all sharing a zero baseline. Theme-aware via
 * CSS vars. Expects data ordered ascending by date.
 *
 * MACD is unbounded, so the y-domain is computed from the data with a
 * symmetric ±margin around zero to keep the zero baseline visually
 * centered when the series straddles it.
 */

import {
  ComposedChart,
  Line,
  Bar,
  Cell,
  ResponsiveContainer,
  ReferenceLine,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts';

interface MacdTrendChartProps {
  data: {
    date: string;
    macd_line: number;
    signal: number | null;
    histogram: number | null;
  }[];
}

export function MacdTrendChart({ data }: MacdTrendChartProps) {
  if (!data.length) return null;

  const lastIndex = data.length - 1;
  const last = data[lastIndex];

  // Symmetric y-domain around 0 with a small margin so the zero baseline is
  // visually centered. We pull from all three series since signal can briefly
  // exceed the line during sharp moves.
  const allValues: number[] = [];
  for (const row of data) {
    if (Number.isFinite(row.macd_line)) allValues.push(row.macd_line);
    if (row.signal !== null && Number.isFinite(row.signal)) allValues.push(row.signal);
    if (row.histogram !== null && Number.isFinite(row.histogram)) allValues.push(row.histogram);
  }
  const maxAbs = allValues.reduce((acc, v) => Math.max(acc, Math.abs(v)), 0) || 1;
  const domainExtent = maxAbs * 1.15;

  return (
    <div className="w-full">
      <div className="h-40 w-full">
      <ResponsiveContainer>
        <ComposedChart data={data} margin={{ top: 6, right: 40, bottom: 16, left: 4 }}>
          <XAxis
            dataKey="date"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            interval={Math.max(1, Math.floor(data.length / 4))}
            tickFormatter={(v: string) => v.slice(5)}
            stroke="hsl(var(--border))"
          />
          <YAxis
            domain={[-domainExtent, domainExtent]}
            orientation="right"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            stroke="hsl(var(--border))"
            width={36}
            tickFormatter={(v: number) => v.toFixed(1)}
          />
          <ReferenceLine
            y={0}
            stroke="hsl(var(--muted-foreground))"
            strokeDasharray="2 4"
            strokeOpacity={0.5}
          />
          <Bar dataKey="histogram" isAnimationActive={false} barSize={3}>
            {data.map((row, idx) => {
              const positive = (row.histogram ?? 0) >= 0;
              return (
                <Cell
                  key={idx}
                  fill={positive ? 'hsl(var(--up))' : 'hsl(var(--down))'}
                  fillOpacity={0.55}
                />
              );
            })}
          </Bar>
          <Line
            type="monotone"
            dataKey="signal"
            stroke="hsl(var(--muted-foreground))"
            strokeWidth={1.5}
            strokeDasharray="4 3"
            dot={false}
            isAnimationActive={false}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="macd_line"
            stroke="hsl(var(--foreground))"
            strokeWidth={1.75}
            dot={(props: { index: number; cx: number; cy: number }) => {
              if (props.index !== lastIndex) return <g />;
              return (
                <g>
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
                    {last.macd_line.toFixed(2)}
                  </text>
                </g>
              );
            }}
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
            formatter={(value: number, name: string) => {
              const label =
                name === 'macd_line' ? 'MACD' : name === 'signal' ? 'Signal' : 'Histogram';
              return [value !== null && value !== undefined ? value.toFixed(2) : '—', label];
            }}
            cursor={{ stroke: 'hsl(var(--muted-foreground))', strokeWidth: 1, strokeDasharray: '2 2' }}
          />
        </ComposedChart>
      </ResponsiveContainer>
      </div>
      <div className="mt-1 flex justify-center gap-4 text-[10px] font-mono text-muted-foreground">
        <span>
          <span className="inline-block w-3 border-t-2 border-foreground align-middle" /> MACD
        </span>
        <span>
          <span className="inline-block w-3 border-t-2 border-dashed border-muted-foreground align-middle" />{' '}
          Signal
        </span>
        <span>
          <span className="inline-block w-2 h-2 align-middle" style={{ background: 'hsl(var(--up) / 0.55)' }} />{' '}
          /
          <span className="inline-block w-2 h-2 align-middle ml-1" style={{ background: 'hsl(var(--down) / 0.55)' }} />{' '}
          Histogram
        </span>
      </div>
    </div>
  );
}
