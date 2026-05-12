/**
 * RSI trend chart — last N working days of RSI(14) for the loaded ticker.
 * Recharts LineChart with OB (70) / OS (30) reference lines, zone background
 * tints, today marker badge, axes, and crosshair tooltip. Theme-aware via CSS
 * vars. Expects data ordered ascending by date.
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

interface RsiTrendChartProps {
  data: { date: string; value: number }[];
}

export function RsiTrendChart({ data }: RsiTrendChartProps) {
  if (!data.length) return null;

  const lastIndex = data.length - 1;
  const last = data[lastIndex];

  return (
    <div className="h-40 w-full">
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 6, right: 32, bottom: 16, left: 4 }}>
          <XAxis
            dataKey="date"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            interval={Math.max(1, Math.floor(data.length / 4))}
            tickFormatter={(v: string) => v.slice(5)}
            stroke="hsl(var(--border))"
          />
          <YAxis
            domain={[0, 100]}
            ticks={[30, 50, 70]}
            orientation="right"
            tick={{ fontSize: 9, fill: 'hsl(var(--muted-foreground))', fontFamily: 'JetBrains Mono, monospace' }}
            stroke="hsl(var(--border))"
            width={28}
          />
          <ReferenceArea y1={70} y2={100} fill="hsl(var(--down))" fillOpacity={0.07} />
          <ReferenceArea y1={0} y2={30} fill="hsl(var(--up))" fillOpacity={0.07} />
          <ReferenceLine
            y={70}
            stroke="hsl(var(--down))"
            strokeDasharray="3 3"
            strokeOpacity={0.6}
            label={{ value: 'OB', position: 'right', fill: 'hsl(var(--down))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          <ReferenceLine y={50} stroke="hsl(var(--muted-foreground))" strokeDasharray="2 4" strokeOpacity={0.3} />
          <ReferenceLine
            y={30}
            stroke="hsl(var(--up))"
            strokeDasharray="3 3"
            strokeOpacity={0.6}
            label={{ value: 'OS', position: 'right', fill: 'hsl(var(--up))', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
          />
          <Line
            type="monotone"
            dataKey="value"
            stroke="hsl(var(--foreground))"
            strokeWidth={1.75}
            dot={(props: any) => {
              if (props.index !== lastIndex) return <g />;
              return (
                <g>
                  <circle cx={props.cx} cy={props.cy} r={4} fill="hsl(var(--primary))" stroke="hsl(var(--card))" strokeWidth={1.5} />
                  <text
                    x={props.cx + 8}
                    y={props.cy + 3}
                    fill="hsl(var(--primary))"
                    fontSize={9}
                    fontFamily="JetBrains Mono, monospace"
                  >
                    {last.value.toFixed(1)}
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
            formatter={(value: number) => [value.toFixed(2), 'RSI']}
            cursor={{ stroke: 'hsl(var(--muted-foreground))', strokeWidth: 1, strokeDasharray: '2 2' }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
