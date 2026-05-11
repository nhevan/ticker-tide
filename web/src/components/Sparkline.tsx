/**
 * Sparkline chart using Recharts for close-price history.
 */

import React from 'react';
import { LineChart, Line, ResponsiveContainer, Tooltip } from 'recharts';
import type { SparklinePoint } from '@/lib/api/types';

interface SparklineProps {
  /** Array of date/close data points in chronological order. */
  data: SparklinePoint[] | undefined;
  /** Optional pixel height for the chart. Defaults to 60. */
  height?: number;
}

/**
 * Render a compact sparkline of close prices.
 *
 * Shows "No data" when the data array is empty or undefined.
 * Uses Recharts LineChart with a minimal tooltip.
 *
 * @param data - Sparkline data points (date + close).
 * @param height - Chart height in pixels.
 */
export function Sparkline({ data, height = 60 }: SparklineProps) {
  if (!data || data.length === 0) {
    return (
      <div
        className="flex items-center justify-center text-xs text-muted-foreground"
        style={{ height }}
      >
        No data
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
        <Line
          type="monotone"
          dataKey="close"
          stroke="#3b82f6"
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
        <Tooltip
          contentStyle={{ fontSize: '11px', padding: '4px 8px' }}
          formatter={(value: number) => [`$${value.toFixed(2)}`, 'Close']}
          labelFormatter={(label: string) => label}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
