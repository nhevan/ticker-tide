/**
 * Compact timeframe summary table rendered inside the dashboard verdict block.
 *
 * Mirrors the table emitted by Telegram /detail (build_timeframe_table in
 * src/notifier/detail_command.py): rows for Daily/Weekly/Monthly, columns for
 * Score / Trend / Mom / Dir. Direction arrow uses the same threshold logic
 * (▲ above threshold, ▼ below negative threshold, ▬ otherwise) — must match
 * the notifier's `detail_command.timeframe_direction_threshold` (default 15.0)
 * for /detail vs dashboard parity.
 */

import type { Snapshot } from '@/lib/api/types';

/**
 * Threshold for the directional indicator arrow.
 *
 * Mirrors `detail_command.timeframe_direction_threshold` in config/notifier.json.
 * Kept as a frontend constant because the value is a rendering choice; if it
 * drifts from the notifier config the only effect is /detail and the dashboard
 * showing different arrows for the same score.
 */
const DIRECTION_THRESHOLD = 15;

interface TimeframeSummaryTableProps {
  snapshot: Snapshot;
}

interface Row {
  label: string;
  score: number | null;
  trend: number | null;
  momentum: number | null;
}

/**
 * Render the three-row timeframe summary table.
 *
 * @param snapshot - Full snapshot with daily/weekly/monthly sections.
 */
export function TimeframeSummaryTable({ snapshot }: TimeframeSummaryTableProps) {
  const rows: Row[] = [
    {
      label: 'Daily',
      score: snapshot.daily.composite_score ?? null,
      trend: snapshot.daily.scores?.trend ?? null,
      momentum: snapshot.daily.scores?.momentum ?? null,
    },
    {
      label: 'Weekly',
      score: snapshot.weekly.composite_score ?? null,
      trend: snapshot.weekly.scores?.trend ?? null,
      momentum: snapshot.weekly.scores?.momentum ?? null,
    },
    {
      label: 'Monthly',
      score: snapshot.monthly.composite_score ?? null,
      trend: snapshot.monthly.scores?.trend ?? null,
      momentum: snapshot.monthly.scores?.momentum ?? null,
    },
  ];

  return (
    <table className="w-full font-mono text-xs">
      <thead>
        <tr className="text-muted-foreground">
          <th className="text-left font-normal"></th>
          <th className="text-right font-normal">Score</th>
          <th className="text-right font-normal">Trend</th>
          <th className="text-right font-normal">Mom</th>
          <th className="pl-3 text-center font-normal">Dir</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.label}>
            <td className="py-0.5 text-foreground">{row.label}</td>
            <td className="py-0.5 text-right tabular-nums">{formatScore(row.score)}</td>
            <td className="py-0.5 text-right tabular-nums">{formatScore(row.trend)}</td>
            <td className="py-0.5 text-right tabular-nums">{formatScore(row.momentum)}</td>
            <td className="py-0.5 pl-3 text-center">{directionSymbol(row.score)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function formatScore(value: number | null): string {
  return value === null || value === undefined ? 'N/A' : value.toFixed(1);
}

function directionSymbol(score: number | null): string {
  if (score === null || score === undefined) return '—';
  if (score > DIRECTION_THRESHOLD) return '▲';
  if (score < -DIRECTION_THRESHOLD) return '▼';
  return '▬';
}
