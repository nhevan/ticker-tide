/**
 * Compact timeframe summary table rendered inside the dashboard verdict block.
 *
 * Three rows (Daily/Weekly/Monthly), four columns: Score / Trend / Mom / Dir.
 *
 * Score and Dir mirror the table emitted by Telegram /detail (build_timeframe_table
 * in src/notifier/detail_command.py). Direction arrow uses the same threshold logic
 * (▲ above threshold, ▼ below negative threshold, ▬ otherwise) — must match the
 * notifier's `detail_command.timeframe_direction_threshold` (default 15.0) for
 * /detail vs dashboard parity on those columns.
 *
 * Trend and Mom are rendered as visual SignedBar widgets (not numeric text) so
 * the [-100, +100] scale is immediately glanceable. This is a deliberate
 * divergence from /detail, which still emits numbers — only Score and Dir are
 * required to match across surfaces.
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
    <div className="rounded border border-dashed border-border p-3">
      <table className="w-full font-mono text-xs">
        <thead>
          <tr className="text-muted-foreground">
            <th className="text-left font-normal"></th>
            <th className="text-right font-normal">Score</th>
            <th className="px-2 text-left font-normal">Trend</th>
            <th className="px-2 text-left font-normal">Mom</th>
            <th className="pl-2 text-center font-normal">Dir</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.label}>
              <td className="py-1 text-foreground">{row.label}</td>
              <td className="py-1 text-right tabular-nums">{formatScore(row.score)}</td>
              <td className="px-2 py-1">
                <SignedBar value={row.trend} label={`${row.label} trend`} />
              </td>
              <td className="px-2 py-1">
                <SignedBar value={row.momentum} label={`${row.label} momentum`} />
              </td>
              <td className="py-1 pl-2 text-center">{directionSymbol(row.score)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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

/**
 * Render a horizontal bar on the [-100, +100] score domain, centred at 0.
 *
 * Positive values grow rightward from the centre tick in emerald; negative
 * values grow leftward in red. When `value` is null or non-finite the bar
 * renders as a muted track with no fill and `aria-label` reports N/A. Uses
 * `Number.isFinite` (not `isNaN`) because `isNaN(null) === false` silently
 * passes nulls through.
 *
 * @param value - Score in [-100, +100], or null/non-finite for "no data".
 * @param label - Accessibility label prefix (e.g. "Daily trend"). The numeric
 *                value is appended to form the final aria-label.
 */
function SignedBar({ value, label }: { value: number | null; label: string }) {
  const hasValue = value !== null && Number.isFinite(value);
  const clamped = hasValue ? Math.max(-100, Math.min(100, value as number)) : 0;
  // Divide by 2 because each bar half spans 50% of the track (centre tick at 50%).
  const widthPct = Math.abs(clamped) / 2;
  const positive = clamped >= 0;
  const ariaText = hasValue ? `${label}: ${(value as number).toFixed(1)}` : `${label}: N/A`;

  return (
    <div
      role="img"
      aria-label={ariaText}
      className="relative h-2 w-full rounded bg-muted"
    >
      <div className="absolute left-1/2 top-0 h-full w-px bg-muted-foreground/40" />
      {hasValue && (
        <div
          className={`absolute top-0 h-full rounded ${positive ? 'bg-emerald-500' : 'bg-red-500'}`}
          style={{
            left: positive ? '50%' : `${50 - widthPct}%`,
            width: `${widthPct}%`,
          }}
        />
      )}
    </div>
  );
}
