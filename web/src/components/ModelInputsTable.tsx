/**
 * ModelInputsTable — daily "Model Inputs" table (production component).
 *
 * Renders the calibrator decomposition from snapshot.daily.calibrator_payload
 * as a split two-column layout: positive drivers (left) vs negative drivers (right).
 *
 * Empty state: when the payload is null/undefined or contributions are missing,
 * renders a muted one-liner rather than returning null, so the section stays
 * visually present.
 *
 * Filtering/sorting contract (applied in order):
 *   1. Drop contributions where !Number.isFinite(c.contribution).
 *   2. Positives = c.contribution > 0, sorted desc by contribution.
 *   3. Negatives = c.contribution < 0, sorted asc by contribution (most negative first).
 *   4. Exactly-zero contributions are dropped from both columns.
 *   5. Column header counts (N) reflect actual rendered rows.
 *
 * Per-cell Number.isFinite guards on raw, z, weight, contribution; non-finite → "—".
 */

import type { CalibratorPayload, CalibratorContribution } from '@/lib/api/types';
import { SignalBadge } from '@/components/SignalBadge';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

// ── Formatters ────────────────────────────────────────────────────────────────

/** Format a number with sign prefix, 2 decimal places. Non-finite → "—". */
function fmt2(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2);
}

/** Format raw (no sign prefix), 2 decimal places. Non-finite → "—". */
function fmtRaw(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(2);
}

/** Format weight with sign prefix, 3 decimal places. Non-finite → "—". */
function fmtWeight(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return n >= 0 ? `+${n.toFixed(3)}` : n.toFixed(3);
}

// ── Sub-components ────────────────────────────────────────────────────────────

interface ColumnProps {
  rows: CalibratorContribution[];
  up: boolean;
}

/**
 * Render one side (positive or negative) of the driver table.
 * When rows is empty, renders a muted "none" placeholder row.
 */
function DriverColumn({ rows, up }: ColumnProps) {
  return (
    <table className="w-full text-xs tabular-nums">
      <thead>
        <tr className="text-[10px] uppercase tracking-wider text-muted-foreground border-b border-border/60">
          <th className="text-left font-medium py-1.5 pr-3">feature</th>
          <th className="text-right font-medium py-1.5 px-2">raw</th>
          <th className="text-right font-medium py-1.5 px-2">z</th>
          <th className="text-right font-medium py-1.5 px-2">w</th>
          <th className="text-right font-medium py-1.5 pl-2">contrib</th>
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr>
            <td colSpan={5} className="py-2 text-center text-[11px] text-muted-foreground/60 italic">
              none
            </td>
          </tr>
        ) : (
          rows.map((r) => (
            <tr key={r.name} className="border-b border-border/30 last:border-b-0">
              <td className="py-1 pr-3 font-mono text-[11px] text-foreground/90">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="block max-w-[10rem] overflow-hidden text-ellipsis whitespace-nowrap cursor-help">
                      {r.name}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent
                    side="top"
                    className="font-mono text-[11px] leading-snug border border-border/60 shadow-md text-popover-foreground"
                    style={{ backgroundColor: 'hsl(var(--card))', opacity: 1 }}
                  >
                    <div className="space-y-1 tabular-nums max-w-xs">
                      <div className="text-muted-foreground text-[10px] uppercase tracking-wider">
                        {r.name}
                      </div>
                      <div>
                        μ ={' '}
                        <span className="text-foreground">
                          {Number.isFinite(r.mean) ? r.mean.toFixed(4) : '—'}
                        </span>
                      </div>
                      <div>
                        σ ={' '}
                        <span className="text-foreground">
                          {Number.isFinite(r.std) ? r.std.toFixed(4) : '—'}
                        </span>
                      </div>
                      {Number.isFinite(r.mean) && Number.isFinite(r.std) ? (
                        <div className="text-[10px] text-muted-foreground font-sans whitespace-normal pt-1 border-t border-border/40">
                          <span className="font-mono text-foreground/80">{r.name}</span>{' '}
                          usually wobbles ±{r.std.toFixed(2)} either side of{' '}
                          {r.mean.toFixed(2)} across the training data.
                        </div>
                      ) : null}
                    </div>
                  </TooltipContent>
                </Tooltip>
              </td>
              <td className="py-1 px-2 text-right text-foreground/70">
                {Number.isFinite(r.raw) ? fmtRaw(r.raw) : '—'}
              </td>
              <td className="py-1 px-2 text-right text-foreground/70">
                {Number.isFinite(r.z) &&
                Number.isFinite(r.raw) &&
                Number.isFinite(r.mean) &&
                Number.isFinite(r.std) ? (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="cursor-help underline decoration-dotted decoration-muted-foreground/40 underline-offset-2">
                        {fmt2(r.z)}
                      </span>
                    </TooltipTrigger>
                    <TooltipContent
                      side="top"
                      className="font-mono text-[11px] leading-snug border border-border/60 shadow-md text-popover-foreground"
                      style={{ backgroundColor: 'hsl(var(--card))', opacity: 1 }}
                    >
                      <div className="space-y-0.5 tabular-nums">
                        <div>
                          z = (raw − μ) ÷ σ
                        </div>
                        <div className="text-muted-foreground">
                          {'  '}= ({r.raw.toFixed(2)} − {r.mean.toFixed(2)}) ÷{' '}
                          {r.std.toFixed(2)}
                        </div>
                        <div className="text-foreground">
                          {'  '}= {fmt2(r.z)}
                        </div>
                      </div>
                    </TooltipContent>
                  </Tooltip>
                ) : Number.isFinite(r.z) ? (
                  fmt2(r.z)
                ) : (
                  '—'
                )}
              </td>
              <td className="py-1 px-2 text-right text-foreground/70">
                {Number.isFinite(r.weight) ? fmtWeight(r.weight) : '—'}
              </td>
              <td
                className={`py-1 pl-2 text-right font-semibold ${
                  up ? 'text-emerald-500' : 'text-rose-500'
                }`}
              >
                {Number.isFinite(r.contribution) ? (
                  <>
                    {up ? '▲' : '▼'} {Math.abs(r.contribution).toFixed(2)}
                  </>
                ) : (
                  '—'
                )}
              </td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────

interface ModelInputsTableProps {
  payload: CalibratorPayload | null | undefined;
  /** Classified signal (BULLISH / NEUTRAL / BEARISH) shown as a badge in the header. */
  signal?: string | null;
}

/**
 * Daily "Model Inputs" table showing the calibrator decomposition.
 *
 * Renders a split two-column layout (positive drivers left, negative right).
 * Falls back to a muted one-liner when calibrator data is absent so the section
 * remains visually present.
 */
export function ModelInputsTable({ payload, signal }: ModelInputsTableProps) {
  const hasData =
    payload != null &&
    Array.isArray(payload.contributions) &&
    payload.contributions.length > 0;

  if (!hasData) {
    return (
      <section className="rounded-md border border-border/60 bg-card p-4 mb-4">
        <div className="mb-2 flex items-center justify-between">
          <h3 className="text-sm font-semibold tracking-wide text-foreground">
            Model inputs
          </h3>
          <SignalBadge signal={signal} />
        </div>
        <p className="text-xs text-muted-foreground">
          Calibrator data not available for this date.
        </p>
      </section>
    );
  }

  const { intercept, prediction, contributions, in_sample_r2 } = payload!;

  // Step 1: drop non-finite contributions.
  const finite = contributions.filter((c) => Number.isFinite(c.contribution));

  // Steps 2–4: split positive vs negative; drop exactly-zero.
  const positives = finite
    .filter((c) => c.contribution > 0)
    .sort((a, b) => b.contribution - a.contribution);

  const negatives = finite
    .filter((c) => c.contribution < 0)
    .sort((a, b) => a.contribution - b.contribution);

  // Header reconciliation sums (computed from filtered arrays).
  const sumPos = positives.reduce((acc, c) => acc + c.contribution, 0);
  const sumNeg = negatives.reduce((acc, c) => acc + c.contribution, 0);

  // intercept and prediction come directly from the payload. fmt2 already
  // renders non-finite as '—', so no outer guards needed.
  const interceptDisplay = fmt2(intercept);
  const predictionDisplay = fmt2(prediction);
  const sumPosDisplay = fmt2(sumPos);
  const sumNegDisplay = fmt2(sumNeg);

  const r2Display = Number.isFinite(in_sample_r2) ? in_sample_r2.toFixed(3) : '—';

  return (
    <TooltipProvider delayDuration={150}>
      <section className="rounded-md border border-border/60 bg-card p-4 mb-4">
        {/* Header row */}
        <div className="flex items-baseline justify-between gap-4 mb-3">
          <div className="flex items-baseline gap-3">
            <h3 className="text-sm font-semibold tracking-wide text-foreground">
              Model inputs
            </h3>
            <span className="text-xs text-muted-foreground tabular-nums">
              R² <span className="text-foreground">{r2Display}</span>
            </span>
          </div>
          <div className="flex items-center gap-3 text-xs text-muted-foreground tabular-nums">
            <span>
              intercept {interceptDisplay}{' '}
              <span className="text-emerald-500">Σ⁺ {sumPosDisplay}</span>{' '}
              <span className="text-rose-500">Σ⁻ {sumNegDisplay}</span>{' '}
              = prediction{' '}
              <span className="font-semibold text-foreground">{predictionDisplay}</span>
            </span>
            <SignalBadge signal={signal} />
          </div>
        </div>

        {/* Two-column split */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-3">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-emerald-500/80 mb-1">
              drivers up ({positives.length})
            </div>
            <DriverColumn rows={positives} up={true} />
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-wider text-rose-500/80 mb-1">
              drivers down ({negatives.length})
            </div>
            <DriverColumn rows={negatives} up={false} />
          </div>
        </div>
      </section>
    </TooltipProvider>
  );
}
