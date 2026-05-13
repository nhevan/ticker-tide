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
              <td className="py-1 pr-3 font-mono text-[11px] text-foreground/90 truncate">{r.name}</td>
              <td className="py-1 px-2 text-right text-foreground/70">
                {Number.isFinite(r.raw) ? fmtRaw(r.raw) : '—'}
              </td>
              <td className="py-1 px-2 text-right text-foreground/70">
                {Number.isFinite(r.z) ? fmt2(r.z) : '—'}
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
}

/**
 * Daily "Model Inputs" table showing the calibrator decomposition.
 *
 * Renders a split two-column layout (positive drivers left, negative right).
 * Falls back to a muted one-liner when calibrator data is absent so the section
 * remains visually present.
 */
export function ModelInputsTable({ payload }: ModelInputsTableProps) {
  const hasData =
    payload != null &&
    Array.isArray(payload.contributions) &&
    payload.contributions.length > 0;

  if (!hasData) {
    return (
      <section className="rounded-md border border-border/60 bg-card p-4 mb-4">
        <h3 className="text-sm font-semibold tracking-wide text-foreground mb-2">
          Model inputs
        </h3>
        <p className="text-xs text-muted-foreground">
          Calibrator data not available for this date.
        </p>
      </section>
    );
  }

  const { intercept, prediction, contributions } = payload!;

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

  // intercept and prediction come directly from the payload.
  const interceptDisplay = Number.isFinite(intercept) ? fmt2(intercept) : '—';
  const predictionDisplay = Number.isFinite(prediction) ? fmt2(prediction) : '—';
  const sumPosDisplay = Number.isFinite(sumPos) ? fmt2(sumPos) : '—';
  const sumNegDisplay = Number.isFinite(sumNeg) ? fmt2(sumNeg) : '—';

  return (
    <section className="rounded-md border border-border/60 bg-card p-4 mb-4">
      {/* Header row */}
      <div className="flex items-baseline justify-between mb-3">
        <h3 className="text-sm font-semibold tracking-wide text-foreground">
          Model inputs
        </h3>
        <div className="text-xs text-muted-foreground tabular-nums">
          intercept {interceptDisplay}{' '}
          +{' '}
          <span className="text-emerald-500">Σ⁺ {sumPosDisplay}</span>{' '}
          +{' '}
          <span className="text-rose-500">Σ⁻ {sumNegDisplay}</span>{' '}
          = prediction{' '}
          <span className="font-semibold text-foreground">{predictionDisplay}</span>
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
  );
}
