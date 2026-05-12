/**
 * CategoryShareBar — stacked horizontal bar showing how much each indicator in
 * a single scoring category contributes to that category's magnitude rollup.
 *
 * Each non-zero indicator gets a segment proportional to |score| / Σ|score|.
 * The active indicator (e.g. 'rsi_14' when viewing the RSI explainer, or
 * 'macd_line' for MACD) is highlighted in primary color. Zero-score indicators
 * are listed separately below the bar so the user can see them without
 * inflating the visual.
 *
 * Below the bar:
 *   - A headline sentence "{active} accounts for X% of the absolute {category} signal"
 *     (only when activeName matches an item with non-zero score).
 *   - One row per non-zero indicator with name, share%, and the |score|/denom math.
 *   - A divider, then the symbolic-first denominator expansion:
 *       Σ|score| (denominator)
 *       = |rsi_14| + |stoch_k| + ... (variable names)
 *       = |63.2| + |65.3| + ...      (substituted values)
 *       = 246.4
 *
 * Empty/zero cases:
 *   - items.length === 0 → "No {category} components in contributions payload." muted italic.
 *   - All scores zero (denom === 0) → "Share undefined (all {category} components zero)." muted italic.
 *   - activeName not in items → bar renders without a highlighted segment (no headline sentence).
 *
 * @param items - ContributionItems pre-filtered by caller to a single category.
 *                The component does NOT re-filter; passing mixed categories breaks the metaphor.
 * @param activeName - Name of the indicator to highlight (e.g. 'rsi_14', 'macd_line').
 * @param category - Display name of the category (e.g. 'momentum', 'trend'). Used in prose only.
 * @returns The stacked bar + legend + denominator breakdown, or an empty-state message.
 */
import type { ContributionItem } from '@/lib/api/types';

interface CategoryShareBarProps {
  /** ContributionItems pre-filtered to a single category. */
  items: ContributionItem[];
  /** Name of the active indicator to highlight in primary color. */
  activeName: string;
  /** Display name of the category (e.g. 'momentum', 'trend'). */
  category: string;
}

export function CategoryShareBar({ items, activeName, category }: CategoryShareBarProps) {
  if (items.length === 0) {
    return (
      <p className="text-xs text-muted-foreground italic">
        No {category} components in contributions payload.
      </p>
    );
  }

  // Defensive filter — backend (build_contributions_payload) already skips None
  // scores, so all incoming scores should be finite floats. Belt-and-suspenders
  // in case future backend changes ever produce NaN/Infinity.
  const finite = items.filter((i) => Number.isFinite(i.score));
  const denom = finite.reduce((acc, i) => acc + Math.abs(i.score), 0);

  if (denom === 0) {
    return (
      <p className="text-xs text-muted-foreground italic">
        Share undefined (all {category} components zero).
      </p>
    );
  }

  const nonZero = finite.filter((i) => Math.abs(i.score) > 0);
  const zero = finite.filter((i) => Math.abs(i.score) === 0);
  const activeItem = nonZero.find((i) => i.name === activeName);

  // SVG coordinate space — presentational only, not a configurable threshold.
  const W = 400;
  const H = 28;
  let cursor = 0;

  return (
    <div className="w-full">
      {activeItem && (
        <p className="mb-2">
          <span className="text-foreground">{activeItem.name}</span> accounts for{' '}
          <span className="font-medium text-primary">
            {((Math.abs(activeItem.score) / denom) * 100).toFixed(1)}%
          </span>{' '}
          of the absolute {category} signal.
        </p>
      )}

      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: H }}>
        {nonZero.map((item, idx) => {
          const share = Math.abs(item.score) / denom;
          const segW = share * W;
          const x = cursor;
          cursor += segW;
          const isActive = item.name === activeName;
          const fill = isActive ? 'hsl(var(--primary))' : 'hsl(var(--muted-foreground))';
          const opacity = isActive ? 0.85 : 0.35 + idx * 0.1;
          const showLabel = segW > 60;
          return (
            <g key={item.name}>
              <rect
                x={x}
                y={0}
                width={segW}
                height={H}
                fill={fill}
                fillOpacity={opacity}
                stroke="hsl(var(--card))"
                strokeWidth={1}
              />
              {showLabel && (
                <text
                  x={x + segW / 2}
                  y={H / 2 + 3}
                  fill={isActive ? 'hsl(var(--primary-foreground))' : 'hsl(var(--foreground))'}
                  fontSize={9}
                  textAnchor="middle"
                  fontFamily="JetBrains Mono, monospace"
                >
                  {item.name.replace('_14', '').replace('_20', '')} {(share * 100).toFixed(1)}%
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <div className="mt-2 text-[10px] text-muted-foreground space-y-0.5">
        {nonZero.map((i) => {
          const share = Math.abs(i.score) / denom;
          const isActive = i.name === activeName;
          return (
            <div key={i.name} className="flex items-baseline gap-2 font-mono">
              <span className={isActive ? 'text-primary font-semibold' : 'text-foreground'}>
                {i.name}
              </span>
              <span className="text-foreground">{(share * 100).toFixed(1)}%</span>
              <span className="text-muted-foreground">
                (= |{i.score.toFixed(1)}| ÷ {denom.toFixed(1)})
              </span>
            </div>
          );
        })}
        {zero.length > 0 && (
          <div className="mt-1 text-muted-foreground italic">
            Not contributing today (score 0): {zero.map((z) => z.name).join(', ')}.
          </div>
        )}
        <div className="mt-1 pt-1 border-t border-border/40 font-mono space-y-0.5">
          <div className="text-muted-foreground">Σ|score| (denominator)</div>
          <div className="text-foreground break-all">
            = {finite.map((i) => `|${i.name}|`).join(' + ')}
          </div>
          <div className="text-foreground break-all">
            = {finite.map((i) => `|${i.score.toFixed(1)}|`).join(' + ')}
          </div>
          <div>
            = <span className="text-foreground font-semibold">{denom.toFixed(1)}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
