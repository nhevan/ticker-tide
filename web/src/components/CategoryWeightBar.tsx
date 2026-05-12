/**
 * CategoryWeightBar — stacked horizontal bar showing how the daily composite's
 * 9 categories are weighted in the current regime, with one category highlighted
 * (typically the home category of the indicator being explained).
 *
 * Below the bar:
 *   - A headline sentence "{active} carries X% of the composite in {regime} regime"
 *     (only when the active category has a non-zero weight).
 *   - One row per non-zero category with weight as percent and raw decimal.
 *   - A "Not weighted in {regime} regime" line for categories whose weight is 0.
 *   - A symbolic-first effective-multiplier math block:
 *       Effective multiplier for {active} today
 *       = {active}_weight × expansion_factor
 *       = 0.20 × 1.50
 *       = 0.30
 *     This block is suppressed entirely when the active category is absent
 *     or has weight 0.
 *
 * Forward-compatibility: categories not in the canonical 9-item order are
 * silently filtered out. To support a 10th category, add it to the
 * CANONICAL_ORDER constant below.
 *
 * @param weights - Map of category name → weight in the current regime. Comes
 *                  from `/api/scoring-rules.regime_weights[regime]`.
 * @param regime - Current regime label ('trending' | 'ranging' | 'volatile').
 * @param expansion - Score expansion factor from /api/scoring-rules. Reflects
 *                    *current config* — may differ from the persisted
 *                    expansion_factor in step 7 if config has drifted since
 *                    the last scoring run.
 * @param activeName - Category to highlight in primary color.
 * @returns The bar + legend + math, or a muted-italic fallback for empty input.
 */
import type { ReactNode } from 'react';

const CANONICAL_ORDER = [
  'trend',
  'momentum',
  'volume',
  'volatility',
  'candlestick',
  'structural',
  'sentiment',
  'fundamental',
  'macro',
] as const;

interface CategoryWeightBarProps {
  weights: Record<string, number>;
  regime: string;
  expansion: number;
  activeName: string;
}

export function CategoryWeightBar({
  weights,
  regime,
  expansion,
  activeName,
}: CategoryWeightBarProps): ReactNode {
  // Order canonically; silently drop unknown keys for forward compatibility.
  const ordered = CANONICAL_ORDER
    .filter((name) => Number.isFinite(weights[name]))
    .map((name) => ({ name, weight: weights[name] }));

  const total = ordered.reduce((acc, w) => acc + w.weight, 0);
  if (ordered.length === 0 || total === 0) {
    return (
      <p className="text-xs text-muted-foreground italic">
        No category weights for {regime} regime.
      </p>
    );
  }

  const nonZero = ordered.filter((w) => w.weight > 0);
  const zero = ordered.filter((w) => w.weight === 0);
  const active = ordered.find((w) => w.name === activeName);
  const showMath = active !== undefined && active.weight > 0;

  // SVG coordinate space — presentational, not configurable.
  const W = 400;
  const H = 28;
  let cursor = 0;

  return (
    <div className="w-full">
      {showMath && (
        <p className="mb-2">
          <span className="text-foreground">{activeName}</span> carries{' '}
          <span className="font-medium text-primary">
            {(active!.weight * 100).toFixed(0)}%
          </span>{' '}
          of the composite in <span className="text-foreground">{regime}</span> regime.
        </p>
      )}

      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: H }}>
        {nonZero.map((w, idx) => {
          const segW = (w.weight / total) * W;
          const x = cursor;
          cursor += segW;
          const isActive = w.name === activeName;
          const fill = isActive ? 'hsl(var(--primary))' : 'hsl(var(--muted-foreground))';
          const opacity = isActive ? 0.85 : Math.min(0.85, 0.35 + idx * 0.08);
          const showLabel = segW > 40;
          return (
            <g key={w.name}>
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
                  {w.name} {(w.weight * 100).toFixed(0)}%
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <div className="mt-2 text-[10px] text-muted-foreground space-y-0.5">
        {nonZero.map((w) => {
          const isActive = w.name === activeName;
          return (
            <div key={w.name} className="flex items-baseline gap-2 font-mono">
              <span className={isActive ? 'text-primary font-semibold' : 'text-foreground'}>
                {w.name}
              </span>
              <span className="text-foreground">{(w.weight * 100).toFixed(0)}%</span>
              <span className="text-muted-foreground">(weight {w.weight.toFixed(2)})</span>
            </div>
          );
        })}
        {zero.length > 0 && (
          <div className="mt-1 text-muted-foreground italic">
            Not weighted in <span className="text-foreground">{regime}</span> regime:{' '}
            {zero.map((z) => z.name).join(', ')}.
          </div>
        )}
        {showMath && (
          <div className="mt-1 pt-1 border-t border-border/40 font-mono space-y-0.5">
            <div className="text-muted-foreground">Effective multiplier for {activeName} today</div>
            <div className="text-foreground break-all">
              = {activeName}_weight × expansion_factor
            </div>
            <div className="text-foreground break-all">
              = {active!.weight.toFixed(2)} × {expansion.toFixed(2)}
            </div>
            <div>
              ={' '}
              <span className="text-foreground font-semibold">
                {(active!.weight * expansion).toFixed(2)}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
