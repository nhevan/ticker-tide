/**
 * Model page — ridge regression diagnostics.
 *
 * Hosts the ridge shrinkage-path panel, which visualises how each feature's
 * coefficient changes across the λ grid used during calibration. Data is
 * served by GET /api/shrinkage-path.
 */

import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from 'recharts';
import { Header } from '@/components/Header';
import { ErrorBanner } from '@/components/ErrorBanner';
import { Skeleton } from '@/components/ui/skeleton';
import { useShrinkagePath } from '@/lib/hooks/useShrinkagePath';
import type { ShrinkagePathFeature } from '@/lib/api/types';

// ---------------------------------------------------------------------------
// Visual constants (category colours and labels stay client-side)
// ---------------------------------------------------------------------------

type FeatureCategory =
  | 'trend'
  | 'momentum'
  | 'volume'
  | 'volatility'
  | 'fundamental'
  | 'macro'
  | 'temporal';

const CATEGORY_COLORS: Record<string, string> = {
  trend: '#2563eb',
  momentum: '#16a34a',
  volume: '#9333ea',
  volatility: '#dc2626',
  fundamental: '#ea580c',
  macro: '#0d9488',
  temporal: '#71717a',
};

function featureColor(feat: ShrinkagePathFeature, idxInCategory: number): string {
  const alphas = ['', 'cc', '99', 'e6'];
  const base = CATEGORY_COLORS[feat.category] ?? '#888888';
  return base + (alphas[idxInCategory % alphas.length] ?? '');
}

const CATEGORY_LABELS: Record<string, string> = {
  trend: 'Trend',
  momentum: 'Momentum',
  volume: 'Volume',
  volatility: 'Volatility',
  fundamental: 'Fundamental',
  macro: 'Macro',
  temporal: 'Temporal',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatLambda(value: number): string {
  if (value >= 1) return value.toFixed(value >= 10 ? 0 : 1);
  return value.toExponential(0);
}

// ---------------------------------------------------------------------------
// Shrinkage panel
// ---------------------------------------------------------------------------

function ShrinkagePathPanel() {
  const { data, isLoading, isError, error } = useShrinkagePath();

  if (isLoading) {
    return (
      <section className="space-y-3 rounded-lg border bg-card p-4">
        <Skeleton className="h-5 w-48" />
        <Skeleton className="h-3 w-full" />
        <Skeleton className="h-3 w-2/3" />
        <Skeleton className="h-[440px] w-full" />
      </section>
    );
  }

  if (isError) {
    const message =
      error instanceof Error ? error.message : 'Failed to load shrinkage path data.';
    return (
      <section className="space-y-3 rounded-lg border bg-card p-4">
        <h2 className="text-base font-semibold">Ridge shrinkage path</h2>
        <ErrorBanner message={message} />
      </section>
    );
  }

  if (!data) {
    return null;
  }

  if (data.cold_start) {
    const dateLabel = data.scoring_date ?? 'this date';
    return (
      <section className="space-y-3 rounded-lg border bg-card p-4">
        <header className="space-y-1">
          <h2 className="text-base font-semibold">Ridge shrinkage path</h2>
        </header>
        <div className="flex items-center justify-center py-12 text-sm text-muted-foreground">
          {`Calibrator cold-start — ${data.training_samples} training samples, below the minimum threshold. No shrinkage path available for ${dateLabel}.`}
        </div>
      </section>
    );
  }

  // Happy path: data.lambdas and data.features are present
  const lambdas = data.lambdas!;
  const features = data.features!;
  const prodLambda = data.production_lambda;

  // Compute per-category color index and attach color to each feature
  const counters: Record<string, number> = {};
  const featuresWithColor = features.map((feat) => {
    counters[feat.category] = (counters[feat.category] ?? 0) + 1;
    return { ...feat, color: featureColor(feat, counters[feat.category] - 1) };
  });

  // Build recharts-compatible row objects: {lambda, [name]: coef, ...}
  const pathData = lambdas.map((lam, i) => {
    const row: Record<string, number> = { lambda: lam };
    for (const feat of features) {
      row[feat.name] = feat.coefs[i];
    }
    return row;
  });

  // Rank features by |coef| at the production lambda index. The backend
  // guarantees prodLambda is forced onto the grid (see build_shrinkage_lambdas)
  // so indexOf must hit exactly. If it doesn't, fail visibly rather than
  // silently rank by coefs[-1] (which would make every coef look ~0).
  const prodIdx = lambdas.indexOf(prodLambda);
  if (prodIdx < 0) {
    return (
      <section className="space-y-3 rounded-lg border bg-card p-4">
        <h2 className="text-base font-semibold">Ridge shrinkage path</h2>
        <ErrorBanner
          message={`Production λ (${prodLambda}) is not on the returned grid — cannot rank features. This is a backend bug; check build_shrinkage_lambdas.`}
        />
      </section>
    );
  }
  const ranked = [...featuresWithColor]
    .map((feat) => ({ ...feat, prodCoef: feat.coefs[prodIdx] }))
    .sort((a, b) => Math.abs(b.prodCoef) - Math.abs(a.prodCoef));

  // Distinct categories present in the API response (for the legend)
  const distinctCategories = Array.from(new Set(features.map((f) => f.category)));

  return (
    <section className="space-y-3 rounded-lg border bg-card p-4">
      <header className="space-y-1">
        <h2 className="text-base font-semibold">Ridge shrinkage path</h2>
        <p className="text-xs text-muted-foreground">
          How each feature&apos;s coefficient shrinks toward zero as λ grows.
          Lines coloured by indicator category. Vertical marker is production
          λ = {prodLambda}. Sidebar ranks features by |coefficient| at production λ.
        </p>
      </header>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_260px]">
        <div className="h-[440px] w-full">
          <ResponsiveContainer>
            <LineChart data={pathData} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis
                dataKey="lambda"
                scale="log"
                domain={['auto', 'auto']}
                type="number"
                tickFormatter={formatLambda}
                label={{ value: 'λ (log scale)', position: 'insideBottom', offset: -4, fontSize: 11 }}
                tick={{ fontSize: 11 }}
              />
              <YAxis
                tick={{ fontSize: 11 }}
                label={{ value: 'coefficient', angle: -90, position: 'insideLeft', fontSize: 11 }}
              />
              <ReferenceLine
                x={prodLambda}
                stroke="#525252"
                strokeDasharray="4 3"
                label={{ value: `prod λ=${prodLambda}`, fontSize: 10 }}
              />
              <ReferenceLine y={0} stroke="#71717a" />
              <Tooltip
                formatter={(value: number) => value.toFixed(3)}
                labelFormatter={(lambda: number) => `λ = ${lambda.toExponential(2)}`}
                contentStyle={{ fontSize: 11 }}
              />
              {featuresWithColor.map((feat) => (
                <Line
                  key={feat.name}
                  type="monotone"
                  dataKey={feat.name}
                  name={feat.label}
                  stroke={feat.color}
                  dot={false}
                  strokeWidth={1.5}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="space-y-1 overflow-y-auto rounded-md border bg-background p-2 text-xs">
          <div className="mb-1 flex items-center justify-between text-[10px] uppercase tracking-wide text-muted-foreground">
            <span>Feature @ λ={prodLambda}</span>
            <span>β</span>
          </div>
          {ranked.map((feat) => (
            <div
              key={feat.name}
              className="flex items-center justify-between gap-2 border-b border-border/40 py-0.5 last:border-0"
            >
              <span className="flex min-w-0 items-center gap-1.5">
                <span
                  className="h-2 w-2 shrink-0 rounded-sm"
                  style={{ background: feat.color }}
                  title={CATEGORY_LABELS[feat.category] ?? feat.category}
                />
                <span className="truncate font-mono">{feat.label}</span>
              </span>
              <span className="shrink-0 tabular-nums">
                <span className={feat.prodCoef >= 0 ? 'text-emerald-600' : 'text-red-600'}>
                  {feat.prodCoef >= 0 ? '+' : ''}
                  {feat.prodCoef.toFixed(2)}
                </span>
              </span>
            </div>
          ))}
        </div>
      </div>

      <div className="flex flex-wrap gap-x-3 gap-y-1 pt-1 text-[10px] text-muted-foreground">
        {distinctCategories.map((cat) => (
          <span key={cat} className="inline-flex items-center gap-1">
            <span
              className="h-2 w-2 rounded-sm"
              style={{ background: CATEGORY_COLORS[cat] ?? '#888888' }}
            />
            {CATEGORY_LABELS[cat] ?? cat}
          </span>
        ))}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

/**
 * Render the Model page. Hosts the ridge shrinkage-path panel.
 */
export function ModelPage() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <Header />
      <main className="space-y-6 px-4 py-6">
        <header className="space-y-1">
          <h1 className="text-lg font-semibold">Model diagnostics</h1>
          <p className="text-sm text-muted-foreground">
            Ridge regression — coefficient shrinkage path across λ.
          </p>
        </header>
        <ShrinkagePathPanel />
      </main>
    </div>
  );
}
