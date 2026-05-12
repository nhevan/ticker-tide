/**
 * RsiPercentileStrip — visual position of today's indicator value on the
 * per-ticker historical distribution. Generic over any bounded 0–100
 * indicator; the indicator label is parameterised via the `label` prop.
 *
 * Renders a gradient bar (green→muted→red), percentile tick marks
 * (p5/p20/p50/p80/p95), today's value as a primary-color dot with floating
 * numeric label, and a one-sentence caption that uses the server-supplied
 * zone label description.
 *
 * Theme-aware via CSS custom properties. SVG-only — no chart library.
 *
 * @param profile - The ticker's percentile distribution from the snapshot.
 * @param today - Today's raw indicator reading (0–100).
 * @param zoneLabel - Server-computed zone label (e.g. "above_mid").
 * @param zoneDescription - Human-friendly prose fragment for the zone (passed from caller's ZONE_LABEL_DESCRIPTIONS map).
 * @param label - Display name for the indicator used in the prose caption (defaults to 'RSI').
 * @returns The rendered strip, or null if any required numeric input is non-finite.
 */
import { useId } from 'react';

interface RsiPercentileStripProps {
  profile: { p5: number; p20: number; p50: number; p80: number; p95: number };
  today: number;
  zoneLabel: string | null;
  zoneDescription: string;
  label?: string;
}

export function RsiPercentileStrip({
  profile,
  today,
  zoneLabel,
  zoneDescription,
  label = 'RSI',
}: RsiPercentileStripProps) {
  // REQUIRED: defensive null/NaN guard. DB columns are nullable; TS type lies.
  // Use Number.isFinite — isNaN(null) returns false in JS and would silently pass.
  if (
    !Number.isFinite(today) ||
    !Number.isFinite(profile.p5) ||
    !Number.isFinite(profile.p20) ||
    !Number.isFinite(profile.p50) ||
    !Number.isFinite(profile.p80) ||
    !Number.isFinite(profile.p95)
  ) {
    return null;
  }

  // Clamp today's value to RSI's mathematical range to avoid clipping the dot
  // out of the SVG viewBox if upstream calculations ever glitch.
  const clampedToday = Math.max(0, Math.min(100, today));

  // REQUIRED: unique gradient ID per instance so multiple strips on the page
  // do not collide in the global SVG <defs> namespace.
  const gradientId = useId();

  const pad = 8;
  const w = 400 - 2 * pad;
  const xAt = (v: number) => pad + (w * v) / 100;

  return (
    <div className="w-full">
      <svg viewBox="0 0 400 44" className="w-full" style={{ height: 44 }}>
        <defs>
          <linearGradient id={gradientId} x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="hsl(var(--up))" stopOpacity={0.5} />
            <stop offset={`${profile.p20}%`} stopColor="hsl(var(--up))" stopOpacity={0.15} />
            <stop offset={`${profile.p50}%`} stopColor="hsl(var(--muted))" stopOpacity={0.45} />
            <stop offset={`${profile.p80}%`} stopColor="hsl(var(--down))" stopOpacity={0.15} />
            <stop offset="100%" stopColor="hsl(var(--down))" stopOpacity={0.5} />
          </linearGradient>
        </defs>
        <rect x={pad} y={20} width={w} height={10} fill={`url(#${gradientId})`} />
        {[profile.p5, profile.p20, profile.p50, profile.p80, profile.p95].map((v) => (
          <line
            key={v}
            x1={xAt(v)}
            x2={xAt(v)}
            y1={18}
            y2={32}
            stroke="hsl(var(--foreground))"
            strokeOpacity={0.45}
            strokeWidth={1}
          />
        ))}
        <circle
          cx={xAt(clampedToday)}
          cy={25}
          r={5}
          fill="hsl(var(--primary))"
          stroke="hsl(var(--card))"
          strokeWidth={1.5}
        />
        <text
          x={xAt(clampedToday)}
          y={14}
          fill="hsl(var(--primary))"
          fontSize={10}
          fontWeight={600}
          textAnchor="middle"
          fontFamily="JetBrains Mono, monospace"
        >
          {today.toFixed(1)}
        </text>
        {[
          { label: 'p5', v: profile.p5 },
          { label: 'p20', v: profile.p20 },
          { label: 'p50', v: profile.p50 },
          { label: 'p80', v: profile.p80 },
          { label: 'p95', v: profile.p95 },
        ].map((t) => (
          <text
            key={t.label}
            x={xAt(t.v)}
            y={42}
            fill="hsl(var(--muted-foreground))"
            fontSize={8}
            textAnchor="middle"
            fontFamily="JetBrains Mono, monospace"
          >
            {t.label}={t.v.toFixed(0)}
          </text>
        ))}
      </svg>
      {zoneLabel && zoneDescription && (
        <div className="mt-1 text-[10px] text-muted-foreground">
          Today's {label} of <span className="font-mono text-foreground">{today.toFixed(1)}</span> sits
          in the <span className="text-foreground">{zoneDescription.toLowerCase()}</span> region.
        </div>
      )}
    </div>
  );
}
