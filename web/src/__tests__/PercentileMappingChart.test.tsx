/**
 * Regression tests for PercentileMappingChart domain prop generalisation.
 *
 * Key invariants:
 *   1. With default domain [0, 100], the mapping sentinel corner points are
 *      x=0 (first point) and x=100 (last point) — pre-existing RSI/Stoch %K
 *      behaviour must be unchanged (zero callsite diff).
 *   2. With domain=[-200, 200] (CCI), the sentinel corner points are
 *      x=-200 (first) and x=200 (last).
 *
 * The corner-point function is not exported, so we verify indirectly by
 * inspecting the rendered SVG polyline 'points' attribute.
 */

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { PercentileMappingChart } from '@/components/PercentileMappingChart';

const RSI_PROFILE = { p5: 20, p20: 35, p50: 50, p80: 65, p95: 80 };
const CCI_PROFILE = { p5: -150, p20: -60, p50: 0, p80: 60, p95: 150 };

/**
 * Parse the first x-coordinate from a polyline 'points' string like "x1,y1 x2,y2 ...".
 */
function firstX(pointsAttr: string): number {
  const firstPair = pointsAttr.trim().split(/\s+/)[0];
  return parseFloat(firstPair.split(',')[0]);
}

/**
 * Parse the last x-coordinate from a polyline 'points' string.
 */
function lastX(pointsAttr: string): number {
  const parts = pointsAttr.trim().split(/\s+/);
  const lastPair = parts[parts.length - 1];
  return parseFloat(lastPair.split(',')[0]);
}

/** Compute expected SVG x for a domain value using the chart's geometry constants. */
function svgX(domainValue: number, domainMin: number, domainMax: number): number {
  const pad = 28;
  const W = 400;
  return pad + ((W - 2 * pad) * (domainValue - domainMin)) / (domainMax - domainMin);
}

describe('PercentileMappingChart — default domain [0, 100]', () => {
  it('sentinel corner points are x=0 and x=100 in SVG space', () => {
    const { container } = render(
      <PercentileMappingChart
        profile={RSI_PROFILE}
        today={50}
        score={0}
        regime="ranging"
      />
    );

    const polylines = container.querySelectorAll('polyline');
    // Two polylines: active and inactive curves. Both share the same domain sentinels.
    expect(polylines.length).toBeGreaterThanOrEqual(1);
    const points = polylines[0].getAttribute('points') ?? '';

    expect(Math.abs(firstX(points) - svgX(0, 0, 100))).toBeLessThan(0.1);
    expect(Math.abs(lastX(points) - svgX(100, 0, 100))).toBeLessThan(0.1);
  });
});

describe('PercentileMappingChart — domain [-200, 200] for CCI', () => {
  it('sentinel corner points are x=-200 and x=200 in SVG space', () => {
    const { container } = render(
      <PercentileMappingChart
        profile={CCI_PROFILE}
        today={100}
        score={-50}
        regime="ranging"
        label="CCI"
        domain={[-200, 200]}
      />
    );

    const polylines = container.querySelectorAll('polyline');
    expect(polylines.length).toBeGreaterThanOrEqual(1);
    const points = polylines[0].getAttribute('points') ?? '';

    expect(Math.abs(firstX(points) - svgX(-200, -200, 200))).toBeLessThan(0.1);
    expect(Math.abs(lastX(points) - svgX(200, -200, 200))).toBeLessThan(0.1);
  });
});
