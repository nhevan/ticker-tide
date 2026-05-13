/**
 * Regression tests for PercentileStrip domain prop generalisation.
 *
 * Key invariants:
 *   1. With default domain [0, 100], the today dot's x-position matches the
 *      expected fraction of the strip width (pre-existing RSI/Stoch %K behaviour
 *      must be unchanged — zero callsite diff).
 *   2. With domain=[-200, 200] (CCI), the x-position correctly reflects the
 *      wider range.
 */

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { PercentileStrip } from '@/components/PercentileStrip';

const RSI_PROFILE = { p5: 10, p20: 30, p50: 50, p80: 70, p95: 90 };
const CCI_PROFILE = { p5: -150, p20: -60, p50: 0, p80: 60, p95: 150 };

const PAD = 8;
const W = 400 - 2 * PAD; // 384

/** Expected cx for a given value within [domainMin, domainMax]. */
function expectedCx(value: number, domainMin: number, domainMax: number): number {
  const clamped = Math.max(domainMin, Math.min(domainMax, value));
  return PAD + (W * (clamped - domainMin)) / (domainMax - domainMin);
}

describe('PercentileStrip — default domain [0, 100]', () => {
  it('today dot sits at 50% of width for RSI=50 with default domain', () => {
    const { container } = render(
      <PercentileStrip
        profile={RSI_PROFILE}
        today={50}
        zoneLabel="above_mid"
        zoneDescription="Above the midpoint"
      />
    );

    const circles = container.querySelectorAll('circle');
    // The first circle is the today dot (tick marks are <line> elements).
    const todayCircle = circles[0];
    const cx = parseFloat(todayCircle.getAttribute('cx') ?? '0');

    // RSI=50 with domain [0, 100]: position = (50 - 0) / 100 = 0.5
    const expected = expectedCx(50, 0, 100);
    expect(Math.abs(cx - expected)).toBeLessThan(0.01);
  });
});

describe('PercentileStrip — domain [-200, 200] for CCI', () => {
  it('today dot sits at 75% of width for CCI=100 with domain [-200, 200]', () => {
    const { container } = render(
      <PercentileStrip
        profile={CCI_PROFILE}
        today={100}
        zoneLabel="overbought"
        zoneDescription="Overbought region"
        label="CCI"
        domain={[-200, 200]}
      />
    );

    const circles = container.querySelectorAll('circle');
    const todayCircle = circles[0];
    const cx = parseFloat(todayCircle.getAttribute('cx') ?? '0');

    // CCI=100 with domain [-200, 200]: position = (100 - (-200)) / 400 = 0.75
    const expected = expectedCx(100, -200, 200);
    expect(Math.abs(cx - expected)).toBeLessThan(0.01);
  });
});
