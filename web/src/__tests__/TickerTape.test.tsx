/**
 * Tests for TickerTape.tsx.
 *
 * Verifies placeholder state, price/change derivation from sparkline,
 * signal pill rendering, loading skeleton, error state, and unknown signal handling.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TickerTape } from '@/components/TickerTape';
import type { Snapshot } from '@/lib/api/types';

/** Build a minimal Snapshot with the given sparkline closes and signal. */
function makeSnapshot(
  closes: number[],
  signal: string | null = 'BULLISH',
  confidence: number | null = 80,
): Snapshot {
  return {
    daily: {
      data_available: true,
      categories: [],
      resolved_period: '2026-05-11',
      signal,
      confidence,
      sparkline: closes.map((close, index) => ({
        date: `2026-05-0${index + 1}`,
        close,
      })),
    },
    weekly: {
      data_available: false,
      categories: [],
      resolved_period: null,
      resolved_period_label: null,
      is_fallback: false,
    },
    monthly: {
      data_available: false,
      categories: [],
      resolved_period: null,
      resolved_period_label: null,
      is_fallback: false,
    },
  };
}

describe('TickerTape', () => {
  it('renders em-dash placeholders when ticker is empty and snapshot is undefined', () => {
    render(
      <TickerTape ticker="" snapshot={undefined} isLoading={false} error={null} />,
    );
    // Multiple em-dash placeholders should be present (symbol, price, pill)
    const dashes = screen.getAllByText('—');
    expect(dashes.length).toBeGreaterThan(1);
  });

  it('renders symbol, price, and green +5.00% when sparkline has [100, 105] and signal is BULLISH', () => {
    const snapshot = makeSnapshot([100, 105], 'BULLISH', 80);
    render(
      <TickerTape ticker="AAPL" snapshot={snapshot} isLoading={false} error={null} />,
    );
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('105.00')).toBeInTheDocument();
    expect(screen.getByText('+5.00%')).toBeInTheDocument();
    const changePct = screen.getByText('+5.00%');
    expect(changePct.getAttribute('data-direction')).toBe('up');
  });

  it('renders red -5.00% when sparkline has [100, 95]', () => {
    const snapshot = makeSnapshot([100, 95], 'BEARISH', 65);
    render(
      <TickerTape ticker="TSLA" snapshot={snapshot} isLoading={false} error={null} />,
    );
    expect(screen.getByText('-5.00%')).toBeInTheDocument();
    const changePct = screen.getByText('-5.00%');
    expect(changePct.getAttribute('data-direction')).toBe('down');
  });

  it('renders —% when sparkline has only one point', () => {
    const snapshot = makeSnapshot([105], 'BULLISH', 70);
    render(
      <TickerTape ticker="MSFT" snapshot={snapshot} isLoading={false} error={null} />,
    );
    expect(screen.getByText('—%')).toBeInTheDocument();
  });

  it('renders —% when prior close is 0', () => {
    const snapshot = makeSnapshot([0, 105], 'BULLISH', 70);
    render(
      <TickerTape ticker="MSFT" snapshot={snapshot} isLoading={false} error={null} />,
    );
    expect(screen.getByText('—%')).toBeInTheDocument();
  });

  it('renders skeleton when isLoading is true', () => {
    render(
      <TickerTape ticker="AAPL" snapshot={undefined} isLoading={true} error={null} />,
    );
    expect(screen.getByTestId('tape-skeleton')).toBeInTheDocument();
  });

  it('renders unknown signal string in muted pill without throwing', () => {
    const snapshot = makeSnapshot([100, 105], 'SURGE', null);
    render(
      <TickerTape ticker="AAPL" snapshot={snapshot} isLoading={false} error={null} />,
    );
    // Should render the raw string without throwing
    expect(screen.getByText('SURGE')).toBeInTheDocument();
  });

  it('renders ERR when error prop is passed', () => {
    render(
      <TickerTape
        ticker="AAPL"
        snapshot={undefined}
        isLoading={false}
        error="Snapshot failed"
      />,
    );
    expect(screen.getByText('ERR')).toBeInTheDocument();
  });
});
