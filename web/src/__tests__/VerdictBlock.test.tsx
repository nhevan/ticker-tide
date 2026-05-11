/**
 * Tests for VerdictBlock.tsx.
 *
 * Mocks useVerdict and useGenerateVerdict to exercise each render state:
 * cached → text; uncached idle → button; generating → skeleton.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { VerdictBlock } from '@/components/VerdictBlock';
import type { Snapshot } from '@/lib/api/types';

vi.mock('@/lib/hooks/useVerdict', () => ({
  useVerdict: vi.fn(),
  useGenerateVerdict: vi.fn(),
}));

import { useVerdict, useGenerateVerdict } from '@/lib/hooks/useVerdict';

const snapshot: Snapshot = {
  daily: {
    data_available: true,
    categories: [],
    resolved_period: '2026-04-25',
    composite_score: 55.0,
    scores: { trend: 40, momentum: 30 },
  },
  weekly: {
    data_available: true,
    categories: [],
    resolved_period: '2026-04-21',
    resolved_period_label: null,
    is_fallback: false,
    composite_score: 12.0,
    scores: { trend: 10, momentum: 5 },
  },
  monthly: {
    data_available: true,
    categories: [],
    resolved_period: '2026-04-01',
    resolved_period_label: null,
    is_fallback: false,
    composite_score: -20.0,
    scores: { trend: -25, momentum: -10 },
  },
};

describe('VerdictBlock', () => {
  it('shows the Generate verdict button when no cached verdict exists', () => {
    vi.mocked(useVerdict).mockReturnValue({
      data: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useVerdict>);
    vi.mocked(useGenerateVerdict).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: null,
      isPending: false,
    } as unknown as ReturnType<typeof useGenerateVerdict>);

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" snapshot={snapshot} />);
    expect(screen.getByRole('button', { name: /generate verdict/i })).toBeInTheDocument();
  });

  it('renders cached verdict text when present', () => {
    vi.mocked(useVerdict).mockReturnValue({
      data: { verdict: 'BUY\nStrong momentum.', generated_at: '2026-04-25T12:00:00Z' },
      isLoading: false,
    } as unknown as ReturnType<typeof useVerdict>);
    vi.mocked(useGenerateVerdict).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: null,
      isPending: false,
    } as unknown as ReturnType<typeof useGenerateVerdict>);

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" snapshot={snapshot} />);
    expect(screen.getByText(/strong momentum/i)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /generate verdict/i })).not.toBeInTheDocument();
  });

  it('calls mutate with ticker and date when button clicked', () => {
    const mutate = vi.fn();
    vi.mocked(useVerdict).mockReturnValue({
      data: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useVerdict>);
    vi.mocked(useGenerateVerdict).mockReturnValue({
      mutate,
      data: undefined,
      error: null,
      isPending: false,
    } as unknown as ReturnType<typeof useGenerateVerdict>);

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" snapshot={snapshot} />);
    fireEvent.click(screen.getByRole('button', { name: /generate verdict/i }));
    expect(mutate).toHaveBeenCalledWith({ ticker: 'AAPL', date: '2026-04-25' });
  });

  it('renders the timeframe summary table with score, trend, mom, and direction', () => {
    vi.mocked(useVerdict).mockReturnValue({
      data: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useVerdict>);
    vi.mocked(useGenerateVerdict).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: null,
      isPending: false,
    } as unknown as ReturnType<typeof useGenerateVerdict>);

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" snapshot={snapshot} />);
    expect(screen.getByText('Daily')).toBeInTheDocument();
    expect(screen.getByText('Weekly')).toBeInTheDocument();
    expect(screen.getByText('Monthly')).toBeInTheDocument();
    expect(screen.getByText('55.0')).toBeInTheDocument(); // daily score → ▲
    expect(screen.getByText('▲')).toBeInTheDocument();
    expect(screen.getByText('▬')).toBeInTheDocument(); // weekly 12.0 → flat
    expect(screen.getByText('▼')).toBeInTheDocument(); // monthly -20 → down
  });

  it('hides the button and shows a skeleton while generating', () => {
    vi.mocked(useVerdict).mockReturnValue({
      data: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useVerdict>);
    vi.mocked(useGenerateVerdict).mockReturnValue({
      mutate: vi.fn(),
      data: undefined,
      error: null,
      isPending: true,
    } as unknown as ReturnType<typeof useGenerateVerdict>);

    const { container } = render(<VerdictBlock ticker="AAPL" date="2026-04-25" snapshot={snapshot} />);
    expect(screen.queryByRole('button', { name: /generate verdict/i })).not.toBeInTheDocument();
    expect(container.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0);
  });
});
