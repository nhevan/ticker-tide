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

vi.mock('@/lib/hooks/useVerdict', () => ({
  useVerdict: vi.fn(),
  useGenerateVerdict: vi.fn(),
}));

import { useVerdict, useGenerateVerdict } from '@/lib/hooks/useVerdict';

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

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" />);
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

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" />);
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

    render(<VerdictBlock ticker="AAPL" date="2026-04-25" />);
    fireEvent.click(screen.getByRole('button', { name: /generate verdict/i }));
    expect(mutate).toHaveBeenCalledWith({ ticker: 'AAPL', date: '2026-04-25' });
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

    const { container } = render(<VerdictBlock ticker="AAPL" date="2026-04-25" />);
    expect(screen.queryByRole('button', { name: /generate verdict/i })).not.toBeInTheDocument();
    expect(container.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0);
  });
});
