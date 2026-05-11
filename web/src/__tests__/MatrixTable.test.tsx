/**
 * Tests for MatrixTable.tsx.
 *
 * Verifies empty-state rendering, cell colour logic via data-tone attributes,
 * and that signalDirection=0 forces all coloured cells to grey.
 */

import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MatrixTable } from '@/components/MatrixTable';

describe('MatrixTable', () => {
  it('shows empty-state message when no data is provided', () => {
    render(
      <MatrixTable
        title="Daily — Indicator Agreement"
        indicators={undefined}
        indicatorScores={undefined}
        signalDirection={1}
      />,
    );
    expect(screen.getByText(/indicator scores not available/i)).toBeInTheDocument();
  });

  it('shows empty-state when both maps are empty objects', () => {
    render(
      <MatrixTable
        title="Daily — Indicator Agreement"
        indicators={{}}
        indicatorScores={{}}
        signalDirection={1}
      />,
    );
    expect(screen.getByText(/indicator scores not available/i)).toBeInTheDocument();
  });

  it('renders the table when indicators are provided', () => {
    render(
      <MatrixTable
        title="Daily — Indicator Agreement"
        indicators={{ rsi_14: 60.5 }}
        indicatorScores={{ rsi_14: 57 }}
        signalDirection={1}
      />,
    );
    // Header columns present
    expect(screen.getByText('Indicator')).toBeInTheDocument();
    expect(screen.getByText('Value')).toBeInTheDocument();
    expect(screen.getByText('Trend')).toBeInTheDocument();
    expect(screen.getByText('Momentum')).toBeInTheDocument();
  });

  describe('cell colour logic with signalDirection=1', () => {
    const indicators = { rsi_14: 60.5 };
    const indicatorScores = { rsi_14: 57, ema_alignment: -30 };

    it('own-category cell is green when score and direction agree (positive)', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={indicators}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'green');
    });

    it('own-category cell is red when score opposes direction', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={indicators}
          indicatorScores={{ rsi_14: -40 }}
          signalDirection={1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'red');
    });

    it('own-category cell is grey when score is null', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={indicators}
          indicatorScores={{ rsi_14: null }}
          signalDirection={1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'grey');
    });

    it('own-category cell is grey when score is 0', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={indicators}
          indicatorScores={{ rsi_14: 0 }}
          signalDirection={1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'grey');
    });

    it('off-category cell is always grey', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={indicators}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
        />,
      );
      // rsi_14 belongs to momentum; trend is an off-category column
      const offCell = screen.getByTestId('cell-rsi_14-trend');
      expect(offCell).toHaveAttribute('data-tone', 'grey');
    });
  });

  describe('cell colour logic with signalDirection=0', () => {
    it('own-category cell is grey even when score is non-zero', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 75 }}
          signalDirection={0}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'grey');
    });
  });

  describe('cell colour logic with signalDirection=-1', () => {
    it('own-category cell is green when negative score agrees with bearish direction', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 35 }}
          indicatorScores={{ rsi_14: -55 }}
          signalDirection={-1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'green');
    });

    it('own-category cell is red when positive score opposes bearish direction', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 70 }}
          indicatorScores={{ rsi_14: 60 }}
          signalDirection={-1}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveAttribute('data-tone', 'red');
    });
  });

  it('formats numeric values to 2 decimal places', () => {
    render(
      <MatrixTable
        title="Test"
        indicators={{ rsi_14: 61.123 }}
        indicatorScores={{ rsi_14: 50 }}
        signalDirection={1}
      />,
    );
    expect(screen.getByText('61.12')).toBeInTheDocument();
  });

  it('shows em-dash for missing indicator values', () => {
    render(
      <MatrixTable
        title="Test"
        indicators={{ rsi_14: null }}
        indicatorScores={{ rsi_14: null }}
        signalDirection={1}
      />,
    );
    // Multiple rows render "—" (one per indicator with no value in the map).
    const dashes = screen.getAllByText('—');
    expect(dashes.length).toBeGreaterThan(0);
  });
});
