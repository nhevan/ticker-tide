/**
 * Tests for MatrixTable.tsx.
 *
 * Verifies empty-state rendering, cell colour logic via data-tone attributes,
 * signalDirection=0 forcing all coloured cells to grey, pattern row rendering,
 * and that the categories prop drives column headers.
 */

import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MatrixTable } from '@/components/MatrixTable';
import type { Pattern } from '@/lib/api/types';

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

  describe('categories prop controls column headers', () => {
    it('monthly_renders_only_five_category_columns', () => {
      render(
        <MatrixTable
          title="Monthly — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'structural']}
          timeframe="monthly"
        />,
      );
      const headers = screen.getAllByRole('columnheader');
      // Indicator + Value + 5 categories = 7
      expect(headers).toHaveLength(7);
      const headerTexts = headers.map((h) => h.textContent);
      expect(headerTexts).not.toContain('Candlestick');
    });
  });

  describe('pattern row rendering', () => {
    const samplePattern: Pattern = {
      pattern_name: 'bullish_engulfing',
      pattern_category: 'candlestick',
      direction: 'bullish',
      strength: 2.5,
      confirmed: true,
      days_ago: 0,
    };

    it('pattern_row_renders_below_indicator_rows_with_humanised_label', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[samplePattern]}
        />,
      );
      // Humanised label should appear (not snake_case)
      expect(screen.getByText('Bullish Engulfing')).toBeInTheDocument();
    });

    it('pattern_own_category_cell_green_when_direction_matches_signal', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, direction: 'bullish' }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveAttribute('data-tone', 'green');
    });

    it('pattern_own_category_cell_red_when_direction_opposes_signal', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={-1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, direction: 'bullish' }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveAttribute('data-tone', 'red');
    });

    it('pattern_own_category_cell_grey_when_direction_neutral', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, direction: 'neutral' }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveAttribute('data-tone', 'grey');
    });

    it('pattern_own_category_cell_grey_when_signal_direction_zero', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={0}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, direction: 'bullish' }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveAttribute('data-tone', 'grey');
    });

    it('pattern_off_category_cell_always_grey', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, direction: 'bullish' }]}
        />,
      );
      // candlestick pattern on 'trend' column is off-category → grey
      const offCell = screen.getByTestId('pattern-cell-bullish_engulfing-0-trend');
      expect(offCell).toHaveAttribute('data-tone', 'grey');
    });

    it('daily_pattern_cell_text_today_confirmed', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, days_ago: 0, confirmed: true }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveTextContent('today ✓');
    });

    it('daily_pattern_cell_text_n_days_confirmed', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, days_ago: 2, confirmed: true }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveTextContent('2d ✓');
    });

    it('daily_pattern_cell_text_n_days_unconfirmed', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[{ ...samplePattern, days_ago: 5, confirmed: false }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveTextContent('5d');
      expect(cell.textContent).not.toContain('✓');
    });

    it('weekly_pattern_cell_text_confirmed', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          recentPatterns={[{ ...samplePattern, confirmed: true }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveTextContent('✓');
    });

    it('weekly_pattern_cell_text_unconfirmed', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          recentPatterns={[{ ...samplePattern, confirmed: false }]}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      // unconfirmed weekly → no checkmark, empty or whitespace
      expect(cell.textContent?.trim()).toBe('');
    });

    it('pattern_row_renders_when_days_ago_undefined', () => {
      const patternWithoutDaysAgo: Pattern = {
        pattern_name: 'bullish_engulfing',
        pattern_category: 'candlestick',
        direction: 'bullish',
        strength: 2.5,
        confirmed: true,
        // days_ago is absent (weekly/monthly path)
      };
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          recentPatterns={[patternWithoutDaysAgo]}
        />,
      );
      // No error and weekly text shows correctly (confirmed → ✓)
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell).toHaveTextContent('✓');
    });

    it('empty_recent_patterns_leaves_indicator_rows_untouched', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          recentPatterns={[]}
        />,
      );
      // No pattern cells
      const patternCells = document.querySelectorAll('[data-testid^="pattern-cell-"]');
      expect(patternCells).toHaveLength(0);
      // Indicator rows still present
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toBeInTheDocument();
    });

    it('undefined_recent_patterns_behaves_like_empty', () => {
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="daily"
          // recentPatterns omitted
        />,
      );
      const patternCells = document.querySelectorAll('[data-testid^="pattern-cell-"]');
      expect(patternCells).toHaveLength(0);
    });
  });
});
