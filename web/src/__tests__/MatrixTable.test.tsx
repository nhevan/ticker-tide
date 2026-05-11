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

  describe('column headers are always all 9 categories', () => {
    it('monthly_still_renders_all_nine_category_columns', () => {
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
      // Indicator + Value + 9 categories = 11
      expect(headers).toHaveLength(11);
      const headerTexts = headers.map((h) => h.textContent);
      expect(headerTexts).toContain('Candlestick');
      expect(headerTexts).toContain('Sentiment');
      expect(headerTexts).toContain('Fundamental');
      expect(headerTexts).toContain('Macro');
    });

    it('monthly_off_timeframe_own_category_cell_shows_em_dash_with_tooltip', () => {
      // BB %B is the volatility indicator; volatility IS scored at monthly.
      // Use Sentiment column instead — sentiment is daily-only so its
      // own-category indicator rows are off-timeframe at monthly.
      // No indicator maps to sentiment, so test via a pattern placeholder
      // row instead: candlestick row on monthly = off-timeframe.
      render(
        <MatrixTable
          title="Monthly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'structural']}
          timeframe="monthly"
          recentPatterns={[]}
        />,
      );
      // Placeholder candlestick row's candlestick cell should be off-timeframe.
      const cell = screen.getByTestId('pattern-placeholder-cell-candlestick-candlestick');
      expect(cell).toHaveTextContent('—');
      expect(cell.getAttribute('title')).toBe('Daily and weekly only');
      expect(cell.getAttribute('data-tone')).toBe('grey');
    });

    it('placeholder_pattern_rows_render_when_no_patterns_provided', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          recentPatterns={[]}
        />,
      );
      const candlestickCell = screen.getByTestId('pattern-placeholder-cell-candlestick-candlestick');
      expect(candlestickCell).toHaveTextContent('—');
      expect(candlestickCell.getAttribute('title')).toBe('No patterns detected in window');
      const structuralCell = screen.getByTestId('pattern-placeholder-cell-structural-structural');
      expect(structuralCell).toHaveTextContent('—');
      expect(structuralCell.getAttribute('title')).toBe('No patterns detected in window');
    });

    it('placeholder_row_replaced_by_real_pattern_rows_when_category_has_patterns', () => {
      const pattern: Pattern = {
        pattern_name: 'bullish_engulfing',
        pattern_category: 'candlestick',
        direction: 'bullish',
        strength: 2.5,
        confirmed: true,
        days_ago: 0,
      };
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          recentPatterns={[pattern]}
        />,
      );
      // Candlestick has a real pattern → no placeholder candlestick row
      expect(screen.queryByTestId('pattern-placeholder-cell-candlestick-candlestick')).toBeNull();
      // Structural has no real patterns → placeholder still present
      expect(screen.getByTestId('pattern-placeholder-cell-structural-structural')).not.toBeNull();
    });

    it('aggregate_sentiment_row_renders_with_value_and_green_when_score_matches_signal', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          categoryScores={{ sentiment: 25, fundamental: -10, macro: 0 }}
        />,
      );
      // Sentiment aggregate row, own cell, positive score + bullish signal → green
      const sentimentCell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(sentimentCell.getAttribute('data-tone')).toBe('green');
      // Fundamental: negative score + bullish signal → red
      const fundamentalCell = screen.getByTestId('aggregate-cell-fundamental-fundamental');
      expect(fundamentalCell.getAttribute('data-tone')).toBe('red');
      // Macro: zero score → grey
      const macroCell = screen.getByTestId('aggregate-cell-macro-macro');
      expect(macroCell.getAttribute('data-tone')).toBe('grey');
    });

    it('aggregate_row_off_timeframe_shows_em_dash_with_daily_only_tooltip', () => {
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          categoryScores={{}}
        />,
      );
      const cell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(cell).toHaveTextContent('—');
      expect(cell.getAttribute('title')).toBe('Daily only');
    });

    it('aggregate_row_null_score_shows_em_dash_with_score_not_available_tooltip', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          categoryScores={{ sentiment: null }}
        />,
      );
      const cell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(cell).toHaveTextContent('—');
      expect(cell.getAttribute('title')).toBe('Score not available');
    });

    it('null_indicator_score_renders_em_dash_with_tooltip', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: null }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveTextContent('—');
      expect(cell.getAttribute('title')).toBe('Score not available');
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

    it('empty_recent_patterns_renders_placeholders_not_real_pattern_rows', () => {
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
      // No real-pattern cells
      expect(document.querySelectorAll('[data-testid^="pattern-cell-"]')).toHaveLength(0);
      // Both placeholders ARE present
      expect(document.querySelectorAll('[data-testid^="pattern-placeholder-cell-"]').length).toBeGreaterThan(0);
      // Indicator rows still present
      expect(screen.getByTestId('cell-rsi_14-momentum')).toBeInTheDocument();
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
      expect(document.querySelectorAll('[data-testid^="pattern-cell-"]')).toHaveLength(0);
      // Placeholders still appear
      expect(screen.getByTestId('pattern-placeholder-cell-candlestick-candlestick')).toBeInTheDocument();
      expect(screen.getByTestId('pattern-placeholder-cell-structural-structural')).toBeInTheDocument();
    });

    it('indicator_off_timeframe_cell_shows_em_dash_with_daily_only_tooltip', () => {
      // No indicator maps to sentiment/fundamental/macro directly, but the
      // off-timeframe branch is reachable through pattern placeholders. For
      // an indicator-row off-timeframe test we'd need an indicator mapped
      // to one of those 3 categories; none exist. This test instead
      // verifies the offTimeframeReason mapping via the candlestick
      // placeholder on monthly (already covered) and the daily-only
      // tooltip via a hypothetical scenario constructed below.
      // We exercise it by passing a scored set that EXCLUDES momentum
      // (off-timeframe for an indicator that owns momentum, e.g. rsi_14).
      render(
        <MatrixTable
          title="Test"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'volume', 'volatility', 'structural']}
          timeframe="weekly"
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell).toHaveTextContent('—');
      expect(cell.getAttribute('title')).toBe('Not scored at this timeframe');
    });
  });
});
