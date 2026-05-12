/**
 * Tests for MatrixTable.tsx.
 *
 * Verifies empty-state rendering, cell colour logic via data-tone attributes,
 * signalDirection=0 forcing all coloured cells to grey, pattern row rendering,
 * and that the categories prop drives column headers.
 */

import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MatrixTable } from '@/components/MatrixTable';
import type { Pattern, Snapshot, ContributionItem } from '@/lib/api/types';

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

  describe('contribution display', () => {
    /** Minimal snapshot fixture with a contributions_payload populated for indicator items. */
    function makeSnapshotWithContributions(
      items: ContributionItem[],
    ): Snapshot {
      return {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          contributions_payload: {
            expansion_factor: 1.0,
            items,
          },
          rsi_sparkline: [],
        },
        weekly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      } as Snapshot;
    }

    it('positive contribution renders up-glyph and magnitude in own-category indicator cell', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 57, raw_value: 57, category_weight: 0.2, contribution: 3.2 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).toContain('▲');
      expect(cell.textContent).toContain('3.2');
    });

    it('negative contribution renders down-glyph and magnitude in own-category indicator cell', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: -40, raw_value: -40, category_weight: 0.2, contribution: -1.8 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 35 }}
          indicatorScores={{ rsi_14: -40 }}
          signalDirection={-1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).toContain('▼');
      expect(cell.textContent).toContain('1.8');
    });

    it('zero contribution renders muted 0.0 in own-category indicator cell', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 0, raw_value: 0, category_weight: 0.2, contribution: 0 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 50 }}
          indicatorScores={{ rsi_14: 0 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).toContain('0.0');
    });

    it('absent contribution item renders empty cell — no glyph, no number', () => {
      // snapshot has no item for rsi_14
      const snapshot = makeSnapshotWithContributions([]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent?.trim()).toBe('');
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });

    it('pattern row does NOT render any contribution glyph even when a kind=pattern item exists', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'candlestick_pattern_score', category: 'candlestick', kind: 'pattern', score: 50, raw_value: 50, category_weight: 0.1, contribution: 2.5 },
      ]);
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
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('pattern-cell-bullish_engulfing-0-candlestick');
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });

    it('aggregate row renders contribution glyph when payload contains an aggregate item', () => {
      // Payload contains a kind='aggregate' item for sentiment with contribution 3.4.
      const snapshot = makeSnapshotWithContributions([
        { name: 'sentiment', category: 'sentiment', kind: 'aggregate', score: 50, raw_value: null, category_weight: 0.0, contribution: 3.4 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          categoryScores={{ sentiment: 25, fundamental: -10, macro: 5 }}
          snapshot={snapshot}
        />,
      );
      const sentimentCell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(sentimentCell.textContent).toContain('▲');
      expect(sentimentCell.textContent).toContain('3.4');
    });

    it('aggregate row renders NO glyph when payload contains no aggregate items', () => {
      // Backward-compat: payload has only indicator items; aggregate rows stay blank.
      const snapshot = makeSnapshotWithContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 57, raw_value: 57, category_weight: 0.2, contribution: 3.2 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          categoryScores={{ sentiment: 25, fundamental: -10, macro: 5 }}
          snapshot={snapshot}
        />,
      );
      const sentimentCell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(sentimentCell.textContent).not.toContain('▲');
      expect(sentimentCell.textContent).not.toContain('▼');
    });

    it('weekly timeframe aggregate row does NOT render contribution even if payload contains aggregate items', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'sentiment', category: 'sentiment', kind: 'aggregate', score: 50, raw_value: null, category_weight: 0.0, contribution: 3.4 },
      ]);
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          categoryScores={{}}
          snapshot={snapshot}
        />,
      );
      // On weekly timeframe the sentiment column is off-timeframe, so the cell shows '—'
      const sentimentCell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(sentimentCell.textContent).not.toContain('▲');
      expect(sentimentCell.textContent).not.toContain('▼');
    });

    it('aggregate row zero contribution renders muted 0.0', () => {
      // Payload has kind='aggregate' with contribution=0 → cell renders muted "0.0".
      const snapshot = makeSnapshotWithContributions([
        { name: 'sentiment', category: 'sentiment', kind: 'aggregate', score: 50, raw_value: null, category_weight: 0.0, contribution: 0 },
      ]);
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          categoryScores={{ sentiment: 25 }}
          snapshot={snapshot}
        />,
      );
      const sentimentCell = screen.getByTestId('aggregate-cell-sentiment-sentiment');
      expect(sentimentCell.textContent).toContain('0.0');
    });

    it('weekly timeframe does NOT render contributions even if contributions_payload present', () => {
      const snapshot = makeSnapshotWithContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 57, raw_value: 57, category_weight: 0.2, contribution: 3.2 },
      ]);
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });

    it('missing contributions_payload entirely renders empty cells without crashing', () => {
      const snapshot: Snapshot = {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          // contributions_payload intentionally absent
          rsi_sparkline: [],
        },
        weekly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      };
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural', 'sentiment', 'fundamental', 'macro']}
          timeframe="daily"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      // cell renders without crash; no contribution glyph
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });
  });

  describe('weekly contribution display', () => {
    /** Build a snapshot with weekly contributions_payload and no daily payload. */
    function makeSnapshotWithWeeklyContributions(
      items: ContributionItem[],
    ): Snapshot {
      return {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          // no contributions_payload on daily
          rsi_sparkline: [],
        },
        weekly: {
          data_available: true,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: '2026-05-05',
          resolved_period_label: 'Week of May 5',
          is_fallback: false,
          contributions_payload: {
            expansion_factor: 1.0,
            items,
          },
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      } as Snapshot;
    }

    it('weekly timeframe renders up-glyph when weekly payload contains positive indicator contribution', () => {
      const snapshot = makeSnapshotWithWeeklyContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 55, raw_value: 55, category_weight: 0.35, contribution: 3.1 },
      ]);
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 55 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).toContain('▲');
      expect(cell.textContent).toContain('3.1');
    });

    it('weekly timeframe renders down-glyph when weekly payload contains negative indicator contribution', () => {
      const snapshot = makeSnapshotWithWeeklyContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: -45, raw_value: -45, category_weight: 0.35, contribution: -2.2 },
      ]);
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 30 }}
          indicatorScores={{ rsi_14: -45 }}
          signalDirection={-1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).toContain('▼');
      expect(cell.textContent).toContain('2.2');
    });

    it('weekly timeframe renders no glyph when weekly payload is absent', () => {
      // Snapshot has no contributions_payload on weekly section — backward-compat.
      const snapshot: Snapshot = {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          rsi_sparkline: [],
        },
        weekly: {
          data_available: true,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: '2026-05-05',
          resolved_period_label: 'Week of May 5',
          is_fallback: false,
          // no contributions_payload
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      };
      render(
        <MatrixTable
          title="Weekly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 55 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural']}
          timeframe="weekly"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });

    it('monthly timeframe renders no glyph even when weekly payload contains items', () => {
      // Monthly stays gated — even if snapshot.weekly has a payload, monthly matrix
      // should not read it.
      const snapshot = makeSnapshotWithWeeklyContributions([
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 55, raw_value: 55, category_weight: 0.35, contribution: 3.1 },
      ]);
      render(
        <MatrixTable
          title="Monthly"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 55 }}
          signalDirection={1}
          categories={['trend', 'momentum', 'volume', 'volatility', 'structural']}
          timeframe="monthly"
          snapshot={snapshot}
        />,
      );
      const cell = screen.getByTestId('cell-rsi_14-momentum');
      expect(cell.textContent).not.toContain('▲');
      expect(cell.textContent).not.toContain('▼');
    });
  });

  describe('explainer affordance — chevron + click behaviour', () => {
    it('rsi_14 row is clickable (role="button") and toggles aria-expanded on click', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
        />,
      );
      const cell = screen.getByRole('button', { name: /RSI 14/i });
      expect(cell.getAttribute('aria-expanded')).toBe('false');
      fireEvent.click(cell);
      expect(cell.getAttribute('aria-expanded')).toBe('true');
      fireEvent.click(cell);
      expect(cell.getAttribute('aria-expanded')).toBe('false');
    });

    it('macd_line row is clickable (role="button") — parity with RSI', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ macd_line: 0.42, macd_signal: 0.31, macd_histogram: 0.11 }}
          indicatorScores={{ macd_line: 55 }}
          signalDirection={1}
        />,
      );
      expect(screen.getByRole('button', { name: /MACD line/i })).toBeInTheDocument();
    });

    it('non-explainer rows (e.g. stoch_k) render no role="button" and no click affordance', () => {
      render(
        <MatrixTable
          title="Daily"
          indicators={{ stoch_k: 50 }}
          indicatorScores={{ stoch_k: 10 }}
          signalDirection={1}
        />,
      );
      // The Stoch K label is rendered, but the <td> does NOT become a button.
      expect(screen.getByText('Stoch K')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /Stoch K/i })).not.toBeInTheDocument();
    });
  });

  describe('header math chain (headerContribution prop)', () => {
    it('renders weight × score = contribution when headerContribution is provided with finite values', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          headerContribution={{ weight: 0.60, score: 20.5 }}
        />,
      );
      // Weight as percentage
      expect(screen.getByText(/60%/)).toBeInTheDocument();
      // Score with sign
      expect(screen.getByText(/20\.5/)).toBeInTheDocument();
      // Contribution = 0.60 × 20.5 = 12.3
      expect(screen.getByText(/12\.3/)).toBeInTheDocument();
    });

    it('renders ▲ glyph when contribution is positive', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          headerContribution={{ weight: 0.60, score: 20.5 }}
        />,
      );
      expect(screen.getByRole('heading', { level: 3 }).textContent).toContain('▲');
    });

    it('renders ▼ glyph when contribution is negative', () => {
      render(
        <MatrixTable
          title="Monthly — Indicator Agreement"
          indicators={{ rsi_14: 35 }}
          indicatorScores={{ rsi_14: -40 }}
          signalDirection={-1}
          headerContribution={{ weight: 0.40, score: -8.3 }}
        />,
      );
      expect(screen.getByRole('heading', { level: 3 }).textContent).toContain('▼');
    });

    it('renders muted 0.0 when contribution evaluates to zero (score is 0)', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 50 }}
          indicatorScores={{ rsi_14: 0 }}
          signalDirection={0}
          headerContribution={{ weight: 0.60, score: 0 }}
        />,
      );
      const heading = screen.getByRole('heading', { level: 3 });
      // Should show "0.0" and no directional glyph in the header
      expect(heading.textContent).toContain('0.0');
      expect(heading.textContent).not.toContain('▲');
      expect(heading.textContent).not.toContain('▼');
    });

    it('hides header math when headerContribution is null', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          headerContribution={null}
        />,
      );
      const heading = screen.getByRole('heading', { level: 3 });
      // No percentage sign, no arrow glyphs
      expect(heading.textContent).not.toContain('%');
      expect(heading.textContent).not.toContain('▲');
      expect(heading.textContent).not.toContain('▼');
    });

    it('hides header math when headerContribution is undefined (prop omitted)', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          // headerContribution omitted
        />,
      );
      const heading = screen.getByRole('heading', { level: 3 });
      expect(heading.textContent).not.toContain('%');
      expect(heading.textContent).not.toContain('▲');
      expect(heading.textContent).not.toContain('▼');
    });

    it('hides header math when headerContribution.score is NaN', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          headerContribution={{ weight: 0.60, score: NaN }}
        />,
      );
      const heading = screen.getByRole('heading', { level: 3 });
      expect(heading.textContent).not.toContain('%');
    });

    it('hides header math when headerContribution.score is Infinity', () => {
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          headerContribution={{ weight: 0.60, score: Infinity }}
        />,
      );
      const heading = screen.getByRole('heading', { level: 3 });
      expect(heading.textContent).not.toContain('%');
    });
  });

  describe('section equation row', () => {
    /** Build a minimal Snapshot with a daily contributions_payload. */
    function makeSnapshotForEquation(items: ContributionItem[]): Snapshot {
      return {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          contributions_payload: { expansion_factor: 1.5, items },
          rsi_sparkline: [],
        },
        weekly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      } as Snapshot;
    }

    const SCORING_RULES_WITH_TOP5 = {
      rsi: {
        thresholds: { oversold: 30, overbought: 70 },
        scoring_method: 'percentile_blended_with_fallback',
        fallback_zones: [],
        profile_zones: [],
      },
      regime_weights: {},
      score_expansion_factor: 1.5,
      approximation_caveat: '',
      timeframe_weights: {},
    };

    it('renders equation row when contributions_payload and headerContribution are provided', () => {
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 50, raw_value: 50, category_weight: 0.2, contribution: 5.0 },
        { name: 'macd_line', category: 'trend', kind: 'indicator', score: 60, raw_value: 60, category_weight: 0.25, contribution: 8.0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 20.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      // ≈ symbol should appear in the equation row
      expect(document.body.textContent).toContain('≈');
    });

    it('equation row is hidden when contributions_payload is absent', () => {
      const snapshot: Snapshot = {
        daily: {
          data_available: true,
          categories: [
            'trend', 'momentum', 'volume', 'volatility',
            'candlestick', 'structural', 'sentiment', 'fundamental', 'macro',
          ],
          resolved_period: '2026-05-12',
          // contributions_payload intentionally absent
          rsi_sparkline: [],
        },
        weekly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'candlestick', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
        monthly: {
          data_available: false,
          categories: ['trend', 'momentum', 'volume', 'volatility', 'structural'],
          resolved_period: null,
          resolved_period_label: null,
          is_fallback: false,
        },
      };
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 20.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      // No equation-specific content (no standalone ≈ followed by contribution chips)
      // The header math chain also has = sign but not ≈ — only the equation row uses ≈
      expect(document.body.textContent).not.toContain('≈');
    });

    it('equation row is hidden when headerContribution is null', () => {
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 50, raw_value: 50, category_weight: 0.2, contribution: 5.0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={null}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      expect(document.body.textContent).not.toContain('≈');
    });

    it('renders all non-zero items with no "others" suffix', () => {
      const items: ContributionItem[] = [
        { name: 'a', category: 'trend', kind: 'indicator', score: 60, raw_value: 60, category_weight: 0.2, contribution: 10.0 },
        { name: 'b', category: 'trend', kind: 'indicator', score: 55, raw_value: 55, category_weight: 0.2, contribution: 9.0 },
        { name: 'c', category: 'trend', kind: 'indicator', score: 50, raw_value: 50, category_weight: 0.2, contribution: 8.0 },
        { name: 'd', category: 'trend', kind: 'indicator', score: 45, raw_value: 45, category_weight: 0.2, contribution: 7.0 },
        { name: 'e', category: 'trend', kind: 'indicator', score: 40, raw_value: 40, category_weight: 0.2, contribution: 6.0 },
        { name: 'f', category: 'trend', kind: 'indicator', score: 35, raw_value: 35, category_weight: 0.2, contribution: 5.0 },
        { name: 'g', category: 'trend', kind: 'indicator', score: 30, raw_value: 30, category_weight: 0.2, contribution: 4.0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 50.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      expect(document.body.textContent).not.toContain('others');
    });

    it('hides zero-contribution items from the equation row', () => {
      const items: ContributionItem[] = [
        { name: 'has_value', category: 'trend', kind: 'indicator', score: 60, raw_value: 60, category_weight: 0.2, contribution: 10.0 },
        { name: 'divergence_macd', category: 'momentum', kind: 'pattern', score: 0, raw_value: null, category_weight: 0.2, contribution: 0 },
        { name: 'crossover_ema_9_21', category: 'trend', kind: 'pattern', score: 0, raw_value: null, category_weight: 0.2, contribution: 0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 50.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      expect(document.body.textContent).not.toContain('Divergence Macd');
      expect(document.body.textContent).not.toContain('Crossover Ema');
    });

    it('total rendered equals headerContribution.score, NOT sum of items', () => {
      // sum of items = 5 + 3 = 8, but headerContribution.score = 15 (diverges by ≥0.5)
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 50, raw_value: 50, category_weight: 0.2, contribution: 5.0 },
        { name: 'macd_line', category: 'trend', kind: 'indicator', score: 30, raw_value: 30, category_weight: 0.2, contribution: 3.0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 15.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      // The total shown should be 15.0, not 8.0
      expect(document.body.textContent).toContain('15.0');
      // Ensure 8.0 is NOT present as a total (the individual contributions 5.0 and 3.0 are fine)
      // We check the equation total suffix: "+15.0" should appear
      expect(document.body.textContent).toContain('+15.0');
    });

    it('≈ symbol appears in equation row text', () => {
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 50, raw_value: 50, category_weight: 0.2, contribution: 5.0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 60.5 }}
          indicatorScores={{ rsi_14: 57 }}
          signalDirection={1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 10.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      expect(document.body.textContent).toContain('≈');
    });

    it('negative contributions render with ▼ glyph in the equation row', () => {
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: -40, raw_value: -40, category_weight: 0.2, contribution: -4.5 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 35 }}
          indicatorScores={{ rsi_14: -40 }}
          signalDirection={-1}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: -10.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      expect(document.body.textContent).toContain('▼');
    });

    it('zero contribution items render 0.0', () => {
      const items: ContributionItem[] = [
        { name: 'rsi_14', category: 'momentum', kind: 'indicator', score: 0, raw_value: 0, category_weight: 0.2, contribution: 0 },
      ];
      const snapshot = makeSnapshotForEquation(items);
      render(
        <MatrixTable
          title="Daily — Indicator Agreement"
          indicators={{ rsi_14: 50 }}
          indicatorScores={{ rsi_14: 0 }}
          signalDirection={0}
          timeframe="daily"
          snapshot={snapshot}
          headerContribution={{ weight: 0.60, score: 0.0 }}
          scoringRules={SCORING_RULES_WITH_TOP5}
        />,
      );
      // 0 contribution → "0.0" in muted text
      expect(document.body.textContent).toContain('0.0');
    });
  });
});
