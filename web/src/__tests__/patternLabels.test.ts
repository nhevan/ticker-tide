/**
 * Tests for patternLabels.ts — humanizePatternName utility.
 */

import { describe, it, expect } from 'vitest';
import { humanizePatternName, PATTERN_DISPLAY_LABELS } from '@/lib/scoring/patternLabels';

describe('humanizePatternName', () => {
  it('humanizePatternName_returns_known_label_when_in_map', () => {
    // bullish_engulfing should be in PATTERN_DISPLAY_LABELS
    expect(humanizePatternName('bullish_engulfing')).toBe(PATTERN_DISPLAY_LABELS['bullish_engulfing']);
    expect(humanizePatternName('bullish_engulfing')).toBe('Bullish Engulfing');
  });

  it('humanizePatternName_falls_back_to_title_case_for_unknown_keys', () => {
    expect(humanizePatternName('some_made_up_pattern')).toBe('Some Made Up Pattern');
  });
});
