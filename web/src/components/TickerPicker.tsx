/**
 * Ticker symbol text input with datalist autocomplete.
 */

import React from 'react';
import { Input } from '@/components/ui/input';

interface TickerPickerProps {
  /** Current ticker value (uppercase). */
  value: string;
  /** Fired when the value changes. */
  onChange: (ticker: string) => void;
  /** Available tickers for autocomplete datalist. */
  tickers: string[];
}

/**
 * Render a ticker input with a datalist for autocomplete suggestions.
 *
 * Normalizes to uppercase on change.
 *
 * @param value - Controlled ticker value.
 * @param onChange - Called with the new uppercase ticker string.
 * @param tickers - List of active tickers for the datalist.
 */
export function TickerPicker({ value, onChange, tickers }: TickerPickerProps) {
  return (
    <div className="flex flex-col gap-1">
      <label htmlFor="ticker-input" className="text-xs font-medium text-muted-foreground">
        Ticker
      </label>
      <Input
        id="ticker-input"
        list="ticker-list"
        value={value}
        onChange={(e) => onChange(e.target.value.toUpperCase())}
        placeholder="e.g. AAPL"
        autoComplete="off"
        spellCheck={false}
        className="w-32"
      />
      <datalist id="ticker-list">
        {tickers.map((ticker) => (
          <option key={ticker} value={ticker} />
        ))}
      </datalist>
    </div>
  );
}
