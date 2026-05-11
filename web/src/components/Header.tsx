/**
 * Application header with ticker/date pickers, Load button, and Sign out.
 */

import React from 'react';
import { Button } from '@/components/ui/button';
import { TickerPicker } from '@/components/TickerPicker';
import { DatePicker } from '@/components/DatePicker';
import { logout } from '@/lib/api/endpoints';
import { useQueryClient } from '@tanstack/react-query';
import { ME_QUERY_KEY } from '@/lib/hooks/useMe';

interface HeaderProps {
  /** Current ticker value. */
  ticker: string;
  /** Called when ticker changes. */
  onTickerChange: (ticker: string) => void;
  /** Current date value. */
  date: string;
  /** Called when date changes. */
  onDateChange: (date: string) => void;
  /** Called when Load button is clicked. */
  onLoad: () => void;
  /** Whether the load is in progress. */
  isLoading: boolean;
  /** Available tickers for autocomplete. */
  tickers: string[];
  /** Optional minimum date constraint. */
  minDate?: string | null;
  /** Optional maximum date constraint. */
  maxDate?: string | null;
}

/**
 * Render the sticky app header with controls and sign-out button.
 *
 * @param props - Header control props.
 */
export function Header({
  ticker,
  onTickerChange,
  date,
  onDateChange,
  onLoad,
  isLoading,
  tickers,
  minDate,
  maxDate,
}: HeaderProps) {
  const queryClient = useQueryClient();
  const [isDark, setIsDark] = React.useState(
    () => document.documentElement.classList.contains('dark'),
  );

  function toggleTheme() {
    const next = !isDark;
    setIsDark(next);
    document.documentElement.classList.toggle('dark', next);
    localStorage.setItem('theme', next ? 'dark' : 'light');
  }

  async function handleSignOut() {
    await logout();
    queryClient.invalidateQueries({ queryKey: ME_QUERY_KEY });
  }

  return (
    <header className="sticky top-0 z-10 border-b bg-background px-4 py-3">
      <div className="flex flex-wrap items-end gap-3">
        <span className="font-display text-xs tracking-widest text-muted-foreground">TICKER·TIDE</span>
        <TickerPicker value={ticker} onChange={onTickerChange} tickers={tickers} />
        <DatePicker value={date} onChange={onDateChange} min={minDate} max={maxDate} />
        <Button
          onClick={onLoad}
          disabled={isLoading || !ticker || !date}
          size="sm"
        >
          {isLoading ? 'Loading…' : 'Load'}
        </Button>
        <div className="ml-auto flex items-center gap-2">
          <Button variant="ghost" size="sm" onClick={toggleTheme} title="Toggle theme">
            {isDark ? 'Light' : 'Dark'}
          </Button>
          <Button variant="ghost" size="sm" onClick={handleSignOut}>
            Sign out
          </Button>
        </div>
      </div>
    </header>
  );
}
