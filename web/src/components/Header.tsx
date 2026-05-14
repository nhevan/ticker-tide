/**
 * Global application header with brand, primary navigation, and account controls.
 *
 * Page-specific controls (e.g. ticker / date pickers on the Ticker Detail page)
 * live inside the page itself, not here.
 */

import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { logout } from '@/lib/api/endpoints';
import { useQueryClient } from '@tanstack/react-query';
import { ME_QUERY_KEY } from '@/lib/hooks/useMe';

/**
 * Render the sticky global header: brand, nav tabs, theme toggle, sign-out.
 */
export function Header() {
  const queryClient = useQueryClient();
  const { pathname } = useLocation();
  const isTickerDetail = pathname === '/';
  const isTickers = pathname === '/tickers';
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
      <div className="flex flex-wrap items-center gap-4">
        <span className="font-display text-xs tracking-widest text-muted-foreground">
          TICKER·TIDE
        </span>
        <nav className="flex items-center gap-1 text-sm">
          <Link
            to="/"
            className={
              isTickerDetail
                ? 'rounded bg-muted px-2 py-1 font-medium'
                : 'rounded px-2 py-1 text-muted-foreground hover:text-foreground'
            }
            aria-current={isTickerDetail ? 'page' : undefined}
          >
            Ticker Detail
          </Link>
          <Link
            to="/tickers"
            className={
              isTickers
                ? 'rounded bg-muted px-2 py-1 font-medium'
                : 'rounded px-2 py-1 text-muted-foreground hover:text-foreground'
            }
            aria-current={isTickers ? 'page' : undefined}
          >
            Tickers
          </Link>
        </nav>
        <div className="ml-auto flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={toggleTheme}
            title="Toggle theme"
          >
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
