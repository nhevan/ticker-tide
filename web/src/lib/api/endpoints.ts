/**
 * Typed API endpoint functions for the Ticker Tide backend.
 *
 * Each function wraps apiFetch() and returns a typed Promise. All functions
 * that call authenticated endpoints will throw UnauthorizedError on 401 —
 * callers should handle this (or let RequireAuth handle navigation to /login).
 *
 * askAI() calls POST /api/llm regardless of the function name.
 */

import { apiFetch } from './client';
import { ApiError } from './client';
import type {
  DateRange,
  LlmResponse,
  MeResponse,
  Snapshot,
  ScoringRules,
  TickerListApiRow,
  TickerRow,
  VerdictResponse,
} from './types';

/**
 * Map a snake_case API row to the camelCase shape consumed by the UI.
 */
function toTickerRow(row: TickerListApiRow): TickerRow {
  return {
    symbol: row.symbol,
    name: row.name,
    sector: row.sector,
    marketCap: row.market_cap,
    price: row.price,
    signal: row.signal,
    confidence: row.confidence,
    finalScore: row.final_score,
    regime: row.regime,
    dailyScore: row.daily_score,
    weeklyScore: row.weekly_score,
    monthlyScore: row.monthly_score,
    peRatio: row.pe_ratio,
  };
}

/** Log in with a password. Sets the session cookie on success. */
export async function login(password: string): Promise<void> {
  await apiFetch<{ ok: boolean }>('/api/login', {
    method: 'POST',
    body: JSON.stringify({ password }),
  });
}

/** Log out. Clears the session cookie. */
export async function logout(): Promise<void> {
  await apiFetch<{ ok: boolean }>('/api/logout', { method: 'POST' });
}

/** Return current authentication state. */
export async function getMe(): Promise<MeResponse> {
  return apiFetch<MeResponse>('/api/me');
}

/**
 * Return the full three-card snapshot for a ticker and date.
 *
 * @param ticker - Ticker symbol (e.g. "AAPL").
 * @param date - ISO date string (YYYY-MM-DD).
 */
export async function getSnapshot(ticker: string, date: string): Promise<Snapshot> {
  return apiFetch<Snapshot>(
    `/api/snapshot?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(date)}`,
  );
}

/** Return the list of active ticker symbols. */
export async function getTickers(): Promise<string[]> {
  return apiFetch<string[]>('/api/tickers');
}

/**
 * Return one summary row per active ticker for the Tickers listing page.
 *
 * Calls GET /api/tickers-list. The endpoint returns snake_case rows;
 * this function maps them to the camelCase {@link TickerRow} shape.
 */
export async function getTickersList(): Promise<TickerRow[]> {
  const rows = await apiFetch<TickerListApiRow[]>('/api/tickers-list');
  return rows.map(toTickerRow);
}

/**
 * Return the min/max date range available for a ticker.
 *
 * @param ticker - Ticker symbol (e.g. "AAPL").
 */
export async function getDateRange(ticker: string): Promise<DateRange> {
  return apiFetch<DateRange>(`/api/dates?ticker=${encodeURIComponent(ticker)}`);
}

/** Arguments for the /api/llm endpoint. */
export interface AskAIArgs {
  ticker: string;
  date: string;
  timeframe: 'daily' | 'weekly' | 'monthly';
}

/**
 * Request AI analysis for a ticker/date/timeframe combination.
 *
 * Calls POST /api/llm. Subject to a 60-second per-(session, ticker, date,
 * timeframe) debounce on the server — a second call within the window
 * returns 429 (ApiError with status 429).
 *
 * @param args - Ticker, date, and timeframe to analyze.
 */
export async function askAI(args: AskAIArgs): Promise<LlmResponse> {
  return apiFetch<LlmResponse>('/api/llm', {
    method: 'POST',
    body: JSON.stringify(args),
  });
}

/** Arguments for /api/verdict (both GET and POST). */
export interface VerdictArgs {
  ticker: string;
  date: string;
}

/**
 * Return the cached dashboard verdict for a ticker/date, or null if none.
 *
 * Returns null on 404 (no cached verdict) rather than throwing. Other errors
 * still throw (ApiError or UnauthorizedError).
 *
 * @param args - Ticker and date to look up.
 */
export async function getVerdict(
  args: VerdictArgs,
): Promise<VerdictResponse | null> {
  try {
    return await apiFetch<VerdictResponse>(
      `/api/verdict?ticker=${encodeURIComponent(args.ticker)}&date=${encodeURIComponent(args.date)}`,
    );
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) {
      return null;
    }
    throw err;
  }
}

/**
 * Generate (or fetch cached) dashboard verdict for a ticker and date.
 *
 * Server is idempotent: a cached row is returned without a fresh Claude call.
 *
 * @param args - Ticker and date to generate the verdict for.
 */
export async function generateVerdict(
  args: VerdictArgs,
): Promise<VerdictResponse> {
  return apiFetch<VerdictResponse>('/api/verdict', {
    method: 'POST',
    body: JSON.stringify(args),
  });
}

/**
 * Return static scoring rules and thresholds from the server.
 *
 * Process-static: the response does not change without a server restart.
 * Use with staleTime: Infinity in TanStack Query.
 */
export async function fetchScoringRules(): Promise<ScoringRules> {
  return apiFetch<ScoringRules>('/api/scoring-rules');
}
