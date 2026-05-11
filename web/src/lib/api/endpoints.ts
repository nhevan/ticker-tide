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
import type { DateRange, LlmResponse, MeResponse, Snapshot } from './types';

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
