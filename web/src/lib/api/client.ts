/**
 * HTTP client utilities for the Ticker Tide API.
 *
 * Wraps fetch with session cookie forwarding and typed error classes.
 * All requests include credentials: "include" so the session cookie is
 * automatically sent on same-origin requests (FastAPI SessionMiddleware).
 */

/** Thrown when the server returns 401 Unauthorized. */
export class UnauthorizedError extends Error {
  constructor(message = 'Not authenticated.') {
    super(message);
    this.name = 'UnauthorizedError';
  }
}

/** Thrown when the server returns any non-2xx response other than 401. */
export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(`API error ${status}: ${detail}`);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
  }
}

/**
 * Fetch a path from the API with session credentials included.
 *
 * Automatically forwards the session cookie via credentials: "include".
 * Throws UnauthorizedError on 401, ApiError on other non-2xx responses.
 *
 * @param path - The API path to fetch (e.g. "/api/snapshot?ticker=AAPL").
 * @param init - Optional fetch init options (method, body, headers, etc.).
 * @returns The parsed JSON response body as type T.
 */
export async function apiFetch<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...init.headers,
    },
  });

  if (response.status === 401) {
    const body = await response.json().catch(() => ({}));
    throw new UnauthorizedError(body?.detail ?? 'Not authenticated.');
  }

  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new ApiError(response.status, body?.detail ?? response.statusText);
  }

  return response.json() as Promise<T>;
}
