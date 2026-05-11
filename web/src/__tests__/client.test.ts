/**
 * Tests for src/lib/api/client.ts
 *
 * Verifies that apiFetch sends credentials: "include", and that it throws
 * the correct typed errors on 401 and other non-2xx responses.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { apiFetch, UnauthorizedError, ApiError } from '@/lib/api/client';

describe('apiFetch', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sets credentials: include on every request', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ authenticated: true }),
    });
    vi.stubGlobal('fetch', mockFetch);

    await apiFetch('/api/me');

    expect(mockFetch).toHaveBeenCalledOnce();
    const [, init] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect(init.credentials).toBe('include');
  });

  it('throws UnauthorizedError on 401', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        json: async () => ({ detail: 'Not authenticated.' }),
      }),
    );

    await expect(apiFetch('/api/me')).rejects.toThrow(UnauthorizedError);
  });

  it('throws ApiError on 404', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        json: async () => ({ detail: 'Not found.' }),
        statusText: 'Not Found',
      }),
    );

    await expect(apiFetch('/api/snapshot?ticker=ZZZZ&date=2026-01-01')).rejects.toThrow(ApiError);
  });

  it('returns parsed JSON on 200', async () => {
    const expected = { authenticated: true };
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: async () => expected,
      }),
    );

    const result = await apiFetch<{ authenticated: boolean }>('/api/me');
    expect(result).toEqual(expected);
  });

  it('UnauthorizedError has the detail message from the response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        json: async () => ({ detail: 'Not authenticated.' }),
      }),
    );

    try {
      await apiFetch('/api/me');
      expect.fail('Expected UnauthorizedError to be thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(UnauthorizedError);
      expect((err as UnauthorizedError).message).toBe('Not authenticated.');
    }
  });

  it('ApiError exposes status and detail', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 503,
        json: async () => ({ detail: 'Frontend not built.' }),
        statusText: 'Service Unavailable',
      }),
    );

    try {
      await apiFetch('/some-path');
      expect.fail('Expected ApiError to be thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      expect((err as ApiError).status).toBe(503);
      expect((err as ApiError).detail).toBe('Frontend not built.');
    }
  });
});
