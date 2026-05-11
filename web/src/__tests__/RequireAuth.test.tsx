/**
 * Tests for RequireAuth.tsx
 *
 * Mocks useMe to verify: renders children when authenticated,
 * redirects to /login when 401.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RequireAuth } from '@/components/RequireAuth';
import { UnauthorizedError } from '@/lib/api/client';

vi.mock('@/lib/hooks/useMe', () => ({
  useMe: vi.fn(),
  ME_QUERY_KEY: ['me'],
}));

import { useMe } from '@/lib/hooks/useMe';

function renderWithAuth(authenticated: boolean | 'loading' | 'error') {
  const mockUseMe = vi.mocked(useMe);

  if (authenticated === 'loading') {
    mockUseMe.mockReturnValue({ data: undefined, error: null, isLoading: true } as unknown as ReturnType<typeof useMe>);
  } else if (authenticated === 'error') {
    mockUseMe.mockReturnValue({
      data: undefined,
      error: new UnauthorizedError(),
      isLoading: false,
    } as unknown as ReturnType<typeof useMe>);
  } else if (authenticated) {
    mockUseMe.mockReturnValue({
      data: { authenticated: true },
      error: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useMe>);
  } else {
    mockUseMe.mockReturnValue({
      data: { authenticated: false },
      error: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useMe>);
  }

  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 0 } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/protected']}>
        <Routes>
          <Route
            path="/protected"
            element={
              <RequireAuth>
                <div>Protected Content</div>
              </RequireAuth>
            }
          />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('RequireAuth', () => {
  it('renders children when authenticated', () => {
    renderWithAuth(true);
    expect(screen.getByText('Protected Content')).toBeInTheDocument();
  });

  it('redirects to /login on UnauthorizedError', () => {
    renderWithAuth('error');
    expect(screen.getByText('Login Page')).toBeInTheDocument();
    expect(screen.queryByText('Protected Content')).not.toBeInTheDocument();
  });

  it('redirects to /login when authenticated is false', () => {
    renderWithAuth(false);
    expect(screen.getByText('Login Page')).toBeInTheDocument();
  });

  it('shows loading state while fetching', () => {
    renderWithAuth('loading');
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
    expect(screen.queryByText('Protected Content')).not.toBeInTheDocument();
  });
});
