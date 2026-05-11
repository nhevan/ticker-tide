/**
 * Tests for LoginPage.tsx
 *
 * Verifies: password submission calls login endpoint, navigates on 200,
 * shows error message on 401.
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { LoginPage } from '@/pages/LoginPage';

// Mock the endpoints module
vi.mock('@/lib/api/endpoints', () => ({
  login: vi.fn(),
  logout: vi.fn(),
}));

// Mock react-router-dom navigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual<typeof import('react-router-dom')>('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

import { login } from '@/lib/api/endpoints';
import { UnauthorizedError } from '@/lib/api/client';

function renderLoginPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 0 } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <LoginPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('LoginPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('submitting password calls login endpoint with the entered value', async () => {
    const mockLogin = vi.mocked(login);
    mockLogin.mockResolvedValueOnce(undefined);

    renderLoginPage();

    fireEvent.change(screen.getByPlaceholderText('Enter password'), {
      target: { value: 'testpass' },
    });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => {
      expect(mockLogin).toHaveBeenCalledWith('testpass');
    });
  });

  it('navigates to / on successful login', async () => {
    vi.mocked(login).mockResolvedValueOnce(undefined);

    renderLoginPage();

    fireEvent.change(screen.getByPlaceholderText('Enter password'), {
      target: { value: 'testpass' },
    });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => {
      expect(mockNavigate).toHaveBeenCalledWith('/');
    });
  });

  it('shows error message on 401', async () => {
    vi.mocked(login).mockRejectedValueOnce(new UnauthorizedError('Not authenticated.'));

    renderLoginPage();

    fireEvent.change(screen.getByPlaceholderText('Enter password'), {
      target: { value: 'wrongpass' },
    });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeInTheDocument();
      expect(screen.getByText(/invalid password/i)).toBeInTheDocument();
    });
  });

  it('does not navigate on failed login', async () => {
    vi.mocked(login).mockRejectedValueOnce(new UnauthorizedError('Not authenticated.'));

    renderLoginPage();

    fireEvent.change(screen.getByPlaceholderText('Enter password'), {
      target: { value: 'wrongpass' },
    });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => {
      expect(mockNavigate).not.toHaveBeenCalled();
    });
  });
});
