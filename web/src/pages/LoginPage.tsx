/**
 * Login page — password input that posts to POST /api/login.
 *
 * On success, navigates to / (TickerDetailPage).
 * On 401, displays an "Invalid password" error message.
 */

import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ErrorBanner } from '@/components/ErrorBanner';
import { login } from '@/lib/api/endpoints';
import { ApiError, UnauthorizedError } from '@/lib/api/client';
import { useQueryClient } from '@tanstack/react-query';
import { ME_QUERY_KEY } from '@/lib/hooks/useMe';

/**
 * Render the login page with a centered password form.
 *
 * Invalidates the /api/me cache on successful login so RequireAuth
 * re-evaluates auth state after the redirect.
 */
export function LoginPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [password, setPassword] = useState('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    setErrorMessage(null);
    setIsSubmitting(true);
    try {
      await login(password);
      queryClient.invalidateQueries({ queryKey: ME_QUERY_KEY });
      navigate('/');
    } catch (err) {
      if (err instanceof UnauthorizedError || (err instanceof ApiError && err.status === 401)) {
        setErrorMessage('Invalid password. Please try again.');
      } else {
        setErrorMessage('Login failed. Please try again.');
      }
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <div className="w-full max-w-sm space-y-6 px-4">
        <div className="text-center">
          <h1 className="text-2xl font-semibold tracking-tight">Ticker Tide</h1>
          <p className="mt-1 text-sm text-muted-foreground">Sign in to continue</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {errorMessage && <ErrorBanner message={errorMessage} />}

          <div className="space-y-2">
            <label htmlFor="password" className="text-sm font-medium">
              Password
            </label>
            <Input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoFocus
              required
              placeholder="Enter password"
            />
          </div>

          <Button type="submit" className="w-full" disabled={isSubmitting}>
            {isSubmitting ? 'Signing in…' : 'Sign in'}
          </Button>
        </form>
      </div>
    </div>
  );
}
