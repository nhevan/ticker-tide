/**
 * Route guard: renders children only when authenticated.
 *
 * On 401 (useMe returns UnauthorizedError), redirects to /login.
 * While the /api/me request is in-flight, renders a loading indicator.
 */

import React from 'react';
import { Navigate } from 'react-router-dom';
import { useMe } from '@/lib/hooks/useMe';
import { UnauthorizedError } from '@/lib/api/client';

interface RequireAuthProps {
  /** Protected content to render when authenticated. */
  children: React.ReactNode;
}

/**
 * Wrap a route element with authentication enforcement.
 *
 * Calls GET /api/me via useMe(). On 401, navigates to /login.
 * On success, renders children. While loading, shows a spinner.
 *
 * @param children - The protected page component.
 */
export function RequireAuth({ children }: RequireAuthProps) {
  const { data, error, isLoading } = useMe();

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <span className="text-sm text-muted-foreground">Loading…</span>
      </div>
    );
  }

  if (error instanceof UnauthorizedError || !data?.authenticated) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}
