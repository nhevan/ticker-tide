/**
 * Root application component with client-side routing.
 *
 * Routes:
 *   /login    → LoginPage (public)
 *   /         → TickerDetailPage (protected by RequireAuth)
 *   /tickers  → TickersPage (protected by RequireAuth)
 *   *         → Navigate to /
 */

import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { LoginPage } from '@/pages/LoginPage';
import { TickerDetailPage } from '@/pages/TickerDetailPage';
import { TickersPage } from '@/pages/TickersPage';
import { ModelPage } from '@/pages/ModelPage';
import { RequireAuth } from '@/components/RequireAuth';

/**
 * Render the top-level route tree.
 */
export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/"
        element={
          <RequireAuth>
            <TickerDetailPage />
          </RequireAuth>
        }
      />
      <Route
        path="/tickers"
        element={
          <RequireAuth>
            <TickersPage />
          </RequireAuth>
        }
      />
      <Route
        path="/model"
        element={
          <RequireAuth>
            <ModelPage />
          </RequireAuth>
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
