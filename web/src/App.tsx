/**
 * Root application component with client-side routing.
 *
 * Routes:
 *   /login  → LoginPage (public)
 *   /       → DashboardPage (protected by RequireAuth)
 *   *       → Navigate to /
 */

import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { LoginPage } from '@/pages/LoginPage';
import { DashboardPage } from '@/pages/DashboardPage';
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
            <DashboardPage />
          </RequireAuth>
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
