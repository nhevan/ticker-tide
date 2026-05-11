/**
 * Full-width error banner for displaying API or application errors.
 */

import React from 'react';

interface ErrorBannerProps {
  /** Error message to display. */
  message: string;
}

/**
 * Render a red error banner with the provided message.
 *
 * @param message - Human-readable error text.
 */
export function ErrorBanner({ message }: ErrorBannerProps) {
  return (
    <div
      role="alert"
      className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
    >
      {message}
    </div>
  );
}
