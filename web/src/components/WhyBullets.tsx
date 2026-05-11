/**
 * "Why" bullet list — top key_signals from the daily snapshot.
 */

import React from 'react';

interface WhyBulletsProps {
  /** Array of key signal description strings from the daily snapshot. */
  keySignals: string[] | undefined;
}

/**
 * Render the "Why" section with key signal bullets.
 *
 * Renders nothing when keySignals is empty or undefined.
 *
 * @param keySignals - Top signal descriptions from scores_daily.key_signals.
 */
export function WhyBullets({ keySignals }: WhyBulletsProps) {
  if (!keySignals || keySignals.length === 0) return null;

  return (
    <div className="mt-2">
      <h4 className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Why
      </h4>
      <ul className="space-y-0.5 text-xs">
        {keySignals.map((signal, index) => (
          <li key={index} className="flex items-start gap-1.5">
            <span className="mt-0.5 text-muted-foreground">•</span>
            <span>{signal}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
