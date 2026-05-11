/**
 * Date input with optional min/max bounds.
 */

import React from 'react';
import { Input } from '@/components/ui/input';

interface DatePickerProps {
  /** Current date value (YYYY-MM-DD). */
  value: string;
  /** Fired when the date value changes. */
  onChange: (date: string) => void;
  /** Optional minimum selectable date (YYYY-MM-DD). */
  min?: string | null;
  /** Optional maximum selectable date (YYYY-MM-DD). */
  max?: string | null;
}

/**
 * Render a date input with optional min/max constraints.
 *
 * @param value - Controlled date value.
 * @param onChange - Called with the new date string.
 * @param min - Minimum selectable date.
 * @param max - Maximum selectable date.
 */
export function DatePicker({ value, onChange, min, max }: DatePickerProps) {
  return (
    <div className="flex flex-col gap-1">
      <label htmlFor="date-input" className="text-xs font-medium text-muted-foreground">
        Date
      </label>
      <Input
        id="date-input"
        type="date"
        value={value}
        min={min ?? undefined}
        max={max ?? undefined}
        onChange={(e) => onChange(e.target.value)}
        className="w-40"
      />
    </div>
  );
}
