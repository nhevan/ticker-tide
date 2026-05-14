/**
 * Dense, fully-sortable, paginated table of tickers.
 *
 * Every column header is a button: clicking toggles asc/desc on the active
 * column, or switches to that column with direction='asc' otherwise. Strings
 * use locale compare; numbers use numeric compare; nulls always sort last
 * regardless of direction. Signal sorts by bullishness rank
 * (BULLISH < NEUTRAL < BEARISH) so descending puts BULLISH on top.
 *
 * Pagination is client-side: the page-size selector is fixed to the canonical
 * set of options (10..1000); switching size or sort resets to page 1.
 *
 * The component receives already-fetched rows — fetching/error/loading lives
 * in the parent page.
 */

import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import type { Signal, TickerRow } from '@/lib/api/types';

const PAGE_SIZE_OPTIONS = [10, 20, 30, 50, 100, 200, 300, 500, 800, 1000] as const;
const DEFAULT_PAGE_SIZE = 50;

type SortableKey =
  | 'symbol'
  | 'name'
  | 'sector'
  | 'price'
  | 'marketCap'
  | 'signal'
  | 'confidence'
  | 'finalScore'
  | 'dailyScore'
  | 'weeklyScore'
  | 'monthlyScore'
  | 'regime'
  | 'peRatio';

type SortDirection = 'asc' | 'desc';

type ColumnDef = {
  key: SortableKey;
  label: string;
  align: 'left' | 'right';
  title?: string;
};

const COLUMNS: ColumnDef[] = [
  { key: 'symbol', label: 'Symbol', align: 'left' },
  { key: 'name', label: 'Name', align: 'left' },
  { key: 'sector', label: 'Sector', align: 'left' },
  { key: 'price', label: 'Price', align: 'right' },
  { key: 'marketCap', label: 'Mkt Cap', align: 'right' },
  { key: 'signal', label: 'Signal', align: 'left' },
  { key: 'confidence', label: 'Conf%', align: 'right' },
  { key: 'finalScore', label: 'Score', align: 'right' },
  { key: 'dailyScore', label: 'D', align: 'right', title: 'Daily score' },
  { key: 'weeklyScore', label: 'W', align: 'right', title: 'Weekly score' },
  { key: 'monthlyScore', label: 'M', align: 'right', title: 'Monthly score' },
  { key: 'regime', label: 'Regime', align: 'left' },
  { key: 'peRatio', label: 'P/E', align: 'right' },
];

// Custom ordinal for signal sorting — see header docstring.
const SIGNAL_SORT_RANK: Record<Signal, number> = {
  BULLISH: 0,
  NEUTRAL: 1,
  BEARISH: 2,
};

/**
 * Format a market-cap number into a compact string (T/B/M), or '—' if null.
 */
function fmtMarketCap(value: number | null): string {
  if (value == null) return '—';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}T`;
  if (value >= 1e9) return `${(value / 1e9).toFixed(1)}B`;
  if (value >= 1e6) return `${(value / 1e6).toFixed(0)}M`;
  return `${value}`;
}

/**
 * Return Tailwind classes for the signal pill, keyed on the signal value.
 * Null/unknown signals render with the neutral style.
 */
function signalClasses(signal: Signal | null): string {
  if (signal === 'BULLISH') {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-950/40 dark:text-emerald-300 dark:border-emerald-900/60';
  }
  if (signal === 'BEARISH') {
    return 'bg-rose-50 text-rose-700 border-rose-200 dark:bg-rose-950/40 dark:text-rose-300 dark:border-rose-900/60';
  }
  return 'bg-muted text-muted-foreground border-border';
}

/**
 * Return Tailwind classes for a signed numeric score cell. Null is treated
 * as zero for color (muted).
 */
function scoreCellClasses(score: number | null): string {
  if (score == null) return 'text-muted-foreground';
  if (score >= 25) return 'text-emerald-600 dark:text-emerald-400 font-medium';
  if (score <= -25) return 'text-rose-600 dark:text-rose-400 font-medium';
  return 'text-muted-foreground';
}

/**
 * Format a signed numeric score as text, with a leading '+' for positives.
 */
function fmtSignedScore(value: number | null, fractionDigits: number): string {
  if (value == null) return '—';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(fractionDigits)}`;
}

/**
 * Compare two row values for the given column. Nulls always sort last
 * regardless of direction. Returns the asc-direction comparator value; the
 * caller flips the sign for desc.
 */
function compareForColumn(
  rowA: TickerRow,
  rowB: TickerRow,
  column: SortableKey,
): number {
  if (column === 'signal') {
    const a = rowA.signal == null ? Number.POSITIVE_INFINITY : SIGNAL_SORT_RANK[rowA.signal];
    const b = rowB.signal == null ? Number.POSITIVE_INFINITY : SIGNAL_SORT_RANK[rowB.signal];
    if (a === Number.POSITIVE_INFINITY && b === Number.POSITIVE_INFINITY) return 0;
    if (a === Number.POSITIVE_INFINITY) return 1;
    if (b === Number.POSITIVE_INFINITY) return -1;
    return a - b;
  }
  const valueA = rowA[column] as string | number | null;
  const valueB = rowB[column] as string | number | null;
  const aIsNull = valueA === null || valueA === undefined;
  const bIsNull = valueB === null || valueB === undefined;
  if (aIsNull && bIsNull) return 0;
  if (aIsNull) return 1;
  if (bIsNull) return -1;
  if (typeof valueA === 'number' && typeof valueB === 'number') {
    return valueA - valueB;
  }
  return String(valueA).localeCompare(String(valueB));
}

/**
 * Render the dense, sortable, paginated ticker table.
 *
 * @param rows - The rows to display (already fetched by the parent).
 */
export function TickersTable({ rows }: { rows: TickerRow[] }): JSX.Element {
  const [sortColumn, setSortColumn] = useState<SortableKey>('symbol');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);
  const [pageIndex, setPageIndex] = useState<number>(0);

  function handleHeaderClick(column: SortableKey): void {
    if (column === sortColumn) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
    setPageIndex(0);
  }

  const sortedRows = useMemo(() => {
    const copy = [...rows];
    copy.sort((rowA, rowB) => {
      const cmp = compareForColumn(rowA, rowB, sortColumn);
      return sortDirection === 'asc' ? cmp : -cmp;
    });
    return copy;
  }, [rows, sortColumn, sortDirection]);

  const totalRows = sortedRows.length;
  const pageCount = Math.max(1, Math.ceil(totalRows / pageSize));

  useEffect(() => {
    if (pageIndex > pageCount - 1) {
      setPageIndex(pageCount - 1);
    }
  }, [pageIndex, pageCount]);

  const clampedPageIndex = Math.min(pageIndex, pageCount - 1);
  const startIndex = clampedPageIndex * pageSize;
  const endIndex = Math.min(startIndex + pageSize, totalRows);
  const visibleRows = sortedRows.slice(startIndex, endIndex);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="bg-muted/50 text-muted-foreground">
          <tr>
            {COLUMNS.map((col) => {
              const isActive = col.key === sortColumn;
              const indicator = isActive ? (sortDirection === 'asc' ? '▲' : '▼') : '';
              const ariaSort = isActive
                ? sortDirection === 'asc'
                  ? 'ascending'
                  : 'descending'
                : 'none';
              return (
                <th
                  key={col.key}
                  scope="col"
                  aria-sort={ariaSort}
                  className={`px-3 py-2 font-medium ${
                    col.align === 'right' ? 'text-right' : 'text-left'
                  }`}
                  title={col.title}
                >
                  <button
                    type="button"
                    onClick={() => handleHeaderClick(col.key)}
                    className={`inline-flex items-center gap-1 hover:text-foreground ${
                      col.align === 'right' ? 'flex-row-reverse' : ''
                    } ${isActive ? 'text-foreground' : ''}`}
                  >
                    <span>{col.label}</span>
                    {indicator && (
                      <span className="text-[9px] leading-none">{indicator}</span>
                    )}
                  </button>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row) => (
            <tr key={row.symbol} className="border-t hover:bg-muted/30">
              <td className="px-3 py-2 font-mono font-semibold">
                {row.latestDate ? (
                  <Link
                    to={`/?ticker=${encodeURIComponent(row.symbol)}&date=${encodeURIComponent(row.latestDate)}`}
                    className="text-foreground hover:underline"
                    title={`Open ${row.symbol} detail for ${row.latestDate}`}
                  >
                    {row.symbol}
                  </Link>
                ) : (
                  row.symbol
                )}
              </td>
              <td className="px-3 py-2 text-muted-foreground">{row.name ?? '—'}</td>
              <td className="px-3 py-2 text-muted-foreground">{row.sector ?? '—'}</td>
              <td className="px-3 py-2 text-right font-mono">
                {row.price == null ? '—' : `$${row.price.toFixed(2)}`}
              </td>
              <td className="px-3 py-2 text-right font-mono text-muted-foreground">
                {fmtMarketCap(row.marketCap)}
              </td>
              <td className="px-3 py-2">
                <span
                  className={`inline-block rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${signalClasses(
                    row.signal,
                  )}`}
                >
                  {row.signal ?? '—'}
                </span>
              </td>
              <td className="px-3 py-2 text-right font-mono">
                {row.confidence == null ? '—' : row.confidence.toFixed(1)}
              </td>
              <td
                className={`px-3 py-2 text-right font-mono ${scoreCellClasses(
                  row.finalScore,
                )}`}
              >
                {fmtSignedScore(row.finalScore, 1)}
              </td>
              <td
                className={`px-3 py-2 text-right font-mono ${scoreCellClasses(
                  row.dailyScore,
                )}`}
              >
                {fmtSignedScore(row.dailyScore, 0)}
              </td>
              <td
                className={`px-3 py-2 text-right font-mono ${scoreCellClasses(
                  row.weeklyScore,
                )}`}
              >
                {fmtSignedScore(row.weeklyScore, 0)}
              </td>
              <td
                className={`px-3 py-2 text-right font-mono ${scoreCellClasses(
                  row.monthlyScore,
                )}`}
              >
                {fmtSignedScore(row.monthlyScore, 0)}
              </td>
              <td className="px-3 py-2 text-muted-foreground capitalize">
                {row.regime ?? '—'}
              </td>
              <td className="px-3 py-2 text-right font-mono text-muted-foreground">
                {row.peRatio == null ? '—' : row.peRatio.toFixed(1)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="flex flex-wrap items-center justify-between gap-3 border-t px-3 py-2 text-xs text-muted-foreground">
        <div className="flex items-center gap-2">
          <label htmlFor="tickers-page-size" className="text-muted-foreground">
            Rows per page
          </label>
          <select
            id="tickers-page-size"
            value={pageSize}
            onChange={(event) => {
              setPageSize(Number(event.target.value));
              setPageIndex(0);
            }}
            className="rounded border bg-background px-2 py-1 text-xs"
          >
            {PAGE_SIZE_OPTIONS.map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </div>
        <div className="font-mono">
          {totalRows === 0
            ? '0 rows'
            : `${startIndex + 1}–${endIndex} of ${totalRows}`}
        </div>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={() => setPageIndex(0)}
            disabled={clampedPageIndex === 0}
            className="rounded border px-2 py-1 hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="First page"
          >
            «
          </button>
          <button
            type="button"
            onClick={() => setPageIndex((idx) => Math.max(0, idx - 1))}
            disabled={clampedPageIndex === 0}
            className="rounded border px-2 py-1 hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Previous page"
          >
            ‹ Prev
          </button>
          <span className="px-2 font-mono">
            Page {clampedPageIndex + 1} of {pageCount}
          </span>
          <button
            type="button"
            onClick={() =>
              setPageIndex((idx) => Math.min(pageCount - 1, idx + 1))
            }
            disabled={clampedPageIndex >= pageCount - 1}
            className="rounded border px-2 py-1 hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Next page"
          >
            Next ›
          </button>
          <button
            type="button"
            onClick={() => setPageIndex(pageCount - 1)}
            disabled={clampedPageIndex >= pageCount - 1}
            className="rounded border px-2 py-1 hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Last page"
          >
            »
          </button>
        </div>
      </div>
    </div>
  );
}
