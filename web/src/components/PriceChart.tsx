/**
 * Candlestick price chart for the Ticker Detail page.
 *
 * Renders a daily OHLCV candlestick chart with a volume sub-pane via the
 * lightweight-charts library. Range presets (1M / 3M / 6M / 1Y / All) drive
 * a backend fetch through usePriceChart. The chart re-renders on theme
 * toggles and resizes responsively with its container.
 */

import { useEffect, useRef, useState } from 'react';
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from 'lightweight-charts';
import { usePriceChart } from '@/lib/hooks/usePriceChart';
import type { PriceBar, PriceRange } from '@/lib/api/types';
import { Skeleton } from '@/components/ui/skeleton';
import { ErrorBanner } from '@/components/ErrorBanner';

// UI default for the visible window on first render. Not a scoring threshold —
// kept as a named constant rather than fetched from config.
const DEFAULT_RANGE: PriceRange = '6M';

const RANGE_OPTIONS: PriceRange[] = ['1M', '3M', '6M', '1Y', 'ALL'];
const RANGE_LABELS: Record<PriceRange, string> = {
  '1M': '1M',
  '3M': '3M',
  '6M': '6M',
  '1Y': '1Y',
  ALL: 'All',
};

// Calendar-day windows for each preset. Distinct from the backend
// `price_chart.range_days` block, which counts trading days for fetch limits.
// The chart always receives the full ALL fetch; these values only drive the
// visible time window via timeScale().setVisibleRange().
const RANGE_CALENDAR_DAYS: Record<Exclude<PriceRange, 'ALL'>, number> = {
  '1M': 30,
  '3M': 90,
  '6M': 180,
  '1Y': 365,
};

const CHART_HEIGHT_PX = 240;
// Top fraction of the price scale reserved for candles; bottom 1−SPLIT is the volume overlay.
const VOLUME_SPLIT = 0.79;

const UP_COLOR = '#26a69a';
const DOWN_COLOR = '#ef5350';
const VOLUME_UP_RGBA = 'rgba(38, 166, 154, 0.5)';
const VOLUME_DOWN_RGBA = 'rgba(239, 83, 80, 0.5)';

interface PriceChartProps {
  ticker: string;
}

/**
 * Transform API bars into lightweight-charts series inputs.
 *
 * Volume bars are coloured to match candle direction. Doji bars
 * (close === open) are treated as up (green) — same convention as the
 * mockup phase.
 */
function toSeriesData(bars: PriceBar[]): {
  candles: CandlestickData<Time>[];
  volumes: HistogramData<Time>[];
} {
  const candles: CandlestickData<Time>[] = [];
  const volumes: HistogramData<Time>[] = [];
  for (const bar of bars) {
    const time = bar.date as Time;
    candles.push({
      time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
    });
    const up = bar.close >= bar.open;
    volumes.push({
      time,
      value: bar.volume,
      color: up ? VOLUME_UP_RGBA : VOLUME_DOWN_RGBA,
    });
  }
  return { candles, volumes };
}

/**
 * Apply the visible time window for a preset to the chart.
 *
 * Anchors `to` at the last bar's date (string `YYYY-MM-DD`) — not `Date.now()`
 * — so the window stays correct when the latest bar is in the past (weekends,
 * holidays, stale data). For `ALL`, fits all loaded content.
 */
function applyVisibleWindow(
  chart: IChartApi,
  rangeKey: PriceRange,
  lastBarDate: string,
): void {
  if (rangeKey === 'ALL') {
    chart.timeScale().fitContent();
    return;
  }
  const calendarDays = RANGE_CALENDAR_DAYS[rangeKey];
  const toDate = new Date(`${lastBarDate}T00:00:00Z`);
  const fromDate = new Date(toDate.getTime() - calendarDays * 24 * 60 * 60 * 1000);
  const fromString = fromDate.toISOString().slice(0, 10);
  chart.timeScale().setVisibleRange({
    from: fromString as Time,
    to: lastBarDate as Time,
  });
}

export function PriceChart({ ticker }: PriceChartProps) {
  const [range, setRange] = useState<PriceRange>(DEFAULT_RANGE);
  const { data, isLoading, error } = usePriceChart(ticker);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  // Tracks which range we have already applied a visible window for, so React
  // Query background refetches do not snap the user's pan/zoom back to default.
  const lastAppliedRangeRef = useRef<PriceRange | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createChart(container, {
      width: container.clientWidth,
      height: CHART_HEIGHT_PX,
      layout: {
        background: { color: 'transparent' },
        textColor: 'rgba(120,120,135,0.9)',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: 'rgba(120,120,135,0.10)' },
        horzLines: { color: 'rgba(120,120,135,0.10)' },
      },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, timeVisible: false },
      crosshair: { mode: 1 },
    });
    chartRef.current = chart;

    const candle = chart.addSeries(CandlestickSeries, {
      upColor: UP_COLOR,
      downColor: DOWN_COLOR,
      borderUpColor: UP_COLOR,
      borderDownColor: DOWN_COLOR,
      wickUpColor: UP_COLOR,
      wickDownColor: DOWN_COLOR,
      wickVisible: true,
      borderVisible: true,
    });
    candleSeriesRef.current = candle;

    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    });
    volume.priceScale().applyOptions({
      scaleMargins: { top: VOLUME_SPLIT, bottom: 0 },
    });
    candle.priceScale().applyOptions({
      scaleMargins: { top: 0.05, bottom: 1 - VOLUME_SPLIT + 0.02 },
    });
    volumeSeriesRef.current = volume;

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        chart.applyOptions({ width: Math.floor(entry.contentRect.width) });
      }
    });
    resizeObserver.observe(container);
    resizeObserverRef.current = resizeObserver;

    return () => {
      resizeObserverRef.current?.disconnect();
      resizeObserverRef.current = null;
      chartRef.current?.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  // Reset the applied-window tracker when the ticker changes, so the next data
  // load re-applies the current preset for the new ticker's bars.
  useEffect(() => {
    lastAppliedRangeRef.current = null;
  }, [ticker]);

  useEffect(() => {
    const bars = data?.bars;
    const candle = candleSeriesRef.current;
    const volume = volumeSeriesRef.current;
    const chart = chartRef.current;
    if (!candle || !volume || !chart) return;
    if (!bars || bars.length === 0) {
      candle.setData([]);
      volume.setData([]);
      return;
    }
    const { candles, volumes } = toSeriesData(bars);
    candle.setData(candles);
    volume.setData(volumes);
    if (lastAppliedRangeRef.current !== range) {
      const lastBarDate = bars[bars.length - 1].date;
      applyVisibleWindow(chart, range, lastBarDate);
      lastAppliedRangeRef.current = range;
    }
  }, [data, range]);

  const errorMessage =
    error instanceof Error ? error.message : error ? String(error) : null;
  const showEmpty =
    !isLoading && !errorMessage && data !== undefined && data.bars.length === 0;

  return (
    <div className="rounded-lg border border-border/60 bg-card p-3">
      <div className="mb-2 flex items-center justify-between">
        <span className="text-[11px] text-muted-foreground">
          {ticker || '—'} · Price ({RANGE_LABELS[range]})
        </span>
        <div className="flex items-center gap-1" role="group" aria-label="Price chart range">
          {RANGE_OPTIONS.map((option) => (
            <button
              key={option}
              type="button"
              onClick={() => setRange(option)}
              aria-pressed={option === range}
              aria-label={`Show ${RANGE_LABELS[option]} range`}
              className={`rounded px-2 py-0.5 text-[11px] font-medium tabular-nums transition-colors ${
                option === range
                  ? 'bg-foreground text-background'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              {RANGE_LABELS[option]}
            </button>
          ))}
        </div>
      </div>
      {errorMessage && <ErrorBanner message={errorMessage} />}
      {!errorMessage && (
        <div className="relative" style={{ height: CHART_HEIGHT_PX }}>
          <div
            ref={containerRef}
            style={{ position: 'absolute', inset: 0, width: '100%', height: '100%' }}
          />
          {isLoading && (
            <Skeleton className="absolute inset-0" />
          )}
        </div>
      )}
      {showEmpty && (
        <p className="mt-2 text-[11px] text-muted-foreground">
          No price data available for this ticker.
        </p>
      )}
    </div>
  );
}
