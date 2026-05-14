/**
 * TanStack Query hook for GET /api/price-chart.
 *
 * Fetches OHLCV bars for a candlestick price chart for the given ticker and
 * range. The query is disabled when ticker is empty. Data is considered fresh
 * for 5 minutes — bar data is static for past dates and changes only once per
 * trading day for the most recent bar.
 */

import { useQuery } from '@tanstack/react-query';
import { fetchPriceChart } from '@/lib/api/endpoints';
import type { PriceChartPayload, PriceRange } from '@/lib/api/types';

/**
 * Return OHLCV bars for the candlestick price chart.
 *
 * The query is enabled only when ticker is non-empty. Data is re-fetched
 * automatically after 5 minutes (staleTime: 5 * 60 * 1000).
 *
 * @param ticker - Ticker symbol (e.g. "AAPL"). Query is disabled when empty.
 * @param range  - Price range key; one of "1M" | "3M" | "6M" | "1Y" | "ALL".
 */
export function usePriceChart(ticker: string, range: PriceRange) {
  return useQuery<PriceChartPayload>({
    queryKey: ['price-chart', ticker, range],
    queryFn: () => fetchPriceChart(ticker, range),
    enabled: !!ticker,
    staleTime: 5 * 60 * 1000,
  });
}
