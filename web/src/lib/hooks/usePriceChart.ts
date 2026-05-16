/**
 * TanStack Query hook for GET /api/price-chart.
 *
 * Always fetches the full ALL range for the given ticker; the frontend then
 * uses the chart's visible time window to display the user-selected preset
 * (1M/3M/6M/1Y/All). This keeps zoom-out interactions working without a
 * refetch and makes preset switches instant. The query is disabled when
 * ticker is empty. Data is considered fresh for 5 minutes — bar data is
 * static for past dates and changes only once per trading day for the most
 * recent bar.
 */

import { useQuery } from '@tanstack/react-query';
import { fetchPriceChart } from '@/lib/api/endpoints';
import type { PriceChartPayload } from '@/lib/api/types';

/**
 * Return all available OHLCV bars for the candlestick price chart.
 *
 * The query is enabled only when ticker is non-empty. Data is re-fetched
 * automatically after 5 minutes (staleTime: 5 * 60 * 1000).
 *
 * @param ticker - Ticker symbol (e.g. "AAPL"). Query is disabled when empty.
 */
export function usePriceChart(ticker: string) {
  return useQuery<PriceChartPayload>({
    queryKey: ['price-chart', ticker, 'ALL'],
    queryFn: () => fetchPriceChart(ticker, 'ALL'),
    enabled: !!ticker,
    staleTime: 5 * 60 * 1000,
  });
}
