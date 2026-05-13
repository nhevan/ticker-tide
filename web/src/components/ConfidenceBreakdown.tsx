/**
 * Confidence breakdown chip row — shows how confidence was computed.
 *
 * Renders one chip per modifier. Chips for fired modifiers are colour-coded
 * (emerald for bonuses, rose for penalties); dormant chips are muted.
 * Clicking a chip toggles an inline "why" explanation below the row.
 */

import { useState, type ReactNode } from 'react';
import { INDICATOR_CATEGORY_MAP, INDICATOR_DISPLAY_LABELS } from '@/lib/scoring/categoryMap';

/** Static metadata for each of the 7 confidence modifiers. */
type ModifierMeta = {
  label: string;
  /** Template string — may reference substitution keys by name in prose. */
  whyTemplate: (args: ModifierWhyArgs) => string;
  /** Optional extra block rendered under the prose (e.g. category breakdown). */
  extra?: (args: ModifierWhyArgs) => ReactNode;
};

type ModifierWhyArgs = {
  value: number;
  dailyScore: number | null;
  weeklyScore: number | null;
  trendScore: number | null;
  volumeScore: number | null;
  earningsDate: string | null;
  scoringDate: string;
  indicatorScores: Record<string, number | null> | null;
};

function categoryComponents(
  category: 'volume' | 'trend',
  indicatorScores: Record<string, number | null> | null,
): Array<{ key: string; label: string; score: number }> {
  if (!indicatorScores) return [];
  return Object.entries(INDICATOR_CATEGORY_MAP)
    .filter(([, cat]) => cat === category)
    .map(([key]) => {
      const raw = indicatorScores[key];
      return Number.isFinite(raw)
        ? { key, label: INDICATOR_DISPLAY_LABELS[key] ?? key, score: raw as number }
        : null;
    })
    .filter((item): item is { key: string; label: string; score: number } => item !== null);
}

function CategoryRollup({
  title,
  components,
  rollup,
}: {
  title: string;
  components: Array<{ key: string; label: string; score: number }>;
  rollup: number | null;
}) {
  if (components.length === 0 || rollup == null || !Number.isFinite(rollup)) return null;
  const absSum = components.reduce((acc, c) => acc + Math.abs(c.score), 0);
  const weightedSum = components.reduce((acc, c) => acc + c.score * Math.abs(c.score), 0);
  if (absSum === 0) {
    return (
      <div className="rounded border bg-muted/30 p-2">
        <div className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-foreground">
          {title} = {signed(rollup)}
        </div>
        <div className="text-[11px] text-muted-foreground">
          All components scored 0 — category rollup defaults to 0.
        </div>
      </div>
    );
  }
  return (
    <div className="rounded border bg-muted/30 p-2">
      <div className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-foreground">
        {title} = {signed(rollup)}
      </div>
      <ul className="mb-1 space-y-0.5 text-[11px]">
        {components.map((c) => (
          <li key={c.key} className="flex justify-between gap-3 font-mono text-foreground">
            <span className="text-muted-foreground">{c.label}</span>
            <span>{signed(c.score)}</span>
          </li>
        ))}
      </ul>
      <div className="font-mono text-[10px] leading-snug text-muted-foreground">
        Σ(score × |score|) / Σ|score| = {weightedSum.toFixed(1)} / {absSum.toFixed(1)} = {(weightedSum / absSum).toFixed(2)}
      </div>
    </div>
  );
}

function daysBetween(fromIso: string, toIso: string): number | null {
  const from = Date.parse(fromIso);
  const to = Date.parse(toIso);
  if (!Number.isFinite(from) || !Number.isFinite(to)) return null;
  return Math.round((to - from) / 86_400_000);
}

function signed(n: number, digits = 1): string {
  return `${n > 0 ? '+' : ''}${n.toFixed(digits)}`;
}

const MODIFIER_META: Record<string, ModifierMeta> = {
  timeframe_agreement: {
    label: 'Timeframe agreement',
    whyTemplate: ({ value, dailyScore, weeklyScore }) => {
      const haveScores = Number.isFinite(dailyScore) && Number.isFinite(weeklyScore);
      if (value > 0 && haveScores) {
        const direction = (dailyScore as number) >= 0 ? 'bullish' : 'bearish';
        return `Looks at whether the short-term (daily) and longer-term (weekly) composite scores point the same way. Today daily is ${signed(dailyScore as number)} and weekly is ${signed(weeklyScore as number)} — both ${direction} and both clearly outside the ±10 "ambiguous" zone. When two independent timeframes tell the same story, the call is more likely to hold up over the next few days, so confidence gets +10.`;
      }
      if (value < 0 && haveScores) {
        return `Compares daily (${signed(dailyScore as number)}) to weekly (${signed(weeklyScore as number)}). They point in opposite directions — one says up, the other says down. Conflicting timeframes usually mean one of them is wrong and we can't tell which, so confidence is penalised by ${value}.`;
      }
      if (haveScores) {
        return `Daily is ${signed(dailyScore as number)} and weekly is ${signed(weeklyScore as number)}. At least one of them sits inside the ±10 "ambiguous" band, so direction isn't clear enough to count as either agreement or disagreement. No effect on confidence.`;
      }
      return `Compares daily vs. weekly composite scores. Same direction & both past ±10 adds +10; opposite directions deducts −15; anything in between is 0. Today this rule didn't move confidence.`;
    },
  },
  volume_confirmation: {
    label: 'Volume confirms',
    whyTemplate: ({ value, volumeScore, trendScore }) => {
      const haveScores = Number.isFinite(volumeScore) && Number.isFinite(trendScore);
      if (value > 0 && haveScores) {
        return `Checks whether volume is flowing in the same direction as the trend. Today volume category is ${signed(volumeScore as number)} and trend category is ${signed(trendScore as number)} — same sign, both past the ±5 "noise" floor. Real buyers/sellers showing up for the move makes it less likely to be a thin-tape head fake, so +10.`;
      }
      if (value < 0 && haveScores) {
        return `Volume (${signed(volumeScore as number)}) and trend (${signed(trendScore as number)}) disagree — price is moving without volume backing it up, or volume is leaking the opposite way. Suggests the move is fragile, so ${value}.`;
      }
      if (haveScores) {
        return `Volume is ${signed(volumeScore as number)}, trend is ${signed(trendScore as number)}. At least one is inside ±5, too small to count as a clear confirmation or divergence. No effect.`;
      }
      return `Looks at volume-category sign vs. trend-category sign. Agreement adds +10; divergence deducts −10. Today this rule didn't move confidence.`;
    },
    extra: ({ volumeScore, trendScore, indicatorScores }) => {
      const volComps = categoryComponents('volume', indicatorScores);
      const trendComps = categoryComponents('trend', indicatorScores);
      if (volComps.length === 0 && trendComps.length === 0) return null;
      return (
        <div className="mt-2 grid gap-2 sm:grid-cols-2">
          <CategoryRollup title="Volume category" components={volComps} rollup={volumeScore} />
          <CategoryRollup title="Trend category" components={trendComps} rollup={trendScore} />
        </div>
      );
    },
  },
  indicator_consensus: {
    label: 'Indicator consensus',
    whyTemplate: ({ value }) => {
      if (value > 0) {
        return `Counts how many of the individual indicators (RSI, MACD, EMAs, A/D line, Stoch, etc.) point the same way as the daily score. More than 60% agree today — broad consensus across independent measurements means the signal isn't riding on one or two outliers. Confidence +5.`;
      }
      if (value < 0) {
        return `Fewer than 50% of the individual indicators agree with the daily direction — mixed picture. The composite is likely being carried by a handful of strong signals while the rest disagree, which is shakier ground. Confidence ${value}.`;
      }
      return `Between 50% and 60% of indicators agree with the daily direction today — neither broad enough to reward nor mixed enough to penalise. No effect.`;
    },
  },
  earnings_proximity: {
    label: 'Earnings proximity',
    whyTemplate: ({ value, earningsDate, scoringDate }) => {
      if (value < 0 && earningsDate) {
        const days = daysBetween(scoringDate, earningsDate);
        const dayPhrase = days != null ? `${days} day${days === 1 ? '' : 's'} away` : 'within the 7-day window';
        return `Next earnings is ${earningsDate} — ${dayPhrase}, inside the 7-day penalty window. Earnings releases can blow up any technical setup because brand-new fundamental information is about to land. Confidence ${value}.`;
      }
      if (earningsDate) {
        const days = daysBetween(scoringDate, earningsDate);
        const dayPhrase = days != null ? `${days} days away` : 'outside the 7-day window';
        return `Next earnings is ${earningsDate} — ${dayPhrase}, comfortably outside the 7-day penalty window. No imminent event risk, so this rule contributes 0.`;
      }
      return `No upcoming earnings on file for this ticker, so there's no event-risk penalty to apply. Rule fires −15 when earnings are within 7 days.`;
    },
  },
  vix_extreme: {
    label: 'VIX extreme',
    whyTemplate: ({ value }) => {
      if (value < 0) {
        return `VIX (the market's "fear gauge") closed above the 30 panic threshold today. When the broader market is stressed, individual stock setups become less reliable — correlations tighten, names move with the index instead of on their own technicals. Confidence ${value}.`;
      }
      return `VIX is below the 30 panic threshold today — the broader market isn't in stress mode, so individual setups can stand on their own technicals. No penalty.`;
    },
  },
  atr_expanding: {
    label: 'ATR expanding',
    whyTemplate: ({ value }) => {
      if (value < 0) {
        return `ATR (Average True Range — typical daily price range) is more than 1.5× its 20-day average. Unusually wide daily swings mean more random noise and a higher chance the signal gets stopped out before it pays off. Confidence ${value}.`;
      }
      return `ATR is within 1.5× its 20-day average — volatility is normal today, no unusual jumpiness. No penalty.`;
    },
  },
  missing_data: {
    label: 'Missing data',
    whyTemplate: ({ value }) => {
      if (value < 0) {
        return `At least one supporting data stream was missing at scoring time. News absence costs −5 (sentiment can't be checked); fundamentals absence costs −3 (no balance-sheet sanity check). Today's total: ${value}.`;
      }
      return `All supporting data streams (news, fundamentals) were available at scoring time — the model had a full picture, no data-gap penalty.`;
    },
  },
};

/** Ordered list of the 7 modifier keys for stable rendering. */
const MODIFIER_KEY_ORDER = [
  'timeframe_agreement',
  'volume_confirmation',
  'indicator_consensus',
  'earnings_proximity',
  'vix_extreme',
  'atr_expanding',
  'missing_data',
] as const;

interface ConfidenceBreakdownProps {
  confidence: number;
  base: number;
  modifiers: Record<string, number>;
  dailyScore: number | null;
  weeklyScore: number | null;
  trendScore: number | null;
  volumeScore: number | null;
  earningsDate: string | null;
  scoringDate: string;
  calibratedScore: number | null;
  indicatorScores: Record<string, number | null> | null;
}

const CALIBRATED_CAP = 8.0;
const CALIBRATED_MULTIPLIER = 10;

/**
 * Render the confidence breakdown below the verdict block.
 *
 * Shows a chip row with one chip per modifier. Clicked chips expand their
 * "why" explanation inline. The summary line shows the confidence equation:
 * base + sum-of-fired-modifiers = final confidence.
 */
export function ConfidenceBreakdown({
  confidence,
  base,
  modifiers,
  dailyScore,
  weeklyScore,
  trendScore,
  volumeScore,
  earningsDate,
  scoringDate,
  calibratedScore,
  indicatorScores,
}: ConfidenceBreakdownProps) {
  const [openKey, setOpenKey] = useState<string | null>(null);

  const chips = MODIFIER_KEY_ORDER.map((key) => {
    const value = modifiers[key] ?? 0;
    const meta = MODIFIER_META[key] ?? { label: key, whyTemplate: () => '' };
    const args: ModifierWhyArgs = {
      value,
      dailyScore,
      weeklyScore,
      trendScore,
      volumeScore,
      earningsDate,
      scoringDate,
      indicatorScores,
    };
    return {
      key,
      label: meta.label,
      value,
      why: meta.whyTemplate(args),
      extra: meta.extra ? meta.extra(args) : null,
      fired: value !== 0,
    };
  });

  const firedBonusSum = chips
    .filter((c) => c.value > 0)
    .reduce((s, c) => s + c.value, 0);
  const firedPenaltySum = chips
    .filter((c) => c.value < 0)
    .reduce((s, c) => s + c.value, 0);

  const active = chips.find((c) => c.key === openKey) ?? null;

  return (
    <div className="mt-4 border-t pt-4">
      <div className="mb-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Confidence breakdown
      </div>
      <div className="mb-2 text-sm">
        <span className="font-semibold">{Number.isFinite(confidence) ? confidence.toFixed(1) : '—'}%</span>
        <span className="text-muted-foreground">
          {' '}= base {Number.isFinite(base) ? base.toFixed(1) : '—'}
          {firedBonusSum > 0 && (
            <span className="text-emerald-700 dark:text-emerald-400"> +{firedBonusSum}</span>
          )}
          {firedPenaltySum < 0 && (
            <span className="text-rose-700 dark:text-rose-400"> {firedPenaltySum}</span>
          )}
        </span>
      </div>
      <div className="flex flex-wrap gap-1.5">
        <BaseChip
          base={base}
          calibratedScore={calibratedScore}
          open={openKey === '__base__'}
          onClick={() =>
            setOpenKey(openKey === '__base__' ? null : '__base__')
          }
        />
        {chips.map((chip) => (
          <ChipButton
            key={chip.key}
            label={chip.label}
            value={chip.value}
            fired={chip.fired}
            open={openKey === chip.key}
            onClick={() =>
              setOpenKey(openKey === chip.key ? null : chip.key)
            }
          />
        ))}
      </div>
      {openKey === '__base__' && (
        <BaseExplanation base={base} calibratedScore={calibratedScore} />
      )}
      {active && (
        <div className="mt-3 space-y-2 rounded border-l-2 border-primary bg-background p-2 text-xs leading-relaxed text-muted-foreground">
          <div>
            <span className="font-medium text-foreground">{active.label}</span>{' '}
            — {active.why}
          </div>
          {active.extra}
        </div>
      )}
    </div>
  );
}

interface BaseChipProps {
  base: number;
  calibratedScore: number | null;
  open: boolean;
  onClick: () => void;
}

function BaseChip({ base, open, onClick }: BaseChipProps) {
  const baseText = Number.isFinite(base) ? base.toFixed(1) : '—';
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border border-sky-300 bg-sky-50 px-2.5 py-0.5 text-xs text-sky-900 transition dark:border-sky-900/60 dark:bg-sky-950/40 dark:text-sky-100 ${
        open ? 'ring-2 ring-primary' : ''
      }`}
    >
      🧮 base {baseText}
    </button>
  );
}

function BaseExplanation({
  base,
  calibratedScore,
}: {
  base: number;
  calibratedScore: number | null;
}) {
  const baseText = Number.isFinite(base) ? base.toFixed(1) : '—';
  if (calibratedScore == null || !Number.isFinite(calibratedScore)) {
    return (
      <div className="mt-3 rounded border-l-2 border-primary bg-background p-2 text-xs leading-relaxed text-muted-foreground">
        <span className="font-medium text-foreground">Base from prediction strength.</span>{' '}
        Calibrated score wasn't available for this row — the base ({baseText}) was derived
        from the raw composite score with a heavy discount, because the raw score has near-zero
        correlation with forward returns.
      </div>
    );
  }
  const calSigned = calibratedScore > 0 ? '+' : '';
  return (
    <div className="mt-3 rounded border-l-2 border-primary bg-background p-2 text-xs leading-relaxed text-muted-foreground">
      <span className="font-medium text-foreground">Base from prediction score.</span>{' '}
      The model's calibrated prediction (
      <span className="font-mono">
        {calSigned}
        {calibratedScore.toFixed(2)}
      </span>
      ) is capped at ±{CALIBRATED_CAP} (because past extreme predictions were less accurate),
      then multiplied by {CALIBRATED_MULTIPLIER} to get a 0–{CALIBRATED_CAP * CALIBRATED_MULTIPLIER}{' '}
      base:{' '}
      <span className="font-mono text-foreground">
        min(|{calibratedScore.toFixed(2)}|, {CALIBRATED_CAP}) × {CALIBRATED_MULTIPLIER} = {baseText}
      </span>
      .
    </div>
  );
}

interface ChipButtonProps {
  label: string;
  value: number;
  fired: boolean;
  open: boolean;
  onClick: () => void;
}

function ChipButton({ label, value, fired, open, onClick }: ChipButtonProps) {
  const tone = !fired
    ? 'border-muted bg-muted/40 text-muted-foreground'
    : value > 0
    ? 'border-emerald-300 bg-emerald-50 text-emerald-900 dark:border-emerald-900/60 dark:bg-emerald-950/40 dark:text-emerald-100'
    : 'border-rose-300 bg-rose-50 text-rose-900 dark:border-rose-900/60 dark:bg-rose-950/40 dark:text-rose-100';
  const sign = value > 0 ? '+' : '';
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-2.5 py-0.5 text-xs transition ${tone} ${open ? 'ring-2 ring-primary' : ''}`}
    >
      {fired ? (value > 0 ? '👍' : '👎') : '·'} {label}{' '}
      {fired ? `${sign}${value}` : '0'}
    </button>
  );
}
