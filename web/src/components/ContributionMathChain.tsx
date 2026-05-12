/**
 * ContributionMathChain — final step of the RSI explainer trace.
 *
 * Renders the 4-factor math chain that combines:
 *   indicator score × magnitude share × regime weight × expansion factor
 * to produce the persisted contribution value. The chain shows the formula
 * symbolically, then with values substituted, then simplified, then the
 * final persisted result. Below the chain, a "Where this goes" footnote
 * lists the downstream consumers of the composite score this contribution
 * feeds into.
 *
 * IMPORTANT MATH NOTE:
 *   The Python scorer uses `score × |score| / Σ|score|`, NOT `score × score / Σ|score|`.
 *   For negative scores these differ in sign. The component implements the
 *   correct `score * Math.abs(score) / denom` formula.
 *
 * IMPORTANT SOURCE-OF-TRUTH NOTE:
 *   The "final" row displays `finalContribution` (the persisted value from
 *   contributions_payload), NOT the client-side recomputed product. The
 *   intermediate rows show the math; the final row is the system's answer.
 *   In normal operation they match to displayed precision; any drift comes
 *   from float precision or the rare edge cases the approximation_caveat
 *   describes.
 *
 * Renders fallback prose ("Approximately X points.") instead of the chain
 * when any required input is non-finite or denom is 0 — never returns null,
 * never leaves the step-7 card visually empty.
 *
 * @param score - Indicator score, from snapshot.daily.indicator_scores.
 * @param denom - Σ|score| over the indicator's category. Caller-computed.
 * @param regimeWeight - Category's regime weight from item.category_weight.
 * @param expansion - Expansion factor from contributions_payload.expansion_factor.
 * @param finalContribution - Persisted contribution from item.contribution.
 *                            Displayed as the "final" row's source of truth.
 * @param activeName - Indicator name for the headline (e.g. 'rsi_14').
 * @returns The math chain table + downstream footnote, or fallback prose.
 */
import type { ReactNode } from 'react';

interface ContributionMathChainProps {
  score: number;
  denom: number;
  regimeWeight: number;
  expansion: number;
  finalContribution: number;
  activeName: string;
}

export function ContributionMathChain({
  score,
  denom,
  regimeWeight,
  expansion,
  finalContribution,
  activeName,
}: ContributionMathChainProps): ReactNode {
  // Defensive guards: any non-finite input or denom===0 → fallback prose.
  // Never return null; the caller relies on this component to always render
  // something so step 7 is never visually empty.
  const inputsValid =
    Number.isFinite(score) &&
    Number.isFinite(denom) &&
    Number.isFinite(regimeWeight) &&
    Number.isFinite(expansion) &&
    Number.isFinite(finalContribution) &&
    denom !== 0;

  if (!inputsValid) {
    const value = Number.isFinite(finalContribution)
      ? `${finalContribution > 0 ? '+' : ''}${finalContribution.toFixed(2)} points`
      : '—';
    return (
      <p>
        Approximately <span className="font-medium">{value}</span>.
      </p>
    );
  }

  // CORRECT formula: score × |score| / denom, NOT score × score / denom.
  // For negative scores these differ in sign — the scorer uses the former.
  const share = Math.abs(score) / denom;
  const afterShare = score * share; // = score × |score| / denom
  const afterWeight = afterShare * regimeWeight;
  // Intentionally do NOT use this for the "final" display — it is shown only
  // in the intermediate rows. The final row uses finalContribution (persisted).
  const recomputed = afterWeight * expansion;
  void recomputed; // surface unused for future debugging assertions if needed

  const sgnFixed = (n: number, digits = 2) =>
    n > 0 ? `+${n.toFixed(digits)}` : n.toFixed(digits);

  return (
    <div>
      <div className="mb-2 text-[10px] text-muted-foreground">
        Computing <span className="font-mono text-foreground">{activeName}</span>'s contribution to the daily composite:
      </div>
      <table className="w-full text-[10px] font-mono">
        <tbody>
          <tr>
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap">contribution</td>
            <td className="text-foreground break-all">
              = score × (|score| ÷ Σ|score|) × regime_weight × expansion_factor
            </td>
          </tr>
          <tr>
            <td className="pr-3 align-top whitespace-nowrap"></td>
            <td className="text-foreground break-all">
              = {sgnFixed(score, 1)} × ({Math.abs(score).toFixed(1)} ÷ {denom.toFixed(1)}) × {regimeWeight.toFixed(2)} × {expansion.toFixed(2)}
            </td>
          </tr>
          <tr>
            <td className="pr-3 align-top whitespace-nowrap"></td>
            <td className="text-foreground break-all">
              = {sgnFixed(score, 1)} × {share.toFixed(3)} × {regimeWeight.toFixed(2)} × {expansion.toFixed(2)}
            </td>
          </tr>
          <tr className="border-t border-border/40">
            <td className="text-muted-foreground pr-3 align-top whitespace-nowrap font-semibold">final (persisted)</td>
            <td className="text-primary font-semibold">{sgnFixed(finalContribution, 2)} points</td>
          </tr>
        </tbody>
      </table>

      <div className="mt-3 pt-2 border-t border-border/40 text-[10px] text-muted-foreground">
        <div className="mb-1 text-foreground font-semibold uppercase tracking-wider text-[9px]">
          Where this goes
        </div>
        <ul className="space-y-0.5 list-disc list-inside">
          <li>
            <span className="text-foreground">Summed</span> with every other category's contribution to produce the <span className="text-foreground">daily composite</span> (range −100 to +100), clamped.
          </li>
          <li>
            The daily composite is <span className="text-foreground">merged</span> with weekly and monthly composites into the <span className="text-foreground">final composite</span> via the timeframe merger.
          </li>
          <li>
            The final composite is <span className="text-foreground">classified</span> as BULLISH / BEARISH / NEUTRAL via ±30 thresholds, driving the dashboard signal pill and ticker tape.
          </li>
          <li>
            Its magnitude becomes the <span className="text-foreground">base confidence</span> (0–100%), then modifiers (timeframe agreement, volume confirmation, earnings proximity, …) nudge it up or down.
          </li>
          <li>
            Stored in <span className="font-mono">scores_daily.final_score</span> for historical analysis and signal-flip detection; thresholded crossings fire Telegram alerts.
          </li>
        </ul>
      </div>
    </div>
  );
}
