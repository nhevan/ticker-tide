"""
Per-indicator and per-pattern contribution builder for the /why Telegram command.

Computes how much each indicator and pattern contributed to the final composite
score by mirroring the magnitude-weighted averaging math in
``src/scorer/category_scorer.rollup_category`` and the adaptive-weight
application in ``apply_adaptive_weights``.

Scope: indicators + patterns only. Sentiment, fundamental, and macro scalars
are NOT decomposed here and will not appear in the returned ``items`` list.
"""

from __future__ import annotations

import logging

from src.scorer.category_scorer import INDICATOR_CATEGORY_MAP, PATTERN_CATEGORY_MAP

logger = logging.getLogger(__name__)


def build_contributions_payload(
    indicator_scores: dict[str, float | None],
    pattern_scores: dict[str, float | None],
    regime_weights: dict[str, float],
    expansion_factor: float,
) -> dict:
    """
    Build the per-indicator + per-pattern contribution payload for /why.

    Approximate decomposition — clamping at ±100, expansion_factor, and
    post-rollup sector adjustment cause the sum of contributions to diverge
    from the final composite score.

    Currently covers indicator + pattern contributions only. Sentiment,
    fundamental, and macro scalars are NOT decomposed and will not appear
    in the returned items list.

    The math per item mirrors ``rollup_category`` exactly:
      - Group non-None scores by category.
      - For category ``c``: ``category_abs_sum = sum(abs(s) for non-None s in c)``.
      - If ``category_abs_sum == 0``: ``contribution = 0.0`` (no division by zero).
      - Else: ``contribution = (s * abs(s) / category_abs_sum) * regime_weight[c]
                               * expansion_factor``.

    Convention for ``raw_value``:
      - **Indicators**: ``raw_value = score``. The scored value (−100 to +100)
        is stored because this function has no access to the original raw
        measurement (e.g. actual RSI reading). A future version may pass raw
        indicators through if available.
      - **Patterns**: ``raw_value = None``. Patterns don't have a single
        numeric measurement that is meaningful outside their detection context.

    Parameters:
        indicator_scores: Dict from ``score_all_indicators()`` mapping indicator
                          names (e.g. ``"rsi_14"``) to scores or ``None``.
        pattern_scores: Dict mapping pattern names (e.g.
                        ``"candlestick_pattern_score"``) to scores or ``None``.
        regime_weights: Dict mapping category names to float weights, as
                        returned by ``get_regime_weights()``.
        expansion_factor: Multiplier applied to the weighted category score
                          before the final composite clamp. Loaded from
                          ``config['scoring']['score_expansion_factor']``.

    Returns:
        Dict with shape::

            {
                "v": 1,
                "expansion_factor": float,   # echoed so consumers can show
                                             # the full math chain
                "items": [
                    {
                        "name": str,
                        "kind": "indicator" | "pattern",
                        "raw_value": float | None,
                        "score": float,
                        "category": str,
                        "category_weight": float,
                        "contribution": float,
                    },
                    ...
                ]
            }

        Items are sorted by ``abs(contribution)`` descending.
    """
    # Collect all non-None scored entries grouped by category.
    # Structure: { category: [(name, score, kind)] }
    category_entries: dict[str, list[tuple[str, float, str]]] = {}

    for name, score in indicator_scores.items():
        category = INDICATOR_CATEGORY_MAP.get(name)
        if category is None:
            # Not a known indicator — skip silently.
            continue
        if score is None:
            continue
        category_entries.setdefault(category, []).append((name, score, "indicator"))

    for name, score in pattern_scores.items():
        category = PATTERN_CATEGORY_MAP.get(name)
        if category is None:
            # Not a known pattern — skip silently.
            continue
        if score is None:
            continue
        category_entries.setdefault(category, []).append((name, score, "pattern"))

    # Build items with contribution values, one category at a time.
    items: list[dict] = []
    for category, entries in category_entries.items():
        regime_weight = regime_weights.get(category)
        if regime_weight is None:
            logger.warning(
                f"build_contributions_payload: category '{category}' not found in "
                f"regime_weights — skipping {len(entries)} item(s)"
            )
            continue
        items.extend(_build_category_items(category, entries, regime_weight, expansion_factor))

    # Sort by |contribution| descending so the biggest drivers appear first.
    items.sort(key=lambda item: abs(item["contribution"]), reverse=True)

    return {"v": 1, "expansion_factor": float(expansion_factor), "items": items}


def _build_category_items(
    category: str,
    entries: list[tuple[str, float, str]],
    regime_weight: float,
    expansion_factor: float,
) -> list[dict]:
    """
    Compute contribution items for a single category.

    Mirrors ``rollup_category``'s magnitude-weighted decomposition: each
    entry's effective weight inside its category is ``abs(score)/abs_sum``,
    and the per-item contribution is that share scaled by the category's
    regime weight and the expansion factor.

    Parameters:
        category: Category name (e.g., ``"momentum"``).
        entries: List of ``(name, score, kind)`` tuples for non-None scores
                 already grouped under this category.
        regime_weight: The category's adaptive weight from ``get_regime_weights``.
        expansion_factor: Multiplier applied after regime weighting.

    Returns:
        List of item dicts with the full contribution payload schema.
    """
    category_abs_sum = sum(abs(score) for _, score, _ in entries)
    items: list[dict] = []
    for name, score, kind in entries:
        if category_abs_sum == 0.0:
            contribution = 0.0
        else:
            contribution = (
                (score * abs(score) / category_abs_sum) * regime_weight * expansion_factor
            )
        raw_value = score if kind == "indicator" else None
        items.append(
            {
                "name": name,
                "kind": kind,
                "raw_value": raw_value,
                "score": score,
                "category": category,
                "category_weight": regime_weight,
                "contribution": float(contribution),
            }
        )
    return items
