"""
Per-indicator, per-pattern, and per-aggregate contribution builder for the
/why Telegram command.

Computes how much each indicator, pattern, and aggregate scalar contributed to
the final composite score by mirroring the magnitude-weighted averaging math in
``src/scorer/category_scorer.rollup_category`` and the adaptive-weight
application in ``apply_adaptive_weights``.

Scope: indicators + patterns + sentiment/fundamental/macro aggregates.
Aggregate scalars (sentiment, fundamental, macro) are decomposed as
single-scalar entries using ``score × weight × expansion`` (no intra-category
share split since the rollup is already a scalar, not a multi-item category).

Note: absence of aggregate items in a payload is a valid backward-compatible
state. Callers that do not pass ``aggregate_scores`` will produce payloads
without aggregate items, and consumers must handle their absence gracefully.
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
    aggregate_scores: dict[str, float | None] | None = None,
) -> dict:
    """
    Build the per-indicator, per-pattern, and per-aggregate contribution payload
    for /why.

    Approximate decomposition — clamping at ±100, expansion_factor, and
    post-rollup sector adjustment cause the sum of contributions to diverge
    from the final composite score.

    Covers indicator + pattern + aggregate (sentiment, fundamental, macro)
    contributions. Aggregate scalars are decomposed as single-scalar entries
    using ``score × weight × expansion`` (no intra-category share split since
    the rollup is already a scalar, not a multi-item category).

    Backward-compatibility note: callers that do not pass ``aggregate_scores``
    will produce payloads without aggregate items. Consumers must handle the
    absence of aggregate items gracefully — it is a valid state for older rows.

    The math per indicator/pattern item mirrors ``rollup_category`` exactly:
      - Group non-None scores by category.
      - For category ``c``: ``category_abs_sum = sum(abs(s) for non-None s in c)``.
      - If ``category_abs_sum == 0``: ``contribution = 0.0`` (no division by zero).
      - Else: ``contribution = (s * abs(s) / category_abs_sum) * regime_weight[c]
                               * expansion_factor``.

    The math per aggregate item:
      - ``contribution = score * regime_weights.get(name, 0.0) * expansion_factor``.
      - None scores are skipped. Zero-weight categories ARE emitted with
        ``contribution = 0.0`` (truthful zero rendering).

    Convention for ``raw_value``:
      - **Indicators**: ``raw_value = score``. The scored value (−100 to +100)
        is stored because this function has no access to the original raw
        measurement (e.g. actual RSI reading). A future version may pass raw
        indicators through if available.
      - **Patterns**: ``raw_value = None``. Patterns don't have a single
        numeric measurement that is meaningful outside their detection context.
      - **Aggregates**: ``raw_value = None``. The aggregate scalar is already
        a pre-computed category score, not a raw measurement.

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
        aggregate_scores: Optional dict mapping aggregate category names
                          (``"sentiment"``, ``"fundamental"``, ``"macro"``) to
                          their pre-computed scalar scores or ``None``. When
                          omitted or empty, no aggregate items are appended —
                          this is a valid backward-compatible state.

    Returns:
        Dict with shape::

            {
                "v": 1,
                "expansion_factor": float,   # echoed so consumers can show
                                             # the full math chain
                "items": [
                    {
                        "name": str,
                        "kind": "indicator" | "pattern" | "aggregate",
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

    # Append aggregate items (sentiment, fundamental, macro) as single-scalar
    # entries. Aggregate names are guaranteed not to collide with indicator or
    # pattern keys (categoryMap is the source of truth — sentiment/fundamental/
    # macro are categories, not indicator keys).
    if aggregate_scores:
        for name, score in aggregate_scores.items():
            if score is None:
                continue
            weight = regime_weights.get(name, 0.0)
            contribution = float(score * weight * expansion_factor)
            items.append(
                {
                    "name": name,
                    "kind": "aggregate",
                    "raw_value": None,
                    "score": score,
                    "category": name,
                    "category_weight": weight,
                    "contribution": contribution,
                }
            )

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
