"""
Tests for src/scorer/contribution.py — build_contributions_payload.

All tests are written BEFORE the implementation (TDD). They will fail with
ImportError initially because the module does not exist yet.
"""

from __future__ import annotations

import pytest

from src.scorer.category_scorer import INDICATOR_CATEGORY_MAP, PATTERN_CATEGORY_MAP
from src.scorer.contribution import build_contributions_payload


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_regime_weights() -> dict:
    """Three-category regime weights with non-uniform values that sum to 1.0."""
    return {
        "trend": 0.4,
        "momentum": 0.35,
        "volume": 0.25,
        # Other categories absent — items in those categories will be skipped.
    }


@pytest.fixture()
def full_regime_weights() -> dict:
    """All 9 categories with equal weights summing to 1.0."""
    weight = round(1.0 / 9, 10)
    return {
        "trend": weight,
        "momentum": weight,
        "volume": weight,
        "volatility": weight,
        "candlestick": weight,
        "structural": weight,
        "sentiment": weight,
        "fundamental": weight,
        "macro": weight,
    }


# ---------------------------------------------------------------------------
# Happy path — typical case
# ---------------------------------------------------------------------------


def test_typical_case_ranking_and_math() -> None:
    """
    5 indicators across 3 categories. Verify ranking matches manual calculation.

    Categories used: trend (2 items), momentum (2 items), volume (1 item).
    Regime weights: trend=0.4, momentum=0.35, volume=0.25.
    expansion_factor = 2.0.

    Manual math for each item contribution:
      contribution = (score * abs(score) / category_abs_sum) * regime_weight * expansion_factor

    trend:
      ema_alignment=60, macd_histogram=30 → abs_sum=90
      ema_alignment: (60*60/90) * 0.4 * 2.0 = 40.0 * 0.8 = 32.0
      macd_histogram: (30*30/90) * 0.4 * 2.0 = 10.0 * 0.8 = 8.0

    momentum:
      rsi_14=50, stoch_k=-10 → abs_sum=60
      rsi_14: (50*50/60) * 0.35 * 2.0 = (2500/60) * 0.7 ≈ 41.667 * 0.7 ≈ 29.167
      stoch_k: (-10*10/60) * 0.35 * 2.0 = (-100/60) * 0.7 ≈ -1.667 * 0.7 ≈ -1.167

    volume:
      obv=20 → abs_sum=20
      obv: (20*20/20) * 0.25 * 2.0 = 20.0 * 0.5 = 10.0

    Ranking by |contribution| desc: rsi_14(≈29.17), ema_alignment(32.0), obv(10.0),
      macd_histogram(8.0), stoch_k(≈1.17)
    Actually: ema_alignment(32.0) > rsi_14(≈29.17) > obv(10.0) > macd_histogram(8.0) > |stoch_k|(≈1.17)
    """
    indicator_scores = {
        "ema_alignment": 60.0,
        "macd_histogram": 30.0,
        "rsi_14": 50.0,
        "stoch_k": -10.0,
        "obv": 20.0,
    }
    pattern_scores: dict = {}
    regime_weights = {"trend": 0.4, "momentum": 0.35, "volume": 0.25}
    expansion_factor = 2.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    assert result["v"] == 1
    items = result["items"]

    # All 5 indicators should appear (all have categories in regime_weights)
    names = [item["name"] for item in items]
    assert "ema_alignment" in names
    assert "macd_histogram" in names
    assert "rsi_14" in names
    assert "stoch_k" in names
    assert "obv" in names

    # Ranking: sorted by |contribution| descending
    abs_contributions = [abs(item["contribution"]) for item in items]
    assert abs_contributions == sorted(abs_contributions, reverse=True)

    # Verify top item is ema_alignment
    assert items[0]["name"] == "ema_alignment"
    assert abs(items[0]["contribution"] - 32.0) < 1e-6

    # Verify second item is rsi_14
    assert items[1]["name"] == "rsi_14"
    expected_rsi = (50.0 * 50.0 / 60.0) * 0.35 * 2.0
    assert abs(items[1]["contribution"] - expected_rsi) < 1e-6

    # All contributions are floats
    for item in items:
        assert isinstance(item["contribution"], float)

    # Category and kind checks
    ema_item = next(i for i in items if i["name"] == "ema_alignment")
    assert ema_item["kind"] == "indicator"
    assert ema_item["category"] == "trend"
    assert ema_item["category_weight"] == 0.4

    obv_item = next(i for i in items if i["name"] == "obv")
    assert obv_item["category"] == "volume"
    assert obv_item["category_weight"] == 0.25


# ---------------------------------------------------------------------------
# All-None category
# ---------------------------------------------------------------------------


def test_all_none_category_skipped() -> None:
    """Items in a category where every score is None are excluded from items."""
    indicator_scores = {
        "ema_alignment": None,   # trend → None
        "macd_histogram": None,  # trend → None
        "rsi_14": 40.0,          # momentum → present
    }
    pattern_scores: dict = {}
    regime_weights = {"trend": 0.4, "momentum": 0.35}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    items = result["items"]
    names = [item["name"] for item in items]

    # None-scored indicators must NOT appear
    assert "ema_alignment" not in names
    assert "macd_histogram" not in names

    # Non-None indicator must appear
    assert "rsi_14" in names

    # rsi_14 is the only non-None item in momentum → abs_sum=40, contribution=40
    rsi_item = next(i for i in items if i["name"] == "rsi_14")
    expected = (40.0 * 40.0 / 40.0) * 0.35 * 1.0  # = 40.0 * 0.35 = 14.0
    assert abs(rsi_item["contribution"] - expected) < 1e-6


# ---------------------------------------------------------------------------
# Zero-sum category
# ---------------------------------------------------------------------------


def test_zero_sum_category_no_division_by_zero() -> None:
    """
    If every non-None score in a category sums to zero in absolute terms
    (i.e., all are exactly 0.0), contribution must be 0.0 with no crash.
    """
    indicator_scores = {
        "ema_alignment": 0.0,
        "macd_histogram": 0.0,
        "rsi_14": 50.0,
    }
    pattern_scores: dict = {}
    regime_weights = {"trend": 0.4, "momentum": 0.35}
    expansion_factor = 1.5

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    items = result["items"]
    names = [item["name"] for item in items]

    # Zero-scored items are present in the output but have 0.0 contribution
    assert "ema_alignment" in names
    assert "macd_histogram" in names

    ema_item = next(i for i in items if i["name"] == "ema_alignment")
    macd_item = next(i for i in items if i["name"] == "macd_histogram")
    assert ema_item["contribution"] == 0.0
    assert macd_item["contribution"] == 0.0

    # rsi_14 in momentum should have non-zero contribution
    rsi_item = next(i for i in items if i["name"] == "rsi_14")
    assert rsi_item["contribution"] != 0.0


# ---------------------------------------------------------------------------
# expansion_factor == 0.0
# ---------------------------------------------------------------------------


def test_expansion_factor_zero_all_contributions_zero() -> None:
    """When expansion_factor == 0.0, every contribution must be 0.0. No crash."""
    indicator_scores = {
        "ema_alignment": 80.0,
        "rsi_14": 60.0,
        "obv": -40.0,
    }
    pattern_scores: dict = {}
    regime_weights = {"trend": 0.4, "momentum": 0.35, "volume": 0.25}
    expansion_factor = 0.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        assert item["contribution"] == 0.0


# ---------------------------------------------------------------------------
# Pattern as top driver
# ---------------------------------------------------------------------------


def test_pattern_as_top_driver() -> None:
    """
    When the highest |contribution| belongs to a pattern, it should appear
    as items[0] with kind == "pattern".
    """
    indicator_scores = {
        "rsi_14": 10.0,   # momentum, small score
    }
    pattern_scores = {
        "candlestick_pattern_score": 90.0,  # candlestick
    }
    regime_weights = {"momentum": 0.2, "candlestick": 0.8}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    items = result["items"]
    assert len(items) >= 2
    assert items[0]["name"] == "candlestick_pattern_score"
    assert items[0]["kind"] == "pattern"


# ---------------------------------------------------------------------------
# Missing key skipped silently
# ---------------------------------------------------------------------------


def test_missing_key_in_input_skipped_silently() -> None:
    """
    An indicator key not in INDICATOR_CATEGORY_MAP (unknown) is skipped
    silently. No crash, and it does not appear in items.
    """
    indicator_scores = {
        "rsi_14": 50.0,
        "totally_unknown_indicator_xyz": 80.0,  # not in any map
    }
    pattern_scores: dict = {}
    regime_weights = {"momentum": 1.0}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    names = [item["name"] for item in result["items"]]
    assert "totally_unknown_indicator_xyz" not in names
    assert "rsi_14" in names


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------


def test_empty_input_returns_empty_items() -> None:
    """Both indicator_scores and pattern_scores are empty → items is an empty list."""
    result = build_contributions_payload(
        indicator_scores={},
        pattern_scores={},
        regime_weights={"trend": 0.5, "momentum": 0.5},
        expansion_factor=1.0,
    )

    assert result == {"v": 1, "expansion_factor": 1.0, "items": []}


def test_expansion_factor_echoed_in_payload_root() -> None:
    """The expansion_factor argument must be echoed at the payload root so
    consumers (e.g., the /why formatter) can display the full math chain."""
    result = build_contributions_payload(
        indicator_scores={"rsi_14": 50.0},
        pattern_scores={},
        regime_weights={"momentum": 0.3},
        expansion_factor=1.5,
    )
    assert result["expansion_factor"] == 1.5


# ---------------------------------------------------------------------------
# Ranking sorted by |contribution| descending
# ---------------------------------------------------------------------------


def test_ranking_sorted_by_abs_contribution_descending() -> None:
    """The items list must be monotonically non-increasing by |contribution|."""
    indicator_scores = {
        "ema_alignment": 80.0,
        "macd_histogram": 20.0,
        "rsi_14": 60.0,
        "stoch_k": -5.0,
        "obv": 40.0,
        "cmf_20": 30.0,
    }
    pattern_scores = {
        "candlestick_pattern_score": 70.0,
        "divergence_rsi": -30.0,
    }
    regime_weights = {
        "trend": 0.3,
        "momentum": 0.25,
        "volume": 0.2,
        "candlestick": 0.15,
    }
    expansion_factor = 1.5

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    abs_contributions = [abs(item["contribution"]) for item in result["items"]]
    for i in range(len(abs_contributions) - 1):
        assert abs_contributions[i] >= abs_contributions[i + 1], (
            f"Ranking violated at index {i}: "
            f"{abs_contributions[i]} < {abs_contributions[i + 1]}"
        )


# ---------------------------------------------------------------------------
# Patterns: raw_value == None
# ---------------------------------------------------------------------------


def test_patterns_raw_value_is_none() -> None:
    """Every item with kind == 'pattern' must have raw_value exactly None."""
    indicator_scores: dict = {}
    pattern_scores = {
        "candlestick_pattern_score": 50.0,
        "structural_pattern_score": -30.0,
        "gap_score": 70.0,
    }
    regime_weights = {"candlestick": 0.4, "structural": 0.6}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        if item["kind"] == "pattern":
            assert item["raw_value"] is None, (
                f"Pattern item '{item['name']}' has raw_value={item['raw_value']!r}, expected None"
            )


# ---------------------------------------------------------------------------
# Indicators: raw_value == score
# ---------------------------------------------------------------------------


def test_indicators_raw_value_equals_score() -> None:
    """
    For indicator items, raw_value must equal score exactly.

    Convention: since build_contributions_payload does not have access to the
    original indicator measurement (only the scored value), raw_value is set to
    score for indicators as a placeholder. This convention is documented in the
    function's docstring.
    """
    indicator_scores = {
        "rsi_14": 45.0,
        "ema_alignment": -70.0,
        "obv": 20.0,
    }
    pattern_scores: dict = {}
    regime_weights = {"momentum": 0.4, "trend": 0.35, "volume": 0.25}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        if item["kind"] == "indicator":
            assert item["raw_value"] == item["score"], (
                f"Indicator item '{item['name']}' has raw_value={item['raw_value']!r} "
                f"but score={item['score']!r}"
            )


# ---------------------------------------------------------------------------
# No assertion that sum(contributions) == raw_daily (by design — approximate)
# All contributions are floats
# ---------------------------------------------------------------------------


def test_all_contributions_are_floats() -> None:
    """Verify every contribution field in the payload is a Python float."""
    indicator_scores = {
        "ema_alignment": 55.0,
        "rsi_14": -20.0,
        "obv": 0.0,
    }
    pattern_scores = {
        "candlestick_pattern_score": 30.0,
    }
    regime_weights = {
        "trend": 0.3,
        "momentum": 0.3,
        "volume": 0.2,
        "candlestick": 0.2,
    }
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        assert isinstance(item["contribution"], float), (
            f"Item '{item['name']}' contribution is {type(item['contribution'])}, expected float"
        )


# ---------------------------------------------------------------------------
# Category not in regime_weights → items silently excluded
# ---------------------------------------------------------------------------


def test_items_in_category_not_in_regime_weights_excluded() -> None:
    """
    If a category from INDICATOR_CATEGORY_MAP is not present in regime_weights,
    the items in that category are silently skipped.
    """
    # Only trend is in regime_weights; momentum/volume items should be excluded
    indicator_scores = {
        "ema_alignment": 60.0,
        "rsi_14": 50.0,    # momentum → not in regime_weights
        "obv": 30.0,       # volume → not in regime_weights
    }
    pattern_scores: dict = {}
    regime_weights = {"trend": 1.0}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    names = [item["name"] for item in result["items"]]
    assert "ema_alignment" in names
    assert "rsi_14" not in names
    assert "obv" not in names


# ---------------------------------------------------------------------------
# Payload structure — required fields on every item
# ---------------------------------------------------------------------------


def test_item_has_all_required_fields() -> None:
    """Every item in the payload must contain the 7 required fields."""
    required_fields = {"name", "kind", "raw_value", "score", "category", "category_weight", "contribution"}

    indicator_scores = {"rsi_14": 40.0, "ema_alignment": 60.0}
    pattern_scores = {"candlestick_pattern_score": -25.0}
    regime_weights = {"momentum": 0.4, "trend": 0.4, "candlestick": 0.2}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        missing = required_fields - set(item.keys())
        assert not missing, f"Item '{item.get('name')}' missing fields: {missing}"


# ---------------------------------------------------------------------------
# kind field — correct assignment
# ---------------------------------------------------------------------------


def test_kind_field_assigned_correctly() -> None:
    """Items from indicator_scores have kind='indicator'; from pattern_scores kind='pattern'."""
    indicator_scores = {"rsi_14": 30.0}
    pattern_scores = {"candlestick_pattern_score": 50.0}
    regime_weights = {"momentum": 0.5, "candlestick": 0.5}
    expansion_factor = 1.0

    result = build_contributions_payload(
        indicator_scores=indicator_scores,
        pattern_scores=pattern_scores,
        regime_weights=regime_weights,
        expansion_factor=expansion_factor,
    )

    for item in result["items"]:
        if item["name"] == "rsi_14":
            assert item["kind"] == "indicator"
        elif item["name"] == "candlestick_pattern_score":
            assert item["kind"] == "pattern"


# ---------------------------------------------------------------------------
# Aggregate score items (sentiment, fundamental, macro)
# ---------------------------------------------------------------------------


class TestAggregateItems:
    """Tests for the aggregate_scores parameter emitting kind='aggregate' items."""

    def test_aggregate_items_emitted_with_correct_fields(self) -> None:
        """
        With non-None aggregate_scores and matching regime_weights, three
        'aggregate' items are appended with the correct name, category, kind,
        score, category_weight, and contribution.
        """
        aggregate_scores = {
            "sentiment": 60.0,
            "fundamental": -20.0,
            "macro": 40.0,
        }
        regime_weights = {
            "trend": 0.4,
            "momentum": 0.3,
            "volume": 0.2,
            "sentiment": 0.03,
            "fundamental": 0.04,
            "macro": 0.03,
        }
        expansion_factor = 2.0

        result = build_contributions_payload(
            indicator_scores={},
            pattern_scores={},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        items = result["items"]
        names = [item["name"] for item in items]
        assert "sentiment" in names
        assert "fundamental" in names
        assert "macro" in names

        sentiment_item = next(i for i in items if i["name"] == "sentiment")
        assert sentiment_item["kind"] == "aggregate"
        assert sentiment_item["category"] == "sentiment"
        assert sentiment_item["score"] == 60.0
        assert sentiment_item["category_weight"] == 0.03
        assert sentiment_item["raw_value"] is None
        expected_contribution = 60.0 * 0.03 * 2.0
        assert abs(sentiment_item["contribution"] - expected_contribution) < 1e-9

        fundamental_item = next(i for i in items if i["name"] == "fundamental")
        assert fundamental_item["kind"] == "aggregate"
        assert fundamental_item["score"] == -20.0
        expected_fund = -20.0 * 0.04 * 2.0
        assert abs(fundamental_item["contribution"] - expected_fund) < 1e-9

        macro_item = next(i for i in items if i["name"] == "macro")
        assert macro_item["kind"] == "aggregate"
        assert macro_item["score"] == 40.0
        expected_macro = 40.0 * 0.03 * 2.0
        assert abs(macro_item["contribution"] - expected_macro) < 1e-9

    def test_none_aggregate_score_is_skipped(self) -> None:
        """
        When aggregate_scores contains a None entry, that entry is skipped.
        Non-None entries are still emitted.
        """
        aggregate_scores = {
            "sentiment": None,
            "fundamental": -20.0,
            "macro": 40.0,
        }
        regime_weights = {"sentiment": 0.1, "fundamental": 0.1, "macro": 0.1}
        expansion_factor = 1.0

        result = build_contributions_payload(
            indicator_scores={},
            pattern_scores={},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        names = [item["name"] for item in result["items"]]
        assert "sentiment" not in names
        assert "fundamental" in names
        assert "macro" in names

    def test_zero_weight_aggregate_emits_zero_contribution(self) -> None:
        """
        When regime_weights[category] == 0.0, the aggregate item IS emitted
        (truthful zero rendering, per user decision) with contribution == 0.0.
        raw_value must be None.
        """
        aggregate_scores = {"sentiment": 50.0, "fundamental": -20.0, "macro": 30.0}
        regime_weights = {
            "sentiment": 0.0,
            "fundamental": 0.0,
            "macro": 0.0,
        }
        expansion_factor = 1.5

        result = build_contributions_payload(
            indicator_scores={},
            pattern_scores={},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        items = result["items"]
        names = [item["name"] for item in items]
        assert "sentiment" in names
        assert "fundamental" in names
        assert "macro" in names

        for item in items:
            assert item["contribution"] == 0.0
            assert item["raw_value"] is None

    def test_aggregate_items_do_not_affect_indicator_and_pattern_kinds(self) -> None:
        """
        Indicator items still have kind='indicator', pattern items still have
        kind='pattern', and aggregate items have kind='aggregate' — no cross-contamination.
        """
        aggregate_scores = {"sentiment": 30.0}
        regime_weights = {"momentum": 0.5, "candlestick": 0.3, "sentiment": 0.2}
        expansion_factor = 1.0

        result = build_contributions_payload(
            indicator_scores={"rsi_14": 40.0},
            pattern_scores={"candlestick_pattern_score": 50.0},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        items = result["items"]
        rsi_item = next(i for i in items if i["name"] == "rsi_14")
        assert rsi_item["kind"] == "indicator"

        pattern_item = next(i for i in items if i["name"] == "candlestick_pattern_score")
        assert pattern_item["kind"] == "pattern"

        sentiment_item = next(i for i in items if i["name"] == "sentiment")
        assert sentiment_item["kind"] == "aggregate"

    def test_sort_order_aggregates_mingle_with_indicators(self) -> None:
        """
        After appending aggregate items, the final list is still sorted by
        abs(contribution) descending — aggregates mingle naturally with indicators.
        """
        aggregate_scores = {"macro": 80.0}
        regime_weights = {
            "momentum": 0.1,  # rsi_14 contribution will be small
            "macro": 0.5,     # macro contribution will be large
        }
        expansion_factor = 1.0

        result = build_contributions_payload(
            indicator_scores={"rsi_14": 10.0},
            pattern_scores={},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        items = result["items"]
        abs_contributions = [abs(item["contribution"]) for item in items]
        assert abs_contributions == sorted(abs_contributions, reverse=True)

        # Macro should come before rsi_14 given the numbers above
        names = [item["name"] for item in items]
        assert names.index("macro") < names.index("rsi_14")

    def test_empty_aggregate_scores_no_aggregate_items(self) -> None:
        """
        When aggregate_scores is not provided (default empty), no aggregate
        items appear. Backward-compat: existing callers that don't pass
        aggregate_scores still work.
        """
        result = build_contributions_payload(
            indicator_scores={"rsi_14": 50.0},
            pattern_scores={},
            regime_weights={"momentum": 1.0},
            expansion_factor=1.0,
        )

        names = [item["name"] for item in result["items"]]
        assert "sentiment" not in names
        assert "fundamental" not in names
        assert "macro" not in names

    def test_aggregate_contribution_is_float(self) -> None:
        """Every aggregate item's contribution must be a Python float."""
        aggregate_scores = {"sentiment": 50.0, "fundamental": -30.0, "macro": 20.0}
        regime_weights = {"sentiment": 0.1, "fundamental": 0.1, "macro": 0.1}
        expansion_factor = 1.0

        result = build_contributions_payload(
            indicator_scores={},
            pattern_scores={},
            regime_weights=regime_weights,
            expansion_factor=expansion_factor,
            aggregate_scores=aggregate_scores,
        )

        for item in result["items"]:
            if item["kind"] == "aggregate":
                assert isinstance(item["contribution"], float), (
                    f"Aggregate item '{item['name']}' contribution is "
                    f"{type(item['contribution'])}, expected float"
                )
