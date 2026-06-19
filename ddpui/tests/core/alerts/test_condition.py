"""Tests for ddpui.core.alerts.condition"""

import pytest

from ddpui.core.alerts import condition as cond


# ── validate_condition ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "alert_type,payload",
    [
        ("metric_threshold", {"operator": "lt", "value": 50}),
        ("metric_threshold", {"operator": "gt", "value": 0.5}),
        ("standalone", {"operator": "eq", "value": 0}),
        ("kpi_rag", {"rag_states": ["red"]}),
        ("kpi_rag", {"rag_states": ["red", "amber"]}),
    ],
)
def test_validate_condition_accepts_valid(alert_type, payload):
    cond.validate_condition(alert_type, payload)


@pytest.mark.parametrize(
    "alert_type,payload",
    [
        ("metric_threshold", {"operator": "nope", "value": 1}),
        ("metric_threshold", {"operator": "lt"}),
        ("metric_threshold", {"value": 1}),
        ("kpi_rag", {"rag_states": []}),
        ("kpi_rag", {"rag_states": ["red", "amber", "green"]}),  # all 3 → fires always
        ("kpi_rag", {"rag_states": ["blue"]}),  # invalid state
        ("bogus_type", {"operator": "lt", "value": 1}),
    ],
)
def test_validate_condition_rejects_invalid(alert_type, payload):
    with pytest.raises(ValueError):
        cond.validate_condition(alert_type, payload)


# ── evaluate ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "operator,target,value,expected",
    [
        ("lt", 50, 49, True),
        ("lt", 50, 50, False),
        ("gt", 100, 101, True),
        ("gt", 100, 100, False),
        ("eq", 5, 5, True),
        ("eq", 5, 6, False),
    ],
)
def test_evaluate_threshold(operator, target, value, expected):
    result = cond.evaluate("metric_threshold", {"operator": operator, "value": target}, value)
    assert result is expected


def test_evaluate_empty_result_does_not_fire():
    """None (empty query result) = condition not satisfied."""
    assert cond.evaluate("metric_threshold", {"operator": "lt", "value": 0}, None) is False
    assert cond.evaluate("kpi_rag", {"rag_states": ["red"]}, None) is False


@pytest.mark.parametrize(
    "selected,current,expected",
    [
        (["red"], "red", True),
        (["red"], "amber", False),
        (["red", "amber"], "amber", True),
        (["green"], "red", False),
    ],
)
def test_evaluate_kpi_rag(selected, current, expected):
    result = cond.evaluate("kpi_rag", {"rag_states": selected}, current)
    assert result is expected


# ── pretty ──────────────────────────────────────────────────────────────────


def test_pretty_threshold():
    assert cond.pretty("metric_threshold", {"operator": "lt", "value": 50}) == "value < 50"


def test_pretty_kpi_rag():
    assert cond.pretty("kpi_rag", {"rag_states": ["red", "amber"]}) == "RAG = red, amber"


def test_pretty_empty():
    assert cond.pretty("metric_threshold", None) == ""
