"""Pure condition helpers — used by the evaluator and the listing UI for
human-readable rendering."""

from typing import Any, Optional

from ddpui.models.alert import AlertType


VALID_OPERATORS = {"lt", "gt", "eq"}
VALID_RAG_STATES = {"red", "amber", "green"}
THRESHOLD_TYPES = (AlertType.METRIC_THRESHOLD, AlertType.STANDALONE)


def validate_condition(alert_type: str, condition: dict) -> None:
    """Raise ValueError if the condition shape is wrong for the alert_type."""
    if not isinstance(condition, dict):
        raise ValueError("condition must be an object")

    if alert_type == AlertType.KPI_RAG:
        rag_states = condition.get("rag_states")
        if not isinstance(rag_states, list) or not rag_states:
            raise ValueError("kpi_rag condition requires non-empty 'rag_states' list")
        if not 1 <= len(rag_states) <= 2:
            raise ValueError("kpi_rag condition must select 1 or 2 RAG states")
        for s in rag_states:
            if s not in VALID_RAG_STATES:
                raise ValueError(f"Invalid RAG state '{s}'")
    elif alert_type in THRESHOLD_TYPES:
        operator = condition.get("operator")
        value = condition.get("value")
        if operator not in VALID_OPERATORS:
            raise ValueError(f"condition.operator must be one of {sorted(VALID_OPERATORS)}")
        if not isinstance(value, (int, float)):
            raise ValueError("condition.value must be a number")
    else:
        raise ValueError(f"Unknown alert_type '{alert_type}'")


def evaluate(alert_type: str, condition: dict, value: Any) -> bool:
    """Return whether the condition fires for the given value.

    Empty / no-rows query results (value is None) are treated as
    "condition not satisfied" per spec — the alert does not fire.
    """
    if value is None:
        return False

    if alert_type == AlertType.KPI_RAG:
        # `value` here is the current RAG state ("red" / "amber" / "green")
        return value in set(condition.get("rag_states", []))

    if alert_type in THRESHOLD_TYPES:
        operator = condition["operator"]
        target = condition["value"]
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        if operator == "lt":
            return v < target
        if operator == "gt":
            return v > target
        if operator == "eq":
            return v == target

    return False


def pretty(alert_type: str, condition: Optional[dict]) -> str:
    """Render a short human-readable form of the condition for the listing UI."""
    if not condition:
        return ""
    if alert_type == AlertType.KPI_RAG:
        states = condition.get("rag_states", [])
        if not states:
            return "RAG"
        return f"RAG = {', '.join(states)}"
    operator = condition.get("operator")
    value = condition.get("value")
    op_symbol = {"lt": "<", "gt": ">", "eq": "="}.get(operator, operator)
    return f"value {op_symbol} {value}"
