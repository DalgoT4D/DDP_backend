"""Tests for ddpui.core.alerts.rendering."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.alerts.rendering import (
    TOKENS_BY_TYPE,
    render,
    resolve_tokens,
)


# ── render() ────────────────────────────────────────────────────────────────


def test_render_substitutes_known_tokens():
    out = render("Hello {{name}}, value is {{value}}", {"name": "Priya", "value": 42})
    assert out == "Hello Priya, value is 42"


def test_render_preserves_unknown_tokens():
    out = render("{{alert_name}} fired at {{unknown}}", {"alert_name": "X"})
    assert out == "X fired at {{unknown}}"


def test_render_preserves_none_or_empty_values():
    """None and empty string should be left as raw tokens — caller didn't supply a value."""
    out = render("{{a}} | {{b}} | {{c}}", {"a": "ok", "b": None, "c": ""})
    assert out == "ok | {{b}} | {{c}}"


def test_render_handles_whitespace_inside_braces():
    out = render("{{ name }}", {"name": "Dalgo"})
    assert out == "Dalgo"


def test_render_handles_empty_template():
    assert render("", {"a": 1}) == ""
    assert render(None, {"a": 1}) == ""


# ── resolve_tokens() ────────────────────────────────────────────────────────


def test_resolve_tokens_metric_threshold():
    tokens = resolve_tokens(
        "metric_threshold",
        alert_name="Daily check",
        metric_name="Active users",
        target_value=50,
        current_value=42,
    )
    assert tokens == {
        "alert_name": "Daily check",
        "metric_name": "Active users",
        "target_value": 50,
        "current_value": 42,
    }
    # No kpi_name / dataset_name pollution
    assert "kpi_name" not in tokens
    assert "dataset_name" not in tokens


def test_resolve_tokens_kpi_rag_includes_rag_status():
    tokens = resolve_tokens(
        "kpi_rag",
        alert_name="KPI watch",
        kpi_name="Signups",
        target_value=1000,
        current_value=900,
        rag_status="amber",
    )
    assert tokens["rag_status"] == "amber"
    assert tokens["kpi_name"] == "Signups"


def test_resolve_tokens_standalone_includes_dataset_name():
    tokens = resolve_tokens(
        "standalone",
        alert_name="Raw count",
        dataset_name="analytics.events",
        target_value=100,
        current_value=120,
    )
    assert tokens["dataset_name"] == "analytics.events"


# ── TOKENS_BY_TYPE ──────────────────────────────────────────────────────────


def test_tokens_by_type_constants_match_frontend_expectations():
    """If this changes, update webapp_v2/types/alerts.ts TOKENS_BY_TYPE too."""
    assert TOKENS_BY_TYPE["metric_threshold"] == [
        "alert_name",
        "metric_name",
        "target_value",
        "current_value",
    ]
    assert TOKENS_BY_TYPE["kpi_rag"] == [
        "alert_name",
        "kpi_name",
        "target_value",
        "current_value",
        "rag_status",
    ]
    assert TOKENS_BY_TYPE["standalone"] == [
        "alert_name",
        "dataset_name",
        "target_value",
        "current_value",
    ]
