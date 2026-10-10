"""Tests for the create_chart tool — the agent's first write-capability.

It writes Dalgo METADATA (a saved Chart), never warehouse data. Persistence is
faked by monkeypatching the save seam; validation logic runs for real.
"""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.core.ai.tools import chart_tools
from ddpui.tests.core.ai.test_agent_loop import make_context


class FakeChart:
    id = 42
    title = "Surveys by district"


@pytest.fixture
def saved(monkeypatch):
    """Capture what would be persisted; return a canned Chart."""
    calls = {}

    def fake_save(ctx, payload):
        calls["ctx"] = ctx
        calls["data"] = payload
        return FakeChart()

    monkeypatch.setattr(chart_tools, "_save_chart", fake_save)
    return calls


def run_tool(ctx, **kwargs):
    return chart_tools.create_chart.func(runtime=type("R", (), {"context": ctx})(), **kwargs)


def make_chart_context(**overrides):
    ctx = make_context()
    ctx.orguser_id = 7
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


def test_creates_bar_chart_with_metric(saved):
    content, artifact = run_tool(
        make_chart_context(),
        title="Surveys by district",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
        extra_config={"dimension_column": "district", "metrics": [{"aggregation": "count"}]},
    )

    assert "Surveys by district" in content
    assert artifact == {
        "type": "chart",
        "object_id": 42,
        "title": "Surveys by district",
        "url_path": "/charts/42",
    }
    ec = saved["data"].extra_config.model_dump()
    assert ec["dimension_column"] == "district"
    assert "x_axis_column" not in ec
    assert [(m["column"], m["aggregation"], m["alias"]) for m in ec["metrics"]] == [
        (None, "count", "count")
    ]


def test_stored_extra_config_matches_the_charts_api_shape(saved):
    """Same ChartCreate dump the Charts API stores — incl. the defaulted keys."""
    run_tool(
        make_chart_context(),
        title="t",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
        extra_config={"dimension_column": "district", "metrics": [{"aggregation": "count"}]},
    )
    extra_config = saved["data"].extra_config.model_dump()
    for key in ("customizations", "filters", "pagination", "sort", "extra_dimension_column"):
        assert key in extra_config


def test_bar_chart_accepts_multiple_metrics(saved):
    """The chart builder supports several metrics per bar/line chart (grouped
    bars); the tool must not flatten that to one."""
    _, artifact = run_tool(
        make_chart_context(),
        title="Silt target vs achieved by state",
        chart_type="bar",
        schema_name="prod",
        table_name="work_orders",
        extra_config={
            "dimension_column": "state",
            "metrics": [
                {"column": "silt_target", "aggregation": "sum", "alias": "Silt target"},
                {"column": "silt_achieved", "aggregation": "sum"},
            ],
        },
    )
    assert artifact["type"] == "chart"
    ec = saved["data"].extra_config.model_dump()
    assert [(m["column"], m["aggregation"], m["alias"]) for m in ec["metrics"]] == [
        ("silt_target", "sum", "Silt target"),
        ("silt_achieved", "sum", "sum_silt_achieved"),
    ]


def test_pie_and_number_take_exactly_one_metric(saved):
    _, artifact = run_tool(
        make_chart_context(),
        title="Share by district",
        chart_type="pie",
        schema_name="prod",
        table_name="surveys",
        extra_config={
            "dimension_column": "district",
            "metrics": [
                {"column": "amount", "aggregation": "sum"},
                {"column": "amount", "aggregation": "avg"},
            ],
        },
    )
    assert artifact["status"] == "rejected"
    assert "data" not in saved


def test_pie_uses_dimension_column_key(saved):
    _, artifact = run_tool(
        make_chart_context(),
        title="Share by district",
        chart_type="pie",
        schema_name="prod",
        table_name="surveys",
        extra_config={
            "dimension_column": "district",
            "metrics": [{"column": "amount", "aggregation": "sum"}],
        },
    )
    assert artifact["type"] == "chart"
    ec = saved["data"].extra_config.model_dump()
    assert ec["dimension_column"] == "district"
    assert ec["metrics"][0]["aggregation"] == "sum"


def test_rejects_disallowed_schema(saved):
    content, artifact = run_tool(
        make_chart_context(),
        title="t",
        chart_type="bar",
        schema_name="secret_schema",
        table_name="surveys",
        extra_config={"dimension_column": "district", "metrics": [{"aggregation": "count"}]},
    )
    assert artifact["status"] == "rejected"
    assert "data" not in saved


def test_rejects_bad_chart_type_and_missing_dimension(saved):
    content, artifact = run_tool(
        make_chart_context(),
        title="t",
        chart_type="map",  # not offered to the agent in v1
        schema_name="prod",
        table_name="surveys",
        extra_config={"dimension_column": "district", "metrics": [{"aggregation": "count"}]},
    )
    assert artifact["status"] == "rejected"

    content, artifact = run_tool(
        make_chart_context(),
        title="t",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
        extra_config={"metrics": [{"aggregation": "count"}]},  # no dimension_column for bar
    )
    assert artifact["status"] == "rejected"
    assert "data" not in saved


def test_rejects_missing_extra_config(saved):
    """When the LLM omits extra_config, the tool should reject gracefully
    instead of raising an unhandled ValidationError."""
    content, artifact = run_tool(
        make_chart_context(),
        title="Surveys by state",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
    )
    assert artifact["status"] == "rejected"
    assert "data" not in saved
