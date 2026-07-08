"""Tests for the create_chart tool — the agent's first write-capability.

It writes Dalgo METADATA (a saved Chart), never warehouse data. Persistence is
faked by monkeypatching the save seam; validation logic runs for real.
"""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.core.chat_with_data.tools import chart_tools
from ddpui.tests.core.chat_with_data.test_agent_loop import make_context


class FakeChart:
    id = 42
    title = "Surveys by district"


@pytest.fixture
def saved(monkeypatch):
    """Capture what would be persisted; return a canned Chart."""
    calls = {}

    def fake_save(ctx, chart_data):
        calls["ctx"] = ctx
        calls["data"] = chart_data
        return FakeChart()

    monkeypatch.setattr(chart_tools, "_save_chart", fake_save)
    return calls


def run_tool(ctx, **kwargs):
    return chart_tools.create_chart.func(runtime=type("R", (), {"context": ctx})(), **kwargs)


def make_chart_context(**overrides):
    ctx = make_context()
    ctx.orguser_id = 7
    ctx.can_create_charts = True
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
        dimension_column="district",
        metric_column=None,
        metric_aggregation="count",
    )

    assert "Surveys by district" in content
    assert artifact == {
        "type": "chart",
        "chart_id": 42,
        "title": "Surveys by district",
        "url_path": "/charts/42",
    }
    data = saved["data"]
    assert data.chart_type == "bar"
    # the render path groups by dimension_column for EVERY chart type —
    # x_axis_column is ignored by the query builder (blank-chart regression)
    assert data.extra_config["dimension_column"] == "district"
    assert "x_axis_column" not in data.extra_config
    assert data.extra_config["metrics"] == [
        {"column": None, "aggregation": "count", "alias": "count"}
    ]


def test_pie_uses_dimension_column_key(saved):
    _, artifact = run_tool(
        make_chart_context(),
        title="Share by district",
        chart_type="pie",
        schema_name="prod",
        table_name="surveys",
        dimension_column="district",
        metric_column="amount",
        metric_aggregation="sum",
    )
    assert artifact["type"] == "chart"
    assert saved["data"].extra_config["dimension_column"] == "district"
    assert saved["data"].extra_config["metrics"][0]["aggregation"] == "sum"


def test_rejects_without_permission(saved):
    content, artifact = run_tool(
        make_chart_context(can_create_charts=False),
        title="t",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
        dimension_column="district",
    )
    assert artifact["status"] == "rejected"
    assert "permission" in content.lower()
    assert "data" not in saved  # nothing persisted


def test_rejects_disallowed_schema(saved):
    content, artifact = run_tool(
        make_chart_context(),
        title="t",
        chart_type="bar",
        schema_name="secret_schema",
        table_name="surveys",
        dimension_column="district",
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
        dimension_column="district",
    )
    assert artifact["status"] == "rejected"

    content, artifact = run_tool(
        make_chart_context(),
        title="t",
        chart_type="bar",
        schema_name="prod",
        table_name="surveys",
        dimension_column=None,  # bar needs one
    )
    assert artifact["status"] == "rejected"
    assert "data" not in saved
