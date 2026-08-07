"""Tests for dashboard tools — list, create-with-charts, add-to-existing.

Persistence is faked at the seams; the grid-placement logic runs for real
(it is the part that decides whether the dashboard renders sensibly).
"""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.core.ai.tools import dashboard_tools
from ddpui.tests.core.ai.test_agent_loop import make_context


def make_dash_context(**overrides):
    ctx = make_context()
    ctx.orguser_id = 7
    ctx.can_create_dashboards = True
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


def run_tool(tool, ctx, **kwargs):
    return tool.func(runtime=type("R", (), {"context": ctx})(), **kwargs)


# ── grid placement (pure logic) ─────────────────────────────────────────────


def test_place_charts_fills_rows_of_three():
    layout, components = dashboard_tools.place_charts([], [10, 11, 12, 13])
    assert [(l["x"], l["y"]) for l in layout] == [(0, 0), (4, 0), (8, 0), (0, 3)]
    assert all(l["w"] == 4 and l["h"] == 3 for l in layout)
    assert components["chart-10"] == {"type": "chart", "config": {"chartId": 10}}
    assert layout[0]["i"] == "chart-10"


def test_place_charts_appends_below_existing_items():
    existing = [{"i": "chart-5", "x": 0, "y": 0, "w": 4, "h": 3}]
    layout, _ = dashboard_tools.place_charts(existing, [10])
    assert layout[0]["y"] == 3  # below the existing row
    assert layout[0]["x"] == 0


# ── list_dashboards ─────────────────────────────────────────────────────────


def test_list_dashboards_renders_ids_and_titles(monkeypatch):
    monkeypatch.setattr(
        dashboard_tools,
        "_load_dashboards",
        lambda ctx: [(3, "Donor Overview", True), (9, "Field Performance", False)],
    )
    out = run_tool(dashboard_tools.list_dashboards, make_dash_context())
    assert "id 3" in out and "Donor Overview" in out and "published" in out
    assert "id 9" in out and "Field Performance" in out


def test_list_dashboards_handles_none(monkeypatch):
    monkeypatch.setattr(dashboard_tools, "_load_dashboards", lambda ctx: [])
    out = run_tool(dashboard_tools.list_dashboards, make_dash_context())
    assert "no dashboards" in out.lower()


# ── create_dashboard ────────────────────────────────────────────────────────


def test_create_dashboard_with_charts(monkeypatch):
    calls = {}

    class FakeDash:
        id = 42
        title = "Donor Overview"

    def fake_create(ctx, title, description, chart_ids):
        calls.update(title=title, chart_ids=chart_ids)
        return FakeDash()

    monkeypatch.setattr(dashboard_tools, "_create_dashboard", fake_create)
    monkeypatch.setattr(dashboard_tools, "_org_chart_ids", lambda ctx, ids: set(ids))

    content, artifact = run_tool(
        dashboard_tools.create_dashboard,
        make_dash_context(),
        title="Donor Overview",
        chart_ids=[10, 11],
    )
    assert artifact == {
        "type": "dashboard",
        "dashboard_id": 42,
        "title": "Donor Overview",
        "url_path": "/dashboards/42",
    }
    assert calls["chart_ids"] == [10, 11]
    assert "Donor Overview" in content


def test_create_dashboard_requires_permission(monkeypatch):
    called = {}
    monkeypatch.setattr(
        dashboard_tools, "_create_dashboard", lambda *a, **k: called.setdefault("hit", True)
    )
    content, artifact = run_tool(
        dashboard_tools.create_dashboard,
        make_dash_context(can_create_dashboards=False),
        title="t",
        chart_ids=[1],
    )
    assert artifact["status"] == "rejected"
    assert "hit" not in called


def test_create_dashboard_rejects_foreign_charts(monkeypatch):
    monkeypatch.setattr(dashboard_tools, "_org_chart_ids", lambda ctx, ids: {10})
    content, artifact = run_tool(
        dashboard_tools.create_dashboard,
        make_dash_context(),
        title="t",
        chart_ids=[10, 999],
    )
    assert artifact["status"] == "rejected"
    assert "999" in content


# ── add_charts_to_dashboard ─────────────────────────────────────────────────


def test_add_charts_to_existing_dashboard(monkeypatch):
    calls = {}

    class FakeDash:
        id = 3
        title = "Donor Overview"

    def fake_add(ctx, dashboard_id, chart_ids):
        calls.update(dashboard_id=dashboard_id, chart_ids=chart_ids)
        return FakeDash()

    monkeypatch.setattr(dashboard_tools, "_add_charts", fake_add)
    monkeypatch.setattr(dashboard_tools, "_org_chart_ids", lambda ctx, ids: set(ids))

    content, artifact = run_tool(
        dashboard_tools.add_charts_to_dashboard,
        make_dash_context(),
        dashboard_id=3,
        chart_ids=[10],
    )
    assert artifact["type"] == "dashboard"
    assert artifact["dashboard_id"] == 3
    assert calls == {"dashboard_id": 3, "chart_ids": [10]}


def test_add_charts_reports_missing_dashboard(monkeypatch):
    def fake_add(ctx, dashboard_id, chart_ids):
        raise dashboard_tools.DashboardNotFound()

    monkeypatch.setattr(dashboard_tools, "_add_charts", fake_add)
    monkeypatch.setattr(dashboard_tools, "_org_chart_ids", lambda ctx, ids: set(ids))

    content, artifact = run_tool(
        dashboard_tools.add_charts_to_dashboard,
        make_dash_context(),
        dashboard_id=404,
        chart_ids=[10],
    )
    assert artifact["status"] == "rejected"
    assert "not found" in content.lower()
