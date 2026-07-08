"""Dashboard tools — list, create-with-charts, add-to-existing.

The suggest-then-act flow is conversational: the system prompt instructs the
agent to call list_dashboards FIRST when the user wants a chart on a dashboard,
offer "add to one of these or create a new one?", and only act on the user's
choice in the next turn.

Like create_chart, these write Dalgo METADATA only — the warehouse stays
read-only. Component/layout shapes mirror exactly what the dashboard builder
UI stores: components {"chart-<id>": {"type": "chart", "config": {"chartId": id}}}
and react-grid-layout entries {i, x, y, w, h} on a 12-column grid.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.chat_with_data.state import RunContext
from ddpui.core.chat_with_data.tools.registry import register_tool

# Grid placement: 12-column grid, three 4-wide × 3-tall charts per row —
# the same footprint the dashboard builder uses for chart components
CHART_W = 4
CHART_H = 3
GRID_COLUMNS = 12
_PER_ROW = GRID_COLUMNS // CHART_W


class DashboardNotFound(Exception):
    """Dashboard missing or not in this org."""


def place_charts(existing_layout: list[dict], chart_ids: list[int]) -> tuple[list[dict], dict]:
    """Grid positions + component configs for chart_ids, appended BELOW any
    existing items so nothing overlaps."""
    base_y = max((item.get("y", 0) + item.get("h", 0) for item in existing_layout), default=0)
    layout: list[dict] = []
    components: dict = {}
    for index, chart_id in enumerate(chart_ids):
        key = f"chart-{chart_id}"
        layout.append(
            {
                "i": key,
                "x": (index % _PER_ROW) * CHART_W,
                "y": base_y + (index // _PER_ROW) * CHART_H,
                "w": CHART_W,
                "h": CHART_H,
            }
        )
        components[key] = {"type": "chart", "config": {"chartId": chart_id}}
    return layout, components


# ── ORM seams (monkeypatched in unit tests; sync ORM is fine in tool threads) ──


def _load_dashboards(ctx: RunContext) -> list[tuple[int, str, bool]]:
    from ddpui.models.dashboard import Dashboard

    return [
        (d.id, d.title, d.is_published)
        for d in Dashboard.objects.filter(org_id=ctx.org_id, dashboard_type="native").order_by(
            "-updated_at"
        )[:30]
    ]


def _org_chart_ids(ctx: RunContext, chart_ids: list[int]) -> set[int]:
    from ddpui.models.visualization import Chart

    return set(
        Chart.objects.filter(org_id=ctx.org_id, id__in=chart_ids).values_list("id", flat=True)
    )


def _create_dashboard(ctx: RunContext, title: str, description: str | None, chart_ids: list[int]):
    from ddpui.models.org_user import OrgUser
    from ddpui.services.dashboard_service import DashboardData, DashboardService

    orguser = OrgUser.objects.select_related("org").get(id=ctx.orguser_id)
    dashboard = DashboardService.create_dashboard(
        DashboardData(title=title, description=description, grid_columns=GRID_COLUMNS), orguser
    )
    if chart_ids:
        tab = dashboard.tabs[0]
        layout, components = place_charts(tab.get("layout_config", []), chart_ids)
        tab["layout_config"] = tab.get("layout_config", []) + layout
        tab["components"] = {**tab.get("components", {}), **components}
        dashboard.tabs = [tab] + dashboard.tabs[1:]
        dashboard.save(update_fields=["tabs"])
    return dashboard


def _add_charts(ctx: RunContext, dashboard_id: int, chart_ids: list[int]):
    from ddpui.models.dashboard import Dashboard

    dashboard = Dashboard.objects.filter(org_id=ctx.org_id, id=dashboard_id).first()
    if dashboard is None:
        raise DashboardNotFound()
    tabs = dashboard.tabs or []
    if not tabs:
        tabs = [{"id": "tab-1", "title": "Untitled Tab 1", "layout_config": [], "components": {}}]
    tab = tabs[0]  # v1: charts land on the first tab
    already = set(tab.get("components", {}).keys())
    new_ids = [cid for cid in chart_ids if f"chart-{cid}" not in already]
    layout, components = place_charts(tab.get("layout_config", []), new_ids)
    tab["layout_config"] = tab.get("layout_config", []) + layout
    tab["components"] = {**tab.get("components", {}), **components}
    dashboard.tabs = [tab] + tabs[1:]
    dashboard.save(update_fields=["tabs"])
    return dashboard


def _rejected(reason: str) -> tuple[str, dict]:
    return (
        f"Dashboard action not done: {reason}",
        {"type": "dashboard", "status": "rejected", "error": reason},
    )


def _dashboard_artifact(dashboard) -> tuple[str, dict]:
    url_path = f"/dashboards/{dashboard.id}"
    content = (
        f"Done — dashboard '{dashboard.title}' (id {dashboard.id}). "
        f"The user can open it at {url_path}."
    )
    return content, {
        "type": "dashboard",
        "dashboard_id": dashboard.id,
        "title": dashboard.title,
        "url_path": url_path,
    }


# ── tools ───────────────────────────────────────────────────────────────────


@register_tool
@tool
def list_dashboards(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's dashboards (id, title, published state). ALWAYS
    call this before creating a dashboard or adding a chart to one, so you can
    ask the user whether to add to an existing dashboard or create a new one."""
    ctx = runtime.context
    dashboards = _load_dashboards(ctx)
    if not dashboards:
        return "This organization has no dashboards yet."
    lines = ["Dashboards:"]
    for dash_id, title, published in dashboards:
        state = "published" if published else "draft"
        lines.append(f"id {dash_id}: {title} ({state})")
    return "\n".join(lines)


@register_tool
@tool(response_format="content_and_artifact")
def create_dashboard(
    title: str,
    chart_ids: list[int],
    runtime: ToolRuntime[RunContext],
    description: str | None = None,
) -> tuple[str, dict]:
    """Create a NEW dashboard containing the given charts (use the chart ids
    returned by create_chart or named by the user). Only call this after the
    user has chosen to create a new dashboard rather than add to an existing
    one — check with list_dashboards + a question first."""
    ctx = runtime.context
    if not ctx.can_create_dashboards:
        return _rejected("you do not have permission to create dashboards in this organization")
    if not chart_ids:
        return _rejected("provide at least one chart_id to place on the dashboard")

    known = _org_chart_ids(ctx, chart_ids)
    missing = [cid for cid in chart_ids if cid not in known]
    if missing:
        return _rejected(f"chart id(s) {missing} do not exist in this organization")

    try:
        dashboard = _create_dashboard(ctx, title, description, chart_ids)
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(f"saving failed ({str(err).splitlines()[0][:300]})")
    return _dashboard_artifact(dashboard)


@register_tool
@tool(response_format="content_and_artifact")
def add_charts_to_dashboard(
    dashboard_id: int,
    chart_ids: list[int],
    runtime: ToolRuntime[RunContext],
) -> tuple[str, dict]:
    """Add charts to an EXISTING dashboard (first tab). Get the dashboard_id
    from list_dashboards and confirm the choice with the user first."""
    ctx = runtime.context
    if not ctx.can_create_dashboards:
        return _rejected("you do not have permission to edit dashboards in this organization")
    if not chart_ids:
        return _rejected("provide at least one chart_id to add")

    known = _org_chart_ids(ctx, chart_ids)
    missing = [cid for cid in chart_ids if cid not in known]
    if missing:
        return _rejected(f"chart id(s) {missing} do not exist in this organization")

    try:
        dashboard = _add_charts(ctx, dashboard_id, chart_ids)
    except DashboardNotFound:
        return _rejected(f"dashboard {dashboard_id} not found — use list_dashboards for valid ids")
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(f"saving failed ({str(err).splitlines()[0][:300]})")
    return _dashboard_artifact(dashboard)
