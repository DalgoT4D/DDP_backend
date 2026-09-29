"""Dashboard tools — list, create-with-charts, add-to-existing.

The suggest-then-act flow is conversational: the system prompt instructs the
agent to call list_dashboards FIRST when the user wants a chart on a dashboard,
offer "add to one of these or create a new one?", and only act on the user's
choice in the next turn.

Like create_chart, these write Dalgo METADATA only — the warehouse stays
read-only. Component/layout shapes mirror exactly what the dashboard builder
UI stores: components {"chart-<id>": {"type": "chart", "config": {"chartId": id}}}
and react-grid-layout entries {i, x, y, w, h} on a 12-column grid.

Both writes go through DashboardService (create_dashboard / update_dashboard),
the same calls the Dashboards API makes — so lock rules, tab validation,
sharing cascade and the audit log are identical to the builder UI.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import created, error_reason, rejection
from ddpui.models.dashboard import Dashboard
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.schemas.chat_with_data_schemas import CreatedArtifact
from ddpui.schemas.dashboard_schema import DashboardTabSchema, DashboardUpdate
from ddpui.services.dashboard_service import (
    DashboardData,
    DashboardLockedError,
    DashboardNotFoundError,
    DashboardService,
)
from ddpui.core.ai.typed_dicts import CreationArtifact

# Grid placement: 12-column grid, three 4-wide × 3-tall charts per row —
# the same footprint the dashboard builder uses for chart components
CHART_W = 4
CHART_H = 3
GRID_COLUMNS = 12
_PER_ROW = GRID_COLUMNS // CHART_W


def _rejected(reason: str) -> tuple[str, CreationArtifact]:
    return rejection("dashboard", "Dashboard action not done", reason)


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
    return [
        (d.id, d.title, d.is_published)
        for d in Dashboard.objects.filter(org_id=ctx.org_id, dashboard_type="native").order_by(
            "-updated_at"
        )[:30]
    ]


def _org_chart_ids(ctx: RunContext, chart_ids: list[int]) -> set[int]:
    return set(
        Chart.objects.filter(org_id=ctx.org_id, id__in=chart_ids).values_list("id", flat=True)
    )


def _load_orguser(ctx: RunContext) -> OrgUser:
    return OrgUser.objects.select_related("org").get(id=ctx.orguser_id)


def _create_dashboard(ctx: RunContext, title: str, description: str | None, chart_ids: list[int]):
    orguser = _load_orguser(ctx)
    dashboard = DashboardService.create_dashboard(
        DashboardData(title=title, description=description, grid_columns=GRID_COLUMNS), orguser
    )
    return _place_on_first_tab(dashboard, chart_ids, orguser)


def _add_charts(ctx: RunContext, dashboard_id: int, chart_ids: list[int]):
    orguser = _load_orguser(ctx)
    dashboard = DashboardService.get_dashboard(dashboard_id, orguser.org)
    return _place_on_first_tab(dashboard, chart_ids, orguser)


def _place_on_first_tab(dashboard, chart_ids: list[int], orguser: OrgUser):
    """Append the charts not already on the first tab (v1: charts land there),
    saved through update_dashboard like a builder save."""
    tabs = dashboard.tabs or [
        {"id": "tab-1", "title": "Untitled Tab 1", "layout_config": [], "components": {}}
    ]
    tab = tabs[0]
    new_ids = [cid for cid in chart_ids if f"chart-{cid}" not in tab.get("components", {})]
    if not new_ids:
        return dashboard
    layout, components = place_charts(tab.get("layout_config", []), new_ids)
    first_tab = {
        **tab,
        "layout_config": tab.get("layout_config", []) + layout,
        "components": {**tab.get("components", {}), **components},
    }
    return DashboardService.update_dashboard(
        dashboard_id=dashboard.id,
        org=orguser.org,
        orguser=orguser,
        data=DashboardUpdate(
            tabs=[DashboardTabSchema(**t) for t in [first_tab] + tabs[1:]],
        ),
    )


def _dashboard_artifact(dashboard) -> tuple[str, CreationArtifact]:
    url_path = f"/dashboards/{dashboard.id}"
    return created(
        CreatedArtifact(
            type="dashboard", object_id=dashboard.id, title=dashboard.title, url_path=url_path
        ),
        f"Done — dashboard '{dashboard.title}' (id {dashboard.id}). "
        f"The user can open it at {url_path}.",
    )


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
) -> tuple[str, CreationArtifact]:
    """Create a NEW dashboard containing the given charts (use the chart ids
    returned by create_chart or named by the user). Only call this after the
    user has chosen to create a new dashboard rather than add to an existing
    one — check with list_dashboards + a question first."""
    ctx = runtime.context
    if not chart_ids:
        return _rejected("provide at least one chart_id to place on the dashboard")

    known = _org_chart_ids(ctx, chart_ids)
    missing = [cid for cid in chart_ids if cid not in known]
    if missing:
        return _rejected(f"chart id(s) {missing} do not exist in this organization")

    try:
        dashboard = _create_dashboard(ctx, title, description, chart_ids)
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(f"saving failed ({error_reason(err)})")
    return _dashboard_artifact(dashboard)


@register_tool
@tool(response_format="content_and_artifact")
def add_charts_to_dashboard(
    dashboard_id: int,
    chart_ids: list[int],
    runtime: ToolRuntime[RunContext],
) -> tuple[str, CreationArtifact]:
    """Add charts to an EXISTING dashboard (first tab). Get the dashboard_id
    from list_dashboards and confirm the choice with the user first."""
    ctx = runtime.context
    if not chart_ids:
        return _rejected("provide at least one chart_id to add")

    known = _org_chart_ids(ctx, chart_ids)
    missing = [cid for cid in chart_ids if cid not in known]
    if missing:
        return _rejected(f"chart id(s) {missing} do not exist in this organization")

    try:
        dashboard = _add_charts(ctx, dashboard_id, chart_ids)
    except DashboardNotFoundError:
        return _rejected(f"dashboard {dashboard_id} not found — use list_dashboards for valid ids")
    except DashboardLockedError as err:
        return _rejected(f"{err.message} — try again later")
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(f"saving failed ({error_reason(err)})")
    return _dashboard_artifact(dashboard)
