"""Dashboard scope — an AI session restricted to one dashboard's tables.

Scope is re-resolved on every turn (not frozen at session create), so charts
added to the dashboard mid-conversation are picked up on the next question, and
a deleted dashboard turns into a friendly per-turn error.

The component walk mirrors ReportService._freeze_chart_configs — the reference
implementation for reading Dashboard.tabs[].components.
"""

from ddpui.core.ai.scopes.base import ResolvedScope, ScopeUnavailable
from ddpui.models.dashboard import Dashboard, DashboardType
from ddpui.models.metric import KPI
from ddpui.models.org import Org
from ddpui.models.visualization import Chart


def resolve_dashboard_scope(org: Org, dashboard_id: int | None) -> ResolvedScope:
    """The dashboard's table allowlist + a prompt context block, or
    ScopeUnavailable when the dashboard is gone or has nothing to query."""
    dashboard = Dashboard.objects.filter(
        id=dashboard_id, org=org, dashboard_type=DashboardType.NATIVE.value
    ).first()
    if dashboard is None:
        raise ScopeUnavailable(
            "This dashboard no longer exists, so this chat can't answer questions "
            "about it. Start a new chat from the Chat with Data page."
        )

    chart_ids = dashboard.component_ids("chart")
    charts = list(Chart.objects.filter(id__in=chart_ids, org=org))

    kpi_ids = dashboard.component_ids("kpi")
    kpis = list(KPI.objects.filter(id__in=kpi_ids, org=org).select_related("metric"))

    filters = list(dashboard.filters.all().order_by("order"))

    tables = {f"{chart.schema_name}.{chart.table_name}" for chart in charts}
    # KPI rows carry no table fields — the table lives on the underlying Metric
    tables |= {f"{kpi.metric.schema_name}.{kpi.metric.table_name}" for kpi in kpis}
    # filters may point at lookup tables no chart uses (e.g. a districts table)
    tables |= {f"{flt.schema_name}.{flt.table_name}" for flt in filters}

    if not tables:
        raise ScopeUnavailable(
            "This dashboard has no charts yet, so there is no data to chat about. "
            "Add a chart to the dashboard or use the full Chat with Data page."
        )

    return ResolvedScope(
        scope_type="dashboard",
        allowed_tables=sorted(tables),
        scope_context=_dashboard_context(dashboard, charts, kpis, filters),
    )


def _dashboard_context(dashboard, charts, kpis, filters) -> str:
    """Markdown block describing the dashboard, injected into the system prompt.
    Chart titles and filter names carry the user's own vocabulary — they help the
    model map a question like "how are the districts doing?" onto the right columns."""
    lines = [f'This chat is about the dashboard "{dashboard.title}".']
    if dashboard.description:
        lines.append(f"Dashboard description: {dashboard.description}")
    if charts:
        lines.append("Charts on this dashboard:")
        for chart in sorted(charts, key=lambda c: c.title):
            lines.append(
                f'- "{chart.title}" — {chart.chart_type} chart on '
                f"{chart.schema_name}.{chart.table_name}"
            )
    if kpis:
        lines.append("KPIs on this dashboard:")
        for kpi in sorted(kpis, key=lambda k: k.name):
            lines.append(
                f'- "{kpi.name}" — measures {kpi.metric.name} on '
                f"{kpi.metric.schema_name}.{kpi.metric.table_name}"
            )
    if filters:
        lines.append("Dashboard filters (how users slice this data):")
        for flt in filters:
            lines.append(
                f'- "{flt.name}" ({flt.filter_type}) on '
                f"{flt.schema_name}.{flt.table_name}.{flt.column_name}"
            )
    return "\n".join(lines)
