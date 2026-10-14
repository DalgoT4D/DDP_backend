"""Read-only inventory tools for the platform guide agent.

The guide agent's job is dependency-aware guidance — "a KPI is built on a
metric; you already have these metrics" — so it needs to SEE what the org
already has. These are thin org-scoped listings: names + the fields
needed to reference an object in a follow-up creation call (ids), nothing
else. No warehouse access, no writes.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.metric.metric_service import MetricService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.org_user import OrgUser
from ddpui.services.chart_service import ChartService

MAX_LISTED = 50


def _load_orguser(ctx: RunContext) -> OrgUser:
    return OrgUser.objects.select_related("org").get(id=ctx.orguser_id)


def _listing(title: str, lines: list[str]) -> str:
    if not lines:
        return f"{title}: none yet."
    shown = lines[:MAX_LISTED]
    suffix = f"\n… ({len(lines) - MAX_LISTED} more not shown)" if len(lines) > MAX_LISTED else ""
    return f"{title} ({len(lines)}):\n" + "\n".join(shown) + suffix


@register_tool
@tool
def list_metrics(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing metrics (id, name, what they measure).
    Check this BEFORE creating a metric or a KPI — a KPI is built on a metric,
    and one may already exist."""
    orguser = _load_orguser(runtime.context)
    metrics, _ = MetricService.list_metrics(orguser.org, page_size=MAX_LISTED)
    lines = [
        f"[id {m.id}] {m.name} — "
        + (m.column_expression or f"{m.aggregation}({m.column})")
        + f" on {m.schema_name}.{m.table_name}"
        for m in metrics
    ]
    return _listing("Metrics", lines)


@register_tool
@tool
def list_kpis(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing KPIs (id, name, underlying metric, target)."""
    orguser = _load_orguser(runtime.context)
    kpis, _ = KPIService.list_kpis(orguser.org, orguser, page_size=MAX_LISTED)
    lines = [
        f"[id {k.id}] {k.name} — metric: {k.metric.name}, target: {k.target_value}" for k in kpis
    ]
    return _listing("KPIs", lines)


@register_tool
@tool
def list_charts(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing charts (id, title, type, source table).
    Check this before creating a chart or building a dashboard from charts."""
    orguser = _load_orguser(runtime.context)
    charts, _ = ChartService.list_charts(orguser.org, orguser, page_size=MAX_LISTED)
    lines = [
        f"[id {c.id}] {c.title} — {c.chart_type} on {c.schema_name}.{c.table_name}" for c in charts
    ]
    return _listing("Charts", lines)


@register_tool
@tool
def list_reports(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing report snapshots (id, title, period)."""
    orguser = _load_orguser(runtime.context)
    reports = ReportService.list_snapshots(orguser.org, orguser)
    lines = [f"[id {r.id}] {r.title}" for r in reports[:MAX_LISTED]]
    return _listing("Reports", lines)
