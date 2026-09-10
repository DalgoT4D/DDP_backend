"""Read-only inventory tools for the platform guide agent.

The guide agent's job is dependency-aware guidance — "a KPI is built on a
metric; you already have these metrics" — so it needs to SEE what the org
already has. These are thin org-scoped ORM listings: names + the fields
needed to reference an object in a follow-up creation call (ids), nothing
else. No warehouse access, no writes.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool

MAX_LISTED = 50


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
    from ddpui.models.metric import Metric

    metrics = Metric.objects.filter(org_id=runtime.context.org_id).order_by("name")
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
    from ddpui.models.metric import KPI

    kpis = (
        KPI.objects.filter(org_id=runtime.context.org_id).select_related("metric").order_by("name")
    )
    lines = [
        f"[id {k.id}] {k.name} — metric: {k.metric.name}, target: {k.target_value}" for k in kpis
    ]
    return _listing("KPIs", lines)


@register_tool
@tool
def list_charts(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing charts (id, title, type, source table).
    Check this before creating a chart or building a dashboard from charts."""
    from ddpui.models.visualization import Chart

    charts = Chart.objects.filter(org_id=runtime.context.org_id).order_by("title")
    lines = [
        f"[id {c.id}] {c.title} — {c.chart_type} on {c.schema_name}.{c.table_name}" for c in charts
    ]
    return _listing("Charts", lines)


@register_tool
@tool
def list_reports(runtime: ToolRuntime[RunContext]) -> str:
    """List the organization's existing report snapshots (id, title, period)."""
    from ddpui.models.report import ReportSnapshot

    reports = ReportSnapshot.objects.filter(org_id=runtime.context.org_id).order_by("-created_at")
    lines = [f"[id {r.id}] {r.title}" for r in reports]
    return _listing("Reports", lines)
