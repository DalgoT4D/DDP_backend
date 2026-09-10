"""Metric and KPI creation tools for the platform guide agent.

Both delegate to the same services the REST API uses (MetricService /
KPIService), so validation is identical to the UI path: create_metric runs a
real test query against the warehouse before saving, create_kpi verifies the
metric belongs to the org. Both write Dalgo METADATA only — the warehouse
stays read-only.

Dependency order matters and the agent's prompt teaches it: a KPI is built
ON a metric, so create_metric (or list_metrics) comes first.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import rejection

VALID_AGGREGATIONS = ("count", "count_distinct", "sum", "avg", "min", "max")
VALID_DIRECTIONS = ("increase", "decrease")
VALID_TIME_GRAINS = ("daily", "weekly", "monthly", "quarterly", "yearly")


def _load_orguser(ctx: RunContext):
    from ddpui.models.org_user import OrgUser

    return OrgUser.objects.select_related("org").get(id=ctx.orguser_id)


@register_tool
@tool(response_format="content_and_artifact")
def create_metric(
    name: str,
    schema_name: str,
    table_name: str,
    runtime: ToolRuntime[RunContext],
    column: str | None = None,
    aggregation: str | None = None,
    column_expression: str | None = None,
    description: str | None = None,
) -> tuple[str, dict]:
    """Create a reusable metric: a named aggregation over one warehouse table.
    Either pass column + aggregation (count/count_distinct/sum/avg/min/max)
    for a simple metric, OR column_expression for a calculated one (e.g.
    "SUM(achieved) / SUM(target)"). Verify real column names with
    get_table_details first. The metric is validated with a test query
    before saving."""
    ctx = runtime.context
    if not ctx.can_create_metrics:
        return rejection(
            "metric", "Metric not created", "you do not have permission to create metrics"
        )
    if aggregation and aggregation not in VALID_AGGREGATIONS:
        return rejection(
            "metric", "Metric not created", f"aggregation must be one of {VALID_AGGREGATIONS}"
        )

    from ddpui.core.metric.metric_service import MetricService

    try:
        metric = MetricService.create_metric(
            name=name,
            description=description,
            schema_name=schema_name,
            table_name=table_name,
            column=column,
            aggregation=aggregation,
            column_expression=column_expression,
            orguser=_load_orguser(ctx),
        )
    except Exception as err:  # pylint: disable=broad-except
        return rejection("metric", "Metric not created", str(err).splitlines()[0][:300])

    content = (
        f"Done — metric '{metric.name}' (id {metric.id}) is saved and validated. "
        "It can now back a KPI or be used on the Metrics page."
    )
    return content, {
        "type": "metric",
        "object_id": metric.id,
        "title": metric.name,
        "url_path": "/metrics",
    }


@register_tool
@tool(response_format="content_and_artifact")
def create_kpi(
    metric_id: int,
    direction: str,
    time_grain: str,
    runtime: ToolRuntime[RunContext],
    name: str | None = None,
    target_value: float | None = None,
    time_dimension_column: str | None = None,
) -> tuple[str, dict]:
    """Create a KPI on top of an EXISTING metric (get metric_id from
    list_metrics or create_metric). direction is "increase" or "decrease"
    (is higher better?); time_grain is daily/weekly/monthly/quarterly/yearly.
    Name defaults to the metric's name."""
    ctx = runtime.context
    if not ctx.can_create_kpis:
        return rejection("kpi", "KPI not created", "you do not have permission to create KPIs")
    if direction not in VALID_DIRECTIONS:
        return rejection("kpi", "KPI not created", f"direction must be one of {VALID_DIRECTIONS}")
    if time_grain not in VALID_TIME_GRAINS:
        return rejection("kpi", "KPI not created", f"time_grain must be one of {VALID_TIME_GRAINS}")

    from ddpui.core.kpi.kpi_service import KPIService
    from ddpui.schemas.kpi_schema import KPICreate, KPIExtraConfig

    try:
        kpi = KPIService.create_kpi(
            KPICreate(
                metric_id=metric_id,
                name=name,
                target_value=target_value,
                direction=direction,
                time_grain=time_grain,
                time_dimension_column=time_dimension_column,
                extra_config=KPIExtraConfig(),
            ),
            _load_orguser(ctx),
        )
    except Exception as err:  # pylint: disable=broad-except
        return rejection("kpi", "KPI not created", str(err).splitlines()[0][:300])

    content = (
        f"Done — KPI '{kpi.name}' (id {kpi.id}) is created on metric '{kpi.metric.name}'. "
        "It appears on the Impact page."
    )
    return content, {
        "type": "kpi",
        "object_id": kpi.id,
        "title": kpi.name,
        "url_path": "/impact",
    }
