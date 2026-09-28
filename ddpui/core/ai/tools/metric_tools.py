"""Metric and KPI creation tools for the platform guide agent.

Both delegate to the same services the REST API uses (MetricService /
KPIService), so validation is identical to the UI path: create_metric runs a
real test query against the warehouse before saving, create_kpi verifies the
metric belongs to the org. Both write Dalgo METADATA only — the warehouse
stays read-only. Allowed values come from the services' own constants, so the
model sees the same choices the services enforce.

Dependency order matters and the agent's prompt teaches it: a KPI is built
ON a metric, so create_metric (or list_metrics) comes first.
"""

from typing import Literal

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import created, error_reason, rejection
from ddpui.core.kpi.kpi_service import VALID_DIRECTIONS, VALID_TIME_GRAINS, KPIService
from ddpui.core.metric.metric_service import VALID_AGGREGATIONS, MetricService
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import CreatedArtifact
from ddpui.schemas.kpi_schema import KPICreate, KPIExtraConfig

Aggregation = Literal[tuple(VALID_AGGREGATIONS)]
Direction = Literal[tuple(VALID_DIRECTIONS)]
TimeGrain = Literal[tuple(VALID_TIME_GRAINS)]


def _load_orguser(ctx: RunContext) -> OrgUser:
    return OrgUser.objects.select_related("org").get(id=ctx.orguser_id)


@register_tool
@tool(response_format="content_and_artifact")
def create_metric(
    name: str,
    schema_name: str,
    table_name: str,
    runtime: ToolRuntime[RunContext],
    column: str | None = None,
    aggregation: Aggregation | None = None,
    column_expression: str | None = None,
    description: str | None = None,
) -> tuple[str, dict]:
    """Create a reusable metric: a named aggregation over one warehouse table.
    Either pass column + aggregation for a simple metric, OR column_expression
    for a calculated one (e.g. "SUM(achieved) / SUM(target)"). Verify real
    column names with get_table_details first. The metric is validated with a
    test query before saving."""
    ctx = runtime.context
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
        return rejection("metric", "Metric not created", error_reason(err))

    return created(
        CreatedArtifact(type="metric", object_id=metric.id, title=metric.name, url_path="/metrics"),
        f"Done — metric '{metric.name}' (id {metric.id}) is saved and validated. "
        "It can now back a KPI or be used on the Metrics page.",
    )


@register_tool
@tool(response_format="content_and_artifact")
def create_kpi(
    metric_id: int,
    direction: Direction,
    time_grain: TimeGrain,
    runtime: ToolRuntime[RunContext],
    name: str | None = None,
    target_value: float | None = None,
    time_dimension_column: str | None = None,
) -> tuple[str, dict]:
    """Create a KPI on top of an EXISTING metric (get metric_id from
    list_metrics or create_metric). direction: is higher better ("increase")
    or lower ("decrease")? Name defaults to the metric's name."""
    ctx = runtime.context
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
        return rejection("kpi", "KPI not created", error_reason(err))

    return created(
        CreatedArtifact(type="kpi", object_id=kpi.id, title=kpi.name, url_path="/impact"),
        f"Done — KPI '{kpi.name}' (id {kpi.id}) is created on metric '{kpi.metric.name}'. "
        "It appears on the Impact page.",
    )
