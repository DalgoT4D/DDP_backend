"""The create_chart tool — the agent's first artifact-creating capability.

It writes Dalgo METADATA (a saved Chart in the org's chart library), never
warehouse data — the warehouse stays read-only. The chart appears in the
Charts page and can be added to dashboards; the artifact carries the link the
UI renders as a chip.

The payload goes through ChartCreate — the Charts API's own request schema —
so per-chart-type rules and the stored extra_config shape are identical to a
chart built in the UI.
"""

from typing import Literal

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import created, error_reason, rejection
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.schemas.chart_schemas import ChartCreate, ChartMetric
from ddpui.schemas.chat_with_data_schemas import CreatedArtifact
from ddpui.services.chart_service import ChartData, ChartService
from ddpui.core.ai.typed_dicts import CreationArtifact

# Offered to the agent in v1 — map/table/pivot_table need config it can't build
AgentChartType = Literal["bar", "line", "pie", "number"]


def _rejected(reason: str) -> tuple[str, CreationArtifact]:
    return rejection("chart", "Chart not created", reason)


def _save_chart(ctx: RunContext, chart_data: ChartData) -> Chart:
    """Persist via the same service the Charts page uses. Sync ORM is fine here:
    LangGraph executes sync tools in a worker thread, not on the event loop."""
    orguser = OrgUser.objects.select_related("org").get(id=ctx.orguser_id)
    return ChartService.create_chart(chart_data, orguser)


def _with_default_alias(metric: ChartMetric) -> dict:
    """The chart builder always names a metric; give the agent's unnamed ones
    the same "<aggregation>_<column>" label."""
    data = metric.model_dump(exclude_none=True)
    if not metric.alias and metric.aggregation:
        data["alias"] = (
            f"{metric.aggregation}_{metric.column}" if metric.column else metric.aggregation
        )
    return data


@register_tool
@tool(response_format="content_and_artifact")
def create_chart(
    title: str,
    chart_type: AgentChartType,
    schema_name: str,
    table_name: str,
    runtime: ToolRuntime[RunContext],
    dimension_column: str | None = None,
    metrics: list[ChartMetric] | None = None,
    description: str | None = None,
) -> tuple[str, CreationArtifact]:
    """Create a saved chart in the organization's chart library from ONE table.

    dimension_column: the column to group by — REQUIRED for bar/line (x-axis)
    and pie (slices); omit for number.
    metrics: the measured values, each {column, aggregation, alias}; omit
    column for a row count. Bar and line charts can plot SEVERAL metrics at
    once (grouped bars / multiple lines — e.g. silt target vs silt achieved per
    state); pie and number take exactly one. Omit metrics entirely for a simple
    row count. Verify column names with get_table_details first. Use a short,
    descriptive title the user will recognize later."""
    ctx = runtime.context
    if schema_name not in ctx.allowed_schemas:
        return _rejected(f"schema '{schema_name}' is not accessible")

    try:
        extra_config: dict = {
            "metrics": [
                _with_default_alias(ChartMetric.model_validate(m))
                for m in (metrics or [ChartMetric(aggregation="count")])
            ]
        }
        if chart_type != "number" and dimension_column:
            extra_config["dimension_column"] = dimension_column
        payload = ChartCreate(
            title=title,
            description=description,
            chart_type=chart_type,
            schema_name=schema_name,
            table_name=table_name,
            extra_config=extra_config,
        )
        chart = _save_chart(
            ctx,
            ChartData(
                title=payload.title,
                description=payload.description,
                chart_type=payload.chart_type,
                schema_name=payload.schema_name,
                table_name=payload.table_name,
                extra_config=payload.extra_config.model_dump(),
            ),
        )
    except ValueError as err:  # pydantic ValidationError is a ValueError
        return _rejected(error_reason(err))
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(f"saving failed ({error_reason(err)})")

    url_path = f"/charts/{chart.id}"
    return created(
        CreatedArtifact(type="chart", object_id=chart.id, title=chart.title, url_path=url_path),
        f"Created chart '{chart.title}' (id {chart.id}). "
        f"The user can open it at {url_path} or add it to a dashboard.",
    )
