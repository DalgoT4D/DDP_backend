"""The create_chart tool — the agent's first artifact-creating capability.

It writes Dalgo METADATA (a saved Chart in the org's chart library), never
warehouse data — the warehouse stays read-only. The chart appears in the
Charts page and can be added to dashboards; the artifact carries the link the
UI renders as a chip.

Registry pattern (spec §12): this is one new module; the agent graph is
untouched.
"""

from langchain.tools import ToolRuntime, tool
from pydantic import BaseModel, Field

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import rejection


class MetricInput(BaseModel):
    """One measured value on the chart."""

    column: str | None = Field(
        default=None, description="Column to aggregate; omit for a row count"
    )
    aggregation: str = Field(
        default="count", description="sum | avg | count | min | max | count_distinct"
    )
    alias: str | None = Field(
        default=None, description="Display name shown on the chart, e.g. 'Silt target'"
    )


def _rejected(reason: str) -> tuple[str, dict]:
    return rejection("chart", "Chart not created", reason)


CHART_TYPES = {"bar", "line", "pie", "number"}
AGGREGATIONS = {"sum", "avg", "count", "min", "max", "count_distinct"}


def _save_chart(ctx: RunContext, chart_data) -> "Chart":  # noqa: F821
    """Persist via the same service the Charts page uses. Sync ORM is fine here:
    LangGraph executes sync tools in a worker thread, not on the event loop.
    Imported lazily so the tool module stays light for unit tests."""
    from ddpui.models.org_user import OrgUser
    from ddpui.services.chart_service import ChartService

    orguser = OrgUser.objects.select_related("org").get(id=ctx.orguser_id)
    return ChartService.create_chart(chart_data, orguser)


@register_tool
@tool(response_format="content_and_artifact")
def create_chart(
    title: str,
    chart_type: str,
    schema_name: str,
    table_name: str,
    runtime: ToolRuntime[RunContext],
    dimension_column: str | None = None,
    metrics: list[MetricInput] | None = None,
    description: str | None = None,
) -> tuple[str, dict]:
    """Create a saved chart in the organization's chart library from ONE table.

    chart_type: bar | line | pie | number.
    dimension_column: the column to group by — REQUIRED for bar/line (x-axis)
    and pie (slices); omit for number.
    metrics: the measured values, each {column, aggregation, alias}.
    aggregation: sum|avg|count|min|max|count_distinct; omit column for a row
    count. Bar and line charts can plot SEVERAL metrics at once (grouped bars /
    multiple lines — e.g. silt target vs silt achieved per state); pie and
    number take exactly one. Omit metrics entirely for a simple row count.
    Verify column names with get_table_details first. Use a short, descriptive
    title the user will recognize later."""
    ctx = runtime.context

    if not ctx.can_create_charts:
        return _rejected("you do not have permission to create charts in this organization")
    if chart_type not in CHART_TYPES:
        return _rejected(f"chart_type must be one of {sorted(CHART_TYPES)}")
    if schema_name not in ctx.allowed_schemas:
        return _rejected(f"schema '{schema_name}' is not accessible")
    if chart_type != "number" and not dimension_column:
        return _rejected(f"a {chart_type} chart needs a dimension_column to group by")

    metric_inputs = [
        MetricInput(**m) if isinstance(m, dict) else m for m in (metrics or [MetricInput()])
    ]
    if chart_type in ("pie", "number") and len(metric_inputs) > 1:
        return _rejected(f"a {chart_type} chart takes exactly one metric")

    metric_dicts = []
    for m in metric_inputs:
        aggregation = (m.aggregation or "count").lower()
        if aggregation not in AGGREGATIONS:
            return _rejected(f"aggregation must be one of {sorted(AGGREGATIONS)}")
        metric_dicts.append(
            {
                "column": m.column,
                "aggregation": aggregation,
                "alias": m.alias or (f"{aggregation}_{m.column}" if m.column else aggregation),
            }
        )

    # dimension_column is the grouping key the render path GROUPs BY for every
    # chart type — the chart builder UI stores multi-metric bar charts the same way
    extra_config: dict = {"metrics": metric_dicts}
    if chart_type != "number":
        extra_config["dimension_column"] = dimension_column

    from ddpui.services.chart_service import ChartData  # light dataclass import

    try:
        chart = _save_chart(
            ctx,
            ChartData(
                title=title,
                chart_type=chart_type,
                schema_name=schema_name,
                table_name=table_name,
                extra_config=extra_config,
                description=description,
            ),
        )
    except Exception as err:  # pylint: disable=broad-except
        message = str(err).split("\n", maxsplit=1)[0][:300]
        return _rejected(f"saving failed ({message})")

    url_path = f"/charts/{chart.id}"
    content = (
        f"Created chart '{chart.title}' (id {chart.id}). "
        f"The user can open it at {url_path} or add it to a dashboard."
    )
    artifact = {
        "type": "chart",
        "chart_id": chart.id,
        "title": chart.title,
        "url_path": url_path,
    }
    return content, artifact
