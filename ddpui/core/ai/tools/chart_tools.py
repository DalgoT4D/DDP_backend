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
from ddpui.schemas.chart_schemas.crud import ChartCreate
from ddpui.schemas.chat_with_data_schemas import CreatedArtifact
from ddpui.services.chart_service import ChartData, ChartService
from ddpui.core.ai.typed_dicts import CreationArtifact

# Offered to the agent in v1 — map/table/pivot_table need config it can't build
AgentChartType = Literal["bar", "line", "pie", "number"]


def _rejected(reason: str) -> tuple[str, CreationArtifact]:
    return rejection("chart", "Chart not created", reason)


def _load_orguser(ctx: RunContext) -> OrgUser:
    return OrgUser.objects.select_related("org").get(id=ctx.orguser_id)


def _save_chart(ctx: RunContext, payload: ChartCreate) -> "Chart":
    """Convert the validated ChartCreate payload to ChartData and persist."""
    orguser = _load_orguser(ctx)
    extra_config_dict = (
        payload.extra_config.model_dump()
        if hasattr(payload.extra_config, "model_dump")
        else payload.extra_config or {}
    )
    return ChartService.create_chart(
        ChartData(
            title=payload.title,
            description=payload.description,
            chart_type=payload.chart_type,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            extra_config=extra_config_dict,
        ),
        orguser,
    )


@register_tool
@tool(response_format="content_and_artifact")
def create_chart(
    title: str,
    chart_type: AgentChartType,
    schema_name: str,
    table_name: str,
    extra_config: dict,
    runtime: ToolRuntime[RunContext],
    description: str | None = None,
) -> tuple[str, CreationArtifact]:
    """Create a saved chart in the organization's chart library from ONE table.

    extra_config structure per chart_type:
    - bar / line: {"dimension_column": "<col>", "metrics": [{"column": "<col>", "aggregation": "<agg>", "alias": "<label>"}]}
    - pie:        {"dimension_column": "<col>", "metrics": [{"column": "<col>", "aggregation": "<agg>"}]}
    - number:     {"metrics": [{"column": "<col>", "aggregation": "<agg>"}]}

    aggregation values: sum | avg | count | min | max | count_distinct.
    Omit column when aggregation is "count" (row count). Bar and line accept
    multiple metrics for grouped bars / multiple lines.
    Verify column names with get_table_details first. Use a short, descriptive
    title the user will recognize later."""
    ctx = runtime.context
    if schema_name not in ctx.allowed_schemas:
        return _rejected(f"schema '{schema_name}' is not accessible")

    try:
        chart = _save_chart(
            ctx,
            ChartCreate(
                title=title,
                description=description,
                chart_type=chart_type,
                schema_name=schema_name,
                table_name=table_name,
                extra_config=extra_config,
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
