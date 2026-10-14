"""Report creation tool for the platform guide agent.

A report is a frozen snapshot of a dashboard. The payload goes through
SnapshotCreate — the Reports API's own request schema — and then
ReportService.create_snapshot, the same path the Reports page uses, so the
frozen configs, date filtering and audit log are identical.
"""

from datetime import date

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import created, error_reason, rejection
from ddpui.core.reports.report_service import ReportService
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import CreatedArtifact
from ddpui.schemas.report_schema import DateColumnSchema, SnapshotCreate
from ddpui.core.ai.typed_dicts import CreationArtifact


def _rejected(reason: str) -> tuple[str, CreationArtifact]:
    return rejection("report", "Report not created", reason)


@register_tool
@tool(response_format="content_and_artifact")
def create_report(
    title: str,
    dashboard_id: int,
    runtime: ToolRuntime[RunContext],
    date_column: DateColumnSchema | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
) -> tuple[str, CreationArtifact]:
    """Create a report: a frozen snapshot of an existing dashboard. Get the
    dashboard_id from list_dashboards and confirm the choice with the user
    first. To limit the report to a date range, pass period_start/period_end
    (YYYY-MM-DD) together with date_column — the datetime column
    {schema_name, table_name, column_name} the dates filter on."""
    ctx = runtime.context
    try:
        payload = SnapshotCreate(
            title=title,
            dashboard_id=dashboard_id,
            date_column=date_column,
            period_start=period_start,
            period_end=period_end,
        )
    except ValueError as err:  # pydantic ValidationError is a ValueError
        return _rejected(error_reason(err))

    try:
        orguser = OrgUser.objects.select_related("org").get(id=ctx.orguser_id)
        snapshot = ReportService.create_snapshot(
            title=payload.title,
            dashboard_id=payload.dashboard_id,
            orguser=orguser,
            date_column=payload.date_column.model_dump() if payload.date_column else {},
            period_end=payload.period_end,
            period_start=payload.period_start,
        )
    except Exception as err:  # pylint: disable=broad-except
        return _rejected(error_reason(err))

    url_path = f"/reports/{snapshot.id}"
    return created(
        CreatedArtifact(
            type="report", object_id=snapshot.id, title=snapshot.title, url_path=url_path
        ),
        f"Done — report '{snapshot.title}' (id {snapshot.id}) is saved. "
        f"The user can open it at {url_path}.",
    )
