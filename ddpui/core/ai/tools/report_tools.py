"""Report creation tool for the platform guide agent.

A report is a frozen snapshot of an EXISTING dashboard, so the agent's flow
is: list_dashboards → ask the user which one → create_report. Delegates to
ReportService.create_snapshot — the same path the Reports page uses, so the
freeze/validation logic is identical.
"""

from datetime import date

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import register_tool
from ddpui.core.ai.tools.rendering import rejection


@register_tool
@tool(response_format="content_and_artifact")
def create_report(
    title: str,
    dashboard_id: int,
    runtime: ToolRuntime[RunContext],
    period_start: str | None = None,
    period_end: str | None = None,
) -> tuple[str, dict]:
    """Create a report: a frozen snapshot of an existing dashboard. Get the
    dashboard_id from list_dashboards and confirm the choice with the user
    first. Optional period_start/period_end (YYYY-MM-DD) limit the report to
    a date range when the dashboard has a date filter."""
    ctx = runtime.context
    # reports ride the dashboard-creation permission, same as the Reports API
    if not ctx.can_create_dashboards:
        return rejection(
            "report", "Report not created", "you do not have permission to create reports"
        )

    try:
        start = date.fromisoformat(period_start) if period_start else None
        end = date.fromisoformat(period_end) if period_end else None
    except ValueError:
        return rejection("report", "Report not created", "dates must be YYYY-MM-DD")

    from ddpui.core.reports.report_service import ReportService
    from ddpui.models.org_user import OrgUser

    try:
        orguser = OrgUser.objects.select_related("org").get(id=ctx.orguser_id)
        snapshot = ReportService.create_snapshot(
            title=title,
            dashboard_id=dashboard_id,
            orguser=orguser,
            period_start=start,
            period_end=end,
        )
    except Exception as err:  # pylint: disable=broad-except
        return rejection("report", "Report not created", str(err).splitlines()[0][:300])

    url_path = f"/reports/{snapshot.id}"
    content = (
        f"Done — report '{snapshot.title}' (id {snapshot.id}) is saved. "
        f"The user can open it at {url_path}."
    )
    return content, {
        "type": "report",
        "object_id": snapshot.id,
        "title": snapshot.title,
        "url_path": url_path,
    }
