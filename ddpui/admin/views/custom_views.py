from django.shortcuts import render
from ddpui.models.tasks import OrgTask
from ddpui.models.org import Org
from ddpui.models.llm import LlmSession
from ddpui.ddpprefect.prefect_service import get_long_running_flow_runs
from ddpui.utils.helpers import find_key_in_dictionary


def orgs_index_view(request):
    nhours = 2
    flow_runs = get_long_running_flow_runs(nhours)
    context = {"flow_runs": [], "nhours": nhours, "workspaces": [], "llm_sessions": []}

    # airbyte workspaces
    for org in Org.objects.order_by("name"):
        url = f"http://localhost:8000/workspaces/{org.airbyte_workspace_id}"
        context["workspaces"].append(
            {"org_slug": org.slug, "workspace_url": url, "workspace_id": org.airbyte_workspace_id}
        )

    # llm feedbacks if any
    for session in LlmSession.objects.filter(feedback__isnull=False).order_by("-updated_at"):
        context["llm_sessions"].append(
            {
                "org_slug": session.org.slug,
                "response": session.response,
                "feedback": session.feedback,
                "user_prompts": "\n".join(session.user_prompts),
            }
        )

    # long running flows
    for flow_run in flow_runs:
        flow_run_data = {
            "state_name": flow_run["state_name"],
            "flow_run_url": f"http://localhost:4200/flow-runs/flow-run/{flow_run['id']}",
            "org_slug": find_key_in_dictionary(flow_run["parameters"], "org_slug"),
            "tasks": [
                x["slug"] for x in find_key_in_dictionary(flow_run["parameters"], "tasks") or []
            ],
            "flow_name": find_key_in_dictionary(flow_run["parameters"], "flow_name"),
            "connection_id": find_key_in_dictionary(flow_run["parameters"], "connection_id"),
            "orgtask_org_slug": None,
            "connection_url": None,
        }

        connection_id = flow_run_data["connection_id"]
        if connection_id:
            orgtask = OrgTask.objects.filter(connection_id=connection_id).first()
            if orgtask:
                flow_run_data["orgtask_org_slug"] = orgtask.org.slug
                flow_run_data[
                    "connection_url"
                ] = f"http://localhost:8000/workspaces/{orgtask.org.airbyte_workspace_id}/connections/{connection_id}"
            else:
                flow_run_data["orgtask_org_slug"] = connection_id

        context["flow_runs"].append(flow_run_data)

    return render(request, "static_admin.html", context)
