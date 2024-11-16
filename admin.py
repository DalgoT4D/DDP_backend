import os
from datetime import datetime, timedelta
import pytz
import streamlit
import django
import dotenv
import requests

dotenv.load_dotenv(".env")

os.environ["DJANGO_SETTINGS_MODULE"] = "ddpui.settings"
django.setup()

from ddpui.models.org import Org
from ddpui.models.tasks import OrgTask
from ddpui.ddpprefect.prefect_service import get_long_running_flow_runs


def find_key_in_dictionary(dictionary: dict, key):
    """Recursively find first occurence of a key in a dictionary and return its value"""
    for k, v in dictionary.items():
        if k == key:
            return v
        if isinstance(v, dict):
            val = find_key_in_dictionary(v, key)
            if val:
                return val
    return None


def show_workspaces():
    """streamlit function to show workspaces"""
    org_to_workspace = Org.objects.order_by("name").values("name", "airbyte_workspace_id")
    streamlit.title("Airbyte workspace URLs")
    for org in org_to_workspace:
        org["airbyte_url"] = f"http://localhost:8000/workspaces/{org['airbyte_workspace_id']}"
        streamlit.markdown(f"[{org['name']}]({org['airbyte_url']})")


def main():
    """main function to run the streamlit app"""

    show_workspaces()

    streamlit.title("Long-running flows")
    flow_runs = get_long_running_flow_runs(2)
    for flow_run in flow_runs:
        streamlit.write(flow_run["state_name"])

        flow_run_url = "http://localhost:4200/flow-runs/flow-run/" + flow_run["id"]
        streamlit.markdown(f"[{flow_run['id']}]({flow_run_url})")

        org_slug = find_key_in_dictionary(flow_run["parameters"], "org_slug")
        if org_slug:
            streamlit.write(org_slug)

        tasks = find_key_in_dictionary(flow_run["parameters"], "tasks")
        if tasks:
            streamlit.write([x["slug"] for x in tasks])

        flow_name = find_key_in_dictionary(flow_run["parameters"], "flow_name")
        if flow_name:
            streamlit.write(flow_name)

        connection_id = find_key_in_dictionary(flow_run["parameters"], "connection_id")
        if connection_id:
            orgtask = OrgTask.objects.filter(connection_id=connection_id).first()
            if orgtask:
                streamlit.write(orgtask.org.slug)
                connection_url = f"http://localhost:8000/workspaces/{org['airbyte_workspace_id']}/connections/{connection_id}"
                streamlit.markdown(f"[{connection_id}]({connection_url})")
            else:
                streamlit.write(connection_id)

        streamlit.write("=" * 20)


# Usage: streamlit run admin.py
main()
