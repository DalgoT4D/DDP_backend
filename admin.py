import os
import streamlit
import django
import dotenv
import requests
from datetime import datetime, timedelta
import pytz

dotenv.load_dotenv(".env")

os.environ["DJANGO_SETTINGS_MODULE"] = "ddpui.settings"
django.setup()

from ddpui.models.org import Org


def main():
    """main function to run the streamlit app"""
    org_to_workspace = Org.objects.values("name", "airbyte_workspace_id")
    streamlit.title("Airbyte workspace URLs")
    for org in org_to_workspace:
        org["airbyte_url"] = f"http://localhost:8000/workspaces/{org['airbyte_workspace_id']}"
        streamlit.markdown(f"[{org['name']}]({org['airbyte_url']})")

    streamlit.title("Long-running flows")
    twohoursago = datetime.now() - timedelta(seconds=2 * 3600)
    r = requests.post(
        "http://localhost:4200/api/flow_runs/filter",
        json={
            "flow_runs": {
                "operator": "and_",
                "state": {
                    "operator": "and_",
                    "type": {"any_": ["RUNNING"]},
                },
                "start_time": {"before_": twohoursago.astimezone(pytz.utc).isoformat()},
            }
        },
        timeout=10,
    )
    flow_runs = r.json()
    for flow_run in flow_runs:
        streamlit.write(flow_run["state_name"])

        if "config" in flow_run["parameters"]:

            streamlit.write(flow_run["parameters"]["config"]["org_slug"])

            streamlit.write([x["slug"] for x in flow_run["parameters"]["config"]["tasks"]])

        elif "payload" in flow_run["parameters"]:
            streamlit.write(flow_run["parameters"]["payload"]["flow_name"])

        else:
            streamlit.write(flow_run["parameters"].keys())

        streamlit.write("=" * 20)


# Usage: streamlit run admin.py
main()
