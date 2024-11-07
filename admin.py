import os
import streamlit
import django
import dotenv

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


# Usage: streamlit run admin.py
main()
