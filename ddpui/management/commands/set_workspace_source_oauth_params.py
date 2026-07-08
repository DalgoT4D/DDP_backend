"""
Registers Dalgo's Google OAuth client id/secret for ONE workspace (not instance-wide),
via Airbyte's public API workspace OAuth credential override:

    PUT /api/public/v1/workspaces/{workspaceId}/oauthCredentials

All values are read from the environment (DDP_backend/.env); only the workspace id is
passed on the command line:

    uv run python manage.py set_workspace_source_oauth_params --workspace-id <uuid>

Env used:
    AIRBYTE_SERVER_HOST, AIRBYTE_SERVER_PORT, AIRBYTE_API_TOKEN
    AIRBYTE_GSHEETS_OAUTH_CLIENT_ID, AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET
"""

import os
import requests
from django.core.management.base import BaseCommand

# Airbyte's short connector name for the Google Sheets source
GSHEETS_SOURCE_NAME = "google-sheets"


class Command(BaseCommand):
    """Set workspace-scoped Google OAuth params for the Google Sheets source"""

    help = "Register Dalgo's Google OAuth client id/secret for one Airbyte workspace"

    def add_arguments(self, parser):
        parser.add_argument(
            "--workspace-id", type=str, required=True, help="Airbyte workspace id to scope to"
        )

    def handle(self, *args, **options):
        workspace_id = options["workspace_id"]
        host = os.getenv("AIRBYTE_SERVER_HOST")
        port = os.getenv("AIRBYTE_SERVER_PORT")
        token = os.getenv("AIRBYTE_API_TOKEN")
        client_id = os.getenv("AIRBYTE_GSHEETS_OAUTH_CLIENT_ID")
        client_secret = os.getenv("AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET")

        missing = [
            name
            for name, val in {
                "AIRBYTE_SERVER_HOST": host,
                "AIRBYTE_SERVER_PORT": port,
                "AIRBYTE_API_TOKEN": token,
                "AIRBYTE_GSHEETS_OAUTH_CLIENT_ID": client_id,
                "AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET": client_secret,
            }.items()
            if not val
        ]
        if missing:
            self.stderr.write("Missing env vars: " + ", ".join(missing))
            return

        url = f"http://{host}:{port}/api/public/v1/workspaces/{workspace_id}/oauthCredentials"
        body = {
            "actorType": "source",
            "name": GSHEETS_SOURCE_NAME,
            "configuration": {"client_id": client_id, "client_secret": client_secret},
        }

        self.stdout.write(f"PUT {url}")
        res = requests.put(
            url,
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )

        if res.status_code in (200, 204):
            self.stdout.write(
                self.style.SUCCESS(f"Registered Google OAuth params for workspace {workspace_id}")
            )
        else:
            self.stderr.write(f"Failed [{res.status_code}]: {res.text}")
