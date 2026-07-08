"""
Registers Dalgo's Google OAuth client id/secret with Airbyte, instance-wide, so that
every workspace (all orgs, now and future) can run the "Sign in with Google" flow for
the Google Sheets source without the user ever entering credentials.

Run once per deploy (idempotent):
    uv run python manage.py set_airbyte_source_oauth_params

Reads AIRBYTE_GSHEETS_OAUTH_CLIENT_ID and AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET from env.
"""

import os
from django.core.management.base import BaseCommand
from ddpui.ddpairbyte import airbyte_service

# well-known Airbyte source-definition id for the Google Sheets connector
GSHEETS_SOURCE_DEFINITION_ID = "71607ba1-c0ac-4799-8049-7f4b90dd50f7"


class Command(BaseCommand):
    """Set instance-wide Google OAuth params for the Google Sheets source"""

    help = "Register Dalgo's Google OAuth client id/secret with Airbyte (instance-wide)"

    def handle(self, *args, **options):
        client_id = os.getenv("AIRBYTE_GSHEETS_OAUTH_CLIENT_ID")
        client_secret = os.getenv("AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET")
        if not client_id or not client_secret:
            self.stderr.write(
                "AIRBYTE_GSHEETS_OAUTH_CLIENT_ID and AIRBYTE_GSHEETS_OAUTH_CLIENT_SECRET "
                "must be set in the environment"
            )
            return

        airbyte_service.set_instancewide_source_oauth_params(
            GSHEETS_SOURCE_DEFINITION_ID,
            {"client_id": client_id, "client_secret": client_secret},
        )
        self.stdout.write(
            self.style.SUCCESS("Registered Google OAuth params for the Google Sheets source")
        )
