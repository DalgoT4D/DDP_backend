"""for each org fetch orchestration pipelines for airbyte sync"""

from datetime import datetime
import pytz
from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.tasks import OrgTask
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET, UPDATE_SCHEMA
from ddpui.models.org import Org, ConnectionJob, ConnectionMeta
from ddpui.ddpairbyte.airbyte_service import get_jobs_for_connection, abreq


class Command(BaseCommand):
    """Docstring"""

    help = """This script is to figure out if the connection is large 
    enough to schedule related schema change & reset jobs for later"""

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--org", type=str, help="org slug, optional", required=True)
        parser.add_argument(
            "--connection_id",
            type=str,
            help="connection_id, optional; if not passed all connections of the org will be considered",
            required=False,
        )
        parser.add_argument(
            "--mark_as_large",
            action="store_true",
            help="mark the connection as large enough",
        )

    def handle(self, *args, **options):
        """for each org, fetch orchestration pipeline(s) for airbyte sync"""
        mark_as_large = options.get("mark_as_large", False)
        org_slug = options.get("org")

        org_tasks = OrgTask.objects.filter(
            org__slug=org_slug, task__slug=TASK_AIRBYTESYNC
        )
        if options.get("connection_id"):
            org_tasks = org_tasks.filter(connection_id=options.get("connection_id"))

        for org_task in org_tasks:
            # TODO: add the logic to fetch warehouse rows and figure out if the connection is large enough
            ConnectionMeta.objects.update_or_create(
                connection_id=org_task.connection_id,
                defaults={"schedule_large_jobs": mark_as_large},
            )
