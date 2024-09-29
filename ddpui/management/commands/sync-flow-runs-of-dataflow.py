"""for each org fetch orchestration pipelines for airbyte sync"""

from datetime import datetime
import pytz
from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.tasks import DataflowOrgTask
from ddpui.celeryworkers.tasks import sync_flow_runs_of_deployments
from ddpui.models.org import OrgDataFlowv1


class Command(BaseCommand):
    """Docstring"""

    help = "Syncs the flow runs of deployment by reading it from prefect"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--orgslug", type=str, help="org slug; optional")
        parser.add_argument(
            "--lookbackhours",
            type=int,
            help="will sync flow runs from last these many hours till now; default is 24hrs",
            default=24,
        )

    def handle(self, *args, **options):
        """for each org, fetch orchestration pipeline(s) for airbyte sync"""
        query = OrgDataFlowv1.objects

        if options["orgslug"]:
            query = query.filter(org__slug=options["orgslug"])

        deployment_ids = [flow.deployment_id for flow in query.all() if flow.deployment_id]
        sync_flow_runs_of_deployments(
            deployment_ids=deployment_ids, look_back_hours=options["lookbackhours"]
        )
