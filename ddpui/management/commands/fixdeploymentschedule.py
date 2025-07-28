"""set prefect deployment schedule from saved cron"""

import requests
from django.core.management.base import BaseCommand
from django.db.models import Q
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import OrgDataFlowv1
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3


class Command(BaseCommand):
    """set prefect deployment schedule from saved cron"""

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug")
        parser.add_argument("--action", choices=["show", "fix"])
        parser.add_argument("--deployment-id", help="optional")
        parser.add_argument("--verbose", action="store_true")

    def fetch_latest_flow_run(self, deployment_id: str) -> dict:
        """fetch the latest flow run for the deployment"""
        flow_runs = requests.post(
            "http://localhost:4200/api/flow_runs/filter",
            json={
                "sort": "EXPECTED_START_TIME_DESC",
                "limit": 1,
                "deployments": {
                    "operator": "and_",
                    "id": {"any_": [deployment_id]},
                },
            },
            timeout=10,
        ).json()
        if flow_runs:
            flow_run = flow_runs[0]
            return flow_run

    def handle(self, *args, **options):
        # fetch deployments which have a cron in our db
        query = Q(dataflow_type="orchestrate", cron__isnull=False) & ~Q(cron="")
        if options["org"]:
            query &= Q(org__slug=options["org"])
        if options["deployment_id"]:
            query &= Q(deployment_id=options["deployment_id"])

        for odf in OrgDataFlowv1.objects.filter(query):
            # fetch the corresponding prefect object
            deployment = prefect_service.get_deployment(odf.deployment_id)

            # does it have a cron?
            if deployment["cron"]:
                if options["verbose"]:
                    self.stdout.write(
                        f"{odf.org.slug} deployment {odf.deployment_name} has a cron! {deployment['cron']} ... vs saved {odf.cron}"
                    )
                continue

            # if there is no cron, then this should be False
            assert not deployment["isScheduleActive"]

            latest_flow_run = self.fetch_latest_flow_run(odf.deployment_id)
            if options["action"] == "show":
                if latest_flow_run:
                    last_run_date = latest_flow_run["expected_start_time"][: len("yyyy-mm-dd")]
                    self.stdout.write(
                        " / ".join(
                            map(
                                str,
                                [
                                    odf.org.slug,
                                    odf.deployment_id,
                                    odf.deployment_name,
                                    last_run_date,
                                    odf.cron,
                                ],
                            )
                        )
                    )

            elif options["action"] == "fix":
                prefect_service.update_dataflow_v1(
                    odf.deployment_id,
                    PrefectDataFlowUpdateSchema3(
                        cron=odf.cron, deployment_params=deployment["parameters"]
                    ),
                )
                if latest_flow_run and not odf.cron.endswith("*"):
                    last_run_date = latest_flow_run["expected_start_time"][: len("yyyy-mm-dd")]
                    self.stdout.write(
                        f"do you need to run the deployment now? deployment is {odf.deployment_name} cron is {odf.cron} last run was on {last_run_date}"
                    )
