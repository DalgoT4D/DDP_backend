import json
import boto3
from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDataFlowv1
from ddpui.ddpprefect import prefect_service


class Command(BaseCommand):
    """fetches logs for an org and saves to s3 bucket"""

    help = "fetches prefect and airbyte logs for an org"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str)
        parser.add_argument("--aws-access-key-id", required=True)
        parser.add_argument("--aws-secret", required=True)

    def fetch_prefect_logs_from_deployments(self, org: str, s3):
        """fetches logs for all deployments of an org and saves them to s3 bucket"""
        for dataflow in OrgDataFlowv1.objects.filter(org__slug=org):
            flow_runs = prefect_service.get_flow_runs_by_deployment_id(
                dataflow.deployment_id
            )
            for flow_run in flow_runs:
                flow_run_id = flow_run["id"]
                logs_dict = prefect_service.get_flow_run_logs(flow_run_id, 0)
                if "logs" in logs_dict["logs"]:
                    flow_run_logs = logs_dict["logs"]["logs"]
                    s3key = f"logs/prefect/{org}/{flow_run_id}.json"
                    s3.put_object(
                        Bucket="dalgo-t4dai",
                        Key=s3key,
                        Body=json.dumps(flow_run_logs),
                    )
                    print(f"Saved s3://dalgo-t4dai/{s3key}")

    def fetch_airbyte_logs(self, org: str):
        """fetches airbyte logs for and org"""
        pass

    def handle(self, *args, **options):
        # Your command logic goes here
        org = options["org"]
        s3 = boto3.client(
            "s3",
            "ap-south-1",
            aws_access_key_id=options["aws_access_key_id"],
            aws_secret_access_key=options["aws_secret"],
        )
        self.fetch_prefect_logs_from_deployments(org, s3)
