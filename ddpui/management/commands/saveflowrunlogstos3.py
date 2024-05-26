import json
import boto3
from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import OrgTask
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service


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

    def fetch_airbyte_logs(self, org: str, s3):
        """fetches airbyte logs for and org"""
        for dataflow in OrgTask.objects.filter(
            org__slug=org,
            task__slug="airbyte-sync",
        ):
            connection_id = dataflow.connection_id
            result = airbyte_service.get_jobs_for_connection(connection_id)
            if len(result["jobs"]) == 0:
                continue
            for job in result["jobs"]:
                job_info = airbyte_service.parse_job_info(job)
                job_id = job_info["job_id"]
                logs = airbyte_service.get_logs_for_job(job_id)
                logs = logs["logs"]
                if len(logs["logLines"]) > 0:
                    s3key = f"logs/airbyte/{org}/{job_id}.json"
                    s3.put_object(
                        Bucket="dalgo-t4dai",
                        Key=s3key,
                        Body=json.dumps(logs),
                    )
                    print(f"Saved s3://dalgo-t4dai/{s3key}")

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
        self.fetch_airbyte_logs(org, s3)
