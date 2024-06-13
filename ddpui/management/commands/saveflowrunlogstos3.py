import json
import boto3
from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import OrgTask
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service
from ddpui.models.tasks import Task


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

    def fetch_grouped_task_prefect_logs_from_deployments(self, org: str, s3):
        """
        fetches logs grouped by pipeline task for all deployments of an org and saves them to s3 bucket
        push only dbt related task's logs
        """
        dbt_tasks = Task.objects.filter(type="dbt").all()
        dbt_slugs = [task.slug for task in dbt_tasks]

        for dataflow in OrgDataFlowv1.objects.filter(org__slug=org):
            flow_runs = prefect_service.get_flow_runs_by_deployment_id(
                dataflow.deployment_id
            )
            for flow_run in flow_runs:
                flow_run_id = flow_run["id"]
                all_task_logs = prefect_service.get_flow_run_logs_v2(flow_run_id)
                for task_logs in all_task_logs:
                    if "logs" in task_logs:
                        label = task_logs["label"]
                        print(
                            f"Found logs for flow_run_id : {flow_run_id} and for task : {label}"
                        )

                        # check which slug is embedded in the label
                        # do string operation & get index of substring
                        task_slug = None
                        for slug in dbt_slugs:
                            if slug in label:
                                task_slug = slug
                                break

                        if task_slug is None:
                            continue

                        # upload logs for the current flow_run_id under the task_slug
                        flow_run_logs = task_logs["logs"]
                        s3key = f"logs/dbt/{org}/{flow_run_id}/{task_slug}.json"
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
        # self.fetch_prefect_logs_from_deployments(org, s3)
        self.fetch_grouped_task_prefect_logs_from_deployments(org, s3)
        # self.fetch_airbyte_logs(org, s3)
