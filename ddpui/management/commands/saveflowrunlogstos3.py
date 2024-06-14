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

    SUCCESS_LOGS = "success"  # grouping logs in s3 by their status
    FAILURE_LOGS = "failure"  # grouping logs in s3 by their status

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
        # dbt_tasks = Task.objects.filter(type="dbt").all()
        dbt_slugs = ["dbt-test", "dbt-run"]
        summ = {
            "dbt-test": {self.SUCCESS_LOGS: 0, self.FAILURE_LOGS: 0},
            "dbt-run": {self.SUCCESS_LOGS: 0, self.FAILURE_LOGS: 0},
        }

        for dataflow in OrgDataFlowv1.objects.filter(org__slug=org):
            flow_runs = prefect_service.get_flow_runs_by_deployment_id(
                dataflow.deployment_id, 100
            )
            for flow_run in flow_runs:
                status = None
                flow_run_id = flow_run["id"]
                all_task_logs = prefect_service.get_flow_run_logs_v2(flow_run_id)
                for task_logs in all_task_logs:
                    if "logs" in task_logs and len(task_logs["logs"]) > 0:
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

                        # set status of the task run
                        if task_slug == "dbt-test":
                            if (
                                task_logs["state_type"] == "FAILED"
                                or flow_run["state_name"] == "DBT_TEST_FAILED"
                            ):
                                status = self.FAILURE_LOGS
                            elif task_logs["state_type"] == "COMPLETED":
                                status = self.SUCCESS_LOGS
                        elif task_slug == "dbt-run":
                            if task_logs["state_type"] == "COMPLETED":
                                status = self.SUCCESS_LOGS
                            elif task_logs["state_type"] == "FAILED":
                                status = self.FAILURE_LOGS

                        # upload logs for the current flow_run_id under the task_slug
                        flow_run_logs = task_logs["logs"]

                        if status is not None:
                            summ[task_slug][status] += 1
                            print(
                                f"Found {status} logs for task : {task_slug} in flow_run : {flow_run_id}"
                            )
                            s3key = f"logs/dbt/{org}/{flow_run_id}/{task_slug}/{status}.json"
                            s3.put_object(
                                Bucket="dalgo-t4dai",
                                Key=s3key,
                                Body=json.dumps(flow_run_logs),
                            )
                            print(f"Saved s3://dalgo-t4dai/{s3key}")

        print(summ)

    def fetch_airbyte_logs(self, org: str, s3):
        """fetches airbyte logs for and org"""
        summ = []
        for dataflow in OrgTask.objects.filter(
            org__slug=org,
            task__slug="airbyte-sync",
        ):
            cnt_failure = 0
            cnt_success = 0
            connection_id = dataflow.connection_id
            # get recent 100 sync for this connection
            result = airbyte_service.get_jobs_for_connection(connection_id, 100, 0)
            if len(result["jobs"]) == 0:
                continue
            for job in result["jobs"]:
                status = None
                jobinfo = job["job"]
                attempts = job["attempts"]
                job_id = jobinfo["id"]
                logs = {"logLines": []}
                if jobinfo["status"] == "succeeded":
                    for attempt in attempts:
                        if attempt["status"] == "succeeded":
                            print(
                                f"Found success logs for connection id : {connection_id} job_id : {job_id}"
                            )
                            status = self.SUCCESS_LOGS
                            logs = airbyte_service.get_logs_for_job(
                                job_id, attempt["id"]
                            )
                            logs = logs["logs"]
                            break
                elif jobinfo["status"] == "failed":
                    for attempt in attempts:
                        if attempt["status"] == "failed":
                            print(
                                f"Found failure logs for connection id : {connection_id} job_id : {job_id}"
                            )
                            status = self.FAILURE_LOGS
                            logs = airbyte_service.get_logs_for_job(
                                job_id, attempt["id"]
                            )
                            logs = logs["logs"]
                            break

                if len(logs["logLines"]) > 0 and status is not None:
                    s3key = f"logs/airbyte/{org}/{job_id}/{status}.json"
                    if status == self.SUCCESS_LOGS:
                        cnt_success += 1
                    if status == self.FAILURE_LOGS:
                        cnt_failure += 1
                    s3.put_object(
                        Bucket="dalgo-t4dai",
                        Key=s3key,
                        Body=json.dumps(logs),
                    )
                    print(f"Saved s3://dalgo-t4dai/{s3key}")

            summ.append(
                f"Saved {cnt_success} success logs, {cnt_failure} failure logs to s3 for connection: {connection_id}"
            )
        # print summary
        print("\n".join(summ))

    def handle(self, *args, **options):
        # Your command logic goes here
        org = options["org"]
        s3 = boto3.client(
            "s3",
            "ap-south-1",
            aws_access_key_id=options["aws_access_key_id"],
            aws_secret_access_key=options["aws_secret"],
        )
        self.fetch_airbyte_logs(org, s3)
        self.fetch_grouped_task_prefect_logs_from_deployments(org, s3)
        # self.fetch_prefect_logs_from_deployments(org, s3)
