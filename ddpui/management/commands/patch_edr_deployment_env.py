"""Rebuild the deployment_params of existing EDR (generate-edr) deployments
using the current `setup_edr_send_report_task_config` helper, so any changes
to that helper (env keys, command args, etc.) roll out to existing scheduled
deployments without hand-editing each one.

For each EDR OrgTask → DataflowOrgTask → OrgDataFlowv1:
  1. Regenerate the task_config via setup_edr_send_report_task_config.
  2. Rebuild the deployment_params:
        {"config": {"tasks": [task_config.to_json()], "org_slug": org.slug}}
  3. Push via update_dataflow_v1.

Assumes the EDR deployment is a single-task deployment (the shape
`ensure_edr_sendreport_dataflow` emits). If a deployment mixes EDR with
other tasks (e.g. once EDR is added to full pipelines), this script would
overwrite those — extend then.

Idempotent — safe to re-run.
"""

from django.core.management.base import BaseCommand

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.ddpprefect.prefect_service import update_dataflow_v1
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.models.tasks import DataflowOrgTask, OrgTask
from ddpui.utils.constants import TASK_GENERATE_EDR


class Command(BaseCommand):
    help = "Rebuild EDR deployment_params from setup_edr_send_report_task_config"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug (default: all orgs)")
        parser.add_argument(
            "--dry-run", action="store_true", help="Print what would happen; no writes"
        )

    def handle(self, *args, **options):
        qs = OrgTask.objects.filter(task__slug=TASK_GENERATE_EDR).select_related("org")
        if options["org"]:
            qs = qs.filter(org__slug=options["org"])

        if qs.count() == 0:
            self.stdout.write("No EDR OrgTasks found")
            return

        patched, skipped = 0, 0

        for orgtask in qs:
            org = orgtask.org
            orgdbt = org.dbt
            if orgdbt is None or orgdbt.dbt_profile_secret_block is None:
                self.stdout.write(f"  [skip] {org.slug}: no dbt_profile_secret_block FK")
                skipped += 1
                continue

            try:
                dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, orgdbt)
            except Exception as err:  # pylint: disable=broad-exception-caught
                self.stdout.write(f"  [skip] {org.slug}: gather_dbt_project_params failed ({err})")
                skipped += 1
                continue

            task_config = setup_edr_send_report_task_config(orgtask, dbt_project_params.project_dir)
            new_params = {
                "config": {
                    "tasks": [task_config.to_json()],
                    "org_slug": org.slug,
                }
            }

            for dot in DataflowOrgTask.objects.filter(orgtask=orgtask).select_related("dataflow"):
                dataflow = dot.dataflow
                if options["dry_run"]:
                    self.stdout.write(
                        f"  [dry-run] {dataflow.deployment_name}: would rebuild deployment_params"
                    )
                    patched += 1
                    continue

                try:
                    update_dataflow_v1(
                        dataflow.deployment_id,
                        PrefectDataFlowUpdateSchema3(
                            deployment_params=new_params, cron=dataflow.cron
                        ),
                    )
                    patched += 1
                    self.stdout.write(f"  [patch] {dataflow.deployment_name}")
                except Exception as err:  # pylint: disable=broad-exception-caught
                    self.stdout.write(f"  [warn] {dataflow.deployment_name}: {err}")

        self.stdout.write(f"\nDone. patched={patched} skipped={skipped}")
