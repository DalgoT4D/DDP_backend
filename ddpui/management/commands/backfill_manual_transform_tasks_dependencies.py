"""
Backfill the git-pull/dbt-clean/dbt-deps dependency chain into every manual
(Transform-tab) dbt deployment, and create chained deployments for any
LONG_RUNNING dbt OrgTask that is currently missing one.

Only touches manual deployments; orchestrated pipelines are handled by
`backfill_auto_managed_tasks`.

Usage:
    ./manage.py backfill_manual_transform_tasks_dependencies [--org <slug>] [--dry-run]
"""

from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.constants import LONG_RUNNING_TASKS
from ddpui.models.tasks import OrgTask, DataflowOrgTask, TaskType
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.orgtaskfunctions import (
    get_transform_task_queue,
    create_prefect_deployment_for_dbtcore_task,
)
from ddpui.core.pipelinefunctions import pipeline_with_orgtasks
from ddpui.core.orchestrate.pipeline_service import PipelineService

logger = CustomLogger("ddpui")
load_dotenv()


class Command(BaseCommand):
    help = (
        "Chain git-pull/dbt-clean/dbt-deps into existing manual dbt deployments "
        "and create missing chained deployments for LONG_RUNNING dbt OrgTasks."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            type=str,
            default=None,
            help="Slug of a single org; if omitted, all orgs are migrated.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would happen without touching Prefect or DB.",
        )

    def handle(self, *args, **options):
        org_slug = options.get("org")
        dry_run = options.get("dry_run", False)

        qs = Org.objects.all()
        if org_slug:
            qs = qs.filter(slug=org_slug)
            if not qs.exists():
                self.stderr.write(f"no org found with slug={org_slug}")
                return

        totals = {
            "processed": 0,
            "skipped_org": 0,
            "updated": 0,
            "created": 0,
            "orphans_flagged": 0,
            "errored_org": 0,
        }

        for org in qs:
            self.stdout.write(f"\n=== org={org.slug} ===")
            try:
                per_org = self._migrate_org(org, dry_run)
                totals["updated"] += per_org["updated"]
                totals["created"] += per_org["created"]
                totals["orphans_flagged"] += per_org["orphans_flagged"]
                if per_org["skipped_org"]:
                    totals["skipped_org"] += 1
                totals["processed"] += 1
            except Exception as err:  # pylint: disable=broad-except
                logger.exception(err)
                self.stderr.write(f"[{org.slug}] fatal error: {err}")
                totals["errored_org"] += 1

        self.stdout.write("\n=== summary ===")
        for key, value in totals.items():
            self.stdout.write(f"  {key}: {value}")

    def _migrate_org(self, org: Org, dry_run: bool) -> dict:
        result = {"skipped_org": False, "updated": 0, "created": 0, "orphans_flagged": 0}

        # preconditions
        if org.dbt is None:
            self.stdout.write("  skip: dbt not configured")
            result["skipped_org"] = True
            return result
        if org.dbt.cli_profile_block is None:
            self.stdout.write("  skip: cli_profile_block missing")
            result["skipped_org"] = True
            return result
        if not org.dbt.gitrepo_url:
            self.stdout.write("  skip: gitrepo_url missing")
            result["skipped_org"] = True
            return result

        transform_queue = get_transform_task_queue(org)
        is_eks = getattr(transform_queue, "is_workpool_eks", False)

        # ensure prep OrgTasks exist so the chain can reference them
        if not dry_run:
            if is_eks:
                PipelineService.get_or_create_git_clone_orgtask(org)
            else:
                PipelineService.get_or_create_git_pull_orgtask(org)
            PipelineService.get_or_create_dbt_clean_orgtask(org)
            PipelineService.get_or_create_dbt_deps_orgtask(org)

        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        cli_profile_block = org.dbt.cli_profile_block

        # Diagnostic: report LONG_RUNNING OrgTasks that have no manual deployment
        # attached. DBT-type orphans will be adopted by Step B below; DBTCLOUD
        # orphans stay flagged since this command can't create dbt-cloud deployments.
        orphans = [
            ot
            for ot in OrgTask.objects.filter(
                org=org, task__slug__in=LONG_RUNNING_TASKS
            ).select_related("task")
            if not DataflowOrgTask.objects.filter(
                orgtask=ot, dataflow__dataflow_type="manual"
            ).exists()
        ]
        if orphans:
            self.stdout.write(
                f"  [flag] {len(orphans)} orphan LONG_RUNNING orgtask(s) with no manual deployment:"
            )
            for ot in orphans:
                self.stdout.write(
                    f"      slug={ot.task.slug:16s} type={ot.task.type:9s} "
                    f"uuid={ot.uuid} generated_by={ot.generated_by}"
                )
            result["orphans_flagged"] = len(orphans)

        # Step A: update every manual dbt dataflow — always overwrite prefect
        # deployment payload and always wipe+recreate DataflowOrgTask mappings
        manual_dataflows = OrgDataFlowv1.objects.filter(
            org=org,
            dataflow_type="manual",
        ).prefetch_related("datafloworgtasks__orgtask__task")

        for dataflow in manual_dataflows:
            # primary = last-by-seq mapped orgtask that is a LONG_RUNNING dbt task
            primary = None
            for dfot in sorted(
                dataflow.datafloworgtasks.all(),
                key=lambda d: d.seq,
                reverse=True,
            ):
                ot = dfot.orgtask
                if ot.task.type == TaskType.DBT and ot.task.slug in LONG_RUNNING_TASKS:
                    primary = ot
                    break
            if primary is None:
                # dbt-cloud-job or orphan — not migratable through this path
                continue

            # build the desired chain
            if is_eks:
                git_orgtask = PipelineService.get_or_create_git_clone_orgtask(org)
            else:
                git_orgtask = PipelineService.get_or_create_git_pull_orgtask(org)

            chain = [
                git_orgtask,
                PipelineService.get_or_create_dbt_clean_orgtask(org),
                PipelineService.get_or_create_dbt_deps_orgtask(org),
                primary,
            ]

            new_task_configs, err = pipeline_with_orgtasks(
                org,
                chain,
                cli_block=cli_profile_block,
                dbt_project_params=dbt_project_params,
                gitrepo_url=org.dbt.gitrepo_url,
            )
            if err:
                self.stderr.write(f"  [update] {primary.task.slug}: build failed: {err}")
                continue

            new_deployment_params = {"config": {"tasks": new_task_configs, "org_slug": org.slug}}

            if dry_run:
                self.stdout.write(
                    f"  [DRY] [update] {primary.task.slug} ({dataflow.deployment_id}): "
                    f"would set {len(new_task_configs)} chained tasks and rewrite mappings"
                )
                result["updated"] += 1
                continue

            try:
                prefect_service.update_dataflow_v1(
                    dataflow.deployment_id,
                    PrefectDataFlowUpdateSchema3(
                        deployment_params=new_deployment_params,
                        cron=dataflow.cron,
                    ),
                )
            except Exception as err:  # pylint: disable=broad-except
                self.stderr.write(f"  [update] {primary.task.slug}: prefect update failed: {err}")
                continue

            # wipe + recreate DataflowOrgTask mappings so seq matches chain order
            DataflowOrgTask.objects.filter(dataflow=dataflow).delete()
            for idx, orgtask in enumerate(chain):
                DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask, seq=idx)

            self.stdout.write(
                f"  [update] {primary.task.slug}: rewrote {dataflow.deployment_id} "
                f"(chain of {len(chain)})"
            )
            result["updated"] += 1

        # Step B: create deployments for LONG_RUNNING dbt OrgTasks without one
        eligible = OrgTask.objects.filter(
            org=org,
            task__slug__in=LONG_RUNNING_TASKS,
            task__type=TaskType.DBT,
        ).select_related("task")

        for orgtask in eligible:
            has_manual = DataflowOrgTask.objects.filter(
                orgtask=orgtask, dataflow__dataflow_type="manual"
            ).exists()
            if has_manual:
                continue

            if dry_run:
                self.stdout.write(
                    f"  [DRY] [create] {orgtask.task.slug}: would create chained deployment"
                )
                result["created"] += 1
                continue

            try:
                create_prefect_deployment_for_dbtcore_task(
                    orgtask, cli_profile_block, dbt_project_params
                )
                self.stdout.write(f"  [create] {orgtask.task.slug}: created")
                result["created"] += 1
            except Exception as err:  # pylint: disable=broad-except
                self.stderr.write(f"  [create] {orgtask.task.slug}: failed: {err}")

        return result
