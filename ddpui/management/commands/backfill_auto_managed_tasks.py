"""
Management command to backfill auto-managed tasks (git pull/clone, dbt clean, dbt deps)
in all existing pipelines.

For each orchestrate pipeline that has transform tasks, this command will
re-run update_pipeline which automatically adds the missing auto-managed steps
based on the org's workpool configuration.
"""

from django.core.management.base import BaseCommand
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask, TaskType
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.ddpprefect import prefect_service
from ddpui.utils.constants import TASK_DBTCLEAN, TASK_DBTDEPS
from ddpui.utils.unified_logger import get_logger
from ddpui.core.orchestrate.pipeline_service import PipelineService

logger = get_logger()


class Command(BaseCommand):
    help = "Backfill auto-managed tasks (git pull/clone, dbt clean, dbt deps) in all existing pipelines"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org-slug",
            type=str,
            required=False,
            help="Only backfill for a specific organization (optional)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be changed without making actual changes",
        )

    def handle(self, *args, **options):
        org_slug = options.get("org_slug")
        dry_run = options["dry_run"]

        if org_slug:
            orgs = Org.objects.filter(slug=org_slug)
            if not orgs.exists():
                self.stdout.write(self.style.ERROR(f"Organization '{org_slug}' not found"))
                return
        else:
            orgs = Org.objects.all()

        total_updated = 0
        total_skipped = 0
        total_errors = 0

        for org in orgs:
            updated, skipped, errors = self.process_org(org, dry_run)
            total_updated += updated
            total_skipped += skipped
            total_errors += errors

        self.stdout.write(f"\n{'[DRY RUN] ' if dry_run else ''}Summary:")
        self.stdout.write(f"  Pipelines updated: {total_updated}")
        self.stdout.write(f"  Pipelines skipped (no transform tasks): {total_skipped}")
        self.stdout.write(f"  Errors: {total_errors}")

    def process_org(self, org: Org, dry_run: bool):
        """Process all orchestrate pipelines for an organization"""
        dataflows = OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate")

        if not dataflows.exists():
            return 0, 0, 0

        self.stdout.write(f"\nOrg: {org.slug} ({org.name})")

        updated = 0
        skipped = 0
        errors = 0

        for dataflow in dataflows:
            # Check if this pipeline has transform tasks
            has_transform = DataflowOrgTask.objects.filter(
                dataflow=dataflow,
                orgtask__task__type=TaskType.DBT,
            ).exists()

            if not has_transform:
                self.stdout.write(f"  → Skipping {dataflow.deployment_name} (no transform tasks)")
                skipped += 1
                continue

            # Check if dbt-clean and dbt-deps are already present
            has_dbt_clean = DataflowOrgTask.objects.filter(
                dataflow=dataflow, orgtask__task__slug=TASK_DBTCLEAN
            ).exists()
            has_dbt_deps = DataflowOrgTask.objects.filter(
                dataflow=dataflow, orgtask__task__slug=TASK_DBTDEPS
            ).exists()

            if has_dbt_clean and has_dbt_deps:
                self.stdout.write(
                    f"  → Skipping {dataflow.deployment_name} (already has dbt-clean and dbt-deps)"
                )
                skipped += 1
                continue

            missing = []
            if not has_dbt_clean:
                missing.append("dbt-clean")
            if not has_dbt_deps:
                missing.append("dbt-deps")

            if dry_run:
                self.stdout.write(
                    f"  [DRY RUN] Would update {dataflow.deployment_name} "
                    f"(missing: {', '.join(missing)})"
                )
                updated += 1
                continue

            try:
                self.update_pipeline(org, dataflow)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ Updated {dataflow.deployment_name} (added: {', '.join(missing)})"
                    )
                )
                updated += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"  ✗ Failed to update {dataflow.deployment_name}: {str(e)}")
                )
                logger.error(
                    f"Failed to backfill auto-managed tasks for {dataflow.deployment_name}: {str(e)}"
                )
                errors += 1

        return updated, skipped, errors

    def update_pipeline(self, org: Org, dataflow: OrgDataFlowv1):
        """Re-run update_pipeline to backfill auto-managed tasks"""
        pipeline_details = PipelineService.get_pipeline_details(org, dataflow.deployment_id)

        transform_tasks = pipeline_details.get("transformTasks", [])

        # Convert UUIDs to strings for Pydantic validation
        transform_tasks_str = [
            {"uuid": str(task["uuid"]), "seq": task["seq"]} for task in transform_tasks
        ]

        update_payload = PrefectDataFlowUpdateSchema3(
            name=pipeline_details["name"],
            cron=pipeline_details["cron"],
            connections=pipeline_details["connections"],
            transformTasks=transform_tasks_str,
        )

        PipelineService.update_pipeline(org, dataflow.deployment_id, update_payload)

        # Toggle schedule inactive → active to clear pre-scheduled runs.
        # Prefect schedules runs 1-2 days in advance; those won't pick up the
        # updated deployment params unless the schedule is reset.
        # Only do this for pipelines that have an active schedule.
        if dataflow.cron and pipeline_details.get("isScheduleActive", False):
            PipelineService.set_pipeline_schedule(org, dataflow.deployment_id, "inactive")
            PipelineService.set_pipeline_schedule(org, dataflow.deployment_id, "active")
