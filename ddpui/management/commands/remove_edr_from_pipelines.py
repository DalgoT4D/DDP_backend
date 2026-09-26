"""
Management command to remove EDR (generate-edr) tasks from existing Prefect
pipeline deployments. The data quality feature was removed but orgs that had
EDR selected in their pipeline still have the task in the Prefect deployment
parameters and in DataflowOrgTask. This command strips it from both.
"""

from django.core.management.base import BaseCommand
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask, TaskType
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.utils.unified_logger import get_logger

logger = get_logger()

EDR_TASK_SLUG = "generate-edr"


class Command(BaseCommand):
    help = "Remove EDR (generate-edr) tasks from Prefect deployments and DataflowOrgTask mappings"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org-slug",
            type=str,
            required=False,
            help="Only process a specific organization (optional)",
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
        self.stdout.write(f"  Pipelines skipped (no EDR task): {total_skipped}")
        self.stdout.write(f"  Errors: {total_errors}")

    def process_org(self, org: Org, dry_run: bool):
        dataflows = OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate")
        if not dataflows.exists():
            return 0, 0, 0

        self.stdout.write(f"\nOrg: {org.slug} ({org.name})")

        updated = 0
        skipped = 0
        errors = 0

        for dataflow in dataflows:
            edr_mappings = DataflowOrgTask.objects.filter(
                dataflow=dataflow,
                orgtask__task__slug=EDR_TASK_SLUG,
            )

            if not edr_mappings.exists():
                skipped += 1
                continue

            self.stdout.write(
                f"  → Found EDR task in pipeline: {dataflow.deployment_name} ({dataflow.deployment_id})"
            )

            if dry_run:
                self.stdout.write(
                    f"  [DRY RUN] Would remove EDR task from {dataflow.deployment_name}"
                )
                updated += 1
                continue

            try:
                self.remove_edr_from_pipeline(dataflow)
                self.stdout.write(
                    self.style.SUCCESS(f"  ✓ Removed EDR task from {dataflow.deployment_name}")
                )
                updated += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"  ✗ Failed to update {dataflow.deployment_name}: {str(e)}")
                )
                logger.error(
                    "Failed to remove EDR from pipeline %s: %s",
                    dataflow.deployment_name,
                    str(e),
                )
                errors += 1

        return updated, skipped, errors

    def remove_edr_from_pipeline(self, dataflow: OrgDataFlowv1):
        """Remove the EDR task from Prefect deployment params and DB mapping."""
        deployment = prefect_service.get_deployment(dataflow.deployment_id)

        params = deployment.get("parameters", {})
        tasks = params.get("config", {}).get("tasks", [])

        filtered_tasks = [t for t in tasks if t.get("slug") != EDR_TASK_SLUG]

        if len(filtered_tasks) < len(tasks):
            params["config"]["tasks"] = filtered_tasks
            update_payload = PrefectDataFlowUpdateSchema3(
                cron=dataflow.cron,
                deployment_params=params,
            )
            prefect_service.update_dataflow_v1(dataflow.deployment_id, update_payload)
        else:
            logger.warning(
                "EDR task not found in Prefect deployment params for %s — cleaning DB only",
                dataflow.deployment_name,
            )

        DataflowOrgTask.objects.filter(
            dataflow=dataflow,
            orgtask__task__slug=EDR_TASK_SLUG,
        ).delete()
