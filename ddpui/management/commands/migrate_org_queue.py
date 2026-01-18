"""
Management command to migrate a specific organization to a new queue.
Updates relevant dataflows for that org based on queue type and then updates the org's queue config.
"""

import os
from typing import List, Set
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from ddpui.models.org import Org, QueueConfigUpdateSchema
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import OrgTask, DataflowOrgTask, TaskType
from ddpui.ddpprefect.prefect_service import prefect_get, prefect_put
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("migrate_org_queue")


class Command(BaseCommand):
    help = "Migrate a specific organization to use a new queue for relevant dataflows"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org-slug",
            type=str,
            required=True,
            help="Organization slug to migrate",
        )
        parser.add_argument(
            "--queue-type",
            type=str,
            required=True,
            choices=["scheduled_pipeline_queue", "connection_sync_queue", "transform_task_queue"],
            help="Type of queue to migrate",
        )
        parser.add_argument(
            "--new-queue",
            type=str,
            required=True,
            help="New queue name to migrate to",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be changed without making actual changes",
        )

    def handle(self, *args, **options):
        org_slug: str = options["org_slug"]
        queue_type: str = options["queue_type"]
        new_queue: str = options["new_queue"]
        dry_run: bool = options["dry_run"]

        # Get work pool name from environment
        work_pool_name: str = os.getenv("PREFECT_WORKER_POOL_NAME")

        if not work_pool_name:
            raise CommandError("PREFECT_WORKER_POOL_NAME environment variable is not set")

        # Get the organization
        try:
            org: Org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            raise CommandError(f"Organization with slug '{org_slug}' not found")

        # Get relevant dataflows based on queue type
        dataflows: List[OrgDataFlowv1] = self.get_dataflows_for_queue_type(org, queue_type)

        # Show what will be done
        self.show_migration_plan(org, queue_type, new_queue, work_pool_name, dataflows, dry_run)

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry run completed. No changes made."))
            return

        if not dataflows:
            self.stdout.write(self.style.WARNING("No dataflows found to migrate."))
            return

        # Confirm before proceeding
        confirm = input(f"Migrate {org_slug} {queue_type} to queue '{new_queue}'? (y/N): ")
        if confirm.lower() != "y":
            self.stdout.write("Migration cancelled.")
            return

        # Perform the migration
        self.perform_migration(org, queue_type, new_queue, work_pool_name, dataflows)

    def get_dataflows_for_queue_type(self, org: Org, queue_type: str) -> List[OrgDataFlowv1]:
        """Get relevant dataflows based on queue type"""

        if queue_type == "scheduled_pipeline_queue":
            # Get all orchestrate dataflows
            return list(OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate"))

        elif queue_type == "connection_sync_queue":
            # Get manual dataflows with airbyte tasks
            airbyte_org_tasks = OrgTask.objects.filter(org=org, task__type=TaskType.AIRBYTE)

            dataflow_ids: Set[str] = set()
            for org_task in airbyte_org_tasks:
                dataflow_org_tasks = DataflowOrgTask.objects.filter(orgtask=org_task)
                for dot in dataflow_org_tasks:
                    if dot.dataflow:
                        dataflow_ids.add(dot.dataflow.id)

            return list(OrgDataFlowv1.objects.filter(id__in=dataflow_ids, dataflow_type="manual"))

        elif queue_type == "transform_task_queue":
            # Get manual dataflows with dbt, github, or dbtcloud tasks
            transform_org_tasks = OrgTask.objects.filter(
                org=org, task__type__in=[TaskType.DBT, TaskType.GIT, TaskType.DBTCLOUD]
            )

            dataflow_ids: Set[str] = set()
            for org_task in transform_org_tasks:
                dataflow_org_tasks = DataflowOrgTask.objects.filter(orgtask=org_task)
                for dot in dataflow_org_tasks:
                    if dot.dataflow:
                        dataflow_ids.add(dot.dataflow.id)

            return list(OrgDataFlowv1.objects.filter(id__in=dataflow_ids, dataflow_type="manual"))

        return []

    def show_migration_plan(
        self,
        org: Org,
        queue_type: str,
        new_queue: str,
        work_pool_name: str,
        dataflows: List[OrgDataFlowv1],
        dry_run: bool,
    ):
        """Show what the migration will do"""
        action = "Would update" if dry_run else "Will update"

        self.stdout.write(f"\n{action} organization: {org.slug} ({org.name})")
        self.stdout.write(f"Queue type: {queue_type}")
        self.stdout.write(f"New queue: {new_queue}")
        self.stdout.write(f"Work pool: {work_pool_name}")
        self.stdout.write(f"Relevant dataflows to update: {len(dataflows)}\n")

        if dataflows:
            for dataflow in dataflows:
                cron_info = f" (cron: {dataflow.cron})" if dataflow.cron else " (no cron)"
                dataflow_type_info = f" [{dataflow.dataflow_type}]"
                self.stdout.write(
                    f"- {dataflow.deployment_name} (ID: {dataflow.deployment_id}){dataflow_type_info}{cron_info}"
                )
        else:
            self.stdout.write("No relevant dataflows found for this queue type")

    def perform_migration(
        self,
        org: Org,
        queue_type: str,
        new_queue: str,
        work_pool_name: str,
        dataflows: List[OrgDataFlowv1],
    ):
        """Perform the actual migration"""
        self.stdout.write(f"\nMigrating {org.slug} {queue_type} to queue '{new_queue}'...")

        # Update dataflows in Prefect
        dataflow_errors = []
        updated_count = 0

        for dataflow in dataflows:
            try:
                self.update_dataflow_queue(dataflow, new_queue, work_pool_name)
                self.stdout.write(f"  ✓ Updated dataflow: {dataflow.deployment_name}")
                updated_count += 1
            except Exception as e:
                error_msg = f"Failed to update dataflow {dataflow.deployment_name}: {str(e)}"
                dataflow_errors.append(error_msg)
                self.stdout.write(self.style.ERROR(f"  ✗ {error_msg}"))
                logger.error(error_msg)

        # Update database configuration
        try:
            update_data = QueueConfigUpdateSchema()
            setattr(update_data, queue_type, new_queue)
            org.update_queue_config(update_data)

            self.stdout.write(f"  ✓ Updated database config for {org.slug}")

        except Exception as e:
            error_msg = f"Failed to update database config: {str(e)}"
            self.stdout.write(self.style.ERROR(f"  ✗ {error_msg}"))
            logger.error(error_msg)
            raise CommandError(error_msg)

        # Summary
        self.stdout.write(f"\nMigration completed:")
        self.stdout.write(f"✓ Dataflows updated: {updated_count}/{len(dataflows)}")
        self.stdout.write(f"✓ Database config updated")

        if dataflow_errors:
            self.stdout.write(
                self.style.WARNING(f"⚠ {len(dataflow_errors)} dataflow update(s) failed")
            )
            self.stdout.write("Check logs for details.")

    def update_dataflow_queue(self, dataflow: OrgDataFlowv1, new_queue: str, work_pool_name: str):
        """Update a Prefect dataflow to use a new queue"""
        try:
            # Get current deployment from Prefect
            prefect_deployment = prefect_get(f"deployments/{dataflow.deployment_id}")
            if not prefect_deployment:
                raise Exception(f"Deployment {dataflow.deployment_id} not found in Prefect")

            # Get existing parameters
            deployment_params = prefect_deployment.get("parameters", {})

            # Prepare update payload
            update_payload = {
                "deployment_params": deployment_params,
                "work_pool_name": work_pool_name,
                "work_queue_name": new_queue,
            }

            # Add cron if it exists in the OrgDataFlowv1
            if dataflow.cron:
                update_payload["cron"] = dataflow.cron

            # Update deployment in Prefect
            result = prefect_put(f"v1/deployments/{dataflow.deployment_id}", update_payload)

            if not result:
                raise Exception("Failed to update dataflow in Prefect")

            logger.info(f"Updated dataflow {dataflow.deployment_name} to use queue {new_queue}")

        except Exception as e:
            raise Exception(f"Prefect API error: {str(e)}")
