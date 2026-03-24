"""
Management command to migrate a specific organization to a new queue.
Updates relevant dataflows for that org based on queue type and then updates the org's queue config.
"""

import os
import json
from typing import List, Set
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from ddpui.models.org import (
    Org,
    OrgDataFlowv1,
    OrgPrefectBlockv1,
    QueueConfigUpdateSchema,
    QueueDetailsSchema,
)
from ddpui.models.tasks import OrgTask, DataflowOrgTask, TaskType, Task
from ddpui.ddpprefect.prefect_service import prefect_get, prefect_put
from ddpui.ddpprefect import GITPULL
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.utils.constants import TASK_GENERATE_EDR, TASK_GITPULL, TASK_GITCLONE
from ddpui.utils.unified_logger import get_logger
from ddpui.core.pipelinefunctions import (
    setup_git_clone_shell_task_config,
    setup_git_shell_task_config,
)
from ddpui.core.orchestrate.pipeline_service import PipelineService

logger = get_logger()


class Command(BaseCommand):
    help = (
        "Migrate a specific organization to use a new queue and/or workpool for relevant dataflows"
    )

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
            choices=[
                "scheduled_pipeline_queue",
                "connection_sync_queue",
                "transform_task_queue",
                "edr_queue",
            ],
            help="Type of queue to migrate",
        )
        parser.add_argument(
            "--new-queue",
            type=str,
            required=True,
            help="New queue name to migrate to",
        )
        parser.add_argument(
            "--new-workpool",
            type=str,
            required=False,
            help="New workpool name to migrate to (if not provided, keeps existing workpool)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be changed without making actual changes",
        )
        parser.add_argument(
            "--is-workpool-eks",
            action="store_true",
            help="Mark the workpool as EKS workpool (default: False)",
        )

    def handle(self, *args, **options):
        org_slug: str = options["org_slug"]
        queue_type: str = options["queue_type"]
        new_queue: str = options["new_queue"]
        new_workpool: str = options.get("new_workpool")
        dry_run: bool = options["dry_run"]
        is_workpool_eks: bool = options["is_workpool_eks"]

        # Get the organization
        try:
            org: Org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            raise CommandError(f"Organization with slug '{org_slug}' not found")

        # Get current queue config to determine current workpool if new_workpool not provided
        current_queue_config = org.get_queue_config()
        current_queue_details = getattr(current_queue_config, queue_type)

        # Use provided workpool or keep existing one
        final_workpool = new_workpool if new_workpool else current_queue_details.workpool

        # Get relevant dataflows based on queue type
        dataflows: List[OrgDataFlowv1] = self.get_dataflows_for_queue_type(org, queue_type)

        # Show what will be done
        self.show_migration_plan(
            org,
            queue_type,
            new_queue,
            final_workpool,
            dataflows,
            dry_run,
            new_workpool is not None,
            is_workpool_eks,
        )

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry run completed. No changes made."))
            return

        # Confirm before proceeding
        workpool_change_msg = (
            f" and workpool to '{final_workpool}'"
            if new_workpool
            else " (keeping existing workpool)"
        )
        confirm = input(
            f"Migrate {org_slug} {queue_type} to queue '{new_queue}'{workpool_change_msg}? (y/N): "
        )
        if confirm.lower() != "y":
            self.stdout.write("Migration cancelled.")
            return

        # Perform the migration
        self.perform_migration(
            org, queue_type, new_queue, final_workpool, dataflows, is_workpool_eks
        )

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

        elif queue_type == "edr_queue":
            # Get manual dataflows with EDR (Elementary Data Reliability) tasks
            edr_org_tasks = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR)

            dataflow_ids: Set[str] = set()
            for org_task in edr_org_tasks:
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
        final_workpool: str,
        dataflows: List[OrgDataFlowv1],
        dry_run: bool,
        workpool_changing: bool,
        is_workpool_eks: bool,
    ):
        """Show what the migration will do"""
        action = "Would update" if dry_run else "Will update"

        self.stdout.write(f"\n{action} organization: {org.slug} ({org.name})")
        self.stdout.write(f"Queue type: {queue_type}")
        self.stdout.write(f"New queue: {new_queue}")

        if workpool_changing:
            self.stdout.write(f"New workpool: {final_workpool}")
        else:
            self.stdout.write(f"Workpool: {final_workpool} (unchanged)")

        self.stdout.write(f"Is EKS workpool: {is_workpool_eks}")

        self.stdout.write(f"Relevant dataflows to update: {len(dataflows)}")

        # Show current vs new configuration
        current_config = org.get_queue_config()
        current_details = getattr(current_config, queue_type)

        self.stdout.write("\nConfiguration change:")
        current_eks_status = getattr(current_details, "is_workpool_eks", False)
        self.stdout.write(
            f"  Current: queue='{current_details.name}', workpool='{current_details.workpool}', is_eks={current_eks_status}"
        )
        self.stdout.write(
            f"  New:     queue='{new_queue}', workpool='{final_workpool}', is_eks={is_workpool_eks}\n"
        )

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
        final_workpool: str,
        dataflows: List[OrgDataFlowv1],
        is_workpool_eks: bool,
    ):
        """Perform the actual migration with nested queue config"""
        self.stdout.write(
            f"\nMigrating {org.slug} {queue_type} to queue '{new_queue}' and workpool '{final_workpool}'..."
        )

        # Update dataflows in Prefect
        dataflow_errors = []
        updated_count = 0

        for dataflow in dataflows:
            try:
                # For scheduled pipelines, also handle git step replacement
                if queue_type == "scheduled_pipeline_queue":
                    self.switch_git_steps_for_scheduled_dataflow(
                        dataflow, new_queue, final_workpool, is_workpool_eks
                    )

                # Update queue/workpool for all dataflows
                self.update_dataflow_queue(dataflow, new_queue, final_workpool)
                self.stdout.write(f"  ✓ Updated dataflow: {dataflow.deployment_name}")
                updated_count += 1
            except Exception as e:
                error_msg = f"Failed to update dataflow {dataflow.deployment_name}: {str(e)}"
                dataflow_errors.append(error_msg)
                self.stdout.write(self.style.ERROR(f"  ✗ {error_msg}"))
                logger.error(error_msg)

        # Only update database configuration if ALL dataflows updated successfully
        if dataflow_errors:
            self.stdout.write(f"\nMigration failed:")
            self.stdout.write(f"✓ Dataflows updated: {updated_count}/{len(dataflows)}")
            self.stdout.write(
                self.style.ERROR(f"✗ {len(dataflow_errors)} dataflow update(s) failed")
            )
            self.stdout.write(
                self.style.ERROR("✗ Database config NOT updated due to dataflow failures")
            )
            self.stdout.write("Check logs for details.")
            raise CommandError("Migration failed due to dataflow update errors")

        # All dataflows updated successfully, now update database config
        try:
            # Create new queue details object
            new_queue_details = QueueDetailsSchema(
                name=new_queue, workpool=final_workpool, is_workpool_eks=is_workpool_eks
            )

            # Create update schema with the new nested structure
            update_data = QueueConfigUpdateSchema()
            setattr(update_data, queue_type, new_queue_details)

            # Apply the update
            org.update_queue_config(update_data)

            self.stdout.write(f"  ✓ Updated database config for {org.slug}")

        except Exception as e:
            error_msg = f"Failed to update database config: {str(e)}"
            self.stdout.write(self.style.ERROR(f"  ✗ {error_msg}"))
            logger.error(error_msg)
            raise CommandError(error_msg)

        # Summary - all successful
        self.stdout.write(f"\nMigration completed successfully:")
        self.stdout.write(f"✓ Dataflows updated: {updated_count}/{len(dataflows)}")
        self.stdout.write(f"✓ Database config updated")

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

    def switch_git_steps_for_scheduled_dataflow(
        self, dataflow: OrgDataFlowv1, new_queue: str, work_pool_name: str, is_workpool_eks: bool
    ):
        """Switch git steps (pull <-> clone) for scheduled dataflow and update queue/workpool"""

        try:
            # Get current pipeline details
            pipeline_details = PipelineService.get_pipeline_details(
                dataflow.org, dataflow.deployment_id
            )

            transform_tasks = pipeline_details.get("transformTasks", [])

            if not transform_tasks:
                # No transform tasks, nothing to replace
                self.stdout.write(f"  → No transform tasks found, no git steps to replace")
                return

            # Find and replace git pull/clone tasks in transformTasks
            if is_workpool_eks:
                # Replace git pull OrgTask with git clone OrgTask
                transform_tasks = self._replace_git_pull_orgtask_with_clone(
                    dataflow.org, transform_tasks
                )
                self.stdout.write(f"  → Replaced git pull OrgTask with git clone for EKS")
            else:
                # Replace git clone OrgTask with git pull OrgTask
                transform_tasks = self._replace_git_clone_orgtask_with_pull(
                    dataflow.org, transform_tasks
                )
                self.stdout.write(f"  → Replaced git clone OrgTask with git pull for EC2")

            # Create update payload with modified transformTasks
            update_payload = PrefectDataFlowUpdateSchema3(
                name=pipeline_details["name"],
                cron=pipeline_details["cron"],
                connections=pipeline_details["connections"],
                transformTasks=transform_tasks,  # Modified
            )

            # Update pipeline using PipelineService
            PipelineService.update_pipeline(dataflow.org, dataflow.deployment_id, update_payload)

            logger.info(
                f"Updated dataflow {dataflow.deployment_name} with queue {new_queue} and git step replacement"
            )

        except Exception as e:
            raise Exception(f"Migration error: {str(e)}")

    def _replace_git_pull_orgtask_with_clone(self, org: Org, transform_tasks: list) -> list:
        """Replace git pull OrgTask with git clone OrgTask for EKS migration"""

        updated_tasks = []
        git_steps_replaced = 0

        for task_info in transform_tasks:
            task_uuid = task_info["uuid"]
            seq = task_info["seq"]

            # Get the OrgTask
            orgtask = OrgTask.objects.filter(uuid=task_uuid).first()
            if not orgtask:
                logger.warning(f"OrgTask with uuid {task_uuid} not found")
                updated_tasks.append(task_info)
                continue

            # Check if this is a git pull task
            if orgtask.task.slug == TASK_GITPULL:
                # Find or create git clone OrgTask for this org
                git_clone_orgtask = self._get_or_create_git_clone_orgtask(org, orgtask)
                if git_clone_orgtask:
                    updated_tasks.append({"uuid": git_clone_orgtask.uuid, "seq": seq})
                    git_steps_replaced += 1
                    logger.info(f"Replaced git pull OrgTask with git clone (seq: {seq})")
                else:
                    logger.warning(f"Could not create git clone OrgTask for org {org.slug}")
                    updated_tasks.append(task_info)
            else:
                # Keep non-git-pull tasks as-is
                updated_tasks.append(task_info)

        if git_steps_replaced > 0:
            logger.info(
                f"Replaced {git_steps_replaced} git pull OrgTask(s) with git clone for org {org.slug}"
            )

        return updated_tasks

    def _replace_git_clone_orgtask_with_pull(self, org: Org, transform_tasks: list) -> list:
        """Replace git clone OrgTask with git pull OrgTask for rollback from EKS"""

        updated_tasks = []
        git_steps_replaced = 0

        for task_info in transform_tasks:
            task_uuid = task_info["uuid"]
            seq = task_info["seq"]

            # Get the OrgTask
            orgtask = OrgTask.objects.filter(uuid=task_uuid).first()
            if not orgtask:
                logger.warning(f"OrgTask with uuid {task_uuid} not found")
                updated_tasks.append(task_info)
                continue

            # Check if this is a git clone task
            if orgtask.task.slug == TASK_GITCLONE:
                # Find git pull OrgTask for this org
                git_pull_orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GITPULL).first()

                if git_pull_orgtask:
                    updated_tasks.append({"uuid": git_pull_orgtask.uuid, "seq": seq})
                    git_steps_replaced += 1
                    logger.info(f"Replaced git clone OrgTask with git pull (seq: {seq})")
                else:
                    logger.warning(f"Could not find git pull OrgTask for org {org.slug}")
                    updated_tasks.append(task_info)
            else:
                # Keep non-git-clone tasks as-is
                updated_tasks.append(task_info)

        if git_steps_replaced > 0:
            logger.info(
                f"Replaced {git_steps_replaced} git clone OrgTask(s) with git pull for org {org.slug}"
            )

        return updated_tasks

    def _get_or_create_git_clone_orgtask(self, org: Org, original_git_pull_orgtask: OrgTask):
        """Get existing git clone OrgTask or create one based on git pull OrgTask"""

        # First try to find existing git clone OrgTask
        existing_clone_orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GITCLONE).first()

        if existing_clone_orgtask:
            return existing_clone_orgtask

        # Create new git clone OrgTask based on git pull OrgTask
        try:
            git_clone_task = Task.objects.get(slug=TASK_GITCLONE)

            git_clone_orgtask = OrgTask.objects.create(
                org=org,
                task=git_clone_task,
                parameters=original_git_pull_orgtask.parameters,
                dbt_project_params=original_git_pull_orgtask.dbt_project_params,
                connection_id=original_git_pull_orgtask.connection_id,
            )

            logger.info(f"Created new git clone OrgTask for org {org.slug}")
            return git_clone_orgtask

        except Exception as e:
            logger.error(f"Failed to create git clone OrgTask: {str(e)}")
            return None
