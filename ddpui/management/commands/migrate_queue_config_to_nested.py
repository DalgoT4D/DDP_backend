"""
Management command to migrate queue config from flat structure to nested structure.
Transforms: {"queue": "name"} -> {"queue": {"name": "name", "workpool": "workpool"}}
"""

import os
from django.core.management.base import BaseCommand, CommandError
from ddpui.models.org import Org
from ddpui.ddpprefect import DDP_WORK_QUEUE, MANUL_DBT_WORK_QUEUE
from ddpui.utils.unified_logger import get_logger

logger = get_logger()


class Command(BaseCommand):
    help = "Migrate queue config from flat structure to nested structure with workpool support"

    def add_arguments(self, parser):
        parser.add_argument(
            "--workpool",
            type=str,
            help="Workpool name to use (if not provided, uses PREFECT_WORKER_POOL_NAME env var)",
        )
        parser.add_argument(
            "--org-slug",
            type=str,
            help="Migrate only specific organization (if not provided, migrates all orgs)",
        )

    def handle(self, *args, **options):
        workpool_override = options.get("workpool")
        org_slug = options.get("org_slug")

        # Determine workpool to use
        if workpool_override:
            workpool_name = workpool_override
            self.stdout.write(f"Using provided workpool: {workpool_name}")
        else:
            workpool_name = os.getenv("PREFECT_WORKER_POOL_NAME")
            if not workpool_name:
                raise CommandError(
                    "No workpool specified. Either provide --workpool or set PREFECT_WORKER_POOL_NAME environment variable"
                )
            self.stdout.write(f"Using workpool from environment: {workpool_name}")

        # Get organizations to migrate
        if org_slug:
            try:
                orgs = [Org.objects.get(slug=org_slug)]
                self.stdout.write(f"Migrating single organization: {org_slug}")
            except Org.DoesNotExist:
                raise CommandError(f"Organization with slug '{org_slug}' not found")
        else:
            orgs = Org.objects.all()
            self.stdout.write(f"Migrating all organizations ({orgs.count()} total)")

        # Show migration plan
        self.show_migration_plan(orgs, workpool_name)

        # Confirm before proceeding (unless single org)
        if not org_slug and len(orgs) > 1:
            confirm = input(f"Migrate {len(orgs)} organizations to nested queue config? (y/N): ")
            if confirm.lower() != "y":
                self.stdout.write("Migration cancelled.")
                return

        # Perform migration
        self.perform_migration(orgs, workpool_name)

    def show_migration_plan(self, orgs, workpool_name):
        """Show what the migration will do"""

        self.stdout.write(f"\nWill migrate queue config to nested structure:")
        self.stdout.write(f"Workpool to use: {workpool_name}")
        self.stdout.write(f"Default queues: {DDP_WORK_QUEUE}, {MANUL_DBT_WORK_QUEUE}\n")

        needs_migration = []
        already_migrated = []
        no_config = []

        for org in orgs:
            if not org.queue_config:
                no_config.append(org)
                continue

            # Check if already migrated (has nested structure)
            if isinstance(org.queue_config.get("scheduled_pipeline_queue"), dict):
                already_migrated.append(org)
            else:
                needs_migration.append(org)

        if needs_migration:
            self.stdout.write(f"Organizations needing migration ({len(needs_migration)}):")
            for org in needs_migration:
                self.stdout.write(f"  - {org.slug}: {org.queue_config}")

        if already_migrated:
            self.stdout.write(f"\nOrganizations already migrated ({len(already_migrated)}):")
            for org in already_migrated:
                self.stdout.write(f"  - {org.slug}: already has nested structure")

        if no_config:
            self.stdout.write(f"\nOrganizations with no queue config ({len(no_config)}):")
            for org in no_config:
                self.stdout.write(f"  - {org.slug}: will get default config")

        self.stdout.write(f"\nSummary:")
        self.stdout.write(f"  To migrate: {len(needs_migration)}")
        self.stdout.write(f"  Already migrated: {len(already_migrated)}")
        self.stdout.write(f"  No config: {len(no_config)}")

    def perform_migration(self, orgs, workpool_name):
        """Perform the actual migration"""
        self.stdout.write(f"\nStarting migration...")

        migrated_count = 0
        skipped_count = 0
        error_count = 0

        for org in orgs:
            try:
                result = self.migrate_org_queue_config(org, workpool_name)
                if result == "migrated":
                    migrated_count += 1
                    self.stdout.write(f"  ✓ Migrated: {org.slug}")
                elif result == "skipped":
                    skipped_count += 1
                    self.stdout.write(f"  - Skipped: {org.slug} (already migrated)")

            except Exception as e:
                error_count += 1
                error_msg = f"Failed to migrate {org.slug}: {str(e)}"
                self.stdout.write(self.style.ERROR(f"  ✗ {error_msg}"))
                logger.error(error_msg)

        # Summary
        self.stdout.write(f"\nMigration completed:")
        self.stdout.write(f"✓ Successfully migrated: {migrated_count}")
        self.stdout.write(f"- Skipped (already migrated): {skipped_count}")

        if error_count > 0:
            self.stdout.write(self.style.ERROR(f"✗ Errors: {error_count}"))
        else:
            self.stdout.write(self.style.SUCCESS("All migrations completed successfully!"))

    def migrate_org_queue_config(self, org, workpool_name):
        """Migrate a single org's queue config"""
        if not org.queue_config:
            # Set default config for orgs without any config
            default_config = {
                "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": workpool_name},
                "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": workpool_name},
                "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": workpool_name},
            }
            org.queue_config = default_config
            org.save()
            return "migrated"

        old_config = org.queue_config

        # Check if already migrated (has nested structure)
        if isinstance(old_config.get("scheduled_pipeline_queue"), dict):
            return "skipped"

        # Transform flat structure to nested
        new_config = {}

        # Handle each queue type
        for queue_type in [
            "scheduled_pipeline_queue",
            "connection_sync_queue",
            "transform_task_queue",
        ]:
            if queue_type in old_config and isinstance(old_config[queue_type], str):
                new_config[queue_type] = {"name": old_config[queue_type], "workpool": workpool_name}
            else:
                # Set defaults if missing using constants
                defaults = {
                    "scheduled_pipeline_queue": DDP_WORK_QUEUE,
                    "connection_sync_queue": DDP_WORK_QUEUE,
                    "transform_task_queue": MANUL_DBT_WORK_QUEUE,
                }
                new_config[queue_type] = {"name": defaults[queue_type], "workpool": workpool_name}

        org.queue_config = new_config
        org.save()
        logger.info(f"Migrated queue_config for org {org.slug} from {old_config} to {new_config}")
        return "migrated"
