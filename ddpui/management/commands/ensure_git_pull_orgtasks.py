"""
Management command to ensure all OrgDbt instances with git repositories have git pull OrgTasks.

This command checks for all OrgDbts that have:
1. gitrepo_url is not null
2. transform_type = 'github' (TransformType.GIT)

For each matching OrgDbt, it verifies if an OrgTask exists with task__type = 'git'.
If missing, it creates a git pull OrgTask for the organization.
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from ddpui.models.org import Org, OrgDbt, TransformType
from ddpui.models.tasks import Task, TaskType, OrgTask, OrgTaskGeneratedBy
from ddpui.utils.constants import TASK_GITPULL


class Command(BaseCommand):
    """
    Management command to ensure all git-enabled OrgDbts have corresponding git pull OrgTasks
    """

    help = "Ensures all OrgDbts with git repositories have git pull OrgTasks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be created without actually creating anything",
        )

    def handle(self, *args, **options):
        """Main command handler"""
        dry_run = options["dry_run"]

        self.stdout.write(self.style.SUCCESS("Starting git pull OrgTask verification..."))

        # Get the git pull task
        try:
            git_pull_task = Task.objects.get(type=TaskType.GIT, slug=TASK_GITPULL, is_system=True)
        except Task.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(
                    "Git pull task not found in database. Run 'python manage.py loaddata seed/*.json' first."
                )
            )
            return

        # Build the query for OrgDbts that need git pull tasks
        orgdbts = OrgDbt.objects.filter(
            gitrepo_url__isnull=False, transform_type=TransformType.GIT
        ).all()

        if not orgdbts:
            self.stdout.write(
                self.style.WARNING(
                    "No OrgDbt instances found with git repositories and GIT transform type."
                )
            )
            return

        self.stdout.write(f"Found {len(orgdbts)} OrgDbt(s) with git repositories...")

        created_count = 0
        skipped_count = 0

        for orgdbt in orgdbts:
            try:
                org = Org.objects.filter(dbt=orgdbt).get()
            except Org.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(
                        f"  OrgDbt ID {orgdbt.id}: No corresponding Org found, skipping..."
                    )
                )
                continue

            # Check if git pull OrgTask already exists for this org
            existing_git_orgtask = OrgTask.objects.filter(org=org, task__type=TaskType.GIT).first()

            if existing_git_orgtask:
                self.stdout.write(
                    f"  {org.slug}: Git pull OrgTask already exists (ID: {existing_git_orgtask.id})"
                )
                skipped_count += 1
                continue

            # Create git pull OrgTask
            if dry_run:
                self.stdout.write(
                    self.style.WARNING(f"  {org.slug}: [DRY RUN] Would create git pull OrgTask")
                )
                created_count += 1
            else:
                with transaction.atomic():
                    new_orgtask = OrgTask.objects.create(
                        org=org, task=git_pull_task, generated_by=OrgTaskGeneratedBy.SYSTEM
                    )
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  {org.slug}: Created git pull OrgTask (ID: {new_orgtask.id})"
                        )
                    )
                    created_count += 1

        # Summary
        self.stdout.write("\n" + "=" * 50)
        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"DRY RUN COMPLETE: Would create {created_count} git pull OrgTask(s)"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"COMPLETE: Created {created_count} git pull OrgTask(s)")
            )
        self.stdout.write(
            f"Skipped {skipped_count} organization(s) that already have git pull OrgTasks"
        )

        if created_count == 0 and skipped_count == 0:
            self.stdout.write(
                self.style.WARNING("No organizations required git pull OrgTask creation.")
            )
