"""
Management command to migrate existing UI organizations to managed Git repositories.
This converts organizations that currently use transform_type="ui" to use managed Git repositories.

Usage:
    python manage.py migrate_ui_orgs_to_managed_git [--dry-run] [--org-slug <slug>]
"""
import os
from pathlib import Path
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from ddpui.models.org import Org, OrgDbt, TransformType
from ddpui.models.tasks import OrgTask, Task
from ddpui.core.git_manager import GitManager, GitManagerError
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils import secretsmanager
from ddpui.ddpdbt.dbt_service import connect_existing_repo_to_remote
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    help = "Migrate UI organizations to managed Git repositories"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be migrated without making changes",
        )
        parser.add_argument(
            "--org-slug",
            type=str,
            help="Migrate only the specified organization (by slug)",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        target_org_slug = options.get("org_slug")

        self.stdout.write(self.style.SUCCESS("Starting UI to Managed Git migration..."))

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be made"))

        # Check required environment variables
        dalgo_github_org = os.getenv("DALGO_GITHUB_ORG")
        org_admin_pat = os.getenv("DALGO_ORG_ADMIN_PAT")
        environment = os.getenv("ENVIRONMENT")

        if not dalgo_github_org or not org_admin_pat or not environment:
            missing_vars = []
            if not dalgo_github_org:
                missing_vars.append("DALGO_GITHUB_ORG")
            if not org_admin_pat:
                missing_vars.append("DALGO_ORG_ADMIN_PAT")
            if not environment:
                missing_vars.append("ENVIRONMENT")

            self.stdout.write(
                self.style.ERROR(
                    f'Missing required environment variables: {", ".join(missing_vars)}'
                )
            )
            return

        # Find UI organizations that need migration
        ui_orgdbt_query = OrgDbt.objects.filter(transform_type=TransformType.UI)

        if target_org_slug:
            ui_orgdbt_query = ui_orgdbt_query.filter(org__slug=target_org_slug)

        ui_orgdbts = list(ui_orgdbt_query.select_related("org"))

        if not ui_orgdbts:
            if target_org_slug:
                self.stdout.write(
                    self.style.WARNING(f"No UI organization found with slug: {target_org_slug}")
                )
            else:
                self.stdout.write(self.style.SUCCESS("No UI organizations found to migrate"))
            return

        self.stdout.write(f"Found {len(ui_orgdbts)} UI organization(s) to migrate")

        success_count = 0
        error_count = 0

        for orgdbt in ui_orgdbts:
            org = orgdbt.org
            self.stdout.write(f"\\nProcessing organization: {org.name} (slug: {org.slug})")

            if dry_run:
                dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
                self.stdout.write(
                    self.style.WARNING(f"  [DRY RUN] Local repo path: {dbt_repo_dir}")
                )
                self.stdout.write(
                    self.style.WARNING(f"  [DRY RUN] Would commit any uncommitted changes")
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"  [DRY RUN] Would create repository: dbt-{org.slug}-{environment}"
                    )
                )
                self.stdout.write(
                    self.style.WARNING(f"  [DRY RUN] Would push existing commits to managed repo")
                )
                self.stdout.write(
                    self.style.WARNING(f"  [DRY RUN] Would create git-pull OrgTask if not present")
                )
                success_count += 1
                continue

            try:
                with transaction.atomic():
                    self._migrate_organization(org, orgdbt, environment)
                    success_count += 1
                    self.stdout.write(self.style.SUCCESS(f"  ✅ Successfully migrated {org.name}"))

            except Exception as e:
                error_count += 1
                self.stdout.write(self.style.ERROR(f"  ❌ Failed to migrate {org.name}: {str(e)}"))
                logger.error(f"Migration failed for org {org.name}: {str(e)}")

        # Summary
        self.stdout.write(f"\\n" + "=" * 50)
        self.stdout.write(f"Migration Summary:")
        self.stdout.write(f"  Successfully migrated: {success_count}")
        if error_count > 0:
            self.stdout.write(f"  Failed migrations: {error_count}")
        self.stdout.write(f"  Total processed: {len(ui_orgdbts)}")

        if dry_run:
            self.stdout.write("\\nTo perform actual migration, run without --dry-run flag")

    def _migrate_organization(self, org: Org, orgdbt: OrgDbt, environment: str):
        """
        Migrate a single organization to managed Git.
        This method runs within a database transaction.
        """
        dalgo_github_org = os.getenv("DALGO_GITHUB_ORG")

        self.stdout.write(f"  Creating managed repository...")

        # 1. Create managed repository
        repo_data = GitManager.create_managed_repository(org_slug=org.slug, environment=environment)
        repo_url = repo_data["clone_url"]

        self.stdout.write(f'    Created repository: {repo_data["full_name"]}')

        # 2. Get org admin PAT for repository operations
        self.stdout.write(f"  Getting org admin PAT...")
        repo_pat = GitManager.get_org_admin_pat()

        # 3. Commit any uncommitted changes in local repository
        self.stdout.write(f"  Committing any uncommitted local changes...")
        self._commit_local_changes(org, orgdbt, repo_pat)

        # 4. Connect existing local repo to managed repository using refactored service
        self.stdout.write(f"  Connecting existing local Git repo to managed repository...")
        connect_existing_repo_to_remote(
            org=org, orgdbt=orgdbt, remote_repo_url=repo_url, access_token=repo_pat
        )

        # 5. Mark as managed repository (the service function doesn't set this flag)
        orgdbt.is_repo_managed_by_system = True
        orgdbt.save()

        self.stdout.write(f"    Connected and pushed existing commits to: {repo_url}")

        # 6. Create git-pull OrgTask if not present
        self.stdout.write(f"  Ensuring git-pull OrgTask exists...")
        self._ensure_git_pull_orgtask(org, orgdbt)

        logger.info(f"Successfully migrated org {org.name} to managed Git repository")

    def _commit_local_changes(self, org: Org, orgdbt: OrgDbt, access_token: str):
        """
        Commit any uncommitted changes in the local repository before migration.
        """
        try:
            dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
            if not dbt_repo_dir.exists():
                self.stdout.write(f"    No local repo directory found at {dbt_repo_dir}")
                return

            # Create GitManager for local operations
            git_manager = GitManager(
                repo_local_path=str(dbt_repo_dir), pat=access_token, validate_git=True
            )

            # Check if there are any changes to commit
            status_summary = git_manager.get_changes_summary()

            if status_summary.has_changes():
                files_to_commit = (
                    status_summary.untracked_files
                    + status_summary.modified_files
                    + status_summary.deleted_files
                )

                self.stdout.write(f"    Found {len(files_to_commit)} uncommitted changes")

                # Commit all changes
                git_manager.commit_changes(
                    message=f"Pre-migration commit for {org.name} - save current state before moving to managed Git"
                )

                self.stdout.write(f"    Committed {len(files_to_commit)} changes")
            else:
                self.stdout.write(f"    No uncommitted changes found")

        except GitManagerError as e:
            logger.error(f"GitManagerError during local commit for {org.name}: {e.message}")
            raise Exception(f"Failed to commit local changes: {e.message}") from e
        except Exception as e:
            logger.error(f"Failed to commit local changes for {org.name}: {str(e)}")
            raise Exception(f"Failed to commit local changes: {str(e)}") from e

    def _ensure_git_pull_orgtask(self, org: Org, orgdbt: OrgDbt):
        """
        Ensure that git-pull OrgTask exists for the organization and orgdbt.
        Create it if it doesn't exist.
        """
        try:
            # Look for git-pull task in the Task table
            git_pull_task = Task.objects.filter(slug="git-pull").first()

            if not git_pull_task:
                self.stdout.write(
                    self.style.WARNING(f"    Warning: git-pull Task not found in Task table")
                )
                return

            # Check if OrgTask already exists for this task and orgdbt
            existing_orgtask = OrgTask.objects.filter(
                org=org, task=git_pull_task, dbt=orgdbt
            ).first()

            if existing_orgtask:
                self.stdout.write(f"    git-pull OrgTask already exists")
                return

            # Create git-pull OrgTask
            org_task = OrgTask.objects.create(
                org=org,
                task=git_pull_task,
                dbt=orgdbt,
                parameters={},
            )
            self.stdout.write(f"    Created git-pull OrgTask with ID: {org_task.id}")

        except Exception as e:
            logger.error(f"Failed to create git-pull OrgTask for {org.name}: {str(e)}")
            self.stdout.write(
                self.style.WARNING(f"    Warning: Failed to create git-pull OrgTask: {str(e)}")
            )
