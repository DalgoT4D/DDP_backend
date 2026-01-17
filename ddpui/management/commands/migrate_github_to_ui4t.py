"""
Django management command to migrate DBT users (both GitHub and UI4T) to UI4T canvas.
Creates CanvasNode and CanvasEdge records from manifest.json.

Features:
- For GitHub/dbtcloud orgs (transform_type = 'github' or 'dbtcloud'):
  - Fixes git remote URL if it contains OAuth tokens
  - Syncs .gitignore with required entries
  - User tab preference set to 'github'

- For UI4T orgs (transform_type = 'ui'):
  - Initializes git repository (git init)
  - Syncs .gitignore with required entries
  - Creates initial commit
  - User tab preference set to 'ui'

Usage:
    # Dry run (no DB changes, but shows what would happen)
    python manage.py migrate_github_to_ui4t --org {slug} --dry-run

    # Actual migration
    python manage.py migrate_github_to_ui4t --org {slug}
"""

import subprocess
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType
from ddpui.models.canvas_models import CanvasNode, CanvasEdge
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.ddpdbt import dbt_service
from ddpui.ddpdbt.dbt_service import (
    DbtProjectManager,
    sync_gitignore_contents,
    cleanup_unused_sources,
)

from ddpui.core.git_manager import GitManager, GitManagerError


class Command(BaseCommand):
    help = "Migrate DBT users (GitHub and UI4T) to UI4T canvas"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            type=str,
            required=True,
            help="Organization slug to migrate",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run migration without saving changes",
        )

    def handle(self, *args, **options):
        org_slug = options["org"]
        dry_run = options["dry_run"]

        self.stdout.write(f"Starting UI4T canvas migration for: {org_slug}")
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY-RUN MODE - No changes will be saved"))

        # Step 1: Validate prerequisites
        self.stdout.write("\n=== VALIDATION ===")
        org, orgdbt, warehouse = self._validate_prerequisites(org_slug)
        self.stdout.write(self.style.SUCCESS("All prerequisites validated"))

        # Step 2: Check if already migrated
        # self.stdout.write("\n=== CHECKING EXISTING DATA ===")
        # if self._check_existing_migration(orgdbt):
        #     self.stdout.write(self.style.WARNING("Migration skipped - already done"))
        #     return

        # Step 3: Setup git based on transform_type (outside transaction - can't be rolled back)
        self.stdout.write("\n=== GIT SETUP ===")
        self._setup_git_for_migration(orgdbt, dry_run)

        # Step 4: Run migration
        self.stdout.write("\n=== RUNNING MIGRATION ===")
        try:
            stats = self._run_migration(org, orgdbt, warehouse, dry_run)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Migration failed: {e}"))
            raise CommandError(str(e))

        # Step 5: Report stats
        self.stdout.write("\n=== MIGRATION COMPLETE ===")
        self._print_stats(stats)

        if not dry_run:
            self.stdout.write(self.style.SUCCESS("\nMigration successful!"))
        else:
            self.stdout.write(self.style.WARNING("\nDry-run complete - no changes were saved"))

    def _validate_prerequisites(self, org_slug: str) -> tuple[Org, OrgDbt, OrgWarehouse]:
        """
        Validate all prerequisites before migration.
        Returns (org, orgdbt, warehouse) or raises CommandError.
        """
        # 1. Check Org exists
        org = Org.objects.filter(slug=org_slug).first()
        if not org:
            raise CommandError(f"Organization '{org_slug}' not found")
        self.stdout.write(f"  Org: {org.name} ({org_slug})")

        # 2. Check OrgDbt exists
        orgdbt = org.dbt
        if not orgdbt:
            raise CommandError(f"No DBT workspace configured for '{org_slug}'")
        self.stdout.write(f"  OrgDbt: {orgdbt.project_dir}")

        # 3. Check transform_type and gitrepo_url
        self.stdout.write(f"  Transform type: {orgdbt.transform_type}")
        is_git_org = orgdbt.transform_type in (TransformType.GIT.value, "dbtcloud")
        if is_git_org:
            if not orgdbt.gitrepo_url:
                raise CommandError("GitHub/dbtcloud org must have gitrepo_url set")
            self.stdout.write(f"  Git repo: {orgdbt.gitrepo_url}")

        # 4. Check OrgWarehouse exists
        warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not warehouse:
            raise CommandError(f"No warehouse configured for '{org_slug}'")
        self.stdout.write(f"  Warehouse: {warehouse.wtype}")

        # 5. Check project directory exists
        dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
        if not dbt_repo_dir.exists():
            raise CommandError(f"DBT repo directory does not exist: {dbt_repo_dir}")
        self.stdout.write(f"  Project dir: {dbt_repo_dir}")

        return org, orgdbt, warehouse

    def _check_existing_migration(self, orgdbt: OrgDbt) -> bool:
        """
        Check if migration was already done.
        Returns True if CanvasNode/CanvasEdge data exists.
        """
        existing_nodes = CanvasNode.objects.filter(orgdbt=orgdbt).count()
        existing_edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()

        if existing_nodes > 0 or existing_edges > 0:
            self.stdout.write(
                f"  Found existing data: {existing_nodes} CanvasNodes, "
                f"{existing_edges} CanvasEdges"
            )
            return True

        self.stdout.write("  No existing canvas data found - proceeding with migration")
        return False

    def _fix_git_remote_url(self, orgdbt: OrgDbt, dry_run: bool = False):
        """
        Check git remote URL and fix if it contains OAuth tokens.
        Replaces URLs like https://oauth2:<pat>@github.com/... with clean URL from orgdbt.gitrepo_url.
        """
        dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
        expected_url = orgdbt.gitrepo_url

        git_manager = GitManager(dbt_repo_dir)

        try:
            # Get current git remote URL
            current_url = git_manager.get_remote_url()

            self.stdout.write(f"  Current git remote: {current_url}")
            self.stdout.write(f"  Expected git remote: {expected_url}")

            # Compare URLs - if different, update
            if current_url != expected_url:
                self.stdout.write(self.style.WARNING("  Git remote URL mismatch - needs update"))

                if dry_run:
                    self.stdout.write(self.style.WARNING("  DRY-RUN: Would update git remote URL"))
                else:
                    # Update git remote URL
                    git_manager.set_remote(expected_url)
                    self.stdout.write(
                        self.style.SUCCESS(f"  Updated git remote to: {expected_url}")
                    )
            else:
                self.stdout.write(self.style.SUCCESS("  Git remote URL is correct"))

        except GitManagerError as e:
            self.stdout.write(self.style.WARNING(f"  Failed to check/update git remote: {e.error}"))

    def _setup_git_for_migration(self, orgdbt: OrgDbt, dry_run: bool = False):
        """
        Setup git based on transform_type:
        - For GitHub/dbtcloud users: fix git remote URL + sync gitignore
        - For UI4T users: git init + sync gitignore + initial commit
        """
        dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
        is_git_org = orgdbt.transform_type in (TransformType.GIT.value, "dbtcloud")
        git_manager = GitManager(dbt_repo_dir)

        if is_git_org:
            self.stdout.write("  Detected GitHub/dbtcloud org")

            # Fix git remote URL if needed
            self._fix_git_remote_url(orgdbt, dry_run)

            # Sync gitignore
            self.stdout.write("  Syncing .gitignore...")
            if not dry_run:
                sync_gitignore_contents(str(dbt_repo_dir))
                self.stdout.write(self.style.SUCCESS("  .gitignore synced"))
            else:
                self.stdout.write(self.style.WARNING("  DRY-RUN: Would sync .gitignore"))

        else:
            self.stdout.write("  Detected UI4T org")

            # Step 1: Git init
            self.stdout.write("  Initializing git repository...")
            if not dry_run:
                try:
                    git_manager.init_repo()
                    self.stdout.write(self.style.SUCCESS("  Git repository initialized"))
                except GitManagerError as e:
                    self.stdout.write(
                        self.style.WARNING(f"  Git init failed (may already exist): {e.error}")
                    )
            else:
                self.stdout.write(self.style.WARNING("  DRY-RUN: Would run git init"))

            # Step 2: Sync gitignore
            self.stdout.write("  Syncing .gitignore...")
            if not dry_run:
                sync_gitignore_contents(str(dbt_repo_dir))
                self.stdout.write(self.style.SUCCESS("  .gitignore synced"))
            else:
                self.stdout.write(self.style.WARNING("  DRY-RUN: Would sync .gitignore"))

            # Step 3: Initial commit
            self.stdout.write("  Creating initial commit...")
            if not dry_run:
                try:
                    git_manager.commit_changes("System: Initial commit - UI4T migration")
                    self.stdout.write(self.style.SUCCESS("  Initial commit created"))
                except GitManagerError as e:
                    self.stdout.write(self.style.WARNING(f"  Initial commit failed: {e.error}"))
            else:
                self.stdout.write(self.style.WARNING("  DRY-RUN: Would create initial commit"))

    def _save_user_tab_preference(self, org: Org, preference: str):
        """Save user tab preference for all OrgUsers in the organization."""
        self.stdout.write(f"  Setting user tab preference to '{preference}' for all org users...")

        org_users = OrgUser.objects.filter(org=org)
        total_users = org_users.count()
        updated_count = 0

        self.stdout.write(f"  Found {total_users} user(s) in organization")

        for org_user in org_users:
            user_pref, created = UserPreferences.objects.get_or_create(orguser=org_user)
            user_pref.last_visited_transform_tab = preference
            user_pref.save()
            updated_count += 1
            action = "Created" if created else "Updated"
            self.stdout.write(f"    {action} preference for user: {org_user.user.email}")

        self.stdout.write(
            self.style.SUCCESS(
                f"  Updated tab preference for {updated_count}/{total_users} user(s)"
            )
        )

    def _run_migration(
        self,
        org: Org,
        orgdbt: OrgDbt,
        warehouse: OrgWarehouse,
        dry_run: bool = False,
    ) -> dict:
        """
        Run the migration within a transaction.
        Uses existing parse_dbt_manifest_to_canvas() function.
        """
        try:
            # Step 1: Generate manifest.json
            self.stdout.write("  Generating manifest.json (dbt deps + dbt compile)...")
            manifest_json = dbt_service.generate_manifest_json_for_dbt_project(org, orgdbt)

            if not manifest_json:
                raise Exception("Failed to generate manifest.json")

            self.stdout.write(self.style.SUCCESS("  Manifest generated successfully"))

            # Step 1.5: Clean up unused sources for UI4T organizations only (before canvas creation)
            cleanup_stats = {"sources_removed": [], "sources_with_edges_skipped": [], "errors": []}
            if orgdbt.transform_type == TransformType.UI.value:
                self.stdout.write("  Cleaning up unused sources for UI4T org...")
                cleanup_stats = cleanup_unused_sources(org, orgdbt, manifest_json)

                if cleanup_stats["sources_removed"]:
                    self.stdout.write(
                        f"    Removed {len(cleanup_stats['sources_removed'])} unused sources: "
                        f"{', '.join(cleanup_stats['sources_removed'])}"
                    )

                if cleanup_stats["sources_with_edges_skipped"]:
                    self.stdout.write(
                        f"    Skipped {len(cleanup_stats['sources_with_edges_skipped'])} sources with canvas connections: "
                        f"{', '.join(cleanup_stats['sources_with_edges_skipped'])}"
                    )

                if cleanup_stats["errors"]:
                    self.stdout.write(
                        self.style.WARNING(
                            f"    Cleanup errors: {', '.join(cleanup_stats['errors'])}"
                        )
                    )

                if (
                    not cleanup_stats["sources_removed"]
                    and not cleanup_stats["sources_with_edges_skipped"]
                ):
                    self.stdout.write("    No unused sources found to clean up")
            else:
                self.stdout.write(
                    f"  Skipping cleanup - not a UI4T org (transform_type: {orgdbt.transform_type})"
                )

            # Step 2: Parse manifest to canvas
            self.stdout.write("  Parsing manifest and creating canvas nodes/edges...")
            stats = dbt_service.parse_dbt_manifest_to_canvas(
                org=org,
                orgdbt=orgdbt,
                org_warehouse=warehouse,
                manifest_json=manifest_json,
                refresh=False,  # Don't regenerate, we just did
            )

            # Add cleanup stats to overall stats
            stats["cleanup_sources_removed"] = len(cleanup_stats["sources_removed"])
            stats["cleanup_sources_skipped"] = len(cleanup_stats["sources_with_edges_skipped"])
            stats["cleanup_errors"] = len(cleanup_stats["errors"])

            # Step 3: Determine user tab preference from original transform_type
            # If transform_type is 'github' or 'dbtcloud' → preference = 'github'
            # If transform_type is 'ui' → preference = 'ui'
            original_transform_type = orgdbt.transform_type
            if original_transform_type in (TransformType.GIT.value, "dbtcloud"):
                usertabpreference = TransformType.GIT.value
                self.stdout.write(
                    f"  transform_type is '{original_transform_type}' - using 'github' preference"
                )
            else:
                usertabpreference = TransformType.UI.value
                self.stdout.write(
                    f"  transform_type is '{original_transform_type}' - using 'ui' preference"
                )

            # Step 5: Save user tab preference for all org users
            self._save_user_tab_preference(org, usertabpreference)

            return stats

        except Exception as e:
            # Transaction will auto-rollback
            raise Exception(f"Migration failed: {e}")

    def _print_stats(self, stats: dict):
        """Print migration statistics."""
        self.stdout.write(f"  Sources processed: {stats.get('sources_processed', 0)}")
        self.stdout.write(f"  Models processed: {stats.get('models_processed', 0)}")
        self.stdout.write(f"  CanvasNodes created: {stats.get('nodes_created', 0)}")
        self.stdout.write(f"  CanvasNodes updated: {stats.get('nodes_updated', 0)}")
        self.stdout.write(f"  CanvasEdges created: {stats.get('edges_created', 0)}")
        self.stdout.write(f"  CanvasEdges skipped: {stats.get('edges_skipped', 0)}")
        self.stdout.write(f"  CanvasEdges deleted: {stats.get('edges_deleted', 0)}")
        self.stdout.write(f"  OrgDbtModels created: {stats.get('orgdbtmodels_created', 0)}")
        self.stdout.write(f"  OrgDbtModels updated: {stats.get('orgdbtmodels_updated', 0)}")

        # Print cleanup statistics if they exist (UI4T orgs only)
        if "cleanup_sources_removed" in stats:
            self.stdout.write(
                f"  Cleanup: Sources removed: {stats.get('cleanup_sources_removed', 0)}"
            )
            self.stdout.write(
                f"  Cleanup: Sources skipped (with edges): {stats.get('cleanup_sources_skipped', 0)}"
            )
            self.stdout.write(f"  Cleanup: Errors: {stats.get('cleanup_errors', 0)}")
