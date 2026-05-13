import time
from django.core.management.base import BaseCommand
from ddpui.models.dashboard import Dashboard


class Command(BaseCommand):
    """
    Migrate existing dashboards to use tabs structure.
    Moves layout_config and components from root level into a default tab.
    """

    help = "Migrate existing dashboards to tabs structure for all orgs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without applying them",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write("\n=== DRY RUN MODE: No changes will be made ===\n")

        migrated_count = 0
        skipped_count = 0

        for dashboard in Dashboard.objects.only(
            "id", "title", "tabs", "layout_config", "components", "org"
        ).iterator(chunk_size=1000):
            # Skip if already has tabs
            if dashboard.tabs and len(dashboard.tabs) > 0:
                skipped_count += 1
                continue

            # Skip if no data to migrate
            has_layout = dashboard.layout_config and len(dashboard.layout_config) > 0
            has_components = dashboard.components and len(dashboard.components) > 0

            if not has_layout and not has_components:
                skipped_count += 1
                continue

            # This dashboard needs migration
            migrated_count += 1
            org_id = getattr(dashboard, "org_id", "N/A")

            if dry_run:
                self.stdout.write(
                    f"[DRY RUN] Would migrate - "
                    f"Dashboard ID: {dashboard.id}, "
                    f"Title: {dashboard.title}, "
                    f"Org ID: {org_id}, "
                    f"Charts: {len(dashboard.layout_config or [])}"
                )
            else:
                # Create default tab with existing data
                default_tab = {
                    "id": f"tab-{int(time.time() * 1000)}",
                    "title": "Untitled Tab 1",
                    "layout_config": dashboard.layout_config or [],
                    "components": dashboard.components or {},
                }

                # Update dashboard structure — only set tabs, leave layout_config
                # and components intact for rollback safety. They will be removed
                # from the DB in a follow-up cleanup PR after the release is stable.
                dashboard.tabs = [default_tab]
                dashboard.save(update_fields=["tabs"])

                self.stdout.write(
                    f"Migrated - "
                    f"Dashboard ID: {dashboard.id}, "
                    f"Title: {dashboard.title}, "
                    f"Org ID: {org_id}, "
                    f"Charts: {len(default_tab['layout_config'])}"
                )

        self.stdout.write("")
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"=== DRY RUN COMPLETE: {migrated_count} dashboards would be migrated, "
                    f"{skipped_count} skipped ==="
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"=== MIGRATION COMPLETE: {migrated_count} dashboards migrated, "
                    f"{skipped_count} skipped ==="
                )
            )
