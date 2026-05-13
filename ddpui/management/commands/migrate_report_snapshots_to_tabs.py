import time
from django.core.management.base import BaseCommand
from ddpui.models.report import ReportSnapshot


class Command(BaseCommand):
    """
    Migrate existing report snapshots to use tabs structure.
    Moves layout_config and components from root level of frozen_dashboard
    into a default tab, matching the new dashboard tabs format.
    """

    help = "Migrate existing report snapshots to tabs structure for all orgs"

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

        for snapshot in ReportSnapshot.objects.only(
            "id", "title", "frozen_dashboard", "org"
        ).iterator(chunk_size=1000):
            frozen = snapshot.frozen_dashboard
            if not isinstance(frozen, dict):
                skipped_count += 1
                continue

            existing_tabs = frozen.get("tabs") or []

            # Skip if already has tabs
            if existing_tabs:
                skipped_count += 1
                continue

            layout_config = frozen.get("layout_config") or []
            components = frozen.get("components") or {}

            # Skip if no root-level data to migrate
            has_layout = bool(layout_config)
            has_components = bool(components)

            if not has_layout and not has_components:
                skipped_count += 1
                continue

            # This snapshot needs migration
            migrated_count += 1
            org_id = getattr(snapshot, "org_id", "N/A")

            if dry_run:
                self.stdout.write(
                    f"[DRY RUN] Would migrate - "
                    f"Snapshot ID: {snapshot.id}, "
                    f"Title: {snapshot.title}, "
                    f"Org ID: {org_id}, "
                    f"Layout items: {len(layout_config)}"
                )
            else:
                default_tab = {
                    "id": f"tab-{int(time.time() * 1000)}",
                    "title": "Untitled Tab 1",
                    "layout_config": layout_config,
                    "components": components,
                }

                # Only add tabs — leave root-level layout_config and components intact
                # for rollback safety. They will be removed in a follow-up cleanup PR
                # after the release is confirmed stable.
                frozen["tabs"] = [default_tab]

                snapshot.frozen_dashboard = frozen
                snapshot.save(update_fields=["frozen_dashboard"])

                self.stdout.write(
                    f"Migrated - "
                    f"Snapshot ID: {snapshot.id}, "
                    f"Title: {snapshot.title}, "
                    f"Org ID: {org_id}, "
                    f"Layout items: {len(default_tab['layout_config'])}"
                )

        self.stdout.write("")
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"=== DRY RUN COMPLETE: {migrated_count} snapshots would be migrated, "
                    f"{skipped_count} skipped ==="
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"=== MIGRATION COMPLETE: {migrated_count} snapshots migrated, "
                    f"{skipped_count} skipped ==="
                )
            )
