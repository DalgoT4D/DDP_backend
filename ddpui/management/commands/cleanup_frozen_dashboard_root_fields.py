from django.core.management.base import BaseCommand
from ddpui.models.report import ReportSnapshot


class Command(BaseCommand):
    """
    Remove stale root-level layout_config and components keys from
    ReportSnapshot.frozen_dashboard JSON blobs.

    These keys were left in place after the initial tabs migration for rollback
    safety. All data now lives inside frozen_dashboard.tabs — the root-level
    keys are no longer read and can be removed.
    """

    help = "Remove root-level layout_config and components from frozen_dashboard JSON"

    def add_arguments(self, parser):
        """Add --dry-run argument to preview changes without applying them."""
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without applying them",
        )

    def handle(self, *args, **options):
        """Execute the cleanup, iterating all ReportSnapshot records in batches."""
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write("\n=== DRY RUN MODE: No changes will be made ===\n")

        cleaned_count = 0
        skipped_count = 0

        snapshots_to_update = []

        for snapshot in ReportSnapshot.objects.only("id", "frozen_dashboard").iterator(
            chunk_size=1000
        ):
            frozen = snapshot.frozen_dashboard
            if not isinstance(frozen, dict):
                skipped_count += 1
                continue

            has_layout = "layout_config" in frozen
            has_components = "components" in frozen

            if not has_layout and not has_components:
                skipped_count += 1
                continue

            cleaned_count += 1

            if dry_run:
                self.stdout.write(
                    f"[DRY RUN] Would clean - Snapshot ID: {snapshot.id}, "
                    f"keys to remove: {[k for k in ('layout_config', 'components') if k in frozen]}"
                )
            else:
                frozen.pop("layout_config", None)
                frozen.pop("components", None)
                snapshot.frozen_dashboard = frozen
                snapshots_to_update.append(snapshot)

        if not dry_run and snapshots_to_update:
            ReportSnapshot.objects.bulk_update(
                snapshots_to_update, ["frozen_dashboard"], batch_size=500
            )
            for snapshot in snapshots_to_update:
                self.stdout.write(f"Cleaned - Snapshot ID: {snapshot.id}")

        self.stdout.write("")
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"=== DRY RUN COMPLETE: {cleaned_count} snapshots would be cleaned, "
                    f"{skipped_count} skipped ==="
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"=== CLEANUP COMPLETE: {cleaned_count} snapshots cleaned, "
                    f"{skipped_count} skipped ==="
                )
            )
