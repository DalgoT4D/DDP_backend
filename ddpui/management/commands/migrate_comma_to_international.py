from django.core.management.base import BaseCommand

from ddpui.models.visualization import Chart


class Command(BaseCommand):
    """
    Migrate number charts from 'comma' format to 'international' format.
    Both formats are functionally identical (e.g., 1,000,000).
    """

    help = "Migrate number charts from 'comma' to 'international' format for all orgs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without applying them",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if dry_run:
            print("\n=== DRY RUN MODE: No changes will be made ===\n")

        # Get all number charts across ALL orgs
        number_charts = Chart.objects.filter(chart_type="number")

        affected_count = 0
        for chart in number_charts:
            if chart.extra_config:
                customizations = chart.extra_config.get("customizations", {})
                if customizations.get("numberFormat") == "comma":
                    affected_count += 1

                    if dry_run:
                        print(
                            f"[DRY RUN] Would update - Chart ID: {chart.id}, "
                            f"Title: {chart.title}, Org ID: {chart.org_id}"
                        )
                    else:
                        chart.extra_config["customizations"]["numberFormat"] = "international"
                        chart.save()
                        print(
                            f"Updated - Chart ID: {chart.id}, "
                            f"Title: {chart.title}, Org ID: {chart.org_id}"
                        )

        if dry_run:
            print(f"\n=== DRY RUN COMPLETE: {affected_count} charts would be updated ===\n")
        else:
            print(f"\n=== MIGRATION COMPLETE: {affected_count} charts updated ===\n")
