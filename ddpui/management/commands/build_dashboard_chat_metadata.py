"""Build dashboard chat metadata artifacts manually."""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from ddpui.core.dashboard_chat.metadata.build_service import DashboardChatMetadataBuildService
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org


class Command(BaseCommand):
    """Build one or more dashboard-scoped metadata artifacts."""

    help = "Build dashboard chat metadata artifacts for one org."

    def add_arguments(self, parser):
        parser.add_argument("--org-slug", required=True, help="Organization slug")
        parser.add_argument(
            "--dashboard-id",
            action="append",
            dest="dashboard_ids",
            type=int,
            help="Specific dashboard id to build. Pass multiple times to build several dashboards.",
        )
        parser.add_argument(
            "--builder-model",
            default="o4-mini",
            help="Label to store as the builder model on the artifact row.",
        )

    def handle(self, *args, **options):
        org_slug = options["org_slug"]
        dashboard_ids = options.get("dashboard_ids") or []
        builder_model = str(options["builder_model"] or "o4-mini")

        org = Org.objects.select_related("dbt").filter(slug=org_slug).first()
        if org is None:
            raise CommandError(f"Unknown org slug: {org_slug}")
        if org.dbt is None:
            raise CommandError(f"Org {org_slug} has no dbt configuration")

        dashboards = Dashboard.objects.filter(org=org).order_by("id")
        if dashboard_ids:
            dashboards = dashboards.filter(id__in=dashboard_ids)
        dashboards = list(dashboards)
        if not dashboards:
            raise CommandError("No dashboards matched the requested scope")

        build_service = DashboardChatMetadataBuildService()
        results = build_service.build_dashboards(
            org=org,
            dashboards=dashboards,
            builder_model=builder_model,
            built_by=None,
        )
        for result in results:
            if result.status == "ready":
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Built dashboard {result.dashboard_id} with {result.table_count} tables"
                    )
                )
            else:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to build dashboard {result.dashboard_id}: {result.error_message}"
                    )
                )
