from django.core.management import BaseCommand

from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.models.org import Org


class Command(BaseCommand):
    help = "Ensures the local org has a working Airbyte workspace."

    def add_arguments(self, parser):
        parser.add_argument("--org-slug", default="admin-dev")
        parser.add_argument("--workspace-name", default=None)

    def handle(self, *args, **options):
        org_slug = options["org_slug"].strip().lower()
        org = Org.objects.filter(slug=org_slug).first()

        if org is None:
            self.stderr.write(self.style.ERROR(f"Org '{org_slug}' does not exist."))
            raise SystemExit(1)

        workspace_name = (options["workspace_name"] or org.slug or org.name).strip()

        if org.airbyte_workspace_id:
            try:
                workspace = airbyte_service.get_workspace(org.airbyte_workspace_id)
                self.stdout.write(
                    self.style.SUCCESS(
                        "Local Airbyte workspace already ready: "
                        f"{workspace['name']} ({workspace['workspaceId']})"
                    )
                )
                return
            except Exception as exc:
                self.stdout.write(
                    self.style.WARNING(
                        "Existing Airbyte workspace reference is stale or unreachable. "
                        f"Will recreate it. Details: {exc}"
                    )
                )
                org.airbyte_workspace_id = None
                org.save(update_fields=["airbyte_workspace_id", "updated_at"])

        workspace = airbytehelpers.setup_airbyte_workspace_v1(workspace_name, org)
        self.stdout.write(
            self.style.SUCCESS(
                f"Local Airbyte workspace ready: {workspace.name} ({workspace.workspaceId})"
            )
        )
