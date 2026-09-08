"""Flip existing deployments' entrypoint to the runner-flow.

Old: proxy/prefect_flows.py:deployment_schedule_flow_v4
New: proxy/prefect_flows_runner.py:deployment_schedule_flow_v5

Idempotent — skips deployments already on the new entrypoint.
Flags: --org, --dry-run.
"""

from django.core.management.base import BaseCommand

from ddpui.ddpprefect.prefect_service import get_deployment, update_deployment_entrypoint
from ddpui.models.org import OrgDataFlowv1

OLD_ENTRYPOINT = "proxy/prefect_flows.py:deployment_schedule_flow_v4"
NEW_ENTRYPOINT = "proxy/prefect_flows_runner.py:deployment_schedule_flow_v5"


class Command(BaseCommand):
    help = "Flip existing Prefect deployments to the runner-flow entrypoint"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug (default: all orgs)")
        parser.add_argument(
            "--dry-run", action="store_true", help="Print planned changes; no writes"
        )

    def handle(self, *args, **options):
        qs = OrgDataFlowv1.objects.select_related("org")
        if options["org"]:
            qs = qs.filter(org__slug=options["org"])

        if qs.count() == 0:
            self.stdout.write("No dataflows found")
            return

        patched, already, skipped, failed = 0, 0, 0, 0

        for dataflow in qs:
            label = f"{dataflow.org.slug}/{dataflow.deployment_name}"

            try:
                deployment = get_deployment(dataflow.deployment_id)
            except Exception as err:  # pylint: disable=broad-exception-caught
                self.stdout.write(f"  [warn] {label}: fetch failed ({err})")
                skipped += 1
                continue

            current = deployment.get("entrypoint")

            if current == NEW_ENTRYPOINT:
                already += 1
                continue

            if current != OLD_ENTRYPOINT:
                self.stdout.write(f"  [skip] {label}: unexpected entrypoint '{current}'")
                skipped += 1
                continue

            if options["dry_run"]:
                self.stdout.write(f"  [dry-run] {label}: {OLD_ENTRYPOINT} → {NEW_ENTRYPOINT}")
                patched += 1
                continue

            try:
                update_deployment_entrypoint(dataflow.deployment_id, NEW_ENTRYPOINT)
                self.stdout.write(f"  [patch] {label}")
                patched += 1
            except Exception as err:  # pylint: disable=broad-exception-caught
                self.stdout.write(f"  [fail] {label}: {err}")
                failed += 1

        self.stdout.write(
            f"\nDone. patched={patched} already_new={already} skipped={skipped} failed={failed}"
        )
