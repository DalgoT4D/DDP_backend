"""Updates the dbt binary path in Prefect deployments for dbt Core Operation tasks"""

import os
from pathlib import Path
from dotenv import load_dotenv
from django.core.management.base import BaseCommand, CommandError

from ddpui.models.org import Org, OrgDbt, OrgDataFlowv1
from ddpui.ddpprefect.prefect_service import get_deployment, update_dataflow_v1
from ddpui.ddpprefect import DBTCORE
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.utils.constants import TASK_GENERATE_EDR

load_dotenv()


class Command(BaseCommand):
    """Updates the dbt binary path in Prefect deployments for dbt Core Operation tasks.

    Two modes:
      - Re-sync: no --new-dbt-venv; fixes deployments to match each org's current dbt_venv.
      - Migrate: --new-dbt-venv <dir>; updates OrgDbt.dbt_venv and all deployment params
                 to the new venv (e.g. 'venv-1.9.8' under $DBT_VENV).
    """

    help = (
        "Update dbt binary paths in Prefect deployments. "
        "Pass --new-dbt-venv to migrate orgs to a new dbt version."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "org_slug",
            nargs="?",
            type=str,
            default=None,
            help="Org slug to update; omit to run across all orgs with dbt configured",
        )
        parser.add_argument(
            "--new-dbt-venv",
            type=str,
            required=True,
            help=(
                "New dbt venv directory to migrate to (relative to DBT_VENV). "
                "E.g. if DBT_VENV=/data/dalgo_dbt_venv and the new venv lives at "
                "/data/dalgo_dbt_venv/venv-1.9.8, pass 'venv-1.9.8'."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be changed without making actual changes",
        )

    def handle(self, *args, **options):
        org_slug = options["org_slug"]
        new_dbt_venv = options["new_dbt_venv"]
        dry_run = options["dry_run"]

        dbt_venv_base = os.getenv("DBT_VENV")
        if not dbt_venv_base:
            raise CommandError("DBT_VENV environment variable is not set")

        # Validate new venv binary exists before touching anything
        if not dry_run:
            new_binary_path = str(Path(dbt_venv_base) / new_dbt_venv / "bin" / "dbt")
            if not Path(new_binary_path).exists():
                raise CommandError(
                    f"dbt binary not found at {new_binary_path} — "
                    "ensure the venv is built before running this command"
                )

        # Resolve org set
        if org_slug:
            try:
                org = Org.objects.get(slug=org_slug)
            except Org.DoesNotExist:
                raise CommandError(f"Organization '{org_slug}' does not exist") from None
            if org.dbt is None:
                raise CommandError(f"Organization '{org_slug}' has no dbt configuration")
            org_dbts = OrgDbt.objects.filter(pk=org.dbt.pk)
        else:
            org_dbts = OrgDbt.objects.exclude(dbt_venv=None)
            count = org_dbts.count()
            self.stdout.write(
                self.style.WARNING(
                    f"No org_slug provided. This will update ALL {count} org(s) with dbt configured."
                )
            )
            confirm = input("Continue? [y/N] ").strip().lower()
            if confirm != "y":
                self.stdout.write("Aborted.")
                return

        if not org_dbts.exists():
            self.stdout.write(self.style.WARNING("No orgs with dbt configuration found"))
            return

        self.stdout.write(self.style.SUCCESS(f"Found {org_dbts.count()} org(s) to process"))

        orgs_updated = 0
        deployments_updated = 0

        for org_dbt in org_dbts:
            try:
                target_org = Org.objects.get(dbt=org_dbt)
            except Org.DoesNotExist:
                self.stdout.write(
                    self.style.WARNING(f"No org references OrgDbt {org_dbt.pk}, skipping")
                )
                continue

            dbt_binary_path = str(Path(dbt_venv_base) / new_dbt_venv / "bin" / "dbt")
            edr_bin_dir = str(Path(dbt_venv_base) / new_dbt_venv / "bin")

            self.stdout.write(f"\nOrg: {target_org.slug}")

            # Always update OrgDbt.dbt_venv
            old_venv = org_dbt.dbt_venv
            if not dry_run:
                org_dbt.dbt_venv = new_dbt_venv
                org_dbt.save(update_fields=["dbt_venv"])
                self.stdout.write(
                    self.style.SUCCESS(f"  dbt_venv: {old_venv!r} → {new_dbt_venv!r} (saved)")
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f"  [DRY RUN] dbt_venv: {old_venv!r} → {new_dbt_venv!r}")
                )

            deployments = OrgDataFlowv1.objects.filter(org=target_org)
            if not deployments.exists():
                self.stdout.write(self.style.WARNING("  No deployments found"))

            org_deployment_updates = 0

            for deployment in deployments:
                try:
                    prefect_deployment = get_deployment(deployment.deployment_id)
                    deployment_params = prefect_deployment["parameters"]

                    if (
                        "config" not in deployment_params
                        or "tasks" not in deployment_params["config"]
                    ):
                        self.stdout.write(
                            self.style.WARNING(
                                f"  {deployment.deployment_name}: no tasks config, skipping"
                            )
                        )
                        continue

                    modified = False

                    for task in deployment_params["config"]["tasks"]:
                        if task["type"] == DBTCORE:
                            if not task.get("commands"):
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"  {deployment.deployment_name}: dbt Core task has no commands, skipping"
                                    )
                                )
                                continue

                            old_cmd = task["commands"][0]
                            parts = old_cmd.split()
                            if parts:
                                parts[0] = dbt_binary_path
                                new_cmd = " ".join(parts)
                                if old_cmd != new_cmd:
                                    self.stdout.write(f"  [{deployment.deployment_name}]")
                                    self.stdout.write(f"    old: {old_cmd}")
                                    self.stdout.write(f"    new: {new_cmd}")
                                    task["commands"][0] = new_cmd
                                    modified = True

                        elif task.get("slug") == TASK_GENERATE_EDR:
                            old_path = task.get("env", {}).get("PATH", "")
                            if old_path != edr_bin_dir:
                                self.stdout.write(
                                    f"  [{deployment.deployment_name}] EDR PATH: {old_path!r} → {edr_bin_dir!r}"
                                )
                                task.setdefault("env", {})["PATH"] = edr_bin_dir
                                modified = True

                    if modified:
                        if not dry_run:
                            update_dataflow_v1(
                                deployment.deployment_id,
                                PrefectDataFlowUpdateSchema3(
                                    deployment_params=deployment_params,
                                    cron=deployment.cron,
                                ),
                            )
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"  Updated deployment {deployment.deployment_name}"
                                )
                            )
                        else:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"  [DRY RUN] Would update {deployment.deployment_name}"
                                )
                            )
                        org_deployment_updates += 1

                except Exception as err:
                    self.stdout.write(
                        self.style.ERROR(f"  Error processing {deployment.deployment_name}: {err}")
                    )

            orgs_updated += 1
            deployments_updated += org_deployment_updates

        label = "[DRY RUN] Would update" if dry_run else "Updated"
        self.stdout.write(
            self.style.SUCCESS(
                f"\n{label} {orgs_updated} org(s), {deployments_updated} deployment(s)"
            )
        )
