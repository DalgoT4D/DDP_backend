"""Backfill dbt-profile Prefect Secret blocks for existing orgs with dbt set up
AND patch that org's deployments so their DBTCORE task_configs carry the
runner-flow env key `dbt-profile-secret-block`.

Per org:
  1. If org.dbt is not set, skip — the block is a dbt-lifecycle artifact.
  2. Preprocess airbyte creds (map to dbt-postgres/dbt-bigquery fields, strip
     ssl_mode/ssl) and upsert `dbt-profile-<slug>` Secret block via
     create_or_update_dbt_profile_secret_blk. Sets
     OrgDbt.dbt_profile_secret_block FK.
  3. For every OrgDataFlowv1 belonging to that org, fetch the deployment, find
     DBTCORE tasks, populate env with `dbt-profile-secret-block`. Idempotent —
     skips deployments already up to date.

CLI profile blocks are not touched.
"""

from django.core.management.base import BaseCommand

from ddpui.core.dbtfunctions import preprocess_airbyte_creds_for_dbt
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.dbthelpers import create_or_update_dbt_profile_secret_blk
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import DBTCORE
from ddpui.ddpprefect.prefect_service import get_deployment, update_dataflow_v1
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.models.org import Org, OrgDataFlowv1, OrgWarehouse
from ddpui.utils import secretsmanager


class Command(BaseCommand):
    help = "Create/update dbt-profile Prefect Secret blocks for existing orgs"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug (default: all orgs)")
        parser.add_argument(
            "--dry-run", action="store_true", help="Print what would happen; no writes"
        )

    def handle(self, *args, **options):
        warehouses = OrgWarehouse.objects.select_related("org").all()
        if options["org"]:
            warehouses = warehouses.filter(org__slug=options["org"])

        total = warehouses.count()
        if total == 0:
            print("No warehouses found")
            return

        print(f"Processing {total} warehouse(s)")
        ok, skipped, failed = 0, 0, 0

        for warehouse in warehouses:
            org: Org = warehouse.org
            try:
                if not org.dbt:
                    print(f"  [skip] {org.slug}: no OrgDbt — dbt-profile block not applicable")
                    skipped += 1
                    continue

                airbyte_creds = secretsmanager.retrieve_warehouse_credentials(warehouse)
                if not airbyte_creds:
                    print(f"  [skip] {org.slug}: no airbyte credentials stored")
                    skipped += 1
                    continue

                dbt_project_params: DbtProjectParams | None = None
                try:
                    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
                except Exception as err:  # pylint: disable=broad-exception-caught
                    # SSL cert path needs dbt_project_params; non-SSL orgs are fine
                    print(
                        f"  [warn] {org.slug}: gather_dbt_project_params failed ({err}); "
                        "proceeding without it"
                    )

                dbt_creds, profile_extras = preprocess_airbyte_creds_for_dbt(
                    warehouse, airbyte_creds, dbt_project_params
                )

                if options["dry_run"]:
                    print(
                        f"  [dry-run] {org.slug}: would upsert dbt-profile-{org.slug} "
                        f"(wtype={warehouse.wtype}, default_schema={org.dbt.default_schema})"
                    )
                    ok += 1
                    continue

                block = create_or_update_dbt_profile_secret_blk(
                    org, warehouse, dbt_creds, profile_extras
                )
                if block is None:
                    print(
                        f"  [fail] {org.slug}: create_or_update_dbt_profile_secret_blk returned None"
                    )
                    failed += 1
                    continue

                print(f"  [ok]   {org.slug}: {block.block_name}")
                ok += 1

                # Patch deployments so DBTCORE task_configs carry the runner env key.
                _patch_deployments_for_org(org, warehouse)
            except Exception as err:  # pylint: disable=broad-exception-caught
                print(f"  [fail] {org.slug}: {err}")
                failed += 1

        print(f"\nDone. ok={ok} skipped={skipped} failed={failed}")


def _patch_deployments_for_org(org: Org, warehouse: OrgWarehouse):
    """Ensure every DBTCORE task_config for this org's deployments has
    env["dbt-profile-secret-block"] populated. Skips deployments already
    up-to-date."""
    if not warehouse.dbt_profile_secret_block:
        print(
            f"    [warn] {org.slug}: no dbt_profile_secret_block on warehouse — skipping deployment patch"
        )
        return

    block_name = warehouse.dbt_profile_secret_block.block_name
    desired_env = {"dbt-profile-secret-block": block_name}

    for dataflow in OrgDataFlowv1.objects.filter(org=org):
        try:
            deployment = get_deployment(dataflow.deployment_id)
        except Exception as err:  # pylint: disable=broad-exception-caught
            print(f"    [warn] {dataflow.deployment_name}: get_deployment failed ({err})")
            continue

        params = deployment.get("parameters", {})
        tasks = params.get("config", {}).get("tasks")
        if not tasks:
            continue

        modified = False
        for task in tasks:
            if task.get("type") != DBTCORE:
                continue
            existing_env = task.setdefault("env", {}) or {}
            for k, v in desired_env.items():
                if existing_env.get(k) != v:
                    existing_env[k] = v
                    modified = True
            task["env"] = existing_env

        if not modified:
            continue

        try:
            update_dataflow_v1(
                dataflow.deployment_id,
                PrefectDataFlowUpdateSchema3(
                    deployment_params=params,
                    cron=dataflow.cron,
                ),
            )
            print(f"    [patch] {dataflow.deployment_name}: env populated on DBTCORE tasks")
        except Exception as err:  # pylint: disable=broad-exception-caught
            print(f"    [warn] {dataflow.deployment_name}: update_dataflow_v1 failed ({err})")
