"""Backfill Prefect Secret blocks for existing OrgWarehouses.

For each warehouse: decrypts airbyte creds, runs the same preprocessing as
`create_or_update_org_cli_block` (extracts bqlocation/priority, maps to dbt
creds, strips ssl_mode/ssl), and calls `create_or_update_wh_secret_block`.

CLI profile blocks are not touched.
"""

from django.core.management.base import BaseCommand

from ddpui.core.dbtfunctions import map_airbyte_destination_spec_to_dbtcli_profile
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.dbthelpers import create_or_update_wh_secret_block
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils import secretsmanager


class Command(BaseCommand):
    help = "Create/update Prefect Secret blocks for existing OrgWarehouses"

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
                airbyte_creds = secretsmanager.retrieve_warehouse_credentials(warehouse)
                if not airbyte_creds:
                    print(f"  [skip] {org.slug}: no airbyte credentials stored")
                    skipped += 1
                    continue

                bqlocation = None
                priority = None
                if warehouse.wtype == "bigquery":
                    bqlocation = airbyte_creds.pop("dataset_location", None)
                    priority = airbyte_creds.pop("transformation_priority", None)

                dbt_project_params: DbtProjectParams | None = None
                if org.dbt:
                    try:
                        dbt_project_params = DbtProjectManager.gather_dbt_project_params(
                            org, org.dbt
                        )
                    except Exception as err:  # pylint: disable=broad-exception-caught
                        # SSL cert path needs dbt_project_params; non-SSL orgs are fine
                        print(
                            f"  [warn] {org.slug}: gather_dbt_project_params failed ({err}); "
                            "proceeding without it"
                        )

                dbt_creds = map_airbyte_destination_spec_to_dbtcli_profile(
                    airbyte_creds, dbt_project_params
                )
                dbt_creds.pop("ssl_mode", None)
                dbt_creds.pop("ssl", None)

                if options["dry_run"]:
                    print(
                        f"  [dry-run] {org.slug}: would upsert dalgo-wh-{org.slug} "
                        f"(wtype={warehouse.wtype})"
                    )
                    ok += 1
                    continue

                block = create_or_update_wh_secret_block(
                    org, warehouse, dbt_creds, bqlocation=bqlocation, priority=priority
                )
                if block is None:
                    print(f"  [fail] {org.slug}: create_or_update_wh_secret_block returned None")
                    failed += 1
                else:
                    print(f"  [ok]   {org.slug}: {block.block_name}")
                    ok += 1
            except Exception as err:  # pylint: disable=broad-exception-caught
                print(f"  [fail] {org.slug}: {err}")
                failed += 1

        print(f"\nDone. ok={ok} skipped={skipped} failed={failed}")
