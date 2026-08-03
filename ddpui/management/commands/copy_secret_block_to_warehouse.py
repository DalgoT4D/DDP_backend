"""Copy dbt_profile_secret_block from OrgDbt to OrgWarehouse for existing orgs.

This is a one-time data migration to accompany migration
0172_orgwarehouse_dbt_profile_secret_block, which adds the FK column but does
not backfill it. Run this command once after applying that migration in each
environment.

Per org:
  1. If org.dbt is not set, skip — block only exists for dbt-enabled orgs.
  2. If OrgDbt.dbt_profile_secret_block is null, skip — nothing to copy.
  3. Copy the FK to OrgWarehouse.dbt_profile_secret_block.

Idempotent — skips warehouses that already have the block set.
"""

from django.core.management.base import BaseCommand

from ddpui.models.org import OrgWarehouse


class Command(BaseCommand):
    help = "Copy dbt_profile_secret_block from OrgDbt to OrgWarehouse"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug (default: all orgs)")
        parser.add_argument(
            "--dry-run", action="store_true", help="Print what would happen; no writes"
        )

    def handle(self, *args, **options):
        warehouses = OrgWarehouse.objects.select_related(
            "org", "org__dbt", "dbt_profile_secret_block"
        ).all()
        if options["org"]:
            warehouses = warehouses.filter(org__slug=options["org"])

        total = warehouses.count()
        if total == 0:
            print("No warehouses found")
            return

        print(f"Processing {total} warehouse(s)")
        ok, skipped, failed = 0, 0, 0

        for warehouse in warehouses:
            org = warehouse.org
            try:
                if not org.dbt:
                    print(f"  [skip] {org.slug}: no OrgDbt")
                    skipped += 1
                    continue

                if not org.dbt.dbt_profile_secret_block:
                    print(f"  [skip] {org.slug}: OrgDbt has no dbt_profile_secret_block")
                    skipped += 1
                    continue

                if warehouse.dbt_profile_secret_block:
                    print(
                        f"  [skip] {org.slug}: already set to "
                        f"{warehouse.dbt_profile_secret_block.block_name}"
                    )
                    skipped += 1
                    continue

                block = org.dbt.dbt_profile_secret_block
                if options["dry_run"]:
                    print(f"  [dry-run] {org.slug}: would copy {block.block_name}")
                    ok += 1
                    continue

                warehouse.dbt_profile_secret_block = block
                warehouse.save(update_fields=["dbt_profile_secret_block"])
                print(f"  [ok]   {org.slug}: copied {block.block_name}")
                ok += 1

            except Exception as err:  # pylint: disable=broad-exception-caught
                print(f"  [fail] {org.slug}: {err}")
                failed += 1

        print(f"\nDone. ok={ok} skipped={skipped} failed={failed}")
