from uuid import uuid4
import json

from django.core.management.base import BaseCommand

from ddpui.models.org import OrgPrefectBlock, Org

from ddpui.ddpprefect.prefect_service import (
    create_dbt_core_block,
    update_dbt_core_block_credentials,
)

from ddpui.ddpprefect import (
    DBTCORE,
)

from ddpui.ddpprefect.schema import PrefectDbtCoreSetup, DbtProfile


class Command(BaseCommand):
    """
    This script deletes an org and all associated entities
    Not only in the Django database, but also in Airbyte and in Prefect
    """

    help = "Tests prefect_service.update_dbt_core_block_credentials"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        """No params"""
        parser.add_argument("--wtype", choices=["postgres", "bigquery"], required=True)

    def handle(self, *args, **options):
        """run the test"""
        org_name_for_this_test = "145e0192-595c-40fb-9bf0-a8232cdbc062"
        org = Org.objects.filter(name=org_name_for_this_test).first()
        if org is None:
            org = Org.objects.create(name=org_name_for_this_test)

        wtype = options.get("wtype")
        # =========================== postgres test ===========================
        # create a dbt core op block
        dbtblock = PrefectDbtCoreSetup(
            block_name=uuid4().hex,
            profiles_dir="/",
            project_dir="/",
            working_dir="/",
            env={},
            commands=["dbt run"],
        )
        profile = DbtProfile(
            name="dbt-profile-name", target_configs_schema="target_configs_schema"
        )
        if wtype == "postgres":
            dbt_credentials = {
                "host": "somehost",
                "port": 1234,
                "username": "someuser",
                "password": "oldpassword",
                "database": "somedbname",
            }
        else:
            credential_file = "<put service account json filename here>"
            with open(credential_file, "r", encoding="utf-8") as test_bq_credentials:
                dbt_credentials = json.load(test_bq_credentials)

        block = create_dbt_core_block(
            dbtblock,
            profile,
            "staging",
            wtype,
            dbt_credentials,
            "asia-south1",  # ignored for postgres
        )
        print(block)
        OrgPrefectBlock.objects.create(
            org=org,
            block_type=DBTCORE,
            block_id=block["block_id"],
            block_name=block["block_name"],
        )
        # always use the block name as returned by prefect-proxy
        block_name = block["block_name"]

        if wtype == "postgres":
            dbt_credentials["password"] = "newpassword"
            update_dbt_core_block_credentials(wtype, block_name, dbt_credentials)
            print(
                f"""
                  in the venv for prefect-proxy, please run

from prefect_dbt.cli.commands import DbtCoreOperation
block = DbtCoreOperation.load("{block_name}")
print(block.dbt_cli_profile.target_configs.dict()['extras']['password'])

                and verify that it is "newpassword"
                """
            )
        else:
            dbt_credentials["private_key_id"] = "new-privatekey-id"
            update_dbt_core_block_credentials(wtype, block_name, dbt_credentials)
            print(
                f"""
                  in the venv for prefect-proxy, please run

from prefect_dbt.cli.commands import DbtCoreOperation
block = DbtCoreOperation.load("{block_name}")
print(block.dbt_cli_profile.target_configs.credentials.service_account_info.get_secret_value()['private_key_id'])

                and verify that it is "new-privatekey-id"
                """
            )
