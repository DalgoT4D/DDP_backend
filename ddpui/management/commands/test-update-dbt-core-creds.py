from uuid import uuid4

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

test_bq_credentials = {
    "type": "service_account",
    "project_id": "tides-saas-309509",
    "private_key_id": "f67b6333d6740e5e1c8744a09638f15df33a9e72",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCovsbY1yZRUNxg\nlhTnorbz0oH7WIPqj8wCNMgjA6QoZPFi6dTZ4yPU/yuGpjVqu7DEBKjVK9qNhiil\nAU3sMcYlnmOeaDH41HiwIsk7ayDHgW8SGTpdL4MQZfvvo1qaG5V7+pFU6C81BoYn\noSudq/Ko3eINR8KJ8OuwoDZYr9+5aAD0CnhUV+YDAk7j7m3j+ViMr/UD+QYVExt+\nu6EtDzfUJTvAfTZZgd9bOGI8vCZC4PYqSxdMT+IF17TWAwyvEy8JybjHgbJ32faP\nzPGndWRNPDX0N7ONW7M2uq2PSW0+9e/t0W1txINu60mvLEvjQO17dohEa2uSKp9H\nM7tCPS1PAgMBAAECggEAD7cgvr4ez/DnbUlc7gut3TxMJbPjKTu2JLh0sSocJjTs\ndClRzO8F/ca774DYNbYAKulCLgQ1CPwnGeLqtmYWTZeYMH8EtGdZTBIZSqLZ/SqV\nRQq7cEVGAAd3dKRygiqye8bktRMgnmAaq2MrtDX41fhAHruBpyYcVSk+lnjerZ4W\nocpCjBXaeSegnOnEl8m/lsMfu/QcsNyRpw5S33vjZdVwzQKek5Ixx2gqw7e1xIIW\nIBq0QYuHHGKWxBV/Q+sFj4mbpCn08sEunJHUw7zoXBTvtfsnvU4uhlPyzYOwXTHT\nEGIu0fIiVaxwq4ZPF0ddJjZbhgm//c5lcev7socIpQKBgQDkaDeo4g4ctDMNHYiw\n2/2/rbV5O0L6YCM5bNuFTUITpSgdsQMJuCohtYKVY+f+ahnbZsX/QEOPR65/6mdv\nCUWauZzCUslr0xv6VXMi7KwMW8J5LhhBv4TlnoZDQTwPwqLsuxcddWSfG4fhU4p2\n5dQBYAFgU8mvSSf58nnLFDUHrQKBgQC9IXBwg72hgOKvtM6or7hYspKcidLlhMaB\nbXWThm41Pr7DZwQUa8p7imhNk0m+bzvjv0/Pnyw8Q9NNAmn2IVsDNViOHGeH2zMo\nuBPqjlcrkV8R/CFX/V0s03i21Ou1/ZXLzbKF/NKNfZJT3sSukPu94flp8R+lx8Rp\noez6Xx3YawKBgQCT2PoecaVc4zAgjzuJ7/0C9DiB7uBeHZjvdQ1r7iSVftTG02v6\nAKIVC98pQHBNePSf1pjXrwuMVYQY/OxTLZdGnltgViJXj2GO230Z4EVGAqeRtUqy\nVHx7/e8+3Z05Pm4j+r7trK2jaDi+nEsGx6JB+Zkqd1IYCqy72D8KO1xQSQKBgGPR\nq0Nm9IWxvXKYzd2P6I6/qMt/nlROsGoM+FolQDNP62S8ERYqEdMEKKqQywH8OTKp\nfRkKXFFuRq5FUOF7l0ppNFTEvwuf8C9UgAZym6U981xNoteKvEt1TSfJ1qHVteK7\nrLY0ynesx7cGQu4TTRpZksaMYXSgq5RIxwcZfmoDAoGAFAf36/M15F1jSTYuaPna\n6eMvy8MqrxQHCA2pp+ruhwSzDzGPHLrl6BdoaUGgaKtsBUicQKXyuQpDHfvB5mKF\nx1+OE/GfG0MmoqwLcgtAOIWp+Y18I6ORDFa2nvtis47ejaWM1iPtO+Z+nDjhVFiB\nsdOQ4IHyvv7bEl7WUhT2R1w=\n-----END PRIVATE KEY-----\n",
    "client_email": "ddp-test-warehouse@tides-saas-309509.iam.gserviceaccount.com",
    "client_id": "115796257357324178883",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ddp-test-warehouse%40tides-saas-309509.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}


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
            dbt_credentials = test_bq_credentials

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
