from django.core.management.base import BaseCommand

from ddpui.models.org_user import Org, OrgUser
from ddpui.models.org import OrgDataFlow, OrgPrefectBlock, OrgWarehouse
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER, AIRBYTECONNECTION, SHELLOPERATION, DBTCORE
from ddpui.ddpdbt import dbt_service
from ddpui.utils import secretsmanager


class Command(BaseCommand):
    """
    This script deletes an org and all associated entities
    Not only in the Django database, but also in Airbyte and in Prefect
    """

    help = "Deletes an organization"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        """The main parameter is the org name"""
        parser.add_argument("--org-name", required=True)
        parser.add_argument("--yes-really", action="store_true")

    def delete_prefect_deployments(self, org: Org):  # skipcq: PYL-R0201
        """fetches and deletes every prefect deployment for this org"""
        print("=========== OrgDataFlow ===========")
        for dataflow in OrgDataFlow.objects.filter(org=org):
            print(dataflow.deployment_id, dataflow.connection_id)
            try:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            except Exception:
                pass
            dataflow.delete()

    def delete_prefect_shell_blocks(self, org: Org):  # skipcq: PYL-R0201
        """fetches and deletes all prefect blocks for this org"""
        print("=========== OrgPrefectBlock: Shell Operations ===========")
        for block in OrgPrefectBlock.objects.filter(org=org, block_type=SHELLOPERATION):
            print(
                block.block_name,
                block.display_name,
                block.command,
            )
            prefect_service.delete_shell_block(block.block_id)
            block.delete()

    def delete_dbt_workspace(self, org: Org):  # skipcq: PYL-R0201
        """deletes the dbt workspace"""
        dbt_service.delete_dbt_workspace(org)

    def delete_airbyte_workspace(self, org: Org):  # skipcq: PYL-R0201
        """
        deletes airbyte sources, destinations, connections
        deletes airbyte server and connection blocks in prefect
        """
        print("=========== OrgPrefectBlock: Airbyte Connections ===========")
        for block in OrgPrefectBlock.objects.filter(
            org=org, block_type=AIRBYTECONNECTION
        ):
            print(
                block.block_name,
                block.block_type,
                block.display_name,
                block.dbt_target_schema,
                block.command,
            )
            prefect_service.delete_airbyte_connection_block(block.block_id)
            block.delete()

        print("=========== OrgPrefectBlock: Airbyte Server(s) ===========")
        for block in OrgPrefectBlock.objects.filter(org=org, block_type=AIRBYTESERVER):
            print(
                block.block_name,
                block.block_type,
                block.display_name,
                block.dbt_target_schema,
                block.command,
            )
            prefect_service.delete_airbyte_server_block(block.block_id)
            block.delete()

        for connection in airbyte_service.get_connections(org.airbyte_workspace_id)[
            "connections"
        ]:
            print("deleting connection in Airbyte " + connection["connectionId"])
            airbyte_service.delete_connection(
                org.airbyte_workspace_id, connection["connectionId"]
            )

        for destination in airbyte_service.get_destinations(org.airbyte_workspace_id)[
            "destinations"
        ]:
            print("deleting destination in Airbyte " + destination["destinationId"])
            airbyte_service.delete_destination(
                org.airbyte_workspace_id, destination["destinationId"]
            )

        for source in airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]:
            print("deleting source in Airbyte " + source["sourceId"])
            airbyte_service.delete_source(org.airbyte_workspace_id, source["sourceId"])

        for warehouse in OrgWarehouse.objects.filter(org=org):
            secretsmanager.delete_warehouse_credentials(warehouse)
            warehouse.delete()

        airbyte_service.delete_workspace(org.airbyte_workspace_id)

    def delete_orgusers(self, org: Org):  # skipcq: PYL-R0201
        """delete all login users"""
        for orguser in OrgUser.objects.filter(org=org):
            orguser.user.delete()
            # this deletes the orguser as well via CASCADE

    def delete_one_org(self, org: Org, yes_really: bool):
        """delete one org"""
        print(f"OrgName: {org.name}   Airbyte workspace ID: {org.airbyte_workspace_id}")
        if yes_really:
            self.delete_prefect_deployments(org)
            self.delete_dbt_workspace(org)
            if org.airbyte_workspace_id:
                self.delete_airbyte_workspace(org)
            self.delete_prefect_shell_blocks(org)
            self.delete_orgusers(org)
            org.delete()

    def handle(self, *args, **options):
        """Docstring"""

        if options["org_name"] == "ALL":
            for org in Org.objects.all():
                self.delete_one_org(org, options["yes_really"])
        else:
            org = Org.objects.filter(name=options["org_name"]).first()
            if org is None:
                print("no such org")
                return

            self.delete_one_org(org, options["yes_really"])
