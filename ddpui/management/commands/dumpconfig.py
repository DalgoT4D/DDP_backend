"""dumps full configuration of an organization

run the script:
    python manage.py dumpconfig --org NGO
                                --output ngo
"""

import json
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgWarehouse, OrgDataFlowv1
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.ddpairbyte import airbyte_service

logger = CustomLogger("ddpui")
load_dotenv()


class Command(BaseCommand):
    """
    This script dumps the configuration of an org to file
    The configuration can be loaded into a fresh Dalgo instance
    """

    help = "Dumps an organization's config"

    def __init__(self):
        super().__init__()
        self.output_config = {}

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            required=True,
            default="all",
            help="The slug for the org",
        )
        parser.add_argument("--output", required=False, help="The output file")

    def dump_warehouse(self, org: Org):
        """dumps warehouse information to the output config"""
        warehouse = OrgWarehouse.objects.filter(org=org).first()
        if warehouse is None:
            logger.error(f"No warehouse found for {org.slug}")
            return None
        retval = {}
        retval["wtype"] = warehouse.wtype
        retval["airbyte_destination_id"] = warehouse.airbyte_destination_id
        retval["bq_location"] = warehouse.bq_location
        retval["credentials"] = retrieve_warehouse_credentials(warehouse)
        retval["destination"] = airbyte_service.get_destination(
            org.airbyte_workspace_id, warehouse.airbyte_destination_id
        )
        return retval

    def dump_sources(self, org: Org):
        """dumps all sources for an org"""
        source_definitions = airbyte_service.get_source_definitions(org.airbyte_workspace_id)[
            "sourceDefinitions"
        ]
        sources = airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]
        for source in sources:
            for sdef in source_definitions:
                if sdef["sourceDefinitionId"] == source["sourceDefinitionId"]:
                    source["sourceDefinitionName"] = sdef["name"]
                    break
        return sources

    def dump_connections(self, org: Org):
        """dumps airbyte connection info"""
        connections = airbyte_service.get_connections(org.airbyte_workspace_id)["connections"]
        formatted_connections = []
        for connection in connections:
            connection["destination"] = airbyte_service.get_destination(
                org.airbyte_workspace_id, connection["destinationId"]
            )
            connection["source"] = airbyte_service.get_source(
                org.airbyte_workspace_id, connection["sourceId"]
            )
            streams = connection["syncCatalog"]["streams"]
            formatted_streams = []
            for stream in streams:
                formatted_stream = {
                    "name": stream["stream"]["name"],
                    "selected": stream["config"]["selected"],
                    "syncMode": stream["config"]["syncMode"],
                    "destinationSyncMode": stream["config"]["destinationSyncMode"],
                    "cursorField": stream["config"]["cursorField"],
                    "primaryKey": stream["config"]["primaryKey"],
                }
                formatted_streams.append(formatted_stream)

            schema = None
            if connection["destination"]["destinationName"] == "BigQuery":
                schema = connection["destination"]["connectionConfiguration"]["dataset_id"]
            elif connection["destination"]["destinationName"] == "Postgres":
                schema = connection["destination"]["connectionConfiguration"]["schema"]

            formatted_connection = {
                "name": connection["name"],
                "source_name": connection["source"]["name"],
                "streams": formatted_streams,
                "destinationSchema": schema,
            }
            formatted_connections.append(formatted_connection)
        return formatted_connections

    def dump_dataflow(self, org: Org):
        """dumps dataflow info"""
        dataflows = OrgDataFlowv1.objects.filter(org=org).exclude(dataflow_type="manual")
        if dataflows is None:
            logger.error(f"No dataflow found for {org.slug}")
            return None
        retval = []
        for dataflow in dataflows:
            retval.append(
                {
                    "org": dataflow.org.id,
                    "name": dataflow.name,
                    "deployment_name": dataflow.deployment_name,
                    "deployment_id": dataflow.deployment_id,
                    "cron": dataflow.cron,
                    "dataflow_type": dataflow.dataflow_type,
                }
            )
        return retval

    def handle(self, *args, **options):
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            logger.error(f"Org {options['org']} not found")
            return

        self.output_config["warehouse"] = self.dump_warehouse(org)
        self.output_config["sources"] = self.dump_sources(org)
        self.output_config["connections"] = self.dump_connections(org)
        self.output_config["flows"] = self.dump_dataflow(org)

        if options["output"]:
            options["output"] = options["output"] + ".json"
            with open(options["output"], "w", encoding="utf-8") as f:
                json.dump(self.output_config, f)
        else:
            print(json.dumps(self.output_config, indent=2))
