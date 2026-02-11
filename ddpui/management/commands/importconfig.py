"""
run the script:
    python manage.py importconfig --port 8002
                                  --email abc@ngo.com
                                  --password pasword
                                  --org ngo
                                  --input ngo.json
"""

import json
from dotenv import load_dotenv
from django.core.management.base import BaseCommand
from ddpui.ddpairbyte.schema import AirbyteConnectionCreate, AirbyteSourceCreate
from ddpui.schemas.org_warehouse_schema import OrgWarehouseSchema
from ddpui.ddpairbyte import airbyte_service
from ddpui.utils.custom_logger import CustomLogger
from testclient.testclient import TestClient

logger = CustomLogger("ddpui")
load_dotenv()


class Command(BaseCommand):
    """
    This script loads the configuration of an org from a file
    The configuration can be loaded into a fresh Dalgo instance
    """

    help = "Loads an organization's config"

    def add_arguments(self, parser):
        parser.add_argument("--port", help="port where local app server is listening")
        parser.add_argument("--email", required=True, help="login email")
        parser.add_argument("--password", required=True, help="password")
        parser.add_argument(
            "--org",
            required=True,
            help="The slug for the org",
        )
        parser.add_argument(
            "--input",
            required=True,
            help="The input file",
        )

    def handle(self, *args, **options):
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} not found"))
            return

        with open(options["input"], "r", encoding="utf-8") as f:
            input_config = json.load(f)

        ngoClient = TestClient(options["port"])

        ngoClient.login(options["email"], options["password"])
        ngoClient.clientheaders["x-dalgo-org"] = options["org"]

        self.import_warehouse(ngoClient, org, input_config["warehouse"])
        for source_config in input_config["sources"]:
            self.import_sources(ngoClient, org, source_config)
        for connection_config in input_config["connections"]:
            self.import_connections(ngoClient, org, connection_config)

    def import_warehouse(self, ngoClient: TestClient, org: Org, config: dict):
        """creates a warehouse for an org from the json"""
        existing_destination = airbyte_service.get_destinations(org.airbyte_workspace_id)
        if existing_destination.get("destinations") and existing_destination["destinations"]:
            logger.info("Warehouse already exists")
            return None, "warehouse already exists"

        name = config["destination"]["name"]
        wtype = config["wtype"]
        warehousedef_id = config["destination"]["destinationDefinitionId"]
        airbyte_config = config["destination"]["connectionConfiguration"]

        payload = OrgWarehouseSchema(
            wtype=wtype,
            name=name,
            destinationDefId=warehousedef_id,
            airbyteConfig=airbyte_config,
        ).dict()

        response = ngoClient.clientpost("organizations/warehouse/", json=payload)

        if response.get("success"):
            print(f"Warehouse created successfully")
        else:
            print(f"Error creating warehouse: {response.get('detail', 'Unknown error')}")

    def import_sources(self, ngoClient: TestClient, org: Org, config: dict):
        """Imports all sources for an org"""
        workspace_id = org.airbyte_workspace_id
        name = config["name"]
        sourcedef_name = config["sourceName"]

        source_definitions = airbyte_service.get_source_definitions(workspace_id)[
            "sourceDefinitions"
        ]

        sourceDefId = None
        for sdef in source_definitions:
            if sdef["name"] == sourcedef_name:
                sourceDefId = sdef["sourceDefinitionId"]
                break

        if sourceDefId is None:
            print(f"Error: No source definition found with name {sourcedef_name}")
            return

        payload = AirbyteSourceCreate(
            name=name,
            sourceDefId=sourceDefId,
            config=config["connectionConfiguration"],
        ).dict()

        response = ngoClient.clientpost("airbyte/sources/", json=payload)

        if "sourceId" in response:
            print(f"Source with id {response['sourceId']} created successfully.")
        else:
            print(f"Error creating source: {response}")

    def import_connections(self, ngoClient: TestClient, org: Org, config: dict):
        """Imports all connections for an org"""
        sources = airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]
        source_name = config["source_name"]
        source = next((s for s in sources if s["name"] == source_name), None)
        if source is None:
            print(f"Error: No source found with name {source_name}")
            return
        source_id = source["sourceId"]
        streams = config["streams"]

        payload = AirbyteConnectionCreate(
            name=config["name"],
            sourceId=source_id,
            destinationSchema=config["destinationSchema"],
            streams=streams,
        ).dict()

        response = ngoClient.clientpost("airbyte/v1/connections/", json=payload, timeout=60)

        if "connectionId" in response:
            print(f"Connection with id {response['connectionId']} created successfully.")
        else:
            print(f"Error creating connection: {response}")
