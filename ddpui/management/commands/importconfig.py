import json
from typing import List
from dotenv import load_dotenv
from django.core.management.base import BaseCommand
import requests
from ninja.errors import HttpError

from ddpui.api import pipeline_api, transform_api
from ddpui.api.airbyte_api import post_airbyte_source
from ddpui.core import orgfunctions
from ddpui.ddpairbyte.schema import AirbyteConnectionCreate, AirbyteSourceCreate
from ddpui.ddpprefect.schema import PrefectDataFlowCreateSchema, PrefectDataFlowCreateSchema4
from ddpui.models.org import Org, OrgWarehouse, OrgDataFlowv1, OrgWarehouseSchema
from ddpui.models.org_user import OrgUser
from ddpui.utils import secretsmanager
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from testclient.testclient import TestClient
import logging

load_dotenv()

logger = logging.getLogger("dumpconfig")

import json
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    """
    This script loads the configuration of an org from a file
    The configuration can be loaded into a fresh Dalgo instance
    """

    help = "Loads an organization's config"

    def add_arguments(self, parser):
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

        self.create_warehouse(org, input_config["warehouse"])
        for source_config in input_config["sources"]:
            self.import_sources(org, source_config)
        for connection_config in input_config["connections"]:
            self.import_connections(org, connection_config)
        self.import_dataflow(org, input_config["flows"])

    def create_warehouse(self, org: Org, config: dict):
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
            airbyteConfig=airbyte_config
        )

        result = orgfunctions.create_warehouse(org, payload)

        if None in result:
            warehouse = OrgWarehouse.objects.get(name=name, org=org)
            if warehouse:
                print(f"Warehouse with id {warehouse.id} created successfully")
            else:
                print("Error creating warehouse")
        else:
            print(f"Error creating warehouse: {result[1]}")

    def import_sources(self, org: Org, config: dict):
        """Imports all sources for an org"""
        workspace_id = org.airbyte_workspace_id
        name = config["name"]
        sourcedef_name = config["sourceName"]

        source_definitions = airbyte_service.get_source_definitions(workspace_id)["sourceDefinitions"]

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
        )

        result = airbyte_service.create_source(workspace_id, payload.name, payload.sourceDefId, payload.config)

        if 'sourceId' in result:
            print(f"Source with id {result['sourceId']} created successfully.")
        else:
            print(f"Error creating source: {result}")


    def import_connections(self, org: Org, config: dict):
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
            normalize=False
        )

        result = airbytehelpers.create_connection(org, payload)

        if 'connectionId' in result[0]:
            print(f"Connection with id {result['connectionId']} created successfully.")
        else:
            print(f"Error creating connection: {result}")

