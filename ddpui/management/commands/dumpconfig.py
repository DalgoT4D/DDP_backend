"""dump s full configuration of an organization"""

import sys
import uuid
import json
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgWarehouse, OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.ddpprefect import prefect_service
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.ddpairbyte import airbyte_service

import logging

load_dotenv()

logger = logging.getLogger("dumpconfig")


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
        source_definitions = airbyte_service.get_source_definitions(
            org.airbyte_workspace_id
        )["sourceDefinitions"]
        sources = airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]
        for source in sources:
            for sdef in source_definitions:
                if sdef["sourceDefinitionId"] == source["sourceDefinitionId"]:
                    source["sourceDefinitionName"] = sdef["name"]
                    break
        return sources

    def dump_connections(self, org: Org):
        """dumps airbyte connection info"""
        connections = airbyte_service.get_connections(org.airbyte_workspace_id)[
            "connections"
        ]
        return connections

    def handle(self, *args, **options):
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            logger.error(f"Org {options['org']} not found")
            return

        self.output_config["warehouse"] = self.dump_warehouse(org)
        self.output_config["sources"] = self.dump_sources(org)
        self.output_config["connections"] = self.dump_connections(org)

        if options["output"]:
            with open(options["output"], "w", encoding="utf-8") as f:
                json.dump(self.output_config, f)
        else:
            print(json.dumps(self.output_config, indent=2))
