"""reads tables from db to create a dbt sources.yml"""

import os
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path

from dotenv import load_dotenv

load_dotenv("dbconnection.env")

# pylint:disable=wrong-import-position
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from ddpui.dbt_automation.utils.dbtproject import dbtProject
from ddpui.core.dbtautomation_service import upsert_multiple_sources_to_a_yaml

basicConfig(level=INFO)
logger = getLogger()


def sync_sources(config, warehouse: WarehouseInterface, dbtproject: dbtProject):
    """
    reads tables from the input_schema to create a dbt sources.yml
    uses the metadata from the existing source definitions, if any
    """
    input_schema = config["source_schema"]
    source_name = config["source_name"]

    tablenames = warehouse.get_tables(input_schema)

    # Convert to the new format expected by upsert function
    sources_groups = {source_name: {input_schema: tablenames}}

    # Use the new upsert function with the schema as the rel_dir_to_models
    return upsert_multiple_sources_to_a_yaml(
        sources_groups=sources_groups, dbt_project=dbtproject, rel_dir_to_models=input_schema
    )


if __name__ == "__main__":
    load_dotenv("dbconnection.env")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    projectdir = os.getenv("DBT_PROJECT_DIR")

    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--source-schema", required=True)

    args = parser.parse_args()

    dbtproject = dbtProject(Path(args.project_dir))

    config = {"source_schema": args.source_schema, "source_name": args.source_name}

    warehouse = get_client()

    sync_sources(
        config,
        warehouse,
        dbtproject,
    )
