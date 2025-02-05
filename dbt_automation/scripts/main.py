"""
This file runs all the operations from the yaml file
"""

import argparse
import os
from logging import basicConfig, getLogger, INFO
import json
import yaml
from dotenv import load_dotenv
from dbt_automation.operations.generic import generic_function
from dbt_automation.operations.arithmetic import arithmetic
from dbt_automation.operations.mergeoperations import merge_operations
from dbt_automation.operations.rawsql import generic_sql_function
from dbt_automation.operations.scaffold import scaffold
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.operations.droprenamecolumns import drop_columns, rename_columns
from dbt_automation.operations.castdatatypes import cast_datatypes
from dbt_automation.operations.coalescecolumns import coalesce_columns
from dbt_automation.operations.concatcolumns import concat_columns
from dbt_automation.operations.mergetables import union_tables
from dbt_automation.operations.syncsources import sync_sources
from dbt_automation.operations.flattenairbyte import flatten_operation
from dbt_automation.operations.flattenjson import flattenjson
from dbt_automation.operations.regexextraction import regex_extraction
from dbt_automation.operations.replace import replace
from dbt_automation.operations.joins import join
from dbt_automation.operations.wherefilter import where_filter
from dbt_automation.operations.groupby import groupby
from dbt_automation.operations.aggregate import aggregate
from dbt_automation.operations.casewhen import casewhen
from dbt_automation.operations.pivot import pivot
from dbt_automation.operations.unpivot import unpivot

OPERATIONS_DICT = {
    "flatten": flatten_operation,
    "flattenjson": flattenjson,
    "unionall": union_tables,
    "syncsources": sync_sources,
    "castdatatypes": cast_datatypes,
    "coalescecolumns": coalesce_columns,
    "arithmetic": arithmetic,
    "concat": concat_columns,
    "dropcolumns": drop_columns,
    "renamecolumns": rename_columns,
    "regexextraction": regex_extraction,
    "scaffold": scaffold,
    "mergeoperations": merge_operations,
    "replace": replace,
    "join": join,
    "where": where_filter,
    "groupby": groupby,
    "aggregate": aggregate,
    "casewhen": casewhen,
    "pivot": pivot,
    "unpivot": unpivot,
    "generic": generic_function,
    "rawsql": generic_sql_function,
}

load_dotenv("./../dbconnection.env")

project_dir = os.getenv("DBT_PROJECT_DIR")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser(
    description="Run operations from the yaml config file: "
    + ", ".join(OPERATIONS_DICT.keys())
)
parser.add_argument(
    "-y",
    "--yamlconfig",
    required=True,
    help="Path to the yaml config file that defines your operations",
)
args = parser.parse_args()

# Load the YAML file
config_data = None
with open(args.yamlconfig, "r", encoding="utf-8") as yaml_file:
    config_data = yaml.safe_load(yaml_file)

if config_data is None:
    raise ValueError("Couldn't read the yaml config data")

# TODO: Add stronger validations for each operation here
if config_data["warehouse"] not in ["postgres", "bigquery"]:
    raise ValueError("unknown warehouse")

logger.info(f"running operations for warehouse {config_data['warehouse']}")

if config_data["warehouse"] == "postgres":
    conn_info = {
        "host": os.getenv("DBHOST"),
        "port": os.getenv("DBPORT"),
        "username": os.getenv("DBUSER"),
        "password": os.getenv("DBPASSWORD"),
        "database": os.getenv("DBNAME"),
    }
else:
    with open(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "r", encoding="utf-8"
    ) as gcp_creds:
        conn_info = json.loads(gcp_creds.read())
warehouse = get_client(config_data["warehouse"], conn_info)

# run operations to generate dbt model(s)
# pylint:disable=logging-fstring-interpolation
for op_data in config_data["operations"]:
    op_type = op_data["type"]
    config = op_data["config"]

    if op_type not in OPERATIONS_DICT:
        # ignore, rename operations to easily disable them
        continue

    logger.info(f"running the {op_type} operation")
    logger.info(f"using config {config}")
    output = OPERATIONS_DICT[op_type](
        config=config, warehouse=warehouse, project_dir=project_dir
    )
    logger.info(f"finished running the {op_type} operation")
    logger.info(output)

warehouse.close()
