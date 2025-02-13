"""generates models to flatten airbyte raw data"""

import sys
from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.sourceschemas import get_source
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.dbtconfigs import mk_model_config
from dbt_automation.utils.columnutils import make_cleaned_column_names, dedup_list
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface


basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=logging-fstring-interpolation,unused-argument
def flatten_operation(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    This function does the flatten operation for all sources (raw tables) in the sources.yml.
    By default, _airbyte_data field is used to flatten
    """

    dbtproject = dbtProject(project_dir)
    logger.info("created the dbt project object")

    SOURCE_SCHEMA = config["source_schema"]
    DEST_SCHEMA = config["dest_schema"]

    # create the output directory
    dbtproject.ensure_models_dir(DEST_SCHEMA)
    logger.info("dbt models directory exists")

    # locate the sources.yml for the input-schema
    sources_filename = dbtproject.sources_filename(SOURCE_SCHEMA)
    logger.info("successfully located the sources.yml file")

    # find the source in that file... it should be the only one
    source = get_source(sources_filename, SOURCE_SCHEMA)
    if source is None:
        logger.error("no source for schema %s in %s", SOURCE_SCHEMA, sources_filename)
        sys.exit(1)

    # for every table in the source, generate an output model file
    models = []

    for srctable in source["tables"]:
        modelname = srctable["name"]
        tablename = srctable["identifier"]
        logger.info(f"flattening table {tablename}")

        sql_columns = []
        json_fields = []
        # get the field names from the json objects
        json_fields = warehouse.get_json_columnspec(
            SOURCE_SCHEMA, tablename, "_airbyte_data"
        )

        # convert to sql-friendly column names
        sql_columns = make_cleaned_column_names(json_fields)

        # after cleaning we may have duplicates
        sql_columns = dedup_list(sql_columns)

        # create the configuration
        model_config = mk_model_config(DEST_SCHEMA, modelname, sql_columns)
        models.append(model_config)

        # and the .sql model
        model_sql = mk_dbtmodel(
            warehouse,
            DEST_SCHEMA,
            source["name"],  # pass the source in the yaml file
            modelname,
            zip(json_fields, sql_columns),
        )
        dbtproject.write_model(DEST_SCHEMA, modelname, model_sql, logger=logger)

        logger.info(f"completed flattening {tablename}")

    # finally write the yml with the models configuration
    models_yml_path = dbtproject.write_model_config(DEST_SCHEMA, models, logger=logger)
    return models_yml_path


# ================================================================================
def mk_dbtmodel(
    warehouse, dest_schema: str, sourcename: str, srctablename: str, columntuples: list
):
    """create the .sql model for this table"""

    dbtmodel = f"""
{{{{ 
  config(
    materialized='table',
    schema='{dest_schema}',
    indexes=[
      {{'columns': ['_airbyte_ab_id'], 'type': 'hash'}}
    ]
  ) 
}}}}
    """
    dbtmodel += "SELECT _airbyte_ab_id "
    dbtmodel += "\n"

    for json_field, sql_column in columntuples:
        dbtmodel += (
            ","
            + warehouse.json_extract_op("_airbyte_data", json_field, sql_column)
            + "\n"
        )

    dbtmodel += f"FROM {{{{source('{sourcename}','{srctablename}')}}}}"
    dbtmodel += "\n"
    return dbtmodel


# ================================================================================
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    import argparse

    load_dotenv("dbconnection.env")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    projectdir = os.getenv("DBT_PROJECT_DIR")

    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--dest-schema", default="staging", help="e.g. staging")
    args = parser.parse_args()

    conn_info = {
        "host": os.getenv("DBHOST"),
        "port": os.getenv("DBPORT"),
        "username": os.getenv("DBUSER"),
        "password": os.getenv("DBPASSWORD"),
        "database": os.getenv("DBNAME"),
    }
    warehouse_client = get_client(args.warehouse, conn_info)

    flatten_operation(
        config={"source_schema": args.source_schema, "dest_schema": args.dest_schema},
        warehouse=warehouse_client,
        project_dir=projectdir,
    )
