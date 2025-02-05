"""generates a model which casts columns to specified SQL data types"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.postgres import PostgresClient
from dbt_automation.utils.tableutils import source_or_ref


basicConfig(level=INFO)
logger = getLogger()

WAREHOUSE_COLUMN_TYPES = {
    "postgres": {},
    "bigquery": {},
}


# pylint:disable=logging-fstring-interpolation
def cast_datatypes_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the cast_datatypes operation.
    """
    columns = config.get("columns", [])
    source_columns = config["source_columns"]

    dbt_code = "SELECT\n"

    select_from = source_or_ref(**config["input"])

    cast_columnnames = [c["columnname"] for c in columns]

    for column in source_columns:
        if column not in cast_columnnames:
            dbt_code += f"{quote_columnname(column, warehouse.name)},\n"

    for column in columns:
        if "columntype" in column:
            warehouse_column_type = WAREHOUSE_COLUMN_TYPES[warehouse.name].get(
                column["columntype"], column["columntype"]
            )
            dbt_code += f"CAST({quote_columnname(column['columnname'], warehouse.name)} AS {warehouse_column_type}) AS {quote_columnname(column['columnname'], warehouse.name)},\n"

    dbt_code = dbt_code.rstrip(",\n") + "\n"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code, source_columns


def cast_datatypes(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform casting of data types and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = cast_datatypes_sql(config, warehouse)
    dbt_sql += "\n" + select_statement
    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
