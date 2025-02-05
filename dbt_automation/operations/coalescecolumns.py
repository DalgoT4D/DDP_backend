"""generates a model which coalesces columns"""

import datetime
from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def coalesce_columns_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config["source_columns"]
    coalesce_columns = config.get("columns", [])
    output_col_name = config["output_column_name"]
    default_value = config.get("default_value", None)

    if isinstance(default_value, str):
        default_value = quote_constvalue(default_value, warehouse.name)
    elif isinstance(default_value, int):
        default_value = str(default_value)
    elif isinstance(default_value, datetime.datetime) or isinstance(
        default_value, datetime.date
    ):
        default_value = quote_constvalue(str(default_value), warehouse.name)
    else:
        default_value = "NULL"

    logger.info("using default value")
    logger.info(default_value)

    dbt_code = "SELECT\n"

    select_from = source_or_ref(**config["input"])

    for col_name in source_columns:
        dbt_code += f"{quote_columnname(col_name, warehouse.name)},\n"

    dbt_code += (
        "COALESCE("
        + ", ".join(
            [
                quote_columnname(col_name, warehouse.name)
                for col_name in coalesce_columns
            ]
            + [default_value]
        )
        + f") AS {quote_columnname(output_col_name, warehouse.name)}\n"
    )

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code, source_columns + [output_col_name]


def coalesce_columns(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = coalesce_columns_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
