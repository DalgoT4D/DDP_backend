"""
Generates a dbt model for pivot
"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


def select_from(input_table: dict):
    """generates the correct FROM clause for the input table"""
    selectfrom = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        return f"FROM {selectfrom}\n"
    return f"FROM {{{{{selectfrom}}}}}\n"


# pylint:disable=unused-argument,logging-fstring-interpolation
def pivot_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config.get("source_columns", [])
    pivot_column_values = config.get("pivot_column_values", [])
    pivot_column_name = config.get("pivot_column_name", None)
    input_table = config["input"]

    if not pivot_column_name:
        raise ValueError("Pivot column name not provided")

    dbt_code = "SELECT\n"

    if len(source_columns) > 0:
        dbt_code += ",\n".join(
            [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
        )
        dbt_code += ",\n"

    dbt_code += "{{ dbt_utils.pivot("
    dbt_code += quote_constvalue(
        quote_columnname(pivot_column_name, warehouse.name), warehouse.name
    )
    dbt_code += ", "
    dbt_code += (
        "["
        + ",".join(
            [
                quote_constvalue(pivot_val, warehouse.name)
                for pivot_val in pivot_column_values
            ]
        )
        + "]"
    )
    dbt_code += ")}}\n"

    dbt_code += select_from(input_table)
    if len(source_columns) > 0:
        dbt_code += "GROUP BY "
        dbt_code += ",".join(
            [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
        )

    return dbt_code, source_columns + pivot_column_values


def pivot(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = pivot_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
