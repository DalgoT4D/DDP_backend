"""
Generates a dbt model for unpivot
This operation will only work in the chain of mergeoperations if its at the first step
"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def unpivot_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config.get("source_columns", [])  # all columns
    exclude_columns = config.get(
        "exclude_columns", []
    )  # exclude from unpivot but keep in the resulting table
    unpivot_on_columns = config.get("unpivot_columns", [])  # columns to unpivot
    input_table = config["input"]
    field_name = config.get("unpivot_field_name", "field_name")
    value_name = config.get("unpivot_value_name", "value")
    cast_datatype_to = config.get("cast_to", "varchar")
    if not cast_datatype_to and warehouse.name == "bigquery":
        cast_datatype_to = "STRING"

    if len(unpivot_on_columns) == 0:
        raise ValueError("No columns specified for unpivot")

    output_columns = list(set(exclude_columns) | set(unpivot_on_columns))  # union
    remove_columns = list(set(source_columns) - set(output_columns))

    dbt_code = "{{ unpivot("
    dbt_code += source_or_ref(**input_table)
    dbt_code += ", exclude="
    dbt_code += (
        "["
        + ",".join(
            [quote_constvalue(col_name, warehouse.name) for col_name in exclude_columns]
        )
        + "] ,"
    )
    dbt_code += f"cast_to={quote_constvalue(cast_datatype_to, warehouse.name)}, "
    dbt_code += "remove="
    dbt_code += (
        "["
        + ",".join(
            [quote_constvalue(col_name, warehouse.name) for col_name in remove_columns]
        )
        + "] ,"
    )
    dbt_code += f"field_name={quote_constvalue(field_name, warehouse.name)}, value_name={quote_constvalue(value_name, warehouse.name)}"
    dbt_code += ")}}\n"

    return dbt_code, output_columns


def unpivot(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = unpivot_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
