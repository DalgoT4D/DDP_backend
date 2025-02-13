"""
This file takes care of dbt string concat operations
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


def concat_columns_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the concat_columns operation.
    """
    output_column_name = config["output_column_name"]
    concat_columns = config["columns"]
    source_columns = config["source_columns"]

    dbt_code = "SELECT " + ", ".join(
        [quote_columnname(col, warehouse.name) for col in source_columns]
    )

    concat_fields = ",".join(
        [
            (
                quote_columnname(col["name"], warehouse.name)
                if col["is_col"]
                else f"'{col['name']}'"
            )
            for col in concat_columns
        ]
    )
    dbt_code += f", CONCAT({concat_fields}) AS {quote_columnname(output_column_name, warehouse.name)}"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"\nFROM {select_from}\n"
    else:
        dbt_code += f"\nFROM {{{{ {select_from} }}}}\n"

    return dbt_code, source_columns + [output_column_name]


def concat_columns(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform concatenation of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = concat_columns_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbt_project.write_model(
        config["dest_schema"], config["output_name"], dbt_sql
    )

    return model_sql_path, output_cols
