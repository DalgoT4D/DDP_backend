"""
This file contains the replace operation for dbt automation
Note this operation does a full replace of the column value(s)
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue

from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def replace_dbt_sql(config: dict, warehouse: WarehouseInterface):
    """
    performs a replace on a column using REPLACE
    config["input"] is dict {"source_name": "", "input_name": "", "input_type": ""}
    """
    source_columns = config.get("source_columns", [])
    columns = config.get("columns", [])
    output_columns = []

    dbt_code = "SELECT\n"

    for col_name in source_columns:
        dbt_code += f"{quote_columnname(col_name, warehouse.name)},\n"

    for column_dict in columns:
        col_name = column_dict["col_name"]
        output_col_name = column_dict["output_column_name"]

        """REPLACE(REPLACE( ... ), "find" , "replace") As output_name"""
        replace_sql_str = f"{quote_columnname(col_name, warehouse.name)}"
        for op in column_dict["replace_ops"]:
            replace_sql_str = f"REPLACE({replace_sql_str}, {quote_constvalue(op['find'], warehouse.name)}, {quote_constvalue(op['replace'], warehouse.name)})"
        replace_sql_str += f" AS {quote_columnname(output_col_name, warehouse.name)}, "

        dbt_code += replace_sql_str
        output_columns.append(output_col_name)

    dbt_code = dbt_code[:-2] + f"\n"
    select_from = source_or_ref(**config["input"])

    if config["input"]["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code, source_columns + output_columns


def replace(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform arithmetic operations and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = replace_dbt_sql(config, warehouse)
    dbt_sql += dbt_sql + "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbtproject.write_model(
        config["dest_schema"], config["output_name"], dbt_sql
    )

    return model_sql_path, output_cols
