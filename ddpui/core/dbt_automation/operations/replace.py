"""
This file contains the replace operation for dbt automation
Note this operation does a full replace of the column value(s)
"""

from logging import basicConfig, getLogger, INFO
from ddpui.core.dbt_automation.utils.dbtproject import dbtProject
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse as WarehouseInterface
from ddpui.core.dbt_automation.utils.columnutils import quote_columnname, quote_constvalue

from ddpui.core.dbt_automation.utils.tableutils import source_or_ref

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
    output_columns: list[str] = list(source_columns)

    select_expressions: list[str] = [
        quote_columnname(col_name, warehouse.name) for col_name in source_columns
    ]

    for col_dict in columns:
        col_name = col_dict["col_name"]
        output_col_name = col_dict["output_column_name"]
        replace_ops = col_dict["replace_ops"]
        if not replace_ops:
            raise ValueError(f"No replace operations provided for column {col_name}")

        replace_sql_str = f"{quote_columnname(col_name, warehouse.name)}"
        for op in replace_ops:
            replace_sql_str = f"REPLACE({replace_sql_str}, {quote_constvalue(op['find'], warehouse.name)}, {quote_constvalue(op['replace'], warehouse.name)})"
        replace_sql_str = (
            f"{replace_sql_str} AS {quote_columnname(output_col_name, warehouse.name)}"
        )

        if output_col_name in source_columns:
            source_col_idx = source_columns.index(output_col_name)
            select_expressions[source_col_idx] = replace_sql_str
        else:
            select_expressions.append(replace_sql_str)
            output_columns.append(output_col_name)

    dbt_code = "SELECT\n" + ",\n".join(select_expressions) + "\n"
    select_from = source_or_ref(**config["input"])

    if config["input"]["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code, output_columns


def replace(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform arithmetic operations and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"

    select_statement, output_cols = replace_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbtproject.write_model(config["dest_schema"], config["output_name"], dbt_sql)

    return model_sql_path, output_cols
