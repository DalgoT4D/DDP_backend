"""
This file contains the airthmetic operations for dbt automation
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue

from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


def generic_function_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    source_columns: list of columns to copy from the input model
    computed_columns: list of computed columns with function_name, operands, and output_column_name
    function_name: name of the function to be used
    operands: list of operands to be passed to the function
    output_column_name: name of the output column
    """
    source_columns = config["source_columns"]
    computed_columns = config["computed_columns"]

    if source_columns == "*":
        dbt_code = "SELECT *"
    else:
        dbt_code = f"SELECT {', '.join([quote_columnname(col, warehouse.name) for col in source_columns])}"

    for computed_column in computed_columns:
        function_name = computed_column["function_name"]
        operands = [
            quote_columnname(str(operand["value"]), warehouse.name)
            if operand["is_col"]
            else quote_constvalue(str(operand["value"]), warehouse.name)
            for operand in computed_column["operands"]
        ]
        output_column_name = computed_column["output_column_name"]

        dbt_code += f", {function_name}({', '.join(operands)}) AS {output_column_name}"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f" FROM {select_from}"
    else:
        dbt_code += f" FROM {{{{{select_from}}}}}"

    return dbt_code, source_columns


def generic_function(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform a generic SQL function operation.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = generic_function_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_model_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, dbt_sql + select_statement)

    return model_sql_path, output_cols