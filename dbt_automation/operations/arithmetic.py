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


# pylint:disable=unused-argument,logging-fstring-interpolation
def arithmetic_dbt_sql(config: dict, warehouse: WarehouseInterface):
    """
    performs arithmetic operations: +/-/*//
    config["input"] is dict {"source_name": "", "input_name": "", "input_type": ""}
    """
    operator = config["operator"]
    operands = config["operands"]  # {"is_col": true, "value": "1"}[]
    output_col_name = config["output_column_name"]
    source_columns = config.get("source_columns", [])

    if operator not in ["add", "sub", "mul", "div"]:
        raise ValueError("Unknown operation")

    if len(operands) < 2:
        raise ValueError("At least two operands are required to perform operations")

    if operator == "div" and len(operands) != 2:
        raise ValueError("Division requires exactly two operands")

    dbt_code = "SELECT "

    dbt_code += ", ".join(
        [quote_columnname(col, warehouse.name) for col in source_columns]
    )

    # dbt_utils function safe_add, safe_subtract, safe_divide
    # takes input as single quoted fields for column eg. 'field1'. regardless of the warehouse

    if operator == "add":
        dbt_code += ","
        dbt_code += "{{dbt_utils.safe_add(["
        for operand in operands:
            dbt_code += f"{quote_constvalue(quote_columnname(str(operand['value']), warehouse.name), warehouse.name) if operand['is_col'] else quote_constvalue(str(operand['value']), warehouse.name)},"
        dbt_code = dbt_code[:-1]
        dbt_code += "])}}"
        dbt_code += f" AS {quote_columnname(output_col_name, warehouse.name)} "

    if operator == "mul":
        dbt_code += ","
        for operand in operands:
            dbt_code += f"{quote_columnname(str(operand['value']), warehouse.name) if operand['is_col'] else quote_constvalue(str(operand['value']), warehouse.name)} * "
        dbt_code = dbt_code[:-2]
        dbt_code += f" AS {quote_columnname(output_col_name, warehouse.name)} "

    if operator == "sub":
        dbt_code += ","
        dbt_code += "{{dbt_utils.safe_subtract(["
        for operand in operands:
            dbt_code += f"{quote_constvalue(quote_columnname(str(operand['value']), warehouse.name), warehouse.name) if operand['is_col'] else quote_constvalue(str(operand['value']), warehouse.name)},"
        dbt_code = dbt_code[:-1]
        dbt_code += "])}}"
        dbt_code += f" AS {quote_columnname(output_col_name, warehouse.name)} "

    if operator == "div":
        dbt_code += ","
        dbt_code += "{{dbt_utils.safe_divide("
        for operand in operands:
            dbt_code += f"{quote_constvalue(quote_columnname(str(operand['value']), warehouse.name), warehouse.name) if operand['is_col'] else quote_constvalue(str(operand['value']), warehouse.name)},"
        dbt_code += ")}}"
        dbt_code += f" AS {quote_columnname(output_col_name, warehouse.name)} "

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code, source_columns + [output_col_name]


def arithmetic(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform arithmetic operations and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = arithmetic_dbt_sql(config, warehouse)
    dbt_sql += dbt_sql + "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbtproject.write_model(
        config["dest_schema"], config["output_name"], dbt_sql
    )

    return model_sql_path, output_cols
