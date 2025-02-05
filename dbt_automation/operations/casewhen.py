"""
Generates a model that create a new col using CASE WHEN
"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def casewhen_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config.get("source_columns", [])
    when_clauses: list[dict] = config.get("when_clauses", [])
    else_clause: dict = config.get("else_clause", None)
    output_col_name: str = config.get("output_column_name", "output_col")
    case_type: str = config.get("case_type", "simple")  # by default its the simple case
    sql_snippet: str = config.get("sql_snippet", "")
    input_table = config["input"]

    if len(when_clauses) == 0:
        raise ValueError("No when clauses specified")

    dbt_code = "SELECT\n"

    dbt_code += ",\n".join(
        [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
    )

    if case_type == "simple":
        dbt_code += ",\nCASE"
        for clause in when_clauses:
            clause_col = quote_columnname(clause["column"], warehouse.name)
            operator = clause["operator"]
            # for between operator we will have two operands; reset will have one operand
            operands = [
                (
                    quote_columnname(operand["value"], warehouse.name)
                    if operand["is_col"]
                    else quote_constvalue(str(operand["value"]), warehouse.name)
                )
                for operand in clause["operands"]
            ]
            then_value = (
                quote_columnname(clause["then"]["value"], warehouse.name)
                if clause["then"]["is_col"]
                else quote_constvalue(str(clause["then"]["value"]), warehouse.name)
            )

            operator_prefix = operator
            if operator == "between":
                operator_prefix = "BETWEEN"
                operator = " AND "

            # expression for between will be : BETWEEN col1 AND col2
            # expression for others will be : <= col1
            expression = f'{operator_prefix} {f"{operator}".join(operands)}'

            dbt_code += f"\n    WHEN {clause_col} {expression} THEN {then_value}"

        # default else value will be NULL
        else_value = "NULL"
        if else_clause["value"]:
            else_value = (
                quote_columnname(else_clause["value"], warehouse.name)
                if else_clause["is_col"]
                else quote_constvalue(str(else_clause["value"]), warehouse.name)
            )
        dbt_code += f"\n    ELSE {else_value}"
        dbt_code += f"\nEND AS {quote_columnname(output_col_name, warehouse.name)}\n"

    elif case_type == "advance":
        # custom sql snippet
        dbt_code += f",\n{sql_snippet}\n"

    select_from = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code, source_columns + [output_col_name]


def casewhen(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = casewhen_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
