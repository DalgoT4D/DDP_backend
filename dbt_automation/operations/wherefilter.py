"""generates a model which filters using the sql where clause"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()

# sql, columns = wherefilter.where_filter_sql({
#     "source_columns": ["salinity", "Arsenic"],
#     "input": {
#         "input_type": "source",
#         "source_name": "destinations_v2",
#         "input_name": "Sheet2",
#     },
#     "where_type": "and",
#     "clauses": [
#         {
#             "column": "Iron",
#             "operator": ">=",
#             "value": "0"
#         },
#         {
#             "column": "Nitrate",
#             "operator": "<>",
#             "value": "50"
#         }
#     ]
# }, wc_client)
#
#       SELECT
#       `salinity`,
#       `Arsenic`
#       FROM {{source('destinations_v2', 'Sheet2')}}
#       WHERE (`Iron` >= '0'  AND `Nitrate` <> '50' )
#
# sql, columns = wherefilter.where_filter_sql({
#     "source_columns": ["salinity", "Arsenic"],
#     "input": {
#         "input_type": "source",
#         "source_name": "destinations_v2",
#         "input_name": "Sheet2",
#     },
#     "where_type": "sql",
#     "sql_snippet": "CAST(`Iron`, INT64) > CAST(`Nitrate`, INT64)"
# }, wc_client)
#
#       SELECT
#       `salinity`,
#       `Arsenic`
#       FROM {{source('destinations_v2', 'Sheet2')}}
#       WHERE (CAST(`Iron`, INT64) > CAST(`Nitrate`, INT64))


# pylint:disable=unused-argument,logging-fstring-interpolation
def where_filter_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config["source_columns"]
    input_table = config["input"]
    clauses: dict = config.get("clauses", [])
    clause_type: str = config.get("where_type", "and")  # and, or, sql
    sql_snippet: str = config.get("sql_snippet", "")

    dbt_code = "SELECT\n"

    select_from = source_or_ref(**input_table)

    dbt_code += ",\n".join(
        [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
    )
    dbt_code += "\n"

    select_from = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    # where
    if len(clauses) == 0 and not sql_snippet:
        raise ValueError("No where clause provided")

    dbt_code += "WHERE ("

    if clause_type in ["and", "or"]:
        temp = []
        for clause in clauses:
            operand = clause["operand"]

            clause = (
                f"{quote_columnname(clause['column'], warehouse.name)} "
                + f"{clause['operator']} "
                + f"{quote_columnname(operand['value'], warehouse.name) if operand['is_col'] else quote_constvalue(str(operand['value']), warehouse.name)} "
            )
            temp.append(clause)

        dbt_code += f" {clause_type.upper()} ".join(temp)

    elif clause_type == "sql":
        dbt_code += f"{sql_snippet}"

    dbt_code += ")"

    return dbt_code, source_columns


def where_filter(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = where_filter_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
