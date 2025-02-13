"""
Generates a model after aggregating columns
"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()

# sql, len_output_set = aggregate.aggregate_dbt_sql({
#     "input": {
#         "input_type": "source",
#         "source_name": "pytest_intermediate",
#         "input_name": "arithmetic_add",
#     },
#     "source_columns": [
#         "col_1", "col_2", "col_3",
#     ],
#     "aggregate_on": [
#         {
#             "operation": "count",
#             "column": "NGO",
#             "output_column_name": "count__ngo"
#         },
#         {
#             "operation": "countdistinct",
#             "column": "Month",
#             "output_column_name": "distinctmonths"
#         },
#     ],

# }, wc_client)

# SELECT
# "col_1",
# "col_2",
# "col_3",
# (SELECT  COUNT("NGO") FROM {{source('pytest_intermediate', 'arithmetic_add')}}
# ) AS "count__ngo",(SELECT  COUNT(DISTINCT "Month") FROM {{source('pytest_intermediate', 'arithmetic_add')}}
# ) AS "distinctmonths"
# FROM {{source('pytest_intermediate', 'arithmetic_add')}}


def select_from(input_table: dict):
    """generates the correct FROM clause for the input table"""
    selectfrom = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        return f"FROM {selectfrom}\n"
    return f"FROM {{{{{selectfrom}}}}}\n"


# pylint:disable=unused-argument,logging-fstring-interpolation
def aggregate_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config.get("source_columns", [])
    aggregate_on: list[dict] = config.get("aggregate_on", [])
    input_table = config["input"]

    dbt_code = "SELECT\n"

    if len(source_columns) > 0:
        dbt_code += ",\n".join(
            [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
        )
        dbt_code += ",\n"

    for agg_col in aggregate_on:
        dbt_code += "(SELECT "
        if agg_col["operation"] == "count":
            dbt_code += (
                f" COUNT({quote_columnname(agg_col['column'], warehouse.name)}) "
            )
        elif agg_col["operation"] == "countdistinct":
            dbt_code += f" COUNT(DISTINCT {quote_columnname(agg_col['column'], warehouse.name)}) "
        else:
            dbt_code += f" {agg_col['operation'].upper()}({quote_columnname(agg_col['column'], warehouse.name)}) "
        dbt_code += select_from(input_table)
        dbt_code += ")"
        dbt_code += (
            f" AS {quote_columnname(agg_col['output_column_name'], warehouse.name)},"
        )

    dbt_code = dbt_code[:-1]  # remove the last comma
    dbt_code += "\n"
    dbt_code += select_from(input_table)

    return dbt_code, source_columns + [
        col["output_column_name"] for col in aggregate_on
    ]


def aggregate(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = aggregate_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
