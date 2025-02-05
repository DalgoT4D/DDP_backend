"""
Generates a model after grouping by and aggregating columns
"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()

# sql, columns = groupby.groupby_dbt_sql({
#     "source_columns": ["NGO", "Month"],
#     "aggregate_on": [
#         {
#             "operation": "count",
#             "column": "measure1",
#             "output_column_name": "measure1__count",
#         },
#         {
#             "operation": "countdistinct",
#             "column": "measure2",
#             "output_column_name": "measure2__count",
#         },
#         {
#             "operation": "sum",
#             "column": "Indicator",
#             "output_column_name": "sum_of_indicator"
#         },
#     ],
#     "input": {
#         "input_type": "source",
#         "source_name": "pytest_intermediate",
#         "input_name": "arithmetic_add",
#     },
# }, wc_client)
#
# =>
#
#       SELECT
#       "NGO",
#       "Month",
#       COUNT("measure1") AS "measure1__count",
#       COUNT(DISTINCT "measure2") AS "measure2__count",
#       SUM("Indicator") AS "sum_of_indicator"
#       FROM {{source('pytest_intermediate', 'arithmetic_add')}}
#       GROUP BY "NGO","Month"


# pylint:disable=unused-argument,logging-fstring-interpolation
def groupby_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config["source_columns"]
    aggregate_on: list[dict] = config.get("aggregate_on", [])
    input_table = config["input"]

    dbt_code = "SELECT\n"

    dbt_code += ",\n".join(
        [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
    )

    for agg_col in aggregate_on:
        if agg_col["operation"] == "count":
            dbt_code += (
                f",\n COUNT({quote_columnname(agg_col['column'], warehouse.name)})"
            )
        elif agg_col["operation"] == "countdistinct":
            dbt_code += f",\n COUNT(DISTINCT {quote_columnname(agg_col['column'], warehouse.name)})"
        else:
            dbt_code += f",\n {agg_col['operation'].upper()}({quote_columnname(agg_col['column'], warehouse.name)})"

        dbt_code += (
            f" AS {quote_columnname(agg_col['output_column_name'], warehouse.name)}"
        )

    dbt_code += "\n"
    select_from = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    if len(source_columns) > 0:
        dbt_code += "GROUP BY "
        dbt_code += ",".join(
            [quote_columnname(col_name, warehouse.name) for col_name in source_columns]
        )

    output_columns = source_columns + [
        col["output_column_name"] for col in aggregate_on
    ]

    return dbt_code, output_columns


def groupby(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = groupby_dbt_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
