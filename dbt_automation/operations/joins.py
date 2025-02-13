"""
This operation implements the standard joins operation for dbt automation
"""

from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

# sql, len_output_set = joins.joins_sql({
#     "input": {
#         "input_type": "source",
#         "source_name": "pytest_intermediate",
#         "input_name": "arithmetic_add",
#     },
#     "source_columns": ["measure1", "measure2"],
#     "other_inputs": [
#         {
#             "input": {
#                 "input_type": "source",
#                 "source_name": "pytest_intermediate",
#                 "input_name": "arithmetic_div",
#             },
#             "source_columns": ["Indicator", "measure2", "Month"],
#             "seq": 1
#         },
#     ],
#     "join_type": "inner",
#     "join_on": {
#         "key1": "NGO",
#         "key2": "NGO",
#         "compare_with": "="
#     },
#     "dest_schema": "joined",
# }, wc_client)
#
# then sql is
#
#    SELECT "t1"."measure1",
#    "t1"."measure2",
#    "t2"."Indicator",
#    "t2"."measure2" AS "measure2_2",
#    "t2"."Month"
#    FROM {{source('pytest_intermediate', 'arithmetic_add')}} t1
#    INNER JOIN {{source('pytest_intermediate', 'arithmetic_div')}} t2
#    ON "t1"."NGO" = "t2"."NGO"
#
# and len_output_set = 5 (columns)


def joins_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """Create a JOIN operation."""

    input_tables = [
        {"input": config["input"], "source_columns": config["source_columns"], "seq": 1}
    ] + config.get("other_inputs", [])
    input_tables.sort(key=lambda x: x["seq"])
    join_type: str = config.get("join_type", "")
    join_on = config.get("join_on", {})

    if join_type not in ["inner", "left", "full outer"]:
        raise ValueError(f"join type {join_type} not supported")

    if len(input_tables) != 2:
        raise ValueError(
            f"join operation requires exactly 2 input tables not {len(input_tables)}"
        )

    aliases = ["t1", "t2"]

    # select
    dbt_code = "\nSELECT "

    output_set = set()  # to check for duplicate column names
    for i, (alias, input_table) in enumerate(zip(aliases, input_tables)):
        source_columns = input_table["source_columns"]

        for col_name in source_columns:
            dbt_code += f"{quote_columnname(alias, warehouse.name)}.{quote_columnname(col_name, warehouse.name)}"
            if col_name in output_set:
                dbt_code += (
                    f" AS {quote_columnname(col_name + f'_{i+1}', warehouse.name)},\n"
                )
                output_set.add(col_name + f"_{i+1}")
            else:
                dbt_code += ",\n"
                output_set.add(col_name)

    dbt_code = dbt_code[:-2]

    table1 = input_tables[0]["input"]
    select_from = source_or_ref(**table1)
    if table1["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + " " + aliases[0] + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + " " + aliases[0] + "\n"

    # join
    table2 = input_tables[1]["input"]
    join_with = source_or_ref(**table2)
    if table2["input_type"] == "cte":
        dbt_code += f" {join_type.upper()} JOIN " + join_with + " " + aliases[1] + "\n"
    else:
        dbt_code += (
            f" {join_type.upper()} JOIN "
            + "{{"
            + join_with
            + "}}"
            + " "
            + aliases[1]
            + "\n"
        )

    dbt_code += f" ON {quote_columnname(aliases[0], warehouse.name)}.{quote_columnname(join_on['key1'], warehouse.name)}"
    dbt_code += f" {join_on['compare_with']} {quote_columnname(aliases[1], warehouse.name)}.{quote_columnname(join_on['key2'], warehouse.name)}\n"

    return dbt_code, list(output_set)


def join(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Generate DBT model file for regex extraction.
    """
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]
    dbt_sql = ""
    if config["input"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = joins_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, dbt_sql)

    return model_sql_path, output_cols
