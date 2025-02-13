from typing import List
from dbt_automation.operations.arithmetic import arithmetic_dbt_sql
from dbt_automation.operations.coalescecolumns import (
    coalesce_columns_dbt_sql,
)
from dbt_automation.operations.concatcolumns import concat_columns_dbt_sql
from dbt_automation.operations.droprenamecolumns import (
    drop_columns_dbt_sql,
    rename_columns_dbt_sql,
)
from dbt_automation.operations.flattenjson import flattenjson_dbt_sql
from dbt_automation.operations.generic import generic_function_dbt_sql
from dbt_automation.operations.mergetables import union_tables_sql
from dbt_automation.operations.regexextraction import regex_extraction_sql
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.operations.castdatatypes import cast_datatypes_sql
from dbt_automation.operations.replace import replace_dbt_sql
from dbt_automation.operations.joins import joins_sql
from dbt_automation.operations.wherefilter import where_filter_sql
from dbt_automation.operations.groupby import groupby_dbt_sql
from dbt_automation.operations.aggregate import aggregate_dbt_sql
from dbt_automation.operations.casewhen import casewhen_dbt_sql
from dbt_automation.operations.flattenjson import flattenjson_dbt_sql
from dbt_automation.operations.mergetables import union_tables_sql
from dbt_automation.operations.pivot import pivot_dbt_sql
from dbt_automation.operations.unpivot import unpivot_dbt_sql
from dbt_automation.operations.rawsql import raw_generic_dbt_sql


def merge_operations_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code by merging SQL code from multiple operations.
    """
    for i, operation in enumerate(config["operations"]):
        operation["as_cte"] = f"cte{i+1}"  # this will go as WITH cte1 as (...)
        if i == 0:
            # first operation input can be model or source
            operation["config"]["input"] = config["input"]
        else:
            # select the previous as_cte as source for next operations
            operation["config"]["input"] = {
                "input_name": config["operations"][i - 1]["as_cte"],
                "input_type": "cte",
                "source_name": None,
            }

    operations = config["operations"]

    if not operations:
        return "-- No operations specified, no SQL generated.", []

    cte_sql_list = []
    output_cols = []  # return the last operations output columns

    # push select statements into the queue
    for cte_counter, operation in enumerate(operations):

        if operation["type"] == "castdatatypes":
            op_select_statement, out_cols = cast_datatypes_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "arithmetic":
            op_select_statement, out_cols = arithmetic_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "coalescecolumns":
            op_select_statement, out_cols = coalesce_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "concat":
            op_select_statement, out_cols = concat_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "dropcolumns":
            op_select_statement, out_cols = drop_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "renamecolumns":
            op_select_statement, out_cols = rename_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "flattenjson":
            op_select_statement, out_cols = flattenjson_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "regexextraction":
            op_select_statement, out_cols = regex_extraction_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "union_tables":
            op_select_statement, out_cols = union_tables_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "replace":
            op_select_statement, out_cols = replace_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "join":
            op_select_statement, out_cols = joins_sql(operation["config"], warehouse)
        elif operation["type"] == "where":
            op_select_statement, out_cols = where_filter_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "groupby":
            op_select_statement, out_cols = groupby_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "aggregate":
            op_select_statement, out_cols = aggregate_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "casewhen":
            op_select_statement, out_cols = casewhen_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "unionall":
            op_select_statement, out_cols = union_tables_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "pivot":
            op_select_statement, out_cols = pivot_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "unpivot":
            op_select_statement, out_cols = unpivot_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "generic":
            op_select_statement, out_cols = generic_function_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "rawsql":
            op_select_statement, out_cols = raw_generic_dbt_sql(
                operation["config"], warehouse
            )

        output_cols = out_cols

        cte_sql = f" , {operation['as_cte']} as (\n"
        if cte_counter == 0:
            cte_sql = f"WITH {operation['as_cte']} as (\n"
        cte_sql += op_select_statement
        cte_sql += f")"

        # last step
        if cte_counter == len(operations) - 1:
            prev_as_cte = operations[cte_counter]["as_cte"]
            cte_sql += "\n-- Final SELECT statement combining the outputs of all CTEs\n"
            cte_sql += f"SELECT *\nFROM {prev_as_cte}"

        cte_sql_list.append(cte_sql)

    return "".join(cte_sql_list), output_cols


def merge_operations(
    config: dict,
    warehouse: WarehouseInterface,
    project_dir: str,
):
    """
    Perform merging of operations and generate a DBT model.
    """

    dbt_sql = (
        "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}\n"
    )

    select_statement, output_cols = merge_operations_sql(
        config,
        warehouse,
    )
    dbt_sql += select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbt_project.write_model(
        config["dest_schema"], config["output_name"], dbt_sql
    )

    return model_sql_path, output_cols
