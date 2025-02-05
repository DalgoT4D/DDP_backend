"""takes a list of tables and a common column spec and creates a dbt model to merge them"""

import os

import argparse
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref
from dbt_automation.utils.columnutils import quote_constvalue

basicConfig(level=INFO)
logger = getLogger()

# sql, len_output_set = mergetables.union_tables_sql({
#     "input": {
#         "input_type": "source",
#         "source_name": "pytest_intermediate",
#         "input_name": "arithmetic_add",
#     },
#     "source_columns": [
#         "NGO", "Month", "measure1", "measure2"
#     ],
#     "other_inputs": [
#         {
#             "input": {
#                 "input_type": "source",
#                 "source_name": "pytest_intermediate",
#                 "input_name": "arithmetic_div",
#             },
#             "source_columns": [
#                 "NGO", "Month", "measure1",
#             ],
#         }
#     ],
# }, wc_client)

# {{ dbt_utils.union_relations(relations=[
#     source('pytest_intermediate', 'arithmetic_add'),
#     source('pytest_intermediate', 'arithmetic_div')
#     ] , include=['NGO','measure2','Month','measure1'])
# }}


# pylint:disable=unused-argument,logging-fstring-interpolation
def union_tables_sql(config, warehouse: WarehouseInterface):
    """
    Generates SQL code for unioning tables using the dbt_utils union_relations macro.
    NOTE: THIS OPERATION WONT WORK WITH CTES AS OFF NOW
    """
    input_tables = [
        {"input": config["input"], "source_columns": config["source_columns"]}
    ] + config.get("other_inputs", [])

    names = set()
    for table in input_tables:
        name = source_or_ref(**table["input"])
        if name in names:
            logger.error("This appears more than once: %s", name)
            raise ValueError("Duplicate inputs found")
        names.add(name)

    relations = "["
    for table in input_tables:
        relations += f"{source_or_ref(**table['input'])},"
    relations = relations[:-1]
    relations += "]"
    dbt_code = ""

    output_cols = set()
    for table in input_tables:
        output_cols.update(table["source_columns"])

    # pylint:disable=consider-using-f-string
    dbt_code += "{{ dbt_utils.union_relations("
    dbt_code += (
        f"relations={relations} , "
        + f"include=[{','.join([quote_constvalue(col, 'postgres') for col in list(output_cols)])}]"
    )
    dbt_code += ")}}"

    return dbt_code, list(output_cols)


def union_tables(config, warehouse: WarehouseInterface, project_dir):
    """Generates a dbt model which uses the dbt_utils union_relations macro to union tables."""
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]
    dbt_sql = ""
    if config["input"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = union_tables_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, dbt_sql)

    return model_sql_path, output_cols


if __name__ == "__main__":
    # pylint:disable=invalid-name
    def list_of_strings(arg: str):
        """converts a comma separated string into a list of strings"""
        return arg.split(",")

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--dest-schema", required=True)
    parser.add_argument("--tablenames", required=True, type=list_of_strings)
    args = parser.parse_args()

    load_dotenv("dbconnection.env")
    projectdir = os.getenv("DBT_PROJECT_DIR")

    union_tables(
        config={
            "output_name": args.output_name,
            "dest_schema": args.dest_schema,
            "tablenames": args.tablenames,
        },
        warehouse="",
        project_dir=projectdir,
    )
