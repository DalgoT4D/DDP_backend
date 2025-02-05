from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

def raw_generic_dbt_sql(
    config: str,
    warehouse: WarehouseInterface,
):
    """
    Parses the given SQL statements to generate DBT code, handling an optional WHERE clause.
    """
    sql_statement_1 = config.get('sql_statement_1')
    sql_statement_2 = config.get('sql_statement_2', '')
    output_cols = []

    if not sql_statement_1:
        raise ValueError("Primary SQL statement (sql_statement_1) is required")

    # Check if 'SELECT' is part of the sql_statement_1, if not, prepend it
    if not sql_statement_1.strip().lower().startswith('select'):
        sql_statement_1 = "SELECT " + sql_statement_1

    dbt_code = f"{sql_statement_1}"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "  FROM " + select_from
    else:
        dbt_code += "  FROM " + "{{" + select_from + "}}"

    if sql_statement_2:
        dbt_code += " " + sql_statement_2

    return dbt_code, output_cols

def generic_sql_function(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform a generic SQL function operation.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = raw_generic_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_model_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, dbt_sql + select_statement)

    return model_sql_path, output_cols
