"""extract from a regex"""

from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref


def regex_extraction_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """Given a regex and a column name, extract the regex from the column."""
    columns = config.get("columns", {})
    source_columns = config["source_columns"]

    dbt_code = f"\nSELECT "

    select_expressions = []

    for col_name, regex in columns.items():
        if warehouse.name == "postgres":
            select_expressions.append(
                f"substring({quote_columnname(col_name, warehouse.name)} FROM '{regex}') AS {quote_columnname(col_name, warehouse.name)}"
            )
        elif warehouse.name == "bigquery":
            select_expressions.append(
                f"REGEXP_EXTRACT({quote_columnname(col_name, warehouse.name)}, r'{regex}') AS {quote_columnname(col_name, warehouse.name)}"
            )

    dbt_code += ", ".join(select_expressions)

    non_regex_columns = [col for col in source_columns if col not in columns]
    dbt_code += ", " + ", ".join(
        [quote_columnname(col, warehouse.name) for col in non_regex_columns]
    )

    dbt_code = dbt_code.rstrip(", ")

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code, non_regex_columns + list(columns.keys())


def regex_extraction(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Generate DBT model file for regex extraction.
    """
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = regex_extraction_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, dbt_sql)

    return model_sql_path, output_cols
