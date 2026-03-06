from ddpui.core.dbt_automation.utils.dbtproject import dbtProject
from ddpui.utils.warehouse.old_client.warehouse_interface import WarehouseInterface
from ddpui.core.dbt_automation.utils.tableutils import source_or_ref
import sqlparse
import re


def _split_columns_respecting_parentheses(select_clause: str) -> list:
    """
    Split column expressions by comma while respecting parentheses.
    Example: "coalesce(a, b), c, sum(x, y) as total" -> ["coalesce(a, b)", "c", "sum(x, y) as total"]
    """
    if not select_clause:
        return []

    parts = []
    current = ""
    paren_count = 0

    for char in select_clause:
        if char == "(":
            paren_count += 1
        elif char == ")":
            paren_count -= 1
        elif char == "," and paren_count == 0:
            if current.strip():
                parts.append(current.strip())
            current = ""
            continue

        current += char

    if current.strip():
        parts.append(current.strip())

    return parts


def extract_output_columns_from_select_clause(select_clause: str, source_columns: list) -> list:
    """
    Extract output column names from SELECT clause part only.
    Input examples:
    - "*"
    - "*, func() as alias"
    - "col1, col2"
    - "col1, *"
    - "count(*) as total, col1"
    - "coalesce(a, b) as c, sum(x, y)"
    """
    if not select_clause:
        return []

    columns = []
    # Use the new parentheses-aware splitting instead of simple split(",")
    parts = _split_columns_respecting_parentheses(select_clause)

    for part in parts:
        if not part:
            continue

        # Handle *
        if part == "*":
            columns.extend(source_columns)
            continue

        # Handle AS aliases
        as_match = re.search(r"^(.*?)\s+AS\s+(\w+)$", part, re.IGNORECASE)
        if as_match:
            columns.append(as_match.group(2))
            continue

        # Handle implicit aliases (space-separated, no AS keyword)
        # Only if it doesn't contain parentheses (to avoid breaking function calls)
        if " " in part and "(" not in part:
            words = part.split()
            if len(words) >= 2:
                columns.append(words[-1])  # Last word is the alias
                continue

        # For functions, expressions, or simple column names
        # Clean up for valid column name
        clean_name = re.sub(r"[^\w]", "_", part.lower())
        clean_name = re.sub(r"_+", "_", clean_name).strip("_")
        if clean_name:
            columns.append(clean_name[:50])  # Limit length
        else:
            columns.append(f"col_{len(columns) + 1}")

    return columns


def raw_generic_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Parses the given SQL statements to generate DBT code, handling an optional WHERE clause.
    """
    sql_statement_1 = config.get("sql_statement_1")
    sql_statement_2 = config.get("sql_statement_2", "")
    source_columns = config.get("source_columns", [])

    # Extract output columns from SELECT clause
    output_cols = extract_output_columns_from_select_clause(sql_statement_1, source_columns)

    if not sql_statement_1:
        raise ValueError("Primary SQL statement (sql_statement_1) is required")

    # Check if 'SELECT' is part of the sql_statement_1, if not, prepend it
    if not sql_statement_1.strip().lower().startswith("select"):
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
        dbt_sql = "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"

    select_statement, output_cols = raw_generic_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_model_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, dbt_sql + select_statement)

    return model_sql_path, output_cols
