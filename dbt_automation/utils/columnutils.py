"""utility functions for operating on SQL column names"""

import re
from collections import Counter


def cleaned_column_name(colname: str) -> str:
    """cleans the column name"""
    colname = colname[:40]
    pattern = r"[^0-9a-zA-Z]"
    colname = re.sub(pattern, "_", colname)
    if colname.isdigit():
        colname = "c" + colname
    return colname


def make_cleaned_column_names(columns: list) -> list:
    """cleans the column names"""
    cleaned_names = [cleaned_column_name(colname) for colname in columns]
    if len(set(cleaned_names)) != len(cleaned_names):
        cleaned_names = dedup_list(cleaned_names)
    return cleaned_names


def dedup_list(names: list):
    """ensures list does not contain duplicates, by appending to
    any duplicates found"""
    column_name_counts = Counter()
    deduped_names = []
    for colname in names:
        column_name_counts[colname] += 1
        if column_name_counts[colname] == 1:
            deduped_name = colname
        else:
            deduped_name = (
                colname + "_" + chr(column_name_counts[colname] - 1 + ord("a"))
            )
        deduped_names.append(deduped_name)
    return deduped_names


def fmt_colname(colname: str, warehouse: str):
    """format a column name for the target warehouse"""
    if warehouse == "postgres":
        return '"' + colname + '"'
    elif warehouse == "bigquery":
        return colname.lower()
    else:
        raise ValueError(f"unsupported warehouse: {warehouse}")


def quote_columnname(colname: str, warehouse: str):
    """encloses the column name within the appropriate quotes"""
    if warehouse == "postgres":
        return '"' + colname + '"'
    elif warehouse == "bigquery":
        return "`" + colname + "`"
    else:
        raise ValueError(f"unsupported warehouse: {warehouse}")


def quote_constvalue(value: str, warehouse: str):
    """encloses a constant string value inside proper quotes"""
    if value is None or value.strip().lower() == "none":
        return "NULL"

    if warehouse == "postgres":
        return "'" + value + "'"
    elif warehouse == "bigquery":
        return "'" + value + "'"
    else:
        raise ValueError(f"unsupported warehouse: {warehouse}")
