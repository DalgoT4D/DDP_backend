"""
This file will take all helpers need to work with dbt sources and dbt models
"""


def source_or_ref(source_name: str, input_name: str, input_type: str) -> str:
    if input_type not in ["source", "model", "cte"]:
        raise ValueError("invalid input type to select from")

    if input_type == "cte":
        return input_name

    source_or_ref = f"ref('{input_name}')"

    if input_type == "source":
        source_or_ref = f"source('{source_name}', '{input_name}')"

    return source_or_ref
