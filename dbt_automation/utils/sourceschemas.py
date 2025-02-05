"""helpers for working with dbt source configs"""

import yaml


# ================================================================================
def mksourcedefinition(sourcename: str, input_schema: str, tables: list):
    """generates the data structure for a dbt sources.yml"""
    airbyte_prefix = "_airbyte_raw_"

    source = {"name": sourcename, "schema": input_schema, "tables": []}

    for tablename in tables:
        cleaned_name = tablename
        source["tables"].append(
            {
                "name": cleaned_name,
                "identifier": tablename,
                "description": "",
            }
        )

    sourcedefinitions = {
        "version": 2,
        "sources": [source],
    }
    return sourcedefinitions


# ================================================================================
def get_source(filename: str, input_schema: str) -> dict:
    """read the config file containing `sources` keys and return the source
    matching the input schema"""
    with open(filename, "r", encoding="utf-8") as sources_file:
        sources = yaml.safe_load(sources_file)

        return next(
            (src for src in sources["sources"] if src["schema"] == input_schema), None
        )
