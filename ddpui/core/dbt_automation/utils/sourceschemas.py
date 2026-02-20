"""helpers for working with dbt source configs"""

import yaml

from ddpui.utils.file_storage.storage_factory import StorageFactory


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
    storage = StorageFactory.get_storage_adapter()
    content = storage.read_file(str(filename))
    sources = yaml.safe_load(content)

    return next((src for src in sources["sources"] if src["schema"] == input_schema), None)
