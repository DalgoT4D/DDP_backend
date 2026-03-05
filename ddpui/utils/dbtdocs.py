# https://hiflylabs.com/2023/03/16/dbt-docs-as-a-static-website/
import json
import re
import os
from pathlib import Path

from ddpui.utils.file_storage.storage_factory import StorageFactory


def create_single_html(orgslug: str) -> str:
    """put the manifest and catalog into the generated index and return"""
    storage = StorageFactory.get_storage_adapter()
    dbttargetdir = Path(os.getenv("CLIENTDBT_ROOT")) / orgslug / "dbtrepo" / "target"

    content_index = storage.read_file(str(dbttargetdir / "index.html"))

    manifest_content = storage.read_file(str(dbttargetdir / "manifest.json"))
    json_manifest = json.loads(manifest_content)

    # In the static website there are 2 more projects inside the documentation: dbt and dbt_bigquery
    # This is technical information that we don't want to provide to our final users, so we drop it
    # Note: depends on the connector, here we use BigQuery
    ignore_projects = ["dbt", "dbt_bigquery"]
    for element_type in [
        "nodes",
        "sources",
        "macros",
        "parent_map",
        "child_map",
    ]:  # navigate into manifest
        # We transform to list to not change dict size during iteration, we use default value {} to handle KeyError
        for key in list(json_manifest.get(element_type, {}).keys()):
            for ignore_project in ignore_projects:
                if re.match(
                    rf"^.*\.{ignore_project}\.", key
                ):  # match with string that start with '*.<ignore_project>.'
                    del json_manifest[element_type][key]  # delete element

    catalog_content = storage.read_file(str(dbttargetdir / "catalog.json"))
    json_catalog = json.loads(catalog_content)

    # create single docs file in public folder
    search_str = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'
    new_str = (
        "o=[{label: 'manifest', data: "
        + json.dumps(json_manifest)
        + "},{label: 'catalog', data: "
        + json.dumps(json_catalog)
        + "}]"
    )
    new_content = content_index.replace(search_str, new_str)
    return new_content
