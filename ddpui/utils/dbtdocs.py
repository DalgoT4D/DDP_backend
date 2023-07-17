# https://hiflylabs.com/2023/03/16/dbt-docs-as-a-static-website/
import json
import re
import os
from pathlib import Path


def create_single_html(orgslug: str) -> str:
    """put the manifest and catalog into the generated index and return"""
    dbttargetdir = Path(os.getenv("CLIENTDBT_ROOT")) / orgslug / "dbtrepo" / "target"
    with open(
        os.path.join(dbttargetdir, "index.html"), "r", encoding="utf-8"
    ) as indexfile:
        content_index = indexfile.read()

    with open(
        os.path.join(dbttargetdir, "manifest.json"), "r", encoding="utf-8"
    ) as manifestfile:
        json_manifest = json.load(manifestfile)

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

    with open(
        os.path.join(dbttargetdir, "catalog.json"), "r", encoding="utf-8"
    ) as catalogfile:
        json_catalog = json.load(catalogfile)

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
