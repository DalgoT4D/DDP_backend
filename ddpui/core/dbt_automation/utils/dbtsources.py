import os
import yaml
from pathlib import Path

from ddpui.utils.file_storage.storage_factory import StorageFactory


# TODO: need to take into account the multiple schemas in a single source file


# ================================================================================
def readsourcedefinitions(sourcefilename: str):
    """read the source definitions from a dbt sources.yml"""
    storage = StorageFactory.get_storage_adapter()
    content = storage.read_file(sourcefilename)
    sourcedefinitions = yaml.safe_load(content)
    return sourcedefinitions


# ================================================================================
def mergesource(dbsource: dict, filesources: list) -> dict:
    """
    finds the file source corresponding to the dbsource
    and update the name if possible
    """
    outputsource = {
        "name": None,
        "schema": dbsource["schema"],
        "tables": None,
    }

    try:
        filesource = next(fs for fs in filesources if fs["schema"] == dbsource["schema"])
        outputsource["name"] = filesource["name"]
        outputsource["tables"] = [
            mergetable(dbtable, filesource["tables"]) for dbtable in dbsource["tables"]
        ]
    except StopIteration:
        outputsource["name"] = dbsource["name"]
        outputsource["tables"] = dbsource["tables"]

    return outputsource


# ================================================================================
def mergetable(dbtable: dict, filetables: list):
    """
    finds the dbtable in the list of filetables by `identifier`
    copies over the name and description if found
    """
    outputtable = {
        "name": dbtable["identifier"],
        "identifier": dbtable["identifier"],
        "description": "",
    }
    for filetable in filetables:
        if outputtable["identifier"] == filetable["identifier"]:
            outputtable["name"] = filetable["name"]
            outputtable["description"] = filetable["description"]
            break
    return outputtable


# ================================================================================
def merge_sourcedefinitions(filedefs: dict, dbdefs: dict) -> dict:
    """outputs source definitions from dbdefs, with the descriptions from filedefs"""
    outputdefs = {}
    outputdefs["version"] = filedefs["version"]
    outputdefs["sources"] = [
        mergesource(dbsource, filedefs["sources"]) for dbsource in dbdefs["sources"]
    ]

    return outputdefs


def merge_sourcedefinitions_v2(filedefs: dict, newdefs: dict) -> dict:
    """
    merge the source definitions intelligently
    1. Merge filedefs under the source name
    2. Then make sure under the same source name, we have all the tables from newdefs
    3. Make sure there are no duplicate tables under the same source name
    """
    outputdefs = {}
    outputdefs = filedefs

    for new_source in newdefs["sources"]:
        # check if source name exists
        existing_source = next(
            (src for src in outputdefs["sources"] if src["name"] == new_source["name"]), None
        )
        if existing_source:
            # source exists, merge tables
            existing_table_ids = {table["identifier"] for table in existing_source["tables"]}
            for new_table in new_source["tables"]:
                if new_table["identifier"] not in existing_table_ids:
                    existing_source["tables"].append(new_table)
        else:
            # source does not exist, add it
            outputdefs["sources"].append(new_source)

    return outputdefs


# ================================================================================
def read_sources(project_dir) -> list[dict]:
    """parse all yaml files inside models dir and read sources"""
    models_dir = Path(project_dir) / "models"

    # {"models/example/sources.yml": [{"name": "source1", "tables": [{"identifier": "table1"}]}
    src_tables = []
    for root, dirs, files in os.walk(models_dir):
        for file in files:
            if file.endswith(".yml") or file.endswith(".yaml"):
                file_path = os.path.join(root, file)
                src_tables += read_sources_from_yaml(project_dir, file_path)

    return src_tables


def read_sources_from_yaml(project_dir, sources_yml_rel_path) -> list[dict]:
    """Read sources from a sources.yaml file"""
    sources = {}
    sources_yml_full_path = str(Path(project_dir) / sources_yml_rel_path)
    storage = StorageFactory.get_storage_adapter()
    content = storage.read_file(sources_yml_full_path)
    yaml_data = yaml.safe_load(content)
    temp_sources = []
    if "sources" in yaml_data:
        temp_sources = yaml_data["sources"]

        if len(temp_sources) > 0:
            src_yml_path = Path(sources_yml_full_path).relative_to(project_dir)
            sources[str(src_yml_path)] = temp_sources

    src_tables = []
    for src_yml_rel_path, srcs_yml in sources.items():
        # yaml can have more than one source
        for src in srcs_yml:
            for table in src["tables"]:
                # keeping the schema same as input of each operations
                src_tables.append(
                    {
                        "source_name": src["name"],
                        "input_name": table["identifier"],  # table
                        "input_type": "source",
                        "schema": src["schema"],
                        "sql_path": src_yml_rel_path,
                    }
                )

    return src_tables
