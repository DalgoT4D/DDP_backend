import os
import yaml
from pathlib import Path


# TODO: need to take into account the multiple schemas in a single source file


# ================================================================================
def readsourcedefinitions(sourcefilename: str):
    """read the source definitions from a dbt sources.yml"""
    with open(sourcefilename, "r", encoding="utf-8") as sourcefile:
        sourcedefinitions = yaml.safe_load(sourcefile)
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
        filesource = next(
            fs for fs in filesources if fs["schema"] == dbsource["schema"]
        )
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


# ================================================================================
def read_sources(project_dir) -> list[dict]:
    """parse all yaml files inside models dir and read sources"""
    models_dir = Path(project_dir) / "models"

    # {"models/example/sources.yml": [{"name": "source1", "tables": [{"identifier": "table1"}]}
    sources = {}
    for root, dirs, files in os.walk(models_dir):
        for file in files:
            if file.endswith(".yml") or file.endswith(".yaml"):
                file_path = os.path.join(root, file)
                temp_sources = []
                with open(file_path, "r") as f:
                    yaml_data = yaml.safe_load(f)
                    if "sources" in yaml_data:
                        temp_sources = yaml_data["sources"]

                if len(temp_sources) > 0:
                    src_yml_path = Path(file_path).relative_to(project_dir)
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
