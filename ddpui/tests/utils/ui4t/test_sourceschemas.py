import pytest
import os
import yaml
from dbt_automation.utils.sourceschemas import get_source, mksourcedefinition

SOURCES_YAML = {
    "version": 2,
    "sources": [
        {
            "name": "Sheets",
            "schema": "staging",
            "tables": [
                {
                    "name": "Sheet1",
                    "identifier": "_airbyte_raw_Sheet1",
                    "description": "",
                },
                {
                    "name": "Sheet2",
                    "identifier": "_airbyte_raw_Sheet2",
                    "description": "",
                },
            ],
        }
    ],
}


@pytest.fixture
def sources_yaml():
    return SOURCES_YAML


def test_mk_source_definition():
    """test mksourcedefinition"""
    sourcename = "sourcename"
    input_schema = "input_schema"
    tables = ["_airbyte_raw_Sheet1", "_airbyte_raw_Sheet2"]
    sourcedefinition = mksourcedefinition(sourcename, input_schema, tables)
    assert sourcedefinition["sources"][0]["name"] == sourcename
    assert sourcedefinition["sources"][0]["schema"] == input_schema
    assert sourcedefinition["sources"][0]["tables"][0]["name"] == "_airbyte_raw_Sheet1"
    assert sourcedefinition["sources"][0]["tables"][1]["name"] == "_airbyte_raw_Sheet2"


def test_get_source(sources_yaml, tmpdir):
    """test get_source"""
    schema = "staging"
    sources_file = tmpdir / "sources" / schema / "sources.yml"
    if not os.path.exists(tmpdir / "sources" / schema):
        os.makedirs(tmpdir / "sources" / schema)
    with open(sources_file, "w", encoding="utf-8") as outfile:
        yaml.safe_dump(sources_yaml, outfile, sort_keys=False)
        outfile.close()

    input_schema = "schema_not_available"
    source = get_source(sources_file, input_schema)
    assert source is None

    source = get_source(sources_file, schema)
    assert source["name"] == sources_yaml["sources"][0]["name"]
    assert source["schema"] == sources_yaml["sources"][0]["schema"]
    assert source["tables"][0]["name"] == sources_yaml["sources"][0]["tables"][0]["name"]
    assert source["tables"][1]["name"] == sources_yaml["sources"][0]["tables"][1]["name"]
