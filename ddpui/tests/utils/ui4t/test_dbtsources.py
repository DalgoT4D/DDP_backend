import pytest
import yaml
import os
from dbt_automation.utils.dbtsources import (
    readsourcedefinitions,
    mergesource,
    mergetable,
    merge_sourcedefinitions,
)


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


def test_readsourcefile_file_not_found(
    tmpdir,
):  # pytest tmpdir fixture
    """test the readsourcedefinitions function"""
    # failure
    schema = "staging"
    sources_file = tmpdir / "sources" / schema / "sources.yml"
    with pytest.raises(FileNotFoundError):
        readsourcedefinitions(sources_file)


def test_reasourcefile_success(tmpdir, sources_yaml):
    # success
    schema = "staging"
    sources_file = tmpdir / "sources" / schema / "sources.yml"
    if not os.path.exists(tmpdir / "sources" / schema):
        os.makedirs(tmpdir / "sources" / schema)
    with open(sources_file, "w", encoding="utf-8") as outfile:
        yaml.safe_dump(sources_yaml, outfile, sort_keys=False)
        outfile.close()

    assert readsourcedefinitions(sources_file) == sources_yaml


@pytest.mark.parametrize(
    "table",
    [
        {
            "name": "Sheet1",
            "identifier": "_airbyte_raw_Sheet1",
            "description": "this description will over written by the same source in the file",
        },
        {
            "name": "Sheet3",
            "identifier": "_airbyte_raw_Sheet3",
            "description": "this description wont be written since it doesnt exist in the sources file",
        },
    ],
)
def test_mergetable(table, sources_yaml):
    """test the mergetable function"""
    source_file_tables = sources_yaml["sources"][0]["tables"]
    table_exists_src_file = [
        src_table
        for src_table in source_file_tables
        if src_table["identifier"] == table["identifier"]
    ]
    assert (
        mergetable(table, source_file_tables) == table_exists_src_file[0]
        if len(table_exists_src_file) == 1
        else table
    )


@pytest.mark.parametrize(
    "dbsource,expected_result",
    [
        (
            {
                "name": "Sheets",
                "schema": "staging",
                "tables": [
                    {
                        "name": "_airbyte_raw_Sheet3",
                        "identifier": "_airbyte_raw_Sheet3",
                        "description": "",
                    },
                    {
                        "name": "_airbyte_raw_Sheet4",
                        "identifier": "_airbyte_raw_Sheet4",
                        "description": "",
                    },
                ],
            },
            {
                "name": "Sheets",
                "schema": "staging",
                "tables": [
                    {
                        "name": "_airbyte_raw_Sheet3",
                        "identifier": "_airbyte_raw_Sheet3",
                        "description": "",
                    },
                    {
                        "name": "_airbyte_raw_Sheet4",
                        "identifier": "_airbyte_raw_Sheet4",
                        "description": "",
                    },
                ],
            },
        ),
        (
            {
                "name": "Sheets",
                "schema": "staging",
                "tables": [
                    {
                        "name": "_airbyte_raw_Sheet1",
                        "identifier": "_airbyte_raw_Sheet1",
                        "description": "",
                    },
                    {
                        "name": "_airbyte_raw_Sheet2",
                        "identifier": "_airbyte_raw_Sheet2",
                        "description": "",
                    },
                ],
            },
            SOURCES_YAML["sources"][0],
        ),
        (
            {
                "name": "Sheets",
                "schema": "staging_new",
                "tables": [
                    {
                        "name": "_airbyte_raw_Sheet1",
                        "identifier": "_airbyte_raw_Sheet1",
                        "description": "",
                    },
                    {
                        "name": "_airbyte_raw_Sheet2",
                        "identifier": "_airbyte_raw_Sheet2",
                        "description": "",
                    },
                ],
            },
            {
                "name": "Sheets",
                "schema": "staging_new",
                "tables": [
                    {
                        "name": "_airbyte_raw_Sheet1",
                        "identifier": "_airbyte_raw_Sheet1",
                        "description": "",
                    },
                    {
                        "name": "_airbyte_raw_Sheet2",
                        "identifier": "_airbyte_raw_Sheet2",
                        "description": "",
                    },
                ],
            },
        ),
    ],
)
def test_mergesource(dbsource, expected_result, sources_yaml):
    """test the mergesource function"""
    sources_file = sources_yaml["sources"]
    assert mergesource(dbsource, sources_file) == expected_result


@pytest.mark.parametrize(
    "dbdefs,expected_result",
    [
        (
            {
                "version": 2,
                "sources": [
                    {
                        "name": "Sheets",
                        "schema": "staging",
                        "tables": [
                            {
                                "name": "_airbyte_raw_Sheet3",
                                "identifier": "_airbyte_raw_Sheet3",
                                "description": "",
                            },
                            {
                                "name": "_airbyte_raw_Sheet4",
                                "identifier": "_airbyte_raw_Sheet4",
                                "description": "",
                            },
                        ],
                    }
                ],
            },
            {
                "version": 2,
                "sources": [
                    {
                        "name": "Sheets",
                        "schema": "staging",
                        "tables": [
                            {
                                "name": "_airbyte_raw_Sheet3",
                                "identifier": "_airbyte_raw_Sheet3",
                                "description": "",
                            },
                            {
                                "name": "_airbyte_raw_Sheet4",
                                "identifier": "_airbyte_raw_Sheet4",
                                "description": "",
                            },
                        ],
                    }
                ],
            },
        ),
        (
            {
                "version": 2,
                "sources": [
                    {
                        "name": "Sheets",
                        "schema": "staging_new",
                        "tables": [
                            {
                                "name": "_airbyte_raw_Sheet1",
                                "identifier": "_airbyte_raw_Sheet1",
                                "description": "",
                            },
                            {
                                "name": "_airbyte_raw_Sheet2",
                                "identifier": "_airbyte_raw_Sheet2",
                                "description": "",
                            },
                        ],
                    }
                ],
            },
            {
                "version": 2,
                "sources": [
                    {
                        "name": "Sheets",
                        "schema": "staging_new",
                        "tables": [
                            {
                                "name": "_airbyte_raw_Sheet1",
                                "identifier": "_airbyte_raw_Sheet1",
                                "description": "",
                            },
                            {
                                "name": "_airbyte_raw_Sheet2",
                                "identifier": "_airbyte_raw_Sheet2",
                                "description": "",
                            },
                        ],
                    }
                ],
            },
        ),
        (
            {
                "version": 2,
                "sources": [
                    {
                        "name": "Sheets",
                        "schema": "staging",
                        "tables": [
                            {
                                "name": "_airbyte_raw_Sheet1",
                                "identifier": "_airbyte_raw_Sheet1",
                                "description": "",
                            },
                            {
                                "name": "_airbyte_raw_Sheet2",
                                "identifier": "_airbyte_raw_Sheet2",
                                "description": "",
                            },
                        ],
                    }
                ],
            },
            {"version": 2, "sources": [SOURCES_YAML["sources"][0]]},
        ),
    ],
)
def test_merge_sourcedefinitions(dbdefs, expected_result, sources_yaml):
    """test the merge_sourcedefinitions function"""
    filedefs = sources_yaml

    assert merge_sourcedefinitions(filedefs, dbdefs) == expected_result
