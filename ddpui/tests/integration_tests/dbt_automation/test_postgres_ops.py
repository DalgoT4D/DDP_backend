"""tests for postgres operations"""

import os
from pathlib import Path
import math
import json
import subprocess
import shutil
import yaml
from logging import basicConfig, getLogger, INFO
from ddpui.core.dbt_automation import assets, seeds
from ddpui.core.dbt_automation.operations.flattenjson import flattenjson
from ddpui.core.dbt_automation.operations.droprenamecolumns import rename_columns, drop_columns
from ddpui.core.dbt_automation.operations.generic import generic_function
from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations
from ddpui.core.dbt_automation.operations.rawsql import generic_sql_function
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.core.dbtautomation_service import upsert_multiple_sources_to_a_yaml
from ddpui.core.dbt_automation.operations.flattenairbyte import flatten_operation
from ddpui.core.dbt_automation.operations.syncsources import sync_sources
from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns
from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns
from ddpui.core.dbt_automation.operations.arithmetic import arithmetic
from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes
from ddpui.core.dbt_automation.utils.dbtproject import dbtProject
from ddpui.core.dbt_automation.operations.regexextraction import regex_extraction
from ddpui.core.dbt_automation.operations.mergetables import union_tables
from ddpui.core.dbt_automation.operations.aggregate import aggregate
from ddpui.core.dbt_automation.operations.casewhen import casewhen
from ddpui.core.dbt_automation.operations.pivot import pivot
from ddpui.core.dbt_automation.operations.unpivot import unpivot
from ddpui.utils.warehouse.client.table_queries import list_table_names


basicConfig(level=INFO)
logger = getLogger()


def scaffold(config, warehouse, tmpdir):
    """Create a dbt project for integration tests and write a postgres profile."""
    project_name = config["project_name"]
    default_schema = config["default_schema"]
    project_root = Path(tmpdir)
    dbt_bin = shutil.which("dbt")
    if not dbt_bin:
        raise RuntimeError("dbt binary is not available on PATH")

    subprocess.check_call(
        [dbt_bin, "init", project_name, "--skip-profile-setup"],
        cwd=project_root,
    )

    project_dir = project_root / project_name

    example_models_dir = project_dir / "models" / "example"
    if example_models_dir.exists():
        shutil.rmtree(example_models_dir)

    assets_module_path = Path(assets.__file__).parent
    shutil.copy(assets_module_path / "packages.yml", project_dir / "packages.yml")
    macros_dir = project_dir / "macros"
    macros_dir.mkdir(exist_ok=True)
    for sql_file in assets_module_path.glob("*.sql"):
        shutil.copy(sql_file, macros_dir / sql_file.name)

    profile = {
        project_name: {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": os.environ.get("TEST_PG_DBHOST"),
                    "port": int(os.environ.get("TEST_PG_DBPORT", "5432")),
                    "user": os.environ.get("TEST_PG_DBUSER"),
                    "password": os.environ.get("TEST_PG_DBPASSWORD"),
                    "dbname": os.environ.get("TEST_PG_DBNAME"),
                    "schema": default_schema,
                    "threads": 4,
                }
            },
        }
    }
    with open(project_dir / "profiles.yml", "w", encoding="utf-8") as profile_file:
        yaml.safe_dump(profile, profile_file, sort_keys=False)

    schema = os.environ.get("TEST_PG_DBSCHEMA_SRC", default_schema)
    seed_files = [
        ("_airbyte_raw_Sheet1", "sample_sheet1.json"),
        ("_airbyte_raw_Sheet2", "sample_sheet2.json"),
    ]

    for table_name, seed_filename in seed_files:
        seed_path = Path(seeds.__file__).parent / seed_filename
        with open(seed_path, "r", encoding="utf-8") as seed_file:
            rows = json.load(seed_file)

        warehouse.execute_transaction(
            [
                (f"CREATE SCHEMA IF NOT EXISTS {schema};", None),
                (
                    f"""CREATE TABLE IF NOT EXISTS {schema}."{table_name}" (
                        _airbyte_ab_id character varying,
                        _airbyte_data jsonb,
                        _airbyte_emitted_at timestamp with time zone
                    );""",
                    None,
                ),
                (f"""TRUNCATE TABLE {schema}."{table_name}";""", None),
            ]
        )

        insert_query = f"""
            INSERT INTO {schema}."{table_name}" (_airbyte_ab_id, _airbyte_data, _airbyte_emitted_at)
            VALUES (:airbyte_ab_id, CAST(:airbyte_data AS jsonb), :airbyte_emitted_at)
        """
        insert_rows = [
            {
                "airbyte_ab_id": row["_airbyte_ab_id"],
                "airbyte_data": (
                    row["_airbyte_data"]
                    if isinstance(row["_airbyte_data"], str)
                    else json.dumps(row["_airbyte_data"])
                ),
                "airbyte_emitted_at": row["_airbyte_emitted_at"],
            }
            for row in rows
        ]
        warehouse.execute_many(insert_query, insert_rows)


class TestPostgresOperations:
    """test rename_columns operation"""

    warehouse = "postgres"
    test_project_dir = None
    wc_client = WarehouseFactory.connect(
        {
            "host": os.environ.get("TEST_PG_DBHOST"),
            "port": os.environ.get("TEST_PG_DBPORT"),
            "username": os.environ.get("TEST_PG_DBUSER"),
            "database": os.environ.get("TEST_PG_DBNAME"),
            "password": os.environ.get("TEST_PG_DBPASSWORD"),
        },
        "postgres",
    )
    schema = os.environ.get("TEST_PG_DBSCHEMA_SRC")  # source schema where the raw data lies

    @staticmethod
    def execute_dbt(cmd: str, select_model: str = None):
        try:
            dbt_bin = Path(TestPostgresOperations.test_project_dir) / "venv" / "bin" / "dbt"
            if not dbt_bin.exists():
                dbt_path = shutil.which("dbt")
                if not dbt_path:
                    raise RuntimeError("dbt binary is not available on PATH")
                dbt_bin = Path(dbt_path)

            select_cli = ["--select", select_model] if select_model is not None else []
            subprocess.check_call(
                [
                    str(dbt_bin),
                    cmd,
                ]
                + select_cli
                + [
                    "--project-dir",
                    TestPostgresOperations.test_project_dir,
                    "--profiles-dir",
                    TestPostgresOperations.test_project_dir,
                ],
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt {cmd} failed with {e.returncode}")
            raise Exception(f"Something went wrong while running dbt {cmd}")

    def test_scaffold(self, tmpdir):
        """This will setup the dbt repo to run dbt commands after running a test operation"""
        logger.info("starting scaffolding")
        project_name = "pytest_dbt"
        config = {
            "project_name": project_name,
            "default_schema": TestPostgresOperations.schema,
        }
        scaffold(config, TestPostgresOperations.wc_client, tmpdir)
        TestPostgresOperations.test_project_dir = Path(tmpdir) / project_name
        TestPostgresOperations.execute_dbt("deps")
        logger.info("finished scaffolding")

    def test_syncsources(self):
        """test the sync sources operation against the warehouse"""
        logger.info("syncing sources")
        config = {
            "source_name": "sample",
            "source_schema": TestPostgresOperations.schema,
        }
        sync_sources(
            config,
            TestPostgresOperations.wc_client,
            dbtProject(TestPostgresOperations.test_project_dir),
            TestPostgresOperations.warehouse,
        )
        sources_yml = (
            Path(TestPostgresOperations.test_project_dir)
            / "models"
            / TestPostgresOperations.schema
            / "sources.yml"
        )
        assert os.path.exists(sources_yml) is True
        logger.info("finished syncing sources")

    def test_flatten(self):
        """test the flatten operation against the warehouse"""
        wc_client = TestPostgresOperations.wc_client
        config = {
            "source_schema": TestPostgresOperations.schema,
            "dest_schema": "pytest_intermediate",
        }
        flatten_operation(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )
        TestPostgresOperations.execute_dbt("run", "_airbyte_raw_Sheet1")
        TestPostgresOperations.execute_dbt("run", "_airbyte_raw_Sheet2")
        logger.info("inside test flatten")
        logger.info(f"inside project directory : {TestPostgresOperations.test_project_dir}")
        tables = list_table_names(
            TestPostgresOperations.wc_client,
            TestPostgresOperations.warehouse,
            "pytest_intermediate",
        )
        assert "_airbyte_raw_Sheet1" in tables
        assert "_airbyte_raw_Sheet2" in tables

    def test_rename_columns(self):
        """test rename_columns for sample seed data"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "rename"
        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "columns": {"NGO": "ngo", "Month": "month"},
        }

        rename_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "ngo" in cols
        assert "month" in cols
        assert "NGO" not in cols
        assert "MONTH" not in cols

    def test_drop_columns(self):
        """test drop_columns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "drop"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "columns": ["MONTH"],
        }

        drop_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "MONTH" not in cols

    def test_coalescecolumns(self):
        """test coalescecolumns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "coalesce"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "columns": ["NGO", "SPOC"],
            "output_column_name": "coalesce",
        }

        coalesce_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "coalesce" in cols
        col_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        col_data_original = wc_client.get_table_data(
            "pytest_intermediate",
            "_airbyte_raw_Sheet1",
            1,
        )
        assert (
            col_data[0]["coalesce"] == col_data_original[0]["NGO"]
            if col_data_original[0]["NGO"] is not None
            else col_data_original[0]["SPOC"]
        )

    def test_concatcolumns(self):
        """test concatcolumns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "concat"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": [
                "NGO",
                "SPOC",
                "Month",
                "measure1",
                "measure2",
                "Indicator",
            ],
            "columns": [
                {
                    "name": "NGO",
                    "is_col": True,
                },
                {
                    "name": "SPOC",
                    "is_col": True,
                },
                {
                    "name": "test",
                    "is_col": False,
                },
            ],
            "output_column_name": "concat_col",
        }

        concat_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "concat_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert table_data[0]["concat_col"] == table_data[0]["NGO"] + table_data[0][
            "SPOC"
        ] + "".join([col["name"] for col in config["columns"] if not col["is_col"]])

    def test_castdatatypes(self):
        """test castdatatypes"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "cast"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "columns": [
                {
                    "columnname": "measure1",
                    "columntype": "int",
                },
                {
                    "columnname": "measure2",
                    "columntype": "int",
                },
            ],
        }

        cast_datatypes(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "measure1" in cols
        assert "measure2" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        # TODO: do stronger check here; fetch datatype from warehouse and then compare/assert
        assert type(table_data[0]["measure1"]) == int
        assert type(table_data[0]["measure2"]) == int

    def test_arithmetic_add(self):
        """test arithmetic addition"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_add"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "cast",
                "source_name": None,
            },  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "add",
            "operands": [
                {"value": "measure1", "is_col": True},
                {"value": "measure2", "is_col": True},
            ],
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "output_column_name": "add_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "add_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert table_data[0]["add_col"] == table_data[0]["measure1"] + table_data[0]["measure2"]

    def test_arithmetic_sub(self):
        """test arithmetic subtraction"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_sub"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "cast",
                "source_name": None,
            },  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "sub",
            "operands": [
                {"value": "measure1", "is_col": True},
                {"value": "measure2", "is_col": True},
            ],
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "output_column_name": "sub_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "sub_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert table_data[0]["sub_col"] == table_data[0]["measure1"] - table_data[0]["measure2"]

    def test_arithmetic_mul(self):
        """test arithmetic multiplication"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_mul"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "cast",
                "source_name": None,
            },  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "mul",
            "operands": [
                {"value": "measure1", "is_col": True},
                {"value": "measure2", "is_col": True},
            ],
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "output_column_name": "mul_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "mul_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert table_data[0]["mul_col"] == table_data[0]["measure1"] * table_data[0]["measure2"]

    def test_arithmetic_div(self):
        """test arithmetic division"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_div"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "cast",
                "source_name": None,
            },  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "div",
            "operands": [
                {"value": "measure1", "is_col": True},
                {"value": "measure2", "is_col": True},
            ],
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "output_column_name": "div_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "div_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            math.ceil(table_data[0]["measure1"] / table_data[0]["measure2"])
            if table_data[0]["measure2"] != 0
            else None
            == (
                math.ceil(table_data[0]["div_col"])
                if table_data[0]["div_col"] is not None
                else None
            )
        )

    def test_regexextract(self):
        """test regex extraction"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "regex_ext"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "columns": {"NGO": "^[C].*"},
        }

        regex_extraction(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "NGO" in cols
        table_data_org = wc_client.get_table_data(
            "pytest_intermediate",
            "_airbyte_raw_Sheet1",
            10,
        )
        table_data_org.sort(key=lambda x: x["Month"])
        table_data_regex = wc_client.get_table_data("pytest_intermediate", output_name, 10)
        table_data_regex.sort(key=lambda x: x["Month"])
        for regex, org in zip(table_data_regex, table_data_org):
            assert (
                regex["NGO"] == org["NGO"] if org["NGO"].startswith("C") else (regex["NGO"] is None)
            )

    def test_aggregate(self):
        """test aggregate col operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "aggregate_col"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "cast",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "aggregate_on": [
                {
                    "column": "measure1",
                    "operation": "sum",
                    "output_column_name": "agg1",
                },
                {
                    "column": "measure2",
                    "operation": "sum",
                    "output_column_name": "agg2",
                },
            ],
        }

        aggregate(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "agg1" in cols
        assert "agg2" in cols
        table_data_agg = wc_client.get_table_data("pytest_intermediate", output_name, 10)
        assert len(table_data_agg) == 5

    def test_casewhen(self):
        """test casewhen operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "casewhen"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet2",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "SPOC"],
            "when_clauses": [
                {
                    "column": "SPOC",
                    "operator": "=",
                    "operands": [{"value": "SPOC A", "is_col": False}],
                    "then": {"value": "A", "is_col": False},
                },
                {
                    "column": "SPOC",
                    "operator": "=",
                    "operands": [{"value": "SPOC B", "is_col": False}],
                    "then": {"value": "B", "is_col": False},
                },
                {
                    "column": "SPOC",
                    "operator": "=",
                    "operands": [{"value": "SPOC C", "is_col": False}],
                    "then": {"value": "C", "is_col": False},
                },
            ],
            "else_clause": {"value": "SPOC", "is_col": True},
            "output_column_name": "spoc_category_renamed",
        }

        casewhen(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "spoc_category_renamed" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 10)
        spoc_values = set([row["spoc_category_renamed"] for row in table_data])

        assert "SPOC A" not in spoc_values
        assert "SPOC B" not in spoc_values
        assert "SPOC C" not in spoc_values

    def test_pivot(self):
        """test pivot operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "pivot_op"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet2",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator", "SPOC"],
            "groupby_columns": ["SPOC"],
            "pivot_column_name": "NGO",
            "pivot_column_values": ["IMAGE", "FDSR", "CRC", "BAMANEH", "JTS"],
        }

        pivot(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert sorted(cols) == sorted(config["pivot_column_values"] + config["groupby_columns"])
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 10)
        assert len(table_data) == 3

    def test_unpivot(self):
        """test unpivot operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "unpivot_op"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet2",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": [
                "NGO",
                "SPOC",
                "Month",
                "measure1",
                "_airbyte_ab_id",
                "measure2",
                "Indicator",
            ],
            "exclude_columns": [],
            "unpivot_columns": ["NGO", "SPOC"],
            "unpivot_field_name": "col_field",
            "unpivot_value_name": "col_val",
        }

        unpivot(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert len(cols) == 2
        assert sorted(cols) == sorted([config["unpivot_field_name"], config["unpivot_value_name"]])

    def test_mergetables(self):
        """test merge tables"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "union"

        config = {
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "other_inputs": [
                {
                    "input": {
                        "input_type": "model",
                        "input_name": "_airbyte_raw_Sheet2",
                        "source_name": None,
                    },
                    "source_columns": [
                        "NGO",
                        "Month",
                        "measure1",
                        "measure2",
                        "Indicator",
                    ],
                },
            ],
        }

        union_tables(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        table_data1 = wc_client.get_table_data(
            "pytest_intermediate",
            "_airbyte_raw_Sheet1",
            10,
        )
        table_data2 = wc_client.get_table_data(
            "pytest_intermediate",
            "_airbyte_raw_Sheet2",
            10,
        )
        table_data_union = wc_client.get_table_data("pytest_intermediate", output_name, 10)

        assert len(table_data1) + len(table_data2) == len(table_data_union)

    def test_flattenjson(self):
        """Test flattenjson."""
        wc_client = TestPostgresOperations.wc_client
        output_name = "flatten_json"

        config = {
            "input": {
                "input_type": "source",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": "sample",
            },
            "dest_schema": "pytest_intermediate",
            "source_schema": "pytest_intermediate",
            "output_name": output_name,
            "source_columns": [
                "_airbyte_ab_id",
                "_airbyte_data",
                "_airbyte_emitted_at",
            ],
            "json_column": "_airbyte_data",
            "json_columns_to_copy": [
                "NGO",
                "Month",
                "measure1",
                "measure2",
                "Indicator",
            ],
        }

        flattenjson(config, wc_client, TestPostgresOperations.test_project_dir)

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "_airbyte_data_NGO" in cols
        assert "_airbyte_data_Month" in cols
        assert "_airbyte_ab_id" in cols
        assert "_airbyte_data" not in cols

    def test_merge_operation(self):
        """test merge_operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "merge"
        config = {
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "operations": [
                {
                    "type": "castdatatypes",
                    "config": {
                        "columns": [
                            {
                                "columnname": "measure1",
                                "columntype": "int",
                            },
                            {
                                "columnname": "measure2",
                                "columntype": "int",
                            },
                        ],
                        "source_columns": [
                            "NGO",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "SPOC",
                        ],
                    },
                },
                {
                    "type": "arithmetic",
                    "config": {
                        "operator": "add",
                        "operands": [
                            {"value": "measure1", "is_col": True},
                            {"value": "measure2", "is_col": True},
                        ],
                        "output_column_name": "add_col",
                        "source_columns": [
                            "NGO",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "SPOC",
                        ],
                    },
                },
                {
                    "type": "renamecolumns",
                    "config": {
                        "source_columns": [
                            "NGO",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "SPOC",
                            "add_col",
                        ],
                        "columns": {"NGO": "ngo"},
                    },
                },
                {
                    "type": "dropcolumns",
                    "config": {
                        "columns": ["SPOC"],
                        "source_columns": [
                            "ngo",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "SPOC",
                            "add_col",
                        ],
                    },
                },
                {
                    "type": "coalescecolumns",
                    "config": {
                        "columns": [
                            "ngo",
                            "Indicator",
                        ],
                        "source_columns": [
                            "ngo",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "add_col",
                        ],
                        "output_column_name": "coalesce",
                    },
                },
                {
                    "type": "concat",
                    "config": {
                        "columns": [
                            {
                                "name": "ngo",
                                "is_col": True,
                            },
                            {
                                "name": "Indicator",
                                "is_col": True,
                            },
                            {
                                "name": "test",
                                "is_col": False,
                            },
                        ],
                        "source_columns": [
                            "ngo",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "add_col",
                            "coalesce",
                        ],
                        "output_column_name": "concat_col",
                    },
                },
                {
                    "type": "regexextraction",
                    "config": {
                        "columns": {"ngo": "^[C].*"},
                        "source_columns": [
                            "ngo",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "add_col",
                            "coalesce",
                            "concat_col",
                        ],
                        "output_column_name": "regex",
                    },
                },
                {
                    "type": "replace",
                    "config": {
                        "source_columns": [
                            "ngo",
                            "Month",
                            "measure1",
                            "measure2",
                            "Indicator",
                            "add_col",
                            "coalesce",
                            "concat_col",
                        ],
                        "columns": [
                            {
                                "col_name": "ngo",
                                "output_column_name": "ngo_replaced",
                                "replace_ops": [{"find": "CRC", "replace": "NGO_REPLACED_NAME"}],
                            }
                        ],
                    },
                },
                {
                    "type": "rawsql",
                    "config": {
                        "sql_statement_1": "*",
                        "sql_statement_2": "WHERE CAST(measure1 AS INTEGER) != 0",
                    },
                },
            ],
        }

        merge_operations(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)
        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]
        assert "ngo" in cols
        assert "measure1" in cols
        assert "measure2" in cols
        assert "Indicator" in cols
        assert "Month" in cols
        assert "add_col" in cols
        assert "coalesce" in cols
        assert "concat_col" in cols
        assert "ngo_replaced" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 10)
        table_data.sort(key=lambda x: x["Month"])
        assert type(table_data[0]["measure1"]) == int
        assert type(table_data[0]["measure2"]) == int
        assert "NGO_REPLACED_NAME" in [row["ngo_replaced"] for row in table_data]

        assert table_data[0]["add_col"] == table_data[0]["measure1"] + table_data[0]["measure2"]

        initial_raw_data = wc_client.get_table_data(
            "pytest_intermediate", "_airbyte_raw_Sheet1", 10
        )

        assert (
            len(
                set([row["concat_col"] for row in table_data])
                - set([row["NGO"] + row["Indicator"] + "test" for row in initial_raw_data])
            )
            == 0
        )

        assert all(row["measure1"] != 0 for row in table_data)

    def test_generic(self):
        """test generic operation"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "generic"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet2",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_model_name": output_name,
            "source_columns": ["NGO", "Month", "measure1", "measure2", "Indicator"],
            "computed_columns": [
                {
                    "function_name": "LOWER",
                    "operands": [{"value": "NGO", "is_col": True}],
                    "output_column_name": "ngo_lower",
                },
                {
                    "function_name": "TRIM",
                    "operands": [{"value": "measure1", "is_col": True}],
                    "output_column_name": "trimmed_measure_1",
                },
            ],
        }

        generic_function(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = [
            col_dict["name"]
            for col_dict in wc_client.get_table_columns("pytest_intermediate", output_name)
        ]

        assert "NGO" in cols
        assert "Indicator" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        ngo_column = [row["ngo_lower"] for row in table_data]

        for value in ngo_column:
            assert value == value.lower(), f"Value {value} in 'NGO' column is not lowercase"

    def test_generic_sql_function(self):
        """test generic raw sql"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "rawsql"

        config = {
            "input": {
                "input_type": "model",
                "input_name": "_airbyte_raw_Sheet1",
                "source_name": None,
            },
            "dest_schema": "pytest_intermediate",
            "output_model_name": output_name,
            "sql_statement_1": "measure1, measure2",
            "sql_statement_2": "WHERE measure1 = '183'",
        }

        generic_sql_function(config, wc_client, TestPostgresOperations.test_project_dir)

        TestPostgresOperations.execute_dbt("run", output_name)

        col_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert "183" in col_data[0]["measure1"]
