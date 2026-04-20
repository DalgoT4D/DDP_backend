"""This script seeds airbyte's raw data into test warehouse"""

import argparse
import os
import json
from pathlib import Path
from logging import basicConfig, getLogger, INFO
from google.cloud import bigquery
from dotenv import load_dotenv
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType
from ddpui.core.dbt_automation import seeds


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
args = parser.parse_args()
warehouse = args.warehouse

load_dotenv("dbconnection.env")

for json_file, tablename in zip(
    [
        os.path.abspath(os.path.join(os.path.abspath(seeds.__file__), "..", "sample_sheet1.json")),
        os.path.abspath(os.path.join(os.path.abspath(seeds.__file__), "..", "sample_sheet2.json")),
    ],
    ["_airbyte_raw_Sheet1", "_airbyte_raw_Sheet2"],
):
    logger.info("seeding %s into %s", json_file, tablename)

    data = []
    with open(json_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    columns = ["_airbyte_ab_id", "_airbyte_data", "_airbyte_emitted_at"]

    # schema check; expecting only airbyte raw data
    for row in data:
        schema_check = [True if key in columns else False for key in row.keys()]
        if all(schema_check) is False:
            raise Exception("Schema mismatch")

    if args.warehouse == "postgres":
        logger.info("Found postgres warehouse")
        conn_info = {
            "host": os.getenv("TEST_PG_DBHOST"),
            "port": os.getenv("TEST_PG_DBPORT"),
            "username": os.getenv("TEST_PG_DBUSER"),
            "user": os.getenv("TEST_PG_DBUSER"),
            "database": os.getenv("TEST_PG_DBNAME"),
            "password": os.getenv("TEST_PG_DBPASSWORD"),
        }
        schema = os.getenv("TEST_PG_DBSCHEMA_SRC")

        wc_client = WarehouseFactory.connect(conn_info, WarehouseType.POSTGRES)

        create_schema_query = f"""
            CREATE SCHEMA IF NOT EXISTS {schema};
        """
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}."{tablename}"
            (
                _airbyte_ab_id character varying,
                _airbyte_data jsonb,
                _airbyte_emitted_at timestamp with time zone
            );
        """
        truncate_table_query = f"""
            TRUNCATE TABLE {schema}."{tablename}";
        """

        logger.info("Starting transaction for schema/table setup and truncate")
        wc_client.execute_transaction(
            [
                (create_schema_query, None),
                (create_table_query, None),
                (truncate_table_query, None),
            ]
        )
        logger.info("Completed transaction for schema/table setup and truncate")

        """
        INSERT INTO your_table_name (column1, column2, column3, ...)
        VALUES ({}, {}, {}, ...);
        """
        # seed sample json data into the newly table created
        logger.info("seeding sample json data")
        insert_query = f"""
            INSERT INTO {schema}."{tablename}" ({', '.join(columns)})
            VALUES (:airbyte_ab_id, CAST(:airbyte_data AS jsonb), :airbyte_emitted_at)
        """

        insert_params = []
        for row in data:
            raw_airbyte_data = row["_airbyte_data"]
            insert_params.append(
                {
                    "airbyte_ab_id": row["_airbyte_ab_id"],
                    "airbyte_data": (
                        raw_airbyte_data
                        if isinstance(raw_airbyte_data, str)
                        else json.dumps(raw_airbyte_data)
                    ),
                    "airbyte_emitted_at": row["_airbyte_emitted_at"],
                }
            )

        logger.info("Starting bulk insert transaction for %s rows", len(insert_params))
        inserted_rows = wc_client.execute_many(insert_query, insert_params)
        logger.info("Completed bulk insert transaction. rows=%s", inserted_rows)

    if args.warehouse == "bigquery":
        logger.info("Found bigquery warehouse")
        conn_info = json.loads(os.getenv("TEST_BG_SERVICEJSON"))
        location = os.getenv("TEST_BG_LOCATION")
        test_dataset = os.getenv("TEST_BG_DATASET_SRC")

        wc_client = WarehouseFactory.connect(conn_info, WarehouseType.BIGQUERY, location)
        bqclient = bigquery.Client.from_service_account_info(conn_info)

        # create the dataset if it does not exist
        dataset = bigquery.Dataset(f"{conn_info['project_id']}.{test_dataset}")
        dataset.location = location

        logger.info("Executing query: create dataset if not exists")
        dataset = bqclient.create_dataset(dataset, timeout=30, exists_ok=True)
        logger.info("Finished query: create dataset if not exists -> %s", dataset.dataset_id)

        # create the staging table if its does not exist
        table_schema = [
            bigquery.SchemaField("_airbyte_ab_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("_airbyte_data", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("_airbyte_emitted_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(
            f"{conn_info['project_id']}.{dataset.dataset_id}.{tablename}",
            schema=table_schema,
        )
        logger.info("Executing query: create table if not exists")
        bqclient.create_table(table, exists_ok=True)
        logger.info("Finished query: create table if not exists")

        # truncate data to (re-)seed fresh data
        truncate_table_query = f"""
            DELETE FROM `{conn_info['project_id']}.{dataset.dataset_id}.{tablename}`
            WHERE true;
        """

        logger.info("Executing query: truncate existing rows")
        wc_client.execute(truncate_table_query)
        logger.info("Finished query: truncate existing rows")

        # seed data
        insert_query = f"""
        INSERT INTO `{conn_info['project_id']}.{dataset.dataset_id}.{tablename}` (_airbyte_ab_id, _airbyte_data, _airbyte_emitted_at)
        VALUES 
        """
        insert_query_values = ",".join(
            [
                f"""('{row["_airbyte_ab_id"]}', '{row["_airbyte_data"]}', '{row["_airbyte_emitted_at"]}')"""
                for row in data
            ]
        )
        insert_query += insert_query_values

        logger.info("Executing query: bulk insert rows")
        wc_client.execute(insert_query)
        logger.info("Finished query: bulk insert rows")


wc_client.close()
logger.info("seeding finished")
