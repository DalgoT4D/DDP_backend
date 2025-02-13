"""This script seeds airbyte's raw data into test warehouse"""

import argparse
import os
import json
from logging import basicConfig, getLogger, INFO
from google.cloud import bigquery
from dotenv import load_dotenv
from dbt_automation.utils.warehouseclient import get_client


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
args = parser.parse_args()
warehouse = args.warehouse

load_dotenv("dbconnection.env")

for json_file, tablename in zip(
    ["seeds/sample_sheet1.json", "seeds/sample_sheet2.json"],
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

        wc_client = get_client(args.warehouse, conn_info)

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

        wc_client.runcmd(create_schema_query)
        wc_client.runcmd(create_table_query)
        wc_client.runcmd(truncate_table_query)

        """
        INSERT INTO your_table_name (column1, column2, column3, ...)
        VALUES ({}, {}, {}, ...);
        """
        # seed sample json data into the newly table created
        logger.info("seeding sample json data")
        for row in data:
            # Execute the insert query with the data from the CSV
            insert_query = f"""INSERT INTO {schema}."{tablename}" ({', '.join(columns)}) VALUES ('{row['_airbyte_ab_id']}', JSON '{row['_airbyte_data']}', '{row['_airbyte_emitted_at']}')"""
            wc_client.runcmd(insert_query)

    if args.warehouse == "bigquery":
        logger.info("Found bigquery warehouse")
        conn_info = json.loads(os.getenv("TEST_BG_SERVICEJSON"))
        location = os.getenv("TEST_BG_LOCATION")
        test_dataset = os.getenv("TEST_BG_DATASET_SRC")

        wc_client = get_client(args.warehouse, conn_info)

        # create the dataset if it does not exist
        dataset = bigquery.Dataset(f"{conn_info['project_id']}.{test_dataset}")
        dataset.location = location

        logger.info("creating the dataset")
        dataset = wc_client.bqclient.create_dataset(dataset, timeout=30, exists_ok=True)
        logger.info("created dataset: %s", dataset.dataset_id)

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
        wc_client.bqclient.create_table(table, exists_ok=True)

        # truncate data to (re-)seed fresh data
        truncate_table_query = f"""
            DELETE FROM `{conn_info['project_id']}.{dataset.dataset_id}.{tablename}`
            WHERE true;
        """

        wc_client.execute(truncate_table_query)

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

        wc_client.execute(insert_query)


wc_client.close()
logger.info("seeding finished")
