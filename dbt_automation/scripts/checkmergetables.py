"""takes a list of tables and a common column spec and creates a dbt model to merge them"""

import os
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import yaml
from tqdm import tqdm

from dotenv import load_dotenv
from dbt_automation.utils.warehouseclient import get_client

load_dotenv("dbconnection.env")

basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--mergespec", required=True)
args = parser.parse_args()

# ================================================================================
with open(args.mergespec, "r", encoding="utf-8") as mergespecfile:
    mergespec = yaml.safe_load(mergespecfile)

conn_info = {
    "host": os.getenv("DBHOST"),
    "port": os.getenv("DBPORT"),
    "username": os.getenv("DBUSER"),
    "password": os.getenv("DBPASSWORD"),
    "database": os.getenv("DBNAME"),
}

client = get_client("postgres", conn_info)
# client = get_client("bigquery", None)  # set json account creds in the env

for table in mergespec["tables"]:
    logger.info("table=%s.%s", table["schema"], table["tablename"])
    columns = client.get_columnspec(table["schema"], table["tablename"])
    quoted_column_names = [f'"{c}"' for c in columns]
    columnlist = ", ".join(quoted_column_names)
    statement = f"SELECT {columnlist} FROM {table['schema']}.{table['tablename']}"
    # logger.info(statement)
    resultset = client.execute(statement)
    for row in tqdm(resultset, desc="Checking table ..."):
        check_statement = "SELECT COUNT(1) FROM "
        check_statement += (
            f"{mergespec['outputsschema']}.merged_{mergespec['mergename']} "
        )
        check_statement += "WHERE "
        filters = []
        for colname, colvalue in zip(columns, row):
            if colvalue is None:
                filters.append(f'"{colname}" IS NULL')
            elif colvalue.find(chr(39)) < 0:
                filters.append(f"\"{colname}\" = '{colvalue}'")

        check_statement += " AND ".join(filters)
        # logger.info(check_statement)
        check_results = client.execute(statement)
        if check_results[0][0] != 1:
            logger.error(
                "check failed, got %d results instead of %d", check_results[0][0], 1
            )
            logger.error(check_statement)
            sys.exit(1)
