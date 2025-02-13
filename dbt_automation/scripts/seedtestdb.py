"""seeds a test database with test data"""

import os
import json
import argparse
import csv
from pathlib import Path
import yaml
from dotenv import load_dotenv
from dbt_automation.utils.warehouseclient import get_client

parser = argparse.ArgumentParser(description="Seed a test database with test data")
parser.add_argument(
    "-y",
    "--yamlconfig",
    required=True,
    help="Path to the yaml config file that defines your test data",
)
args = parser.parse_args()

load_dotenv("dbconnection.env")

# Load the YAML file
config_data = None
with open(args.yamlconfig, "r", encoding="utf-8") as yaml_file:
    config_data = yaml.safe_load(yaml_file)

if config_data is None:
    raise ValueError("Couldn't read the yaml config data")

# TODO: Add stronger validations for each operation here
if config_data["warehouse"] not in ["postgres", "bigquery"]:
    raise ValueError("unknown warehouse")

if config_data["warehouse"] == "postgres":
    conn_info = {
        "host": os.getenv("DBHOST"),
        "port": os.getenv("DBPORT"),
        "username": os.getenv("DBUSER"),
        "password": os.getenv("DBPASSWORD"),
        "database": os.getenv("DBNAME"),
    }
else:
    with open(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "r", encoding="utf-8"
    ) as gcp_creds:
        conn_info = json.loads(gcp_creds.read())
warehouse = get_client(config_data["warehouse"], conn_info)

seed_path = Path(args.yamlconfig).parent
seed_data = config_data["seed_data"]
for seed in seed_data:
    warehouse.ensure_schema(seed["schema"])
    for table in seed["tables"]:
        reader = csv.DictReader(open(seed_path / table["csv"], "r", encoding="utf-8"))
        # bigquery has a race condition when dropping / recreating tables
        # comment on the next two lines in the inserts fail
        warehouse.drop_table(seed["schema"], table["name"])
        warehouse.ensure_table(seed["schema"], table["name"], reader.fieldnames)
        for row in reader:
            print(row)
            warehouse.insert_row(seed["schema"], table["name"], row)
