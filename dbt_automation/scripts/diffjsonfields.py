"""compares a json field from two tables in two databases"""

import argparse
import json

from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--ref-schema", required=True)
parser.add_argument("--comp-schema", required=True)
# parser.add_argument("--working-dir", required=True)
parser.add_argument("--json-field", required=True)
parser.add_argument("--config-yaml", default="connections.yaml")
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_schema = args.ref_schema
comp_schema = args.comp_schema

ref_tables = ref_client.get_tables(ref_schema)
comp_tables = comp_client.get_tables(comp_schema)

json_field = args.json_field

for table in ref_tables:
    if table in comp_tables:
        ref_data = ref_client.execute(
            f"""
        SELECT {json_field}
        FROM {ref_schema}.{table}"""
        )
        for row in ref_data:
            ref_json = row[0]
            comp_data = comp_client.execute(
                f"""
            SELECT {json_field}
            FROM {comp_schema}.{table}
            WHERE {json_field}->>'_id' = '{ref_json["_id"]}'
            """
            )
            comp_json = comp_data[0][0]

            for key in ref_json:
                if key in comp_json:
                    if ref_json[key] != comp_json[key]:
                        print(f"{table} {key} differs")
                else:
                    print(f"{table} {key} not in comp")
                    print(comp_json.keys())
            for key in comp_json:
                if key not in ref_json:
                    print(f"{table} {key} not in ref")
    else:
        print(f"{table} not in comp")
