"""syncs one schema in one database to the corresponding schema in the other database"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import json
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--id-column", required=True)
parser.add_argument("--delete", action="store_true")
parser.add_argument("--show-diffs", action="store_true")
args = parser.parse_args()

with open("connections.yaml", "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ID_COLUMN = args.id_column

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_tables = ref_client.get_tables("staging")
comp_tables = comp_client.get_tables("staging")

if set(ref_tables) != set(comp_tables):
    print("not the same table sets")
    print(f"ref - comp: {set(ref_tables) - set(comp_tables)}")
    print(f"comp - ref: {set(comp_tables) - set(ref_tables)}")
    sys.exit(1)

for tablename in ref_tables:
    print(tablename)
    if not tablename.startswith("_airbyte_raw_"):
        print("skipping")
        continue

    statement = f"SELECT DISTINCT _airbyte_data->'{ID_COLUMN}' FROM staging.{tablename}"

    resultset_ref = [x[0] for x in ref_client.execute(statement)]
    resultset_comp = [x[0] for x in comp_client.execute(statement)]

    if len(resultset_ref) != len(resultset_comp):
        print(
            f"{tablename} row counts differ: ref={len(resultset_ref)} comp={len(resultset_comp)}"
        )
        resultset_comp = set(resultset_comp)
        resultset_ref = set(resultset_ref)

        if len(resultset_ref - resultset_comp) > 0:
            for extra_id in resultset_ref - resultset_comp:
                if extra_id:
                    extra_id_statement = f"SELECT _airbyte_data FROM staging.{tablename} WHERE _airbyte_data->>'{ID_COLUMN}' = '{extra_id}'"
                    extra_id_result = ref_client.execute(extra_id_statement)[0]
                    airbyte_data = extra_id_result[0]
                    if args.show_diffs:
                        print(json.dumps(airbyte_data, indent=2))
                        print("=" * 80)
                else:
                    print(f"found _airbyte_data->>{ID_COLUMN} = None in ref")
            sys.exit(1)

        for extra_id in resultset_comp - resultset_ref:
            if extra_id:
                print(f"deleting {extra_id[0]}")
                del_statement = f"DELETE FROM staging.{tablename} WHERE _airbyte_data->>'{ID_COLUMN}' = '{extra_id[0]}'"
                if args.delete:
                    comp_client.execute(del_statement)
            else:
                print(f"found _airbyte_data->>{ID_COLUMN} = None in comp")
