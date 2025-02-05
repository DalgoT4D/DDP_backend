"""diffs two schemas in two databases"""
import os
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import subprocess
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--ref-schema", required=True)
parser.add_argument("--comp-schema", required=True)
parser.add_argument("--working-dir", required=True)
parser.add_argument("--config-yaml", default="connections.yaml")
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

working_dir = args.working_dir


def db2csv(
    connection,
    schema: str,
    table: str,
    columns: list,
    exclude_columns: list,
    csvfile: str,
):
    """dumps the table into a csv file"""
    columns_list = [f'"{x}"' for x in columns if x not in exclude_columns]
    if not os.path.exists(csvfile):
        cmdfile = f"{working_dir}/{table}.cmd"
        print(f"writing {cmdfile}")
        with open(cmdfile, "w", encoding="utf-8") as cmd:
            cmd.write("\\COPY (SELECT ")
            cmd.write(",".join(columns_list))
            cmd.write(f" FROM {schema}.{table}) TO '{csvfile}' WITH CSV HEADER")
            cmd.close()
        subprocess_cmd = [
            "psql",
            "-h",
            connection["host"],
            "-p",
            str(connection.get("port", "5432")),
            "-d",
            connection["database"],
            "-U",
            connection["username"],
            "-f",
            cmdfile,
        ]
        os.environ["PGPASSWORD"] = connection["password"]
        subprocess.check_call(subprocess_cmd)
        os.unlink(cmdfile)

    sorted_csv = csvfile.replace(".csv", ".sorted.csv")
    if not os.path.exists(sorted_csv):
        subprocess_cmd = ["sort", csvfile, "-o", sorted_csv]
        subprocess.check_call(subprocess_cmd)
    return sorted_csv


ref_schema = args.ref_schema
comp_schema = args.comp_schema

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_tables = ref_client.get_tables(ref_schema)
comp_tables = comp_client.get_tables(comp_schema)

if set(ref_tables) != set(comp_tables):
    print("WARNING: not the same table sets")
    if len(ref_tables) > len(comp_tables):
        print("ref has more tables")
        print(set(ref_tables) - set(comp_tables))
    else:
        print("comp has more tables")
        print(set(comp_tables) - set(ref_tables))

    ref_tables = set(ref_tables) & set(comp_tables)

columns_specs = {}
for tablename in ref_tables:
    ref_columns = ref_client.get_columnspec(ref_schema, tablename)
    comp_columns = comp_client.get_columnspec(comp_schema, tablename)
    if set(ref_columns) != set(comp_columns):
        print(f"columns for {tablename} are not the same")
        if len(ref_columns) > len(comp_columns):
            print("ref has more columns")
            print(set(ref_columns) - set(comp_columns))
        else:
            print("comp has more columns")
            print(set(comp_columns) - set(ref_columns))
        sys.exit(1)
    columns_specs[tablename] = ref_columns

os.makedirs(working_dir, exist_ok=True)
os.makedirs(f"{working_dir}/{ref_schema}", exist_ok=True)
os.makedirs(f"{working_dir}/{comp_schema}", exist_ok=True)

for tablename in ref_tables:
    print(f"TABLE: {tablename}")

    ref_csvfile = f"{working_dir}/{ref_schema}/{tablename}.ref.csv"
    sorted_ref_csv = db2csv(
        connection_info["ref"],
        ref_schema,
        tablename,
        columns_specs[tablename],
        ["_airbyte_ab_id", "_airbyte_emitted_at"],
        ref_csvfile,
    )

    # ok, now do the same for the comp
    comp_csvfile = f"{working_dir}/{comp_schema}/{tablename}.comp.csv"
    sorted_comp_csv = db2csv(
        connection_info["comp"],
        comp_schema,
        tablename,
        columns_specs[tablename],
        ["_airbyte_ab_id", "_airbyte_emitted_at"],
        comp_csvfile,
    )

    has_mismatches = False
    # and finally compare the two sorted csv files
    with open(sorted_comp_csv, "r", encoding="utf-8") as sorted_comp, open(
        sorted_ref_csv,
        "r",
        encoding="utf-8",
    ) as sorted_ref:
        for idx, (line1, line2) in enumerate(zip(sorted_comp, sorted_ref)):
            if line1 != line2:
                print(f"mismatch for {tablename} at {idx}")
                print(line1.strip())
                print(line2.strip())
                has_mismatches = True

    if not has_mismatches:
        # delete the sorted csv files
        os.remove(sorted_ref_csv)
        os.remove(sorted_comp_csv)
