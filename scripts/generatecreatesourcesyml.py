"""generates a sources.yml file for the createsources script"""
#!env python

import argparse
from csv import DictReader
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--aws-access-key-id", required=True)
parser.add_argument("--aws-secret-access-key", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--csvs", required=True)
parser.add_argument("--destination-schema", default="staging")
args = parser.parse_args()

yaml_object = {
    "sources": [],
    "connections": [],
}

with open(args.csvs, "r", encoding="utf-8") as csvs:
    reader = DictReader(csvs)
    for line in reader:
        print(line)
        source = {
            "name": line["source"],
            "stype": "File (CSV, JSON, Excel, Feather, Parquet)",
            "config": {
                "url": line["s3location"],
                "format": "csv",
                "provider": {
                    "storage": "S3",
                    "aws_access_key_id": args.aws_access_key_id,
                    "aws_secret_access_key": args.aws_secret_access_key,
                },
                "dataset_name": line["dataset"],
                "reader_options": "{}",
            },
        }
        connection = {
            "name": line["connection"],
            "source": line["source"],
            "streams": [{"name": line["dataset"], "syncMode": "overwrite"}],
            "destinationSchema": args.destination_schema,
        }
        yaml_object["sources"].append(source)
        yaml_object["connections"].append(connection)

with open(args.output, "w", encoding="utf-8") as output:
    yaml.safe_dump(yaml_object, output)
