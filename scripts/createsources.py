"""creates sources and connections in an airbyte workspace"""
#!env python

import os
import sys
import argparse
import yaml
from dotenv import load_dotenv
from testclient import TestClient
from ddpui.utils.helpers import remove_nested_attribute


parser = argparse.ArgumentParser()
parser.add_argument("--verbose", action="store_true")
parser.add_argument("--env", required=True)
parser.add_argument(
    "--file", required=True, help="path of the file containing ngo config details"
)
args = parser.parse_args()

load_dotenv(args.env)
for required_var in ["APP_HOST", "APP_PORT", "EMAIL", "PASSWORD", "ORG"]:
    if not os.getenv(required_var):
        print(f"missing required env variable {required_var}")
        sys.exit(1)

ngoClient = TestClient(os.getenv("APP_PORT"), host=os.getenv("APP_HOST"))

with open(args.file, "r", encoding="utf-8") as yaml_file:
    spec = yaml.safe_load(yaml_file)

ngoClient.login(os.getenv("EMAIL"), os.getenv("PASSWORD"))
ngoClient.clientheaders["x-dalgo-org"] = os.getenv("ORG")

source_definitions = ngoClient.clientget("airbyte/source_definitions")

sources = ngoClient.clientget("airbyte/sources")
for source in sources:
    print(remove_nested_attribute(source, "icon"))
# create sources
for src in spec["sources"]:
    if src["name"] in [s["name"] for s in sources]:
        print(f"source {src['name']} already exists")
        continue
    for sourceDef in source_definitions:
        if sourceDef["name"] == src["stype"]:
            sourceDefId = sourceDef["sourceDefinitionId"]
            # add source
            try:
                source_creation_response = ngoClient.clientpost(
                    "airbyte/sources/",
                    json={
                        "name": src["name"],
                        "sourceDefId": sourceDefId,
                        "config": src["config"],
                    },
                )
                print(source_creation_response)
                sources = ngoClient.clientget("airbyte/sources")
                break
            except Exception as error:
                print(error)
                sys.exit(1)

# create connections between all sources to destinations
connections_response = ngoClient.clientget("airbyte/connections")
if args.verbose:
    for connection in connections_response:
        print(remove_nested_attribute(connection, "icon"))

for connection in spec["connections"]:
    if connection["name"] in [c["name"] for c in connections_response]:
        print(f"connection {connection['name']} already exists")
        continue

    src = [s for s in sources if s["name"] == connection["source"]][0]
    connPayload = {
        "name": connection["name"],
        "normalize": False,
        "sourceId": src["sourceId"],
        "streams": [],
        "destinationSchema": connection["destinationSchema"],
    }

    schemaCatalog = ngoClient.clientget(
        f'airbyte/sources/{src["sourceId"]}/schema_catalog'
    )
    if args.verbose:
        print("source's schema catalog:")
        print(remove_nested_attribute(schemaCatalog, ""))

    for streamData in schemaCatalog["catalog"]["streams"]:
        for specStream in connection["streams"]:
            if streamData["stream"]["name"] == specStream["name"]:
                streamPayload = {
                    "selected": True,
                    "name": specStream["name"],
                    "supportsIncremental": False,
                    "destinationSyncMode": specStream["syncMode"],
                    "syncMode": "full_refresh",
                }
                connPayload["streams"].append(streamPayload)

    if args.verbose:
        print("connection creation payload:")
        print(connPayload)
    new_connection_response = ngoClient.clientpost(
        "airbyte/connections/", json=connPayload
    )
    print("new connection:")
    print(remove_nested_attribute(new_connection_response, "icon"))
