import sys
from testclient import TestClient
import argparse
from uuid import uuid4
import os
from dotenv import load_dotenv
import json
from time import sleep
from ddpui.utils.helpers import remove_nested_attribute
from django.utils.text import slugify

load_dotenv(".env")


# python scripts/resetNGO.py --port 8002 --org-name NGO3 --email user3@ngo3 --password password --file scripts/ngo1.json

# spec = {
#     "warehouse": {
#         "wtype": "", # airbyte destination definition
#         "airbyteConfig": {
#             "host": "",
#             "port": 5432,
#             "username": "",
#             "password": "",
#             "database": "",
#             "schema": "",
#         }
#     },
#     "sources": [
#         {
#             "stype": "SurveyCTO", # from airbyte source definition
#             "name": "testing-surveyCto-source",
#             "config": {
#                 "form_id" : [""],
#                 "password": "",
#                 "server_name": "",
#                 "start_date": "",
#                 "username": ""
#             }
#         }
#     ]
# }

parser = argparse.ArgumentParser()
parser.add_argument("--port", required=True, help="port where app server is listening")
parser.add_argument("--verbose", action="store_true")
parser.add_argument("--org-name", required=True, help="name of NGO")
parser.add_argument("--email", required=True, help="login email")
parser.add_argument("--password", required=True, help="password")
parser.add_argument(
    "--file", required=True, help="path of the file containing ngo config details"
)
args = parser.parse_args()

with open(args.file, "r", encoding="utf-8") as json_file:
    spec = json.loads(json_file.read())

ngoClient = TestClient(args.port)

try:
    ngoClient.login(args.email, args.password)
except Exception:
    # signup
    ngoClient.clientpost(
        "organizations/users/",
        json={
            "email": args.email,
            "password": args.password,
            "signupcode": os.getenv("SIGNUPCODE"),
        },
    )
    ngoClient.login(args.email, args.password)

currentuser = ngoClient.clientget("currentuser")
if currentuser.get("org") is None:
    # create org
    try:
        createorg_response = ngoClient.clientpost(
            "organizations/",
            json={
                "name": args.org_name,
            },
        )
        if (
            createorg_response.get("detail")
            == "client org with this name already exists"
        ):
            print("please use another --org-name")
            sys.exit(1)
        print(createorg_response)
        currentuser = ngoClient.clientget("currentuser")

    except Exception as error:
        sys.exit(1)

print(currentuser)

# create warehouse
warehouse_response = ngoClient.clientget("organizations/warehouses")
if len(warehouse_response["warehouses"]) == 0:
    destination_definitions = ngoClient.clientget("airbyte/destination_definitions")
    if args.verbose:
        print(destination_definitions)

    create_warehouse_payload = spec["warehouse"]
    for destdef in destination_definitions:
        if destdef["name"].lower() == spec["warehouse"]["wtype"].lower():
            destinationDefinitionId = destdef["destinationDefinitionId"]
            create_warehouse_payload["destinationDefId"] = destinationDefinitionId
            create_warehouse_payload["wtype"] = create_warehouse_payload[
                "wtype"
            ].lower()

    print(create_warehouse_payload)
    destination = ngoClient.clientpost(
        "organizations/warehouse/", json=create_warehouse_payload
    )
    print(destination)
    warehouse_response = ngoClient.clientget("organizations/warehouses")

if args.verbose:
    print(remove_nested_attribute(warehouse_response, "icon"))

sources = ngoClient.clientget("airbyte/sources")
# create sources

if len(sources) == 0:
    source_definitions = ngoClient.clientget("airbyte/source_definitions")
    for src in spec["sources"]:
        for sourceDef in source_definitions:
            sourceDefId = sourceDef["sourceDefinitionId"]
            if sourceDef["name"] == src["stype"]:
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
                    sys.exit(1)

if args.verbose:
    for source in sources:
        print(remove_nested_attribute(source, "icon"))

# create connections between all sources to destinations
connections_response = ngoClient.clientget("airbyte/connections")
if args.verbose:
    print(connections_response)

for idx, src in enumerate(sources):
    if src["sourceId"] in [c["sourceId"] for c in connections_response]:
        print(f"connection to source {src['sourceId']} already exists")
        continue

    connPayload = {
        "name": f"test-conn-{idx}",
        "normalize": False,
        "sourceId": src["sourceId"],
        "streams": [],
    }

    schemaCatalog = ngoClient.clientget(
        f'airbyte/sources/{src["sourceId"]}/schema_catalog'
    )
    if args.verbose:
        print("source's schema catalog:")
        print(remove_nested_attribute(schemaCatalog, ""))
    for streamData in schemaCatalog["catalog"]["streams"]:
        streamPayload = {}
        streamPayload["selected"] = True
        streamPayload["name"] = streamData["stream"]["name"]
        streamPayload["supportsIncremental"] = False
        streamPayload["destinationSyncMode"] = "append"
        streamPayload["syncMode"] = "full_refresh"
        connPayload["streams"].append(streamPayload)

    if args.verbose:
        print("connection creation payload:")
        print(connPayload)
    new_connection_response = ngoClient.clientpost(
        "airbyte/connections/", json=connPayload
    )
    print("new connection:")
    print(new_connection_response)

dbtworkspace_response = ngoClient.clientget("dbt/dbt_workspace")
if "error" in dbtworkspace_response:
    # # create dbt workspace
    r = ngoClient.clientpost(
        "dbt/workspace/",
        json={
            "gitrepoUrl": spec["dbt"]["gitrepo_url"],
            "dbtVersion": "1.4.5",
            "profile": {
                "name": spec["dbt"]["profile"],
                "target": "dev",
                "target_configs_schema": slugify(
                    spec["dbt"]["target_schema_prefix"] + args.org_name
                ),
            },
        },
        timeout=30,
    )
    task_id = r["task_id"]
    while True:
        sleep(2)
        resp = ngoClient.clientget("tasks/" + task_id)
        if "progress" not in resp:
            continue
        progress = resp["progress"]
        laststatus = None
        for step in progress:
            print(step)
            laststatus = step["status"]
        if laststatus in ["failed", "completed"]:
            break
        print("=" * 40)


dbtworkspace_response = ngoClient.clientget("dbt/dbt_workspace")
if "error" in dbtworkspace_response:
    print("unable to create dbt workspace")
    sys.exit(1)

print(dbtworkspace_response)

dbtblocks_response = ngoClient.clientget("prefect/blocks/dbt/")

if len(dbtblocks_response) == 0:
    dbtblockcreation_response = ngoClient.clientpost(
        "prefect/blocks/dbt/",
        json={
            "profile": {
                "name": spec["dbt"]["profile"],
                "target_configs_schema": dbtworkspace_response["default_schema"],
            },
        },
        timeout=60,
    )
    print(dbtblockcreation_response)
    dbtblocks_response = ngoClient.clientget("prefect/blocks/dbt/")

print([x["blockName"] for x in dbtblocks_response])

flows_response = ngoClient.clientget("prefect/flows/")

if len(flows_response) == 0:
    createflows_response = ngoClient.clientpost(
        "prefect/flows/",
        json={
            "name": "flow-" + str(uuid4()),
            "connectionBlocks": [],
            "dbtTransform": "yes",
            "cron": "0 1 * * *",
        },
    )
    if "detail" in createflows_response:
        print(createflows_response["detail"])
        sys.exit(1)
    if args.verbose:
        print(createflows_response)
    flows_response = ngoClient.clientget("prefect/flows")

print(flows_response)
