from testclient import TestClient
import argparse
import os
from dotenv import load_dotenv
import json
from time import sleep

load_dotenv(".env.resetngo")

# ngo config in json

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
    spec = json.load(json_file)

ngoClient = TestClient(args.port)

# signup
ngoClient.clientpost(
    "organizations/users/",
    json={
        "email": args.email,
        "password": args.password,
        "signupcode": os.getenv("SIGNUPCODE"),
    },
)

# login
ngoClient.login(args.email, args.password)

# create org
ngoClient.clientpost(
    "organizations/",
    json={
        "name": args.org_name,
    },
)

# create warehouse
destination_definitions = ngoClient.clientget("airbyte/destination_definitions")
if args.verbose:
    print(destination_definitions)

for destdef in destination_definitions:
    if destdef["name"] == spec["warehouse"]["wtype"]:
        destinationDefinitionId = destdef["destinationDefinitionId"]
        spec["warehouse"]["destinationDefId"] = destinationDefinitionId
        spec["warehouse"]["wtype"] = spec["warehouse"]["wtype"].lower()

destination = ngoClient.clientpost("organizations/warehouse/", json=spec["warehouse"])
spec["warehouse"]["destinationId"] = destination["destinationId"]

# create sources
source_definitions = ngoClient.clientget("airbyte/source_definitions")
for src in spec["sources"]:
    for sourceDef in source_definitions:
        sourceDefId = sourceDef["sourceDefinitionId"]
        if sourceDef["name"] == src["stype"]:
            # add source
            ngoClient.clientpost(
                "airbyte/sources/",
                json={
                    "name": src["name"],
                    "sourceDefId": sourceDefId,
                    "config": src["config"],
                },
            )
            break

# create connections between all sources to destinations
sources = ngoClient.clientget("airbyte/sources")
for idx, src in enumerate(sources):
    connPayload = {
        "name": f"test-conn-{idx}",
        "normalize": False,
        "sourceId": src["sourceId"],
        "streams": [],
    }

    schemaCatalog = ngoClient.clientget(
        f'airbyte/sources/{src["sourceId"]}/schema_catalog'
    )
    for streamData in schemaCatalog["catalog"]["streams"]:
        streamPayload = {}
        streamPayload["selected"] = True
        streamPayload["name"] = streamData["stream"]["name"]
        streamPayload["supportsIncremental"] = False
        streamPayload["destinationSyncMode"] = "append"
        streamPayload["syncMode"] = "full_refresh"
        connPayload["streams"].append(streamPayload)

    ngoClient.clientpost("airbyte/connections/", json=connPayload)

# # create dbt workspace
r = ngoClient.clientpost(
    "dbt/workspace/",
    json={
        "gitrepoUrl": os.getenv("DBT_TEST_REPO"),
        # "gitrepoAccessToken": DBT_TEST_REPO_ACCESSTOKEN,
        "dbtVersion": "1.4.5",
        "profile": {
            "name": os.getenv("DBT_PROFILE"),
            "target": "dev",
            "target_configs_schema": os.getenv("DBT_TARGETCONFIGS_SCHEMA"),
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

r = ngoClient.clientpost(
    "prefect/blocks/dbt/",
    json={
        "profile": {
            "name": os.getenv("DBT_PROFILE"),
            "target_configs_schema": os.getenv("DBT_TARGETCONFIGS_SCHEMA"),
        },
    },
    timeout=60,
)
print(r)


ngoClient.clientpost(
    "flows/",
    json={
        "name": "",
        "connectionBlocks": [],
        "dbtTransform": "yes",
        "cron": "",
    },
)
