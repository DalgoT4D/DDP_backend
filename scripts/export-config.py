import os
import argparse

import json
from testclient import TestClient
from ddpui.utils.helpers import remove_nested_attribute

parser = argparse.ArgumentParser()
parser.add_argument("--port", help="port where local app server is listening")
parser.add_argument("--verbose", action="store_true")
parser.add_argument("--email", required=True, help="login email")
parser.add_argument("--password", required=True, help="password")
args = parser.parse_args()

if args.port:
    ngoClient = TestClient(args.port)
else:
    ngoClient = TestClient("443", host="ddpapi.projecttech4dev.org")

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

config = {}

warehouse_response = ngoClient.clientget("organizations/warehouses")
if args.verbose:
    print(remove_nested_attribute(warehouse_response, "icon"))
warehouse = warehouse_response["warehouses"][0]
config["warehouse"] = {
    "wtype": warehouse["wtype"],
    "airbyteConfig": warehouse["airbyte_destination"]["connectionConfiguration"],
}

config["sources"] = []
sources = ngoClient.clientget("airbyte/sources")
for source in sources:
    if args.verbose:
        print(remove_nested_attribute(source, "icon"))
    source_config = {
        "stype": source["sourceName"],
        "name": source["name"],
        "config": source["connectionConfiguration"],
    }
    config["sources"].append(source_config)

# connections = ngoClient.clientget("airbyte/connections")
# print(connections)

config["dbt"] = {}
dbtworkspace_response = ngoClient.clientget("dbt/dbt_workspace")
# print(dbtworkspace_response)
config["dbt"]["gitrepo_url"] = dbtworkspace_response["gitrepo_url"]
target_schema_prefix, profile = tuple(
    dbtworkspace_response["default_schema"].split("-")
)
config["dbt"]["profile"] = profile
config["dbt"]["target_schema_prefix"] = target_schema_prefix

# dbtblocks_response = ngoClient.clientget("prefect/blocks/dbt/")
# print(dbtblocks_response)

# flows_response = ngoClient.clientget("prefect/flows/")
# print(flows_response)

print(json.dumps(config, indent=2))
