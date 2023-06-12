from testclient import TestClient
import argparse
import os
from dotenv import load_dotenv
import json

load_dotenv()

# ngo config in json

# ngoToBeRestored = {
#     "email": "",
#     "password": "",
#     "org_name": "",
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
parser.add_argument(
    "-p", "--port", required=True, help="port where app server is listening"
)
parser.add_argument(
    "-f", "--file", required=True, help="path of the file containing ngo config details"
)
args = parser.parse_args()

with open(args.file, 'r') as json_file:
    ngoToBeRestored = json.load(json_file)

ngoClient = TestClient(args.port)

# # signup
# ngoClient.clientpost("organizations/users/", json={
#         "email": ngoToBeRestored["email"], 
#         "password": ngoToBeRestored["password"], 
#         "signupcode": os.getenv("SIGNUPCODE")
#     }
# )

# login
ngoClient.login(ngoToBeRestored["email"], ngoToBeRestored["password"])

# create org
ngoClient.clientpost(
    "organizations/",
    json={
        "name": ngoToBeRestored["org_name"],
    },
)

# # create warehouse
# destination_definitions = ngoClient.clientget("airbyte/destination_definitions")
# print(destination_definitions)
# for destdef in destination_definitions:
#     if destdef["name"] == ngoToBeRestored["warehouse"]["wtype"]:
#         destinationDefinitionId = destdef["destinationDefinitionId"]
#         ngoToBeRestored["warehouse"]["destinationDefId"] = destinationDefinitionId
#         ngoToBeRestored["warehouse"]["wtype"] = ngoToBeRestored["warehouse"]["wtype"].lower()

# ngoClient.clientpost(
#     "organizations/warehouse/",
#     json=ngoToBeRestored["warehouse"]
# )

# # create sources
# source_definitions = ngoClient.clientget("airbyte/source_definitions")
# for src in ngoToBeRestored["sources"]:
#     for sourceDef in source_definitions:
#         sourceDefId = sourceDef["sourceDefinitionId"]
#         if sourceDef["name"] == src['stype']:
#             # add source
#             ngoClient.clientpost(
#                 "airbyte/sources/",
#                 json={
#                     "name": src["name"],
#                     "sourceDefId": sourceDefId,
#                     "config": src["config"]
#                 }
#             )

# # create connections between all sources to destinations
# sources = ngoClient.clientget("airbyte/sources")
# for i, src in enumerate(sources):
#     connPayload = {
#         'name': f'test-conn-{i}',
#         'normalize': False,
#         'sourceId': src["sourceId"],
#         'streams': []
#     }

#     schemaCatalog = ngoClient.clientget(f'airbyte/sources/{src["sourceId"]}/schema_catalog')
#     for streamData in schemaCatalog['catalog']['streams']:
#         streamPayload = {}
#         streamPayload['selected'] = True
#         streamPayload['name'] = streamData['stream']['name']
#         streamPayload['supportsIncremental'] = False
#         streamPayload['destinationSyncMode'] = 'append'
#         streamPayload['syncMode'] = 'full_refresh'
#         connPayload['streams'].append(streamPayload)

#     ngoClient.clientpost("airbyte/connections/", json=connPayload)

# # create dbt workspace
