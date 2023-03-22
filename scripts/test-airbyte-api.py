import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ddpui.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import requests
import argparse
import sys
from ddpui import airbyteapi

parser = argparse.ArgumentParser()
parser.add_argument('--port', default=8000)
parser.add_argument('--workspace-id')
args = parser.parse_args()

# ========================================================================================================================
if args.workspace_id is None:
  r = airbyteapi.get_workspaces()
  assert(type(r) == dict)
  for workspace in r['workspaces']:
    print(workspace['workspaceId'], workspace['name'])

  print("creating new workspace")
  new_workspace = airbyteapi.createworkspace("new-workspace")
  assert(new_workspace['name'] == "new-workspace")
  newname = new_workspace['name'].upper()
  print("changing name of new workspace")
  airbyteapi.setworkspacename(new_workspace['workspaceId'], newname)

  r = airbyteapi.get_workspace(new_workspace['workspaceId'])
  assert(r['name'] == newname)
  print("verified that name was changed")

else:
  print("=> fetching source definitions for workspace")
  r = airbyteapi.getsourcedefinitions(args.workspace_id)
  for sourcedef in r[:2]:
    assert('sourceDefinitionId' in sourcedef)
    print("    %s %s" % (sourcedef['name'], sourcedef['dockerRepository']))
    print("==> fetching source definition specification")
    rr = airbyteapi.getsourcedefinitionspecification(args.workspace_id, sourcedef['sourceDefinitionId'])
    print("    %s %s" % (rr['title'], rr['type']))
    # break

  print("=> fetching sources for workspace")
  r = airbyteapi.getsources(args.workspace_id)
  for source in r:
    print(source['name'], source['sourceId'])
    print("verifying that we can get the source directly...")
    osource = airbyteapi.getsource(args.workspace_id, source['sourceId'])
    assert(source == osource)
    print("...success")

  # 
  print("creating a source to read a public csv from the web")
  source = airbyteapi.createsource(
    args.workspace_id, 
    "web-text-source", 
    "778daa7c-feaf-4db6-96f3-70fd645acc77", 
    {
      "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
      "format": "csv",
      "provider": { "storage": "HTTPS" },
      "dataset_name": "covid19data",
    }
  )
  assert('sourceId' in source)
  # print("checking connection to new source " + source['sourceId'])

  # check_result = airbyteapi.checksourceconnection(args.workspace_id, source['sourceId'])
  # if check_result.get('status') != 'succeeded':
  #   print(check_result)
  #   sys.exit(1)

  # print("connection test passed")

  print("getting catalog for source schema...")
  r = airbyteapi.getsourceschemacatalog(args.workspace_id, source['sourceId'])
  print("...success")

  print("getting destination definitions")
  r = airbyteapi.getdestinationdefinitions(args.workspace_id)
  for destdef in r:
    print(f"fetching spec for {destdef['name']}")
    rr = airbyteapi.getdestinationdefinitionspecification(args.workspace_id, destdef['destinationDefinitionId'])
    print(f"fetched spec: {rr['title']}")
    break

  print("fetching destinations")
  r = airbyteapi.getdestinations(args.workspace_id)
  print(f"fetched {len(r)} destinations")
  destination = airbyteapi.getdestination(args.workspace_id, r[0]['destinationId'])
  assert(destination == r[0])

  print("creating destination connection")
  r = airbyteapi.createdestination(
    args.workspace_id, 
    "dest-local-csv",
    "8be1cf83-fde1-477f-a4ad-318d23c9f3c6",
    {
      "destination_path": "/tmp"
    }
  )
  print(r['name'], r['destinationId'])
  print("checking connection to destination")
  check_result = airbyteapi.checkdestinationconnection(args.workspace_id, r['destinationId'])
  if check_result.get('status') != 'succeeded':
    print(check_result)
    sys.exit(1)

  print("fetching connections")
  r = airbyteapi.getconnections(args.workspace_id)
  print(f"found {len(r)} connections")

  if len(r) > 0:
    print("verifying that we can get the destination directly...")
    connection = airbyteapi.getconnection(args.workspace_id, r[0]['connectionId'])
    assert(connection == r[0])
    print("...success")
