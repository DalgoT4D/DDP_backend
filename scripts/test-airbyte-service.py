import os
import argparse
import sys
import django
from ddpui.ddpairbyte import airbyte_service, schema

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

parser = argparse.ArgumentParser()
parser.add_argument("--port", default=8000)
parser.add_argument("--workspace-id")
args = parser.parse_args()

# ========================================================================================================================
if args.workspace_id is None:
    r = airbyte_service.get_workspaces()
    assert isinstance(r, dict)
    for workspace in r["workspaces"]:
        print(workspace["workspaceId"], workspace["name"])

    print("creating new workspace")
    new_workspace = airbyte_service.create_workspace("new-workspace")
    assert new_workspace["name"] == "new-workspace"
    newname = new_workspace["name"].upper()
    print("changing name of new workspace")
    airbyte_service.set_workspace_name(new_workspace["workspaceId"], newname)

    r = airbyte_service.get_workspace(new_workspace["workspaceId"])
    assert r["name"] == newname
    print("verified that name was changed")

else:
    print("=> fetching source definitions for workspace")
    r = airbyte_service.get_source_definitions(args.workspace_id)
    for sourcedef in r[:2]:
        assert "sourceDefinitionId" in sourcedef
        print(f'    {sourcedef["name"]} {sourcedef["dockerRepository"]}')
        print("==> fetching source definition specification")
        rr = airbyte_service.get_source_definition_specification(
            args.workspace_id, sourcedef["sourceDefinitionId"]
        )
        print(f'    {rr["title"]} {rr["type"]}')
        # break

    print("=> fetching sources for workspace")
    r = airbyte_service.get_sources(args.workspace_id)
    for source in r:
        print(source["name"], source["sourceId"])
        print("verifying that we can get the source directly...")
        osource = airbyte_service.get_source(args.workspace_id, source["sourceId"])
        assert source == osource
        print("...success")

    #
    print("creating a source to read a public csv from the web")
    source = airbyte_service.create_source(
        args.workspace_id,
        "web-text-source",
        "778daa7c-feaf-4db6-96f3-70fd645acc77",
        {
            "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
            "format": "csv",
            "provider": {"storage": "HTTPS"},
            "dataset_name": "covid19data",
        },
    )
    assert "sourceId" in source
    # print("checking connection to new source " + source['sourceId'])

    # check_result = airbyte_service.checksourceconnection(args.workspace_id, source['sourceId'])
    # if check_result.get('status') != 'succeeded':
    #   print(check_result)
    #   sys.exit(1)

    # print("connection test passed")

    print("getting catalog for source schema...")
    source_schema_catalog = airbyte_service.get_source_schema_catalog(
        args.workspace_id, source["sourceId"]
    )
    print("...success")

    print("getting destination definitions")
    r = airbyte_service.get_destination_definitions(args.workspace_id)
    for destdef in r:
        print(f"fetching spec for {destdef['name']}")
        rr = airbyte_service.get_destination_definition_specification(
            args.workspace_id, destdef["destinationDefinitionId"]
        )
        print(f"fetched spec: {rr['title']}")
        break

    print("fetching destinations")
    r = airbyte_service.get_destinations(args.workspace_id)
    print(f"fetched {len(r)} destinations")

    if len(r) == 0:
        print("creating destination connection")
        r = airbyte_service.create_destination(
            args.workspace_id,
            "dest-local-csv",
            "8be1cf83-fde1-477f-a4ad-318d23c9f3c6",
            {"destination_path": "/tmp"},
        )
        print(r["name"], r["destinationId"])
        destination = airbyte_service.get_destination(
            args.workspace_id, r["destinationId"]
        )
        assert destination == r
    else:
        destination = r[0]

    print("checking connection to destination")
    check_result = airbyte_service.check_destination_connection(
        args.workspace_id, destination["destinationId"]
    )
    if check_result.get("status") != "succeeded":
        print(check_result)
        sys.exit(1)

    print("fetching connections")
    r = airbyte_service.get_connections(args.workspace_id)
    print(f"found {len(r)} connections")

    # fetch the source catalog, select a stream and create a connection

    for conn in r:
        print("verifying that we can get the destination directly...")
        connection = airbyte_service.get_connection(
            args.workspace_id, conn["connectionId"]
        )
        assert connection == conn
        print("...success. deleting this connection now")
        airbyte_service.delete_connection(args.workspace_id, connection["connectionId"])

    print("creating a new connection")
    connection_info = schema.AirbyteConnectionCreate(
        name="conn",
        sourceId=source["sourceId"],
        destinationId=destination["destinationId"],
        streamNames=[
            x["stream"]["name"] for x in source_schema_catalog["catalog"]["streams"]
        ],
    )
    result = airbyte_service.create_connection(args.workspace_id, None, connection_info)
    print(result)

    print("syncing the new connection")
    airbyte_service.sync_connection(args.workspace_id, result["connectionId"])
