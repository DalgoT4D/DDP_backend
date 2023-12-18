"""test various dalgo services via client api"""
import os
import json
import argparse
from time import sleep
from faker import Faker
import requests
import yaml
from dotenv import load_dotenv
from testclient.testclient import TestClient

# =============================================================================
load_dotenv("testclient/.env.test")

with open("testclient/config.yaml", "r", -1, "utf8") as configfile:
    config = yaml.safe_load(configfile)

# =============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument(
    "--host",
    default="localhost",
    help="app server host",
)
parser.add_argument("-p", "--port", default=8002, help="app server port")
args = parser.parse_args()

# =============================================================================
faker = Faker("en-IN")
tester = TestClient(args.port, verbose=args.verbose, host=args.host)

# =============================================================================
if config["user"]["create_new"]:
    tester.clientpost(
        "organizations/users/",
        json={
            "email": os.getenv("DALGO_USER"),
            "password": os.getenv("DALGO_PASSWORD"),
            "signupcode": os.getenv("SIGNUPCODE"),
        },
    )
    # todo: grant permission to create new orgs
    # until this is done, we need to grant manually

tester.login(os.getenv("DALGO_USER"), os.getenv("DALGO_PASSWORD"))
if config["org"]["create_new"]:
    tester.clientget("currentuser")
    tester.clientpost(
        "v1/organizations/", json={"name": config["org"]["name"]}, timeout=30
    )

tester.clientheaders["x-dalgo-org"] = config["org"]["name"]

# =============================================================================
if config["warehouse"]["delete_existing"]:
    tester.clientdelete("v1/organizations/warehouses/")


if config["org"]["create_new"]:
    destination_definitions = tester.clientget("airbyte/destination_definitions")
    tester.create_warehouse(config["warehouse"]["wtype"], destination_definitions)

# =============================================================================
source_definitions = None

sources = tester.clientget("airbyte/sources")
# create sources
for src in config["airbyte"]["sources"]:
    if src["name"] in [s["name"] for s in sources]:
        print(f"source {src['name']} already exists")
    else:
        if source_definitions is None:
            source_definitions = tester.clientget("airbyte/source_definitions")
        tester.create_source(
            source_definitions, src["name"], src["stype"], src["config"]
        )

sources = tester.clientget("airbyte/sources")

connections = tester.clientget("airbyte/v1/connections")
for connection_config in config["airbyte"]["connections"]:
    if connection_config["name"] in [c["name"] for c in connections]:
        print(f"connection {connection_config['name']} already exists")
        continue

    created = False
    for src in sources:
        if src["name"] == connection_config["source"]:
            tester.create_connection(connection_config, src["sourceId"])
            created = True
            break
    if not created:
        raise ValueError(
            f"could not find source {src['name']} for connection {connection_config['name']}"
        )

# =============================================================================
if config["dbt_workspace"]["setup_new"]:
    task_id = tester.setup_dbt_workspace(
        {
            "gitrepoUrl": os.getenv("DBT_TEST_REPO"),
            # "gitrepoAccessToken": os.getenv("DBT_TEST_REPO_ACCESSTOKEN"),
            "profile": {
                "name": os.getenv("DBT_PROFILE"),
                "target": "dev",
                "target_configs_schema": os.getenv("DBT_TARGETCONFIGS_SCHEMA"),
            },
        }
    )
    while True:
        sleep(5)
        resp = tester.clientget("tasks/" + task_id)
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

# =============================================================================
if config["dbt_workspace"]["git_pull"]:
    tester.clientpost("dbt/git_pull/")

# =============================================================================
if config["prefect"]["dbt"]["delete_blocks"]:
    tester.clientdelete("prefect/blocks/dbt/")

# =============================================================================
if config["prefect"]["dbt"]["delete_tasks"]:
    tester.clientdelete("prefect/tasks/transform/")

# =============================================================================
if config["prefect"]["dbt"]["create_tasks"]:
    tester.clientpost("prefect/tasks/transform/")

# =============================================================================
if config["prefect"]["dbt"]["run_flow_dbtrun"]:
    flow_run_id = tester.run_dbtrun()

    if flow_run_id:
        sleep(10)
        PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")

        ntries: int = 0
        while ntries < 20:
            sleep(1)
            ntries += 1
            r = requests.get(
                f"{PREFECT_PROXY_API_URL}/proxy/flow_runs/logs/" + flow_run_id,
                timeout=20,
            )
            for log in r.json()["logs"]:
                print(log["message"])
            print("=" * 80)

flows = tester.get_flows()
print(json.dumps(flows, indent=1))

if len(flows) > 0:
    deployment_id = flows[0]["deploymentId"]
    fetched_flow = tester.clientget("prefect/v1/flows/" + deployment_id)

if config["prefect"]["create_flow"]:
    connection_id = connections[0]["connectionId"]
    response = tester.create_flow(connection_id)
    print(json.dumps(response, indent=1))

if config["prefect"]["delete_flows"]:
    for flow in flows:
        tester.clientdelete("prefect/v1/flows/" + flow["deploymentId"])
