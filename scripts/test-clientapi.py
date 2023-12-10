"""test various dalgo services via client api"""
import os
import sys
import json
import argparse
from time import sleep
from uuid import uuid4
from faker import Faker
import requests
import yaml
from dotenv import load_dotenv
from testclient.testclient import TestClient

load_dotenv("testclient/.env.test")
with open("testclient/config.yaml", "r", -1, "utf8") as configfile:
    config = yaml.safe_load(configfile)
print(config)
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument(
    "--host",
    default="localhost",
    help="app server host",
)
parser.add_argument("-p", "--port", default=8002, help="app server port")
args = parser.parse_args()


DBT_PROFILE = os.getenv("DBT_PROFILE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("DBT_TARGETCONFIGS_SCHEMA")

DBT_TEST_REPO = os.getenv("DBT_TEST_REPO")
DBT_TEST_REPO_ACCESSTOKEN = os.getenv("DBT_TEST_REPO_ACCESSTOKEN")
SIGNUPCODE = os.getenv("SIGNUPCODE")

faker = Faker("en-IN")
tester = TestClient(args.port, verbose=args.verbose, host=args.host)
if config["org"]["create_new"]:
    email = os.getenv("DALGO_USER")
    password = os.getenv("DALGO_PASSWORD")
    # tester.clientpost(
    #     "organizations/users/",
    #     json={"email": email, "password": password, "signupcode": SIGNUPCODE},
    # )
    tester.login(email, password)
    tester.clientget("currentuser")
    tester.clientpost(
        "v1/organizations/", json={"name": config["org"]["name"]}, timeout=30
    )
else:
    tester.login(os.getenv("DALGO_USER"), os.getenv("DALGO_PASSWORD"))

tester.clientheaders["x-dalgo-org"] = config["org"]["name"]

if config["warehouse"]["delete_existing"]:
    tester.clientdelete("v1/organizations/warehouses/")

destination_definitions = tester.clientget("airbyte/destination_definitions")

if config["org"]["create_new"]:
    tester.create_warehouse(config["warehouse"]["wtype"], destination_definitions)

# ======
source_definitions = tester.clientget("airbyte/source_definitions")

sources = tester.clientget("airbyte/sources")
# create sources
for src in config["airbyte"]["sources"]:
    if src["name"] in [s["name"] for s in sources]:
        print(f"source {src['name']} already exists")
    else:
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

# ======
if config["dbt_workspace"]["setup_new"]:
    tester.clientdelete("dbt/workspace/")
    r = tester.clientpost(
        "dbt/workspace/",
        json={
            "gitrepoUrl": DBT_TEST_REPO,
            # "gitrepoAccessToken": DBT_TEST_REPO_ACCESSTOKEN,
            "profile": {
                "name": DBT_PROFILE,
                "target": "dev",
                "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
            },
        },
        timeout=30,
    )
    task_id = r["task_id"]
    while True:
        sleep(2)
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

if config["dbt_workspace"]["git_pull"]:
    tester.clientpost("dbt/git_pull/")

if config["prefect"]["dbt"]["delete_blocks"]:
    tester.clientdelete("prefect/blocks/dbt/")

if config["prefect"]["dbt"]["create_blocks"]:
    r = tester.clientpost(
        "prefect/blocks/dbt/",
        json={
            "profile": {
                "name": DBT_PROFILE,
                "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
            },
        },
        timeout=60,
    )
    print(r)

if config["prefect"]["dbt"]["run_flow"]:
    r = tester.clientget("prefect/blocks/dbt/")
    flow_run_name: str = str(uuid4())
    tester.clientpost(
        "prefect/flows/dbt_run/",
        json={
            "blockName": r["block_names"][0],
            "flowRunName": flow_run_name,
        },
        timeout=10,
    )
    sleep(10)
    PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")
    nattempts: int = 0
    while nattempts < 10:
        try:
            print(
                f"POST {PREFECT_PROXY_API_URL}/proxy/flow_run/ with name={flow_run_name}"
            )
            response = requests.post(
                f"{PREFECT_PROXY_API_URL}/proxy/flow_run/",
                json={"name": flow_run_name},
                timeout=5,
            )
            message = response.json()
            print(message)
            if "flow_run" in message:
                break
        except ValueError:
            print(response.text)
            sys.exit(1)
        except Exception as error:
            print(str(error))
            nattempts += 1
            sleep(3)

    if nattempts == 10:
        sys.exit(1)

    flow_run = message["flow_run"]
    print(flow_run)

    ntries: int = 0
    while ntries < 20:
        sleep(1)
        ntries += 1
        r = requests.get(
            f"{PREFECT_PROXY_API_URL}/proxy/flow_runs/logs/" + flow_run["id"],
            timeout=20,
        )
        for log in r.json()["logs"]:
            print(log["message"])
        print("=" * 80)

if config["prefect"]["dbt"]["delete_blocks"]:
    tester.clientdelete("prefect/blocks/dbt/")
