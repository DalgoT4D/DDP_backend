import os
import sys
import json
from time import sleep
from faker import Faker
from uuid import uuid4
import requests
import argparse
from dotenv import load_dotenv
from testclient import TestClient

load_dotenv(".env.test")

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument(
    "-p", "--port", required=True, help="port where app server is listening"
)
args = parser.parse_args()


DBT_PROFILE = os.getenv("DBT_PROFILE")
WAREHOUSETYPE = os.getenv("WAREHOUSETYPE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("DBT_TARGETCONFIGS_SCHEMA")
DBT_CREDENTIALS_USERNAME = os.getenv("DBT_CREDENTIALS_USERNAME")
DBT_CREDENTIALS_PASSWORD = os.getenv("DBT_CREDENTIALS_PASSWORD")
DBT_CREDENTIALS_DATABASE = os.getenv("DBT_CREDENTIALS_DATABASE")
DBT_CREDENTIALS_HOST = os.getenv("DBT_CREDENTIALS_HOST")
DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE = os.getenv(
    "DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE"
)
BIGQUERY_PROJECTID = os.getenv("BIGQUERY_PROJECTID")
BIGQUERY_DATASETID = os.getenv("BIGQUERY_DATASETID")
BIGQUERY_DATASETLOCATION = os.getenv("BIGQUERY_DATASETLOCATION")

DBT_TEST_REPO = os.getenv("DBT_TEST_REPO")
DBT_TEST_REPO_ACCESSTOKEN = os.getenv("DBT_TEST_REPO_ACCESSTOKEN")
SIGNUPCODE = os.getenv("SIGNUPCODE")

faker = Faker("en-IN")
tester = TestClient(args.port, verbose=args.verbose)
email = faker.email()
password = faker.password()
tester.clientpost(
    "organizations/users/",
    json={"email": email, "password": password, "signupcode": SIGNUPCODE},
)
tester.login(email, password)
tester.clientget("currentuser")
tester.clientpost(
    "organizations/",
    json={
        "name": faker.company()[:20],
    },
)
# tester.clientdelete("organizations/warehouses/")


# dbtCredentials = None
# airbyteConfig = None
# destinationDefinitionId = None

destination_definitions = tester.clientget("airbyte/destination_definitions")
if WAREHOUSETYPE == "postgres":
    for destdef in destination_definitions:
        if destdef["name"] == "Postgres":
            destinationDefinitionId = destdef["destinationDefinitionId"]
            break
    dbtCredentials = {
        "host": DBT_CREDENTIALS_HOST,
        "port": 5432,
        "username": DBT_CREDENTIALS_USERNAME,
        "password": DBT_CREDENTIALS_PASSWORD,
        "database": DBT_CREDENTIALS_DATABASE,
    }
    airbyteConfig = {
        "host": DBT_CREDENTIALS_HOST,
        "port": 5432,
        "username": DBT_CREDENTIALS_USERNAME,
        "password": DBT_CREDENTIALS_PASSWORD,
        "database": DBT_CREDENTIALS_DATABASE,
        "schema": DBT_TARGETCONFIGS_SCHEMA,
    }
elif WAREHOUSETYPE == "bigquery":
    for destdef in destination_definitions:
        if destdef["name"] == "BigQuery":
            destinationDefinitionId = destdef["destinationDefinitionId"]
            break
    with open(DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE, "r", -1, "utf8") as credsfile:
        dbtCredentials = json.loads(credsfile.read())
        # print(dbtCredentials)
    airbyteConfig = {
        "project_id": BIGQUERY_PROJECTID,
        "dataset_id": BIGQUERY_DATASETID,
        "dataset_location": BIGQUERY_DATASETLOCATION,
        "credentials_json": json.dumps(dbtCredentials),
    }
else:
    raise Exception("unknown WAREHOUSETYPE " + WAREHOUSETYPE)

tester.clientpost(
    "organizations/warehouse/",
    json={
        "wtype": WAREHOUSETYPE,
        "destinationDefId": destinationDefinitionId,
        "airbyteConfig": airbyteConfig,
    },
)

if True:
    tester.clientdelete("dbt/workspace/")
    r = tester.clientpost(
        "dbt/workspace/",
        json={
            "gitrepoUrl": DBT_TEST_REPO,
            # "gitrepoAccessToken": DBT_TEST_REPO_ACCESSTOKEN,
            "dbtVersion": "1.4.5",
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

if False:
    tester.clientpost("dbt/git_pull/")

tester.clientdelete("prefect/blocks/dbt/")

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
        print(f"POST {PREFECT_PROXY_API_URL}/proxy/flow_run/ with name={flow_run_name}")
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
        f"{PREFECT_PROXY_API_URL}/proxy/flow_runs/logs/" + flow_run["id"], timeout=10
    )
    for log in r.json()["logs"]:
        print(log["message"])
    print("=" * 80)

# tester.clientdelete("prefect/blocks/dbt/")
