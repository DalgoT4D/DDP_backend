import os
import json

from dotenv import load_dotenv

load_dotenv()

from testclient import TestClient

TESTING_DBT_TEST_REPO = os.getenv("TESTING_DBT_TEST_REPO")

DBT_PROFILE = os.getenv("TESTING_DBT_PROFILE")
WAREHOUSETYPE = os.getenv("TESTING_WAREHOUSETYPE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("TESTING_DBT_TARGETCONFIGS_SCHEMA")
DBT_CREDENTIALS_USERNAME = os.getenv("TESTING_DBT_CREDENTIALS_USERNAME")
DBT_CREDENTIALS_PASSWORD = os.getenv("TESTING_DBT_CREDENTIALS_PASSWORD")
DBT_CREDENTIALS_DATABASE = os.getenv("TESTING_DBT_CREDENTIALS_DATABASE")
DBT_CREDENTIALS_HOST = os.getenv("TESTING_DBT_CREDENTIALS_HOST")
DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE = os.getenv(
    "TESTING_DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE"
)
DBT_TEST_REPO = os.getenv("TESTING_DBT_TEST_REPO")
DBT_TEST_REPO_ACCESSTOKEN = os.getenv("TESTING_DBT_TEST_REPO_ACCESSTOKEN")

tester = TestClient(8002)

tester.login("user1@ddp", "password")
tester.clientget("currentuser")
tester.clientdelete("organizations/warehouses/")

credentials = None
if WAREHOUSETYPE == "postgres":
    credentials = {
        "host": DBT_CREDENTIALS_HOST,
        "port": "5432",
        "username": DBT_CREDENTIALS_USERNAME,
        "password": DBT_CREDENTIALS_PASSWORD,
        "database": DBT_CREDENTIALS_DATABASE,
    }
elif WAREHOUSETYPE == "bigquery":
    with open(DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE, "r", -1, "utf8") as credsfile:
        credentials = json.loads(credsfile.read())
        print(credentials)
else:
    raise Exception("unknown WAREHOUSETYPE " + WAREHOUSETYPE)

tester.clientpost(
    "organizations/warehouse/",
    json={
        "wtype": WAREHOUSETYPE,
        "credentials": credentials,
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

if True:
    tester.clientpost("dbt/git_pull/")

tester.clientdelete("prefect/blocks/dbt/")

r = tester.clientpost(
    "prefect/blocks/dbt/",
    json={
        "profile": {
            "name": DBT_PROFILE,
            "target": "dev",
            "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
        },
    },
    timeout=60,
)
print(r)

tester.clientpost(
    "prefect/flows/dbt_run/", json={"blockName": r['block_name']}, timeout=60
)

# tester.clientdelete("prefect/blocks/dbt/")
