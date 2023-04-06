import argparse
from testclient import TestClient

import os
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--port", default=8000)
parser.add_argument("--email", required=True)
parser.add_argument("--password", required=True)
args = parser.parse_args()

tester = TestClient(args.port)
tester.login(args.email, args.password)

DBT_PROFILE = os.getenv("TESTING_DBT_PROFILE")
DBT_TARGETCONFIGS_TYPE = os.getenv("TESTING_DBT_TARGETCONFIGS_TYPE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("TESTING_DBT_TARGETCONFIGS_SCHEMA")
DBT_CREDENTIALS_USERNAME = os.getenv("TESTING_DBT_CREDENTIALS_USERNAME")
DBT_CREDENTIALS_PASSWORD = os.getenv("TESTING_DBT_CREDENTIALS_PASSWORD")
DBT_CREDENTIALS_DATABASE = os.getenv("TESTING_DBT_CREDENTIALS_DATABASE")
DBT_CREDENTIALS_HOST = os.getenv("TESTING_DBT_CREDENTIALS_HOST")
DBT_TEST_REPO = os.getenv("TESTING_DBT_TEST_REPO")

r = tester.clientpost(
    "dbt/workspace/",
    json={
        "gitrepo_url": DBT_TEST_REPO,
        "dbtversion": "1.4.5",
        "profile": {
            "name": DBT_PROFILE,
            "target": "dev",
            "target_configs_type": DBT_TARGETCONFIGS_TYPE,
            "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
        },
        "credentials": {
            "host": DBT_CREDENTIALS_HOST,
            "port": "5432",
            "username": DBT_CREDENTIALS_USERNAME,
            "password": DBT_CREDENTIALS_PASSWORD,
            "database": DBT_CREDENTIALS_DATABASE,
        },
    },
)

print(r)

tester.clientdelete("dbt/workspace/")
