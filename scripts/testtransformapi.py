"""tests for the transform api"""

import os
from time import sleep
import json
import argparse
from dotenv import load_dotenv
from testclient.testclient import TestClient

# =============================================================================
load_dotenv("testclient/.env.test")

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument(
    "--host",
    default="localhost",
    help="app server host",
)
parser.add_argument("-p", "--port", default=8002, help="app server port")
parser.add_argument("--org", required=True)
args = parser.parse_args()

tc = TestClient(args.port, verbose=args.verbose, host=args.host)

tc.login(os.getenv("DALGO_USER"), os.getenv("DALGO_PASSWORD"))

tc.clientheaders["x-dalgo-org"] = args.org

# make sure the source is detected
tc.clientpost(
    "transform/dbt_project/sync_sources/",
    json={},
)

# fetch sources and models
sources_and_models = tc.clientget("transform/dbt_project/sources_models/")

SOURCE_NAME = "googlesheets"
SCHEMA_NAME = "googlesheets"
source = None
for sm in sources_and_models:
    if (
        sm["input_type"] == "source"
        and sm["schema"] == SCHEMA_NAME
        and sm["source_name"] == SOURCE_NAME
    ):
        source = sm
        break

if source is None:
    raise Exception(f"source {SOURCE_NAME} not found")

# acquire a lock
lock = tc.clientpost(
    "transform/dbt_project/canvas/lock/",
    json={},
)

try:
    # create a model from the source
    new_model = tc.clientpost(
        "transform/dbt_project/model/",
        json={
            "canvas_lock_id": lock["lock_id"],
            "target_model_uuid": "",
            "select_columns": ["State", "District_Name"],
            "op_type": "castdatatypes",
            "config": {
                "columns": [
                    {"columnname": "Iron", "columntype": "INT"},
                    {"columnname": "Arsenic", "columntype": "INT"},
                ]
            },
            "input_uuid": source["id"],
        },
    )
    print(json.dumps(new_model, indent=2))

    res = tc.clientpost(
        "transform/dbt_project/model/",
        json={
            "canvas_lock_id": lock["lock_id"],
            "target_model_uuid": new_model["target_model_id"],
            "select_columns": ["State", "District_Name"],
            "op_type": "arithmetic",
            "config": {
                "operator": "add",
                "operands": [
                    {"is_col": True, "value": "Iron"},
                    {"is_col": True, "value": "Arsenic"},
                ],
                "output_column_name": "IronPlusArsenic",
            },
        },
    )
    print(json.dumps(res, indent=2))

    tc.clientpost(
        f"transform/dbt_project/model/{new_model['target_model_id']}/save/",
        json={
            "canvas_lock_id": lock["lock_id"],
            "name": "first_model",
            "display_name": "First Model",
            "dest_schema": "sheets",
        },
    )

    # sources_and_models = tc.clientget("transform/dbt_project/sources_models/")

    # graph = tc.clientget("transform/dbt_project/graph/")

    # run the dbt-run system deployment
    res = tc.clientpost("dbt/run_dbt_via_celery/")
    done = False
    while not done:
        sleep(1)
        progress = tc.clientget(f"tasks/{res['task_id']}")
        for p in progress["progress"]:
            print(p["message"])
            if p["status"] in ["failed", "completed"]:
                done = True
                break


except Exception as e:
    print(e)

finally:
    tc.clientpost(
        "transform/dbt_project/canvas/unlock/",
        json={"lock_id": lock["lock_id"]},
    )
