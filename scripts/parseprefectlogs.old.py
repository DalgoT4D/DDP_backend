"""parse the logs from a flow run"""
import os
import re
import json
import argparse
import requests

parser = argparse.ArgumentParser(description="Parse the logs from a flow run")
parser.add_argument("flowrun", help="flow run id")
args = parser.parse_args()


def classify_message(message: str):
    """classify a message"""
    # print(message)

    patterns = [
        {
            "pattern": "exception",
            "regex": re.compile(r"Encountered exception during execution:"),
            "action": "ignore",
        },
        {
            "pattern": "task_run_created",
            "regex": re.compile(r"Created task run '[\w\-\d]+' for task '[\w]+'"),
            "action": "ignore",
        },
        {
            "pattern": "starting_task",
            "regex": re.compile(r"Executing '([\w\-\d]+)' immediately..."),
            "action": "return_match",
            "match": "prefect_task_name",
        },
        {
            "pattern": "starting_sync",
            "regex": re.compile(
                r"Triggering Airbyte Connection ([\w\-]+), in workspace at "
            ),
            "action": "return_match",
            "match": "airbyte_connection_id",
        },
        {
            "pattern": "airbyte_job_failed",
            "regex": re.compile(r"Job (\d+) failed."),
            "action": "return_match",
            "match": "airbyte_job_id",
        },
        {
            "pattern": "airbyte_job_succeeded",
            "regex": re.compile(r"Job (\d+) succeeded."),
            "action": "return_match",
            "match": "airbyte_job_id",
        },
        {
            "pattern": "task_run_failed",
            "regex": re.compile(
                r"Finished in state Failed\('Task run encountered an exception (.*?)'\)"
            ),
            "action": "return_match",
            "match": "message",
        },
        {
            "pattern": "flow_run_failed",
            "regex": re.compile(
                r"Finished in state Failed\('Flow run encountered an exception. (.*?)'\)"
            ),
            "action": "return_match",
            "match": "message",
        },
        {
            "pattern": "run_completed",
            "regex": re.compile(r"Finished in state (\w+)"),
            "action": "return_match",
            "match": "run_state",
        },
        {
            "pattern": "downloading_flow_code",
            "regex": re.compile("Downloading flow code from storage"),
            "action": "ignore",
        },
        {
            "pattern": "creating_run",
            "regex": re.compile("Created subflow run"),
            "action": "ignore",
        },
        {
            "pattern": "shell_output_with_timestamp",
            "regex": re.compile(r"PID \d+ stream output:\s*\d{2}:\d{2}:\d{2}(.*)"),
            "action": "return_match",
            "match": "shell_output",
        },
        {
            "pattern": "no_dbt_packages_found",
            "regex": re.compile(r"(Warning: No packages were found in packages.yml)"),
            "action": "return_match",
            "match": "shell_output",
        },
        {
            "pattern": "shell_output",
            "regex": re.compile(r"PID \d+ stream output:\s*(.*)"),
            "action": "return_match",
            "match": "shell_output",
        },
        {
            "pattern": "shell_output_triggered",
            "regex": re.compile(r"PID \d+ triggered .*"),
            "action": "ignore",
        },
        {
            "pattern": "shell_output_completed",
            "regex": re.compile(r"PID \d+ completed with return code (\d)"),
            "action": "return_match",
            "match": "return_code",
        },
    ]
    for pattern in patterns:
        # stop at the first match
        m = pattern["regex"].search(message)
        if m:
            retval = {"pattern": pattern["pattern"]}

            if pattern["action"] == "return_match":
                retval["match_key"] = pattern["match"]
                retval["match_value"] = m.groups()[0].strip()

            return retval

    return {"error": "<unknown-pattern>", "message": message}


def strip_escape_sequences(message: str):
    """strip escape sequences"""
    return (
        message.replace("\x1b[0m", "")
        .replace("\u001b[33m", "")
        .replace("\u001b[32m", "")
    )


def process_messages(messages: list):
    """process messages"""

    current_step = None
    current_task_id = None
    current_task_name = None
    airbyte_connection_id = None

    workflow = []

    for message in messages:
        if current_task_id != message["task_run_id"]:
            # close last task
            current_step = None

        current_task_id = message["task_run_id"]

        cls = classify_message(strip_escape_sequences(message["message"]))

        if cls.get("error"):
            workflow.append(
                {
                    "task_run_id": message["task_run_id"],
                    "category": cls["error"],
                }
            )
            continue

        if cls.get("match_key") is None:
            continue

        if cls["pattern"] == "starting_sync":
            current_step = "airbyte_connection"
            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "category": "airbyte_sync",
                    "airbyte_connection_id": cls["match_value"],
                }
            )

        elif cls["pattern"] == "airbyte_job_failed":
            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "category": "airbyte_sync",
                    "airbyte_connection_id": airbyte_connection_id,
                    "airbyte_job_id": cls["match_value"],
                }
            )

        elif cls["pattern"] == "airbyte_job_succeeded":
            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "category": "airbyte_sync",
                    "airbyte_connection_id": airbyte_connection_id,
                    "airbyte_job_id": cls["match_value"],
                }
            )

        elif cls["pattern"] == "starting_task":
            current_task_name = cls["match_value"]
            # print(current_task_id, current_task_name)

        elif cls["pattern"] == "task_run_failed":
            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "message": cls["match_value"],
                }
            )
            current_task_id = None
            current_task_name = None

        elif cls["pattern"] == "flow_run_failed":
            workflow.append(
                {
                    "category": "flow_run_failed",
                    "pattern": cls["pattern"],
                    "message": cls["match_value"],
                }
            )
            # print("flow_run FAILED")

        elif cls["pattern"] == "run_completed":
            if current_task_id:
                workflow.append(
                    {
                        "task_run_id": current_task_id,
                        "task_name": current_task_name,
                        "pattern": "task_run_completed",
                        "status": cls["match_value"],
                    }
                )
                current_task_id = None
                current_task_name = None
            else:
                workflow.append(
                    {
                        "pattern": "flow_run_completed",
                        "status": cls["match_value"],
                    }
                )

        elif cls["match_key"] == "shell_output":
            category = "shell"

            if current_task_name.find("gitpulljob") > -1:
                current_step = "git-pull"
                category = "dbt"

            elif current_task_name.find("dbtjob") > -1:
                category = "dbt"

                if cls["match_value"].find("Cleaned ") > -1:
                    current_step = "dbt-clean"

                elif cls["match_value"].find("Found ") == 0:
                    current_step = "dbt-unknown"

                elif cls["match_value"].find("packages.yml") > -1:
                    current_step = "dbt-deps"

            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "category": category,
                    "step": current_step,
                    "output": cls["match_value"],
                }
            )

        elif cls["match_key"] == "return_code":
            workflow.append(
                {
                    "task_run_id": current_task_id,
                    "task_name": current_task_name,
                    "pattern": cls["pattern"],
                    "step": current_step,
                    "return_code": cls["match_value"],
                }
            )

        else:
            print(
                "**",
                current_task_id,
                cls,
            )

    return workflow


def run():
    """runs the script"""
    logfile = f"flow_runs/production/{args.flowrun}.run"
    if not os.path.exists(logfile):
        r = requests.post(
            "http://localhost:4200/api/logs/filter",
            json={
                "logs": {
                    "operator": "and_",
                    "flow_run_id": {
                        "any_": [args.flowrun],
                    },
                },
            },
            timeout=10,
        )
        with open(logfile, "w", encoding="utf-8") as f:
            f.write(r.text)
        messages = r.json()
    else:
        with open(logfile, "r", encoding="utf-8") as f:
            r = f.read()
        messages = json.loads(r)

    parsed_flow = process_messages(messages)
    # print(parsed_flow)

    bucketed_flow = []
    current_task = {
        "steps": [],
        "task_run_id": None,
        "task_name": None,
        "description": None,
    }

    for step in parsed_flow:
        if step["pattern"] == "flow_run_completed":
            continue

        if current_task["task_run_id"] is None:
            current_task["task_run_id"] = step["task_run_id"]
            current_task["task_name"] = step["task_name"]

        if current_task["task_run_id"] != step["task_run_id"]:
            bucketed_flow.append(current_task)
            current_task = {
                "steps": [],
                "task_run_id": step["task_run_id"],
                "task_name": step["task_name"],
                "description": step.get("step", step["pattern"]),
            }

        del step["task_run_id"]
        del step["task_name"]

        if step["pattern"] == "task_run_completed":
            if current_task["task_name"].find("trigger") > -1:
                current_task["category"] = "airbyte_sync"
            elif current_task["task_name"].find("wait_for_completion") > -1:
                current_task["category"] = "airbyte_sync"
            elif current_task["task_name"].find("fetch_result") > -1:
                current_task["category"] = "airbyte_sync"

        if "category" in step:
            if "category" not in current_task:
                current_task["category"] = step["category"]
            del step["category"]

        current_task["steps"].append(step)

        if current_task["description"] is None:
            current_task["description"] = step.get("step", step["pattern"])

    bucketed_flow.append(current_task)

    last_description = None
    for stage in bucketed_flow:
        # print(stage)
        description = stage["description"]
        if description == "dbt-unknown":
            if last_description == "dbt-clean":
                description = "dbt-deps"
            elif last_description == "dbt-deps":
                description = "dbt-run"
            elif last_description == "dbt-run":
                description = "dbt-test"
            elif last_description == "dbt-test":
                description = "dbt-docs"
        elif description == "task_run_completed":
            continue
        last_description = description
        stage["description"] = description

    for stage in bucketed_flow:
        print(stage["category"], stage["description"])
    # print(json.dumps(bucketed_flow, indent=2))


if __name__ == "__main__":
    run()
