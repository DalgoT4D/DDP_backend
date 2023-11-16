"""fetches the logs from the prefect database"""
import os
import re
import argparse
import psycopg2
from dotenv import load_dotenv

parser = argparse.ArgumentParser(description="Parse the logs from a flow run")
parser.add_argument("flowrun", help="flow run id")
args = parser.parse_args()


def fetch_logs_from_db(flow_run_id: str):
    """fetches the logs from the prefect database"""
    load_dotenv("scripts/parseprefectlogs.env", verbose=True, override=True)
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cursor = connection.cursor()
    query = f"""
        SELECT "log"."timestamp",
            "task_run"."name",
            "task_run"."state_name",
            "task_run"."state_type",
            "log"."message"
        FROM "log"
        JOIN "task_run"
        ON "log"."task_run_id" = "task_run"."id"
        WHERE "log"."flow_run_id" = '{flow_run_id}'
        ORDER BY "timestamp"
    """
    cursor.execute(query)
    records = cursor.fetchall()
    cursor.close()
    connection.close()
    header = ["timestamp", "task_name", "state_name", "state_type", "message"]

    return [dict(zip(header, record)) for record in records]


def remove_color_codes(line: str):
    """Remove terminal color codes from the line"""
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", line)


def remove_timestamps(line: str):
    """Remove timestamps from the line"""
    return re.sub(r"\d{2}:\d{2}:\d{2}", "", line)


def skip_line(line: str):
    """returns whether a line should be skipped"""
    patterns = [
        re.compile(r"^PID \d+ .*"),
        re.compile(r"Running with dbt=.*"),
        re.compile(r"Registered adapter: .*"),
        re.compile(r"Concurrency:.*"),
        re.compile(r"Building catalog.*"),
        re.compile(r"Catalog written to .*"),
        re.compile(r"Completed successfully.*"),
        re.compile(r"Finished in state .*"),
        re.compile(r"There are 1 unused configuration paths:"),
        re.compile(r"Update your versions in packages.yml, then run dbt deps"),
        re.compile(r"^- models\."),
        re.compile(
            r"Configuration paths exist in your dbt_project.yml file which do not apply to any resources"
        ),
        re.compile(
            r"Unable to do partial parsing because saved manifest not found. Starting full parse."
        ),
        re.compile(r"(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])$"),
    ]
    if any([pattern.search(line) for pattern in patterns]):
        return True
    return False


def parse_airbyte_wait_for_completion_log(line: str):
    """classify log lines from airbyte sync jobs"""
    pattern_1 = re.compile(r"prefect_airbyte.exceptions.AirbyteSyncJobFailed: ")
    if pattern_1.match(line):
        return {
            "pattern": "airbyte-sync-job-failed",
        }


def parse_dbt_run_log(line: str):
    """parses a log line from dbt run"""
    pattern_1 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_1.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
        }


def parse_dbt_test_log(line: str):
    """parses a log line from dbt test"""
    pattern_0 = re.compile(r"Failure in test ([\w_]+) \(([\w\/\.]+)\)")
    if pattern_0.match(line):
        return {
            "pattern": "failure-in-test",
            "model": pattern_0.match(line).groups()[0],
            "file": pattern_0.match(line).groups()[1],
        }
    pattern_1 = re.compile(r"\d+ of \d+ (PASS|FAIL) .*")
    if pattern_1.match(line):
        return {
            "pattern": "test-passed-or-failed",
        }
    pattern_2 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_2.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
        }
    pattern_3 = re.compile(r"Finished running \d+ tests in \d+ hours \d+ minutes")
    if pattern_3.match(line):
        return {
            "pattern": "timing-report",
        }
    pattern_4 = re.compile(r"Completed with \d+ errors? and \d+ warnings?")
    if pattern_4.match(line):
        return {
            "pattern": "timing-report",
        }


def parse_dbt_docs_generate_log(line: str):
    """parses a log line from dbt run"""
    pattern_1 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_1.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
        }


def rename_task_name(task_name: str):
    """renames the task name"""
    if task_name == "gitpulljob-0":
        return "git pull"
    elif task_name == "dbtjob-0":
        return "dbt clean"
    elif task_name == "dbtjob-1":
        return "dbt deps"
    elif task_name == "dbtjob-2":
        return "dbt run"
    elif task_name == "dbtjob-3":
        return "dbt test"
    elif task_name == "dbtjob-4":
        return "dbt docs generate"
    return task_name


def run():
    """runs the script"""
    messages = fetch_logs_from_db(args.flowrun)
    for message in messages:
        if message["task_name"] == "trigger-0":
            continue
        message["task_name"] = rename_task_name(message["task_name"])
        lines = message["message"].split("\n")
        for line in lines:
            line = remove_color_codes(line.strip())
            if skip_line(line):
                continue
            line = remove_timestamps(line).strip()
            if message["task_name"] == "wait_for_completion-0":
                if message["state_name"] == "Failed":
                    match = parse_airbyte_wait_for_completion_log(line)
                    if match:
                        print(
                            f"[{message['task_name']}] [{message['state_name']}] {match['pattern']}"
                        )
                else:
                    print(f"[{message['task_name']}] {line}")
            elif message["task_name"] == "dbt run":
                if line.find(" START ") > -1:
                    continue
                match = parse_dbt_run_log(line)
                if match:
                    pass
                    # ignore other matches
                else:
                    print(f"[{message['task_name']}] {line}")
            elif message["task_name"] == "dbt test":
                if line.find(" START ") > -1:
                    continue
                match = parse_dbt_test_log(line)
                if match:
                    if match["pattern"] == "failure-in-test":
                        print(
                            f"[{message['task_name']}] => test failed for model {match['model']} in file {match['file']}"
                        )
                    # ignore other matches
                else:
                    print(f"[{message['task_name']}] {line}")
            elif message["task_name"] == "dbt docs generate":
                if line.find(" START ") > -1:
                    continue
                match = parse_dbt_docs_generate_log(line)
                if match:
                    pass
                    # ignore other matches
                else:
                    print(f"[{message['task_name']}] {line}")
            else:
                print(f"[{message['task_name']}] [{message['state_name']}] {line}")


if __name__ == "__main__":
    # 3b5473c2-f164-4fee-ad6a-2030d3a3deb3
    # 9755ec98-db63-40c7-8e52-eccf6b220d12
    # 55beb129-ba43-48a9-8139-14765c0b26fc
    # ed16d9ff-3fba-4bf3-bbe9-04ee51a22092
    run()
