"""utilities for parsing and summarizing prefect logs"""

import re
import logging
import psycopg2

logger = logging.getLogger()


def fetch_logs_from_db(connection_info: dict, flow_run_id: str):
    """fetches the logs from the prefect database"""

    connection = psycopg2.connect(**connection_info)
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
            "status": "failed",
        }
    pattern_2 = re.compile(r"Job \d+ succeeded")
    if pattern_2.match(line):
        return {
            "pattern": "airbyte-sync-job-succeeded",
            "status": "success",
        }


def parse_git_pull_log(line: str):
    """parses a log line from git pull"""
    pattern_1 = re.compile(r"Already up to date.")
    if pattern_1.match(line):
        return {
            "pattern": "already-up-to-date",
            "status": "success",
        }


def parse_dbt_clean_log(line: str):
    """parses a log line from dbt run"""
    pattern_start_step = re.compile(r"\d+ of \d+ START ")
    if pattern_start_step.search(line):
        return {
            "pattern": "step-started",
            "status": "success",
        }
    pattern_1 = re.compile(r"Checking target")
    if pattern_1.match(line):
        return {
            "pattern": "checking-target",
            "status": "success",
        }
    pattern_2 = re.compile(r"Cleaned target")
    if pattern_2.match(line):
        return {
            "pattern": "cleaned-target",
            "status": "success",
        }
    pattern_3 = re.compile(r"Checking dbt_packages")
    if pattern_3.match(line):
        return {
            "pattern": "checking-dbt-packages",
            "status": "success",
        }
    pattern_4 = re.compile(r"Cleaned dbt_packages")
    if pattern_4.match(line):
        return {
            "pattern": "cleaned-dbt-packages",
            "status": "success",
        }
    pattern_5 = re.compile(r"Finished cleaning all paths")
    if pattern_5.match(line):
        return {
            "pattern": "cleaned-all-paths",
            "status": "success",
        }


def parse_dbt_deps_log(line: str):
    """parses a log line from dbt run"""
    pattern_start_step = re.compile(r"\d+ of \d+ START ")
    if pattern_start_step.search(line):
        return {
            "pattern": "step-started",
            "status": "success",
        }
    pattern_1 = re.compile(r"Installing")
    if pattern_1.match(line):
        return {
            "pattern": "installing-package",
            "status": "success",
        }
    pattern_2 = re.compile(r"Installed from version")
    if pattern_2.match(line):
        return {
            "pattern": "installed-package",
            "status": "success",
        }
    pattern_3 = re.compile(r"Updated version available")
    if pattern_3.match(line):
        return {
            "pattern": "updated-version-available",
            "status": "success",
        }
    pattern_4 = re.compile(r"Updates available for packages")
    if pattern_4.match(line):
        return {
            "pattern": "updates-available-for-packages",
            "status": "success",
        }


def parse_dbt_run_log(line: str):
    """parses a log line from dbt run"""
    pattern_1 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_1.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
            "status": "success",
        }
    pattern_start_step = re.compile(r"\d+ of \d+ START ")
    if pattern_start_step.search(line):
        return {
            "pattern": "step-started",
            "status": "success",
        }
    pattern_3 = re.compile(r"\d+ of \d+ OK created ")
    if pattern_3.search(line):
        return {
            "pattern": "step-success",
            "status": "success",
        }
    pattern_4 = re.compile(r"Finished running")
    if pattern_4.match(line):
        return {
            "pattern": "run-finished",
            "status": "success",
        }
    pattern_5 = re.compile(
        r"Done. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)"
    )
    if pattern_5.match(line):
        match = pattern_5.match(line)
        passed = int(match.groups()[0])
        warnings = int(match.groups()[1])
        errors = int(match.groups()[2])
        skipped = int(match.groups()[3])

        return {
            "pattern": "run-summary",
            "status": "success" if warnings + errors == 0 else "failed",
            "passed": passed,
            "warnings": warnings,
            "errors": errors,
            "skipped": skipped,
        }


def parse_dbt_test_log(line: str):
    """parses a log line from dbt test"""
    pattern_start_step = re.compile(r"\d+ of \d+ START ")
    if pattern_start_step.search(line):
        return {
            "pattern": "step-started",
            "status": "success",
        }
    pattern_0 = re.compile(r"Failure in test ([\w_]+) \(([\w\/\.]+)\)")
    if pattern_0.match(line):
        return {
            "pattern": "failure-in-test",
            "model": pattern_0.match(line).groups()[0],
            "file": pattern_0.match(line).groups()[1],
            "status": "failed",
        }
    pattern_1 = re.compile(r"\d+ of \d+ PASS .*")
    if pattern_1.match(line):
        return {
            "pattern": "test-passed",
            "status": "success",
        }
    pattern_1b = re.compile(r"\d+ of \d+ FAIL .*")
    if pattern_1b.match(line):
        return {
            "pattern": "test-failed",
            "status": "failed",
        }
    pattern_2 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_2.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
            "status": "success",
        }
    pattern_3 = re.compile(r"Finished running \d+ tests in \d+ hours \d+ minutes")
    if pattern_3.match(line):
        return {
            "pattern": "timing-report",
            "status": "success",
        }
    pattern_4 = re.compile(r"Completed with \d+ errors? and \d+ warnings?")
    if pattern_4.match(line):
        return {
            "pattern": "timing-report",
        }


def parse_dbt_docs_generate_log(line: str):
    """parses a log line from dbt run"""
    pattern_start_step = re.compile(r"\d+ of \d+ START ")
    if pattern_start_step.search(line):
        return {
            "pattern": "step-started",
            "status": "success",
        }
    pattern_1 = re.compile(
        r"Found \d+ models, \d+ tests, \d+ sources, \d+ exposures, \d+ metrics, \d+ macros, \d+ groups, \d+ semantic models"
    )
    if pattern_1.match(line):
        return {
            "pattern": "found-models-tests-sources-etc",
            "status": "success",
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


def parse_prefect_logs(connection_info: dict, flow_run_id: str):
    """fetches parses and summarizes the logs for a flow run"""
    messages = fetch_logs_from_db(connection_info, flow_run_id)

    result = []
    last_task_name = None
    task_summary = {"task_name": None, "state_name": None}

    for message in messages:
        # some task types are just skipped
        if message["task_name"] == "trigger-0":
            logger.debug(f"skipping task: {message['task_name']}")
            continue

        # rename to more descriptive task names
        message["task_name"] = rename_task_name(message["task_name"])

        if task_summary["task_name"] is None:
            task_summary["task_name"] = message["task_name"]

        if last_task_name is None or last_task_name != message["task_name"]:
            if task_summary["state_name"] != "unknown":
                result.append(task_summary)
            last_task_name = message["task_name"]
            task_summary = {"task_name": message["task_name"], "state_name": None}
            logger.debug(f"new task: {message['task_name']}")

        # some log lines are multiline
        lines = message["message"].split("\n")

        for line in lines:
            # escape characters
            line = remove_color_codes(line.strip())

            # skip lines based on regex patterns
            if skip_line(line):
                continue

            # clean up a little
            line = remove_timestamps(line).strip()

            # now start parsing based on the task
            if message["task_name"] == "wait_for_completion-0":
                # airbyte sync failure
                if message["state_name"] == "Failed":
                    match = parse_airbyte_wait_for_completion_log(line)
                    if match:
                        logger.debug(
                            f"[{message['task_name']}] [{message['state_name']}] {match['pattern']}"
                        )
                        task_summary.update(match)
                # airbyte sync success
                elif message["state_name"] == "Completed":
                    match = parse_airbyte_wait_for_completion_log(line)
                    if match:
                        logger.debug(
                            f"[{message['task_name']}] [{message['state_name']}] {match['pattern']}"
                        )
                        task_summary.update(match)
                else:
                    logger.warning(
                        f"[{message['task_name']}] {message['state_name']} {line}"
                    )

            # git pull
            elif message["task_name"] == "git pull":
                # we ignore most output from git pull
                match = parse_git_pull_log(line)
                if match:
                    if match["pattern"] == "already-up-to-date":
                        logger.debug(f"[{message['task_name']}] => already-up-to-date")
                        task_summary.update(match)
                    # ignore other matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            # dbt clean
            elif message["task_name"] == "dbt clean":
                # we ignore most output from dbt clean
                match = parse_dbt_clean_log(line)
                if match:
                    if match["pattern"] == "cleaned-all-paths":
                        logger.debug(f"[{message['task_name']}] => cleaned all paths")
                        task_summary.update(match)
                    # ignore other matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            # dbt deps
            elif message["task_name"] == "dbt deps":
                # we ignore most output from dbt deps
                match = parse_dbt_deps_log(line)
                if match:
                    if match["pattern"] == "installed-package":
                        logger.debug(f"[{message['task_name']}] => installed package")
                        task_summary.update(match)
                    # ignore other matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            # dbt run
            elif message["task_name"] == "dbt run":
                # we ignore most output from dbt run
                match = parse_dbt_run_log(line)
                if match:
                    if match["pattern"] == "run-summary":
                        task_summary.update(match)
                    # ignore all matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            # dbt test
            elif message["task_name"] == "dbt test":
                # look for patterns we care about
                match = parse_dbt_test_log(line)
                if match:
                    if match["pattern"] == "failure-in-test":
                        logger.debug(
                            f"[{message['task_name']}] => test failed for model {match['model']} in file {match['file']}"
                        )
                        task_summary.update(match)
                    # ignore other matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            # dbt docs
            elif message["task_name"] == "dbt docs generate":
                match = parse_dbt_docs_generate_log(line)

                # we ignore most output from dbt docs
                if match:
                    pass
                    # ignore all matches
                else:
                    logger.warning(f"[{message['task_name']}] {line}")

            else:
                logger.warning(
                    f"[{message['task_name']}] [{message['state_name']}] {line}"
                )

    if task_summary["state_name"] is not None:
        result.append(task_summary)
    return result
