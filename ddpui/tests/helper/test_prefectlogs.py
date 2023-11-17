from unittest.mock import patch, Mock
from ddpui.utils.prefectlogs import (
    remove_timestamps,
    skip_line,
    parse_airbyte_wait_for_completion_log,
    parse_dbt_clean_log,
    parse_dbt_deps_log,
    parse_dbt_docs_generate_log,
    parse_dbt_run_log,
    parse_dbt_test_log,
    parse_git_pull_log,
    parse_prefect_logs,
    rename_task_name,
)


def test_remove_timestamps():
    """Test remove_timestamps"""
    assert remove_timestamps("00:00:00") == ""


def test_skip_line():
    """test skip_line"""
    assert skip_line("PID 123 started") is True
    assert skip_line("Running with dbt=1.2.4") is True
    assert skip_line("Registered adapter: postgres") is True
    assert skip_line("Concurrency: 1") is True
    assert skip_line("Building catalog.") is True
    assert skip_line("Catalog written to .") is True
    assert skip_line("Completed successfully.") is True
    assert skip_line("Finished in state .") is True
    assert skip_line("There are 1 unused configuration paths:") is True
    assert skip_line("Update your versions in packages.yml, then run dbt deps") is True
    assert (
        skip_line(
            "Configuration paths exist in your dbt_project.yml file which do not apply to any resources"
        )
        is True
    )
    assert (
        skip_line(
            "Unable to do partial parsing because saved manifest not found. Starting full parse."
        )
        is True
    )


def test_parse_airbyte_wait_for_completion_log():
    """tests parse_airbyte_wait_for_completion_log"""
    assert parse_airbyte_wait_for_completion_log(
        "prefect_airbyte.exceptions.AirbyteSyncJobFailed: "
    ) == {"pattern": "airbyte-sync-job-failed", "status": "failed"}
    assert parse_airbyte_wait_for_completion_log("Job 123 succeeded") == {
        "pattern": "airbyte-sync-job-succeeded",
        "status": "success",
    }


def test_parse_git_pull_log():
    """tests parse_git_pull_log"""
    assert parse_git_pull_log("Already up to date.") == {
        "pattern": "already-up-to-date",
        "status": "success",
    }


def test_parse_dbt_clean_log():
    """tests parse_dbt_clean_log"""
    assert parse_dbt_clean_log("1 of 1 START ") == {
        "pattern": "step-started",
        "status": "success",
    }
    assert parse_dbt_clean_log("Checking target ") == {
        "pattern": "checking-target",
        "status": "success",
    }
    assert parse_dbt_clean_log("Cleaned target ") == {
        "pattern": "cleaned-target",
        "status": "success",
    }
    assert parse_dbt_clean_log("Checking dbt_packages ") == {
        "pattern": "checking-dbt-packages",
        "status": "success",
    }
    assert parse_dbt_clean_log("Cleaned dbt_packages ") == {
        "pattern": "cleaned-dbt-packages",
        "status": "success",
    }
    assert parse_dbt_clean_log("Finished cleaning all paths") == {
        "pattern": "cleaned-all-paths",
        "status": "success",
    }


def test_parse_dbt_deps_logs():
    """tests parse_dbt_deps_logs"""
    assert parse_dbt_deps_log("1 of 1 START ") == {
        "pattern": "step-started",
        "status": "success",
    }
    assert parse_dbt_deps_log("Installing package") == {
        "pattern": "installing-package",
        "status": "success",
    }
    assert parse_dbt_deps_log("Installed from version") == {
        "pattern": "installed-package",
        "status": "success",
    }
    assert parse_dbt_deps_log("Updated version available") == {
        "pattern": "updated-version-available",
        "status": "success",
    }
    assert parse_dbt_deps_log("Updates available for packages") == {
        "pattern": "updates-available-for-packages",
        "status": "success",
    }


def test_parse_dbt_run_log():
    """Tests parse_dbt_run_log"""
    assert parse_dbt_run_log(
        "Found 1 models, 1 tests, 1 sources, 1 exposures, 1 metrics, 1 macros, 1 groups, 1 semantic models"
    ) == {
        "pattern": "found-models-tests-sources-etc",
        "status": "success",
    }
    assert parse_dbt_run_log("1 of 1 START ") == {
        "pattern": "step-started",
        "status": "success",
    }
    assert parse_dbt_run_log("1 of 1 OK created ") == {
        "pattern": "step-success",
        "status": "success",
    }
    assert parse_dbt_run_log("Finished running") == {
        "pattern": "run-finished",
        "status": "success",
    }
    assert parse_dbt_run_log("Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10") == {
        "pattern": "run-summary",
        "status": "success",
        "passed": 10,
        "warnings": 0,
        "errors": 0,
        "skipped": 0,
    }
    assert parse_dbt_run_log("Done. PASS=10 WARN=1 ERROR=0 SKIP=0 TOTAL=10") == {
        "pattern": "run-summary",
        "status": "failed",
        "passed": 10,
        "warnings": 1,
        "errors": 0,
        "skipped": 0,
    }
    assert parse_dbt_run_log("Done. PASS=10 WARN=0 ERROR=1 SKIP=0 TOTAL=10") == {
        "pattern": "run-summary",
        "status": "failed",
        "passed": 10,
        "warnings": 0,
        "errors": 1,
        "skipped": 0,
    }


def test_parse_dbt_test_log():
    """tests parse_dbt_test_log"""
    assert parse_dbt_test_log("1 of 2 START ") == {
        "pattern": "step-started",
        "status": "success",
    }
    assert parse_dbt_test_log("Failure in test model (file)") == {
        "pattern": "failure-in-test",
        "model": "model",
        "file": "file",
        "status": "failed",
    }
    assert parse_dbt_test_log("1 of 2 PASS ") == {
        "pattern": "test-passed",
        "status": "success",
    }
    assert parse_dbt_test_log("1 of 2 FAIL ") == {
        "pattern": "test-failed",
        "status": "failed",
    }
    assert parse_dbt_test_log(
        "Found 1 models, 1 tests, 1 sources, 1 exposures, 1 metrics, 1 macros, 1 groups, 1 semantic models"
    ) == {
        "pattern": "found-models-tests-sources-etc",
        "status": "success",
    }
    assert parse_dbt_test_log("Finished running 0 tests in 0 hours 0 minutes") == {
        "pattern": "timing-report",
        "status": "success",
    }
    assert parse_dbt_test_log("Completed with 2 errors and 2 warnings") == {
        "pattern": "completed-with-errors-and-warnings",
    }
    assert parse_dbt_test_log("Completed with 1 error and 1 warning") == {
        "pattern": "completed-with-errors-and-warnings",
    }
    assert parse_dbt_test_log("Got 1 results, configured to fail if ") == {
        "pattern": "configured-to-fail-if",
    }
    assert parse_dbt_test_log("compiled Code at ") == {
        "pattern": "compiled-code-at",
    }
    assert parse_dbt_test_log("Done. PASS=10 WARN=1 ERROR=0 SKIP=0 TOTAL=10") == {
        "pattern": "test-summary",
        "status": "failed",
        "passed": 10,
        "warnings": 1,
        "errors": 0,
        "skipped": 0,
    }
    assert parse_dbt_test_log("Done. PASS=10 WARN=0 ERROR=1 SKIP=0 TOTAL=10") == {
        "pattern": "test-summary",
        "status": "failed",
        "passed": 10,
        "warnings": 0,
        "errors": 1,
        "skipped": 0,
    }


def test_parse_dbt_docs_generate_log():
    """tests parse_dbt_docs_generate_log"""
    assert parse_dbt_docs_generate_log("1 of 1 START ") == {
        "pattern": "step-started",
        "status": "success",
    }
    assert parse_dbt_docs_generate_log(
        "Found 1 models, 1 tests, 1 sources, 1 exposures, 1 metrics, 1 macros, 1 groups, 1 semantic models"
    ) == {
        "pattern": "found-models-tests-sources-etc",
        "status": "success",
    }


def test_rename_task_name():
    """tests rename_task_name"""
    assert rename_task_name("wait_for_completion-0") == "airbyte sync"
    assert rename_task_name("gitpulljob-0") == "git pull"
    assert rename_task_name("dbtjob-0") == "dbt clean"
    assert rename_task_name("dbtjob-1") == "dbt deps"
    assert rename_task_name("dbtjob-2") == "dbt run"
    assert rename_task_name("dbtjob-3") == "dbt test"
    assert rename_task_name("dbtjob-4") == "dbt docs generate"


def test_parse_prefect_logs_1():
    """Tests parse_prefect_logs"""
    with patch("ddpui.utils.prefectlogs.fetch_logs_from_db", return_value=[]):
        result = parse_prefect_logs({}, "flow-run-id")
        assert not result


def test_parse_prefect_logs_2():
    """Tests parse_prefect_logs"""
    with patch(
        "ddpui.utils.prefectlogs.fetch_logs_from_db",
        return_value=[{"task_name": "trigger-0"}],
    ):
        result = parse_prefect_logs({}, "flow-run-id")
        assert not result


def test_parse_prefect_logs_3():
    """Tests parse_prefect_logs"""
    with patch(
        "ddpui.utils.prefectlogs.fetch_logs_from_db",
        return_value=[
            {
                "task_name": "airbyte sync",
                "message": "Job 23 succeeded",
                "state_name": "Completed",
            }
        ],
    ):
        result = parse_prefect_logs({}, "flow-run-id")
        assert result == [
            {
                "log_lines": ["Job 23 succeeded"],
                "pattern": "airbyte-sync-job-succeeded",
                "status": "success",
                "task_name": "airbyte sync",
            }
        ]
