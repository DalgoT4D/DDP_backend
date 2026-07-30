import os
import yaml
from pathlib import Path
from ninja.errors import HttpError
from unittest.mock import patch, Mock, mock_open, MagicMock, ANY
import pytest
from django.contrib.auth.models import User
from ddpui import settings
from ddpui.models.org import Org, OrgDbt, OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask, TaskType
from ddpui.ddpdbt.elementary_service import (
    elementary_setup_status,
    get_elementary_target_schema,
    get_elementary_package_version,
    check_dbt_files,
    extract_profile_from_generate_elementary_cli_profile,
    refresh_elementary_report_via_prefect,
    get_dbt_version,
    get_edr_version,
    ensure_edr_sendreport_dataflow,
    create_elementary_profile,
)
from ddpui.utils.constants import TASK_GENERATE_EDR
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE, DDP_WORK_QUEUE, EDR_WORK_QUEUE, DBTCLIPROFILE
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_dbt():
    """org dbt"""
    return OrgDbt.objects.create(
        project_dir="test-project-dir",
        target_type="tgt_type",
        default_schema="test-default_schema",
    )


@pytest.fixture
def org(org_dbt):
    """org with dbt"""
    queue_config = {
        "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "test_workpool"},
    }
    return Org.objects.create(slug="test-org", dbt=org_dbt, queue_config=queue_config)


@pytest.fixture
def authuser():
    """auth user"""
    return User.objects.create(email="fake-email", username="fake-username")


@pytest.fixture
def orguser(org, authuser):
    """org user"""
    return OrgUser.objects.create(org=org, user=authuser)


@pytest.fixture
def task():
    """task of type generate-edr"""
    edrtask = Task.objects.create(type=TaskType.EDR, slug=TASK_GENERATE_EDR, label="EDR generate")
    yield edrtask
    edrtask.delete()


@pytest.fixture
def orgtask(org, task):
    """org task of type generate-edr"""
    edrorgtask = OrgTask.objects.create(org=org, task=task)
    yield edrorgtask
    edrorgtask.delete()


@pytest.fixture
def edr_deployment_org():
    """org task of type generate-edr"""
    edrtask = Task.objects.create(type=TaskType.EDR, slug=TASK_GENERATE_EDR, label="EDR generate")
    dbt = OrgDbt.objects.create(
        project_dir="test-project-dir",
        target_type="tgt_type",
        default_schema="test-default_schema",
    )
    queue_config = {
        "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "test_workpool"},
    }
    org = Org.objects.create(slug="test-org", dbt=dbt, queue_config=queue_config)
    dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-name",
        deployment_name="deployment-name",
        deployment_id="deployment-id",
        dataflow_type="manual",
        cron="0 0 * * *",
    )
    edrorgtask = OrgTask.objects.create(org=org, task=edrtask)
    dfot = DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=edrorgtask)
    yield org
    dfot.delete()
    edrorgtask.delete()
    dataflow.delete()


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager")
def test_elementary_setup_status_success(dbt_project_manager, edr_deployment_org):
    """tests elementary_setup_status"""
    dbt_project_manager.get_dbt_project_dir = Mock(return_value=Path("test-project-dir"))
    with patch("ddpui.ddpdbt.elementary_service.os.path.exists", return_value=True):
        result = elementary_setup_status(edr_deployment_org)

        dbt_project_manager.get_dbt_project_dir.assert_called_once_with(edr_deployment_org.dbt)

        assert result == {"status": "set-up"}


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager")
@patch("ddpui.ddpdbt.elementary_service.os.path.exists")
def test_elementary_setup_status_no_edr_deployment_found(
    mock_os_path_exists, dbt_project_manager, org
):
    """tests elementary_setup_status"""
    dbt_project_manager.get_dbt_project_dir = Mock(return_value="test-project-dir")
    mock_os_path_exists.return_value = True

    result = elementary_setup_status(org)
    assert result == {"status": "not-set-up"}

    dbt_project_manager.get_dbt_project_dir.assert_called_once_with(org.dbt)
    mock_os_path_exists.assert_called_once_with(
        Path("test-project-dir/elementary_profiles/profiles.yml")
    )


def test_elementary_setup_status_no_dbt(org):
    """tests elementary_setup_status when dbt is not configured"""
    org.dbt = None
    result = elementary_setup_status(org)
    assert result == {"error": "dbt is not configured for this client"}


def test_get_elementary_target_schema_schema():
    """tests get_elementary_target_schema"""
    dbt_project_content = """
    models:
      elementary:
        schema: elementary
    """
    with patch("builtins.open", mock_open(read_data=dbt_project_content)):
        result = get_elementary_target_schema("dbt_project.yml")
        assert result == {"schema": "elementary"}


def test_get_elementary_target_schema_plus_schema():
    """tests get_elementary_target_schema"""
    dbt_project_content = """
    models:
      elementary:
        +schema: elementary
    """
    with patch("builtins.open", mock_open(read_data=dbt_project_content)):
        result = get_elementary_target_schema("dbt_project.yml")
        assert result == {"+schema": "elementary"}


def test_get_elementary_target_schema_no_elementary():
    """tests get_elementary_target_schema"""
    dbt_project_content = """
    models:
      not_elementary:
        schema: not_elementary
    """
    with patch("builtins.open", mock_open(read_data=dbt_project_content)):
        result = get_elementary_target_schema("dbt_project.yml")
        assert result is None


def test_get_elementary_target_schema_no_schema():
    """tests get_elementary_target_schema"""
    dbt_project_content = """
    models:
      elementary:
        other_key: other_value
    """
    with patch("builtins.open", mock_open(read_data=dbt_project_content)):
        result = get_elementary_target_schema("dbt_project.yml")
        assert result is None


def test_get_elementary_package_version_found():
    """tests get_elementary_package_version"""
    packages_content = """
    packages:
      - package: elementary-data/elementary
        version: 0.15.2
    """
    with patch("builtins.open", mock_open(read_data=packages_content)):
        result = get_elementary_package_version("packages.yml")
        assert result == {"package": "elementary-data/elementary", "version": "0.15.2"}


def test_get_elementary_package_version_not_found():
    """tests get_elementary_package_version"""
    packages_content = """
    packages:
      - package: other-package
        version: 1.0.0
    """
    with patch("builtins.open", mock_open(read_data=packages_content)):
        result = get_elementary_package_version("packages.yml")
        assert result is None


def test_get_elementary_package_version_no_packages_key():
    """tests get_elementary_package_version"""
    packages_content = """
    other_key:
      - package: elementary-data/elementary
        version: 0.15.2
    """
    with patch("builtins.open", mock_open(read_data=packages_content)):
        result = get_elementary_package_version("packages.yml")
        assert result is None


def test_get_elementary_package_version_empty_file():
    """tests get_elementary_package_version"""
    packages_content = ""
    with patch("builtins.open", mock_open(read_data=packages_content)):
        result = get_elementary_package_version("packages.yml")
        assert result is None


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_missing_packages_yml(
    mock_path,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = True
    mock_packages_yml.exists.return_value = False

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == ("packages.yml" if settings.DEBUG else "packages.yml not found", None)


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_missing_dbt_project_yml(
    mock_path,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = False
    mock_packages_yml.exists.return_value = True

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == ("dbt_project.yml" if settings.DEBUG else "dbt_project.yml not found", None)


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_package_version")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_target_schema")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_missing_elementary_package_missing_target_schema(
    mock_path,
    mock_get_elementary_target_schema,
    mock_get_elementary_package_version,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = True
    mock_packages_yml.exists.return_value = True

    mock_get_elementary_target_schema.return_value = None
    mock_get_elementary_package_version.return_value = None

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == (
        None,
        {
            "exists": {},
            "missing": {
                "elementary_package": ANY,
                "elementary_target_schema": ANY,
            },
        },
    )


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_package_version")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_target_schema")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_have_elementary_package_missing_target_schema(
    mock_path,
    mock_get_elementary_target_schema,
    mock_get_elementary_package_version,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = True
    mock_packages_yml.exists.return_value = True

    mock_get_elementary_target_schema.return_value = None
    mock_get_elementary_package_version.return_value = {
        "package": "elementary-data/elementary",
        "version": "0.19.1",
    }

    # Set environment variable to control the expected upgrade version
    os.environ["LATEST_ELEMENTARY_PACKAGE_VERSION"] = "0.20.0"

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == (
        None,
        {
            "exists": {
                "elementary_package": {
                    "package": "elementary-data/elementary",
                    "version": "0.19.1",
                    "needs_upgrade": "0.20.0",
                },
            },
            "missing": {
                "elementary_target_schema": ANY,
            },
        },
    )

    # Clean up environment variable
    del os.environ["LATEST_ELEMENTARY_PACKAGE_VERSION"]


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_package_version")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_target_schema")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_needs_upgrade(
    mock_path,
    mock_get_elementary_target_schema,
    mock_get_elementary_package_version,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = True
    mock_packages_yml.exists.return_value = True

    mock_get_elementary_target_schema.return_value = None
    mock_get_elementary_package_version.return_value = {
        "package": "elementary-data/elementary",
        "version": "0.19.1",
    }
    os.environ["LATEST_ELEMENTARY_PACKAGE_VERSION"] = "0.20.1"
    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == (
        None,
        {
            "exists": {
                "elementary_package": {
                    "package": "elementary-data/elementary",
                    "version": "0.19.1",
                    "needs_upgrade": "0.20.1",
                },
            },
            "missing": {
                "elementary_target_schema": ANY,
            },
        },
    )

    del os.environ["LATEST_ELEMENTARY_PACKAGE_VERSION"]


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_package_version")
@patch("ddpui.ddpdbt.elementary_service.get_elementary_target_schema")
@patch("ddpui.ddpdbt.elementary_service.Path")
def test_check_dbt_files_missing_elementary_package_have_target_schema(
    mock_path,
    mock_get_elementary_target_schema,
    mock_get_elementary_package_version,
    mock_gather_dbt_project_params,
    org,
):
    """tests check_dbt_files"""
    mock_gather_dbt_project_params.retval = Mock(project_dir="test-project-dir")

    mock_dbt_project_yml = MagicMock()
    mock_dbt_project_yml.__str__.return_value = "dbt_project.yml"
    mock_packages_yml = MagicMock()
    mock_packages_yml.__str__.return_value = "packages.yml"

    # Configure the mock to handle the "/" operator
    mock_path.return_value.__truediv__.side_effect = lambda other: (
        mock_dbt_project_yml if other == "dbt_project.yml" else mock_packages_yml
    )

    # Configure the mock to handle the exists() method
    mock_dbt_project_yml.exists.return_value = True
    mock_packages_yml.exists.return_value = True

    mock_get_elementary_target_schema.return_value = {"+schema": "elementary"}
    mock_get_elementary_package_version.return_value = None

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == (
        None,
        {
            "exists": {
                "elementary_target_schema": {"+schema": "elementary"},
            },
            "missing": {
                "elementary_package": ANY,
            },
        },
    )


def test_extract_profile_from_generate_elementary_cli_profile_failure():
    """tests extract_profile_from_generate_elementary_cli_profile"""
    profile = """
bad_key:
  target: test-target
  schema: test-schema
  table: test-table
  columns: 
    - col1
    - col2
""".split(
        "\n"
    )

    error, _ = extract_profile_from_generate_elementary_cli_profile(profile)
    assert error == {"error": "macro elementary.generate_elementary_cli_profile returned nothing"}


def test_extract_profile_from_generate_elementary_cli_profile_success():
    """tests extract_profile_from_generate_elementary_cli_profile"""
    profile = """
elementary:
  target: test-target
  schema: test-schema
  table: test-table
  columns: 
    - col1
    - col2
""".split(
        "\n"
    )

    _, result = extract_profile_from_generate_elementary_cli_profile(profile)
    assert result == {
        "elementary": {
            "target": "test-target",
            "schema": "test-schema",
            "table": "test-table",
            "columns": ["col1", "col2"],
        }
    }


def test_extract_profile_strips_ansi_codes():
    """ANSI escape codes in dbt output must be stripped before YAML parsing."""
    # dbt emits colour codes around log lines; the profile itself may have them too
    lines = [
        "\x1b[0mRunning with dbt=1.7.0\x1b[0m",
        "\x1b[32melementary:\x1b[0m",
        "\x1b[0m  target: default\x1b[0m",
        "\x1b[0m  outputs:\x1b[0m",
        "\x1b[0m    default:\x1b[0m",
        "\x1b[0m      type: postgres\x1b[0m",
        "",
    ]
    _, result = extract_profile_from_generate_elementary_cli_profile(lines)
    assert result == {
        "elementary": {
            "target": "default",
            "outputs": {"default": {"type": "postgres"}},
        }
    }


def test_extract_profile_stops_at_trailing_dbt_log_line():
    """A non-indented line after the YAML block (dbt warning/log) must not be
    included in the buffer — it would break YAML parsing."""
    lines = [
        "elementary:",
        "  target: default",
        "  outputs:",
        "    default:",
        "      type: postgres",
        # dbt sometimes prints a warning after the macro output
        "Some dbt warning that is not indented",
        "another dbt line",
    ]
    _, result = extract_profile_from_generate_elementary_cli_profile(lines)
    assert result == {
        "elementary": {
            "target": "default",
            "outputs": {"default": {"type": "postgres"}},
        }
    }


@patch("ddpui.ddpdbt.elementary_service.prefect_service.lock_tasks_for_deployment")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.create_deployment_flow_run")
def test_refresh_elementary_report_via_prefect(
    mock_create_deployment_flow_run, mock_lock_tasks_for_deployment, orguser, orgtask
):
    """tests refresh_elementary_report_via_prefect"""
    odf = OrgDataFlowv1.objects.create(
        org=orguser.org,
        name="test-name",
        deployment_name="test-name",
        deployment_id="test-deployment-id",
        dataflow_type="manual",  # we dont want it to show in flows/pipelines page
        cron="0 0 * * *",
    )

    mock_lock_tasks_for_deployment.return_value = []
    mock_create_deployment_flow_run.return_value = {
        "flow_run_id": "fake-flow-run-id",
        "name": "fake-name",
    }

    DataflowOrgTask.objects.create(orgtask=orgtask, dataflow=odf)

    response = refresh_elementary_report_via_prefect(orguser)
    assert response == {
        "flow_run_id": "fake-flow-run-id",
        "name": "fake-name",
    }

    mock_lock_tasks_for_deployment.assert_called_once_with("test-deployment-id", orguser)
    mock_create_deployment_flow_run.assert_called_once_with(odf.deployment_id)

    odf.delete()


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_get_dbt_version_success(mock_check_output, mock_gather_dbt_project_params, org):
    """tests get_dbt_version"""
    mock_gather_dbt_project_params.return_value = Mock(dbt_binary="test-binary")
    mock_check_output.return_value = "line1\nline2\ninstalled: 0.19.0\nline4"

    response = get_dbt_version(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)
    mock_check_output.assert_called_once_with(["test-binary", "--version"], text=True)

    assert response == "0.19.0"


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_get_dbt_version_failure(mock_check_output, mock_gather_dbt_project_params, org):
    """tests get_dbt_version"""
    mock_gather_dbt_project_params.return_value = Mock(dbt_binary="test-binary")
    mock_check_output.return_value = "line1\nline2\nline3\nline4"

    response = get_dbt_version(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)
    mock_check_output.assert_called_once_with(["test-binary", "--version"], text=True)

    assert response == "Not available"


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_get_edr_version_failure(mock_check_output, mock_gather_dbt_project_params, org):
    """tests get_edr_version"""
    mock_gather_dbt_project_params.return_value = Mock(venv_binary="venv/bin")
    mock_check_output.return_value = "line1\nline2\nline3\nline4"

    response = get_edr_version(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    mock_check_output.assert_called_once_with(["venv/bin/edr", "--version"], text=True)

    assert response == "Not available"


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_get_edr_version_success(mock_check_output, mock_gather_dbt_project_params, org):
    """tests get_edr_version"""
    mock_gather_dbt_project_params.return_value = Mock(venv_binary="venv/bin")
    mock_check_output.return_value = "line1\nline2\nElementary version is 1.\nline4"

    response = get_edr_version(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    mock_check_output.assert_called_once_with(["venv/bin/edr", "--version"], text=True)

    assert response == "1"


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.setup_edr_send_report_task_config")
@patch("ddpui.ddpdbt.elementary_service.generate_hash_id")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.create_dataflow_v1")
@patch("ddpui.core.orgtaskfunctions.get_edr_send_report_task")
def test_ensure_edr_sendreport_dataflow(
    mock_get_edr_send_report_task,
    mock_create_dataflow_v1,
    mock_generate_hash_id,
    mock_setup_edr_send_report_task_config,
    mock_gather_dbt_project_params,
    org,
    orgtask,
):
    """tests ensure_edr_sendreport_dataflow"""
    os.environ["PREFECT_WORKER_POOL_NAME"] = "test_workpool"
    cron = "0 0 * * *"

    mock_gather_dbt_project_params.return_value = Mock(
        venv_binary="venv/bin", project_dir="project-dir"
    )
    mock_get_edr_send_report_task.return_value = orgtask
    mock_setup_edr_send_report_task_config.return_value = Mock(
        to_json=Mock(return_value={"task": "config"})
    )
    mock_generate_hash_id.return_value = "hashcode"

    deployment_name = f"pipeline-{org.slug}-generate-edr-hashcode"

    mock_create_dataflow_v1.return_value = {
        "deployment": {
            "name": deployment_name,
            "id": "deployment-id",
        }
    }

    ensure_edr_sendreport_dataflow(org, cron)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)
    mock_setup_edr_send_report_task_config.assert_called_once_with(orgtask, "project-dir", seq=0)
    mock_generate_hash_id.assert_called_once_with(8)

    # The create_dataflow_v1 should be called with the org's edr_queue config
    expected_call_args = mock_create_dataflow_v1.call_args

    # Check the first argument (PrefectDataFlowCreateSchema3)
    assert expected_call_args[0][0].deployment_name == deployment_name
    assert expected_call_args[0][0].flow_name == deployment_name
    assert expected_call_args[0][0].orgslug == org.slug
    assert expected_call_args[0][0].deployment_params == {
        "config": {
            "tasks": [{"task": "config"}],
            "org_slug": orgtask.org.slug,
        }
    }
    assert expected_call_args[0][0].cron == cron

    # Check the second argument (QueueDetailsSchema)
    queue_details = expected_call_args[0][1]
    assert hasattr(queue_details, "name")
    assert hasattr(queue_details, "workpool")
    assert queue_details.name == EDR_WORK_QUEUE
    assert queue_details.workpool == "test_workpool"


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.setup_edr_send_report_task_config")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.update_dataflow_v1")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.create_dataflow_v1")
@patch("ddpui.core.orgtaskfunctions.get_edr_send_report_task")
def test_ensure_edr_sendreport_dataflow_updates_existing(
    mock_get_edr_send_report_task,
    mock_create_dataflow_v1,
    mock_update_dataflow_v1,
    mock_setup_edr_send_report_task_config,
    mock_gather_dbt_project_params,
    edr_deployment_org,
):
    """when a deployment already exists the command updates it instead of skipping"""
    os.environ["PREFECT_WORKER_POOL_NAME"] = "test_workpool"
    cron = "0 6 * * *"

    orgtask = OrgTask.objects.filter(org=edr_deployment_org, task__slug=TASK_GENERATE_EDR).first()
    mock_get_edr_send_report_task.return_value = orgtask
    mock_gather_dbt_project_params.return_value = Mock(
        venv_binary="venv/bin", project_dir="project-dir"
    )
    mock_setup_edr_send_report_task_config.return_value = Mock(
        to_json=Mock(return_value={"task": "config"})
    )

    result = ensure_edr_sendreport_dataflow(edr_deployment_org, cron)

    assert result["status"] == "success"
    assert result.get("updated") is True

    # update was called, create was not
    mock_update_dataflow_v1.assert_called_once()
    mock_create_dataflow_v1.assert_not_called()

    # the deployment_params passed to update contains the edr task config
    update_payload = mock_update_dataflow_v1.call_args[0][1]
    assert update_payload.cron == cron
    assert update_payload.deployment_params["config"]["tasks"] == [{"task": "config"}]


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.setup_edr_send_report_task_config")
@patch("ddpui.ddpdbt.elementary_service.setup_git_clone_shell_task_config")
@patch("ddpui.ddpdbt.elementary_service.generate_hash_id")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.create_dataflow_v1")
@patch("ddpui.core.orgtaskfunctions.get_edr_send_report_task")
def test_ensure_edr_sendreport_dataflow_eks(
    mock_get_edr_send_report_task,
    mock_create_dataflow_v1,
    mock_generate_hash_id,
    mock_setup_git_clone,
    mock_setup_edr_send_report_task_config,
    mock_gather_dbt_project_params,
    org,
    orgtask,
):
    """on EKS (is_workpool_eks=True on edr_queue) a git-clone task is prepended"""
    import json as _json

    os.environ["PREFECT_WORKER_POOL_NAME"] = "test_workpool"
    os.environ["PREFECT_EKS_WORKER_POOL_NAME"] = "eks_workpool"

    # Ensure git-clone Task exists in DB
    from ddpui.utils.constants import TASK_GITCLONE

    git_clone_task, _ = Task.objects.get_or_create(
        slug=TASK_GITCLONE,
        defaults={"type": TaskType.GIT, "label": "git clone"},
    )

    cron = "0 0 * * *"
    mock_gather_dbt_project_params.return_value = Mock(
        venv_binary="venv/bin",
        project_dir="project-dir",
        clients_base_dir="/mnt/clientdbts",
        project_dir_relative="org/dbtrepo",
    )
    mock_get_edr_send_report_task.return_value = orgtask
    mock_setup_git_clone.return_value = Mock(to_json=Mock(return_value={"slug": "git-clone"}))
    mock_setup_edr_send_report_task_config.return_value = Mock(
        to_json=Mock(return_value={"slug": "generate-edr"})
    )
    mock_generate_hash_id.return_value = "hashcode"

    deployment_name = f"pipeline-{org.slug}-generate-edr-hashcode"
    mock_create_dataflow_v1.return_value = {
        "deployment": {"name": deployment_name, "id": "deployment-id"}
    }

    result = ensure_edr_sendreport_dataflow(org, cron)

    assert result["status"] == "success"

    # git-clone config is built
    mock_setup_git_clone.assert_called_once()

    # edr config is built with seq=1 (after git-clone at seq=0)
    mock_setup_edr_send_report_task_config.assert_called_once_with(orgtask, "project-dir", seq=1)

    # both tasks are present in the deployment params
    call_args = mock_create_dataflow_v1.call_args
    tasks = call_args[0][0].deployment_params["config"]["tasks"]
    assert len(tasks) == 2
    assert tasks[0]["slug"] == "git-clone"
    assert tasks[1]["slug"] == "generate-edr"

    # cleanup
    del os.environ["PREFECT_EKS_WORKER_POOL_NAME"]


def test_create_elementary_profile_no_dbt(org):
    """tests create_elementary_profile when dbt is not configured"""
    org.dbt = None

    result = create_elementary_profile(org)
    assert result == {"error": "dbt is not configured for this client"}


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_create_elementary_profile_with_existing_profiles_yml(
    mock_subprocess, mock_gather_params, org, tmp_path
):
    """tests create_elementary_profile when profiles.yml exists on disk"""
    # Create temporary directories and files
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    profiles_dir = project_dir / "profiles"
    profiles_dir.mkdir()
    profiles_file = profiles_dir / "profiles.yml"

    # Write existing profiles.yml
    dbt_profile_content = {
        "test_profile": {"outputs": {"test-target": {"schema": "test_schema", "host": "localhost"}}}
    }
    with open(profiles_file, "w") as f:
        yaml.safe_dump(dbt_profile_content, f)

    # Create dbt_project.yml
    dbt_project_file = project_dir / "dbt_project.yml"
    dbt_project_content = {"name": "test_project", "version": "1.0.0", "profile": "test_profile"}
    with open(dbt_project_file, "w") as f:
        yaml.safe_dump(dbt_project_content, f)

    # Setup mocks
    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt", target="test-target"
    )
    mock_subprocess.return_value = """elementary:
  target: test-target
  outputs:
    test-target:
      type: postgres
      schema: elementary_schema"""

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    mock_subprocess.assert_called_once()

    # Verify elementary profile was created
    elementary_dir = project_dir / "elementary_profiles"
    assert elementary_dir.exists()
    elementary_file = elementary_dir / "profiles.yml"
    assert elementary_file.exists()


@patch("ddpui.ddpdbt.elementary_service.write_dbt_profiles_yml")
@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_create_elementary_profile_missing_profiles_yml_calls_write_dbt_profiles_yml(
    mock_subprocess, mock_gather_params, mock_write_dbt_profiles_yml, org, tmp_path
):
    """If profiles.yml is missing on disk, create_elementary_profile calls
    write_dbt_profiles_yml(org) to generate it from warehouse creds (no more
    CLI-block fetch fallback)."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    dbt_project_file = project_dir / "dbt_project.yml"
    with open(dbt_project_file, "w") as f:
        yaml.safe_dump({"name": "test_project", "profile": "test_profile"}, f)

    # write_dbt_profiles_yml is mocked but we still need profiles.yml on disk
    # after it's called — the code reads it right after. Simulate that side effect.
    profiles_dir = project_dir / "profiles"
    profiles_dir.mkdir()

    def _write_profile_file(_org):
        with open(profiles_dir / "profiles.yml", "w") as f:
            yaml.safe_dump(
                {
                    "test_profile": {
                        "target": "default",
                        "outputs": {
                            "default": {"type": "postgres", "host": "h", "schema": "analytics"}
                        },
                    }
                },
                f,
            )

    mock_write_dbt_profiles_yml.side_effect = _write_profile_file
    mock_gather_params.return_value = Mock(project_dir=str(project_dir), dbt_binary="test-dbt")
    mock_subprocess.return_value = """elementary:
  target: default
  outputs:
    default:
      type: postgres
      schema: elementary_schema"""

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    mock_write_dbt_profiles_yml.assert_called_once_with(org)


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_create_elementary_profile_elementary_dir_already_exists(
    mock_subprocess, mock_gather_params, org, tmp_path
):
    """tests create_elementary_profile when elementary_profiles directory already exists"""
    # Create temporary directories and files
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    profiles_dir = project_dir / "profiles"
    profiles_dir.mkdir()
    profiles_file = profiles_dir / "profiles.yml"

    # Create elementary_profiles directory that already exists
    elementary_dir = project_dir / "elementary_profiles"
    elementary_dir.mkdir()

    # Write existing profiles.yml
    dbt_profile_content = {
        "test_profile": {"outputs": {"test-target": {"schema": "test_schema", "host": "localhost"}}}
    }
    with open(profiles_file, "w") as f:
        yaml.safe_dump(dbt_profile_content, f)

    # Create dbt_project.yml
    dbt_project_file = project_dir / "dbt_project.yml"
    dbt_project_content = {"name": "test_project", "version": "1.0.0", "profile": "test_profile"}
    with open(dbt_project_file, "w") as f:
        yaml.safe_dump(dbt_project_content, f)

    # Setup mocks
    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt", target="test-target"
    )
    mock_subprocess.return_value = """elementary:
  target: test-target
  outputs:
    test-target:
      type: postgres
      schema: elementary_schema"""

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    # Verify elementary profile was still created (overwrites existing)
    elementary_file = elementary_dir / "profiles.yml"
    assert elementary_file.exists()


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
def test_create_elementary_profile_macro_target_mismatch(
    mock_subprocess, mock_gather_params, org, tmp_path
):
    """When the elementary macro emits target='default' but the dbt profile on
    disk uses a custom target name, create_elementary_profile must still read
    warehouse creds from the dbt profile's configured target — not blindly use
    the macro's target as the key into dbt outputs (which caused KeyError: 'default')."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    profiles_dir = project_dir / "profiles"
    profiles_dir.mkdir()

    # dbt profile uses a custom target name, NOT "default"
    dbt_profile_content = {
        "test_profile": {
            "target": "custom_target",
            "outputs": {
                "custom_target": {
                    "type": "postgres",
                    "host": "db.example.com",
                    "schema": "analytics",
                }
            },
        }
    }
    with open(profiles_dir / "profiles.yml", "w") as f:
        yaml.safe_dump(dbt_profile_content, f)

    dbt_project_file = project_dir / "dbt_project.yml"
    with open(dbt_project_file, "w") as f:
        yaml.safe_dump({"name": "test_project", "profile": "test_profile"}, f)

    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt"
    )
    # elementary macro emits target: default — does NOT match the dbt profile
    mock_subprocess.return_value = """elementary:
  target: default
  outputs:
    default:
      type: postgres
      schema: elementary_schema"""

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    # The written elementary profile must carry the creds from the dbt profile
    elementary_file = project_dir / "elementary_profiles" / "profiles.yml"
    assert elementary_file.exists()
    written = yaml.safe_load(elementary_file.read_text())
    output = written["elementary"]["outputs"]["default"]
    assert output["host"] == "db.example.com"
    assert output["schema"] == "elementary_schema"


# ==================== install_elementary celery task tests ====================


@patch("ddpui.celeryworkers.tasks.ensure_edr_sendreport_dataflow")
@patch("ddpui.celeryworkers.tasks.run_dbt_commands")
@patch("ddpui.celeryworkers.tasks.create_elementary_profile")
@patch("ddpui.celeryworkers.tasks.TaskProgress")
def test_install_elementary_happy_path(
    mock_task_progress_cls,
    mock_create_profile,
    mock_run_dbt_commands,
    mock_ensure_edr,
    org,
):
    """all three sub-steps succeed; each emits a running+completed pair to
    TaskProgress under (stepIndex, step, status)."""
    from ddpui.celeryworkers.tasks import install_elementary

    mock_progress = Mock()
    mock_task_progress_cls.return_value = mock_progress
    mock_create_profile.return_value = {"status": "success"}
    mock_run_dbt_commands.apply.return_value = Mock(maybe_throw=Mock(return_value=None))
    mock_ensure_edr.return_value = {"status": "success"}

    install_elementary(org.id, "task-1", "install-elementary-test-org")

    # 3 steps × 2 statuses (running + completed) = 6 emits, no failed
    assert mock_progress.add.call_count == 6
    emitted = [call.args[0] for call in mock_progress.add.call_args_list]
    for i in range(3):
        assert emitted[i * 2]["stepIndex"] == i
        assert emitted[i * 2]["status"] == "running"
        assert emitted[i * 2 + 1]["stepIndex"] == i
        assert emitted[i * 2 + 1]["status"] == "completed"

    mock_create_profile.assert_called_once_with(org)
    mock_run_dbt_commands.apply.assert_called_once()
    mock_ensure_edr.assert_called_once_with(org, "0 0 * * *")


@patch("ddpui.celeryworkers.tasks.ensure_edr_sendreport_dataflow")
@patch("ddpui.celeryworkers.tasks.run_dbt_commands")
@patch("ddpui.celeryworkers.tasks.create_elementary_profile")
@patch("ddpui.celeryworkers.tasks.TaskProgress")
def test_install_elementary_step0_failure(
    mock_task_progress_cls,
    mock_create_profile,
    mock_run_dbt_commands,
    mock_ensure_edr,
    org,
):
    """step 0 (create profile) fails: emit failed for step 0, don't touch step 1 or 2, re-raise."""
    from ddpui.celeryworkers.tasks import install_elementary

    mock_progress = Mock()
    mock_task_progress_cls.return_value = mock_progress
    mock_create_profile.return_value = {"error": "profile blew up"}

    with pytest.raises(Exception, match="profile blew up"):
        install_elementary(org.id, "task-1", "install-elementary-test-org")

    mock_run_dbt_commands.apply.assert_not_called()
    mock_ensure_edr.assert_not_called()
    # last emit is failed for step 0
    last_emit = mock_progress.add.call_args_list[-1].args[0]
    assert last_emit["stepIndex"] == 0
    assert last_emit["status"] == "failed"
    assert last_emit["message"] == "profile blew up"


@patch("ddpui.celeryworkers.tasks.ensure_edr_sendreport_dataflow")
@patch("ddpui.celeryworkers.tasks.run_dbt_commands")
@patch("ddpui.celeryworkers.tasks.create_elementary_profile")
@patch("ddpui.celeryworkers.tasks.TaskProgress")
def test_install_elementary_step1_failure(
    mock_task_progress_cls,
    mock_create_profile,
    mock_run_dbt_commands,
    mock_ensure_edr,
    org,
):
    """step 1 (dbt commands) fails: profile completed, ensure_edr not called, failed emitted for step 1."""
    from ddpui.celeryworkers.tasks import install_elementary

    mock_progress = Mock()
    mock_task_progress_cls.return_value = mock_progress
    mock_create_profile.return_value = {"status": "success"}
    mock_run_dbt_commands.apply.return_value = Mock(
        maybe_throw=Mock(side_effect=Exception("dbt exploded"))
    )

    with pytest.raises(Exception, match="dbt exploded"):
        install_elementary(org.id, "task-1", "install-elementary-test-org")

    mock_ensure_edr.assert_not_called()
    last_emit = mock_progress.add.call_args_list[-1].args[0]
    assert last_emit["stepIndex"] == 1
    assert last_emit["status"] == "failed"


@patch("ddpui.celeryworkers.tasks.ensure_edr_sendreport_dataflow")
@patch("ddpui.celeryworkers.tasks.run_dbt_commands")
@patch("ddpui.celeryworkers.tasks.create_elementary_profile")
@patch("ddpui.celeryworkers.tasks.TaskProgress")
def test_install_elementary_step2_failure(
    mock_task_progress_cls,
    mock_create_profile,
    mock_run_dbt_commands,
    mock_ensure_edr,
    org,
):
    """step 2 (schedule reports) fails: first two completed, failed emitted for step 2."""
    from ddpui.celeryworkers.tasks import install_elementary

    mock_progress = Mock()
    mock_task_progress_cls.return_value = mock_progress
    mock_create_profile.return_value = {"status": "success"}
    mock_run_dbt_commands.apply.return_value = Mock(maybe_throw=Mock(return_value=None))
    mock_ensure_edr.return_value = {"error": "edr scheduling failed"}

    with pytest.raises(Exception, match="edr scheduling failed"):
        install_elementary(org.id, "task-1", "install-elementary-test-org")

    last_emit = mock_progress.add.call_args_list[-1].args[0]
    assert last_emit["stepIndex"] == 2
    assert last_emit["status"] == "failed"


# ==================== run_dbt_commands failure-propagation ====================


@patch("ddpui.celeryworkers.tasks.write_dbt_profiles_yml")
@patch("ddpui.celeryworkers.tasks.DbtProjectManager")
@patch("ddpui.celeryworkers.tasks.TaskProgress")
def test_run_dbt_commands_propagates_inner_failure(
    mock_task_progress_cls,
    mock_dbt_project_manager,
    mock_write_dbt_profiles_yml,
    org,
):
    """When an inner step of run_dbt_commands fails, the celery task must end
    in FAILURE state so .apply().maybe_throw() re-raises for callers like
    install_elementary. Previously the outer `except Exception` swallowed the
    error, so the task ended in SUCCESS state and install_elementary proceeded
    to schedule EDR reports even though the elementary dbt install had failed.

    Exercises the real run_dbt_commands via .apply() (not a mocked boundary) so
    any regression to swallowing behavior is caught."""
    from ddpui.celeryworkers.tasks import run_dbt_commands

    mock_task_progress_cls.return_value = Mock()
    mock_dbt_project_manager.gather_dbt_project_params.return_value = Mock()
    mock_write_dbt_profiles_yml.side_effect = Exception("warehouse not found for org")

    result = run_dbt_commands.apply(args=[org.id, org.dbt.id, "task-id", None])

    with pytest.raises(Exception, match="warehouse not found for org"):
        result.maybe_throw()
