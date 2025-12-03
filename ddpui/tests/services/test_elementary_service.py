import os
import yaml
from pathlib import Path
from unittest.mock import patch, Mock, mock_open, MagicMock, ANY
import pytest
from django.contrib.auth.models import User
from ddpui import settings
from ddpui.models.org import Org, OrgDbt, OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask, TaskProgressHashPrefix
from ddpui.ddpdbt.elementary_service import (
    elementary_setup_status,
    get_elementary_target_schema,
    get_elementary_package_version,
    check_dbt_files,
    create_elementary_tracking_tables,
    extract_profile_from_generate_elementary_cli_profile,
    refresh_elementary_report_via_prefect,
    get_dbt_version,
    get_edr_version,
    create_edr_sendreport_dataflow,
    create_elementary_profile,
)
from ddpui.utils.constants import TASK_GENERATE_EDR, DBTCLIPROFILE
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.core.exceptions import HttpError

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
    return Org.objects.create(slug="test-org", dbt=org_dbt)


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
    edrtask = Task.objects.create(type="edr", slug=TASK_GENERATE_EDR, label="EDR generate")
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
    edrtask = Task.objects.create(type="edr", slug=TASK_GENERATE_EDR, label="EDR generate")
    dbt = OrgDbt.objects.create(
        project_dir="test-project-dir",
        target_type="tgt_type",
        default_schema="test-default_schema",
    )
    org = Org.objects.create(slug="test-org", dbt=dbt)
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

    response = check_dbt_files(org)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)

    assert response == (
        None,
        {
            "exists": {
                "elementary_package": {
                    "package": "elementary-data/elementary",
                    "version": "0.19.1",
                },
            },
            "missing": {
                "elementary_target_schema": ANY,
            },
        },
    )


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


@patch("ddpui.ddpdbt.elementary_service.TaskProgress")
@patch("ddpui.ddpdbt.elementary_service.uuid4")
@patch("ddpui.ddpdbt.elementary_service.run_dbt_commands")
def test_create_elementary_tracking_tables(
    mock_run_dbt_commands, mock_uuid4, mock_task_progress, org
):
    """tests create_elementary_tracking_tables"""
    mock_task_progress.return_value = Mock(add=Mock())
    mock_uuid4.return_value = "test-uuid"
    mock_run_dbt_commands.delay = Mock()

    response = create_elementary_tracking_tables(org)
    assert response == {
        "task_id": "test-uuid",
        "hashkey": f"{TaskProgressHashPrefix.RUNDBTCMDS.value}-test-org",
    }

    mock_task_progress.assert_called_once_with("test-uuid", "run-dbt-commands-" + org.slug)
    mock_run_dbt_commands.delay.assert_called_once_with(
        org.id,
        "test-uuid",
        {
            # run parameters
            "options": {
                "select": "elementary",
            }
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
def test_create_edr_sendreport_dataflow(
    mock_create_dataflow_v1,
    mock_generate_hash_id,
    mock_setup_edr_send_report_task_config,
    mock_gather_dbt_project_params,
    org,
    orgtask,
):
    """tests create_edr_sendreport_dataflow"""
    cron = "0 0 * * *"

    mock_gather_dbt_project_params.return_value = Mock(
        venv_binary="venv/bin", project_dir="project-dir"
    )
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

    create_edr_sendreport_dataflow(org, orgtask, cron)

    mock_gather_dbt_project_params.assert_called_once_with(org, org.dbt)
    mock_setup_edr_send_report_task_config.assert_called_once_with(
        orgtask, "project-dir", "venv/bin"
    )
    mock_generate_hash_id.assert_called_once_with(8)

    mock_create_dataflow_v1.assert_called_once_with(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org.slug,
            deployment_params={
                "config": {
                    "tasks": [{"task": "config"}],
                    "org_slug": orgtask.org.slug,
                }
            },
            cron=cron,
        ),
        MANUL_DBT_WORK_QUEUE,
    )


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

    # Setup mocks
    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt", target="test-target"
    )
    mock_subprocess.return_value = "elementary:\n  schema: elementary_schema"

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    mock_subprocess.assert_called_once()

    # Verify elementary profile was created
    elementary_dir = project_dir / "elementary_profiles"
    assert elementary_dir.exists()
    elementary_file = elementary_dir / "profiles.yml"
    assert elementary_file.exists()


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.ddpdbt.elementary_service.subprocess.check_output")
@patch("ddpui.ddpdbt.elementary_service.prefect_service.get_dbt_cli_profile_block")
def test_create_elementary_profile_without_profiles_yml_fetch_from_prefect(
    mock_get_block, mock_subprocess, mock_gather_params, org, tmp_path
):
    """tests create_elementary_profile when profiles.yml doesn't exist, fetches from Prefect blocks"""
    # Create Prefect block
    OrgPrefectBlockv1.objects.create(
        org=org, block_type=DBTCLIPROFILE, block_name="test-cli-profile"
    )

    # Create temporary project directory (no profiles.yml)
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    # Setup mocks
    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt", target="test-target"
    )
    mock_get_block.return_value = {
        "profile": {
            "test_profile": {
                "outputs": {"test-target": {"schema": "test_schema", "host": "localhost"}}
            }
        }
    }
    mock_subprocess.return_value = "elementary:\n  schema: elementary_schema"

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    mock_get_block.assert_called_once_with("test-cli-profile")
    mock_subprocess.assert_called_once()

    # Verify profiles directory and file were created
    profiles_dir = project_dir / "profiles"
    assert profiles_dir.exists()
    profiles_file = profiles_dir / "profiles.yml"
    assert profiles_file.exists()

    # Verify elementary profile was created
    elementary_dir = project_dir / "elementary_profiles"
    assert elementary_dir.exists()
    elementary_file = elementary_dir / "profiles.yml"
    assert elementary_file.exists()


@patch("ddpui.ddpdbt.elementary_service.DbtProjectManager.gather_dbt_project_params")
def test_create_elementary_profile_missing_prefect_block(mock_gather_params, org, tmp_path):
    """tests create_elementary_profile when profiles.yml doesn't exist and no Prefect block found"""
    # Create temporary project directory (no profiles.yml)
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    mock_gather_params.return_value = Mock(project_dir=str(project_dir))

    # No OrgPrefectBlockv1 created, so it should raise HttpError
    with pytest.raises(HttpError) as exc_info:
        create_elementary_profile(org)

    assert "is missing" in str(exc_info.value)


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

    # Setup mocks
    mock_gather_params.return_value = Mock(
        project_dir=str(project_dir), dbt_binary="test-dbt", target="test-target"
    )
    mock_subprocess.return_value = "elementary:\n  schema: elementary_schema"

    result = create_elementary_profile(org)

    assert result == {"status": "success"}
    # Verify elementary profile was still created (overwrites existing)
    elementary_file = elementary_dir / "profiles.yml"
    assert elementary_file.exists()
