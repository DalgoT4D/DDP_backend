from unittest.mock import patch, Mock
import os
from datetime import datetime
from pathlib import Path
import yaml
import pytest
import pytz
from ninja.errors import HttpError
from ddpui.ddpairbyte.airbytehelpers import (
    add_custom_airbyte_connector,
    upgrade_custom_sources,
    setup_airbyte_workspace_v1,
    AIRBYTESERVER,
    get_job_info_for_connection,
    update_destination,
    delete_source,
    create_airbyte_deployment,
    create_connection,
    get_sync_job_history_for_connection,
    create_or_update_org_cli_block,
    schedule_update_connection_schema,
)
from ddpui.ddpairbyte.schema import (
    AirbyteDestinationUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
    AirbyteConnectionCreate,
)
from ddpui.models.role_based_access import Role
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgWarehouse,
    OrgDbt,
    TransformType,
    ConnectionMeta,
    OrgSchemaChange,
)
from ddpui.models.org_user import OrgUser, OrgUserRole, User
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE, schema, DBTCORE
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTECLEAR


pytestmark = pytest.mark.django_db

# ================================================================================


@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org with a fake airbyte workspace id"""
    org = Org.objects.create(slug="test-org", airbyte_workspace_id="wsid")
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="bigquery",
        name="test-warehouse",
        airbyte_destination_id="fake-destination-id",
    )
    block = OrgPrefectBlockv1.objects.create(
        org=org, block_type=AIRBYTESERVER, block_id="blockid", block_name="blockname"
    )
    yield org
    warehouse.delete()
    block.delete()
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser_ = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser_
    orguser_.delete()


@pytest.fixture
def task():
    """a pytest fixture which creates a Task object"""
    task_ = Task.objects.create(slug="test-task", label="test-label")
    yield task_
    task_.delete()


@pytest.fixture
def sync_task():
    """a pytest fixture which creates a Task object"""
    task_ = Task.objects.create(slug=TASK_AIRBYTESYNC, label=TASK_AIRBYTESYNC)
    yield task_
    task_.delete()


@pytest.fixture
def clear_task():
    """a pytest fixture which creates a Task object"""
    task_ = Task.objects.create(slug=TASK_AIRBYTECLEAR, label=TASK_AIRBYTECLEAR)
    yield task_
    task_.delete()


@pytest.fixture
def org_task(org_with_workspace, task):
    """a pytest fixture which creates an OrgTask object"""
    orgtask = OrgTask.objects.create(
        task=task, org=org_with_workspace, connection_id="connection_id"
    )
    yield orgtask
    orgtask.delete()


# ================================================================================


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_custom_source_definition")
def test_add_custom_airbyte_connector(mock_create_custom_source_definition: Mock):
    """very simple test"""
    add_custom_airbyte_connector("wsid", "cname", "cdr", "cdit", "cdurl")
    mock_create_custom_source_definition.assert_called_once_with(
        workspace_id="wsid",
        name="cname",
        docker_repository="cdr",
        docker_image_tag="cdit",
        documentation_url="cdurl",
    )


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_source_definitions",
    mock_get_source_definitions=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
def test_upgrade_custom_sources(
    mock_add_custom_airbyte_connector: Mock,
    mock_get_source_definitions: Mock,
):
    """tests upgrading one of the existing custom connectors"""
    mock_get_source_definitions.return_value = {
        "sourceDefinitions": [
            {
                "name": "KoboToolbox",
                "dockerRepository": "tech4dev/source-kobotoolbox",
                "dockerImageTag": "0.0.0",
            },
            {
                "name": "Avni",
                "dockerRepository": "tech4dev/source-avni",
                "dockerImageTag": "0.0.0",
            },
            {
                "name": "CommCare T4D",
                "dockerRepository": "tech4dev/source-commcare",
                "dockerImageTag": "0.0.0",
            },
        ]
    }
    upgrade_custom_sources("workspace_id")
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "KoboToolbox", "tech4dev/source-kobotoolbox", "0.3.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "tech4dev/source-avni", "0.2.1", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "CommCare T4D", "tech4dev/source-commcare", "0.3.0", ""
    )


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_source_definitions",
    mock_get_source_definitions=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
def test_upgrade_custom_sources_add(
    mock_add_custom_airbyte_connector: Mock,
    mock_get_source_definitions: Mock,
):
    """tests upgrading one of the existing custom connectors"""
    mock_get_source_definitions.return_value = {"sourceDefinitions": []}
    upgrade_custom_sources("workspace_id")
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "KoboToolbox", "tech4dev/source-kobotoolbox", "0.3.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "tech4dev/source-avni", "0.2.1", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "CommCare T4D", "tech4dev/source-commcare", "0.3.0", ""
    )


# ===========================================================================
@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_workspace",
    mock_create_workspace=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.get_airbyte_server_block_id",
    mock_get_airbyte_server_block_id=Mock(),
)
def test_setup_airbyte_workspace_fail_v1_noproxy(
    mock_get_airbyte_server_block_id: Mock,
    mock_add_custom_airbyte_connector: Mock,
    create_workspace: Mock,
):
    """failing test"""
    org_save = Mock()
    org = Mock(save=org_save)
    create_workspace.return_value = {
        "workspaceId": "wsid",
        "initialSetupComplete": False,
    }
    mock_add_custom_airbyte_connector.return_value = 1
    mock_get_airbyte_server_block_id.side_effect = Exception("error")
    with pytest.raises(Exception) as excinfo:
        setup_airbyte_workspace_v1("workspace_name", org)
    assert str(excinfo.value) == "could not connect to prefect-proxy"


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_workspace",
    mock_create_workspace=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.get_airbyte_server_block_id",
    mock_get_airbyte_server_block_id=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.create_airbyte_server_block",
    mock_create_airbyte_server_block=Mock(),
)
def test_setup_airbyte_workspace_v1_create_server_block(
    mock_create_airbyte_server_block: Mock,
    mock_get_airbyte_server_block_id: Mock,
    mock_add_custom_airbyte_connector: Mock,
    create_workspace: Mock,
):
    """failing test"""
    org = Org.objects.create(name="org", slug="org")
    create_workspace.return_value = {
        "workspaceId": "wsid",
        "name": "wsname",
        "initialSetupComplete": False,
    }
    mock_add_custom_airbyte_connector.return_value = 1
    mock_get_airbyte_server_block_id.return_value = None
    mock_create_airbyte_server_block.return_value = (1, "cleaned-block-name")
    setup_airbyte_workspace_v1("workspace_name", org)
    mock_get_airbyte_server_block_id.assert_called_once_with("org-airbyte-server")
    mock_create_airbyte_server_block.assert_called_once_with("org-airbyte-server")
    assert OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
    org.delete()


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_workspace",
    mock_create_workspace=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.get_airbyte_server_block_id",
    mock_get_airbyte_server_block_id=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.create_airbyte_server_block",
    mock_create_airbyte_server_block=Mock(),
)
def test_setup_airbyte_workspace_v1_server_block_exists(
    mock_create_airbyte_server_block: Mock,
    mock_get_airbyte_server_block_id: Mock,
    mock_add_custom_airbyte_connector: Mock,
    create_workspace: Mock,
):
    """failing test"""
    org = Org.objects.create(name="org", slug="org")
    create_workspace.return_value = {
        "workspaceId": "wsid",
        "name": "wsname",
        "initialSetupComplete": False,
    }
    mock_add_custom_airbyte_connector.return_value = 1
    mock_get_airbyte_server_block_id.return_value = 1
    response = setup_airbyte_workspace_v1("workspace_name", org)
    mock_get_airbyte_server_block_id.assert_called_once_with("org-airbyte-server")
    mock_create_airbyte_server_block.assert_not_called()
    assert OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
    assert response.name == "wsname"
    assert response.workspaceId == "wsid"
    assert response.initialSetupComplete is False
    org.delete()


def test_get_job_info_for_connection_connection_dne():
    """tests get_job_info_for_connection"""
    org = Org.objects.create(name="org", slug="org")
    _, error = get_job_info_for_connection(org, "connection_id")
    assert error == "connection not found"


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_jobs_for_connection",
    mock_get_jobs_for_connection=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_logs_for_job",
    mock_get_logs_for_job=Mock(),
)
def test_get_job_info_for_connection_job_dne(
    mock_get_logs_for_job: Mock, mock_get_jobs_for_connection: Mock
):
    """tests get_job_info_for_connection"""
    org = Org.objects.create(name="org", slug="org")
    task = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=task, connection_id="connection_id")

    mock_get_jobs_for_connection.return_value = {"jobs": []}
    mock_get_logs_for_job.return_value = (
        [
            "line1",
            "line2",
        ],
    )

    result, error = get_job_info_for_connection(org, "connection_id")
    assert error is None
    assert result == {"status": "not found"}


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_jobs_for_connection",
    mock_get_jobs_for_connection=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_logs_for_job",
    mock_get_logs_for_job=Mock(),
)
def test_get_job_info_for_connection(
    mock_get_logs_for_job: Mock, mock_get_jobs_for_connection: Mock
):
    """tests get_job_info_for_connection"""
    org = Org.objects.create(name="org", slug="org")
    task = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=task, connection_id="connection_id")

    mock_get_jobs_for_connection.return_value = {
        "jobs": [
            {
                "job": {
                    "configType": "sync",
                    "id": "JOB_ID",
                    "status": "JOB_STATUS",
                },
                "attempts": [
                    {
                        "id": 1,
                        "status": "failed",
                        "recordsSynced": 0,
                    },
                    {
                        "id": 2,
                        "endedAt": 123123123,
                        "createdAt": 123123123,
                        "status": "succeeded",
                        "recordsSynced": 100,
                        "bytesSynced": 0,
                        "totalStats": {
                            "recordsEmitted": 0,
                            "recordsCommitted": 0,
                            "bytesEmitted": 500,
                        },
                    },
                ],
            }
        ],
        "totalJobCount": 1,
    }
    mock_get_logs_for_job.return_value = [
        "line1",
        "line2",
    ]

    result, error = get_job_info_for_connection(org, "connection_id")
    assert error is None

    assert result["logs"] == [
        "line1",
        "line2",
    ]


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_jobs_for_connection",
    mock_get_jobs_for_connection=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.parse_job_info",
    mock_parse_job_info=Mock(),
)
def test_get_sync_history_for_connection_no_jobs(
    mock_parse_job_info: Mock, mock_get_jobs_for_connection: Mock
):
    """tests get_sync_job_history_for_connection for success"""
    org = Org.objects.create(name="org", slug="org")
    task = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=task, connection_id="connection_id")

    mock_get_jobs_for_connection.return_value = {
        "jobs": [
            {
                "job": {
                    "id": "JOB_ID",
                    "status": "JOB_STATUS",
                },
                "attempts": [
                    {
                        "id": 1,
                        "status": "failed",
                        "recordsSynced": 0,
                    },
                    {
                        "id": 2,
                        "endedAt": 123123123,
                        "createdAt": 123123123,
                        "status": "succeeded",
                        "recordsSynced": 100,
                        "bytesSynced": 0,
                        "totalStats": {
                            "recordsEmitted": 0,
                            "recordsCommitted": 0,
                            "bytesEmitted": 500,
                        },
                    },
                ],
            }
        ],
        "totalJobCount": 1,
    }
    mock_parse_job_info.return_value = "job-info"

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert "history" in result
    assert len(result["history"]) == 1
    assert result["history"][0] == "job-info"
    assert result["totalSyncs"] == 1


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_jobs_for_connection",
    mock_get_jobs_for_connection=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_logs_for_job",
    mock_get_logs_for_job=Mock(),
)
def test_get_sync_history_for_connection_success(
    mock_get_logs_for_job: Mock, mock_get_jobs_for_connection: Mock
):
    """tests get_sync_job_history_for_connection for the case when the connection has no syncs created yet"""
    org = Org.objects.create(name="org", slug="org")
    task = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=task, connection_id="connection_id")

    mock_get_jobs_for_connection.return_value = {"jobs": [], "totalJobCount": 0}
    mock_get_logs_for_job.return_value = [
        "line1",
        "line2",
    ]

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert result == []


@patch("ddpui.ddpairbyte.airbytehelpers.generate_hash_id")
@patch("ddpui.ddpairbyte.airbytehelpers.logger")
@patch("ddpui.ddpairbyte.airbytehelpers.prefect_service.create_dataflow_v1")
@patch("ddpui.ddpairbyte.airbytehelpers.setup_airbyte_sync_task_config")
@patch("ddpui.ddpairbyte.airbytehelpers.OrgDataFlowv1")
@patch("ddpui.ddpairbyte.airbytehelpers.DataflowOrgTask")
def test_create_airbyte_deployment(
    mock_dataflow_org_task,
    mock_org_dataflow,
    mock_setup_task_config,
    mock_create_dataflow,
    mock_logger,
    mock_generate_hash_id,
    org_with_workspace,
    org_task,
):
    """test create_airbyte_deployment"""
    mock_generate_hash_id.return_value = "12345678"
    mock_setup_task_config.return_value.to_json.return_value = {"task": "config"}
    mock_create_dataflow.return_value = {
        "deployment": {"id": "deployment-id", "name": "deployment-name"}
    }
    mock_org_dataflow.objects.filter.return_value.first.return_value = None

    org_airbyte_server_block = OrgPrefectBlockv1.objects.filter(
        org=org_with_workspace,
        block_type=AIRBYTESERVER,
    ).first()
    result = create_airbyte_deployment(org_with_workspace, org_task, org_airbyte_server_block)

    mock_generate_hash_id.assert_called_once_with(8)
    mock_logger.info.assert_called_once_with("using the hash code 12345678 for the deployment name")
    mock_create_dataflow.assert_called_once()
    mock_org_dataflow.objects.filter.assert_called_once_with(deployment_id="deployment-id")
    mock_org_dataflow.objects.create.assert_called_once_with(
        org=org_with_workspace,
        name="manual-test-org-test-task-12345678",
        deployment_name="deployment-name",
        deployment_id="deployment-id",
        dataflow_type="manual",
    )
    mock_dataflow_org_task.objects.create.assert_called_once_with(
        dataflow=mock_org_dataflow.objects.create.return_value,
        orgtask=org_task,
    )

    assert result == mock_org_dataflow.objects.create.return_value


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_connection")
@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.delete_connection")
@patch("ddpui.ddpairbyte.airbytehelpers.create_airbyte_deployment")
@patch("ddpui.ddpairbyte.airbytehelpers.logger")
def test_create_connection(
    mock_logger,
    mock_create_airbyte_deployment,
    mock_delete_connection,
    mock_create_connection,
    org_with_workspace,
    sync_task,
    clear_task,
):
    mock_create_connection.return_value = {
        "connectionId": "connection-id",
        "sourceId": "source-id",
        "destinationId": "destination-id",
        "sourceCatalogId": "source-catalog-id",
        "syncCatalog": {},
        "status": "status",
        "name": "test-connection",
    }
    payload = AirbyteConnectionCreate(
        name="test-connection",
        sourceId="source-id",
        destinationId="destination-id",
        streams=[],
        catalogId="catalog-id",
        syncCatalog={"streams": []},
    )

    new_dataflow = Mock(clear_conn_dataflow=None)
    create_airbyte_deployment.return_value = new_dataflow

    result, error = create_connection(org_with_workspace, payload)

    assert error is None

    mock_create_connection.assert_called_once_with(org_with_workspace.airbyte_workspace_id, payload)

    assert result["name"] == "test-connection"
    assert result["connectionId"] == "connection-id"
    assert result["source"] == {"id": "source-id"}
    assert result["destination"] == {"id": "destination-id"}
    assert result["catalogId"] == "source-catalog-id"
    assert result["syncCatalog"] == {}
    assert result["status"] == "status"


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    mock_update_destination=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
    mock_retrieve_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.update_warehouse_credentials",
    mock_update_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_or_update_org_cli_block",
    mock_create_or_update_org_cli_block=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.run_dbt_task_sync",
    mock_run_dbt_task_sync=Mock(),
)
def test_update_destination_name(
    mock_run_dbt_task_sync: Mock,
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {}
    mock_update_warehouse_credentials.return_value = None
    mock_cli_profile_block = Mock(block_name="block-name")
    mock_dbt_project_params = Mock(
        dbt_binary="dbt-binary", project_dir="dbt-project-dir", working_dir="dbt-project-dir"
    )
    mock_create_or_update_org_cli_block.return_value = (
        (mock_cli_profile_block, mock_dbt_project_params),
        None,
    )

    payload = AirbyteDestinationUpdate(
        name="new-name", destinationDefId="destinationDefId", config={}
    )
    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    warehouse.refresh_from_db()

    assert warehouse.name == "new-name"
    mock_create_or_update_org_cli_block.assert_called_once()


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    mock_update_destination=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
    mock_retrieve_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.update_warehouse_credentials",
    mock_update_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_or_update_org_cli_block",
    mock_create_or_update_org_cli_block=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.run_dbt_task_sync",
    mock_run_dbt_task_sync=Mock(),
)
def test_update_destination_postgres_config(
    mock_run_dbt_task_sync: Mock,
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={"host": "new-host", "port": "123", "password": "*****"},
    )
    mock_cli_profile_block = Mock(block_name="block-name")
    mock_dbt_project_params = Mock(
        dbt_binary="dbt-binary", project_dir="dbt-project-dir", working_dir="dbt-project-dir"
    )
    mock_create_or_update_org_cli_block.return_value = (
        (mock_cli_profile_block, mock_dbt_project_params),
        None,
    )
    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_update_warehouse_credentials.assert_called_once_with(
        warehouse,
        {
            "host": "new-host",
            "port": "123",
        },
    )
    mock_create_or_update_org_cli_block.assert_called_once()


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    mock_update_destination=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
    mock_retrieve_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.update_warehouse_credentials",
    mock_update_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_or_update_org_cli_block",
    mock_create_or_update_org_cli_block=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.run_dbt_task_sync",
    mock_run_dbt_task_sync=Mock(),
)
def test_update_destination_bigquery_config(
    mock_run_dbt_task_sync: Mock,
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="bigquery", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={"credentials_json": '{"key": "value"}'},
    )
    mock_cli_profile_block = Mock(block_name="block-name")
    mock_dbt_project_params = Mock(
        dbt_binary="dbt-binary", project_dir="dbt-project-dir", working_dir="dbt-project-dir"
    )
    mock_create_or_update_org_cli_block.return_value = (
        (mock_cli_profile_block, mock_dbt_project_params),
        None,
    )
    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_update_warehouse_credentials.assert_called_once_with(
        warehouse,
        {
            "key": "value",
        },
    )
    mock_create_or_update_org_cli_block.assert_called_once()


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    mock_update_destination=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
    mock_retrieve_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.update_warehouse_credentials",
    mock_update_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_or_update_org_cli_block",
    mock_create_or_update_org_cli_block=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.run_dbt_task_sync",
    mock_run_dbt_task_sync=Mock(),
)
def test_update_destination_snowflake_config(
    mock_run_dbt_task_sync: Mock,
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="snowflake", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {"credentials": {"password": "*****"}}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={"credentials": {"password": "newpassword"}},
    )
    mock_cli_profile_block = Mock(block_name="block-name")
    mock_dbt_project_params = Mock(
        dbt_binary="dbt-binary", project_dir="dbt-project-dir", working_dir="dbt-project-dir"
    )
    mock_create_or_update_org_cli_block.return_value = (
        (mock_cli_profile_block, mock_dbt_project_params),
        None,
    )
    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_update_warehouse_credentials.assert_called_once_with(
        warehouse,
        {"credentials": {"password": "newpassword"}},
    )
    mock_create_or_update_org_cli_block.assert_called_once()


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    mock_update_destination=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
    mock_retrieve_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.utils.secretsmanager.update_warehouse_credentials",
    mock_update_warehouse_credentials=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_or_update_org_cli_block",
    mock_create_or_update_org_cli_block=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.create_elementary_profile",
    mock_create_elementary_profile=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.prefect_service.run_dbt_task_sync",
    mock_run_dbt_task_sync=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.uuid4",
    mock_uuid4=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.timezone",
    mock_uuid4=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.elementary_setup_status",
    mock_uuid4=Mock(),
)
def test_update_destination_cliprofile(
    mock_elementary_setup_status: Mock,
    mock_timezone: Mock,
    mock_uuid4: Mock,
    mock_run_dbt_task_sync: Mock,
    mock_create_elementary_profile: Mock,
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="snowflake", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {"credentials": {"password": "*****"}}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={
            "credentials": {"password": "newpassword"},
            "dataset_location": "LOCATIUON",
        },
    )

    OrgPrefectBlockv1.objects.create(org=org, block_type=DBTCLIPROFILE, block_name="cliblockname")

    mock_cli_profile_block = Mock(block_name="block-name")
    mock_dbt_project_params = Mock(
        dbt_binary="dbt-binary", project_dir="dbt-project-dir", working_dir="dbt-project-dir"
    )
    mock_create_or_update_org_cli_block.return_value = (
        (mock_cli_profile_block, mock_dbt_project_params),
        None,
    )
    mock_uuid4.return_value = "fake-uuid"
    mock_timezone.as_ist = Mock(return_value=Mock(isoformat=Mock(return_value="isoformatted-time")))

    mock_elementary_setup_status.return_value = "set-up"

    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_create_or_update_org_cli_block.assert_called_once_with(org, warehouse, payload.config)
    mock_create_elementary_profile.assert_called_once_with(org)

    dbtdebugtask = schema.PrefectDbtTaskSetup(
        seq=1,
        slug="dbt-debug",
        commands=["dbt-binary debug"],
        type=DBTCORE,
        env={},
        working_dir="dbt-project-dir",
        profiles_dir="dbt-project-dir/profiles/",
        project_dir="dbt-project-dir",
        cli_profile_block="block-name",
        cli_args=[],
        orgtask_uuid="fake-uuid",
        flow_name="org-dbt-debug",
        flow_run_name="isoformatted-time",
    )
    mock_run_dbt_task_sync.assert_called_once_with(dbtdebugtask)


@patch("ddpui.ddpairbyte.airbyte_service.get_connections", mock_get_connections=Mock())
@patch(
    "ddpui.ddpprefect.prefect_service.delete_deployment_by_id",
    mock_delete_deployment_by_id=Mock(),
)
@patch("ddpui.ddpairbyte.airbyte_service.delete_source", mock_delete_source=Mock())
def test_delete_source(
    mock_delete_source: Mock,
    mock_delete_deployment_by_id: Mock,
    mock_get_connections: Mock,
):
    """test delete_source"""
    org = Org.objects.create(name="org", slug="org")

    mock_get_connections.return_value = {
        "connections": [
            {"sourceId": "source_id", "connectionId": "connection_id"},
            {"sourceId": "source_id2", "connectionId": "connection_id2"},
        ]
    }

    synctask = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    orgtask = OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id")
    OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id2")

    dataflow = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="manual", deployment_id="deployment-id"
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)

    delete_source(org, "source_id")

    mock_delete_deployment_by_id.assert_called_once_with("deployment-id")

    assert not OrgDataFlowv1.objects.filter(
        org=org, dataflow_type="manual", deployment_id="deployment-id"
    ).exists()

    # this one was deleted
    assert not OrgTask.objects.filter(
        org=org, task=synctask, connection_id="connection_id"
    ).exists()
    # but this one was not deleted
    assert OrgTask.objects.filter(org=org, task=synctask, connection_id="connection_id2").exists()

    mock_delete_source.assert_called_once()


@patch(
    "ddpui.ddpprefect.prefect_service.create_dbt_cli_profile_block",
    mock_create_dbt_cli_profile_block=Mock(),
)
def test_create_or_update_org_cli_block_create_case(
    mock_create_dbt_cli_profile_block: Mock,
):
    """test create_or_update_org_cli_block when its created for the first time"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_create_dbt_cli_profile_block.return_value = {
        "block_id": "some_id",
        "block_name": "some_name",
    }

    dummy_creds = {
        "username": "username",
        "password": "password",
        "host": "host",
        "port": "port",
        "database": "database",
    }

    create_or_update_org_cli_block(org, warehouse, dummy_creds)

    org_cli_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=DBTCLIPROFILE).first()
    assert org_cli_block is not None
    assert org_cli_block.block_id == "some_id"
    assert org_cli_block.block_name == "some_name"


@patch(
    "ddpui.ddpprefect.prefect_service.update_dbt_cli_profile_block",
    mock_update_dbt_cli_profile_block=Mock(),
)
def test_create_or_update_org_cli_block_update_case(
    mock_update_dbt_cli_profile_block: Mock, tmp_path
):
    """test create_or_update_org_cli_block when the block is updated"""
    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_update_dbt_cli_profile_block.return_value = {
        "block_id": "some_id",
        "block_name": "some_name",
    }

    dummy_creds = {
        "username": "username",
        "password": "password",
        "host": "host",
        "port": "port",
        "database": "database",
    }
    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="some_id",
        block_name="some_name",
    )
    project_name = "dbtrepo"

    project_dir = Path(tmp_path) / org.slug
    project_dir.mkdir(parents=True, exist_ok=True)
    dbtrepo_dir = project_dir / project_name
    dbtrepo_dir.mkdir(parents=True, exist_ok=True)
    dbt = OrgDbt.objects.create(
        project_dir=f"{org.slug}/{project_name}",
        dbt_venv=str(tmp_path),
        target_type="postgres",
        default_schema="default",
        transform_type=TransformType.GIT,
    )
    org.dbt = dbt
    org.save()

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(str(dbtrepo_dir / "dbt_project.yml"), "w", encoding="utf-8") as output:
        yaml.safe_dump(yml_obj, output)

    create_or_update_org_cli_block(org, warehouse, dummy_creds)

    mock_update_dbt_cli_profile_block.assert_called_once_with(
        block_name=cli_profile_block.block_name,
        wtype=warehouse.wtype,
        credentials=dummy_creds,
        bqlocation=None,
        profilename=yml_obj["profile"],
        target="default",
    )


def test_schedule_update_connection_schema_test_for_small_connection(orguser):
    """Tests the flow of schema change update when the connection is not large enough"""
    connection_id = "same-conn-id"
    payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={"diff": "catalogDiff"})

    with pytest.raises(HttpError) as excinfo:
        schedule_update_connection_schema(orguser, connection_id, payload)
    assert str(excinfo.value) == "airbyte server block not found"

    OrgPrefectBlockv1.objects.create(
        org=orguser.org, block_type=AIRBYTESERVER, block_name="airbyte-server"
    )

    with pytest.raises(HttpError) as excinfo:
        schedule_update_connection_schema(orguser, connection_id, payload)
    assert str(excinfo.value) == "Orgtask not found"

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(task=task, org=orguser.org, connection_id=connection_id)
    dataflow = OrgDataFlowv1.objects.create(
        org=orguser.org,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    with patch(
        "ddpui.ddpprefect.prefect_service.lock_tasks_for_deployment"
    ) as lock_tasks_mock, patch(
        "ddpui.ddpprefect.prefect_service.schedule_deployment_flow_run"
    ) as schedule_flow_run_mock:
        schedule_update_connection_schema(orguser, connection_id, payload)

        lock_tasks_mock.assert_called_once()
        args, kwargs = schedule_flow_run_mock.call_args
        # schedule time should none
        assert args[2] is None


def test_schedule_update_connection_schema_test_for_large_connection(orguser):
    """Tests the flow of schema change update when the connection is large enough"""
    connection_id = "same-conn-id"
    payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={"diff": "catalogDiff"})

    OrgPrefectBlockv1.objects.create(
        org=orguser.org, block_type=AIRBYTESERVER, block_name="airbyte-server"
    )

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(task=task, org=orguser.org, connection_id=connection_id)
    dataflow = OrgDataFlowv1.objects.create(
        org=orguser.org,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    # mark the connection as large
    ConnectionMeta.objects.create(connection_id=connection_id, schedule_large_jobs=True)

    with patch(
        "ddpui.ddpprefect.prefect_service.lock_tasks_for_deployment"
    ) as lock_tasks_mock, patch(
        "ddpui.ddpprefect.prefect_service.schedule_deployment_flow_run"
    ) as schedule_flow_run_mock, patch(
        "ddpui.ddpprefect.prefect_service.delete_flow_run"
    ) as delete_flow_run_mock:
        schedule_flow_run_mock.return_value = {"flow_run_id": "schame-change-schedule-flow-run-id"}

        OrgSchemaChange.objects.create(
            connection_id=connection_id, org=orguser.org, change_type="non-breaking"
        )

        schedule_update_connection_schema(orguser, connection_id, payload)

        lock_tasks_mock.assert_not_called()
        args, kwargs = schedule_flow_run_mock.call_args
        # schedule time should not be none
        assert args[2] is not None

        schema_change = OrgSchemaChange.objects.filter(
            org=orguser.org, connection_id=connection_id
        ).first()

        assert schema_change is not None
        assert schema_change.schedule_job is not None

        scheduled_job = schema_change.schedule_job
        assert scheduled_job is not None
        assert scheduled_job.scheduled_at > datetime.now(pytz.utc)
