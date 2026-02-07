import os
from unittest.mock import patch, Mock, ANY
from datetime import datetime
import pytz
import pytest
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
    schedule_update_connection_schema,
    fetch_and_update_airbyte_job_details,
    fetch_and_update_airbyte_jobs_for_all_connections,
    fetch_and_update_org_schema_changes,
)
from ddpui.ddpairbyte.schema import (
    AirbyteDestinationUpdate,
    AirbyteConnectionCreate,
    AirbyteConnectionSchemaUpdateSchedule,
)
from ddpui.models.role_based_access import Role
from ddpui.models.tasks import TaskType
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgWarehouse,
    OrgSchemaChange,
    OrgDbt,
)
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.org_user import OrgUser, User
from ddpui.models.airbyte import AirbyteJob
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE, schema, DBTCORE
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTECLEAR
from ddpui.ddpprefect import DDP_WORK_QUEUE, MANUL_DBT_WORK_QUEUE


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
    queue_config = {
        "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
        "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "test_workpool"},
    }

    org = Org.objects.create(
        slug="test-org", airbyte_workspace_id="wsid", queue_config=queue_config
    )
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


@pytest.fixture
def org_sync_task(org_with_workspace, sync_task):
    """a pytest fixture which creates an OrgTask object"""
    orgtask = OrgTask.objects.create(
        task=sync_task, org=org_with_workspace, connection_id="connection_id"
    )
    yield orgtask
    orgtask.delete()


@pytest.fixture
def airbyte_server_block(orguser):
    """an OrgPrefectBlockv1 of type airbyte-server"""
    block = OrgPrefectBlockv1.objects.create(
        org=orguser.org,
        block_type=AIRBYTESERVER,
    )
    yield block
    block.delete()


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
    task = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
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
    task = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
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


def test_get_sync_history_for_connection_no_jobs():
    """tests get_sync_job_history_for_connection for success"""
    org = Org.objects.create(name="org", slug="org")
    synctask = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id")

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert result == {"history": [], "totalSyncs": 0}


def test_get_sync_history_for_connection_success():
    """tests get_sync_job_history_for_connection for the case when the connection has no syncs created yet"""
    org = Org.objects.create(name="org", slug="org")
    synctask = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id")
    started_at = datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    ended_at = datetime(2025, 1, 1, 0, 10, 0, tzinfo=pytz.UTC)
    job = AirbyteJob.objects.create(
        job_id=1,
        config_id="connection_id",
        job_type="sync",
        created_at=started_at,
        started_at=started_at,
        ended_at=ended_at,
        status="succeeded",
        records_emitted=10,
        bytes_emitted=20,
        bytes_committed=10,
        records_committed=5,
        stream_stats={"a": 2},
        reset_config={},
        attempts=[{"attempt": {"id": 1}}, {"attempt": {"id": 2}}],
    )

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert "history" in result
    assert len(result["history"]) == 1
    assert result["history"][0] == {
        "job_id": 1,
        "status": "succeeded",
        "job_type": "sync",
        "created_at": started_at,
        "started_at": started_at,
        "ended_at": ended_at,
        "records_emitted": job.records_emitted,
        "bytes_emitted": "20 bytes",
        "records_committed": job.records_committed,
        "bytes_committed": "10 bytes",
        "stream_stats": job.stream_stats,
        "reset_config": job.reset_config,
        "duration_seconds": 600,  # end - start
        "last_attempt_no": 2,
    }
    assert result["totalSyncs"] == 1
    job.delete()


@patch.dict("os.environ", {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
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

    # Check that create_dataflow_v1 was called with the correct arguments
    mock_create_dataflow.assert_called_once()
    call_args = mock_create_dataflow.call_args
    assert len(call_args[0]) == 2  # payload and queue_details

    # Verify the second argument is a QueueDetailsSchema with correct workpool
    queue_details = call_args[0][1]
    assert hasattr(queue_details, "name")
    assert hasattr(queue_details, "workpool")
    assert queue_details.workpool == "test_workpool"

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
    # Create OrgDbt to enable dbt functionality
    org_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo", project_dir="/path/to/dbt/project"
    )
    org.dbt = org_dbt
    org.save()
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
    # Create OrgDbt to enable dbt functionality
    org_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo", project_dir="/path/to/dbt/project"
    )
    org.dbt = org_dbt
    org.save()
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
    # Create OrgDbt to enable dbt functionality
    org_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo", project_dir="/path/to/dbt/project"
    )
    org.dbt = org_dbt
    org.save()
    warehouse = OrgWarehouse.objects.create(org=org, wtype="bigquery", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={
            "credentials_json": '{"key": "value"}',
            "dataset_location": "LOCATION",
            "transformation_priority": "batch",
        },
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
            "dataset_location": "LOCATION",
            "transformation_priority": "batch",
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
    # Create OrgDbt to enable dbt functionality
    org_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo", project_dir="/path/to/dbt/project"
    )
    org.dbt = org_dbt
    org.save()
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
    # Create OrgDbt to enable dbt functionality
    org_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo", project_dir="/path/to/dbt/project"
    )
    org.dbt = org_dbt
    org.save()
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
    """test delete_sourc; prevent deletion if source is used in a connection or is part of a dataflow"""
    org = Org.objects.create(name="org", slug="org")

    connections = [
        {"sourceId": "source_id1", "connectionId": "connection_id", "name": "conn1"},
        {"sourceId": "source_id2", "connectionId": "connection_id2", "name": "conn2"},
    ]

    mock_get_connections.return_value = {"connections": connections}

    synctask = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
    orgtask = OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id")
    OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id2")

    dataflow = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="manual", deployment_id="deployment-id"
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)

    with pytest.raises(HttpError) as excinfo:
        delete_source(org, "source_id1")

    assert excinfo.value.status_code == 403
    assert (
        str(excinfo.value)
        == f"Cannot delete source. It is used in connection(s): {', '.join([c['name'] for c in connections if c['sourceId'] == 'source_id1'])}. Please remove these connections first."
    )

    # check a successful deletion with no links to connections or dataflows
    delete_source(org, "source_id")
    mock_delete_source.assert_called_once_with(org.airbyte_workspace_id, "source_id")


def test_schedule_update_connection_schema_no_serverblock(orguser):
    """tests schedule_update_connection_schema with no server block"""
    with pytest.raises(Exception) as excinfo:
        payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={})
        schedule_update_connection_schema(orguser, "fake-connection-id", payload)
    assert str(excinfo.value) == "airbyte server block not found"


def test_schedule_update_connection_schema_no_orgtask(
    orguser: OrgUser, airbyte_server_block: OrgPrefectBlockv1
):
    """tests schedule_update_connection_schema with no server block"""
    with pytest.raises(Exception) as excinfo:
        payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={})
        schedule_update_connection_schema(orguser, "fake-connection-id", payload)
    assert str(excinfo.value) == "Orgtask not found"


@patch("ddpui.core.pipelinefunctions.setup_airbyte_update_schema_task_config", Mock(to_json=Mock()))
def test_schedule_update_connection_schema_no_dataflow(
    orguser: OrgUser, airbyte_server_block: OrgPrefectBlockv1, org_sync_task: OrgTask
):
    """tests schedule_update_connection_schema with no dataflow"""
    org_sync_task.org = orguser.org
    org_sync_task.save()
    with pytest.raises(Exception) as excinfo:
        payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={})
        schedule_update_connection_schema(orguser, org_sync_task.connection_id, payload)
    assert str(excinfo.value) == "no dataflow mapped"


@patch("ddpui.core.pipelinefunctions.setup_airbyte_update_schema_task_config", Mock(to_json=Mock()))
@patch("ddpui.ddpprefect.prefect_service.create_deployment_flow_run")
def test_schedule_update_connection_schema_success(
    mock_create_deployment_flow_run: Mock,
    orguser: OrgUser,
    airbyte_server_block: OrgPrefectBlockv1,
    org_sync_task: OrgTask,
):
    """tests successful flow run creation in schedule_update_connection_schema"""
    org_sync_task.org = orguser.org
    org_sync_task.save()
    odf = OrgDataFlowv1.objects.create(
        org=orguser.org, name="odf-name", dataflow_type="manual", deployment_id="fake-deployment-id"
    )
    DataflowOrgTask.objects.create(orgtask=org_sync_task, dataflow=odf)
    payload = AirbyteConnectionSchemaUpdateSchedule(catalogDiff={})
    mock_create_deployment_flow_run.return_value = {
        "flow_run_id": "flow_run_id",
        "name": "name",
    }
    schedule_update_connection_schema(orguser, org_sync_task.connection_id, payload)

    mock_create_deployment_flow_run.assert_called_once_with(
        "fake-deployment-id",
        {
            "config": {
                "org_slug": orguser.org.slug,
                "tasks": [
                    {
                        "slug": "update-schema",
                        "airbyte_server_block": "",
                        "connection_id": "connection_id",
                        "timeout": ANY,
                        "type": "Airbyte Connection",
                        "orgtask_uuid": ANY,
                        "flow_name": None,
                        "flow_run_name": None,
                        "seq": 1,
                        "catalog_diff": {},
                    }
                ],
            }
        },
    )
    prefect_flow_run = PrefectFlowRun.objects.filter(
        deployment_id="fake-deployment-id", flow_run_id="flow_run_id"
    ).first()
    assert prefect_flow_run is not None
    assert prefect_flow_run.orguser == orguser


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_job_info_without_logs",
    mock_get_job_info_without_logs=Mock(),
)
def test_fetch_and_update_airbyte_job_details_creates_job(mock_get_job_info_without_logs):
    # Setup mock job info
    job_id = "123"
    job_info = {
        "job": {
            "id": job_id,
            "configType": "sync",
            "configId": "conn-1",
            "status": "succeeded",
            "resetConfig": None,
            "refreshConfig": None,
            "streamAggregatedStats": {},
            "aggregatedStats": {
                "recordsEmitted": 100,
                "bytesEmitted": 2048,
                "recordsCommitted": 100,
                "bytesCommitted": 2048,
            },
            "createdAt": 1700000000,
        },
        "attempts": [
            {"attempt": {"id": 1, "createdAt": 1700000000, "endedAt": 1700001000}},
            {"attempt": {"id": 2, "createdAt": 1700000500, "endedAt": 1700002000}},
        ],
    }
    mock_get_job_info_without_logs.return_value = job_info

    # Patch from_timestamp to just return the int for simplicity
    result = fetch_and_update_airbyte_job_details(job_id)

    # Check AirbyteJob is created
    job = AirbyteJob.objects.get(job_id=job_id)
    assert job.job_type == job_info["job"]["configType"]
    assert job.status == job_info["job"]["status"]
    assert job.records_emitted == job_info["job"]["aggregatedStats"]["recordsEmitted"]
    assert job.bytes_emitted == job_info["job"]["aggregatedStats"]["bytesEmitted"]
    assert result["job_id"] == job_id


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_job_info_without_logs",
    mock_get_job_info_without_logs=Mock(),
)
def test_fetch_and_update_airbyte_job_details_updates_existing_job(mock_get_job_info_without_logs):
    # Create initial job
    job_id = "456"
    AirbyteJob.objects.create(
        job_id=job_id,
        job_type="sync",
        config_id="conn-2",
        status="running",
        reset_config=None,
        refresh_config=None,
        stream_stats={},
        records_emitted=10,
        bytes_emitted=100,
        records_committed=10,
        bytes_committed=100,
        started_at=datetime.now(),
        ended_at=datetime.now(),
        created_at=datetime.now(),
        attempts=[],
    )

    # Setup mock job info with updated values
    job_info = {
        "job": {
            "id": job_id,
            "configType": "sync",
            "configId": "conn-2",
            "status": "failed",
            "resetConfig": None,
            "refreshConfig": None,
            "streamAggregatedStats": {},
            "aggregatedStats": {
                "recordsEmitted": 20,
                "bytesEmitted": 200,
                "recordsCommitted": 20,
                "bytesCommitted": 200,
            },
            "createdAt": datetime.now(),
        },
        "attempts": [
            {
                "attempt": {
                    "id": 1,
                    "createdAt": str(datetime.now()),
                    "endedAt": str(datetime.now()),
                }
            },
        ],
    }
    mock_get_job_info_without_logs.return_value = job_info

    with patch("ddpui.ddpairbyte.airbytehelpers.from_timestamp", side_effect=lambda x: x):
        result = fetch_and_update_airbyte_job_details(job_id)

    job = AirbyteJob.objects.get(job_id=job_id)
    assert job.status == job_info["job"]["status"]
    assert job.records_emitted == job_info["job"]["aggregatedStats"]["recordsEmitted"]
    assert job.bytes_emitted == job_info["job"]["aggregatedStats"]["bytesEmitted"]
    assert result["job_id"] == job_id


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_job_info_without_logs",
    mock_get_job_info_without_logs=Mock(),
)
def test_fetch_and_update_airbyte_job_details_no_attempts_raises(mock_get_job_info_without_logs):
    job_id = "789"
    job_info = {
        "job": {
            "id": job_id,
            "configType": "sync",
            "configId": "conn-3",
            "status": "succeeded",
            "resetConfig": None,
            "refreshConfig": None,
            "streamAggregatedStats": {},
            "aggregatedStats": {},
            "createdAt": 1700000000,
        },
        "attempts": [],
    }
    mock_get_job_info_without_logs.return_value = job_info

    with pytest.raises(Exception, match="No attempts found for job_id=789"):
        fetch_and_update_airbyte_job_details(job_id)


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_job_info_without_logs",
    mock_get_job_info_without_logs=Mock(),
)
def test_fetch_and_update_airbyte_job_details_no_started_at_raises(mock_get_job_info_without_logs):
    job_id = "999"
    job_info = {
        "job": {
            "id": job_id,
            "configType": "sync",
            "configId": "conn-4",
            "status": "succeeded",
            "resetConfig": None,
            "refreshConfig": None,
            "streamAggregatedStats": {},
            "aggregatedStats": {},
            "createdAt": 1700000000,
        },
        "attempts": [
            {"attempt": {"id": 1, "createdAt": None, "endedAt": 1700001000}},
        ],
    }
    mock_get_job_info_without_logs.return_value = job_info

    with pytest.raises(Exception, match="No startedAt found for job_id=999"):
        fetch_and_update_airbyte_job_details(job_id)


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_jobs_for_connection")
@patch("ddpui.ddpairbyte.airbytehelpers.fetch_and_update_airbyte_job_details")
def test_fetch_and_update_airbyte_jobs_for_all_connections(
    mock_fetch_and_update_airbyte_job_details, mock_get_jobs_for_connection
):
    """tests fetch_and_update_airbyte_jobs_for_all_connections"""
    org = Org.objects.create(name="org", slug="org")
    synctask = Task.objects.create(type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync")
    OrgTask.objects.create(org=org, task=synctask, connection_id="connection_id")

    mock_get_jobs_for_connection.return_value = {
        "jobs": [{"job": {"id": "test_job_id"}}],
        "totalJobCount": 1,
    }

    fetch_and_update_airbyte_jobs_for_all_connections(org=org, last_n_days=1)

    mock_get_jobs_for_connection.assert_called_once()
    mock_fetch_and_update_airbyte_job_details.assert_called_once_with("test_job_id")


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_breaking_change(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with a breaking change"""
    connection_id = "test_connection_id"

    mock_get_connection_catalog.return_value = {
        "schemaChange": "breaking",
        "catalogDiff": {"transforms": [{"stream_name": "test_stream"}]},
    }

    OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
    assert not OrgSchemaChange.objects.filter(connection_id=connection_id).exists()
    fetch_and_update_org_schema_changes(org_with_workspace, connection_id)

    assert OrgSchemaChange.objects.filter(connection_id=connection_id).exists()
    OrgSchemaChange.objects.filter(connection_id=connection_id).delete()


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_non_breaking_change_with_diff(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with a non-breaking change and a diff"""
    connection_id = "test_connection_id"

    mock_get_connection_catalog.return_value = {
        "schemaChange": "non_breaking",
        "catalogDiff": {"transforms": [{"stream_name": "test_stream"}]},
    }

    OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
    assert not OrgSchemaChange.objects.filter(connection_id=connection_id).exists()
    fetch_and_update_org_schema_changes(org_with_workspace, connection_id)

    assert OrgSchemaChange.objects.filter(connection_id=connection_id).exists()
    OrgSchemaChange.objects.filter(connection_id=connection_id).delete()


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_non_breaking_change_without_diff(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with a non-breaking change and no diff"""
    connection_id = "test_connection_id"
    OrgSchemaChange.objects.create(
        org=org_with_workspace, connection_id=connection_id, change_type="breaking"
    )

    mock_get_connection_catalog.return_value = {
        "schemaChange": "non_breaking",
        "catalogDiff": {"transforms": []},
    }

    fetch_and_update_org_schema_changes(org_with_workspace, connection_id)

    # no transforms, no schema change
    assert not OrgSchemaChange.objects.filter(connection_id=connection_id).exists()


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_no_change(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with no change"""
    connection_id = "test_connection_id"
    OrgSchemaChange.objects.create(
        org=org_with_workspace, connection_id=connection_id, change_type="breaking"
    )

    mock_get_connection_catalog.return_value = {
        "schemaChange": "no_change",
        "catalogDiff": {},
    }

    fetch_and_update_org_schema_changes(org_with_workspace, connection_id)

    # no change
    assert not OrgSchemaChange.objects.filter(connection_id=connection_id).exists()


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_invalid_change_type(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with an invalid change type"""
    connection_id = "test_connection_id"

    mock_get_connection_catalog.return_value = {
        "schemaChange": "invalid_change_type",
        "catalogDiff": {},
    }

    err, result = fetch_and_update_org_schema_changes(org_with_workspace, connection_id)
    assert err is None
    assert "Something went wrong" in result


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog")
def test_fetch_and_update_org_schema_changes_api_error(
    mock_get_connection_catalog,
    org_with_workspace,
):
    """tests fetch_and_update_org_schema_changes with an API error"""
    connection_id = "test_connection_id"

    mock_get_connection_catalog.side_effect = Exception("API Error")

    _, error = fetch_and_update_org_schema_changes(org_with_workspace, connection_id)

    assert "Something went wrong" in error


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
def test_update_destination_no_dbt_workspace(
    mock_create_or_update_org_cli_block: Mock,
    mock_update_warehouse_credentials: Mock,
    mock_retrieve_warehouse_credentials: Mock,
    mock_update_destination: Mock,
):
    """test update_destination when dbt workspace is not setup (org.dbt is None)"""
    # Create org without dbt workspace
    org = Org.objects.create(name="org", slug="org", dbt=None)
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_update_destination.return_value = {
        "destinationId": "DESTINATION_ID",
    }
    mock_retrieve_warehouse_credentials.return_value = {"host": "localhost", "port": "5432"}
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="new-name",
        destinationDefId="destinationDefId",
        config={"host": "newhost", "port": "5433"},
    )

    response, error = update_destination(org, "destination_id", payload)

    # Should complete successfully
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    # Verify warehouse name was updated
    warehouse.refresh_from_db()
    assert warehouse.name == "new-name"

    # Verify warehouse credentials were updated
    mock_update_warehouse_credentials.assert_called_once_with(
        warehouse,
        {"host": "newhost", "port": "5433"},
    )

    # Verify create_or_update_org_cli_block was NOT called since dbt workspace is not setup
    mock_create_or_update_org_cli_block.assert_not_called()
