from unittest.mock import patch, Mock
import os
import yaml
from datetime import datetime
from pathlib import Path
import pytest
import pytz
from ninja.errors import HttpError
from ddpui.ddpairbyte.airbytehelpers import (
    add_custom_airbyte_connector,
    update_connection_schema,
    upgrade_custom_sources,
    setup_airbyte_workspace_v1,
    AIRBYTESERVER,
    get_job_info_for_connection,
    update_destination,
    delete_source,
    get_sync_job_history_for_connection,
    create_or_update_org_cli_block,
    schedule_update_connection_schema,
)
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionSchemaUpdate,
    AirbyteDestinationUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
)
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgWarehouse,
    OrgDbt,
    TransformType,
    ConnectionJob,
    ConnectionMeta,
    OrgSchemaChange,
)
from ddpui.models.org_user import OrgUser, OrgUserRole, User
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE


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
    print("creating org_without_workspace")
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


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
        "workspace_id", "KoboToolbox", "tech4dev/source-kobotoolbox", "0.2.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "tech4dev/source-avni", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "CommCare T4D", "tech4dev/source-commcare", "0.1.0", ""
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
        "workspace_id", "KoboToolbox", "tech4dev/source-kobotoolbox", "0.2.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "tech4dev/source-avni", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "CommCare T4D", "tech4dev/source-commcare", "0.1.0", ""
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
    mock_get_logs_for_job.return_value = {
        "logs": {
            "logLines": [
                "line1",
                "line2",
            ],
        },
    }

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
    mock_get_logs_for_job.return_value = {
        "logs": {
            "logLines": [
                "line1",
                "line2",
            ],
        },
    }

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
    mock_get_logs_for_job.return_value = {
        "logs": {
            "logLines": [
                "line1",
                "line2",
            ],
        },
    }

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert result == []


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
def test_update_destination_name(
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
    mock_create_or_update_org_cli_block.return_value = ((None, None), None)

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
def test_update_destination_postgres_config(
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
def test_update_destination_bigquery_config(
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
def test_update_destination_snowflake_config(
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
def test_update_destination_cliprofile(
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

    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_create_or_update_org_cli_block.assert_called_once_with(org, warehouse, payload.config)


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

    task = Task.objects.create(type="airbyte", slug="airbyte-sync", label="AIRBYTE sync")
    orgtask = OrgTask.objects.create(org=org, task=task, connection_id="connection_id")
    OrgTask.objects.create(org=org, task=task, connection_id="connection_id2")

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
    assert not OrgTask.objects.filter(org=org, task=task, connection_id="connection_id").exists()
    # but this one was not deleted
    assert OrgTask.objects.filter(org=org, task=task, connection_id="connection_id2").exists()

    mock_delete_source.assert_called_once()


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection")
@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.update_schema_change")
def test_update_schema_changes_connection(mock_update_schema_change, mock_get_connection, db):
    """
    Test the update_connection_schema function.
    """
    # Create a test org
    org = Org.objects.create(name="Test Org", slug="test-org")

    # Create a test task
    task = Task.objects.create(
        type="test-type",
        slug="test-slug",
        label="test-task",
        command="test-command",
        is_system=True,
    )

    # Create a test OrgTask
    org_task = OrgTask.objects.create(
        org=org,
        task=task,
        connection_id="test-connection-id",
        parameters={},
        generated_by="system",
    )

    # Create a test OrgWarehouse
    org_warehouse = OrgWarehouse.objects.create(
        wtype="bigquery",
        name="Test Warehouse",
        credentials='{"key": "value"}',
        org=org,
        airbyte_destination_id="test-airbyte-destination-id",
        bq_location="us-central1",
    )

    # Set up mock responses
    mock_get_connection.return_value = {"name": "Test Connection"}
    mock_update_schema_change.return_value = {"updated": True}, None

    # Call the function
    payload = AirbyteConnectionSchemaUpdate(
        name="Updated Connection",
        syncCatalog={},
        connectionId="test-connection-id",
        sourceCatalogId="test-source-catalog-id",
    )
    response, error = update_connection_schema(org, "test-connection-id", payload)

    # Assert the response
    assert response == ({"updated": True}, None)
    assert error is None

    # Assert the mock functions were called with the expected arguments
    mock_get_connection.assert_called_once_with(org.airbyte_workspace_id, "test-connection-id")


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

    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    project_dir = Path(tmp_path) / org.slug
    project_dir.mkdir(parents=True, exist_ok=True)
    dbtrepo_dir = project_dir / project_name
    dbtrepo_dir.mkdir(parents=True, exist_ok=True)
    dbt = OrgDbt.objects.create(
        project_dir=str(project_dir),
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
