from unittest.mock import patch, Mock
import pytest
from ddpui.ddpairbyte.airbytehelpers import (
    add_custom_airbyte_connector,
    get_connection_catalog,
    update_connection_schema,
    upgrade_custom_sources,
    setup_airbyte_workspace_v1,
    AIRBYTESERVER,
    get_job_info_for_connection,
    update_destination,
    delete_source,
    get_sync_job_history_for_connection,
)
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionSchemaUpdate,
    AirbyteDestinationUpdate,
)
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE

pytestmark = pytest.mark.django_db


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_custom_source_definition"
)
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
    assert (
        OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
    )
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
    assert (
        OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
    )
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
    task = Task.objects.create(
        type="airbyte", slug="airbyte-sync", label="AIRBYTE sync"
    )
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
    task = Task.objects.create(
        type="airbyte", slug="airbyte-sync", label="AIRBYTE sync"
    )
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
                        "status": "failed",
                        "recordsSynced": 0,
                    },
                    {
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
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_logs_for_job",
    mock_get_logs_for_job=Mock(),
)
def test_get_sync_history_for_connection_no_jobs(
    mock_get_logs_for_job: Mock, mock_get_jobs_for_connection: Mock
):
    """tests get_sync_job_history_for_connection for success"""
    org = Org.objects.create(name="org", slug="org")
    task = Task.objects.create(
        type="airbyte", slug="airbyte-sync", label="AIRBYTE sync"
    )
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
                        "status": "failed",
                        "recordsSynced": 0,
                    },
                    {
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

    result, error = get_sync_job_history_for_connection(org, "connection_id")
    assert error is None
    assert "history" in result
    assert len(result["history"]) == 1
    assert result["history"][0]["logs"] == [
        "line1",
        "line2",
    ]
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
    task = Task.objects.create(
        type="airbyte", slug="airbyte-sync", label="AIRBYTE sync"
    )
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
def test_update_destination_name(
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
    mock_retrieve_warehouse_credentials.return_value = None
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="new-name", destinationDefId="destinationDefId", config={}
    )
    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    warehouse.refresh_from_db()

    assert warehouse.name == "new-name"


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
def test_update_destination_postgres_config(
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
def test_update_destination_bigquery_config(
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
def test_update_destination_snowflake_config(
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
    mock_retrieve_warehouse_credentials.return_value = {
        "credentials": {"password": "*****"}
    }
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
    "ddpui.ddpprefect.prefect_service.update_dbt_cli_profile_block",
    mock_update_dbt_cli_profile_block=Mock(),
)
def test_update_destination_cliprofile(
    mock_update_dbt_cli_profile_block: Mock,
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
    mock_retrieve_warehouse_credentials.return_value = {
        "credentials": {"password": "*****"}
    }
    mock_update_warehouse_credentials.return_value = None

    payload = AirbyteDestinationUpdate(
        name="name",
        destinationDefId="destinationDefId",
        config={
            "credentials": {"password": "newpassword"},
            "dataset_location": "LOCATIUON",
        },
    )

    OrgPrefectBlockv1.objects.create(
        org=org, block_type=DBTCLIPROFILE, block_name="cliblockname"
    )

    response, error = update_destination(org, "destination_id", payload)
    assert error is None
    assert response == {"destinationId": "DESTINATION_ID"}

    mock_update_dbt_cli_profile_block.assert_called_once_with(
        block_name="cliblockname",
        wtype=warehouse.wtype,
        credentials={"credentials": {"password": "newpassword"}},
        bqlocation="LOCATIUON",
    )


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

    task = Task.objects.create(
        type="airbyte", slug="airbyte-sync", label="AIRBYTE sync"
    )
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
    assert not OrgTask.objects.filter(
        org=org, task=task, connection_id="connection_id"
    ).exists()
    # but this one was not deleted
    assert OrgTask.objects.filter(
        org=org, task=task, connection_id="connection_id2"
    ).exists()

    mock_delete_source.assert_called_once()


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog",
    mock_get_connection_catalog=Mock(),
)
def test_get_connection_catalog(
    mock_get_connection_catalog: Mock,
):
    """test get_connection_catalog"""
    org = Org.objects.create(name="org", slug="org")

    mock_get_connection_catalog.return_value = {
        "name": "Test Connection",
        "connectionId": "connection_id",
        "catalogId": "catalog_id",
        "syncCatalog": True,
        "schemaChange": False,
        "catalogDiff": [],
    }

    result, error = get_connection_catalog(org, "connection_id")

    assert error is None
    assert result == {
        "name": "Test Connection",
        "connectionId": "connection_id",
        "catalogId": "catalog_id",
        "syncCatalog": True,
        "schemaChange": False,
        "catalogDiff": [],
    }


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection_catalog",
    mock_get_connection_catalog=Mock(),
)
def test_get_connection_catalog_fail(
    mock_get_connection_catalog: Mock,
):
    """Test for failing get_connection_catalog"""
    org_save = Mock()
    org = Mock(save=org_save)
    mock_get_connection_catalog.side_effect = Exception("error")

    res, error = get_connection_catalog(org, "connection_name")

    assert res is None
    assert error == "Error getting catalog for connection connection_name: error"


@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.get_connection")
@patch("ddpui.ddpairbyte.airbytehelpers.airbyte_service.update_schema_change")
def test_update_schema_changes_connection(
    mock_update_schema_change, mock_get_connection, db
):
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
    mock_get_connection.assert_called_once_with(
        org.airbyte_workspace_id, "test-connection-id"
    )
