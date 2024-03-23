import os
from unittest.mock import MagicMock, Mock, patch
from django.core.management import call_command

import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui import ddpprefect
from ddpui.api.airbyte_api import (
    delete_airbyte_connection_v1,
    delete_airbyte_source_v1,
    get_airbyte_connection_v1,
    get_airbyte_connections_v1,
    get_latest_job_for_connection,
    post_airbyte_connection_reset_v1,
    post_airbyte_connection_v1,
    post_airbyte_workspace_v1,
    put_airbyte_connection_v1,
    put_airbyte_destination_v1,
)
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
    AirbyteWorkspace,
    AirbyteWorkspaceCreate,
)
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task

pytestmark = pytest.mark.django_db


# ================================================================================
@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    print("creating org_without_workspace")
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


# ================================================================================
def test_get_airbyte_connection_v1_without_workspace(org_without_workspace):
    """tests GET /v1/connections/{connection_id} failure with no workspace"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connection_v1(mock_request, "fake-conn-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(
        return_value={
            "name": "fake-conn",
            "sourceId": "fake-source-id-1",
            "connectionId": "fake-connection-id-1",
            "destinationId": "fake-destination-id-1",
            "catalogId": "fake-source-catalog-id-1",
            "syncCatalog": "sync-catalog",
            "namespaceDefinition": "namespace-definition",
            "status": "conn-status",
            "source": {"id": "fake-source-id-1", "sourceName": "fake-source-name-1"},
            "destination": {
                "id": "fake-destination-id-1",
                "destinationName": "fake-destination-name-1",
            },
        }
    ),
)
def test_get_airbyte_connection_v1_success(org_with_workspace):
    """tests GET /v1/connections/{connection_id} success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=mock_orguser.org, connection_id="fake-conn-id"
    )

    dataflow = OrgDataFlowv1.objects.create(
        org=org_with_workspace,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
    )

    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    result = get_airbyte_connection_v1(mock_request, "fake-conn-id")

    assert result["name"] == "fake-conn"
    assert result["source"]["id"] == "fake-source-id-1"
    assert result["source"]["name"] == "fake-source-name-1"
    assert result["destination"]["id"] == "fake-destination-id-1"
    assert result["destination"]["name"] == "fake-destination-name-1"
    assert result["catalogId"] == "fake-source-catalog-id-1"
    assert result["syncCatalog"] == "sync-catalog"
    assert result["status"] == "conn-status"
    assert result["deploymentId"] == "fake-deployment-id"


# ================================================================================
def test_get_airbyte_connections_v1_without_workspace(org_without_workspace):
    """tests GET /v1/connections failure with no workspace"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connections_v1(mock_request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(
        return_value={
            "name": "fake-conn",
            "sourceId": "fake-source-id-1",
            "connectionId": "fake-connection-id-1",
            "destinationId": "fake-destination-id-1",
            "catalogId": "fake-source-catalog-id-1",
            "syncCatalog": "sync-catalog",
            "status": "conn-status",
            "source": {"id": "fake-source-id-1", "name": "fake-source-name-1"},
            "destination": {
                "id": "fake-destination-id-1",
                "name": "fake-destination-name-1",
            },
        }
    ),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_last_flow_run_by_deployment_id=Mock(
        return_value={
            "flow-run-id": "00000",
            "startTime": 0,
            "expectedStartTime": 0,
        }
    ),
)
def test_get_airbyte_connections_success(org_with_workspace):
    """tests GET /v1/connections success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=mock_orguser.org, connection_id="fake-conn-id"
    )

    dataflow = OrgDataFlowv1.objects.create(
        org=org_with_workspace,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
    )

    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    OrgWarehouse.objects.create(org=org_with_workspace, name="fake-warehouse-name")

    result = get_airbyte_connections_v1(mock_request)
    assert len(result) == 1

    assert result[0]["name"] == "fake-conn"
    assert result[0]["source"]["id"] == "fake-source-id-1"
    assert result[0]["source"]["name"] == "fake-source-name-1"
    assert result[0]["destination"]["id"] == "fake-destination-id-1"
    assert result[0]["destination"]["name"] == "fake-warehouse-name"
    assert result[0]["catalogId"] == "fake-source-catalog-id-1"
    assert result[0]["syncCatalog"] == "sync-catalog"
    assert result[0]["status"] == "conn-status"
    assert result[0]["deploymentId"] == "fake-deployment-id"
    assert result[0]["lastRun"] == {
        "flow-run-id": "00000",
        "startTime": 0,
        "expectedStartTime": 0,
    }
    assert result[0]["lock"] is None


# ================================================================================
def test_post_airbyte_connection_v1_without_workspace(org_without_workspace):
    """tests POST /v1/connections/ failure with no workspace"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream-1", "stream-2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_v1_without_stream_names(org_with_workspace):
    """tests POST /v1/connections/ failure with no streams passed in payload"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=[],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)

    assert str(excinfo.value) == "must specify stream names"


def test_post_airbyte_connection_v1_without_warehouse(org_with_workspace):
    """tests POST /v1/connections/ failure without warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)

    assert str(excinfo.value) == "need to set up a warehouse first"


@pytest.fixture
def warehouse_without_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(org=org_with_workspace)
    yield warehouse
    warehouse.delete()


@pytest.fixture
def warehouse_with_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    )
    yield warehouse
    warehouse.delete()


def test_post_airbyte_connection_v1_without_destination_id(
    org_with_workspace, warehouse_without_destination
):
    """tests POST /v1/connections/ failure without warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)

    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_normalization_operation=Mock(
        return_value={"operationId": "fake-operation-id"}
    ),
)
def test_post_airbyte_connection_v1_without_server_block(
    org_with_workspace, warehouse_with_destination
):
    """tests POST /v1/connections/ failure without server block"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(Exception) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)
    assert (
        str(excinfo.value)
        == "test-org-slug has no Airbyte Server block in OrgPrefectBlock"
    )


@pytest.fixture
def airbyte_server_block(org_with_workspace):
    block = OrgPrefectBlockv1.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake ab server block",
    )
    yield block
    block.delete()


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_normalization_operation=Mock(
        return_value={"operationId": "fake-operation-id"}
    ),
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
)
def test_post_airbyte_connection_v1_task_not_supported(
    org_with_workspace, warehouse_with_destination, airbyte_server_block
):
    """tests POST /v1/connections/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(Exception) as excinfo:
        post_airbyte_connection_v1(mock_request, payload)
    assert str(excinfo.value) == "task not supported"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_normalization_operation=Mock(
        return_value={"operationId": "fake-operation-id"}
    ),
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        return_value={
            "deployment": {"id": "fake-deployment-id", "name": "fake-deployment-name"}
        }
    ),
)
def test_post_airbyte_connection_v1_success(
    org_with_workspace, warehouse_with_destination, airbyte_server_block
):
    """tests POST /v1/connections/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    call_command("loaddata", "seed/tasks.json")
    for task in Task.objects.all():
        OrgTask.objects.create(org=org_with_workspace, task=task)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )

    # create the master task
    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    Task.objects.create(**airbyte_task_config)

    response = post_airbyte_connection_v1(mock_request, payload)

    # reload the warehouse from the database
    warehouse_with_destination = OrgWarehouse.objects.filter(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    ).first()
    assert warehouse_with_destination.airbyte_norm_op_id is not None

    assert response["name"] == "conn-name"
    assert response["connectionId"] == "fake-connection-id"
    assert response["source"]["id"] == "fake-source-id"
    assert response["destination"]["id"] == "fake-destination-id"
    assert response["catalogId"] == "fake-source-catalog-id"
    assert response["status"] == "running"
    assert response["deploymentId"] == "fake-deployment-id"


# ================================================================================
def test_post_airbyte_connection_reset_v1_no_workspace(org_without_workspace):
    """tests POST /v1/connections/{connection_id}/reset failure with no workspace"""
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_without_workspace

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset_v1(mock_request, "connection_id")
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_reset_v1_no_connection_task(org_with_workspace):
    """tests POST /v1/connections/{connection_id}/reset failure with connection task created"""
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset_v1(mock_request, "connection_id")
    assert str(excinfo.value) == "connection not found"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "the-connection-id"}}
    ),
)
def test_post_airbyte_connection_reset_v1_success(org_with_workspace):
    """tests POST /v1/connections/{connection_id}/reset success"""
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    connection_id = "connection_id"

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    reset_connection_mock = MagicMock()

    with patch(
        "ddpui.ddpairbyte.airbyte_service.reset_connection"
    ) as reset_connection_mock:
        result = post_airbyte_connection_reset_v1(mock_request, connection_id)
        assert result["success"] == 1

    reset_connection_mock.assert_called_once_with(connection_id)


# ================================================================================
def test_put_airbyte_connection_v1_no_workspace(org_without_workspace):
    """tests PUT /v1/connections/{connection_id}/update failure with no workspace"""
    payload = AirbyteConnectionUpdate(name="connection-name-1", streams=[])
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_without_workspace

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(mock_request, connection_id, payload)
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_put_airbyte_connection_v1_no_warehouse_destination(org_with_workspace):
    """tests PUT /v1/connections/{connection_id}/update failure with warehouse but no destination id"""
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    OrgWarehouse.objects.create(org=org_with_workspace)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(mock_request, connection_id, payload)
    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


def test_put_airbyte_connection_v1_no_warehouse(org_with_workspace):
    """tests PUT /v1/connections/{connection_id}/update failure with no warehouse"""
    payload = AirbyteConnectionUpdate(name="connection-name", streams=[1])
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(mock_request, connection_id, payload)
    assert str(excinfo.value) == "need to set up a warehouse first"


def test_put_airbyte_connection_v1_no_streams(
    org_with_workspace, warehouse_with_destination
):
    """tests PUT /v1/connections/{connection_id}/update failure with no streams"""
    payload = AirbyteConnectionUpdate(name="connection-name-1", streams=[])
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(mock_request, connection_id, payload)
    assert str(excinfo.value) == "must specify stream names"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"conn-key": "conn-val"}),
)
def test_put_airbyte_connection_v1_without_normalization(org_with_workspace):
    """tests PUT /v1/connections/{connection_id}/update succes with no normalization"""
    payload = AirbyteConnectionUpdate(name="connection-name", streams=[1])
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection_v1(mock_request, connection_id, payload)

    connection = {
        "conn-key": "conn-val",
        "operationIds": [],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"operationIds": [1]}),
)
def test_put_airbyte_connection_v1_with_normalization_with_opids(org_with_workspace):
    """tests PUT /v1/connections/{connection_id}/update succes with normalization with opids"""
    payload = AirbyteConnectionUpdate(
        name="connection-block-name", streams=[1], normalize=True
    )
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection_v1(mock_request, connection_id, payload)

    connection = {
        "operationIds": [1],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"operationIds": []}),
    create_normalization_operation=Mock(return_value={"operationId": "norm-op-id"}),
)
def test_put_airbyte_connection_v1_with_normalization_without_opids(org_with_workspace):
    """tests PUT /v1/connections/{connection_id}/update succes with normalization without opids"""
    payload = AirbyteConnectionUpdate(
        name="connection-block-name", streams=[1], normalize=True
    )
    connection_id = "connection_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection_v1(mock_request, connection_id, payload)

    warehouse.refresh_from_db()
    assert warehouse.airbyte_norm_op_id == "norm-op-id"

    connection = {
        "operationIds": [warehouse.airbyte_norm_op_id],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


# ================================================================================
def test_delete_airbyte_connection_v1_without_org():
    """tests DELETE /v1/connections/{connection_id} failure without org"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection_v1(mock_request, "conn-block-id")

    assert str(excinfo.value) == "create an organization first"


def test_delete_airbyte_connection_v1_without_workspace(org_without_workspace):
    """tests DELETE /v1/connections/{connection_id} failure without workspace"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection_v1(mock_request, "conn-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_deployment_by_id=Mock(),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    delete_connection=Mock(),
)
def test_delete_airbyte_connection_success(org_with_workspace):
    """tests DELETE /v1/connections/{connection_id} success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    connection_id = "conn-1"

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=mock_request.orguser.org, connection_id=connection_id
    )

    dataflow = OrgDataFlowv1.objects.create(
        name="conn-deployment",
        deployment_name="conn-deployment",
        deployment_id="conn-deployment-id",
        cron=None,
        dataflow_type="manual",
        org=mock_orguser.org,
    )

    DataflowOrgTask.objects.create(orgtask=org_task, dataflow=dataflow)

    assert (
        OrgTask.objects.filter(
            org=org_with_workspace, task=task, connection_id=connection_id
        ).count()
        == 1
    )
    response = delete_airbyte_connection_v1(mock_request, connection_id)
    assert response["success"] == 1
    assert OrgTask.objects.filter(org=org_with_workspace).count() == 0
    assert OrgDataFlowv1.objects.filter(org=org_with_workspace).count() == 0
    assert DataflowOrgTask.objects.filter(orgtask__org=org_with_workspace).count() == 0


def test_post_airbyte_workspace_with_existing_workspace(org_with_workspace):
    """Tests post_airbyte_workspace_v1 when organization already has a workspace"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = org_with_workspace.airbyte_workspace_id

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteWorkspaceCreate(name="New Workspace")

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_workspace_v1(mock_request, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "org already has a workspace"


def test_post_airbyte_workspace_success():
    """Tests post_airbyte_workspace_v1"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = None

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteWorkspaceCreate(name="New Workspace")

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.setup_airbyte_workspace_v1"
    ) as setup_workspace_mock:
        setup_workspace_mock.return_value = AirbyteWorkspace(
            workspaceId=1, name="New Workspace", initialSetupComplete=True
        )

        workspace = post_airbyte_workspace_v1(mock_request, payload)

    setup_workspace_mock.assert_called_once_with(payload.name, mock_org)

    assert isinstance(workspace, AirbyteWorkspace)
    assert workspace.name == "New Workspace"


def test_get_latest_job_for_connection_with_no_workspace():
    """Tests get_latest_job_for_connection when organization has no workspace"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = None

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    connection_id = "connection_123"

    with pytest.raises(HttpError) as excinfo:
        get_latest_job_for_connection(mock_request, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_get_latest_job_for_connection_with_workspace():
    """Tests get_latest_job_for_connection when organization has a workspace"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = "workspace_123"

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    connection_id = "connection_123"

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.get_job_info_for_connection"
    ) as get_job_info_mock:
        job_info = {"status": "success", "details": "Job completed successfully"}
        get_job_info_mock.return_value = (job_info, None)

        returned_job_info = get_latest_job_for_connection(mock_request, connection_id)

    get_job_info_mock.assert_called_once_with(mock_org, connection_id)

    assert returned_job_info == job_info


def test_get_latest_job_for_connection_with_error():
    """Tests get_latest_job_for_connection when job information retrieval returns an error"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = "workspace_123"

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    connection_id = "connection_123"

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.get_job_info_for_connection"
    ) as get_job_info_mock:
        error_message = "Failed to retrieve job information"
        get_job_info_mock.return_value = (None, error_message)

        with pytest.raises(HttpError) as excinfo:
            get_latest_job_for_connection(mock_request, connection_id)

    get_job_info_mock.assert_called_once_with(mock_org, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == error_message


def test_put_airbyte_destination_with_no_organization():
    """Tests put_airbyte_destination_v1 when organization is missing"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_destination_v1(mock_request, destination_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an organization first"


def test_put_airbyte_destination_with_no_workspace():
    """Tests put_airbyte_destination_v1 when organization has no workspace"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = None

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_destination_v1(mock_request, destination_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_put_airbyte_destination_success():
    """Tests put_airbyte_destination_v1 when updating destination is successful"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = "workspace_123"

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.update_destination"
    ) as update_destination_mock:
        updated_destination = {
            "destinationId": destination_id,
            "name": "Updated Destination",
        }
        update_destination_mock.return_value = (updated_destination, None)

        response = put_airbyte_destination_v1(mock_request, destination_id, payload)

    update_destination_mock.assert_called_once_with(mock_org, destination_id, payload)

    assert response == {"destinationId": destination_id}


def test_delete_airbyte_source_with_no_workspace():
    """Tests delete_airbyte_source_v1 when organization has no workspace"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = None

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    source_id = "source_123"

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_source_v1(mock_request, source_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_delete_airbyte_source_success():
    """Tests delete_airbyte_source_v1 when deleting source is successful"""
    mock_org = Mock()
    mock_org.airbyte_workspace_id = "workspace_123"

    mock_orguser = Mock()
    mock_orguser.org = mock_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    source_id = "source_123"

    with patch("ddpui.ddpairbyte.airbytehelpers.delete_source") as delete_source_mock:
        delete_source_mock.return_value = (None, None)

        response = delete_airbyte_source_v1(mock_request, source_id)

    delete_source_mock.assert_called_once_with(mock_org, source_id)

    assert response == {"success": 1}
