import os
import django

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org_user import User, OrgUser
from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow, OrgWarehouse
from ddpui.models.orgjobs import BlockLock, DataflowBlock
from ddpui.models.org import OrgDbt, Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.api.client.airbyte_api import (
    get_airbyte_connection_v1,
    get_airbyte_connections_v1,
    post_airbyte_connection_v1,
)
from ddpui.ddpairbyte.schema import AirbyteWorkspace, AirbyteConnectionCreate
from ddpui import ddpprefect

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

    result = get_airbyte_connections_v1(mock_request)
    assert len(result) == 1

    # assert result[0]["name"] == "fake-display-name"
    # assert result[0]["blockId"] == "fake-block-id"
    # assert result[0]["blockName"] == "fake-block-name"
    # assert result[0]["blockData"]["connection_id"] == "fake-connection-id"
    # assert result[0]["source"]["id"] == "fake-source-id-1"
    # assert result[0]["source"]["sourceName"] == "fake-source-name-1"
    # assert result[0]["destination"]["id"] == "fake-destination-id-1"
    # assert result[0]["destination"]["destinationName"] == "fake-destination-name-1"
    # assert result[0]["catalogId"] == "fake-source-catalog-id-1"
    # assert result[0]["syncCatalog"] == "sync-catalog"
    # assert result[0]["status"] == "conn-status"
    # assert result[0]["deploymentId"] == "fake-deployment-id"
    # assert result[0]["lastRun"] == {
    #     "flow-run-id": "00000",
    #     "startTime": 0,
    #     "expectedStartTime": 0,
    # }
    # assert result[0]["lock"] is None
    assert result[0]["name"] == "fake-conn"
    assert result[0]["source"]["id"] == "fake-source-id-1"
    assert result[0]["source"]["name"] == "fake-source-name-1"
    assert result[0]["destination"]["id"] == "fake-destination-id-1"
    assert result[0]["destination"]["name"] == "fake-destination-name-1"
    assert result[0]["catalogId"] == "fake-source-catalog-id-1"
    assert result[0]["syncCatalog"] == "sync-catalog"
    assert result[0]["status"] == "conn-status"
    assert result[0]["deploymentId"] == "fake-deployment-id"


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

    with patch("ddpui.api.client.airbyte_api.write_dataflowblocks"):
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
