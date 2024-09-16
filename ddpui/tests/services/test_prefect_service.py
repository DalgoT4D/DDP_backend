import os
import django
from unittest.mock import patch, Mock
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

pytestmark = pytest.mark.django_db


from ddpui.ddpprefect.prefect_service import (
    prefect_get,
    prefect_put,
    prefect_post,
    prefect_delete_a_block,
    HttpError,
    get_airbyte_server_block_id,
    update_airbyte_server_block,
    update_airbyte_connection_block,
    get_dbtcore_block_id,
    create_airbyte_server_block,
    delete_airbyte_server_block,
    delete_airbyte_connection_block,
    get_shell_block_id,
    PrefectDbtCoreSetup,
    create_dbt_core_block,
    delete_dbt_core_block,
    PrefectSecretBlockCreate,
    create_secret_block,
    delete_secret_block,
    update_dbt_core_block_credentials,
    update_dbt_core_block_schema,
    run_airbyte_connection_sync,
    PrefectAirbyteSync,
    run_dbt_core_sync,
    PrefectDbtCore,
    get_flow_runs_by_deployment_id,
    set_deployment_schedule,
    get_filtered_deployments,
    delete_deployment_by_id,
    get_deployment,
    get_flow_run_logs,
    get_flow_run,
    create_deployment_flow_run,
    create_dbt_cli_profile_block,
)

PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.requests.get")
def test_prefect_get_connection_error(mock_get: Mock):
    mock_get.side_effect = Exception("conn-error")
    with pytest.raises(HttpError) as excinfo:
        prefect_get("endpoint-1", timeout=1)
    assert str(excinfo.value) == "connection error"
    mock_get.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-1",
        headers={"x-ddp-org": ""},
        timeout=1,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.get")
def test_prefect_get_other_error(mock_get: Mock):
    mock_get.return_value = Mock(
        raise_for_status=Mock(side_effect=Exception("another error")),
        status_code=400,
        text="error-text",
    )
    with pytest.raises(HttpError) as excinfo:
        prefect_get("endpoint-2", timeout=2)
    assert str(excinfo.value) == "error-text"
    mock_get.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-2",
        headers={"x-ddp-org": ""},
        timeout=2,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.get")
def test_prefect_get_success(mock_get: Mock):
    mock_get.return_value = Mock(
        raise_for_status=Mock(), status_code=200, json=Mock(return_value={"k": "v"})
    )
    response = prefect_get("endpoint-3", timeout=3)
    assert response == {"k": "v"}
    mock_get.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-3",
        headers={"x-ddp-org": ""},
        timeout=3,
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.requests.post")
def test_prefect_post_connection_error(mock_post: Mock):
    mock_post.side_effect = Exception("conn-error")
    payload = {"k1": "v1", "k2": "v2"}
    with pytest.raises(HttpError) as excinfo:
        prefect_post("endpoint-1", payload, timeout=1)
    assert str(excinfo.value) == "connection error"
    mock_post.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-1",
        headers={"x-ddp-org": ""},
        timeout=1,
        json=payload,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.post")
def test_prefect_post_other_error(mock_post: Mock):
    mock_post.return_value = Mock(
        raise_for_status=Mock(side_effect=Exception("another error")),
        status_code=400,
        text="error-text",
    )
    payload = {"k1": "v1", "k2": "v2"}
    with pytest.raises(HttpError) as excinfo:
        prefect_post("endpoint-2", payload, timeout=2)
    assert str(excinfo.value) == "error-text"
    mock_post.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-2",
        headers={"x-ddp-org": ""},
        timeout=2,
        json=payload,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.post")
def test_prefect_post_success(mock_post: Mock):
    mock_post.return_value = Mock(
        raise_for_status=Mock(), status_code=200, json=Mock(return_value={"k": "v"})
    )
    payload = {"k1": "v1", "k2": "v2"}
    response = prefect_post("endpoint-3", payload, timeout=3)
    assert response == {"k": "v"}
    mock_post.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-3",
        headers={"x-ddp-org": ""},
        timeout=3,
        json=payload,
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.requests.put")
def test_prefect_put_connection_error(mock_put: Mock):
    mock_put.side_effect = Exception("conn-error")
    payload = {"k1": "v1", "k2": "v2"}
    with pytest.raises(HttpError) as excinfo:
        prefect_put("endpoint-1", payload, timeout=1)
    assert str(excinfo.value) == "connection error"
    mock_put.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-1",
        headers={"x-ddp-org": ""},
        timeout=1,
        json=payload,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.put")
def test_prefect_put_other_error(mock_put: Mock):
    mock_put.return_value = Mock(
        raise_for_status=Mock(side_effect=Exception("another error")),
        status_code=400,
        text="error-text",
    )
    payload = {"k1": "v1", "k2": "v2"}
    with pytest.raises(HttpError) as excinfo:
        prefect_put("endpoint-2", payload, timeout=2)
    assert str(excinfo.value) == "error-text"
    mock_put.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-2",
        headers={"x-ddp-org": ""},
        timeout=2,
        json=payload,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.put")
def test_prefect_put_success(mock_put: Mock):
    mock_put.return_value = Mock(
        raise_for_status=Mock(), status_code=200, json=Mock(return_value={"k": "v"})
    )
    payload = {"k1": "v1", "k2": "v2"}
    response = prefect_put("endpoint-3", payload, timeout=3)
    assert response == {"k": "v"}
    mock_put.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/proxy/endpoint-3",
        headers={"x-ddp-org": ""},
        timeout=3,
        json=payload,
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.requests.delete")
def test_prefect_delete_a_block_connection_error(mock_delete: Mock):
    mock_delete.side_effect = Exception("conn-error")
    with pytest.raises(HttpError) as excinfo:
        prefect_delete_a_block("blockid-1", timeout=1)
    assert str(excinfo.value) == "connection error"
    mock_delete.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/delete-a-block/blockid-1",
        headers={"x-ddp-org": ""},
        timeout=1,
    )


@patch("ddpui.ddpprefect.prefect_service.requests.delete")
def test_prefect_delete_a_block_other_error(mock_delete: Mock):
    mock_delete.return_value = Mock(
        raise_for_status=Mock(side_effect=Exception("another error")),
        status_code=400,
        text="error-text",
    )
    with pytest.raises(HttpError) as excinfo:
        prefect_delete_a_block("blockid-2", timeout=2)
    assert str(excinfo.value) == "error-text"
    mock_delete.assert_called_once_with(
        f"{PREFECT_PROXY_API_URL}/delete-a-block/blockid-2",
        headers={"x-ddp-org": ""},
        timeout=2,
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_airbyte_server_block_id(mock_get: Mock):
    blockname = "theblockname"
    mock_get.return_value = {"block_id": "the-block-id"}
    response = get_airbyte_server_block_id(blockname)
    mock_get.assert_called_once_with(f"blocks/airbyte/server/{blockname}")
    assert response == "the-block-id"


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_airbyte_server_block(mock_post: Mock):
    blockname = "theblockname"
    mock_post.return_value = {
        "block_id": "the-block-id",
        "cleaned_block_name": "theblockname",
    }
    response = create_airbyte_server_block(blockname)
    mock_post.assert_called_once_with(
        "blocks/airbyte/server/",
        {
            "blockName": blockname,
            "serverHost": os.getenv("AIRBYTE_SERVER_HOST"),
            "serverPort": os.getenv("AIRBYTE_SERVER_PORT"),
            "apiVersion": os.getenv("AIRBYTE_SERVER_APIVER"),
        },
    )
    assert response == ("the-block-id", "theblockname")


def test_update_airbyte_server_block():
    with pytest.raises(Exception) as excinfo:
        update_airbyte_server_block("blockname")
    assert str(excinfo.value) == "not implemented"


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_airbyte_server_block(mock_delete: Mock):
    delete_airbyte_server_block("blockid")
    mock_delete.assert_called_once_with("blockid")


# =============================================================================
def test_update_airbyte_connection_block():
    with pytest.raises(Exception) as excinfo:
        update_airbyte_connection_block("blockname")
    assert str(excinfo.value) == "not implemented"


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_airbyte_connection_block(mock_delete: Mock):
    delete_airbyte_connection_block("blockid")
    mock_delete.assert_called_once_with("blockid")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_shell_block_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid"}
    response = get_shell_block_id("blockname")
    assert response == "theblockid"
    mock_get.assert_called_once_with("blocks/shell/blockname")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_dbtcore_block_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid"}
    response = get_dbtcore_block_id("blockname")
    assert response == "theblockid"
    mock_get.assert_called_once_with("blocks/dbtcore/blockname")


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_dbt_core_block(mock_delete: Mock):
    delete_dbt_core_block("blockid")
    mock_delete.assert_called_once_with("blockid")


@patch("ddpui.ddpprefect.prefect_service.prefect_put")
def test_update_dbt_core_block_schema(mock_put: Mock):
    mock_put.return_value = "retval"
    response = update_dbt_core_block_schema("block_name", "target")

    assert response == "retval"
    mock_put.assert_called_once_with(
        "blocks/dbtcore_edit_schema/",
        {
            "blockName": "block_name",
            "target_configs_schema": "target",
        },
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_dbt_cli_profile_block(mock_post: Mock):
    mock_post.return_value = "retval"
    response = create_dbt_cli_profile_block(
        "block-name",
        "profilename",
        "target",
        "wtype",
        credentials={"c1": "c2"},
        bqlocation=None,
    )
    assert response == "retval"
    mock_post.assert_called_once_with(
        "blocks/dbtcli/profile/",
        {
            "cli_profile_block_name": "block-name",
            "profile": {
                "name": "profilename",
                "target": "target",
                "target_configs_schema": "target",
            },
            "wtype": "wtype",
            "credentials": {"c1": "c2"},
            "bqlocation": None,
        },
    )


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_secret_block(mock_post: Mock):
    mock_post.return_value = {"block_id": "block-id"}
    secret_block = PrefectSecretBlockCreate(block_name="bname", secret="secret")
    response = create_secret_block(secret_block)
    assert response == {"block_id": "block-id"}
    mock_post.assert_called_once_with(
        "blocks/secret/",
        {"blockName": "bname", "secret": "secret"},
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_secret_block(mock_delete: Mock):
    delete_secret_block("blockid")
    mock_delete.assert_called_once_with("blockid")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_flow_runs_by_deployment_id_limit(mock_get: Mock):
    mock_get.return_value = {"flow_runs": []}
    response = get_flow_runs_by_deployment_id("depid1", 100)
    assert response == []
    mock_get.assert_called_once_with(
        "flow_runs", params={"deployment_id": "depid1", "limit": 100}, timeout=60
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_flow_runs_by_deployment_id_nolimit(mock_get: Mock):
    mock_get.return_value = {"flow_runs": []}
    response = get_flow_runs_by_deployment_id("depid1")
    assert response == []
    mock_get.assert_called_once_with(
        "flow_runs", params={"deployment_id": "depid1", "limit": None}, timeout=60
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_flow_runs_by_deployment_id_insert_pfr(mock_get: Mock):
    mock_get.return_value = {
        "flow_runs": [
            {
                "id": "flowrunid",
                "name": "flowrunname",
                "startTime": "",
                "expectedStartTime": "2021-01-01T00:00:00.000Z",
                "totalRunTime": 10.0,
                "status": "COMPLETED",
                "state_name": "COMPLETED",
            }
        ]
    }
    response = get_flow_runs_by_deployment_id("depid1")
    assert len(response) == 1
    assert response[0]["deployment_id"] == "depid1"
    assert response[0]["id"] == "flowrunid"
    assert response[0]["name"] == "flowrunname"
    assert response[0]["startTime"] == "2021-01-01T00:00:00+00:00"
    assert response[0]["expectedStartTime"] == "2021-01-01T00:00:00+00:00"
    assert response[0]["totalRunTime"] == 10.0
    assert response[0]["status"] == "COMPLETED"
    assert response[0]["state_name"] == "COMPLETED"
    mock_get.assert_called_once_with(
        "flow_runs", params={"deployment_id": "depid1", "limit": None}, timeout=60
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_set_deployment_schedule(mock_post: Mock):
    set_deployment_schedule("depid1", "newstatus")
    mock_post.assert_called_once_with("deployments/depid1/set_schedule/newstatus", {})


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_get_filtered_deployments(mock_post: Mock):
    mock_post.return_value = {"deployments": ["deployments"]}
    response = get_filtered_deployments("org", ["depid1", "depid2"])
    assert response == ["deployments"]
    mock_post.assert_called_once_with(
        "deployments/filter",
        {"org_slug": "org", "deployment_ids": ["depid1", "depid2"]},
    )


@patch("ddpui.ddpprefect.prefect_service.requests.delete")
def test_delete_deployment_by_id_error(mock_delete: Mock):
    mock_delete.return_value = Mock(
        raise_for_status=Mock(side_effect=Exception("error")),
        status_code=400,
        text="errortext",
    )
    with pytest.raises(HttpError) as excinfo:
        delete_deployment_by_id("depid")
    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "errortext"


@patch("ddpui.ddpprefect.prefect_service.requests.delete")
def test_delete_deployment_by_id_success(mock_delete: Mock):
    mock_delete.return_value = Mock(
        raise_for_status=Mock(),
    )
    response = delete_deployment_by_id("depid")
    assert response["success"] == 1


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_deployment(mock_get: Mock):
    mock_get.return_value = "retval"
    response = get_deployment("depid")
    assert response == "retval"
    mock_get.assert_called_once_with("deployments/depid")


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_flow_run_logs(mock_get: Mock):
    mock_get.return_value = "the-logs"
    response = get_flow_run_logs("flowrunid","taskrunid", 10, 3)
    assert response == {"logs": "the-logs"}
    mock_get.assert_called_once_with("flow_runs/logs/flowrunid", params={"offset": 3, "limit": 10, "task_run_id": "taskrunid"})


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_flow_run(mock_get: Mock):
    mock_get.return_value = "retval"
    response = get_flow_run("flowrunid")
    assert response == "retval"
    mock_get.assert_called_once_with("flow_runs/flowrunid")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_deployment_flow_run(mock_post: Mock):
    mock_post.return_value = "retval"
    response = create_deployment_flow_run("depid")
    assert response == "retval"
    mock_post.assert_called_once_with("deployments/depid/flow_run", {})
