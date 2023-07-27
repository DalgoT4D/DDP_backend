import os
import django
from unittest.mock import patch, Mock
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.ddpprefect.prefect_service import (
    prefect_get,
    prefect_put,
    prefect_post,
    prefect_delete_a_block,
    HttpError,
    get_airbyte_server_block_id,
    get_airbye_connection_blocks,
    update_airbyte_server_block,
    update_airbyte_connection_block,
    get_airbyte_connection_block_by_id,
    get_airbyte_connection_block_id,
    get_dbtcore_block_id,
    create_airbyte_server_block,
    delete_airbyte_server_block,
    create_airbyte_connection_block,
    PrefectAirbyteConnectionSetup,
    delete_airbyte_server_block,
    delete_airbyte_connection_block,
    post_prefect_blocks_bulk_delete,
    get_shell_block_id,
    PrefectShellSetup,
    create_shell_block,
    delete_shell_block,
    PrefectDbtCoreSetup,
    create_dbt_core_block,
    delete_dbt_core_block,
    PrefectSecretBlockCreate,
    create_secret_block,
    delete_secret_block,
    update_dbt_core_block_credentials,
    update_dbt_core_block_schema,
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
    mock_post.return_value = {"block_id": "the-block-id"}
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
    assert response == "the-block-id"


def test_update_airbyte_server_block():
    with pytest.raises(Exception) as excinfo:
        update_airbyte_server_block("blockname")
    assert str(excinfo.value) == "not implemented"


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_airbyte_server_block(mock_delete: Mock):
    delete_airbyte_server_block("blockid")
    mock_delete.assert_called_once_with("blockid")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_airbyte_connection_block_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid"}
    response = get_airbyte_connection_block_id("blockname")
    assert response == "theblockid"
    mock_get.assert_called_once_with("blocks/airbyte/connection/byblockname/blockname")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_get_airbye_connection_blocks(mock_post: Mock):
    mock_post.return_value = "blocks-blocks-blocks"
    response = get_airbye_connection_blocks(["blockname1", "blockname2"])
    assert response == "blocks-blocks-blocks"
    mock_post.assert_called_once_with(
        "blocks/airbyte/connection/filter",
        {"block_names": ["blockname1", "blockname2"]},
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_airbyte_connection_block_by_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid", "blockname": "theblockname"}
    response = get_airbyte_connection_block_by_id("blockid")
    assert response == {"block_id": "theblockid", "blockname": "theblockname"}
    mock_get.assert_called_once_with("blocks/airbyte/connection/byblockid/blockid")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_airbyte_connection_block(mock_post: Mock):
    mock_post.return_value = {"block_id": "block-id"}
    conninfo = PrefectAirbyteConnectionSetup(
        serverBlockName="srvr-block-name",
        connectionId="conn-id",
        connectionBlockName="conn-block-name",
    )
    response = create_airbyte_connection_block(conninfo)
    assert response == "block-id"
    mock_post.assert_called_once_with(
        "blocks/airbyte/connection/",
        {
            "serverBlockName": conninfo.serverBlockName,
            "connectionId": conninfo.connectionId,
            "connectionBlockName": conninfo.connectionBlockName,
        },
    )


def test_update_airbyte_connection_block():
    with pytest.raises(Exception) as excinfo:
        update_airbyte_connection_block("blockname")
    assert str(excinfo.value) == "not implemented"


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_airbyte_connection_block(mock_delete: Mock):
    delete_airbyte_connection_block("blockid")
    mock_delete.assert_called_once_with("blockid")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_post_prefect_blocks_bulk_delete(mock_post: Mock):
    mock_post.return_value = "retval"
    response = post_prefect_blocks_bulk_delete([1, 2, 3])
    assert response == "retval"
    mock_post.assert_called_once_with("blocks/bulk/delete/", {"block_ids": [1, 2, 3]})


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_shell_block_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid"}
    response = get_shell_block_id("blockname")
    assert response == "theblockid"
    mock_get.assert_called_once_with("blocks/shell/blockname")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_shell_block(mock_post: Mock):
    mock_post.return_value = {"block_id": "block-id"}
    shellinfo = PrefectShellSetup(
        blockname="theblockname",
        commands=["c1", "c2"],
        env={"ekey": "eval"},
        workingDir="/working/dir",
    )
    response = create_shell_block(shellinfo)
    assert response == {"block_id": "block-id"}
    mock_post.assert_called_once_with(
        "blocks/shell/",
        {
            "blockName": "theblockname",
            "commands": ["c1", "c2"],
            "env": {"ekey": "eval"},
            "workingDir": "/working/dir",
        },
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_shell_block(mock_delete: Mock):
    delete_shell_block("blockid")
    mock_delete.assert_called_once_with("blockid")


# =============================================================================
@patch("ddpui.ddpprefect.prefect_service.prefect_get")
def test_get_dbtcore_block_id(mock_get: Mock):
    mock_get.return_value = {"block_id": "theblockid"}
    response = get_dbtcore_block_id("blockname")
    assert response == "theblockid"
    mock_get.assert_called_once_with("blocks/dbtcore/blockname")


@patch("ddpui.ddpprefect.prefect_service.prefect_post")
def test_create_dbt_core_block(mock_post: Mock):
    mock_post.return_value = "retval"
    dbtcore = PrefectDbtCoreSetup(
        block_name="theblockname",
        working_dir="/working/dir",
        profiles_dir="/profiles/dir",
        project_dir="/project/dir",
        env={"ekey": "eval"},
        commands=["c1", "c2"],
    )
    response = create_dbt_core_block(
        dbtcore,
        "profilename",
        "target",
        "wtype",
        credentials={"c1": "c2"},
        bqlocation=None,
    )
    assert response == "retval"
    mock_post.assert_called_once_with(
        "blocks/dbtcore/",
        {
            "blockName": dbtcore.block_name,
            "profile": {
                "name": "profilename",
                "target": "target",
                "target_configs_schema": "target",
            },
            "wtype": "wtype",
            "credentials": {"c1": "c2"},
            "bqlocation": None,
            "commands": dbtcore.commands,
            "env": dbtcore.env,
            "working_dir": dbtcore.working_dir,
            "profiles_dir": dbtcore.profiles_dir,
            "project_dir": dbtcore.project_dir,
        },
    )


@patch("ddpui.ddpprefect.prefect_service.prefect_delete_a_block")
def test_delete_dbt_core_block(mock_delete: Mock):
    delete_dbt_core_block("blockid")
    mock_delete.assert_called_once_with("blockid")


@patch("ddpui.ddpprefect.prefect_service.prefect_put")
def test_update_dbt_core_block_credentials(mock_put: Mock):
    mock_put.return_value = "retval"
    response = update_dbt_core_block_credentials(
        "wtype",
        "block_name",
        {"c1": "c2"},
    )

    assert response == "retval"
    mock_put.assert_called_once_with(
        "blocks/dbtcore_edit/wtype/",
        {
            "blockName": "block_name",
            "credentials": {"c1": "c2"},
        },
    )


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
