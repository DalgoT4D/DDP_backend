from unittest.mock import patch, Mock
import pytest
from ddpui.ddpairbyte.airbytehelpers import (
    add_custom_airbyte_connector,
    upgrade_custom_sources,
    setup_airbyte_workspace,
    AIRBYTESERVER,
)
from ddpui.models.org import Org, OrgPrefectBlock

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
                "name": "Kobotoolbox",
                "dockerRepository": "airbyte/source-kobotoolbox",
                "dockerImageTag": "0.0.0",
            },
            {
                "name": "Avni",
                "dockerRepository": "airbyte/source-avni",
                "dockerImageTag": "0.0.0",
            },
            {
                "name": "custom_commcare",
                "dockerRepository": "airbyte/source-commcare",
                "dockerImageTag": "0.0.0",
            },
        ]
    }
    upgrade_custom_sources("workspace_id")
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Kobotoolbox", "airbyte/source-kobotoolbox", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "airbyte/source-avni", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "custom_commcare", "airbyte/source-commcare", "0.1.1", ""
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
        "workspace_id", "Kobotoolbox", "airbyte/source-kobotoolbox", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "Avni", "airbyte/source-avni", "0.1.0", ""
    )
    mock_add_custom_airbyte_connector.assert_any_call(
        "workspace_id", "custom_commcare", "airbyte/source-commcare", "0.1.1", ""
    )


@patch(
    "ddpui.ddpairbyte.airbytehelpers.airbyte_service.create_workspace",
    mock_create_workspace=Mock(),
)
@patch(
    "ddpui.ddpairbyte.airbytehelpers.add_custom_airbyte_connector",
    mock_add_custom_airbyte_connector=Mock(),
)
def test_setup_airbyte_workspace_fail(
    mock_add_custom_airbyte_connector: Mock, create_workspace: Mock
):
    """failing test"""
    org_save = Mock()
    org = Mock(save=org_save)
    create_workspace.return_value = {
        "workspaceId": "wsid",
        "initialSetupComplete": False,
    }
    mock_add_custom_airbyte_connector.side_effect = Exception("error")
    with pytest.raises(Exception):
        setup_airbyte_workspace("workspace_name", org)
    org_save.assert_called_once()
    assert org.airbyte_workspace_id == "wsid"
    mock_add_custom_airbyte_connector.assert_called_once()


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
def test_setup_airbyte_workspace_fail_noproxy(
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
        setup_airbyte_workspace("workspace_name", org)
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
def test_setup_airbyte_workspace_create_server_block(
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
    mock_create_airbyte_server_block.return_value = 1
    setup_airbyte_workspace("workspace_name", org)
    mock_get_airbyte_server_block_id.assert_called_once_with("org-airbyte-server")
    mock_create_airbyte_server_block.assert_called_once_with("org-airbyte-server")
    assert (
        OrgPrefectBlock.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
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
def test_setup_airbyte_workspace_server_block_exists(
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
    response = setup_airbyte_workspace("workspace_name", org)
    mock_get_airbyte_server_block_id.assert_called_once_with("org-airbyte-server")
    mock_create_airbyte_server_block.assert_not_called()
    assert (
        OrgPrefectBlock.objects.filter(org=org, block_type=AIRBYTESERVER).count() == 1
    )
    assert response.name == "wsname"
    assert response.workspaceId == "wsid"
    assert response.initialSetupComplete is False
    org.delete()
