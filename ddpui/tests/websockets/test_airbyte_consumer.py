from uuid import uuid4
from unittest.mock import Mock, patch
import json

from ddpui.models.org import OrgType
from ddpui.models.tasks import TaskProgressStatus
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.websockets.airbyte_consumer import (
    SchemaCatalogConsumer,
    polling_celery,
    SourceCheckConnectionConsumer,
    DestinationCheckConnectionConsumer,
)
from ddpui.websockets.schemas import WebsocketResponse, WebsocketResponseStatus


def test_websocket_receive_no_workspace():
    """tests websocket_receive"""
    consumer = SchemaCatalogConsumer()
    consumer.connect = Mock()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(airbyte_workspace_id=None))
    consumer.websocket_receive({"text": "{}"})
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={},
            message="Create an airbyte workspace first.",
            status=WebsocketResponseStatus.ERROR,
        )
    )


def test_websocket_receive_no_source_id():
    """tests websocket_receive"""
    consumer = SchemaCatalogConsumer()
    consumer.connect = Mock()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(airbyte_workspace_id=1))
    consumer.websocket_receive({"text": "{}"})
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={},
            message="SourceId is required in the payload",
            status=WebsocketResponseStatus.ERROR,
        )
    )


def test_polling_celery_no_stp():
    """tests polling_celery"""
    consumer = Mock(respond=Mock())
    task_key = uuid4().hex
    polling_celery(consumer, task_key)
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={}, message="No Task of this task_key found", status=WebsocketResponseStatus.ERROR
        )
    )


def test_polling_celery_failed():
    """tests polling_celery"""
    consumer = Mock(respond=Mock())
    task_key = uuid4().hex
    stp = SingleTaskProgress(task_key, 100)
    stp.add({"status": TaskProgressStatus.FAILED})
    polling_celery(consumer, task_key)
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={}, message="Failed to get schema catalog", status=WebsocketResponseStatus.ERROR
        )
    )


def test_polling_celery_succeeded():
    """tests polling_celery"""
    consumer = Mock(respond=Mock())
    task_key = uuid4().hex
    stp = SingleTaskProgress(task_key, 100)
    stp.add({"status": TaskProgressStatus.COMPLETED, "result": "test-result"})
    polling_celery(consumer, task_key)
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": TaskProgressStatus.COMPLETED, "result": "test-result"},
            message="Successfully fetched source schema",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_update_success(mock_airbyte_service):
    """tests source check connection for update success"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_source_connection_for_update.return_value = {
        "jobInfo": {"succeeded": True, "logs": {"logLines": ["log1", "log2"]}}
    }

    payload = {"sourceId": "some-id", "name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    mock_airbyte_service.check_source_connection_for_update.assert_called_once()
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "succeeded", "logs": ["log1", "log2"]},
            message="Source connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_create_success(mock_airbyte_service):
    """tests source check connection for create success"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(airbyte_workspace_id="ws-id"))

    mock_airbyte_service.check_source_connection.return_value = {
        "jobInfo": {"succeeded": True, "logs": {"logLines": ["log1"]}}
    }

    payload = {"name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    mock_airbyte_service.check_source_connection.assert_called_once()
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "succeeded", "logs": ["log1"]},
            message="Source connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_failed(mock_airbyte_service):
    """tests source check connection for failure"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_source_connection_for_update.return_value = {
        "jobInfo": {"succeeded": False, "logs": {"logLines": ["error log"]}}
    }

    payload = {"sourceId": "some-id", "name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "failed", "logs": ["error log"]},
            message="Source connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_exception(mock_airbyte_service):
    """tests source check connection when exception is raised"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_source_connection_for_update.side_effect = Exception(
        "Something went wrong"
    )

    payload = {"sourceId": "some-id", "name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={},
            message="Error: Something went wrong",
            status=WebsocketResponseStatus.ERROR,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbytehelpers")
@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_demo_org(mock_airbyte_service, mock_airbyte_helpers):
    """tests source check connection for demo org"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(base_plan=lambda: OrgType.DEMO, airbyte_workspace_id="ws-id"))

    mock_airbyte_service.get_source_definition.return_value = {"name": "Postgres"}
    mock_airbyte_helpers.get_demo_whitelisted_source_config.return_value = (
        {"config": "whitelisted"},
        None,
    )
    mock_airbyte_service.check_source_connection.return_value = {
        "jobInfo": {"succeeded": True, "logs": {"logLines": ["log1"]}}
    }

    payload = {"name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    mock_airbyte_service.get_source_definition.assert_called_once()
    mock_airbyte_helpers.get_demo_whitelisted_source_config.assert_called_once_with("Postgres")
    mock_airbyte_service.check_source_connection.assert_called_once()
    # check that payload.config was updated
    called_payload = mock_airbyte_service.check_source_connection.call_args[0][1]
    assert called_payload.config == {"config": "whitelisted"}

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "succeeded", "logs": ["log1"]},
            message="Source connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbytehelpers")
@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_source_check_connection_demo_org_whitelist_error(
    mock_airbyte_service, mock_airbyte_helpers
):
    """tests source check connection for demo org with whitelist error"""
    consumer = SourceCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(base_plan=lambda: OrgType.DEMO, airbyte_workspace_id="ws-id"))

    mock_airbyte_service.get_source_definition.return_value = {"name": "Postgres"}
    mock_airbyte_helpers.get_demo_whitelisted_source_config.return_value = (
        None,
        "Some error",
    )

    payload = {"name": "test", "sourceDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={},
            message="Error in getting whitelisted source config",
            status=WebsocketResponseStatus.ERROR,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_destination_check_connection_update_success(mock_airbyte_service):
    """tests destination check connection for update success"""
    consumer = DestinationCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_destination_connection_for_update.return_value = {
        "jobInfo": {"succeeded": True, "logs": {"logLines": ["log1", "log2"]}}
    }

    payload = {
        "destinationId": "some-id",
        "name": "test",
        "destinationDefId": "def-id",
        "config": {},
    }
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    mock_airbyte_service.check_destination_connection_for_update.assert_called_once()
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "succeeded", "logs": ["log1", "log2"]},
            message="Destination connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_destination_check_connection_create_success(mock_airbyte_service):
    """tests destination check connection for create success"""
    consumer = DestinationCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock(airbyte_workspace_id="ws-id"))

    mock_airbyte_service.check_destination_connection.return_value = {
        "jobInfo": {"succeeded": True, "logs": {"logLines": ["log1"]}}
    }

    payload = {"name": "test", "destinationDefId": "def-id", "config": {}}
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    mock_airbyte_service.check_destination_connection.assert_called_once()
    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "succeeded", "logs": ["log1"]},
            message="Destination connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_destination_check_connection_failed(mock_airbyte_service):
    """tests destination check connection for failure"""
    consumer = DestinationCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_destination_connection_for_update.return_value = {
        "jobInfo": {"succeeded": False, "logs": {"logLines": ["error log"]}}
    }

    payload = {
        "destinationId": "some-id",
        "name": "test",
        "destinationDefId": "def-id",
        "config": {},
    }
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={"status": "failed", "logs": ["error log"]},
            message="Destination connection check completed",
            status=WebsocketResponseStatus.SUCCESS,
        )
    )


@patch("ddpui.websockets.airbyte_consumer.airbyte_service")
def test_destination_check_connection_exception(mock_airbyte_service):
    """tests destination check connection when exception is raised"""
    consumer = DestinationCheckConnectionConsumer()
    consumer.respond = Mock()
    consumer.orguser = Mock(org=Mock())

    mock_airbyte_service.check_destination_connection_for_update.side_effect = Exception(
        "Something went wrong"
    )

    payload = {
        "destinationId": "some-id",
        "name": "test",
        "destinationDefId": "def-id",
        "config": {},
    }
    message = {"text": json.dumps(payload)}

    consumer.websocket_receive(message)

    consumer.respond.assert_called_once_with(
        WebsocketResponse(
            data={},
            message="Error: Something went wrong",
            status=WebsocketResponseStatus.ERROR,
        )
    )
