from uuid import uuid4
from unittest.mock import Mock
from ddpui.models.tasks import TaskProgressStatus
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.websockets.airbyte_consumer import polling_celery
from ddpui.websockets.schemas import WebsocketResponse, WebsocketResponseStatus


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
            data={}, message="Invalid credentials", status=WebsocketResponseStatus.ERROR
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
