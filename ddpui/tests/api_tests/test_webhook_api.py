import os
import django
from uuid import uuid4
import json
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.client.webhook_api import (
    post_notification,
    get_org_from_flow_run,
    generate_notification_email,
)
from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.models.org_user import OrgUser, User, OrgUserRole

pytestmark = pytest.mark.django_db


def test_post_notification_unauthorized():
    """tests the api endpoint /notifications/"""
    request = Mock()
    request.headers = {}
    os.environ["X-Notification-Key"] = ""
    with pytest.raises(HttpError) as excinfo:
        post_notification(request)
    assert str(excinfo.value) == "unauthorized"


def test_post_notification():
    """tests the api endpoint /notifications/"""
    request = Mock()
    body = """this is a message
    and another
    Flow run ID: test-flow-run-id
    more text
    """
    request.body = json.dumps({"body": body})
    request.headers = {
        "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
    }
    blockid = str(uuid4())
    flow_run = {"parameters": {"airbyte_connection": {"_block_document_id": blockid}}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name="tempblockname", block_type="fake-type", block_id=blockid
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        mock_get_flow_run.return_value = flow_run
        with patch(
            "ddpui.ddpprefect.prefect_service.get_flow_run_logs"
        ) as mock_get_flow_run_logs:
            mock_get_flow_run_logs.return_value = {"logs": []}
            user = User.objects.create(email="email", username="username")
            OrgUser.objects.create(org=org, user=user, role=OrgUserRole.ACCOUNT_MANAGER)
            with patch(
                "ddpui.api.client.webhook_api.send_text_message"
            ) as mock_send_text_message:
                response = post_notification(request)
                assert response["status"] == "ok"
                mock_send_text_message.assert_called_once()


def test_get_org_from_flow_run_by_blockname():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"block_name": blockname}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="12345"
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_connection():
    """tests get_org_from_flow_run"""
    blockid = str(uuid4())
    flow_run = {"parameters": {"airbyte_connection": {"_block_document_id": blockid}}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name="tempblockname", block_type="fake-type", block_id=blockid
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_airbyte_blocks():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"airbyte_blocks": [{"blockName": blockname}]}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="blockid"
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_dbt_blocks():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"dbt_blocks": [{"blockName": blockname}]}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="blockid"
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_generate_notification_email():
    """tests the email generated"""
    response = generate_notification_email(
        "orgname", "flow-run-id", ["log-message-1", "log-message-2"]
    )
    assert response.find("orgname") > -1
    assert response.find("flow-run-id") > -1
    assert response.find("log-message-1") > -1
    assert response.find("log-message-2") > -1
