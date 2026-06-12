from unittest.mock import Mock, patch

import pytest

from ddpui.websockets import BaseConsumer


@patch("ddpui.websockets.AccessToken")
@patch("ddpui.websockets.User.objects.filter")
@patch("ddpui.websockets.OrgUser.objects.filter")
def test_base_consumer_authenticate_user_uses_cookie_token_when_query_token_missing(
    mock_orguser_filter,
    mock_user_filter,
    mock_access_token,
):
    user = Mock(email="test@example.com")
    orguser = Mock()
    mock_access_token.return_value.payload = {"user_id": 42}
    mock_user_filter.return_value.first.return_value = user
    mock_orguser_filter.return_value.filter.return_value.first.return_value = orguser

    consumer = BaseConsumer()
    consumer.scope = {
        "headers": [
            (b"cookie", b"csrftoken=test; access_token=cookie-token; refresh_token=refresh-token")
        ]
    }

    assert consumer.authenticate_user(None, "test-org") is True
    mock_access_token.assert_called_once_with("cookie-token")


def test_base_consumer_get_cookie_token_returns_none_without_cookie_header():
    consumer = BaseConsumer()
    consumer.scope = {"headers": []}

    assert consumer._get_cookie_token() is None

