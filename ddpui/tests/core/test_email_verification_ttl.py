"""Regression tests for issue #1314.

Email verification tokens stored in Redis must expire after 24 hours,
matching the existing TTL applied to password-reset tokens. Without a TTL,
stale tokens accumulate in Redis and remain valid indefinitely.

Both the signup and resend flows must call ``redis.expire(key, 86400)``
on the ``email-verification:*`` key.
"""

from unittest.mock import Mock, patch

from ddpui.core import orguserfunctions

EXPECTED_TTL_SECONDS = 3600 * 24  # 24 hours


@patch("ddpui.core.orguserfunctions.awsses.send_signup_email")
@patch("ddpui.core.orguserfunctions.from_orguser")
@patch("ddpui.core.orguserfunctions.RedisClient.get_instance")
@patch("ddpui.core.orguserfunctions.create_orguser")
def test_signup_orguser_sets_24h_ttl_on_email_verification_key(
    mock_create_orguser, mock_redis_get_instance, mock_from_orguser, mock_send_email
):
    """signup_orguser must call redis.expire with 86400s on the verification key."""
    fake_orguser = Mock(id=42)
    mock_create_orguser.return_value = fake_orguser
    mock_from_orguser.return_value = {"id": 42}
    mock_redis = Mock()
    mock_redis_get_instance.return_value = mock_redis

    payload = Mock(email="user@example.com")
    result, error = orguserfunctions.signup_orguser(payload)

    assert error is None
    assert result == {"id": 42}

    mock_redis.set.assert_called_once()
    set_key = mock_redis.set.call_args.args[0]
    assert set_key.startswith("email-verification:")

    mock_redis.expire.assert_called_once_with(set_key, EXPECTED_TTL_SECONDS)


@patch("ddpui.core.orguserfunctions.awsses.send_signup_email")
@patch("ddpui.core.orguserfunctions.RedisClient.get_instance")
def test_resend_verification_email_sets_24h_ttl_on_email_verification_key(
    mock_redis_get_instance, mock_send_email
):
    """resend_verification_email must call redis.expire with 86400s on the verification key."""
    mock_redis = Mock()
    mock_redis_get_instance.return_value = mock_redis
    fake_orguser = Mock(id=99)

    _, error = orguserfunctions.resend_verification_email(fake_orguser, "user@example.com")

    assert error is None

    mock_redis.set.assert_called_once()
    set_key = mock_redis.set.call_args.args[0]
    assert set_key.startswith("email-verification:")

    mock_redis.expire.assert_called_once_with(set_key, EXPECTED_TTL_SECONDS)
