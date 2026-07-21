from unittest.mock import patch


@patch("ddpui.core.trial.activation.RedisClient")
def test_create_and_consume_token(mock_redis_cls):
    store = {}
    r = mock_redis_cls.get_instance.return_value
    r.set.side_effect = lambda k, v: store.__setitem__(k, v)
    r.get.side_effect = lambda k: store.get(k)
    r.delete.side_effect = lambda k: store.pop(k, None)
    from ddpui.core.trial import activation

    tok = activation.create_activation_token("a@b.org", "Acme", "account-manager")
    assert activation.consume_activation_token(tok) == {
        "email": "a@b.org",
        "org_name": "Acme",
        "role": "account-manager",
    }
    assert activation.consume_activation_token(tok) is None  # consumed once


@patch("ddpui.core.trial.activation.RedisClient")
def test_store_and_fetch_clone_params(mock_redis_cls):
    store = {}
    r = mock_redis_cls.get_instance.return_value
    r.set.side_effect = lambda k, v: store.__setitem__(k, v)
    r.get.side_effect = lambda k: store.get(k)
    r.expire.side_effect = lambda k, ttl: None
    from ddpui.core.trial import activation

    activation.store_clone_params("task-1", "a@b.org", "Acme", "account-manager", 42)
    assert activation.fetch_clone_params("task-1") == {
        "email": "a@b.org",
        "org_name": "Acme",
        "role": "account-manager",
        "template_org_id": 42,
    }
    # unlike the activation token, params are NOT deleted on fetch — a retry may itself be retried
    assert activation.fetch_clone_params("task-1") is not None
    assert activation.fetch_clone_params("nope") is None


@patch("ddpui.core.trial.activation.RedisClient")
def test_clone_lock_acquire_and_release(mock_redis_cls):
    """acquire returns True only when SET NX succeeds; release deletes the key."""
    r = mock_redis_cls.get_instance.return_value
    from ddpui.core.trial import activation

    r.set.return_value = True
    assert activation.acquire_clone_lock("a@b.org") is True
    r.set.assert_called_once_with(
        f"{activation.CLONE_LOCK_PREFIX}a@b.org",
        "1",
        nx=True,
        ex=activation.CLONE_LOCK_TTL_SECONDS,
    )

    r.set.return_value = None  # redis-py returns None when NX fails
    assert activation.acquire_clone_lock("a@b.org") is False

    activation.release_clone_lock("a@b.org")
    r.delete.assert_called_once_with(f"{activation.CLONE_LOCK_PREFIX}a@b.org")
