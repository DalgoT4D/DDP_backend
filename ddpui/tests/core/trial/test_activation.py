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
