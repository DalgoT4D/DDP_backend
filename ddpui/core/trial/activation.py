"""Redis-backed activation tokens for the free-trial signup flow."""

import json
from uuid import uuid4

from ddpui.utils.redis_client import RedisClient

TOKEN_PREFIX = "trial-activation:"
TOKEN_TTL_SECONDS = 3600 * 24


def create_activation_token(email: str, org_name: str, role: str) -> str:
    """Create and store an activation token, returning its hex value."""
    token = uuid4().hex
    redis = RedisClient.get_instance()
    key = f"{TOKEN_PREFIX}{token}"
    redis.set(key, json.dumps({"email": email, "org_name": org_name, "role": role}))
    redis.expire(key, TOKEN_TTL_SECONDS)
    return token


def consume_activation_token(token: str) -> dict | None:
    """Look up and delete an activation token, returning its payload if valid."""
    redis = RedisClient.get_instance()
    key = f"{TOKEN_PREFIX}{token}"
    raw = redis.get(key)
    if raw is None:
        return None
    redis.delete(key)
    return json.loads(raw)
