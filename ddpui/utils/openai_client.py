"""Shared OpenAI client helpers."""

from functools import lru_cache

from openai import OpenAI


@lru_cache(maxsize=16)
def get_shared_openai_client(
    api_key: str,
    *,
    timeout_seconds: float | None = None,
    max_retries: int = 0,
) -> OpenAI:
    """Return a shared OpenAI client for one api-key/timeout/retry tuple."""
    client_kwargs = {
        "api_key": api_key,
        "max_retries": max_retries,
    }
    if timeout_seconds is not None:
        client_kwargs["timeout"] = timeout_seconds
    return OpenAI(**client_kwargs)
