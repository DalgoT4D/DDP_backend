"""Shared model construction for every AI feature.

Each agent module and one-shot LLM call picks its model with an env var and a
default; this is the one place that turns that pair into a client, so model
configuration works the same way across the package.
"""

import os

from langchain_anthropic import ChatAnthropic


def resolve_model_name(env_var: str, default_model: str) -> str:
    """The model id a job will use — for building the client and for tracing."""
    return os.getenv(env_var, default_model)


def build_model(env_var: str, default_model: str, max_tokens: int) -> ChatAnthropic:
    """A ChatAnthropic client for one AI job. Deployment-level API key; no
    temperature (rejected by Claude Sonnet 5 / Opus 4.7+)."""
    return ChatAnthropic(
        model=resolve_model_name(env_var, default_model),
        max_tokens=max_tokens,
    )
