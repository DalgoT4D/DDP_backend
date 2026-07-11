"""Shared model construction for every AI feature.

Each agent module and one-shot LLM call picks its model with an env var and a
default; this is the one place that turns that pair into a client, so model
configuration works the same way across the package.

The env var value picks the PROVIDER too, via init_chat_model's inference:
"claude-sonnet-5" builds ChatAnthropic (needs ANTHROPIC_API_KEY), "gpt-5.5"
builds ChatOpenAI (needs OPENAI_API_KEY), and the explicit "provider:model"
form ("openai:gpt-5.5") works for anything ambiguous. Defaults stay Anthropic;
no temperature is set anywhere (rejected by Claude Sonnet 5 / Opus 4.7+).
"""

import os

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel


def resolve_model_name(env_var: str, default_model: str) -> str:
    """The model id a job will use — for building the client and for tracing."""
    return os.getenv(env_var, default_model)


def build_model(env_var: str, default_model: str, max_tokens: int) -> BaseChatModel:
    """A chat-model client for one AI job, provider inferred from the model id."""
    return init_chat_model(
        resolve_model_name(env_var, default_model),
        max_tokens=max_tokens,
    )
