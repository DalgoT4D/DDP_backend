"""Shared model factory tests — provider inference from the model id."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from ddpui.core.ai.agent.base import build_model, resolve_model_name


def test_claude_id_builds_an_anthropic_client(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.delenv("TEST_SUMMARY_MODEL", raising=False)

    model = build_model("TEST_SUMMARY_MODEL", "claude-sonnet-5", 1000)

    assert isinstance(model, ChatAnthropic)
    assert model.max_tokens == 1000


def test_gpt_id_in_env_var_builds_an_openai_client(monkeypatch):
    """Setting e.g. REPORT_SUMMARY_MODEL=gpt-5.5 switches the provider —
    no code change needed to A/B test OpenAI on a feature."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("TEST_SUMMARY_MODEL", "gpt-5.5")

    model = build_model("TEST_SUMMARY_MODEL", "claude-sonnet-5", 1000)

    assert isinstance(model, ChatOpenAI)
    assert model.model_name == "gpt-5.5"


def test_explicit_provider_prefix_is_supported(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("TEST_SUMMARY_MODEL", "openai:gpt-5.5")

    model = build_model("TEST_SUMMARY_MODEL", "claude-sonnet-5", 1000)

    assert isinstance(model, ChatOpenAI)


def test_resolve_model_name_prefers_env(monkeypatch):
    monkeypatch.setenv("TEST_SUMMARY_MODEL", "gpt-5.5")
    assert resolve_model_name("TEST_SUMMARY_MODEL", "claude-sonnet-5") == "gpt-5.5"
    monkeypatch.delenv("TEST_SUMMARY_MODEL")
    assert resolve_model_name("TEST_SUMMARY_MODEL", "claude-sonnet-5") == "claude-sonnet-5"


# ── user-selectable models (UI model picker) ────────────────────────────────


def test_available_models_filters_by_provider_key(monkeypatch):
    from ddpui.core.ai.agent import chat_data_agent as cda

    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    offered = [m["id"] for m in cda.available_models()]
    assert "claude-sonnet-5" in offered
    assert "gpt-5.5" not in offered


def test_resolve_selected_model_rejects_unknown_ids(monkeypatch):
    """Client-supplied ids are never trusted — unknown or unavailable ids fall
    back to the default instead of reaching init_chat_model."""
    from ddpui.core.ai.agent import chat_data_agent as cda

    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("CHAT_WITH_DATA_MODEL", raising=False)

    assert cda.resolve_selected_model("claude-sonnet-5") == "claude-sonnet-5"
    assert cda.resolve_selected_model("gpt-5.5") == "claude-sonnet-5"  # key absent
    assert cda.resolve_selected_model("evil:model") == "claude-sonnet-5"
    assert cda.resolve_selected_model(None) == "claude-sonnet-5"


def test_default_model_prefers_env_when_offerable(monkeypatch):
    from ddpui.core.ai.agent import chat_data_agent as cda

    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.setenv("CHAT_WITH_DATA_MODEL", "gpt-5.5")
    assert cda.default_model_id() == "gpt-5.5"
