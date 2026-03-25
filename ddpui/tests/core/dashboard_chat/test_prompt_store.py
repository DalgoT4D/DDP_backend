"""Tests for dashboard chat prompt template storage and caching."""

import pytest
from django.core.cache import cache

from ddpui.core.dashboard_chat.prompt_store import (
    DEFAULT_DASHBOARD_CHAT_PROMPTS,
    DashboardChatPromptStore,
)
from ddpui.models.dashboard_chat import (
    DashboardChatPromptTemplate,
    DashboardChatPromptTemplateKey,
)

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


def test_prompt_store_returns_default_when_no_db_override_exists():
    """Missing prompt rows should fall back to the built-in default prompt text."""
    store = DashboardChatPromptStore()

    prompt = store.get(DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION)
    final_answer_prompt = store.get(DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION)

    assert (
        prompt
        == DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION]
    )
    assert (
        final_answer_prompt
        == DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION]
    )


def test_prompt_store_uses_db_override_and_invalidates_cache_on_save():
    """Saving a prompt template should invalidate the cached prompt immediately."""
    prompt_template = DashboardChatPromptTemplate.objects.get(
        key=DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM,
    )
    prompt_template.prompt = "first prompt"
    prompt_template.save()
    store = DashboardChatPromptStore()

    assert store.get(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM) == "first prompt"

    prompt_template.prompt = "updated prompt"
    prompt_template.save()

    assert store.get(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM) == "updated prompt"


def test_prompt_store_falls_back_to_default_after_delete():
    """Deleting a prompt template should invalidate the cache and restore the default prompt."""
    prompt_template = DashboardChatPromptTemplate.objects.get(
        key=DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES,
    )
    prompt_template.prompt = "custom answer prompt"
    prompt_template.save()
    store = DashboardChatPromptStore()

    assert store.get(DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES) == "custom answer prompt"

    prompt_template.delete()

    assert store.get(DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES) == (
        DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES]
    )
