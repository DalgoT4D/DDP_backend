"""Tests for dashboard chat prompt template lookup."""

import pytest

from ddpui.core.dashboard_chat.agents.prompt_template_store import (
    DEFAULT_DASHBOARD_CHAT_PROMPTS,
    DashboardChatPromptStore,
)
from ddpui.models.dashboard_chat import (
    DashboardChatPromptTemplate,
    DashboardChatPromptTemplateKey,
)

pytestmark = pytest.mark.django_db


def test_prompt_store_returns_default_when_no_db_override_exists():
    """Missing prompt rows should fall back to the built-in default prompt text."""
    DashboardChatPromptTemplate.objects.filter(
        key__in=[
            DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION,
            DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION,
        ]
    ).delete()
    store = DashboardChatPromptStore()

    prompt = store.get(DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION)
    final_answer_prompt = store.get(DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION)
    verifier_prompt = store.get(DashboardChatPromptTemplateKey.SQL_VERIFICATION)

    assert (
        prompt
        == DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION]
    )
    assert (
        final_answer_prompt
        == DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION]
    )
    assert (
        verifier_prompt
        == DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.SQL_VERIFICATION]
    )
    assert "Which fellow performed best in midline RF for grade 3?" in prompt
    assert (
        '"How many beneficiaries participated?" means count beneficiaries, not sum services delivered.'
        in (DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM])
    )


def test_default_new_query_prompt_requires_joins_for_cross_table_questions():
    """The built-in SQL writer prompt should require joins when fields live across tables."""
    prompt = DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM]

    assert "If the answer needs fields from multiple tables" in prompt
    assert "write one SQL statement that joins them on the natural key" in prompt
    assert "FIRST call get_chart_table_metadata" in prompt
    assert "Only if they cannot fully answer it, use search_metadata" in prompt
    assert (
        "identify the exact entity, measure, grain, time scope, threshold, and every requested part or comparison"
        in prompt
    )
    assert "match the counting method to the table grain" in prompt
    assert "entity_counting_guidance" in prompt
    assert (
        "the SQL is incomplete unless every named entity or period appears in the result" in prompt
    )
    assert "prefer numeric score, percent, or mastery columns" in prompt
    assert "Use as many tool calls as needed for clarity, but keep them targeted" in prompt
    assert "Do not call read_full_metadata unless" in prompt
    assert "Interpret a calendar year as January 1 through December 31 of that year" in prompt
    assert "Interpret calendar quarters as Q1 = January 1 to March 31" in prompt
    assert (
        "Interpret a financial year or fiscal year such as 2025-26 or FY 2025-26 as April 1, 2025 through March 31, 2026"
        in prompt
    )
    assert "Do not invent latest-row logic, latest-date logic, MAX(date) filters" in prompt
    assert "metadata must never authorize latest/as-of logic by itself" in prompt
    assert (
        "For quarter and year questions, use direct date filters and aggregate within that window"
        in prompt
    )
    assert (
        "If the user asks for names or a list of entities, return one row per entity name" in prompt
    )
    assert (
        "If the question asks for an overall ranking, count, or performance comparison and does not name a stage"
        in prompt
    )
    assert (
        "Choose the table set that fully preserves the requested grain, dimensions, filters, and comparison logic"
        in prompt
    )
    assert (
        "If a surfaced chart table or aggregate table has already rolled up over a dimension"
        in prompt
    )
    assert "snapshot" not in prompt.lower()


def test_default_follow_up_prompt_requires_joins_when_follow_up_needs_another_table():
    """Follow-up prompt should also tell the model to join rather than substitute proxies."""
    prompt = DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM]

    assert "If the current tables cannot fully answer the follow-up" in prompt
    assert "related tables and join paths" in prompt
    assert "Do not invent latest-row logic, latest-date logic, MAX(date) filters" in prompt
    assert "metadata must never authorize latest/as-of logic by itself" in prompt
    assert (
        "financial or fiscal year such as 2025-26 as April 1, 2025 through March 31, 2026" in prompt
    )
    assert (
        "If the follow-up asks for names or a list of entities, return one row per entity name"
        in prompt
    )
    assert (
        "If the follow-up asks for an overall ranking, count, or performance comparison and does not name a stage"
        in prompt
    )
    assert (
        "If a surfaced chart table or aggregate table has already rolled up over something the follow-up now needs"
        in prompt
    )
    assert "snapshot" not in prompt.lower()


def test_default_intent_prompt_routes_advisory_questions_without_sql():
    """Advisory questions should be routed to the context/advice path by default."""
    prompt = DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION]

    assert (
        '"how can we improve", "what should we do", "what do you recommend", or "how to improve"'
        in prompt
    )
    assert "overall available scope" in prompt


def test_default_sql_verification_prompt_rejects_latest_row_and_name_aggregation_patterns():
    """Verifier prompt should explicitly guard against known semantic SQL anti-patterns."""
    prompt = DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.SQL_VERIFICATION]

    assert "Reject SQL if it invents latest-row" in prompt
    assert "Metadata must never justify latest-row/as-of/reporting-date logic by itself" in prompt
    assert "require direct date filters and aggregation within that window" in prompt
    assert 'Use severity "warning"' in prompt
    assert "Reject SQL if it aggregates names into one string" in prompt
    assert "Do not reject SQL because it has LIMIT" in prompt
    assert "referenced metadata proves that the table cannot answer it" in prompt


def test_prompt_store_uses_db_override_after_save():
    """Saving a prompt template should update the prompt returned at runtime."""
    prompt_template, _ = DashboardChatPromptTemplate.objects.update_or_create(
        key=DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM,
        defaults={
            "prompt": DEFAULT_DASHBOARD_CHAT_PROMPTS[
                DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM
            ]
        },
    )
    prompt_template.prompt = "first prompt"
    prompt_template.save()
    store = DashboardChatPromptStore()

    assert store.get(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM) == "first prompt"

    prompt_template.prompt = "updated prompt"
    prompt_template.save()

    assert store.get(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM) == "updated prompt"


def test_prompt_store_falls_back_to_default_after_delete():
    """Deleting a prompt template should restore the default prompt text."""
    prompt_template, _ = DashboardChatPromptTemplate.objects.update_or_create(
        key=DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES,
        defaults={
            "prompt": DEFAULT_DASHBOARD_CHAT_PROMPTS[
                DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES
            ]
        },
    )
    prompt_template.prompt = "custom answer prompt"
    prompt_template.save()
    store = DashboardChatPromptStore()

    assert (
        store.get(DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES) == "custom answer prompt"
    )

    prompt_template.delete()

    assert store.get(DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES) == (
        DEFAULT_DASHBOARD_CHAT_PROMPTS[DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES]
    )
