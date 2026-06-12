"""Tests for deterministic dashboard-chat time-scope guidance."""

from datetime import date

from ddpui.core.dashboard_chat.orchestration import tool_loop_message_builder
from ddpui.core.dashboard_chat.orchestration.time_scope import (
    build_required_time_scope_prompt,
    resolve_time_scope,
)


class PromptOnlyLlm:
    """Minimal prompt provider for message-builder tests."""

    @staticmethod
    def get_prompt(prompt_key):
        return f"prompt:{prompt_key}"


def test_required_time_scope_prompt_resolves_this_year_for_generator():
    """Explicit relative-year questions should inject concrete date constraints."""
    prompt = build_required_time_scope_prompt(
        "How much Total Silt is left to Excavate to meet the endline goals this year?",
        date(2026, 6, 12),
    )

    assert prompt is not None
    assert "REQUIRED TIME SCOPE" in prompt
    assert "start_date=2026-01-01" in prompt
    assert "end_date_exclusive=2027-01-01" in prompt
    assert "Every metric source" in prompt
    assert "Do not call run_sql_query until the SQL includes the direct date filter" in prompt


def test_required_time_scope_prompt_does_not_trigger_for_stage_trend_language():
    """Vague temporal or assessment-stage language should stay with the LLM."""
    prompt = build_required_time_scope_prompt(
        "for RC, what has been the overall trend across grades from baseline to endline?",
        date(2026, 6, 12),
    )

    assert prompt is None


def test_resolve_time_scope_uses_calendar_quarters_unless_fiscal_context_is_named():
    """Bare Q references are calendar quarters; fiscal context switches to April year."""
    calendar_ranges = resolve_time_scope("Show Q2 performance", date(2026, 6, 12))
    fiscal_ranges = resolve_time_scope("Show Q2 FY 2025-26 performance", date(2026, 6, 12))

    assert calendar_ranges == [
        {
            "label": "Q2",
            "start_date": "2026-04-01",
            "end_date_exclusive": "2026-07-01",
            "calendar_basis": "calendar year 2026",
        }
    ]
    assert fiscal_ranges == [
        {
            "label": "Q2",
            "start_date": "2025-07-01",
            "end_date_exclusive": "2025-10-01",
            "calendar_basis": "fiscal year starting 2025-04-01",
        }
    ]


def test_new_query_messages_include_required_time_scope_only_when_explicit(monkeypatch):
    """The SQL generator gets the extra scope message only for explicit windows."""
    monkeypatch.setattr(
        tool_loop_message_builder.timezone,
        "localdate",
        lambda: date(2026, 6, 12),
    )

    time_bounded_messages = tool_loop_message_builder.build_new_query_messages(
        PromptOnlyLlm(),
        {
            "user_query": "How much silt is left this year?",
            "chart_registry_payload": [],
        },
    )
    regular_messages = tool_loop_message_builder.build_new_query_messages(
        PromptOnlyLlm(),
        {
            "user_query": "What is the baseline to endline trend?",
            "chart_registry_payload": [],
        },
    )

    assert any(
        message["content"].startswith("REQUIRED TIME SCOPE")
        for message in time_bounded_messages
    )
    assert not any(
        message["content"].startswith("REQUIRED TIME SCOPE")
        for message in regular_messages
    )
