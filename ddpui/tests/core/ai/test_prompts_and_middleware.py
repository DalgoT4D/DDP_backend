"""Tests for the system prompt builder and agent middleware helpers."""

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ddpui.core.ai.agent.middleware import count_failed_sql_attempts
from ddpui.core.ai.agent.chat_data_agent import build_system_prompt
from ddpui.core.ai.agent.run_context import RunContext


def failed_tool_msg(text="Query failed: boom"):
    return ToolMessage(content=text, name="execute_sql", tool_call_id="x")


def test_count_failed_sql_attempts_counts_since_last_user_message():
    messages = [
        HumanMessage("q1"),
        failed_tool_msg(),  # belongs to the previous question
        HumanMessage("q2"),
        AIMessage("trying"),
        failed_tool_msg(),
        failed_tool_msg("SQL rejected: no writes"),
        ToolMessage(content="Query returned 3 rows.", name="execute_sql", tool_call_id="y"),
        ToolMessage(content="Table not found", name="get_table_details", tool_call_id="z"),
    ]
    # 2 failures after the last HumanMessage; success and other tools don't count
    assert count_failed_sql_attempts(messages) == 2


def make_ctx(dialect="postgres", **overrides):
    return RunContext(
        org_id=1,
        org_slug="ngo",
        dialect=dialect,
        allowed_schemas=["prod", "staging"],
        max_result_rows=100,
        query_timeout_s=30,
        **overrides,
    )


def test_system_prompt_names_dialect_schemas_and_rules():
    prompt = build_system_prompt(make_ctx())
    assert "PostgreSQL" in prompt
    assert "prod" in prompt and "staging" in prompt
    assert "profile_column" in prompt  # instructs value-validation before filtering
    assert "read-only" in prompt.lower()


def test_system_prompt_switches_dialect_for_bigquery():
    prompt = build_system_prompt(make_ctx(dialect="bigquery"))
    assert "BigQuery" in prompt
    assert "PostgreSQL" not in prompt


def test_system_prompt_allows_exactly_the_markdown_subset_the_ui_renders():
    """Contract with webapp_v2's AssistantMarkdown: the prompt may only permit
    what that renderer styles (bold, bullets, numbered lists, ### headings,
    > callouts) and must ban the rest."""
    prompt = build_system_prompt(make_ctx())
    for allowed in ["**bold**", '"- " bullets', '"1." numbered lists', '"### "', '"> "']:
        assert allowed in prompt
    assert "no code blocks, no links, no markdown tables" in prompt


def test_scoped_prompt_restricts_to_dashboard_tables():
    ctx = make_ctx(
        scope_type="dashboard",
        allowed_tables=["prod.field_visits", "prod.surveys"],
        scope_context='This chat is about the dashboard "Field Performance".',
    )
    prompt = build_system_prompt(ctx)
    assert "scoped to one dashboard" in prompt
    assert "prod.surveys" in prompt and "prod.field_visits" in prompt
    assert '"Field Performance"' in prompt
    # off-scope questions: point at the full chat page, never try other tables
    assert "full Chat with Data" in prompt


def test_org_prompt_has_no_scope_section():
    prompt = build_system_prompt(make_ctx())
    assert "scoped to one dashboard" not in prompt
