"""Tests for the system prompt builder and agent middleware helpers."""

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ddpui.core.chat_with_data.middleware import count_failed_sql_attempts
from ddpui.core.chat_with_data.prompts import build_system_prompt
from ddpui.core.chat_with_data.state import RunContext


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


def make_ctx(dialect="postgres"):
    return RunContext(
        org_id=1,
        org_slug="ngo",
        dialect=dialect,
        allowed_schemas=["prod", "staging"],
        max_result_rows=100,
        query_timeout_s=30,
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
