"""Tests for dashboard chat OpenAI client helpers."""

import json

import ddpui.core.dashboard_chat.agents.openai_llm_client as llm_client_module
from ddpui.core.dashboard_chat.agents.openai_llm_client import OpenAIDashboardChatLlmClient
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatConversationContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatRetrievedDocument,
)


class FakePromptStore:
    """Minimal prompt store stub for unit tests."""

    def get(self, key):
        return f"prompt:{key}"


class FakeCompletions:
    """Capture outgoing chat completion payloads."""

    def __init__(self):
        self.calls = []
        self.response_content = "Composed answer"

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return FakeResponse(self.response_content)


class FakeChat:
    """Expose a chat.completions surface matching the OpenAI client."""

    def __init__(self):
        self.completions = FakeCompletions()


class FakeClient:
    """Minimal fake OpenAI client."""

    def __init__(self):
        self.chat = FakeChat()


class FakeUsage:
    """Minimal usage payload."""

    prompt_tokens = 10
    completion_tokens = 5
    total_tokens = 15


class FakeMessage:
    """Minimal assistant message wrapper."""

    def __init__(self, content):
        self.content = content


class FakeChoice:
    """Minimal choice wrapper."""

    def __init__(self, content):
        self.message = FakeMessage(content)


class FakeResponse:
    """Minimal OpenAI response wrapper."""

    def __init__(self, content):
        self.choices = [FakeChoice(content)]
        self.usage = FakeUsage()


class RaisingCompletions:
    """Fake completions client that always raises."""

    def create(self, **kwargs):
        raise RuntimeError("boom")


class RaisingClient:
    """Minimal fake client whose completions always fail."""

    def __init__(self):
        self.chat = type("Chat", (), {"completions": RaisingCompletions()})()


def test_classify_intent_uses_prototype_router_message_shape():
    """Intent classification should use the prototype router prompt contract."""
    fake_client = FakeClient()
    fake_client.chat.completions.response_content = json.dumps(
        {
            "intent": "follow_up_sql",
            "confidence": 0.95,
            "reason": "Follow-up detected",
            "force_tool_usage": True,
            "missing_info": [],
            "follow_up_context": {
                "is_follow_up": True,
                "follow_up_type": "add_dimension",
                "reusable_elements": {"previous_sql": "SELECT COUNT(*) FROM analytics.program_reach"},
                "modification_instruction": "split by donor_type",
            },
        }
    )
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=fake_client,
        prompt_store=FakePromptStore(),
    )

    decision = llm_client.classify_intent(
        user_query="Now split that by donor type",
        conversation_context=DashboardChatConversationContext(
            last_sql_query="SELECT COUNT(*) FROM analytics.program_reach",
            last_tables_used=["analytics.program_reach"],
            last_chart_ids=["2"],
            last_response_type="sql_result",
            last_answer_text="Asha Menon, Farah Ali, Leela Joseph, Meera Das, and Noor Khan each improved literacy for 3 students.",
            last_intent="query_with_sql",
        ),
    )

    assert decision.intent == DashboardChatIntent.FOLLOW_UP_SQL
    messages = fake_client.chat.completions.calls[0]["messages"]
    assert messages[0]["role"] == "system"
    assert "CONVERSATION CONTEXT" in messages[0]["content"]
    assert "Previous SQL: SELECT COUNT(*) FROM analytics.program_reach" in messages[0]["content"]
    assert "Last intent: query_with_sql" in messages[0]["content"]
    assert "Last answer text: Asha Menon, Farah Ali, Leela Joseph" in messages[0]["content"]
    assert '"these facilitators", "those students", "they", or "them"' in messages[0]["content"]
    assert messages[1] == {
        "role": "user",
        "content": "Classify this query: Now split that by donor type",
    }


def test_compose_small_talk_uses_capabilities_prompt():
    """Small talk should use the DB-backed prototype capabilities prompt."""
    fake_client = FakeClient()
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=fake_client,
        prompt_store=FakePromptStore(),
    )

    answer = llm_client.compose_small_talk("hi")

    assert answer == "Composed answer"
    assert fake_client.chat.completions.calls[0]["messages"] == [
        {"role": "system", "content": "prompt:small_talk_capabilities"},
        {"role": "user", "content": "hi"},
    ]


def test_classify_intent_falls_back_to_needs_clarification_on_openai_failure(monkeypatch):
    """Router failures should degrade safely instead of crashing the whole turn."""
    monkeypatch.setattr(llm_client_module, "sleep", lambda *_args: None)
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=RaisingClient(),
        prompt_store=FakePromptStore(),
    )

    decision = llm_client.classify_intent(
        user_query="Why did funding drop?",
        conversation_context=DashboardChatConversationContext(),
    )

    assert decision.intent == DashboardChatIntent.NEEDS_CLARIFICATION
    assert (
        decision.clarification_question
        == OpenAIDashboardChatLlmClient.TECHNICAL_DIFFICULTIES_MESSAGE
    )


def test_reset_usage_clears_previous_usage_events():
    """Usage logging must be scoped to one dashboard-chat turn."""
    fake_client = FakeClient()
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=fake_client,
        prompt_store=FakePromptStore(),
    )

    llm_client.compose_small_talk("hi")
    assert llm_client.usage_summary()["totals"]["total_tokens"] == 15

    llm_client.reset_usage()

    assert llm_client.usage_summary()["totals"]["total_tokens"] == 0


def test_compose_final_answer_uses_structured_json_summary_for_table_responses():
    """Table-like answers should return narrative markdown and leave tabular rendering to the UI."""
    fake_client = FakeClient()
    fake_client.chat.completions.response_content = json.dumps(
        {
            "title": "Top 5 facilitators with the best outcomes in Q2 2025",
            "summary": "All five facilitators tied at 3 improved literacy students in Q2 2025.",
            "key_points": [
                "No facilitator exceeded 3 improved literacy students.",
                "The UI should render the full table separately.",
            ],
        }
    )
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=fake_client,
        prompt_store=FakePromptStore(),
    )

    answer = llm_client.compose_final_answer(
        user_query="Give me a district wise pass rate breakdown",
        intent=DashboardChatIntent.QUERY_WITH_SQL,
        response_format="text_with_table",
        draft_answer=None,
        retrieved_documents=[
            DashboardChatRetrievedDocument(
                document_id="doc-1",
                source_type="dashboard_export",
                source_identifier="dashboard:1:chart:2",
                content="District performance chart with literacy and numeracy pass rate metrics",
            )
        ],
        sql="SELECT district_name, avg_literacy_pass_rate FROM analytics.district_program_performance_quarterly",
        sql_results=[
            {
                "district_name": "North",
                "avg_literacy_pass_rate": "25%",
            }
        ],
        warnings=["Using current dashboard context only."],
    )

    assert answer == (
        "### Top 5 facilitators with the best outcomes in Q2 2025\n\n"
        "All five facilitators tied at 3 improved literacy students in Q2 2025.\n\n"
        "- No facilitator exceeded 3 improved literacy students.\n"
        "- The UI should render the full table separately."
    )
    messages = fake_client.chat.completions.calls[0]["messages"]
    assert messages[0] == {
        "role": "system",
        "content": (
            "prompt:final_answer_composition\n\n"
            "For table-like responses, return valid JSON only with this shape:\n"
            "{\n"
            '  "title": "short heading or null",\n'
            '  "summary": "1-2 sentence narrative summary",\n'
            '  "key_points": ["short point", "short point"]\n'
            "}\n\n"
            "Rules:\n"
            "- Do not include markdown tables.\n"
            "- Do not include pipe characters or ASCII table formatting.\n"
            "- Do not repeat every row from the result set.\n"
            "- The UI will render the structured table separately from sql_results.\n"
            "- Keep key_points to at most 3 concise bullets."
        ),
    }
    payload = json.loads(messages[1]["content"])
    assert payload["response_format"] == "text_with_table"
    assert payload["row_count"] == 1
    assert payload["retrieved_context"][0]["source_type"] == "dashboard_export"
    assert payload["retrieved_context"][0]["content"].startswith("District performance chart")
    assert llm_client.usage_summary()["calls"][0]["operation"] == "final_answer_table_summary"


def test_compose_final_answer_keeps_freeform_markdown_for_text_responses():
    """Pure text answers should continue to use the freeform markdown composer path."""
    fake_client = FakeClient()
    fake_client.chat.completions.response_content = "## Overview\n\nThis dashboard tracks literacy outcomes."
    llm_client = OpenAIDashboardChatLlmClient(
        api_key="test-key",
        client=fake_client,
        prompt_store=FakePromptStore(),
    )

    answer = llm_client.compose_final_answer(
        user_query="Tell me about this dashboard",
        intent=DashboardChatIntent.QUERY_WITHOUT_SQL,
        response_format="text",
        draft_answer="This dashboard tracks literacy outcomes.",
        retrieved_documents=[],
        sql=None,
        sql_results=None,
        warnings=[],
    )

    assert answer == "## Overview\n\nThis dashboard tracks literacy outcomes."
    messages = fake_client.chat.completions.calls[0]["messages"]
    assert messages[0] == {
        "role": "system",
        "content": "prompt:final_answer_composition",
    }
    payload = json.loads(messages[1]["content"])
    assert payload["response_format"] == "text"
    assert llm_client.usage_summary()["calls"][0]["operation"] == "final_answer_composition"
