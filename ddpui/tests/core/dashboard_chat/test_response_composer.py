"""Pure unit tests for dashboard-chat response composition."""

from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.orchestration.response_composer import compose_final_answer_text


class FinalAnswerComposerLlm:
    def __init__(self):
        self.compose_calls = []

    def compose_final_answer(self, **kwargs):
        self.compose_calls.append(kwargs)
        return "LLM answer"


def test_compose_final_answer_text_lists_names_without_final_llm():
    """Name-list SQL results should be emitted directly without summarization or rounding drift."""
    llm = FinalAnswerComposerLlm()
    state = {
        "user_query": "Give me the names of students below 20 percent in endline maths",
        "intent_decision": DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.9,
            reason="Needs SQL",
            force_tool_usage=True,
        ).model_dump(mode="json"),
    }

    answer = compose_final_answer_text(
        llm,
        state,
        {
            "answer_text": "",
            "retrieved_documents": [],
            "sql": "SELECT student_name_end FROM scores",
            "sql_results": [
                {"student_name_end": "Ameenabee F"},
                {"student_name_end": "Arasu R"},
            ],
            "warnings": [],
        },
        response_format="text",
    )

    assert answer == "- Ameenabee F\n- Arasu R"
    assert llm.compose_calls == []
