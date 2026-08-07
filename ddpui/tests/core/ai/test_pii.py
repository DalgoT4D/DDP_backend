"""PII middleware tests — the model must never see raw PII.

Real agent graph + scripted model (records every request it receives), so
these prove masking behaviorally for both surfaces: the user's typed message
and execute_sql tool results.
"""

import os
from typing import Any

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.agent.pii import PII_RULES, build_pii_middleware
from ddpui.tests.core.ai.test_agent_loop import ScriptedChatModel, make_context, sql_call
from ddpui.tests.core.ai.test_tools import FakeWarehouse


class RecordingScriptedModel(ScriptedChatModel):
    """Scripted model that also records the messages of every request."""

    requests: list = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.requests.append(list(messages))
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def run_agent(model, question, warehouse=None):
    agent = build_agent(model=model)
    return agent.invoke(
        {"messages": [("user", question)]},
        context=make_context(warehouse=warehouse),
    )


def all_text(messages) -> str:
    return "\n".join(str(m.content) for m in messages)


def test_rules_build_one_middleware_each_covering_input_and_tool_results():
    middlewares = build_pii_middleware()
    assert len(middlewares) == len(PII_RULES)


def test_email_and_phone_in_user_message_are_masked_before_the_model():
    model = RecordingScriptedModel(script=[AIMessage(content="Noted.")], requests=[])

    run_agent(model, "email priya@ngo.org or call 9876543210 about the surveys")

    seen = all_text(model.requests[0])
    assert "priya@ngo.org" not in seen
    assert "9876543210" not in seen
    assert "REDACTED" in seen  # placeholders replace the raw values


def test_query_results_are_masked_before_the_model_narrates_them():
    warehouse = FakeWarehouse(
        rows=[{"name": "Priya", "email": "priya@ngo.org", "phone": "+91 9876543210"}]
    )
    model = RecordingScriptedModel(
        script=[
            sql_call("SELECT name, email, phone FROM prod.contacts", "c1"),
            AIMessage(content="One contact found."),
        ],
        requests=[],
    )

    run_agent(model, "list the field coordinator contacts", warehouse=warehouse)

    # the model's second request contains the tool result — masked
    narration_request = all_text(model.requests[1])
    assert "priya@ngo.org" not in narration_request
    assert "9876543210" not in narration_request


def test_ordinary_numbers_and_ids_are_not_mangled():
    """The phone regex must not fire on counts or long numeric ids."""
    model = RecordingScriptedModel(script=[AIMessage(content="Noted.")], requests=[])

    run_agent(model, "why are there 1284 surveys and id 123456789012 in prod?")

    seen = all_text(model.requests[0])
    assert "1284" in seen
    assert "123456789012" in seen
