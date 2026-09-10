"""Query-understanding router tests — scripted model, no API key."""

import json
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage

from ddpui.core.ai.llm_calls.router import RouteResult, route_question


class FakeModel:
    def __init__(self, content):
        self._content = content

    async def ainvoke(self, prompt):
        return AIMessage(content=self._content)


class ExplodingModel:
    async def ainvoke(self, prompt):
        raise RuntimeError("api down")


def run(coro):
    import asyncio

    return asyncio.run(coro)


def test_routes_a_data_question():
    model = FakeModel(
        json.dumps(
            {
                "intent": "data_question",
                "complexity": "simple",
                "entities": ["farmers", "Maharashtra", "last month"],
                "clarification": None,
            }
        )
    )
    route = run(route_question("How many farmers enrolled in Maharashtra last month?", model))
    assert route == RouteResult(
        intent="data_question",
        complexity="simple",
        entities=["farmers", "Maharashtra", "last month"],
        clarification=None,
    )


def test_routes_small_talk():
    model = FakeModel(json.dumps({"intent": "small_talk", "complexity": "simple"}))
    route = run(route_question("thanks, that's great!", model))
    assert route.intent == "small_talk"


def test_fails_open_on_garbage_and_errors():
    assert run(route_question("q", FakeModel("not json"))).intent == "data_question"
    route = run(route_question("q", ExplodingModel()))
    assert route.intent == "data_question"
    assert route.complexity == "simple"


def test_unknown_values_fail_open():
    model = FakeModel(json.dumps({"intent": "banana", "complexity": "galactic"}))
    route = run(route_question("q", model))
    assert route.intent == "data_question"
    assert route.complexity == "simple"


def test_history_reaches_the_router_prompt():
    class CapturingModel(FakeModel):
        def __init__(self, content):
            super().__init__(content)
            self.prompts = []

        async def ainvoke(self, prompt):
            self.prompts.append(prompt)
            return await super().ainvoke(prompt)

    model = CapturingModel(json.dumps({"intent": "data_question", "complexity": "simple"}))
    run(
        route_question(
            "can we create a chart of this?",
            model,
            history=["User: list of top donors", "Assistant: Here are the top donors..."],
        )
    )
    prompt = model.prompts[0]
    assert "list of top donors" in prompt
    assert "Recent conversation" in prompt


def test_routes_platform_help():
    model = FakeModel(json.dumps({"intent": "platform_help", "complexity": "simple"}))
    route = run(route_question("how do I create a KPI?", model))
    assert route.intent == "platform_help"


def test_creation_requests_override_data_routing_mid_conversation():
    """The user's exact failure: mid-data-conversation, the model routes a
    creation follow-up as data_question — the deterministic backstop must
    force platform_help so the guide agent (which shares the thread) builds it."""
    model = FakeModel(json.dumps({"intent": "data_question", "complexity": "simple"}))

    for question in [
        "create a KPI for total silt carted vs target",
        "create a chart of farmers by vulnerability category",
        "make me a dashboard for the field team",
        "chart this by district",
    ]:
        route = run(route_question(question, model))
        assert route.intent == "platform_help", question


def test_creation_backstop_applies_even_when_the_router_fails():
    route = run(route_question("create a chart of farmers by district", ExplodingModel()))
    assert route.intent == "platform_help"


def test_backstop_leaves_genuine_data_questions_alone():
    model = FakeModel(json.dumps({"intent": "data_question", "complexity": "simple"}))
    for question in [
        "how many reports did we create last month?",
        "how many farmers are in each vulnerability category?",
    ]:
        route = run(route_question(question, model))
        assert route.intent == "data_question", question
