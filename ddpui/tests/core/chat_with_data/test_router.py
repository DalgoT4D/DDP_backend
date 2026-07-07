"""Query-understanding router tests — scripted model, no API key."""

import json
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage

from ddpui.core.chat_with_data.router import RouteResult, route_question


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
