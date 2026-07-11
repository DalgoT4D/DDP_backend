"""Post-execution result validator tests — scripted model, no API key."""

import asyncio
import json
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage

from ddpui.core.ai.llm_calls.turn_audit import audit_turn


class FakeModel:
    def __init__(self, content):
        self._content = content
        self.prompts = []

    async def ainvoke(self, prompt):
        self.prompts.append(prompt)
        return AIMessage(content=self._content)


def run(coro):
    return asyncio.run(coro)


def test_returns_verdict_with_caveat():
    model = FakeModel(
        json.dumps(
            {
                "verdict": "warn",
                "assumptions": ["counts rows, not distinct farmers"],
                "caveat": "This counts visit records, not unique farmers.",
            }
        )
    )
    result = run(
        audit_turn(
            question="how many farmers enrolled?",
            sql_queries=[{"sql": "SELECT COUNT(*) FROM prod.visits", "status": "success"}],
            result_table={"columns": ["count"], "rows": [["1284"]], "row_count": 1},
            answer="1,284 farmers enrolled.",
            model=model,
        )
    )
    assert result["verdict"] == "warn"
    assert "unique farmers" in result["caveat"]
    # the judge saw the SQL, the result, and the answer
    prompt = model.prompts[0]
    assert "SELECT COUNT(*)" in prompt and "1284" in prompt and "1,284 farmers" in prompt


def test_ok_verdict_passes_through():
    model = FakeModel(json.dumps({"verdict": "ok", "assumptions": [], "caveat": None}))
    result = run(
        audit_turn(
            question="q",
            sql_queries=[{"sql": "SELECT 1", "status": "success"}],
            result_table=None,
            answer="a",
            model=model,
        )
    )
    assert result == {"verdict": "ok", "assumptions": [], "caveat": None}


def test_skips_when_no_sql_ran():
    assert run(audit_turn(question="q", sql_queries=[], result_table=None, answer="a")) is None


def test_never_raises_on_garbage_or_errors():
    assert (
        run(
            audit_turn(
                question="q",
                sql_queries=[{"sql": "SELECT 1", "status": "success"}],
                result_table=None,
                answer="a",
                model=FakeModel("not json"),
            )
        )
        is None
    )

    class ExplodingModel:
        async def ainvoke(self, prompt):
            raise RuntimeError("529")

    assert (
        run(
            audit_turn(
                question="q",
                sql_queries=[{"sql": "SELECT 1", "status": "success"}],
                result_table=None,
                answer="a",
                model=ExplodingModel(),
            )
        )
        is None
    )
