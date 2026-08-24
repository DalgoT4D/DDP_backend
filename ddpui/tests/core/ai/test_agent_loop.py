"""Agent-loop tests with a scripted chat model — real graph, real tools, fake LLM.

The scripted model emits a fixed sequence of AIMessages (tool calls, then an
answer), so these tests exercise create_agent wiring, ToolRuntime context
injection, middleware, and the guard — deterministically, with no API key.
"""

from typing import Any, Optional

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.tests.core.ai.test_tools import FakeWarehouse


class ScriptedChatModel(BaseChatModel):
    """Returns queued responses; repeats the last one if the script runs out.

    Each response is emitted as a copy with unique message/tool-call ids — real
    models never reuse ids, and duplicated ids corrupt langgraph's message
    reducer (add_messages dedupes by id), producing spurious routing errors.
    """

    script: list[AIMessage]
    calls: int = 0

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        template = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        response = AIMessage(
            content=template.content,
            tool_calls=[{**tc, "id": f"{tc['id']}-{self.calls}"} for tc in template.tool_calls],
        )
        return ChatResult(generations=[ChatGeneration(message=response)])

    def bind_tools(self, tools: Any, **kwargs) -> "ScriptedChatModel":
        return self

    @property
    def _llm_type(self) -> str:
        return "scripted"


def make_context(warehouse=None) -> RunContext:
    return RunContext(
        org_id=1,
        org_slug="ngo",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_result_rows=100,
        query_timeout_s=30,
        warehouse=warehouse or FakeWarehouse(),
    )


def sql_call(sql: str, call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": "execute_sql", "args": {"sql": sql}, "id": call_id}],
    )


def test_realistic_discovery_turn_fits_in_the_recursion_limit():
    """Regression: a normal 7-tool-call discovery turn must complete under the
    PRODUCTION recursion limit, with the PRODUCTION middleware stack — including
    human-in-the-loop, whose approval pauses are auto-approved here the way the
    consumer resumes them. Every middleware hook is its own graph node (each
    PIIMiddleware adds two), so adding middleware silently shrinks how many tool
    calls fit — this broke real turns at 3 tool calls when the PII middleware
    landed while RECURSION_LIMIT was still 25."""
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command

    from ddpui.core.ai.agent.chat_data_agent import RECURSION_LIMIT

    def tool_call(name, args, call_id):
        return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id}])

    model = ScriptedChatModel(
        script=[
            tool_call("list_schemas", {}, "c1"),
            tool_call("list_tables", {"schema_name": "prod"}, "c2"),
            tool_call("get_table_details", {"schema_name": "prod", "table_name": "t"}, "c3"),
            tool_call("get_table_details", {"schema_name": "prod", "table_name": "t"}, "c4"),
            tool_call(
                "profile_column",
                {"schema_name": "prod", "table_name": "t", "column_name": "c"},
                "c5",
            ),
            sql_call("SELECT COUNT(*) AS n FROM prod.t", "c6"),
            sql_call("SELECT district, COUNT(*) AS n FROM prod.t GROUP BY 1", "c7"),
            AIMessage(content="Here is your answer."),
        ]
    )
    agent = build_agent(model=model, checkpointer=InMemorySaver())
    context = make_context(FakeWarehouse(rows=[{"n": 5}]))
    config = {"configurable": {"thread_id": "t1"}, "recursion_limit": RECURSION_LIMIT}
    result = agent.invoke(
        {"messages": [HumanMessage("tell me about the work order data")]},
        context=context,
        config=config,
    )
    resumes = 0
    while "__interrupt__" in result:
        requests = result["__interrupt__"][0].value["action_requests"]
        decisions = [{"type": "approve"} for _ in requests]
        result = agent.invoke(
            Command(resume={"decisions": decisions}), context=context, config=config
        )
        resumes += 1
    assert result["messages"][-1].content == "Here is your answer."
    assert resumes == 2  # both execute_sql calls paused for approval


def test_sql_error_recovery_second_attempt_succeeds():
    class FlakyWarehouse(FakeWarehouse):
        def execute(self, sql):
            if "districtname" in sql:
                raise RuntimeError('column "districtname" does not exist')
            return super().execute(sql)

    warehouse = FlakyWarehouse(rows=[{"district": "Pune", "n": 700}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT districtname FROM prod.surveys", "c1"),  # fails
            sql_call("SELECT district, COUNT(*) AS n FROM prod.surveys GROUP BY 1", "c2"),
            AIMessage(content="Most surveys were in Pune."),
        ]
    )
    agent = build_agent(model=model, human_in_the_loop=False)
    result = agent.invoke(
        {"messages": [HumanMessage("surveys by district?")]},
        context=make_context(warehouse),
    )
    assert result["messages"][-1].content == "Most surveys were in Pune."
    # the failing SQL error came back as a ToolMessage the model could read
    tool_messages = [m for m in result["messages"] if m.type == "tool"]
    assert any(str(m.content).startswith("Query failed:") for m in tool_messages)


def test_retry_exhaustion_ends_with_deterministic_apology():
    class AlwaysFailingWarehouse(FakeWarehouse):
        def execute(self, sql):
            raise RuntimeError("relation does not exist")

    # model never gives up by itself — the limiter middleware must stop it
    model = ScriptedChatModel(script=[sql_call("SELECT x FROM prod.ghost", "c")])
    agent = build_agent(model=model, human_in_the_loop=False)
    result = agent.invoke(
        {"messages": [HumanMessage("anything?")]},
        context=make_context(AlwaysFailingWarehouse()),
    )
    final = result["messages"][-1]
    assert final.type == "ai"
    assert "rephrase" in final.content
    assert model.calls == 3  # three attempts, then the limiter ended the run


def test_happy_path_runs_guarded_sql_and_answers():
    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="You ran 1,284 surveys in June."),
        ]
    )
    agent = build_agent(model=model, human_in_the_loop=False)

    result = agent.invoke(
        {"messages": [HumanMessage("how many surveys in June?")]},
        context=make_context(warehouse),
    )

    assert result["messages"][-1].content == "You ran 1,284 surveys in June."
    assert any("LIMIT 100" in sql for sql in warehouse.executed)
    assert model.calls == 2
