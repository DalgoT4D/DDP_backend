"""Tests for replaying checkpointer messages into UI-shaped history."""

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ddpui.core.ai.chat.history import map_messages


def test_maps_turns_with_sql_attachments_on_the_answer():
    artifact = {
        "sql": "SELECT COUNT(*) AS n FROM prod.surveys LIMIT 100",
        "status": "success",
        "row_count": 1,
        "columns": ["n"],
        "rows": [["1284"]],
    }
    messages = [
        HumanMessage("how many surveys?"),
        AIMessage("", tool_calls=[{"name": "execute_sql", "args": {}, "id": "c1"}]),
        ToolMessage(
            content="Query returned 1 rows.",
            name="execute_sql",
            tool_call_id="c1",
            artifact=artifact,
        ),
        AIMessage("You ran 1,284 surveys."),
    ]

    out = map_messages(messages)

    assert [(m.role, m.content) for m in out] == [
        ("user", "how many surveys?"),
        ("assistant", "You ran 1,284 surveys."),
    ]
    assert out[1].sql_attachments[0].sql == artifact["sql"]
    assert out[1].sql_attachments[0].rows == [["1284"]]


def test_non_sql_tools_and_empty_ai_messages_are_hidden():
    messages = [
        HumanMessage("q"),
        AIMessage("", tool_calls=[{"name": "list_tables", "args": {}, "id": "c1"}]),
        ToolMessage(content="Tables in prod: ...", name="list_tables", tool_call_id="c1"),
        AIMessage("Answer."),
    ]
    out = map_messages(messages)
    assert [(m.role, m.content) for m in out] == [("user", "q"), ("assistant", "Answer.")]
    assert out[1].sql_attachments == []


def test_block_list_content_renders_only_text():
    """Thinking-enabled models store content as block lists (signed thinking
    block + text). History must replay only the text, never the block repr."""
    messages = [
        HumanMessage("how many surveys?"),
        AIMessage(
            content=[
                {"type": "thinking", "thinking": "", "signature": "Eq8FCkYIBxgCKkB..."},
                {"type": "text", "text": "You ran 1,284 surveys."},
            ]
        ),
    ]
    out = map_messages(messages)
    assert [(m.role, m.content) for m in out] == [
        ("user", "how many surveys?"),
        ("assistant", "You ran 1,284 surveys."),
    ]


def test_legacy_dashboard_artifacts_replay_on_the_answer():
    """Checkpoints written before CreatedArtifact keyed the id as dashboard_id;
    reloading those sessions must still show the chip."""
    messages = [
        HumanMessage("put it on a new dashboard"),
        AIMessage("", tool_calls=[{"name": "create_dashboard", "args": {}, "id": "d1"}]),
        ToolMessage(
            content="Done — dashboard 'Field Ops' (id 7).",
            name="create_dashboard",
            tool_call_id="d1",
            artifact={
                "type": "dashboard",
                "dashboard_id": 7,
                "title": "Field Ops",
                "url_path": "/dashboards/7",
            },
        ),
        AIMessage("Created the Field Ops dashboard."),
    ]
    out = map_messages(messages)
    assert [a.model_dump() for a in out[1].artifacts] == [
        {"type": "dashboard", "object_id": 7, "title": "Field Ops", "url_path": "/dashboards/7"}
    ]


def test_rejected_creations_do_not_replay_as_chips():
    messages = [
        HumanMessage("chart it"),
        AIMessage("", tool_calls=[{"name": "create_chart", "args": {}, "id": "c1"}]),
        ToolMessage(
            content="Chart not created: no permission",
            name="create_chart",
            tool_call_id="c1",
            artifact={"type": "chart", "status": "rejected", "error": "no permission"},
        ),
        AIMessage("I couldn't create the chart."),
    ]
    out = map_messages(messages)
    assert out[1].artifacts == []


def test_legacy_chart_artifacts_replay_on_the_answer():
    messages = [
        HumanMessage("chart surveys by district"),
        AIMessage("", tool_calls=[{"name": "create_chart", "args": {}, "id": "c1"}]),
        ToolMessage(
            content="Created chart 'Surveys by district' (id 42).",
            name="create_chart",
            tool_call_id="c1",
            artifact={
                "type": "chart",
                "chart_id": 42,
                "title": "Surveys by district",
                "url_path": "/charts/42",
            },
        ),
        AIMessage("Done — it's in your Charts page."),
    ]
    out = map_messages(messages)
    assert [a.model_dump() for a in out[1].artifacts] == [
        {"type": "chart", "object_id": 42, "title": "Surveys by district", "url_path": "/charts/42"}
    ]


def test_created_metrics_replay_with_their_type():
    """The chip carries the object type, so the UI never guesses from url_path."""
    messages = [
        HumanMessage("make a metric for total surveys"),
        AIMessage("", tool_calls=[{"name": "create_metric", "args": {}, "id": "m1"}]),
        ToolMessage(
            content="Done — metric 'Total surveys' (id 5).",
            name="create_metric",
            tool_call_id="m1",
            artifact={
                "type": "metric",
                "object_id": 5,
                "title": "Total surveys",
                "url_path": "/metrics",
            },
        ),
        AIMessage("Created the metric."),
    ]
    out = map_messages(messages)
    assert [a.model_dump() for a in out[1].artifacts] == [
        {"type": "metric", "object_id": 5, "title": "Total surveys", "url_path": "/metrics"}
    ]


def test_a_message_typed_instead_of_approving_replays_as_the_users_bubble():
    from ddpui.core.ai.agent.hitl import REDIRECT_PREFIX

    messages = [
        HumanMessage("all districts in maharashtra"),
        AIMessage("", tool_calls=[{"name": "execute_sql", "args": {}, "id": "c1"}]),
        ToolMessage(
            content=f"{REDIRECT_PREFIX}no, only Pune",
            name="execute_sql",
            tool_call_id="c1",
            status="error",
        ),
        AIMessage("Here is Pune."),
    ]

    out = map_messages(messages)

    assert [(m.role, m.content) for m in out] == [
        ("user", "all districts in maharashtra"),
        ("user", "no, only Pune"),
        ("assistant", "Here is Pune."),
    ]


def _ask(call_id: str, question: str) -> AIMessage:
    return AIMessage(
        "", tool_calls=[{"name": "ask_user", "args": {"question": question}, "id": call_id}]
    )


def test_an_ask_user_exchange_replays_as_question_and_answer():
    messages = [
        HumanMessage("how many enrollments?"),
        _ask("q1", "Which program do you mean?"),
        ToolMessage(content="Girls' Education", name="ask_user", tool_call_id="q1"),
        AIMessage("For Girls' Education: 312 enrollments."),
    ]

    assert [(m.role, m.content) for m in map_messages(messages)] == [
        ("user", "how many enrollments?"),
        ("assistant", "Which program do you mean?"),
        ("user", "Girls' Education"),
        ("assistant", "For Girls' Education: 312 enrollments."),
    ]


def test_an_unanswered_question_shows_no_placeholder_reply():
    from ddpui.core.ai.agent.hitl import NO_ANSWER

    messages = [
        HumanMessage("q"),
        _ask("q1", "Which program?"),
        ToolMessage(content=NO_ANSWER, name="ask_user", tool_call_id="q1"),
        AIMessage("Here are all programs."),
    ]

    assert [m.role for m in map_messages(messages)] == ["user", "assistant", "assistant"]


def test_one_message_resolving_a_question_and_a_step_replays_once():
    from ddpui.core.ai.agent.hitl import REDIRECT_PREFIX

    messages = [
        HumanMessage("q"),
        AIMessage(
            "",
            tool_calls=[
                {"name": "ask_user", "args": {"question": "Which month?"}, "id": "q1"},
                {"name": "execute_sql", "args": {}, "id": "c1"},
            ],
        ),
        ToolMessage(content="June only", name="ask_user", tool_call_id="q1"),
        ToolMessage(
            content=f"{REDIRECT_PREFIX}June only",
            name="execute_sql",
            tool_call_id="c1",
            status="error",
        ),
        AIMessage("June: 40."),
    ]

    replies = [m.content for m in map_messages(messages) if m.role == "user"]
    assert replies == ["q", "June only"]
