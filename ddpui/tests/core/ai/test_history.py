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


def test_created_dashboards_replay_on_the_answer():
    """Dashboards created in chat must reappear as chips on reload, exactly
    like the live turn showed them (same chip shape, url_path picks the icon)."""
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
    assert out[1].charts == [{"chart_id": 7, "title": "Field Ops", "url_path": "/dashboards/7"}]


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
    assert out[1].charts == []


def test_created_charts_replay_on_the_answer():
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
    assert out[1].charts == [
        {"chart_id": 42, "title": "Surveys by district", "url_path": "/charts/42"}
    ]
