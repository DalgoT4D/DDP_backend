"""toolsets.py names must match the registry — a typo would silently drop a
tool, an approval gate, or a UI label."""

from ddpui.core.ai import toolsets
from ddpui.core.ai.tools.registry import get_tools


def registered_names() -> set[str]:
    return {tool.name for tool in get_tools()}


def test_every_toolset_name_is_a_registered_tool():
    registered = registered_names()
    for name in (
        *toolsets.SQL_AGENT_TOOLS,
        *toolsets.GUIDE_AGENT_TOOLS,
        *toolsets.SQL_APPROVAL_TOOLS,
        *toolsets.GUIDE_APPROVAL_TOOLS,
        *toolsets.PII_REVIEW_TOOLS,
        toolsets.QUESTION_TOOL,
        toolsets.HANDOFF_TOOL,
    ):
        assert name in registered, name


def test_approval_tools_belong_to_their_agent():
    assert set(toolsets.SQL_APPROVAL_TOOLS) <= set(toolsets.SQL_AGENT_TOOLS)
    assert set(toolsets.GUIDE_APPROVAL_TOOLS) <= set(toolsets.GUIDE_AGENT_TOOLS)


def test_pii_review_tools_are_gated():
    assert set(toolsets.PII_REVIEW_TOOLS) <= set(toolsets.SQL_APPROVAL_TOOLS)


def test_every_registered_tool_has_a_ui_label():
    assert set(toolsets.TOOL_LABELS) == registered_names()
