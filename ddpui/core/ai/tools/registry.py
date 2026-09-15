"""Tool registry — the extension point for the Chat with Data agent.

Every tool module registers its tools with @register_tool at import time.
Adding a capability (charts, exports, ...) means one new module here and an
import line in get_tools(); the agent graph never changes.
"""

import importlib

from langchain_core.tools import BaseTool

_REGISTRY: dict[str, BaseTool] = {}

# Modules that define @register_tool tools, imported lazily by get_tools() so that
# registry.py itself never has a circular import on the tool modules.
_TOOL_MODULES = (
    "ddpui.core.ai.tools.schema_tools",
    "ddpui.core.ai.tools.profile_tools",
    "ddpui.core.ai.tools.sql_tools",
    "ddpui.core.ai.tools.chart_tools",
    "ddpui.core.ai.tools.dashboard_tools",
    "ddpui.core.ai.tools.clarify_tools",
    "ddpui.core.ai.tools.docs_tools",
    "ddpui.core.ai.tools.guide_tools",
    "ddpui.core.ai.tools.metric_tools",
    "ddpui.core.ai.tools.report_tools",
)


def register_tool(tool_obj: BaseTool) -> BaseTool:
    """Decorator: add a tool to the registry (idempotent by tool name)."""
    _REGISTRY[tool_obj.name] = tool_obj
    return tool_obj


def get_tools(names: tuple[str, ...] | list[str] | None = None) -> list[BaseTool]:
    """Registered tools, importing the tool modules on first call.

    `names` selects a per-agent subset (each agent module declares its own
    tool-name tuple); None returns everything. Unknown names raise so a typo
    in an agent's tool list fails at build, not silently at runtime."""
    for module_name in _TOOL_MODULES:
        importlib.import_module(module_name)
    if names is None:
        return list(_REGISTRY.values())
    missing = [name for name in names if name not in _REGISTRY]
    if missing:
        raise KeyError(f"unknown tool name(s): {missing}")
    return [_REGISTRY[name] for name in names]
