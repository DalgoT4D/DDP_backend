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
    "ddpui.core.chat_with_data.tools.schema_tools",
    "ddpui.core.chat_with_data.tools.profile_tools",
    "ddpui.core.chat_with_data.tools.sql_tools",
)


def register_tool(tool_obj: BaseTool) -> BaseTool:
    """Decorator: add a tool to the registry (idempotent by tool name)."""
    _REGISTRY[tool_obj.name] = tool_obj
    return tool_obj


def get_tools() -> list[BaseTool]:
    """All registered tools, importing the tool modules on first call."""
    for module_name in _TOOL_MODULES:
        importlib.import_module(module_name)
    return list(_REGISTRY.values())
