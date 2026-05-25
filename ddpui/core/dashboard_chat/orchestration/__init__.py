"""LangGraph orchestration modules for dashboard chat."""

from __future__ import annotations

from typing import Any

__all__ = ["DashboardChatRuntime", "get_dashboard_chat_runtime"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from ddpui.core.dashboard_chat.orchestration.orchestrator import (
            DashboardChatRuntime,
            get_dashboard_chat_runtime,
        )

        exported = {
            "DashboardChatRuntime": DashboardChatRuntime,
            "get_dashboard_chat_runtime": get_dashboard_chat_runtime,
        }
        return exported[name]
    raise AttributeError(name)
