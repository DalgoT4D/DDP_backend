"""Main dashboard chat LangGraph orchestrator."""

from collections.abc import Callable, Sequence
from functools import lru_cache
from typing import Any

from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig, DashboardChatSourceConfig
from ddpui.core.dashboard_chat.llm_client import DashboardChatLlmClient
from ddpui.core.dashboard_chat.openai_llm_client import OpenAIDashboardChatLlmClient
from ddpui.core.dashboard_chat.vector_store import ChromaDashboardChatVectorStore
from ddpui.core.dashboard_chat.warehouse_tools import DashboardChatWarehouseTools
from ddpui.models.org import Org

from .bindings import bind_dashboard_chat_runtime_methods
from .definition import build_dashboard_chat_graph
from .state import DashboardChatRuntimeState, GREETING_PATTERN
from .tool_specifications import DASHBOARD_CHAT_TOOL_SPECIFICATIONS


class DashboardChatRuntime:
    """Run dashboard chat turns with the prototype's explicit intent routing and tool loop."""

    TOOL_SPECIFICATIONS = DASHBOARD_CHAT_TOOL_SPECIFICATIONS

    def __init__(
        self,
        vector_store: ChromaDashboardChatVectorStore | None = None,
        llm_client: DashboardChatLlmClient | None = None,
        warehouse_tools_factory: Callable[[Org], DashboardChatWarehouseTools] | None = None,
        runtime_config: DashboardChatRuntimeConfig | None = None,
        source_config: DashboardChatSourceConfig | None = None,
    ):
        self.runtime_config = runtime_config or DashboardChatRuntimeConfig.from_env()
        self.source_config = source_config or DashboardChatSourceConfig.from_env()
        self.vector_store = vector_store or ChromaDashboardChatVectorStore()
        self.llm_client = llm_client or OpenAIDashboardChatLlmClient(
            model=self.runtime_config.llm_model,
            timeout_ms=self.runtime_config.llm_timeout_ms,
            max_attempts=self.runtime_config.llm_max_attempts,
        )
        self.warehouse_tools_factory = warehouse_tools_factory or (
            lambda org: DashboardChatWarehouseTools(
                org=org,
                max_rows=self.runtime_config.max_query_rows,
            )
        )
        self.graph = build_dashboard_chat_graph(self)

    def run(
        self,
        org: Org,
        dashboard_id: int,
        user_query: str,
        session_id: str | None = None,
        vector_collection_name: str | None = None,
        conversation_history: Sequence[dict[str, Any]] | None = None,
    ):
        """Run one dashboard chat turn."""
        if hasattr(self.llm_client, "reset_usage"):
            self.llm_client.reset_usage()
        if hasattr(self.vector_store, "reset_usage"):
            self.vector_store.reset_usage()
        initial_state: DashboardChatRuntimeState = {
            "org": org,
            "dashboard_id": dashboard_id,
            "session_id": session_id,
            "vector_collection_name": vector_collection_name,
            "user_query": user_query,
            "conversation_history": self._normalize_conversation_history(conversation_history),
            "warnings": [],
            "usage": {},
        }
        final_state = self.graph.invoke(initial_state)
        return final_state["response"]

@lru_cache(maxsize=1)
def get_dashboard_chat_runtime() -> DashboardChatRuntime:
    """Return the shared dashboard chat runtime used by live chat turns."""
    return DashboardChatRuntime()

bind_dashboard_chat_runtime_methods(DashboardChatRuntime)

__all__ = [
    "DashboardChatRuntime",
    "DashboardChatRuntimeState",
    "GREETING_PATTERN",
    "get_dashboard_chat_runtime",
]
