"""Prototype-faithful LangGraph runtime for dashboard chat orchestration."""

from collections.abc import Callable, Sequence
import json
import logging
import re
from typing import Any, TypedDict

from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder
from django.db import close_old_connections, connections
from langgraph.graph import END, START, StateGraph

from ddpui.core.dashboard_chat.allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
    build_dashboard_chat_table_name,
    normalize_dashboard_chat_table_name,
)
from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.llm_client import (
    DashboardChatLlmClient,
    OpenAIDashboardChatLlmClient,
)
from ddpui.core.dashboard_chat.session_cache import (
    DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS,
    build_dashboard_chat_session_snapshot_cache_key,
    deserialize_allowlist,
    deserialize_distinct_cache,
    deserialize_schema_snippets,
    serialize_allowlist,
    serialize_distinct_cache,
    serialize_schema_snippets,
)
from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatCitation,
    DashboardChatConversationContext,
    DashboardChatConversationMessage,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSchemaSnippet,
    DashboardChatSqlValidationResult,
)
from ddpui.core.dashboard_chat.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector_documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.vector_store import ChromaDashboardChatVectorStore
from ddpui.core.dashboard_chat.warehouse_tools import (
    DashboardChatWarehouseTools,
    DashboardChatWarehouseToolsError,
)
from ddpui.models.dashboard_chat import (
    DashboardChatPromptTemplateKey,
)
from ddpui.models.org import Org
from ddpui.services.dashboard_service import DashboardService

logger = logging.getLogger(__name__)
GREETING_PATTERN = re.compile(
    r"^\s*(hi|hello|hey|yo|good\s+morning|good\s+afternoon|good\s+evening|thanks|thank\s+you)\b[\s!.?]*$",
    re.IGNORECASE,
)


class DashboardChatRuntimeState(TypedDict, total=False):
    """LangGraph state for one dashboard chat turn."""

    org: Org
    dashboard_id: int
    session_id: str | None
    vector_collection_name: str | None
    user_query: str
    conversation_history: list[DashboardChatConversationMessage]
    conversation_context: DashboardChatConversationContext
    small_talk_response: str | None
    dashboard_export: dict[str, Any]
    dbt_index: dict[str, Any]
    allowlist: DashboardChatAllowlist
    session_schema_cache: dict[str, DashboardChatSchemaSnippet]
    session_distinct_cache: set[tuple[str, str, str]]
    intent_decision: DashboardChatIntentDecision
    retrieved_documents: list[DashboardChatRetrievedDocument]
    citations: list[DashboardChatCitation]
    tool_calls: list[dict[str, Any]]
    sql: str | None
    sql_validation: DashboardChatSqlValidationResult | None
    sql_results: list[dict[str, Any]] | None
    warnings: list[str]
    usage: dict[str, Any]
    response: DashboardChatResponse


class DashboardChatRuntime:
    """Run dashboard chat turns with the prototype's explicit intent routing and tool loop."""

    TOOL_SPECIFICATIONS = [
        {
            "type": "function",
            "function": {
                "name": "retrieve_docs",
                "description": "Search for relevant charts, datasets, dbt models, or context sections.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "types": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["chart", "dataset", "context", "dbt_model"],
                            },
                            "description": "Document types to search",
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_schema_snippets",
                "description": "Get column information for database tables.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "tables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Fully-qualified table names (schema.table)",
                        }
                    },
                    "required": ["tables"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_dbt_models",
                "description": "Search dbt models by keyword to find relevant data models.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query for model names/descriptions",
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_dbt_model_info",
                "description": "Get detailed information about a specific dbt model.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "model_name": {
                            "type": "string",
                            "description": "Model name or schema.table",
                        }
                    },
                    "required": ["model_name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_distinct_values",
                "description": "Get distinct values for a column (required before filtering on text columns).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Fully-qualified table name",
                        },
                        "column": {"type": "string", "description": "Column name"},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                    },
                    "required": ["table", "column"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_sql_query",
                "description": "Execute a read-only SQL query on the database.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SELECT query to execute"}
                    },
                    "required": ["sql"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_tables_by_keyword",
                "description": "Find tables whose name or columns match a keyword (no hard-coding).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Keyword such as donor, funding, student",
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 15},
                    },
                    "required": ["keyword"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "check_table_row_count",
                "description": "Get the total number of rows in a table to check if it has data.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Fully-qualified table name (schema.table)",
                        }
                    },
                    "required": ["table"],
                },
            },
        },
    ]

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
        self.graph = self._build_graph()

    def run(
        self,
        org: Org,
        dashboard_id: int,
        user_query: str,
        session_id: str | None = None,
        vector_collection_name: str | None = None,
        conversation_history: Sequence[DashboardChatConversationMessage | dict[str, Any]]
        | None = None,
    ) -> DashboardChatResponse:
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

    def _build_graph(self):
        """Build the explicit prototype-aligned intent graph."""
        graph = StateGraph(DashboardChatRuntimeState)
        graph.add_node("load_context", self._wrap_node(self._node_load_context))
        graph.add_node("route_intent", self._wrap_node(self._node_route_intent))
        graph.add_node("handle_small_talk", self._wrap_node(self._node_handle_small_talk))
        graph.add_node("handle_irrelevant", self._wrap_node(self._node_handle_irrelevant))
        graph.add_node(
            "handle_needs_clarification",
            self._wrap_node(self._node_handle_needs_clarification),
        )
        graph.add_node("handle_query_with_sql", self._wrap_node(self._node_handle_query_with_sql))
        graph.add_node(
            "handle_query_without_sql",
            self._wrap_node(self._node_handle_query_without_sql),
        )
        graph.add_node("handle_follow_up_sql", self._wrap_node(self._node_handle_follow_up_sql))
        graph.add_node(
            "handle_follow_up_context",
            self._wrap_node(self._node_handle_follow_up_context),
        )
        graph.add_node("finalize", self._wrap_node(self._node_finalize_response))

        graph.add_edge(START, "load_context")
        graph.add_edge("load_context", "route_intent")
        graph.add_conditional_edges(
            "route_intent",
            self._route_after_intent,
            {
                DashboardChatIntent.SMALL_TALK.value: "handle_small_talk",
                DashboardChatIntent.IRRELEVANT.value: "handle_irrelevant",
                DashboardChatIntent.NEEDS_CLARIFICATION.value: "handle_needs_clarification",
                DashboardChatIntent.QUERY_WITH_SQL.value: "handle_query_with_sql",
                DashboardChatIntent.QUERY_WITHOUT_SQL.value: "handle_query_without_sql",
                DashboardChatIntent.FOLLOW_UP_SQL.value: "handle_follow_up_sql",
                DashboardChatIntent.FOLLOW_UP_CONTEXT.value: "handle_follow_up_context",
            },
        )
        graph.add_edge("handle_small_talk", "finalize")
        graph.add_edge("handle_irrelevant", "finalize")
        graph.add_edge("handle_needs_clarification", "finalize")
        graph.add_edge("handle_query_with_sql", "finalize")
        graph.add_edge("handle_query_without_sql", "finalize")
        graph.add_edge("handle_follow_up_sql", "finalize")
        graph.add_edge("handle_follow_up_context", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile()

    @staticmethod
    def _wrap_node(handler: Callable[[DashboardChatRuntimeState], DashboardChatRuntimeState]):
        """Run each LangGraph node with thread-local Django DB cleanup."""

        def wrapped(state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
            close_old_connections()
            try:
                return handler(state)
            finally:
                connections.close_all()

        return wrapped

    def _node_load_context(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Load or reuse the session-stable dashboard context snapshot."""
        snapshot = self._load_session_snapshot(state)
        state["dashboard_export"] = snapshot["dashboard_export"]
        state["dbt_index"] = snapshot["dbt_index"]
        state["allowlist"] = snapshot["allowlist"]
        state["session_schema_cache"] = snapshot["schema_cache"]
        state["session_distinct_cache"] = snapshot["distinct_cache"]
        return state

    def _node_route_intent(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Use the prototype router prompt for all non-trivial routing."""
        conversation_context = self._extract_conversation_context(state["conversation_history"])
        fast_path_intent = self._build_fast_path_intent(state["user_query"])
        if fast_path_intent is not None:
            state["conversation_context"] = conversation_context
            state["intent_decision"] = fast_path_intent
            state["small_talk_response"] = self._build_fast_path_small_talk_response(
                state["user_query"]
            )
            return state
        intent_decision = self.llm_client.classify_intent(
            user_query=state["user_query"],
            conversation_context=conversation_context,
        )
        state["conversation_context"] = conversation_context
        state["intent_decision"] = intent_decision
        return state

    def _node_handle_small_talk(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Handle simple social turns without any tool use."""
        state["response"] = DashboardChatResponse(
            answer_text=state.get("small_talk_response")
            or self._compose_small_talk_response(state["user_query"]),
            intent=DashboardChatIntent.SMALL_TALK,
            usage=self._build_usage_summary(),
        )
        return state

    def _node_handle_irrelevant(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Handle questions outside dashboard chat scope."""
        state["response"] = DashboardChatResponse(
            answer_text=(
                "I can only answer questions about this dashboard, its charts, and the data behind them."
            ),
            intent=DashboardChatIntent.IRRELEVANT,
            usage=self._build_usage_summary(),
        )
        return state

    def _node_handle_needs_clarification(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Ask for clarification when the router says the query is underspecified."""
        intent_decision = state["intent_decision"]
        state["response"] = DashboardChatResponse(
            answer_text=(
                intent_decision.clarification_question
                or self._clarification_fallback(intent_decision.missing_info)
            ),
            intent=DashboardChatIntent.NEEDS_CLARIFICATION,
            usage=self._build_usage_summary(),
        )
        return state

    def _node_handle_query_with_sql(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Run the prototype new-query tool loop for SQL-routed questions."""
        return self._run_prototype_intent(state, max_turns=15, follow_up=False)

    def _node_handle_query_without_sql(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Run the prototype new-query tool loop for context-only questions."""
        return self._run_prototype_intent(state, max_turns=15, follow_up=False)

    def _node_handle_follow_up_sql(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Run the prototype follow-up loop for SQL-modifying turns."""
        return self._run_prototype_intent(state, max_turns=6, follow_up=True)

    def _node_handle_follow_up_context(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Run the prototype follow-up loop for explanatory follow-ups."""
        return self._run_prototype_intent(state, max_turns=6, follow_up=True)

    def _run_prototype_intent(
        self,
        state: DashboardChatRuntimeState,
        *,
        max_turns: int,
        follow_up: bool,
    ) -> DashboardChatRuntimeState:
        """Execute one prototype-style tool loop and store the response on state."""
        allowlist = state["allowlist"]

        query_embedding = self._embed_query(
            state["user_query"],
            embedding_cache={},
        )

        messages = (
            self._build_follow_up_messages(state)
            if follow_up
            else self._build_new_query_messages(state)
        )
        execution_result = self._execute_tool_loop(
            state=state,
            messages=messages,
            max_turns=max_turns,
            initial_embedding_cache={state["user_query"]: query_embedding},
        )

        state["retrieved_documents"] = execution_result["retrieved_documents"]
        state["citations"] = self._build_citations(
            retrieved_documents=execution_result["retrieved_documents"],
            dashboard_export=state["dashboard_export"],
            allowlist=allowlist,
        )
        state["tool_calls"] = execution_result["tool_calls"]
        state["sql"] = execution_result["sql"]
        state["sql_validation"] = execution_result["sql_validation"]
        state["sql_results"] = execution_result["sql_results"]
        state["warnings"] = execution_result["warnings"]
        state["response"] = DashboardChatResponse(
            answer_text=execution_result["answer_text"],
            intent=state["intent_decision"].intent,
            citations=state["citations"],
            warnings=execution_result["warnings"],
            sql=execution_result["sql"],
            sql_results=execution_result["sql_results"],
            usage=self._build_usage_summary(),
            tool_calls=execution_result["tool_calls"],
        )
        return state

    def _build_new_query_messages(
        self,
        state: DashboardChatRuntimeState,
    ) -> list[dict[str, Any]]:
        """Build the prototype new-query message stack."""
        system_prompt = self.llm_client.get_prompt(
            DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM
        )
        return [
            {
                "role": "system",
                "content": system_prompt,
            },
            {"role": "user", "content": state["user_query"]},
        ]

    def _build_follow_up_messages(
        self,
        state: DashboardChatRuntimeState,
    ) -> list[dict[str, Any]]:
        """Build the prototype follow-up message stack."""
        modification_type = self._detect_sql_modification_type(state["user_query"])
        system_prompt = self.llm_client.get_prompt(
            DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM
        )
        return [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "system",
                "content": self._build_follow_up_context_prompt(
                    state["conversation_context"],
                    state["user_query"],
                ),
            },
            {"role": "system", "content": f"MODIFICATION_TYPE: {modification_type}"},
            {"role": "user", "content": state["user_query"]},
        ]

    def _execute_tool_loop(
        self,
        *,
        state: DashboardChatRuntimeState,
        messages: list[dict[str, Any]],
        max_turns: int,
        initial_embedding_cache: dict[str, list[float]] | None = None,
    ) -> dict[str, Any]:
        """Execute the prototype's iterative tool loop."""
        execution_context: dict[str, Any] = {
            "distinct_cache": set(state.get("session_distinct_cache") or set()),
            "embedding_cache": dict(initial_embedding_cache or {}),
            "schema_cache": dict(state.get("session_schema_cache") or {}),
            "retrieved_documents": [],
            "retrieved_document_ids": set(),
            "tool_calls": [],
            "warnings": list(state.get("warnings", [])),
            "warehouse_tools": None,
            "last_sql": None,
            "last_sql_results": None,
            "last_sql_validation": None,
        }
        self._seed_distinct_cache_from_previous_sql(state, execution_context)
        intent_decision = state["intent_decision"]

        for turn_index in range(max_turns):
            tool_choice = "required" if intent_decision.force_tool_usage and turn_index == 0 else "auto"
            ai_message = self.llm_client.run_tool_loop_turn(
                messages=messages,
                tools=self.TOOL_SPECIFICATIONS,
                tool_choice=tool_choice,
                operation=f"tool_loop_{intent_decision.intent.value}",
            )
            tool_calls = ai_message.get("tool_calls") or []
            assistant_record: dict[str, Any] = {
                "role": "assistant",
                "content": ai_message.get("content", "") or "",
            }
            if tool_calls:
                assistant_record["tool_calls"] = [
                    {
                        "id": tool_call.get("id"),
                        "type": "function",
                        "function": {
                            "name": tool_call.get("name"),
                            "arguments": (
                                tool_call.get("args")
                                if isinstance(tool_call.get("args"), str)
                                else json.dumps(tool_call.get("args") or {})
                            ),
                        },
                    }
                    for tool_call in tool_calls
                ]
            messages.append(assistant_record)

            if not tool_calls:
                return self._build_execution_result(
                    answer_text=(
                        (ai_message.get("content") or "").strip()
                        or self._fallback_answer_text(
                            execution_context["retrieved_documents"],
                            execution_context["last_sql_results"],
                        )
                    ),
                    execution_context=execution_context,
                    max_turns_reached=False,
                )

            for tool_call in tool_calls:
                raw_args = tool_call.get("args") or {}
                args = raw_args
                if isinstance(raw_args, str):
                    try:
                        args = json.loads(raw_args)
                    except json.JSONDecodeError:
                        args = {}
                result = self._execute_tool(
                    tool_name=str(tool_call.get("name") or ""),
                    args=args,
                    state=state,
                    execution_context=execution_context,
                )
                execution_context["tool_calls"].append(
                    self._summarize_tool_call(
                        tool_name=str(tool_call.get("name") or ""),
                        args=args,
                        result=result,
                    )
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(
                            self._serialize_tool_result(result),
                            cls=DjangoJSONEncoder,
                        ),
                    }
                )
                if str(tool_call.get("name") or "") == "run_sql_query" and result.get("success"):
                    return self._build_execution_result(
                        answer_text=(
                            result.get("data_preview")
                            or self._fallback_answer_text(
                                execution_context["retrieved_documents"],
                                execution_context["last_sql_results"],
                            )
                        ),
                        execution_context=execution_context,
                        max_turns_reached=False,
                    )

        return self._build_execution_result(
            answer_text=self._max_turns_message(
                state["user_query"],
                execution_context["retrieved_documents"],
            ),
            execution_context=execution_context,
            max_turns_reached=True,
        )

    def _execute_tool(
        self,
        *,
        tool_name: str,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute one prototype tool against the Dalgo runtime primitives."""
        try:
            if tool_name == "retrieve_docs":
                return self._tool_retrieve_docs(args, state, execution_context)
            if tool_name == "get_schema_snippets":
                return self._tool_get_schema_snippets(args, state, execution_context)
            if tool_name == "search_dbt_models":
                return self._tool_search_dbt_models(args, state, execution_context)
            if tool_name == "get_dbt_model_info":
                return self._tool_get_dbt_model_info(args, state, execution_context)
            if tool_name == "get_distinct_values":
                return self._tool_get_distinct_values(args, state, execution_context)
            if tool_name == "run_sql_query":
                return self._run_sql_with_distinct_guard(args, state, execution_context)
            if tool_name == "list_tables_by_keyword":
                return self._tool_list_tables_by_keyword(args, state, execution_context)
            if tool_name == "check_table_row_count":
                return self._tool_check_table_row_count(args, state, execution_context)
            return {"error": f"Unknown tool: {tool_name}"}
        except DashboardChatWarehouseToolsError as error:
            logger.warning("Dashboard chat tool %s failed: %s", tool_name, error)
            execution_context["warnings"].append(str(error))
            return {"error": str(error)}
        except Exception as error:
            logger.exception("Dashboard chat tool %s failed", tool_name)
            execution_context["warnings"].append(str(error))
            return {"error": str(error)}

    def _tool_retrieve_docs(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Retrieve current-dashboard, org, and dbt context using the prototype tool contract."""
        query = str(args.get("query") or state["user_query"]).strip()
        limit = max(1, min(int(args.get("limit", 8)), 20))
        requested_types = [
            str(doc_type)
            for doc_type in (args.get("types") or ["chart", "dataset", "context", "dbt_model"])
        ]
        retrieved_documents: list[DashboardChatRetrievedDocument] = []

        if "chart" in requested_types:
            retrieved_documents.extend(
                self._query_vector_store(
                    org=state["org"],
                    collection_name=state.get("vector_collection_name"),
                    query_text=query,
                    source_types=self.source_config.filter_enabled(
                        [DashboardChatSourceType.DASHBOARD_EXPORT.value]
                    ),
                    dashboard_id=state["dashboard_id"],
                    query_embedding=self._embed_query(query, execution_context["embedding_cache"]),
                )
            )
        if "context" in requested_types:
            retrieved_documents.extend(
                self._query_vector_store(
                    org=state["org"],
                    collection_name=state.get("vector_collection_name"),
                    query_text=query,
                    source_types=self.source_config.filter_enabled(
                        [DashboardChatSourceType.DASHBOARD_CONTEXT.value]
                    ),
                    dashboard_id=state["dashboard_id"],
                    query_embedding=self._embed_query(query, execution_context["embedding_cache"]),
                )
            )
            retrieved_documents.extend(
                self._query_vector_store(
                    org=state["org"],
                    collection_name=state.get("vector_collection_name"),
                    query_text=query,
                    source_types=self.source_config.filter_enabled(
                        [DashboardChatSourceType.ORG_CONTEXT.value]
                    ),
                    query_embedding=self._embed_query(query, execution_context["embedding_cache"]),
                )
            )
        if "dataset" in requested_types or "dbt_model" in requested_types:
            dbt_results = self._query_vector_store(
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=self.source_config.filter_enabled(
                    [
                        DashboardChatSourceType.DBT_MANIFEST.value,
                        DashboardChatSourceType.DBT_CATALOG.value,
                    ]
                ),
                query_embedding=self._embed_query(query, execution_context["embedding_cache"]),
            )
            retrieved_documents.extend(
                self._filter_allowlisted_dbt_results(dbt_results, state["allowlist"])
            )

        merged_results = self._dedupe_retrieved_documents(retrieved_documents)[:limit]
        for document in merged_results:
            if document.document_id in execution_context["retrieved_document_ids"]:
                continue
            execution_context["retrieved_document_ids"].add(document.document_id)
            execution_context["retrieved_documents"].append(document)

        docs = [
            self._tool_document_payload(
                document,
                state["allowlist"],
                state["dashboard_export"],
            )
            for document in merged_results
        ]
        return {"docs": docs, "count": len(docs)}

    def _tool_get_schema_snippets(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Return schema snippets for allowlisted tables only."""
        requested_tables = [str(table_name).lower() for table_name in args.get("tables") or []]
        allowed_tables = [
            table_name
            for table_name in requested_tables
            if state["allowlist"].is_allowed(table_name)
        ]
        filtered_tables = sorted(set(requested_tables) - set(allowed_tables))
        schema_cache = self._schema_cache(
            state,
            execution_context,
            tables=allowed_tables,
        )
        tables_payload = [
            {"table": table_name, "columns": snippet.columns}
            for table_name, snippet in schema_cache.items()
            if table_name in allowed_tables
        ]
        response: dict[str, Any] = {"tables": tables_payload}
        if filtered_tables:
            response["filtered_tables"] = filtered_tables
            response["filter_note"] = (
                f"{len(filtered_tables)} tables were filtered out because they are not used by the current dashboard."
            )
        return response

    def _tool_search_dbt_models(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Search allowlisted dbt nodes by name, description, and column metadata."""
        query = str(args.get("query") or "").strip().lower()
        limit = max(1, min(int(args.get("limit", 8)), 20))
        if not query:
            return {"models": [], "count": 0}

        results: list[dict[str, Any]] = []
        for node in self._dbt_resources_by_unique_id(state).values():
            table_name = node.get("table")
            haystacks = [
                str(node.get("name") or ""),
                str(node.get("description") or ""),
                str(table_name or ""),
            ]
            for column in node.get("columns") or []:
                haystacks.append(str(column.get("name") or ""))
                haystacks.append(str(column.get("description") or ""))
            if query not in " ".join(haystacks).lower():
                continue
            results.append(
                {
                    "name": str(node.get("name") or ""),
                    "schema": str(node.get("schema") or ""),
                    "database": str(node.get("database") or ""),
                    "description": str(node.get("description") or ""),
                    "columns": [
                        str(column.get("name") or "")
                        for column in (node.get("columns") or [])
                    ][:20],
                    "table": table_name,
                }
            )
            if len(results) >= limit:
                break

        return {"models": results, "count": len(results)}

    def _tool_get_dbt_model_info(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Return one dbt model's description, columns, and lineage."""
        model_name = str(args.get("model_name") or "").strip().lower()
        if not model_name:
            return {"error": "model_name is required"}

        matched_unique_id: str | None = None
        matched_node: dict[str, Any] | None = None
        for unique_id, node in self._dbt_resources_by_unique_id(state).items():
            table_name = node.get("table")
            candidates = {
                str(node.get("name") or "").lower(),
                str(table_name or "").lower(),
            }
            if model_name not in candidates:
                continue
            matched_unique_id = unique_id
            matched_node = node
            break

        if matched_unique_id is None or matched_node is None:
            return {"error": f"Model not found: {model_name}"}

        return {
            "model": str(matched_node.get("name") or ""),
            "schema": str(matched_node.get("schema") or ""),
            "database": str(matched_node.get("database") or ""),
            "description": str(matched_node.get("description") or ""),
            "columns": list(matched_node.get("columns") or [])[:50],
            "upstream": list(matched_node.get("upstream") or []),
            "downstream": list(matched_node.get("downstream") or []),
        }

    def _tool_get_distinct_values(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Return distinct values and persist validated filter values for the session."""
        table_name = str(args.get("table") or "").lower()
        column_name = str(args.get("column") or "")
        limit = max(1, min(int(args.get("limit", 50)), 200))
        if not state["allowlist"].is_allowed(table_name):
            return {
                "error": "table_not_allowed",
                "table": table_name,
                "message": (
                    f"Table {table_name} is not accessible in the current dashboard context."
                ),
            }

        schema_cache = self._schema_cache(state, execution_context)
        snippet = schema_cache.get(table_name)
        normalized_column_name = column_name.lower()
        if snippet is not None and normalized_column_name not in {
            str(column.get("name") or "").lower() for column in snippet.columns
        }:
            candidates = self._find_tables_with_column(normalized_column_name, schema_cache)
            return {
                "error": "column_not_in_table",
                "table": table_name,
                "column": column_name,
                "candidates": candidates,
                "message": (
                    f"Column {column_name} is not available on {table_name}. "
                    "Use a table that contains it, inspect that schema, and retry the lookup."
                ),
            }

        values = self._warehouse_tools(execution_context, state["org"]).get_distinct_values(
            table_name=table_name,
            column_name=column_name,
            limit=limit,
        )
        self._record_validated_distinct_values(
            state=state,
            execution_context=execution_context,
            table_name=table_name,
            column_name=column_name,
            values=values,
        )
        return {
            "table": table_name,
            "column": column_name,
            "values": values,
            "count": len(values),
        }

    def _tool_list_tables_by_keyword(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Search allowlisted tables by table name or column name."""
        keyword = str(args.get("keyword") or "").strip().lower()
        limit = max(1, min(int(args.get("limit", 15)), 50))
        if not keyword:
            return {"tables": []}

        allowlist_tables_source = state["allowlist"].prioritized_tables() or sorted(
            state["allowlist"].allowed_tables
        )
        allowlisted_tables = list(
            dict.fromkeys(table_name.lower() for table_name in allowlist_tables_source)
        )
        direct_match_tables = [
            table_name
            for table_name in allowlisted_tables
            if keyword in table_name or keyword in table_name.rsplit(".", 1)[-1]
        ]

        schema_cache: dict[str, Any] = {}
        lookup_tables = direct_match_tables or allowlisted_tables
        if lookup_tables:
            try:
                schema_cache = self._schema_cache(
                    state,
                    execution_context,
                    tables=lookup_tables,
                )
            except Exception as error:
                logger.warning("Dashboard chat keyword table lookup fell back to names only: %s", error)
                execution_context["warnings"].append(str(error))

        matches: list[dict[str, Any]] = []
        seen_tables: set[str] = set()

        for table_name in direct_match_tables:
            column_names = [
                str(column.get("name") or "")
                for column in getattr(schema_cache.get(table_name), "columns", [])
            ]
            matches.append({"table": table_name, "columns": column_names[:40]})
            seen_tables.add(table_name)
            if len(matches) >= limit:
                break

        for table_name, snippet in schema_cache.items():
            if table_name in seen_tables:
                continue
            column_names = [str(column.get("name") or "") for column in snippet.columns]
            if not any(keyword in column_name.lower() for column_name in column_names):
                continue
            matches.append({"table": table_name, "columns": column_names[:40]})
            if len(matches) >= limit:
                break

        if matches:
            return {
                "tables": matches,
                "hint": (
                    f"Found {len(matches)} allowlisted tables. Check schema before assuming table structure."
                ),
            }
        return {
            "tables": [],
            "hint": (
                f"No allowlisted tables matched '{keyword}'. Try a broader keyword or retrieve chart docs first."
            ),
        }

    def _tool_check_table_row_count(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Count rows in one allowlisted table."""
        table_name = str(args.get("table") or "").lower()
        if not state["allowlist"].is_allowed(table_name):
            return {
                "error": "table_not_allowed",
                "table": table_name,
                "message": (
                    f"Table {table_name} is not accessible in the current dashboard context."
                ),
            }

        sql = f"SELECT COUNT(*) AS row_count FROM {table_name} LIMIT 1"
        validation = DashboardChatSqlGuard(
            allowlist=state["allowlist"],
            max_rows=1,
        ).validate(sql)
        if not validation.is_valid or not validation.sanitized_sql:
            return {"error": "sql_validation_failed", "issues": validation.errors}

        rows = self._warehouse_tools(execution_context, state["org"]).execute_sql(
            validation.sanitized_sql
        )
        row_count = 0
        if rows:
            row_count = int(rows[0].get("row_count") or 0)
        return {"table": table_name, "row_count": row_count, "has_data": row_count > 0}

    def _run_sql_with_distinct_guard(
        self,
        args: dict[str, Any],
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate SQL like the prototype and let the tool loop self-correct on failures."""
        sql = str(args.get("sql") or "").strip()
        if not sql:
            return {"error": "sql_missing", "message": "SQL is required"}

        allowlist_validation = self._validate_sql_allowlist(sql, state["allowlist"])
        if not allowlist_validation["valid"]:
            return {
                "error": "table_not_allowed",
                "invalid_tables": allowlist_validation["invalid_tables"],
                "message": allowlist_validation["message"],
            }

        follow_up_dimension_validation = self._validate_follow_up_dimension_usage(
            sql=sql,
            state=state,
            execution_context=execution_context,
        )
        if follow_up_dimension_validation is not None:
            return follow_up_dimension_validation
        missing_distinct = self._missing_distinct(sql, state, execution_context)
        if missing_distinct:
            return {
                "error": "must_fetch_distinct_values",
                "missing": missing_distinct,
                "message": (
                    "Call get_distinct_values for these columns, then regenerate the SQL using one of the returned values."
                ),
            }

        validation = DashboardChatSqlGuard(
            allowlist=state["allowlist"],
            max_rows=self.runtime_config.max_query_rows,
        ).validate(sql)
        execution_context["last_sql_validation"] = validation
        if not validation.is_valid or not validation.sanitized_sql:
            return {
                "error": "sql_validation_failed",
                "issues": validation.errors,
                "warnings": validation.warnings,
            }

        missing_columns = self._missing_columns_in_primary_table(
            sql=validation.sanitized_sql,
            state=state,
            execution_context=execution_context,
        )
        if missing_columns is not None:
            return missing_columns

        execution_context["last_sql"] = validation.sanitized_sql
        try:
            rows = self._warehouse_tools(execution_context, state["org"]).execute_sql(
                validation.sanitized_sql
            )
        except Exception as error:
            structured_error = self._structured_sql_execution_error(
                sql=validation.sanitized_sql,
                error=error,
                state=state,
                execution_context=execution_context,
            )
            if structured_error is not None:
                return structured_error
            return {
                "success": False,
                "error": str(error),
                "sql_used": validation.sanitized_sql,
            }

        serialized_rows = json.loads(json.dumps(rows, cls=DjangoJSONEncoder))
        execution_context["last_sql_results"] = serialized_rows
        self._record_validated_filters_from_sql(
            state=state,
            execution_context=execution_context,
            sql=validation.sanitized_sql,
        )
        return {
            "success": True,
            "row_count": len(serialized_rows),
            "data_preview": self._preview_sql_rows(serialized_rows),
            "error": None,
            "sql_used": validation.sanitized_sql,
            "columns": list(serialized_rows[0].keys()) if serialized_rows else [],
            "rows": serialized_rows,
        }

    def _missing_columns_in_primary_table(
        self,
        *,
        sql: str,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Return a corrective tool error when SQL references columns absent from the referenced query tables."""
        table_references = self._table_references(sql)
        referenced_tables = [
            reference["table_name"]
            for reference in table_references
            if reference.get("table_name")
        ]
        if not referenced_tables:
            return None

        schema_cache = self._schema_cache(state, execution_context, tables=referenced_tables)
        all_schema_cache = self._schema_cache(state, execution_context)
        missing_columns_by_table: dict[str, set[str]] = {}
        candidate_tables_by_column: dict[str, list[str]] = {}
        tables_in_query = list(dict.fromkeys(referenced_tables))

        for qualifier, column_name in self._referenced_sql_identifier_refs(sql):
            resolved_table = self._resolve_identifier_table(
                qualifier=qualifier,
                column_name=column_name,
                table_references=table_references,
                schema_cache=schema_cache,
            )
            if resolved_table is not None:
                continue

            if qualifier is not None:
                target_table = (
                    self._resolve_table_qualifier(qualifier, table_references)
                    or self._primary_table_name(sql)
                    or tables_in_query[0]
                )
            else:
                matching_tables = self._tables_with_column(
                    column_name,
                    tables_in_query,
                    schema_cache,
                )
                if len(matching_tables) > 1:
                    continue
                target_table = self._primary_table_name(sql) or tables_in_query[0]

            missing_columns_by_table.setdefault(target_table, set()).add(column_name)
            candidate_tables_by_column[column_name] = self._find_tables_with_column(
                column_name,
                all_schema_cache,
            )

        missing_columns = sorted(
            {
                column_name
                for columns in missing_columns_by_table.values()
                for column_name in columns
            }
        )
        if not missing_columns:
            return None

        primary_table = self._primary_table_name(sql) or tables_in_query[0]
        target_table = (
            next(iter(missing_columns_by_table))
            if len(missing_columns_by_table) == 1
            else primary_table
        )
        best_table = self._best_table_for_missing_columns(
            missing_columns,
            all_schema_cache,
        )
        message = (
            f"Column(s) {', '.join(missing_columns)} do not exist on {target_table}. "
            "Use a table that contains the requested dimension or measure, and rewrite the SQL using columns from that table."
        )
        if best_table:
            message += f" Best candidate table: {best_table}."
        result = {
            "error": "column_not_in_table",
            "table": target_table,
            "missing_columns": missing_columns,
            "candidate_tables": candidate_tables_by_column,
            "best_table": best_table,
            "message": message,
        }
        if len(missing_columns) == 1:
            column_name = missing_columns[0]
            result["column"] = column_name
            result["candidates"] = candidate_tables_by_column.get(column_name, [])
        return result

    def _structured_sql_execution_error(
        self,
        *,
        sql: str,
        error: Exception,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Convert warehouse execution errors into prototype-style corrective feedback when possible."""
        error_text = str(error)
        missing_column_match = re.search(
            r'column "(?:[\w]+\.)?([^"]+)" does not exist',
            error_text,
            flags=re.IGNORECASE,
        )
        if missing_column_match:
            missing_column = missing_column_match.group(1).lower()
            schema_cache = self._schema_cache(state, execution_context)
            candidate_tables = self._find_tables_with_column(missing_column, schema_cache)
            return {
                "error": "column_not_in_table",
                "table": self._primary_table_name(sql),
                "column": missing_column,
                "missing_columns": [missing_column],
                "candidates": candidate_tables,
                "candidate_tables": {missing_column: candidate_tables},
                "best_table": candidate_tables[0] if candidate_tables else None,
                "message": (
                    f"Column {missing_column} is not available on the current table. "
                    "Pick a table that contains it, inspect that schema, and rewrite the SQL using that table's real columns."
                ),
                "sql_used": sql,
            }
        return None

    def _validate_follow_up_dimension_usage(
        self,
        *,
        sql: str,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Keep add-dimension follow-ups from succeeding without actually changing query granularity."""
        intent_decision = state["intent_decision"]
        if intent_decision.intent != DashboardChatIntent.FOLLOW_UP_SQL:
            return None
        if intent_decision.follow_up_context.follow_up_type != "add_dimension":
            return None

        requested_dimension = self._extract_requested_follow_up_dimension(
            intent_decision.follow_up_context.modification_instruction or state["user_query"]
        )
        if not requested_dimension:
            return None

        previous_sql = state["conversation_context"].last_sql_query or ""
        current_dimensions = self._structural_dimensions_from_sql(sql)
        previous_dimensions = self._structural_dimensions_from_sql(previous_sql)
        normalized_requested_dimension = self._normalize_dimension_name(requested_dimension)
        if (
            normalized_requested_dimension in current_dimensions
            and normalized_requested_dimension not in previous_dimensions
        ):
            return None

        candidate_tables = self._find_tables_with_column(
            requested_dimension,
            self._schema_cache(state, execution_context),
        )
        return {
            "error": "requested_dimension_missing",
            "requested_dimension": requested_dimension,
            "previous_dimensions": sorted(previous_dimensions),
            "current_dimensions": sorted(current_dimensions),
            "candidate_tables": candidate_tables,
            "message": (
                f"The follow-up asked to split by '{requested_dimension}', but the SQL does not use that column. "
                "Use the requested dimension exactly, or pick a table that contains it."
            ),
        }

    def _node_finalize_response(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Attach warehouse citations and metadata to the finished response."""
        response = state["response"]
        citations = list(response.citations)
        sql_validation = state.get("sql_validation")
        if (
            sql_validation is not None
            and sql_validation.is_valid
            and sql_validation.sanitized_sql is not None
        ):
            citations.extend(
                DashboardChatCitation(
                    source_type="warehouse_table",
                    source_identifier=table_name,
                    title=f"Warehouse table: {table_name}",
                    snippet=f"SQL executed against {table_name}.",
                    table_name=table_name,
                )
                for table_name in sql_validation.tables
                if table_name
            )

        allowlist = state.get("allowlist") or DashboardChatAllowlist()
        state["response"] = DashboardChatResponse(
            answer_text=response.answer_text,
            intent=response.intent,
            citations=list(dict.fromkeys(citations)),
            warnings=response.warnings,
            sql=response.sql,
            sql_results=response.sql_results,
            usage=response.usage,
            tool_calls=response.tool_calls,
            metadata={
                "dashboard_id": state["dashboard_id"],
                "retrieved_document_ids": [
                    document.document_id for document in state.get("retrieved_documents") or []
                ],
                "allowlisted_tables": sorted(allowlist.allowed_tables),
                "sql_guard_errors": sql_validation.errors if sql_validation is not None else [],
                "intent_reason": state["intent_decision"].reason,
                "missing_info": state["intent_decision"].missing_info,
                "follow_up_type": state["intent_decision"].follow_up_context.follow_up_type,
            },
        )
        return state

    def _build_execution_result(
        self,
        *,
        answer_text: str,
        execution_context: dict[str, Any],
        max_turns_reached: bool,
    ) -> dict[str, Any]:
        """Normalize tool-loop state into one runtime response payload."""
        if max_turns_reached:
            execution_context["tool_calls"].append({"name": "max_turns_reached"})
        warnings = list(dict.fromkeys(execution_context["warnings"]))
        return {
            "answer_text": answer_text.strip(),
            "retrieved_documents": execution_context["retrieved_documents"],
            "tool_calls": execution_context["tool_calls"],
            "sql": execution_context["last_sql"],
            "sql_validation": execution_context["last_sql_validation"],
            "sql_results": execution_context["last_sql_results"],
            "warnings": warnings,
        }

    def _warehouse_tools(
        self,
        execution_context: dict[str, Any],
        org: Org,
    ) -> DashboardChatWarehouseTools:
        """Build the warehouse tool helper lazily for the turn."""
        warehouse_tools = execution_context.get("warehouse_tools")
        if warehouse_tools is None:
            warehouse_tools = self.warehouse_tools_factory(org)
            execution_context["warehouse_tools"] = warehouse_tools
        return warehouse_tools

    def _schema_cache(
        self,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
        tables: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Load and cache schema snippets for allowlisted tables."""
        requested_tables = [
            table_name.lower()
            for table_name in (
                tables if tables is not None else state["allowlist"].prioritized_tables()
            )
            if state["allowlist"].is_allowed(table_name)
        ]
        cache = execution_context["schema_cache"]
        missing_tables = [table_name for table_name in requested_tables if table_name not in cache]
        if missing_tables:
            snippets = self._warehouse_tools(execution_context, state["org"]).get_schema_snippets(
                missing_tables
            )
            for table_name, snippet in snippets.items():
                cache[table_name.lower()] = snippet
            if snippets:
                self._persist_session_schema_cache(state, cache)
        if tables is None:
            return cache
        return {
            table_name: cache[table_name]
            for table_name in requested_tables
            if table_name in cache
        }

    def _seed_distinct_cache_from_previous_sql(
        self,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> None:
        """Treat text filters from the previous successful SQL as already validated for follow-ups."""
        previous_sql = state["conversation_context"].last_sql_query
        if not previous_sql:
            return

        self._record_validated_filters_from_sql(
            state=state,
            execution_context=execution_context,
            sql=previous_sql,
        )

    @staticmethod
    def _dbt_resources_by_unique_id(
        state: DashboardChatRuntimeState,
    ) -> dict[str, dict[str, Any]]:
        """Return the allowlisted dbt index built at session start."""
        dbt_index = state.get("dbt_index") or {}
        return dict(dbt_index.get("resources_by_unique_id") or {})

    def _embed_query(
        self,
        query_text: str,
        embedding_cache: dict[str, list[float]],
    ) -> list[float]:
        """Cache embeddings per query string during one turn."""
        if query_text not in embedding_cache:
            embedding_cache[query_text] = self.vector_store.embed_query(query_text)
        return embedding_cache[query_text]

    @staticmethod
    def _route_after_intent(state: DashboardChatRuntimeState) -> str:
        """Route to one explicit handler per prototype intent."""
        return state["intent_decision"].intent.value

    @staticmethod
    def _normalize_conversation_history(
        conversation_history: Sequence[DashboardChatConversationMessage | dict[str, Any]] | None,
    ) -> list[DashboardChatConversationMessage]:
        """Normalize stored history into the typed runtime message format."""
        normalized_messages: list[DashboardChatConversationMessage] = []
        for item in conversation_history or []:
            if isinstance(item, DashboardChatConversationMessage):
                normalized_messages.append(item)
                continue
            normalized_messages.append(
                DashboardChatConversationMessage(
                    role=str(item.get("role") or "user"),
                    content=str(item.get("content") or ""),
                    payload=item.get("payload") or {},
                )
            )
        return normalized_messages

    @classmethod
    def _extract_conversation_context(
        cls,
        conversation_history: Sequence[DashboardChatConversationMessage],
    ) -> DashboardChatConversationContext:
        """Extract reusable conversation context like the prototype conversation manager."""
        context = DashboardChatConversationContext()
        recent_history = list(conversation_history)[-10:]

        for message in reversed(recent_history):
            if message.role != "assistant":
                continue

            payload = message.payload or {}
            sql = payload.get("sql")
            metadata = payload.get("metadata") or {}
            citations = payload.get("citations") or []
            chart_ids = cls._extract_chart_ids_from_payload(payload)

            if chart_ids and context.last_sql_query and not context.last_chart_ids:
                context = DashboardChatConversationContext(
                    last_sql_query=context.last_sql_query,
                    last_tables_used=context.last_tables_used,
                    last_chart_ids=chart_ids,
                    last_metrics=context.last_metrics,
                    last_dimensions=context.last_dimensions,
                    last_filters=context.last_filters,
                    last_response_type=context.last_response_type,
                    last_answer_text=context.last_answer_text,
                    last_intent=context.last_intent,
                )
                break

            if sql and not context.last_sql_query:
                tables = [
                    str(table_name).lower()
                    for table_name in metadata.get("query_plan_tables") or []
                    if table_name
                ]
                if not tables:
                    tables = [
                        str(citation.get("table_name")).lower()
                        for citation in citations
                        if citation.get("table_name")
                    ]
                if not tables:
                    tables = DashboardChatSqlGuard._extract_table_names(str(sql))
                context = DashboardChatConversationContext(
                    last_sql_query=str(sql),
                    last_tables_used=list(dict.fromkeys(tables)),
                    last_chart_ids=chart_ids,
                    last_metrics=cls._extract_metrics_from_sql(str(sql)),
                    last_dimensions=cls._extract_dimensions_from_sql(str(sql)),
                    last_filters=cls._extract_filters_from_sql(str(sql)),
                    last_response_type="sql_result",
                    last_answer_text=message.content,
                    last_intent=str(payload.get("intent") or ""),
                )
                if chart_ids:
                    break
                continue

            if payload and context.last_response_type is None:
                context = DashboardChatConversationContext(
                    last_chart_ids=chart_ids,
                    last_response_type="metadata_answer",
                    last_answer_text=message.content,
                    last_intent=str(payload.get("intent") or ""),
                )

        return context

    @staticmethod
    def _extract_chart_ids_from_payload(payload: dict[str, Any]) -> list[str]:
        """Extract chart ids from persisted metadata/citations like the prototype chat history."""
        metadata = payload.get("metadata") or {}
        chart_ids = [str(chart_id) for chart_id in metadata.get("chart_ids_used") or [] if chart_id]
        if chart_ids:
            return list(dict.fromkeys(chart_ids))

        extracted_chart_ids: list[str] = []
        for citation in payload.get("citations") or []:
            source_identifier = str(citation.get("source_identifier") or "")
            chart_id = DashboardChatRuntime._chart_id_from_source_identifier(source_identifier)
            if chart_id is not None:
                extracted_chart_ids.append(str(chart_id))
        return list(dict.fromkeys(extracted_chart_ids))

    @classmethod
    def _build_follow_up_context_prompt(
        cls,
        conversation_context: DashboardChatConversationContext,
        user_query: str,
    ) -> str:
        """Build the prototype follow-up context prompt."""
        return "\n".join(
            [
                "PREVIOUS QUERY CONTEXT:",
                f"Last SQL: {conversation_context.last_sql_query or 'None'}",
                f"Tables used: {', '.join(conversation_context.last_tables_used) or 'None'}",
                f"Metrics: {', '.join(conversation_context.last_metrics) or 'None'}",
                f"Dimensions: {', '.join(conversation_context.last_dimensions) or 'None'}",
                f"Filters: {', '.join(conversation_context.last_filters) or 'None'}",
                "",
                f"NEW INSTRUCTION: {user_query}",
                "",
                "TASK: Modify the previous query based on the new instruction. Reuse tables and context where possible.",
            ]
        )

    @staticmethod
    def _detect_sql_modification_type(user_query: str) -> str:
        """Detect the same coarse follow-up modification categories as the prototype."""
        lowered_query = user_query.lower()
        if any(keyword in lowered_query for keyword in ["by", "split by", "break down", "group by"]):
            return "add_dimension"
        if any(keyword in lowered_query for keyword in ["filter", "only", "exclude", "where"]):
            return "add_filter"
        if any(
            keyword in lowered_query
            for keyword in ["last", "this", "previous", "next", "monthly", "weekly", "quarterly"]
        ):
            return "modify_timeframe"
        if any(
            keyword in lowered_query
            for keyword in ["total", "sum", "count", "average", "avg", "maximum", "minimum"]
        ):
            return "change_aggregation"
        return "general_modification"

    @staticmethod
    def _extract_requested_follow_up_dimension(text: str) -> str | None:
        """Extract the requested follow-up dimension and normalize natural-language spaces."""
        normalized_text = text.strip().lower()
        patterns = [
            r"split\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
            r"break\s+down\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
            r"group\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
            r"\bby\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized_text)
            if not match:
                continue
            candidate = re.split(
                r"\b(with|for|in|across|between)\b",
                match.group(1),
                maxsplit=1,
            )[0]
            candidate = re.sub(r"[^a-zA-Z0-9_\s]", " ", candidate)
            normalized_candidate = "_".join(part for part in candidate.split() if part)
            if normalized_candidate:
                return normalized_candidate
        return None

    @staticmethod
    def _extract_metrics_from_sql(sql: str) -> list[str]:
        """Extract aggregate expressions from the previous SQL for follow-up prompts."""
        select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
        if not select_clause:
            return []
        metrics: list[str] = []
        for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
            normalized_expression = expression.strip()
            if normalized_expression and DashboardChatSqlGuard._contains_aggregate(
                normalized_expression
            ):
                metrics.append(normalized_expression)
        return metrics[:5]

    @staticmethod
    def _extract_dimensions_from_sql(sql: str) -> list[str]:
        """Extract GROUP BY dimensions from the previous SQL."""
        match = re.search(
            r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return []
        return [
            dimension.strip().strip('`"')
            for dimension in match.group(1).split(",")
            if dimension.strip()
        ][:5]

    @staticmethod
    def _extract_filters_from_sql(sql: str) -> list[str]:
        """Extract WHERE-clause filters from the previous SQL."""
        match = re.search(
            r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return []

        where_clause = match.group(1).strip()
        filters: list[str] = []
        for pattern in [
            r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']+)'",
            r"([a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s*\([^)]+\)",
        ]:
            for filter_match in re.findall(pattern, where_clause, flags=re.IGNORECASE):
                if isinstance(filter_match, tuple) and len(filter_match) == 2:
                    filters.append(f"{filter_match[0]} = {filter_match[1]}")
                else:
                    filters.append(str(filter_match))
        return filters[:5]

    def _query_vector_store(
        self,
        *,
        org: Org,
        collection_name: str | None,
        query_text: str,
        source_types: Sequence[str],
        dashboard_id: int | None = None,
        query_embedding: list[float] | None = None,
    ) -> list[DashboardChatRetrievedDocument]:
        """Query Chroma and normalize the results."""
        if not source_types:
            return []

        results = self.vector_store.query(
            org.id,
            query_text=query_text,
            n_results=self.runtime_config.retrieval_limit,
            source_types=list(source_types),
            dashboard_id=dashboard_id,
            query_embedding=query_embedding,
            collection_name=collection_name,
        )
        return [
            DashboardChatRetrievedDocument(
                document_id=result.document_id,
                source_type=str(result.metadata.get("source_type") or ""),
                source_identifier=str(result.metadata.get("source_identifier") or ""),
                content=result.content,
                dashboard_id=result.metadata.get("dashboard_id"),
                distance=result.distance,
            )
            for result in results
        ]

    @staticmethod
    def _filter_allowlisted_dbt_results(
        results: Sequence[DashboardChatRetrievedDocument],
        allowlist: DashboardChatAllowlist,
    ) -> list[DashboardChatRetrievedDocument]:
        """Keep only dbt docs that belong to the dashboard lineage."""
        filtered_results: list[DashboardChatRetrievedDocument] = []
        for result in results:
            unique_id = DashboardChatRuntime._unique_id_from_source_identifier(
                result.source_identifier
            )
            if allowlist.is_unique_id_allowed(unique_id):
                filtered_results.append(result)
        return filtered_results

    @staticmethod
    def _dedupe_retrieved_documents(
        results: Sequence[DashboardChatRetrievedDocument],
    ) -> list[DashboardChatRetrievedDocument]:
        """Deduplicate retrieved documents while preserving better-ranked items."""
        scored_results: list[tuple[float, DashboardChatRetrievedDocument]] = []
        for result in results:
            scored_results.append((result.distance if result.distance is not None else 999.0, result))

        merged_results: list[DashboardChatRetrievedDocument] = []
        seen_document_ids: set[str] = set()
        for _, result in sorted(scored_results, key=lambda item: item[0]):
            if result.document_id in seen_document_ids:
                continue
            merged_results.append(result)
            seen_document_ids.add(result.document_id)
        return merged_results

    def _build_citations(
        self,
        *,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        dashboard_export: dict[str, Any],
        allowlist: DashboardChatAllowlist,
    ) -> list[DashboardChatCitation]:
        """Build citations from the retrieved tool-loop documents."""
        dashboard_title = dashboard_export["dashboard"].get("title") or "Current dashboard"
        chart_lookup = {
            chart.get("id"): chart.get("title") or f"Chart {chart.get('id')}"
            for chart in dashboard_export.get("charts") or []
        }
        citations: list[DashboardChatCitation] = []
        for document in retrieved_documents[:6]:
            chart_id = self._chart_id_from_source_identifier(document.source_identifier)
            table_name = None
            if document.source_type in {
                DashboardChatSourceType.DBT_MANIFEST.value,
                DashboardChatSourceType.DBT_CATALOG.value,
            }:
                unique_id = self._unique_id_from_source_identifier(document.source_identifier)
                table_name = allowlist.unique_id_to_table.get(unique_id) if unique_id else None
            citations.append(
                DashboardChatCitation(
                    source_type=document.source_type,
                    source_identifier=document.source_identifier,
                    title=self._citation_title(
                        document=document,
                        dashboard_title=dashboard_title,
                        chart_lookup=chart_lookup,
                        table_name=table_name,
                    ),
                    snippet=self._compact_snippet(document.content),
                    dashboard_id=document.dashboard_id,
                    table_name=table_name,
                )
            )
        return citations

    @staticmethod
    def _citation_title(
        *,
        document: DashboardChatRetrievedDocument,
        dashboard_title: str,
        chart_lookup: dict[int, str],
        table_name: str | None,
    ) -> str:
        """Map a retrieved document into a human-readable citation title."""
        if document.source_type == DashboardChatSourceType.ORG_CONTEXT.value:
            return "Organization context"
        if document.source_type == DashboardChatSourceType.DASHBOARD_CONTEXT.value:
            return f"Dashboard context: {dashboard_title}"
        if document.source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
            chart_id = DashboardChatRuntime._chart_id_from_source_identifier(
                document.source_identifier
            )
            if chart_id is not None and chart_id in chart_lookup:
                return f"Chart: {chart_lookup[chart_id]}"
            return f"Dashboard export: {dashboard_title}"
        if document.source_type == DashboardChatSourceType.DBT_MANIFEST.value:
            return f"dbt manifest: {table_name or document.source_identifier}"
        if document.source_type == DashboardChatSourceType.DBT_CATALOG.value:
            return f"dbt catalog: {table_name or document.source_identifier}"
        return document.source_identifier

    def _load_session_snapshot(self, state: DashboardChatRuntimeState) -> dict[str, Any]:
        """Return the current session's frozen dashboard context snapshot."""
        session_id = state.get("session_id")
        if not session_id:
            return self._build_session_snapshot(state)

        cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
        cached_snapshot = cache.get(cache_key)
        if cached_snapshot is not None:
            cached_dbt_index = cached_snapshot.get("dbt_index")
            if cached_dbt_index is None and cached_snapshot.get("manifest_json") is not None:
                cached_dbt_index = DashboardChatAllowlistBuilder.build_dbt_index(
                    cached_snapshot.get("manifest_json"),
                    deserialize_allowlist(cached_snapshot.get("allowlist")),
                )
            return {
                "dashboard_export": dict(cached_snapshot["dashboard_export"]),
                "dbt_index": cached_dbt_index or {"resources_by_unique_id": {}},
                "allowlist": deserialize_allowlist(cached_snapshot.get("allowlist")),
                "schema_cache": deserialize_schema_snippets(cached_snapshot.get("schema_cache")),
                "distinct_cache": deserialize_distinct_cache(
                    cached_snapshot.get("distinct_cache")
                ),
            }

        snapshot = self._build_session_snapshot(state)
        cache.set(
            cache_key,
            {
                "dashboard_export": snapshot["dashboard_export"],
                "dbt_index": snapshot["dbt_index"],
                "allowlist": serialize_allowlist(snapshot["allowlist"]),
                "schema_cache": serialize_schema_snippets(snapshot["schema_cache"]),
                "distinct_cache": serialize_distinct_cache(snapshot["distinct_cache"]),
            },
            DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS,
        )
        return snapshot

    def _build_session_snapshot(self, state: DashboardChatRuntimeState) -> dict[str, Any]:
        """Build one session-stable snapshot of dashboard-specific runtime context."""
        dashboard_export = DashboardService.export_dashboard_context(
            state["dashboard_id"],
            state["org"],
        )
        manifest_json = DashboardChatAllowlistBuilder.load_manifest_json(state["org"].dbt)
        allowlist = DashboardChatAllowlistBuilder.build(
            dashboard_export,
            manifest_json=manifest_json,
        )
        return {
            "dashboard_export": dashboard_export,
            "dbt_index": DashboardChatAllowlistBuilder.build_dbt_index(
                manifest_json,
                allowlist,
            ),
            "allowlist": allowlist,
            "schema_cache": {},
            "distinct_cache": set(),
        }

    def _persist_session_schema_cache(
        self,
        state: DashboardChatRuntimeState,
        schema_cache: dict[str, DashboardChatSchemaSnippet],
    ) -> None:
        """Persist lazily loaded schema snippets back into the session snapshot cache."""
        session_id = state.get("session_id")
        if not session_id:
            state["session_schema_cache"] = dict(schema_cache)
            return

        cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
        cached_snapshot = cache.get(cache_key)
        if cached_snapshot is None:
            return
        cached_snapshot["schema_cache"] = serialize_schema_snippets(schema_cache)
        cache.set(cache_key, cached_snapshot, DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS)
        state["session_schema_cache"] = dict(schema_cache)

    def _persist_session_distinct_cache(
        self,
        state: DashboardChatRuntimeState,
        distinct_cache: set[tuple[str, str, str]],
    ) -> None:
        """Persist validated distinct values back into the session snapshot cache."""
        session_id = state.get("session_id")
        if not session_id:
            state["session_distinct_cache"] = set(distinct_cache)
            return

        cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
        cached_snapshot = cache.get(cache_key)
        if cached_snapshot is None:
            return
        cached_snapshot["distinct_cache"] = serialize_distinct_cache(distinct_cache)
        cache.set(cache_key, cached_snapshot, DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS)
        state["session_distinct_cache"] = set(distinct_cache)

    @staticmethod
    def _compact_snippet(content: str, max_length: int = 220) -> str:
        """Collapse whitespace and trim long snippets for citations and suggestions."""
        normalized = " ".join(content.split())
        if len(normalized) <= max_length:
            return normalized
        return normalized[: max_length - 3].rstrip() + "..."

    def _tool_document_payload(
        self,
        document: DashboardChatRetrievedDocument,
        allowlist: DashboardChatAllowlist,
        dashboard_export: dict[str, Any],
    ) -> dict[str, Any]:
        """Convert a runtime retrieval result into the prototype tool payload shape."""
        metadata: dict[str, Any] = {
            "type": self._prototype_doc_type(document.source_type),
            "source_type": document.source_type,
            "source_identifier": document.source_identifier,
        }
        chart_id = self._chart_id_from_source_identifier(document.source_identifier)
        if chart_id is not None:
            metadata["chart_id"] = chart_id
            metadata["dashboard_id"] = document.dashboard_id
            chart_metadata = self._chart_tool_metadata(chart_id, dashboard_export)
            if chart_metadata:
                metadata.update(chart_metadata)
        unique_id = self._unique_id_from_source_identifier(document.source_identifier)
        if unique_id:
            metadata["dbt_unique_id"] = unique_id
            metadata["table_name"] = allowlist.unique_id_to_table.get(unique_id)
        return {
            "doc_id": document.document_id,
            "content": document.content,
            "metadata": metadata,
            "similarity_score": document.distance,
        }

    @classmethod
    def _chart_tool_metadata(
        cls,
        chart_id: int,
        dashboard_export: dict[str, Any],
    ) -> dict[str, Any]:
        """Return structured chart metadata that nudges the tool loop toward exact chart fields."""
        chart = next(
            (
                candidate
                for candidate in (dashboard_export.get("charts") or [])
                if candidate.get("id") == chart_id
            ),
            None,
        )
        if chart is None:
            return {}

        preferred_table = build_dashboard_chat_table_name(
            chart.get("schema_name"),
            chart.get("table_name"),
        )
        metric_columns = cls._chart_metric_columns(chart)
        dimension_columns = cls._chart_dimension_columns(chart)
        time_column = cls._chart_time_column(chart, dimension_columns)
        payload: dict[str, Any] = {
            "chart_title": str(chart.get("title") or ""),
            "chart_type": str(chart.get("chart_type") or ""),
        }
        if preferred_table:
            payload["preferred_table"] = preferred_table
        if metric_columns:
            payload["metric_columns"] = metric_columns
        if dimension_columns:
            payload["dimension_columns"] = dimension_columns
        if time_column:
            payload["time_column"] = time_column
        return payload

    @staticmethod
    def _prototype_doc_type(source_type: str) -> str:
        """Map Dalgo source types into the prototype doc-type vocabulary."""
        if source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
            return "chart"
        if source_type in {
            DashboardChatSourceType.DBT_MANIFEST.value,
            DashboardChatSourceType.DBT_CATALOG.value,
        }:
            return "dbt_model"
        return "context"

    def _validate_sql_allowlist(
        self,
        sql: str,
        allowlist: DashboardChatAllowlist,
    ) -> dict[str, Any]:
        """Validate that all referenced tables are in the dashboard allowlist."""
        referenced_tables = DashboardChatSqlGuard._extract_table_names(sql)
        invalid_tables = [
            table_name for table_name in referenced_tables if not allowlist.is_allowed(table_name)
        ]
        if invalid_tables:
            return {
                "valid": False,
                "invalid_tables": invalid_tables,
                "message": (
                    "SQL references tables not available in the current dashboard: "
                    + ", ".join(invalid_tables)
                    + ". Use list_tables_by_keyword to find allowed tables."
                ),
            }
        return {"valid": True, "invalid_tables": [], "message": ""}

    @staticmethod
    def _primary_table_name(sql: str) -> str | None:
        """Return the primary FROM table for single-query correction logic."""
        table_match = re.search(r"\bFROM\s+([`\"]?)([\w\.]+)\1", sql, re.IGNORECASE)
        if not table_match:
            return None
        return normalize_dashboard_chat_table_name(table_match.group(2))

    @classmethod
    def _table_references(cls, sql: str) -> list[dict[str, str | None]]:
        """Return normalized FROM/JOIN table references and aliases from one SQL statement."""
        references: list[dict[str, str | None]] = []
        for match in re.finditer(
            r"\b(?:FROM|JOIN)\s+([`\"]?)([\w\.]+)\1(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
            sql,
            flags=re.IGNORECASE,
        ):
            table_name = normalize_dashboard_chat_table_name(match.group(2))
            if not table_name:
                continue
            alias = str(match.group(3) or "").lower() or None
            references.append(
                {
                    "table_name": table_name,
                    "alias": alias,
                    "short_name": table_name.split(".")[-1],
                }
            )
        return references

    @classmethod
    def _resolve_table_qualifier(
        cls,
        qualifier: str,
        table_references: Sequence[dict[str, str | None]],
    ) -> str | None:
        """Resolve a qualifier like `f` or `analytics_table` to one query table."""
        normalized_qualifier = qualifier.lower().strip().strip('`"')
        matches = [
            str(reference["table_name"])
            for reference in table_references
            if normalized_qualifier
            in {
                str(reference.get("alias") or ""),
                str(reference.get("short_name") or ""),
                str(reference.get("table_name") or ""),
            }
        ]
        deduped_matches = list(dict.fromkeys(match for match in matches if match))
        if len(deduped_matches) == 1:
            return deduped_matches[0]
        return None

    @staticmethod
    def _table_columns(snippet: DashboardChatSchemaSnippet | Any) -> set[str]:
        """Return the normalized column names available on one schema snippet."""
        return {
            str(column.get("name") or "").lower()
            for column in getattr(snippet, "columns", []) or []
        }

    @classmethod
    def _tables_with_column(
        cls,
        column_name: str,
        table_names: Sequence[str],
        schema_cache: dict[str, Any],
    ) -> list[str]:
        """Return the query tables that contain one column."""
        normalized_column_name = column_name.lower()
        return [
            table_name
            for table_name in table_names
            if normalized_column_name in cls._table_columns(schema_cache.get(table_name))
        ]

    @classmethod
    def _resolve_identifier_table(
        cls,
        *,
        qualifier: str | None,
        column_name: str,
        table_references: Sequence[dict[str, str | None]],
        schema_cache: dict[str, Any],
    ) -> str | None:
        """Resolve one referenced column to a concrete query table when it is unambiguous."""
        if qualifier is not None:
            resolved_table = cls._resolve_table_qualifier(qualifier, table_references)
            if not resolved_table:
                return None
            if column_name.lower() in cls._table_columns(schema_cache.get(resolved_table)):
                return resolved_table
            return None

        query_tables = [str(reference["table_name"]) for reference in table_references if reference.get("table_name")]
        matching_tables = cls._tables_with_column(column_name, query_tables, schema_cache)
        if len(matching_tables) == 1:
            return matching_tables[0]
        return None

    @classmethod
    def _referenced_sql_identifier_refs(cls, sql: str) -> list[tuple[str | None, str]]:
        """Extract likely physical identifier references from the outer SQL."""
        table_aliases = {
            alias.lower()
            for alias in re.findall(
                r"\b(?:FROM|JOIN)\s+[`\"]?[\w\.]+[`\"]?(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
                sql,
                flags=re.IGNORECASE,
            )
            if alias
        }
        select_aliases = cls._select_aliases(sql)
        referenced_identifiers: list[tuple[str | None, str]] = []

        select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
        if select_clause:
            for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
                referenced_identifiers.extend(
                    cls._extract_identifier_refs_from_sql_segment(expression, table_aliases)
                )

        for pattern in [
            r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
            r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)",
            r"\bORDER\s+BY\s+(.+?)(?:\bLIMIT\b|$)",
        ]:
            match = re.search(pattern, sql, flags=re.IGNORECASE | re.DOTALL)
            if match:
                referenced_identifiers.extend(
                    cls._extract_identifier_refs_from_sql_segment(
                        match.group(1),
                        table_aliases,
                        ignored_identifiers=select_aliases,
                    )
                )

        return list(dict.fromkeys(referenced_identifiers))

    @staticmethod
    def _select_aliases(sql: str) -> set[str]:
        """Return aliases introduced by the outer SELECT clause."""
        select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
        if not select_clause:
            return set()

        aliases: set[str] = set()
        for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
            alias_match = re.search(
                r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
                expression,
                flags=re.IGNORECASE,
            )
            if alias_match:
                aliases.add(alias_match.group(1).lower())
        return aliases

    @staticmethod
    def _extract_identifier_refs_from_sql_segment(
        segment: str,
        table_aliases: set[str],
        ignored_identifiers: set[str] | None = None,
    ) -> list[tuple[str | None, str]]:
        """Pull qualified and unqualified column-like identifiers out of one SQL segment."""
        normalized_segment = re.sub(r"'[^']*'", " ", segment)
        normalized_segment = re.sub(
            r"\bAS\s+[A-Za-z_][A-Za-z0-9_]*",
            " ",
            normalized_segment,
            flags=re.IGNORECASE,
        )
        ignored_tokens = {
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP",
            "BY",
            "ORDER",
            "LIMIT",
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "DISTINCT",
            "AND",
            "OR",
            "AS",
            "IN",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "TRUE",
            "FALSE",
            "NULL",
            "NOT",
            "ASC",
            "DESC",
            "ON",
            "JOIN",
        }
        ignored_identifiers = {identifier.lower() for identifier in (ignored_identifiers or set())}
        identifiers: list[tuple[str | None, str]] = []
        for match in re.finditer(
            r"(?:(?P<qualifier>[A-Za-z_][A-Za-z0-9_]*)\.)?(?P<identifier>[A-Za-z_][A-Za-z0-9_]*)",
            normalized_segment,
        ):
            qualifier = match.group("qualifier")
            identifier = match.group("identifier")
            if not identifier:
                continue
            if identifier.upper() in ignored_tokens:
                continue
            if identifier.lower() in table_aliases or identifier.lower() in ignored_identifiers:
                continue
            trailing_segment = normalized_segment[match.end() :].lstrip()
            if qualifier is None and trailing_segment.startswith("("):
                continue
            identifiers.append((qualifier.lower() if qualifier else None, identifier.lower()))
        return identifiers

    @staticmethod
    def _best_table_for_missing_columns(
        missing_columns: Sequence[str],
        schema_cache: dict[str, Any],
    ) -> str | None:
        """Return the first allowlisted table that covers all missing columns."""
        wanted_columns = {column_name.lower() for column_name in missing_columns}
        for table_name, snippet in schema_cache.items():
            available_columns = {
                str(column.get("name") or "").lower() for column in snippet.columns
            }
            if wanted_columns.issubset(available_columns):
                return table_name
        return None

    def _missing_distinct(
        self,
        sql: str,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Detect text filters that require a prior distinct-values call."""
        where_match = re.search(
            r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not where_match:
            return []

        table_references = self._table_references(sql)
        query_tables = [
            reference["table_name"]
            for reference in table_references
            if reference.get("table_name")
        ]
        if not query_tables:
            return []
        primary_table = self._primary_table_name(sql) or query_tables[0]

        full_schema_cache = self._schema_cache(state, execution_context, tables=query_tables)
        all_schema_cache = self._schema_cache(state, execution_context)

        column_types = {
            table_name: {
                str(column.get("name") or "").lower(): str(
                    column.get("data_type") or column.get("type") or ""
                ).lower()
                for column in getattr(snippet, "columns", [])
            }
            for table_name, snippet in full_schema_cache.items()
        }
        missing: list[dict[str, Any]] = []
        for qualifier, column_name, value in self._extract_text_filter_values(where_match.group(1)):
            normalized_column = column_name.lower()
            resolved_table = self._resolve_identifier_table(
                qualifier=qualifier,
                column_name=normalized_column,
                table_references=table_references,
                schema_cache=full_schema_cache,
            )
            if resolved_table is None and qualifier is None:
                matching_tables = self._tables_with_column(
                    normalized_column,
                    query_tables,
                    full_schema_cache,
                )
                if len(matching_tables) > 1:
                    continue
            if resolved_table is None:
                candidate_tables = self._find_tables_with_column(
                    normalized_column,
                    all_schema_cache,
                )
                if qualifier is None and candidate_tables:
                    continue
                missing.append(
                    {
                        "table": primary_table,
                        "column": column_name,
                        "error": "column_not_in_table",
                        "candidates": candidate_tables,
                    }
                )
                continue
            data_type = column_types.get(resolved_table, {}).get(normalized_column, "")
            if not data_type:
                continue
            if not self._is_text_type(data_type):
                continue
            if (
                not self._has_validated_distinct_value(
                    execution_context["distinct_cache"],
                    table_name=resolved_table,
                    column_name=normalized_column,
                    value=value,
                )
            ):
                missing.append(
                    {"table": resolved_table, "column": column_name, "value": value}
                )
        return missing

    @staticmethod
    def _extract_text_filter_values(where_clause: str) -> list[tuple[str | None, str, str]]:
        """Extract quoted text filter values from one WHERE clause."""
        extracted_values: list[tuple[str | None, str, str]] = []
        for qualifier, column_name, value in re.findall(
            r"(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']+)'",
            where_clause,
            flags=re.IGNORECASE,
        ):
            extracted_values.append((qualifier.lower() if qualifier else None, column_name, value))

        for match in re.finditer(
            r"(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s*\(([^)]*)\)",
            where_clause,
            flags=re.IGNORECASE,
        ):
            qualifier = match.group(1)
            column_name = match.group(2)
            for value in re.findall(r"'([^']+)'", match.group(3)):
                extracted_values.append(
                    (qualifier.lower() if qualifier else None, column_name, value)
                )
        return extracted_values

    @staticmethod
    def _normalize_distinct_value(value: Any) -> str:
        """Normalize one distinct value for exact cache lookups."""
        return str(value).strip().lower()

    @classmethod
    def _has_validated_distinct_value(
        cls,
        distinct_cache: set[tuple[Any, ...]],
        *,
        table_name: str,
        column_name: str,
        value: Any,
    ) -> bool:
        """Return whether this exact text filter value was already validated in-session."""
        normalized_value = cls._normalize_distinct_value(value)
        normalized_column = column_name.lower()
        normalized_table = table_name.lower()
        return (
            (normalized_table, normalized_column, normalized_value) in distinct_cache
            or ("*", normalized_column, normalized_value) in distinct_cache
            or (normalized_table, normalized_column) in distinct_cache
            or ("*", normalized_column) in distinct_cache
        )

    @staticmethod
    def _find_tables_with_column(
        column_name: str,
        schema_cache: dict[str, Any],
        limit: int = 10,
    ) -> list[str]:
        """Find allowlisted tables that contain one column."""
        matches: list[str] = []
        normalized_column_name = column_name.lower()
        for table_name, snippet in schema_cache.items():
            if any(
                normalized_column_name == str(column.get("name") or "").lower()
                for column in snippet.columns
            ):
                matches.append(table_name)
            if len(matches) >= limit:
                break
        return matches

    @staticmethod
    def _is_text_type(data_type: str) -> bool:
        """Treat common string-like warehouse types as requiring distinct-value lookup."""
        return any(
            text_token in data_type
            for text_token in ["char", "text", "string", "varchar"]
        )

    @staticmethod
    def _preview_sql_rows(rows: list[dict[str, Any]], max_rows: int = 5) -> str:
        """Render a compact human-readable preview for successful SQL executions."""
        if not rows:
            return "No matching rows found."
        preview_rows = rows[:max_rows]
        preview_lines = [json.dumps(row, cls=DjangoJSONEncoder) for row in preview_rows]
        if len(rows) > max_rows:
            preview_lines.append(f"... {len(rows) - max_rows} more rows")
        return "\n".join(preview_lines)

    def _record_validated_distinct_values(
        self,
        *,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
        table_name: str,
        column_name: str,
        values: Sequence[Any],
    ) -> None:
        """Persist exact validated filter values for the current session."""
        normalized_table = table_name.lower()
        normalized_column = column_name.lower()
        distinct_cache = execution_context["distinct_cache"]
        for value in values:
            normalized_value = self._normalize_distinct_value(value)
            distinct_cache.add((normalized_table, normalized_column, normalized_value))
            # Follow-ups often move to an upstream table with the same validated dimension.
            distinct_cache.add(("*", normalized_column, normalized_value))
        self._persist_session_distinct_cache(state, distinct_cache)

    def _record_validated_filters_from_sql(
        self,
        *,
        state: DashboardChatRuntimeState,
        execution_context: dict[str, Any],
        sql: str,
    ) -> None:
        """Seed exact validated filter values from a successful SQL statement."""
        table_references = self._table_references(sql)
        if not table_references:
            return
        where_match = re.search(
            r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not where_match:
            return

        query_tables = [
            reference["table_name"]
            for reference in table_references
            if reference.get("table_name")
        ]
        schema_cache = dict(execution_context.get("schema_cache") or {})
        values_by_target: dict[tuple[str, str], list[str]] = {}
        for qualifier, column_name, value in self._extract_text_filter_values(where_match.group(1)):
            normalized_column = column_name.lower()
            resolved_table = self._resolve_identifier_table(
                qualifier=qualifier,
                column_name=normalized_column,
                table_references=table_references,
                schema_cache=schema_cache,
            )
            if resolved_table is None and qualifier is None:
                if schema_cache:
                    matching_tables = self._tables_with_column(
                        normalized_column,
                        query_tables,
                        schema_cache,
                    )
                    if len(matching_tables) == 1:
                        resolved_table = matching_tables[0]
                elif len(query_tables) == 1:
                    resolved_table = query_tables[0]
            values_by_target.setdefault((resolved_table or "*", normalized_column), []).append(value)

        if not values_by_target:
            return

        for (table_name, column_name), values in values_by_target.items():
            self._record_validated_distinct_values(
                state=state,
                execution_context=execution_context,
                table_name=table_name,
                column_name=column_name,
                values=values,
            )

    @classmethod
    def _structural_dimensions_from_sql(cls, sql: str) -> set[str]:
        """Return normalized non-aggregate dimensions used by one SQL statement."""
        if not sql:
            return set()

        dimensions: set[str] = set()
        for dimension in cls._extract_dimensions_from_sql(sql):
            identifier_refs = cls._extract_identifier_refs_from_sql_segment(
                dimension,
                table_aliases=set(),
            )
            if identifier_refs:
                dimensions.update(
                    cls._normalize_dimension_name(column_name)
                    for _, column_name in identifier_refs
                )
                continue
            dimensions.add(cls._normalize_dimension_name(dimension))
        select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
        if not select_clause:
            return {dimension for dimension in dimensions if dimension}

        for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
            normalized_expression = expression.strip()
            if not normalized_expression or DashboardChatSqlGuard._contains_aggregate(
                normalized_expression
            ):
                continue
            for _, column_name in cls._extract_identifier_refs_from_sql_segment(
                normalized_expression,
                table_aliases=set(),
                ignored_identifiers=cls._select_aliases(sql),
            ):
                dimensions.add(cls._normalize_dimension_name(column_name))
        return {dimension for dimension in dimensions if dimension}

    @staticmethod
    def _normalize_dimension_name(value: str) -> str:
        """Normalize dimension names from SQL expressions and natural-language follow-ups."""
        normalized_value = value.strip().strip('`"').lower()
        normalized_value = normalized_value.split(".")[-1]
        normalized_value = re.sub(r"[^a-z0-9_]+", "_", normalized_value)
        normalized_value = re.sub(r"_+", "_", normalized_value).strip("_")
        return normalized_value

    @classmethod
    def _chart_metric_columns(cls, chart: dict[str, Any]) -> list[str]:
        """Extract the most likely metric columns from one chart export payload."""
        extra_config = chart.get("extra_config") or {}
        metrics: list[str] = []
        for metric in extra_config.get("metrics") or []:
            if isinstance(metric, str) and metric.strip():
                metrics.append(metric.strip())
                continue
            if isinstance(metric, dict):
                for key in ["column", "name", "field", "metric", "metric_column"]:
                    value = metric.get(key)
                    if isinstance(value, str) and value.strip():
                        metrics.append(value.strip())
                        break
        for key in [
            "metric_col",
            "metric_column",
            "measure_col",
            "measure_column",
            "value_column",
            "y_axis_column",
        ]:
            value = extra_config.get(key)
            if isinstance(value, str) and value.strip():
                metrics.append(value.strip())
        return list(dict.fromkeys(metrics))

    @classmethod
    def _chart_dimension_columns(cls, chart: dict[str, Any]) -> list[str]:
        """Extract dimension-like fields from one chart export payload."""
        extra_config = chart.get("extra_config") or {}
        dimensions: list[str] = []
        for key in ["dimension_col", "extra_dimension", "group_by", "category_column", "x_axis_column"]:
            value = extra_config.get(key)
            if isinstance(value, str) and value.strip():
                dimensions.append(value.strip())
        for value in extra_config.get("dimensions") or []:
            if isinstance(value, str) and value.strip():
                dimensions.append(value.strip())
        return list(dict.fromkeys(dimensions))

    @classmethod
    def _chart_time_column(
        cls,
        chart: dict[str, Any],
        dimension_columns: Sequence[str],
    ) -> str | None:
        """Extract or infer the chart's time dimension when one is present."""
        extra_config = chart.get("extra_config") or {}
        for key in ["time_column", "time_dimension", "date_column"]:
            value = extra_config.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for dimension in dimension_columns:
            if cls._looks_like_time_dimension(dimension):
                return dimension
        return None

    @staticmethod
    def _looks_like_time_dimension(column_name: str) -> bool:
        """Return whether a dimension name probably represents time bucketing."""
        normalized_column = column_name.lower()
        return any(
            token in normalized_column
            for token in ["date", "day", "week", "month", "quarter", "year", "time"]
        )

    @staticmethod
    def _serialize_tool_result(result: dict[str, Any]) -> dict[str, Any]:
        """Trim large tool payloads before feeding them back into the model."""
        serialized = dict(result)
        docs = serialized.get("docs")
        if isinstance(docs, list) and len(docs) > 6:
            serialized["docs"] = docs[:6]
        rows = serialized.get("rows")
        if isinstance(rows, list) and len(rows) > 5:
            serialized["rows"] = rows[:5]
        values = serialized.get("values")
        if isinstance(values, list) and len(values) > 20:
            serialized["values"] = values[:20]
        return serialized

    def _summarize_tool_call(
        self,
        *,
        tool_name: str,
        args: dict[str, Any],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist a compact execution trace for one tool call."""
        entry: dict[str, Any] = {"name": tool_name, "args": args}
        if tool_name == "retrieve_docs":
            entry["count"] = result.get("count", 0)
            entry["doc_ids"] = [doc.get("doc_id") for doc in result.get("docs", [])[:6]]
        elif tool_name == "get_schema_snippets":
            entry["tables"] = [table.get("table") for table in result.get("tables", [])]
        elif tool_name == "search_dbt_models":
            entry["count"] = result.get("count", 0)
            entry["models"] = [model.get("table") or model.get("name") for model in result.get("models", [])]
        elif tool_name == "get_dbt_model_info":
            entry["model"] = result.get("model")
            entry["column_count"] = len(result.get("columns") or [])
        elif tool_name == "get_distinct_values":
            entry["error"] = result.get("error")
            entry["count"] = result.get("count", 0)
            entry["values_sample"] = (result.get("values") or [])[:10]
        elif tool_name == "list_tables_by_keyword":
            entry["tables"] = [table.get("table") for table in result.get("tables", [])]
        elif tool_name == "check_table_row_count":
            entry["row_count"] = result.get("row_count")
        elif tool_name == "run_sql_query":
            entry["success"] = result.get("success", False)
            entry["row_count"] = result.get("row_count", 0)
            entry["sql_used"] = result.get("sql_used")
            entry["error"] = result.get("error")
        else:
            entry["result"] = result
        return entry

    def _max_turns_message(
        self,
        user_query: str,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
    ) -> str:
        """Return a bounded fallback when the prototype tool loop exhausts its budget."""
        if retrieved_documents:
            return (
                "I found relevant dashboard context, but I couldn't complete the analysis safely. "
                "Please rephrase the question or ask about a metric shown on this dashboard."
            )
        return (
            f"I couldn't find enough dashboard-backed context to answer: {user_query}. "
            "Please rephrase or ask about a metric shown on this dashboard."
        )

    def _build_usage_summary(self) -> dict[str, Any]:
        """Collect per-turn usage from the LLM client and embedding provider when supported."""
        usage: dict[str, Any] = {}
        if hasattr(self.llm_client, "usage_summary"):
            llm_usage = self.llm_client.usage_summary()
            if llm_usage:
                usage["llm"] = llm_usage
        if hasattr(self.vector_store, "usage_summary"):
            embedding_usage = self.vector_store.usage_summary()
            if embedding_usage:
                usage["embeddings"] = embedding_usage
        return usage

    def _compose_small_talk_response(self, user_query: str) -> str:
        """Generate the prototype small-talk response or fall back to a fixed helper."""
        if hasattr(self.llm_client, "compose_small_talk"):
            try:
                return self.llm_client.compose_small_talk(user_query)
            except Exception:
                logger.exception("Dashboard chat small-talk generation failed")
        return "Hi! I can help with your program data and metrics. What would you like to know?"

    @staticmethod
    def _build_fast_path_intent(user_query: str) -> DashboardChatIntentDecision | None:
        """Handle obvious greetings and thanks without an LLM round trip."""
        if not GREETING_PATTERN.match(user_query.strip()):
            return None
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.SMALL_TALK,
            confidence=1.0,
            reason="Obvious greeting or thanks",
        )

    @staticmethod
    def _build_fast_path_small_talk_response(user_query: str) -> str:
        """Keep greeting replies instant and deterministic."""
        normalized_query = user_query.strip().lower()
        if "thank" in normalized_query:
            return "You're welcome. Ask me anything about this dashboard or its data."
        if "good morning" in normalized_query:
            return "Good morning. Ask me anything about this dashboard or the data behind it."
        if "good afternoon" in normalized_query:
            return "Good afternoon. Ask me anything about this dashboard or the data behind it."
        if "good evening" in normalized_query:
            return "Good evening. Ask me anything about this dashboard or the data behind it."
        return "Hi. Ask me anything about this dashboard or the data behind it."

    @staticmethod
    def _clarification_fallback(missing_info: Sequence[str]) -> str:
        """Mirror the prototype's specific clarification nudges when the router omits a question."""
        missing = {item.lower() for item in missing_info}
        prompts: list[str] = []
        if "metric" in missing:
            prompts.append("which metric")
        if "time_range" in missing or "time period" in missing:
            prompts.append("what time period")
        if "dimension" in missing:
            prompts.append("which breakdown or dimension")
        if not prompts:
            return "Could you be more specific about the metric, program, or time period you want?"
        return "Could you clarify " + ", ".join(prompts) + "?"

    @staticmethod
    def _fallback_answer_text(
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        sql_results: list[dict[str, Any]] | None,
    ) -> str:
        """Fallback response when the model returns no final text."""
        if sql_results is not None:
            if not sql_results:
                return "I didn't find any matching rows for that question."
            return DashboardChatRuntime._preview_sql_rows(sql_results)
        if retrieved_documents:
            return DashboardChatRuntime._compact_snippet(retrieved_documents[0].content)
        return "I couldn't find enough context to answer that."

    @staticmethod
    def _chart_id_from_source_identifier(source_identifier: str) -> int | None:
        """Extract chart ids from dashboard export source identifiers."""
        parts = source_identifier.split(":")
        if len(parts) >= 4 and parts[-2] == "chart":
            try:
                return int(parts[-1])
            except ValueError:
                return None
        return None

    @staticmethod
    def _unique_id_from_source_identifier(source_identifier: str) -> str | None:
        """Extract dbt unique ids from manifest/catalog source identifiers."""
        if ":" not in source_identifier:
            return None
        prefix, unique_id = source_identifier.split(":", 1)
        if prefix not in {"manifest", "catalog"}:
            return None
        return unique_id
