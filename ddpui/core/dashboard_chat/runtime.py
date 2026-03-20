"""LangGraph runtime for dashboard chat orchestration."""

from collections.abc import Callable, Sequence
import json
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ddpui.core.dashboard_chat.allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
)
from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.llm_client import (
    DashboardChatLlmClient,
    OpenAIDashboardChatLlmClient,
)
from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatCitation,
    DashboardChatConversationMessage,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatPlanMode,
    DashboardChatQueryPlan,
    DashboardChatRelatedDashboard,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSqlDraft,
    DashboardChatSqlValidationResult,
)
from ddpui.core.dashboard_chat.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector_documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.vector_store import ChromaDashboardChatVectorStore
from ddpui.core.dashboard_chat.warehouse_tools import (
    DashboardChatWarehouseTools,
    DashboardChatWarehouseToolsError,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.services.dashboard_service import DashboardService

SIMPLE_GREETINGS = {"hi", "hey", "hello", "thanks", "thank you", "gm", "good morning"}
DATA_QUERY_KEYWORDS = {
    "count",
    "counts",
    "trend",
    "compare",
    "breakdown",
    "how many",
    "total",
    "sum",
    "average",
    "avg",
    "top",
    "bottom",
    "show me",
    "list",
    "split by",
    "group by",
}
CONTEXT_QUERY_KEYWORDS = {
    "what does",
    "explain",
    "definition",
    "metric",
    "why",
    "how is",
    "which chart",
    "which dataset",
    "context",
}


class DashboardChatRuntimeState(TypedDict, total=False):
    """LangGraph state for dashboard chat."""

    org: Org
    dashboard_id: int
    user_query: str
    conversation_history: list[DashboardChatConversationMessage]
    dashboard_export: dict[str, Any]
    dashboard_summary: str
    allowlist: DashboardChatAllowlist
    intent_decision: DashboardChatIntentDecision
    retrieved_documents: list[DashboardChatRetrievedDocument]
    citations: list[DashboardChatCitation]
    related_dashboards: list[DashboardChatRelatedDashboard]
    schema_prompt: str
    schema_snippets: dict[str, Any]
    query_plan: DashboardChatQueryPlan
    distinct_values: dict[str, list[str]]
    sql_draft: DashboardChatSqlDraft | None
    sql_validation: DashboardChatSqlValidationResult | None
    sql_results: list[dict[str, Any]] | None
    warnings: list[str]
    response: DashboardChatResponse


class DashboardChatRuntime:
    """Run dashboard chat queries through a LangGraph workflow."""

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
        conversation_history: Sequence[DashboardChatConversationMessage | dict[str, str]] | None = None,
    ) -> DashboardChatResponse:
        """Run a single dashboard chat turn and return the structured response."""
        initial_state: DashboardChatRuntimeState = {
            "org": org,
            "dashboard_id": dashboard_id,
            "user_query": user_query,
            "conversation_history": self._normalize_conversation_history(conversation_history),
            "warnings": [],
        }
        final_state = self.graph.invoke(initial_state)
        return final_state["response"]

    def _build_graph(self):
        """Create the LangGraph state machine."""
        graph = StateGraph(DashboardChatRuntimeState)
        graph.add_node("load_context", self._node_load_context)
        graph.add_node("route_intent", self._node_route_intent)
        graph.add_node("build_allowlist", self._node_build_allowlist)
        graph.add_node("retrieve_docs", self._node_retrieve_docs)
        graph.add_node("load_schema_snippets", self._node_load_schema_snippets)
        graph.add_node("plan_query", self._node_plan_query)
        graph.add_node("lookup_distinct_values", self._node_lookup_distinct_values)
        graph.add_node("generate_sql", self._node_generate_sql)
        graph.add_node("validate_sql", self._node_validate_sql)
        graph.add_node("execute_sql", self._node_execute_sql)
        graph.add_node("compose_answer", self._node_compose_answer)
        graph.add_node("finalize", self._node_finalize_response)

        graph.add_edge(START, "load_context")
        graph.add_edge("load_context", "route_intent")
        graph.add_conditional_edges(
            "route_intent",
            self._route_after_intent,
            {
                "compose_answer": "compose_answer",
                "build_allowlist": "build_allowlist",
            },
        )
        graph.add_edge("build_allowlist", "retrieve_docs")
        graph.add_edge("retrieve_docs", "load_schema_snippets")
        graph.add_edge("load_schema_snippets", "plan_query")
        graph.add_conditional_edges(
            "plan_query",
            self._route_after_plan,
            {
                "compose_answer": "compose_answer",
                "lookup_distinct_values": "lookup_distinct_values",
            },
        )
        graph.add_edge("lookup_distinct_values", "generate_sql")
        graph.add_edge("generate_sql", "validate_sql")
        graph.add_conditional_edges(
            "validate_sql",
            self._route_after_sql_validation,
            {
                "compose_answer": "compose_answer",
                "execute_sql": "execute_sql",
            },
        )
        graph.add_edge("execute_sql", "compose_answer")
        graph.add_edge("compose_answer", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile()

    def _node_load_context(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Load dashboard context and summary."""
        dashboard_export = DashboardService.export_dashboard_context(
            state["dashboard_id"],
            state["org"],
        )
        state["dashboard_export"] = dashboard_export
        state["dashboard_summary"] = self._build_dashboard_summary(dashboard_export)
        return state

    def _node_route_intent(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Classify the user query."""
        intent_decision = self._heuristic_intent_decision(
            user_query=state["user_query"],
            conversation_history=state["conversation_history"],
        )
        if intent_decision is None:
            intent_decision = self.llm_client.classify_intent(
                user_query=state["user_query"],
                conversation_history=state["conversation_history"],
                dashboard_summary=state["dashboard_summary"],
            )
        state["intent_decision"] = intent_decision
        return state

    def _node_build_allowlist(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Build the dashboard table allowlist from export data and dbt lineage."""
        manifest_json = DashboardChatAllowlistBuilder.load_manifest_json(state["org"].dbt)
        state["allowlist"] = DashboardChatAllowlistBuilder.build(
            state["dashboard_export"],
            manifest_json=manifest_json,
        )
        return state

    def _node_retrieve_docs(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Retrieve dashboard, org, and dbt context from Chroma."""
        org = state["org"]
        dashboard_results = self._query_vector_store(
            org=org,
            query_text=state["user_query"],
            source_types=self.source_config.filter_enabled(
                [
                DashboardChatSourceType.DASHBOARD_EXPORT.value,
                DashboardChatSourceType.DASHBOARD_CONTEXT.value,
                ]
            ),
            dashboard_id=state["dashboard_id"],
        )
        org_results = self._query_vector_store(
            org=org,
            query_text=state["user_query"],
            source_types=self.source_config.filter_enabled(
                [DashboardChatSourceType.ORG_CONTEXT.value]
            ),
        )
        dbt_results = self._filter_allowlisted_dbt_results(
            self._query_vector_store(
                org=org,
                query_text=state["user_query"],
                source_types=self.source_config.filter_enabled(
                    [
                    DashboardChatSourceType.DBT_MANIFEST.value,
                    DashboardChatSourceType.DBT_CATALOG.value,
                    ]
                ),
            ),
            state["allowlist"],
        )

        retrieved_documents = self._merge_retrieval_results(
            dashboard_results=dashboard_results,
            org_results=org_results,
            dbt_results=dbt_results,
        )
        state["retrieved_documents"] = retrieved_documents
        state["citations"] = self._build_citations(
            retrieved_documents=retrieved_documents,
            dashboard_export=state["dashboard_export"],
            allowlist=state["allowlist"],
        )
        state["related_dashboards"] = self._build_related_dashboards(
            org=org,
            current_dashboard_id=state["dashboard_id"],
            query_text=state["user_query"],
        )
        return state

    def _node_load_schema_snippets(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Load schema snippets for the relevant dashboard tables."""
        intent_decision = state["intent_decision"]
        if not intent_decision.force_sql_path and intent_decision.intent != DashboardChatIntent.DATA_QUERY:
            state["schema_snippets"] = {}
            state["schema_prompt"] = ""
            return state

        candidate_tables = state["allowlist"].prioritized_tables(
            limit=self.runtime_config.max_schema_tables,
        )
        if not candidate_tables:
            state["schema_snippets"] = {}
            state["schema_prompt"] = ""
            state["warnings"] = state.get("warnings", []) + [
                "No dashboard tables were available for schema inspection.",
            ]
            return state

        try:
            warehouse_tools = self.warehouse_tools_factory(state["org"])
            schema_snippets = warehouse_tools.get_schema_snippets(candidate_tables)
        except DashboardChatWarehouseToolsError as error:
            state["schema_snippets"] = {}
            state["schema_prompt"] = ""
            state["warnings"] = state.get("warnings", []) + [str(error)]
            return state

        state["schema_snippets"] = schema_snippets
        state["schema_prompt"] = "\n\n".join(
            snippet.to_prompt_text() for snippet in schema_snippets.values()
        )
        return state

    def _node_plan_query(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Produce the structured execution plan."""
        intent_decision = state["intent_decision"]

        if intent_decision.intent == DashboardChatIntent.CONTEXT_QUERY and not intent_decision.force_sql_path:
            state["query_plan"] = DashboardChatQueryPlan(
                mode=DashboardChatPlanMode.CONTEXT,
                reason=intent_decision.reason,
            )
            return state

        if intent_decision.intent in {
            DashboardChatIntent.SMALL_TALK,
            DashboardChatIntent.IRRELEVANT,
            DashboardChatIntent.NEEDS_CLARIFICATION,
        }:
            state["query_plan"] = DashboardChatQueryPlan(
                mode=DashboardChatPlanMode.CLARIFY,
                reason=intent_decision.reason,
                clarification_question=intent_decision.clarification_question,
            )
            return state

        allowlisted_tables = state["allowlist"].prioritized_tables()
        if not allowlisted_tables and intent_decision.force_sql_path:
            state["query_plan"] = DashboardChatQueryPlan(
                mode=DashboardChatPlanMode.CLARIFY,
                reason="Current dashboard does not expose any allowlisted tables for SQL.",
                clarification_question=(
                    "I can explain this dashboard, but it does not expose a data source I can query safely."
                ),
            )
            return state

        query_plan = self.llm_client.plan_query(
            user_query=state["user_query"],
            conversation_history=state["conversation_history"],
            dashboard_summary=state["dashboard_summary"],
            retrieved_documents=state.get("retrieved_documents", []),
            schema_prompt=state.get("schema_prompt", ""),
            allowlisted_tables=allowlisted_tables,
        )
        query_plan = self._normalize_query_plan(
            query_plan=query_plan,
            allowlist=state["allowlist"],
            default_tables=allowlisted_tables,
        )
        state["query_plan"] = query_plan
        return state

    def _node_lookup_distinct_values(
        self,
        state: DashboardChatRuntimeState,
    ) -> DashboardChatRuntimeState:
        """Fetch distinct values for requested text filters."""
        distinct_values: dict[str, list[str]] = {}
        query_plan = state["query_plan"]
        if not query_plan.text_filters:
            state["distinct_values"] = distinct_values
            return state

        try:
            warehouse_tools = self.warehouse_tools_factory(state["org"])
        except DashboardChatWarehouseToolsError as error:
            state["warnings"] = state.get("warnings", []) + [str(error)]
            state["distinct_values"] = distinct_values
            return state

        available_tables = set(state.get("schema_snippets", {}).keys())
        for text_filter in query_plan.text_filters:
            table_name = text_filter.table_name.lower()
            if not state["allowlist"].is_allowed(table_name) or table_name not in available_tables:
                continue
            distinct_key = f"{table_name}.{text_filter.column_name}"
            distinct_values[distinct_key] = warehouse_tools.get_distinct_values(
                table_name=table_name,
                column_name=text_filter.column_name,
                limit=self.runtime_config.max_distinct_values,
            )

        state["distinct_values"] = distinct_values
        return state

    def _node_generate_sql(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Generate SQL from the structured plan."""
        query_plan = state["query_plan"]
        if query_plan.mode != DashboardChatPlanMode.SQL:
            state["sql_draft"] = None
            return state

        sql_draft = self.llm_client.generate_sql(
            user_query=state["user_query"],
            dashboard_summary=state["dashboard_summary"],
            query_plan=query_plan,
            schema_prompt=self._schema_prompt_for_plan(
                state.get("schema_snippets", {}),
                query_plan,
            ),
            distinct_values=state.get("distinct_values", {}),
            allowlisted_tables=state["allowlist"].prioritized_tables(),
        )
        state["sql_draft"] = sql_draft
        return state

    def _node_validate_sql(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Run SQL guard validation."""
        sql_draft = state.get("sql_draft")
        if sql_draft is None or not sql_draft.sql:
            state["sql_validation"] = None
            return state

        validation = DashboardChatSqlGuard(
            allowlist=state["allowlist"],
            max_rows=self.runtime_config.max_query_rows,
        ).validate(sql_draft.sql)
        state["sql_validation"] = validation
        return state

    def _node_execute_sql(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Execute the validated SQL."""
        sql_validation = state["sql_validation"]
        if sql_validation is None or not sql_validation.sanitized_sql:
            state["sql_results"] = None
            return state

        try:
            warehouse_tools = self.warehouse_tools_factory(state["org"])
            state["sql_results"] = warehouse_tools.execute_sql(sql_validation.sanitized_sql)
        except DashboardChatWarehouseToolsError as error:
            state["warnings"] = state.get("warnings", []) + [str(error)]
            state["sql_results"] = None
        return state

    def _node_compose_answer(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Assemble the answer text across simple, context, and SQL paths."""
        intent_decision = state["intent_decision"]
        query_plan = state.get("query_plan")
        sql_draft = state.get("sql_draft")
        sql_validation = state.get("sql_validation")

        warnings = list(dict.fromkeys(state.get("warnings", [])))
        if sql_draft is not None:
            warnings.extend(warning for warning in sql_draft.warnings if warning not in warnings)
        if sql_validation is not None:
            warnings.extend(warning for warning in sql_validation.warnings if warning not in warnings)

        if intent_decision.intent == DashboardChatIntent.SMALL_TALK:
            answer_text = (
                "I can help explain this dashboard or answer questions about the data behind its charts."
            )
        elif intent_decision.intent == DashboardChatIntent.IRRELEVANT:
            answer_text = (
                "I can help with questions about this dashboard, its charts, and the data behind them."
            )
        elif intent_decision.intent == DashboardChatIntent.NEEDS_CLARIFICATION:
            answer_text = (
                intent_decision.clarification_question
                or "Could you be more specific about the metric, program, or time period you want?"
            )
        elif query_plan and query_plan.mode == DashboardChatPlanMode.CLARIFY:
            answer_text = query_plan.clarification_question or (
                sql_draft.clarification_question
                if sql_draft is not None and sql_draft.clarification_question
                else "I need a bit more detail before I can answer that safely."
            )
        elif sql_draft is not None and not sql_draft.sql and sql_draft.clarification_question:
            answer_text = sql_draft.clarification_question
        elif sql_validation is not None and not sql_validation.is_valid:
            answer_text = "I couldn't answer that safely from this dashboard context."
            if sql_validation.errors:
                answer_text += f" {sql_validation.errors[0]}"
        else:
            try:
                answer_text = self.llm_client.compose_answer(
                    user_query=state["user_query"],
                    dashboard_summary=state["dashboard_summary"],
                    retrieved_documents=state.get("retrieved_documents", []),
                    sql=sql_validation.sanitized_sql if sql_validation else None,
                    sql_results=state.get("sql_results"),
                    warnings=warnings,
                    related_dashboard_titles=[
                        related_dashboard.title
                        for related_dashboard in state.get("related_dashboards", [])
                    ],
                )
            except Exception:
                answer_text = self._fallback_answer_text(
                    retrieved_documents=state.get("retrieved_documents", []),
                    sql_results=state.get("sql_results"),
                )

        state["response"] = DashboardChatResponse(
            answer_text=answer_text.strip(),
            intent=intent_decision.intent,
            citations=state.get("citations", []),
            related_dashboards=state.get("related_dashboards", []),
            warnings=warnings,
            sql=sql_validation.sanitized_sql if sql_validation else None,
            sql_results=state.get("sql_results"),
            metadata={},
        )
        return state

    def _node_finalize_response(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        """Attach metadata and table citations to the final response."""
        response = state["response"]
        citations = list(response.citations)
        sql_validation = state.get("sql_validation")
        if sql_validation is not None:
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

        state["response"] = DashboardChatResponse(
            answer_text=response.answer_text,
            intent=response.intent,
            citations=list(dict.fromkeys(citations)),
            related_dashboards=response.related_dashboards,
            warnings=response.warnings,
            sql=response.sql,
            sql_results=response.sql_results,
            metadata={
                "dashboard_id": state["dashboard_id"],
                "query_plan_mode": state.get("query_plan").mode.value
                if state.get("query_plan")
                else None,
                "query_plan_tables": state.get("query_plan").relevant_tables
                if state.get("query_plan")
                else [],
                "retrieved_document_ids": [
                    document.document_id for document in state.get("retrieved_documents", [])
                ],
                "allowlisted_tables": sorted(state["allowlist"].allowed_tables),
                "sql_guard_errors": state.get("sql_validation").errors
                if state.get("sql_validation")
                else [],
            },
        )
        return state

    @staticmethod
    def _route_after_intent(state: DashboardChatRuntimeState) -> str:
        """Route simple intents directly to answer composition."""
        intent = state["intent_decision"].intent
        if intent in {
            DashboardChatIntent.SMALL_TALK,
            DashboardChatIntent.IRRELEVANT,
            DashboardChatIntent.NEEDS_CLARIFICATION,
        }:
            return "compose_answer"
        return "build_allowlist"

    @staticmethod
    def _route_after_plan(state: DashboardChatRuntimeState) -> str:
        """Route SQL plans to distinct lookup and all others to answer composition."""
        query_plan = state["query_plan"]
        if query_plan.mode == DashboardChatPlanMode.SQL:
            return "lookup_distinct_values"
        return "compose_answer"

    @staticmethod
    def _route_after_sql_validation(state: DashboardChatRuntimeState) -> str:
        """Only execute SQL after it passes validation."""
        sql_validation = state.get("sql_validation")
        if sql_validation is not None and sql_validation.is_valid and sql_validation.sanitized_sql:
            return "execute_sql"
        return "compose_answer"

    @staticmethod
    def _normalize_conversation_history(
        conversation_history: Sequence[DashboardChatConversationMessage | dict[str, str]] | None,
    ) -> list[DashboardChatConversationMessage]:
        """Normalize conversation history into typed messages."""
        normalized_messages: list[DashboardChatConversationMessage] = []
        for item in conversation_history or []:
            if isinstance(item, DashboardChatConversationMessage):
                normalized_messages.append(item)
            else:
                normalized_messages.append(
                    DashboardChatConversationMessage(
                        role=str(item.get("role") or "user"),
                        content=str(item.get("content") or ""),
                    )
                )
        return normalized_messages

    @staticmethod
    def _heuristic_intent_decision(
        user_query: str,
        conversation_history: Sequence[DashboardChatConversationMessage],
    ) -> DashboardChatIntentDecision | None:
        """Fast path for obvious intents before consulting the LLM."""
        normalized_query = user_query.strip().lower()
        if not normalized_query:
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.NEEDS_CLARIFICATION,
                reason="Empty query",
                clarification_question="What would you like to know about this dashboard?",
            )

        if normalized_query in SIMPLE_GREETINGS:
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.SMALL_TALK,
                reason="Greeting or pleasantry",
            )

        if any(keyword in normalized_query for keyword in DATA_QUERY_KEYWORDS):
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.DATA_QUERY,
                reason="Contains data-analysis keywords",
                force_sql_path=True,
            )

        if any(keyword in normalized_query for keyword in CONTEXT_QUERY_KEYWORDS):
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.CONTEXT_QUERY,
                reason="Contains definition or explanation keywords",
            )

        if len(normalized_query.split()) <= 2 and conversation_history:
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.DATA_QUERY,
                reason="Short follow-up treated as a data refinement",
                force_sql_path=True,
            )
        return None

    def _query_vector_store(
        self,
        org: Org,
        query_text: str,
        source_types: Sequence[str],
        dashboard_id: int | None = None,
    ) -> list[DashboardChatRetrievedDocument]:
        """Run a vector query and map it into runtime documents."""
        if not source_types:
            return []

        results = self.vector_store.query(
            org.id,
            query_text=query_text,
            n_results=self.runtime_config.retrieval_limit,
            source_types=list(source_types),
            dashboard_id=dashboard_id,
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
    def _merge_retrieval_results(
        dashboard_results: Sequence[DashboardChatRetrievedDocument],
        org_results: Sequence[DashboardChatRetrievedDocument],
        dbt_results: Sequence[DashboardChatRetrievedDocument],
    ) -> list[DashboardChatRetrievedDocument]:
        """Prioritize current-dashboard docs, then org docs, then dbt docs."""
        scored_results: list[tuple[tuple[int, float], DashboardChatRetrievedDocument]] = []
        for priority, result_group in enumerate([dashboard_results, org_results, dbt_results]):
            for result in result_group:
                scored_results.append(
                    (
                        (priority, result.distance if result.distance is not None else 999.0),
                        result,
                    )
                )

        merged_results: list[DashboardChatRetrievedDocument] = []
        seen_document_ids: set[str] = set()
        for _, result in sorted(scored_results, key=lambda item: item[0]):
            if result.document_id in seen_document_ids:
                continue
            merged_results.append(result)
            seen_document_ids.add(result.document_id)
        return merged_results

    def _build_related_dashboards(
        self,
        org: Org,
        current_dashboard_id: int,
        query_text: str,
    ) -> list[DashboardChatRelatedDashboard]:
        """Suggest other dashboards with matching retrieved context."""
        related_dashboard_source_types = self.source_config.filter_enabled(
            [
                DashboardChatSourceType.DASHBOARD_CONTEXT.value,
                DashboardChatSourceType.DASHBOARD_EXPORT.value,
            ]
        )
        if not related_dashboard_source_types:
            return []

        related_results = self.vector_store.query(
            org.id,
            query_text=query_text,
            n_results=self.runtime_config.related_dashboard_limit * 4,
            source_types=related_dashboard_source_types,
        )
        candidate_dashboard_ids = [
            result.metadata.get("dashboard_id")
            for result in related_results
            if result.metadata.get("dashboard_id")
            and result.metadata.get("dashboard_id") != current_dashboard_id
        ]
        if not candidate_dashboard_ids:
            return []

        dashboard_titles = {
            dashboard.id: dashboard.title
            for dashboard in Dashboard.objects.filter(org=org, id__in=set(candidate_dashboard_ids))
        }
        suggestions: list[DashboardChatRelatedDashboard] = []
        seen_dashboard_ids: set[int] = set()
        for result in related_results:
            dashboard_id = result.metadata.get("dashboard_id")
            if (
                dashboard_id in seen_dashboard_ids
                or dashboard_id == current_dashboard_id
                or dashboard_id not in dashboard_titles
            ):
                continue
            suggestions.append(
                DashboardChatRelatedDashboard(
                    dashboard_id=int(dashboard_id),
                    title=dashboard_titles[dashboard_id],
                    reason=self._compact_snippet(result.content),
                )
            )
            seen_dashboard_ids.add(int(dashboard_id))
            if len(suggestions) >= self.runtime_config.related_dashboard_limit:
                break
        return suggestions

    def _build_citations(
        self,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        dashboard_export: dict[str, Any],
        allowlist: DashboardChatAllowlist,
    ) -> list[DashboardChatCitation]:
        """Build structured citations from retrieved documents."""
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
            title = self._citation_title(
                document=document,
                dashboard_title=dashboard_title,
                chart_lookup=chart_lookup,
                table_name=table_name,
            )
            citations.append(
                DashboardChatCitation(
                    source_type=document.source_type,
                    source_identifier=document.source_identifier,
                    title=title,
                    snippet=self._compact_snippet(document.content),
                    dashboard_id=document.dashboard_id,
                    table_name=table_name,
                )
            )

        return citations

    @staticmethod
    def _citation_title(
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
            chart_id = DashboardChatRuntime._chart_id_from_source_identifier(document.source_identifier)
            if chart_id is not None and chart_id in chart_lookup:
                return f"Chart: {chart_lookup[chart_id]}"
            return f"Dashboard export: {dashboard_title}"
        if document.source_type == DashboardChatSourceType.DBT_MANIFEST.value:
            return f"dbt manifest: {table_name or document.source_identifier}"
        if document.source_type == DashboardChatSourceType.DBT_CATALOG.value:
            return f"dbt catalog: {table_name or document.source_identifier}"
        return document.source_identifier

    @staticmethod
    def _build_dashboard_summary(dashboard_export: dict[str, Any]) -> str:
        """Format the dashboard summary fed into the LLM."""
        dashboard_payload = dashboard_export["dashboard"]
        lines = [
            f"Dashboard: {dashboard_payload.get('title')}",
            f"Description: {dashboard_payload.get('description') or 'None'}",
        ]
        for chart in dashboard_export.get("charts") or []:
            lines.append(
                "{title} uses {schema}.{table} ({chart_type})".format(
                    title=chart.get("title"),
                    schema=chart.get("schema_name"),
                    table=chart.get("table_name"),
                    chart_type=chart.get("chart_type"),
                )
            )
        return "\n".join(lines)

    @staticmethod
    def _normalize_query_plan(
        query_plan: DashboardChatQueryPlan,
        allowlist: DashboardChatAllowlist,
        default_tables: Sequence[str],
    ) -> DashboardChatQueryPlan:
        """Drop out-of-bounds tables and backfill safe defaults."""
        relevant_tables = [
            table_name.lower()
            for table_name in query_plan.relevant_tables
            if allowlist.is_allowed(table_name)
        ]
        schema_lookup_tables = [
            table_name.lower()
            for table_name in query_plan.schema_lookup_tables
            if allowlist.is_allowed(table_name)
        ]
        if query_plan.mode == DashboardChatPlanMode.SQL and not relevant_tables:
            relevant_tables = [table_name.lower() for table_name in default_tables]
        if query_plan.mode == DashboardChatPlanMode.SQL and not schema_lookup_tables:
            schema_lookup_tables = relevant_tables
        return DashboardChatQueryPlan(
            mode=query_plan.mode,
            reason=query_plan.reason,
            relevant_tables=relevant_tables,
            schema_lookup_tables=schema_lookup_tables,
            text_filters=[
                text_filter
                for text_filter in query_plan.text_filters
                if allowlist.is_allowed(text_filter.table_name)
            ],
            answer_strategy=query_plan.answer_strategy,
            clarification_question=query_plan.clarification_question,
        )

    @staticmethod
    def _schema_prompt_for_plan(
        schema_snippets: dict[str, Any],
        query_plan: DashboardChatQueryPlan,
    ) -> str:
        """Filter the schema prompt down to the planned tables when possible."""
        if not schema_snippets:
            return ""
        desired_tables = query_plan.schema_lookup_tables or query_plan.relevant_tables
        if not desired_tables:
            return "\n\n".join(snippet.to_prompt_text() for snippet in schema_snippets.values())
        return "\n\n".join(
            schema_snippets[table_name].to_prompt_text()
            for table_name in desired_tables
            if table_name in schema_snippets
        )

    @staticmethod
    def _fallback_answer_text(
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        sql_results: list[dict[str, Any]] | None,
    ) -> str:
        """Fallback response when answer composition fails."""
        if sql_results is not None:
            if not sql_results:
                return "I didn't find any matching rows for that question."
            return "Here are the matching results: " + json.dumps(sql_results[:3], default=str)
        if retrieved_documents:
            return DashboardChatRuntime._compact_snippet(retrieved_documents[0].content)
        return "I couldn't find enough context to answer that."

    @staticmethod
    def _compact_snippet(content: str, max_length: int = 220) -> str:
        """Collapse whitespace and trim long snippets for citations and suggestions."""
        normalized = " ".join(content.split())
        if len(normalized) <= max_length:
            return normalized
        return normalized[: max_length - 3].rstrip() + "..."

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
