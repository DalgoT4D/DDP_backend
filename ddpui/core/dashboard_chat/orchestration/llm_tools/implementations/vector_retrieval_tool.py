"""Retrieval-facing tool handlers exposed to the LLM tool loop."""

from typing import Any

from ddpui.core.dashboard_chat.vector.vector_documents import DashboardChatSourceType

from ddpui.core.dashboard_chat.orchestration.retrieval_support import (
    build_tool_document_payload,
    dedupe_retrieved_documents,
    filter_allowlisted_dbt_results,
    get_or_embed_query,
    retrieve_vector_documents,
)
from ddpui.models.org import Org
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
)


def handle_retrieve_docs_tool(
    vector_store,
    source_config,
    runtime_config,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Retrieve current-dashboard, org, and dbt context using the tool contract."""
    org = Org.objects.select_related("dbt").get(id=int(state["org_id"]))
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    dashboard_export = state.get("dashboard_export_payload") or {}
    query = str(args.get("query") or state["user_query"]).strip()
    limit = max(1, min(int(args.get("limit", 8)), 20))
    requested_types = [
        str(doc_type)
        for doc_type in (args.get("types") or ["chart", "dataset", "context", "dbt_model"])
    ]
    retrieved_documents = []
    cached_embedding = get_or_embed_query(
        vector_store,
        query,
        turn_context.query_embeddings,
    )

    if "chart" in requested_types:
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=org,
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_EXPORT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=cached_embedding,
            )
        )
    if "context" in requested_types:
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=org,
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_CONTEXT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=cached_embedding,
            )
        )
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=org,
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled([DashboardChatSourceType.ORG_CONTEXT]),
                query_embedding=cached_embedding,
            )
        )
    if "dataset" in requested_types or "dbt_model" in requested_types:
        dbt_results = retrieve_vector_documents(
            vector_store,
            runtime_config,
            org=org,
            collection_name=state.get("vector_collection_name"),
            query_text=query,
            source_types=source_config.filter_enabled(
                [
                    DashboardChatSourceType.DBT_MANIFEST,
                    DashboardChatSourceType.DBT_CATALOG,
                ]
            ),
            query_embedding=cached_embedding,
        )
        retrieved_documents.extend(filter_allowlisted_dbt_results(dbt_results, allowlist))

    merged_results = dedupe_retrieved_documents(retrieved_documents)[:limit]
    for document in merged_results:
        if document.document_id in turn_context.retrieved_document_ids:
            continue
        turn_context.retrieved_document_ids.add(document.document_id)
        turn_context.retrieved_documents.append(document)

    docs = [
        build_tool_document_payload(document, allowlist, dashboard_export)
        for document in merged_results
    ]
    return {"docs": docs, "count": len(docs)}
