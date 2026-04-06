from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntent
from ddpui.core.dashboard_chat.contracts.response_contracts import (
    DashboardChatCitation,
    DashboardChatResponse,
)
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatRetrievedDocument
from ddpui.core.dashboard_chat.orchestration.retrieval_support import (
    build_citations,
    explore_table_url,
)
from ddpui.core.dashboard_chat.orchestration.nodes.finalize import finalize_node
from ddpui.core.dashboard_chat.vector.vector_documents import DashboardChatSourceType


def test_dashboard_chat_response_to_dict_includes_citation_urls():
    response = DashboardChatResponse(
        answer_text="Answer",
        intent=DashboardChatIntent.QUERY_WITH_SQL,
        citations=[
            DashboardChatCitation(
                source_type="warehouse_table",
                source_identifier="analytics.sessions",
                title="Warehouse table: analytics.sessions",
                snippet="SQL executed against analytics.sessions.",
                url="/explore?schema_name=analytics&table_name=sessions",
            )
        ],
    )

    payload = response.to_dict()

    assert payload["citations"][0]["url"] == "/explore?schema_name=analytics&table_name=sessions"


def test_explore_table_url_requires_schema_qualified_table_name():
    assert explore_table_url("analytics.sessions") == "/explore?schema_name=analytics&table_name=sessions"
    assert explore_table_url("sessions") is None


def test_build_citations_adds_frontend_urls():
    allowlist = DashboardChatAllowlist(
        allowed_tables={"analytics.sessions"},
        unique_id_to_table={"model.dalgo.sessions": "analytics.sessions"},
    )
    retrieved_documents = [
        DashboardChatRetrievedDocument(
            document_id="doc-1",
            source_type=DashboardChatSourceType.DASHBOARD_EXPORT.value,
            source_identifier="dashboard:6:chart:7",
            content="Chart content",
            dashboard_id=6,
        ),
        DashboardChatRetrievedDocument(
            document_id="doc-2",
            source_type=DashboardChatSourceType.DBT_MANIFEST.value,
            source_identifier="manifest:model.dalgo.sessions",
            content="Model content",
            dashboard_id=6,
        ),
        DashboardChatRetrievedDocument(
            document_id="doc-3",
            source_type=DashboardChatSourceType.DASHBOARD_EXPORT.value,
            source_identifier="dashboard:6",
            content="Dashboard export content",
            dashboard_id=6,
        ),
    ]
    dashboard_export = {
        "dashboard": {"id": 6, "title": "Impact Overview"},
        "charts": [{"id": 7, "title": "Sessions by District"}],
    }

    citations = build_citations(
        retrieved_documents=retrieved_documents,
        dashboard_export=dashboard_export,
        allowlist=allowlist,
    )

    assert citations[0].url == "/charts/7"
    assert citations[0].title == "Chart: Sessions by District"
    assert citations[0].snippet == 'Reviewed chart configuration and metadata for "Sessions by District".'
    assert len(citations) == 1


def test_build_citations_ignores_dbt_docs_for_user_facing_sources():
    allowlist = DashboardChatAllowlist(
        allowed_tables={"analytics.sessions"},
        unique_id_to_table={"model.dalgo.sessions": "analytics.sessions"},
    )
    retrieved_documents = [
        DashboardChatRetrievedDocument(
            document_id="doc-1",
            source_type=DashboardChatSourceType.DBT_MANIFEST.value,
            source_identifier="manifest:model.dalgo.sessions",
            content="Manifest content",
            dashboard_id=6,
        ),
        DashboardChatRetrievedDocument(
            document_id="doc-2",
            source_type=DashboardChatSourceType.DBT_CATALOG.value,
            source_identifier="catalog:model.dalgo.sessions",
            content="Catalog content",
            dashboard_id=6,
        ),
    ]

    citations = build_citations(
        retrieved_documents=retrieved_documents,
        dashboard_export={"dashboard": {"id": 6, "title": "Impact Overview"}, "charts": []},
        allowlist=allowlist,
    )

    assert citations == []


def test_build_citations_formats_context_file_cards_for_users():
    citations = build_citations(
        retrieved_documents=[
            DashboardChatRetrievedDocument(
                document_id="doc-1",
                source_type=DashboardChatSourceType.ORG_CONTEXT.value,
                source_identifier="org:1",
                content="Organization markdown content",
                dashboard_id=6,
            ),
            DashboardChatRetrievedDocument(
                document_id="doc-2",
                source_type=DashboardChatSourceType.DASHBOARD_CONTEXT.value,
                source_identifier="dashboard_context:6",
                content="Dashboard markdown content",
                dashboard_id=6,
            ),
        ],
        dashboard_export={"dashboard": {"id": 6, "title": "Impact Overview"}, "charts": []},
        allowlist=DashboardChatAllowlist(allowed_tables=set(), unique_id_to_table={}),
    )

    assert citations[0].title == "Organization context file"
    assert citations[0].snippet == (
        "Studied context about the organization from the organization context file."
    )
    assert citations[0].url == "/settings/organization"
    assert citations[1].title == "Dashboard context file: Impact Overview"
    assert citations[1].snippet == 'Studied context about "Impact Overview" from the dashboard context file.'
    assert citations[1].url == "/settings/organization?dashboard_id=6"


def test_finalize_prefers_sql_table_citation_over_retrieval_table_citation():
    state = {
        "dashboard_id": 6,
        "retrieved_documents": [],
        "allowlist_payload": {"allowed_tables": ["analytics.sessions"], "unique_id_to_table": {}},
        "intent_decision": {
            "intent": "query_with_sql",
            "confidence": 0.95,
            "reason": "Needs SQL",
            "missing_info": [],
            "follow_up_context": {"is_follow_up": False, "follow_up_type": None},
        },
        "sql_validation": {
            "is_valid": True,
            "sanitized_sql": "select * from analytics.sessions",
            "errors": [],
            "tables": ["analytics.sessions"],
        },
        "response": DashboardChatResponse(
            answer_text="Answer",
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            citations=[
                DashboardChatCitation(
                    source_type="warehouse_table",
                    source_identifier="analytics.sessions",
                    title="Warehouse table: analytics.sessions",
                    snippet="Schema and model context referenced analytics.sessions.",
                    url="/explore?schema_name=analytics&table_name=sessions",
                )
            ],
        ).to_dict(),
    }

    finalized = DashboardChatResponse.model_validate(finalize_node(state)["response"])

    assert len(finalized.citations) == 1
    assert finalized.citations[0].snippet == "SQL executed against analytics.sessions."
