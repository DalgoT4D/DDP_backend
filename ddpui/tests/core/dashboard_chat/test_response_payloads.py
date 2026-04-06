from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatCitation,
    DashboardChatIntent,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
)
from ddpui.core.dashboard_chat.orchestration.retrieval_support import (
    build_citations,
    explore_table_url,
)
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
    assert citations[1].url == "/explore?schema_name=analytics&table_name=sessions"
