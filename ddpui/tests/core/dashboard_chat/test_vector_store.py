"""Tests for dashboard chat vector document and store primitives."""

from datetime import datetime, timezone
from unittest.mock import patch

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector_documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
    build_dashboard_chat_collection_name,
)
from ddpui.core.dashboard_chat.vector_store import ChromaDashboardChatVectorStore


class FakeEmbeddingProvider:
    """Deterministic embedding provider for vector store tests."""

    def embed_documents(self, texts):
        return [[float(index), float(len(text))] for index, text in enumerate(texts, start=1)]

    def embed_query(self, text):
        return [99.0, float(len(text))]


class FakeCollection:
    """In-memory collection stub used by the vector store tests."""

    def __init__(self):
        self.upsert_calls = []
        self.query_calls = []
        self.get_calls = []
        self.delete_calls = []
        self.documents = {}
        self.query_response = {
            "ids": [["doc-1"]],
            "documents": [["matched content"]],
            "metadatas": [[{"source_type": "org_context"}]],
            "distances": [[0.12]],
        }

    def upsert(self, **kwargs):
        self.upsert_calls.append(kwargs)
        for document_id, document, metadata in zip(
            kwargs["ids"],
            kwargs["documents"],
            kwargs["metadatas"],
        ):
            self.documents[document_id] = {
                "id": document_id,
                "document": document,
                "metadata": metadata,
            }

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        return self.query_response

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        rows = list(self.documents.values())
        where = kwargs.get("where")
        if where:
            rows = [row for row in rows if _matches_where(row["metadata"], where)]
        return {
            "ids": [row["id"] for row in rows],
            "documents": [row["document"] for row in rows],
            "metadatas": [row["metadata"] for row in rows],
        }

    def delete(self, ids=None, where=None):
        self.delete_calls.append({"ids": ids, "where": where})
        if ids is not None:
            for document_id in ids:
                self.documents.pop(document_id, None)
            return

        for document_id, row in list(self.documents.items()):
            if _matches_where(row["metadata"], where):
                self.documents.pop(document_id, None)


def _matches_where(metadata, where):
    """Minimal Chroma where-clause matcher for test stubs."""
    if where is None:
        return True
    if "$and" in where:
        return all(_matches_where(metadata, condition) for condition in where["$and"])
    for key, value in where.items():
        if isinstance(value, dict) and "$in" in value:
            if metadata.get(key) not in value["$in"]:
                return False
            continue
        if metadata.get(key) != value:
            return False
    return True


class FakeChromaClient:
    """In-memory Chroma HTTP client stub."""

    def __init__(self):
        self.collections = {}
        self.deleted_collections = []

    def get_or_create_collection(self, name, metadata=None):
        if name not in self.collections:
            self.collections[name] = FakeCollection()
        return self.collections[name]

    def get_collection(self, name):
        if name not in self.collections:
            raise ValueError("collection does not exist")
        return self.collections[name]

    def delete_collection(self, name):
        self.deleted_collections.append(name)
        del self.collections[name]


def test_dashboard_chat_vector_store_config_reads_env():
    """Vector store config should read the dedicated dashboard chat env vars."""
    with patch.dict(
        "os.environ",
        {
            "AI_DASHBOARD_CHAT_CHROMA_HOST": "chroma.internal",
            "AI_DASHBOARD_CHAT_CHROMA_PORT": "8100",
            "AI_DASHBOARD_CHAT_CHROMA_SSL": "true",
            "AI_DASHBOARD_CHAT_CHROMA_COLLECTION_PREFIX": "tenant_",
            "AI_DASHBOARD_CHAT_CHROMA_EMBEDDING_MODEL": "text-embedding-3-large",
        },
    ):
        config = DashboardChatVectorStoreConfig.from_env()

    assert config.chroma_host == "chroma.internal"
    assert config.chroma_port == 8100
    assert config.chroma_ssl is True
    assert config.collection_prefix == "tenant_"
    assert config.embedding_model == "text-embedding-3-large"


def test_collection_name_uses_org_prefix():
    """Collections should be split by org using the configured prefix."""
    assert build_dashboard_chat_collection_name(42) == "org_42"
    assert build_dashboard_chat_collection_name(42, prefix="tenant_") == "tenant_42"


def test_vector_document_has_stable_id_and_required_metadata():
    """Document IDs should be deterministic and metadata should include required keys."""
    updated_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
    document = DashboardChatVectorDocument(
        org_id=7,
        source_type=DashboardChatSourceType.DBT_MANIFEST,
        source_identifier="model.public.fact_enrollments",
        content="manifest chunk",
        dashboard_id=9,
        chart_id=12,
        title="Fact Enrollments",
        chunk_index=3,
        updated_at=updated_at,
    )

    assert document.document_id == document.document_id
    assert document.metadata() == {
        "org_id": 7,
        "source_type": "dbt_manifest",
        "source_identifier": "model.public.fact_enrollments",
        "chunk_index": 3,
        "document_hash": document.document_hash,
        "dashboard_id": 9,
        "chart_id": 12,
        "title": "Fact Enrollments",
        "updated_at": updated_at.isoformat(),
    }


def test_upsert_documents_uses_embeddings_and_metadata():
    """Upserts should use deterministic IDs, embeddings, and per-org collections."""
    fake_client = FakeChromaClient()
    store = ChromaDashboardChatVectorStore(
        config=DashboardChatVectorStoreConfig(collection_prefix="org_"),
        embedding_provider=FakeEmbeddingProvider(),
        client=fake_client,
    )
    documents = [
        DashboardChatVectorDocument(
            org_id=11,
            source_type=DashboardChatSourceType.ORG_CONTEXT,
            source_identifier="org_context",
            content="organization context chunk",
        ),
        DashboardChatVectorDocument(
            org_id=11,
            source_type=DashboardChatSourceType.DASHBOARD_CONTEXT,
            source_identifier="dashboard:5:context",
            content="dashboard context chunk",
            dashboard_id=5,
            title="Impact Overview",
            chunk_index=1,
        ),
    ]

    document_ids = store.upsert_documents(11, documents)
    collection = fake_client.collections["org_11"]
    upsert_call = collection.upsert_calls[0]

    assert document_ids == [documents[0].document_id, documents[1].document_id]
    assert upsert_call["ids"] == document_ids
    assert upsert_call["documents"] == ["organization context chunk", "dashboard context chunk"]
    assert upsert_call["metadatas"][0]["source_type"] == "org_context"
    assert upsert_call["metadatas"][1]["dashboard_id"] == 5
    assert upsert_call["metadatas"][1]["title"] == "Impact Overview"
    assert upsert_call["embeddings"] == [[1.0, 26.0], [2.0, 23.0]]


def test_query_scopes_to_org_collection_and_where_filters():
    """Queries should stay inside the org collection and forward source/dashboard filters."""
    fake_client = FakeChromaClient()
    fake_client.get_or_create_collection("org_3")
    store = ChromaDashboardChatVectorStore(
        config=DashboardChatVectorStoreConfig(),
        embedding_provider=FakeEmbeddingProvider(),
        client=fake_client,
    )

    results = store.query(
        3,
        query_text="what changed?",
        source_types=[DashboardChatSourceType.DBT_CATALOG, DashboardChatSourceType.ORG_CONTEXT],
        dashboard_id=9,
    )

    query_call = fake_client.collections["org_3"].query_calls[0]
    assert query_call["query_embeddings"] == [[99.0, 13.0]]
    assert query_call["where"] == {
        "$and": [
            {"source_type": {"$in": ["dbt_catalog", "org_context"]}},
            {"dashboard_id": 9},
        ]
    }
    assert results[0].document_id == "doc-1"
    assert results[0].content == "matched content"
    assert results[0].distance == 0.12


def test_delete_collection_returns_false_for_missing_org():
    """Deleting a missing collection should be a no-op."""
    store = ChromaDashboardChatVectorStore(
        config=DashboardChatVectorStoreConfig(),
        embedding_provider=FakeEmbeddingProvider(),
        client=FakeChromaClient(),
    )

    assert store.delete_collection(404) is False


def test_get_documents_and_delete_documents_respect_where_filters():
    """Collection reads and deletes should honor source and dashboard scoping."""
    fake_client = FakeChromaClient()
    store = ChromaDashboardChatVectorStore(
        config=DashboardChatVectorStoreConfig(),
        embedding_provider=FakeEmbeddingProvider(),
        client=fake_client,
    )
    documents = [
        DashboardChatVectorDocument(
            org_id=23,
            source_type=DashboardChatSourceType.ORG_CONTEXT,
            source_identifier="org:23:context",
            content="org chunk",
        ),
        DashboardChatVectorDocument(
            org_id=23,
            source_type=DashboardChatSourceType.DASHBOARD_CONTEXT,
            source_identifier="dashboard:7:context",
            content="dashboard chunk",
            dashboard_id=7,
        ),
    ]
    store.upsert_documents(23, documents)

    stored_documents = store.get_documents(
        23,
        source_types=[DashboardChatSourceType.DASHBOARD_CONTEXT],
        dashboard_id=7,
        include_documents=True,
    )

    assert len(stored_documents) == 1
    assert stored_documents[0].content == "dashboard chunk"
    assert stored_documents[0].metadata["dashboard_id"] == 7

    deleted_count = store.delete_documents(
        23,
        source_types=[DashboardChatSourceType.DASHBOARD_CONTEXT],
    )

    assert deleted_count == 1
    assert len(store.get_documents(23)) == 1
    assert store.get_documents(23)[0].metadata["source_type"] == "org_context"
