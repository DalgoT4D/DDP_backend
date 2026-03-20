"""Configuration helpers for dashboard chat infrastructure."""

from dataclasses import dataclass
from enum import Enum
import os

from ddpui.core.dashboard_chat.vector_documents import DashboardChatSourceType


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean env var using Dalgo's common truthy values."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv_env(value: str | None) -> tuple[str, ...] | None:
    """Parse a comma-separated env var into a normalized tuple."""
    if value is None:
        return None
    parsed_values = tuple(
        item.strip().lower() for item in value.split(",") if item and item.strip()
    )
    return parsed_values or None


@dataclass(frozen=True)
class DashboardChatSourceConfig:
    """Environment-backed enablement for retrieval source types."""

    enabled_source_types: tuple[str, ...] = tuple(
        source_type.value for source_type in DashboardChatSourceType
    )

    @classmethod
    def from_env(cls) -> "DashboardChatSourceConfig":
        """Build source-type config from environment variables."""
        env_value = _parse_csv_env(os.getenv("AI_DASHBOARD_CHAT_ENABLED_SOURCE_TYPES"))
        return cls(
            enabled_source_types=env_value
            or tuple(source_type.value for source_type in DashboardChatSourceType)
        )

    def is_enabled(self, source_type: DashboardChatSourceType | str) -> bool:
        """Return whether the given source type should participate in runtime work."""
        source_type_value = source_type.value if isinstance(source_type, Enum) else source_type
        return source_type_value in self.enabled_source_types

    def filter_enabled(
        self,
        source_types: list[DashboardChatSourceType | str] | tuple[DashboardChatSourceType | str, ...],
    ) -> list[str]:
        """Keep only the configured source types from a requested set."""
        return [
            source_type.value if isinstance(source_type, Enum) else source_type
            for source_type in source_types
            if self.is_enabled(source_type)
        ]


@dataclass(frozen=True)
class DashboardChatVectorStoreConfig:
    """Environment-backed configuration for the Chroma sidecar and embeddings."""

    chroma_host: str = "localhost"
    chroma_port: int = 8003
    chroma_ssl: bool = False
    collection_prefix: str = "org_"
    embedding_model: str = "text-embedding-3-small"

    @classmethod
    def from_env(cls) -> "DashboardChatVectorStoreConfig":
        """Build vector store config from environment variables."""
        return cls(
            chroma_host=os.getenv("AI_DASHBOARD_CHAT_CHROMA_HOST", "localhost"),
            chroma_port=int(os.getenv("AI_DASHBOARD_CHAT_CHROMA_PORT", "8003")),
            chroma_ssl=_parse_bool(os.getenv("AI_DASHBOARD_CHAT_CHROMA_SSL"), False),
            collection_prefix=os.getenv("AI_DASHBOARD_CHAT_CHROMA_COLLECTION_PREFIX", "org_"),
            embedding_model=os.getenv(
                "AI_DASHBOARD_CHAT_CHROMA_EMBEDDING_MODEL",
                "text-embedding-3-small",
            ),
        )


@dataclass(frozen=True)
class DashboardChatRuntimeConfig:
    """Environment-backed configuration for dashboard chat orchestration."""

    llm_model: str = "gpt-4o-mini"
    llm_timeout_ms: int = 45000
    retrieval_limit: int = 6
    related_dashboard_limit: int = 3
    max_query_rows: int = 200
    max_distinct_values: int = 50
    max_schema_tables: int = 4

    @classmethod
    def from_env(cls) -> "DashboardChatRuntimeConfig":
        """Build runtime config from environment variables."""
        return cls(
            llm_model=os.getenv("AI_DASHBOARD_CHAT_LLM_MODEL", "gpt-4o-mini"),
            llm_timeout_ms=int(os.getenv("AI_DASHBOARD_CHAT_LLM_TIMEOUT_MS", "45000")),
            retrieval_limit=int(os.getenv("AI_DASHBOARD_CHAT_RETRIEVAL_LIMIT", "6")),
            related_dashboard_limit=int(
                os.getenv("AI_DASHBOARD_CHAT_RELATED_DASHBOARD_LIMIT", "3")
            ),
            max_query_rows=int(os.getenv("AI_DASHBOARD_CHAT_MAX_QUERY_ROWS", "200")),
            max_distinct_values=int(
                os.getenv("AI_DASHBOARD_CHAT_MAX_DISTINCT_VALUES", "50")
            ),
            max_schema_tables=int(os.getenv("AI_DASHBOARD_CHAT_MAX_SCHEMA_TABLES", "4")),
        )
