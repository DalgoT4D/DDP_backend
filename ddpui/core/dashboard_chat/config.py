"""Configuration helpers for dashboard chat infrastructure."""

from dataclasses import dataclass
import os


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean env var using Dalgo's common truthy values."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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
