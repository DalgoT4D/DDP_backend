"""Configuration helpers for the live metadata-first dashboard chat runtime."""

from dataclasses import dataclass
import os


def _parse_optional_int(value: str | None) -> int | None:
    """Parse an integer env var, treating blank/zero/non-positive as disabled."""
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    parsed = int(stripped)
    return parsed if parsed > 0 else None


def _parse_optional_str(value: str | None) -> str | None:
    """Parse a string env var, treating blank values as disabled."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
@dataclass(frozen=True)
class DashboardChatRuntimeConfig:
    """Environment-backed configuration for dashboard chat orchestration."""

    llm_model: str = "gpt-4o-mini"
    intent_llm_model: str | None = None
    final_answer_llm_model: str | None = None
    llm_reasoning_effort: str | None = None
    llm_timeout_ms: int | None = None
    llm_max_attempts: int = 1
    retrieval_limit: int = 6
    max_query_rows: int = 200
    max_distinct_values: int = 50
    max_schema_tables: int = 4

    @classmethod
    def from_env(cls) -> "DashboardChatRuntimeConfig":
        """Build runtime config from environment variables."""
        return cls(
            llm_model=os.getenv("AI_DASHBOARD_CHAT_LLM_MODEL", "gpt-4o-mini"),
            intent_llm_model=_parse_optional_str(
                os.getenv("AI_DASHBOARD_CHAT_INTENT_LLM_MODEL")
            ),
            final_answer_llm_model=_parse_optional_str(
                os.getenv("AI_DASHBOARD_CHAT_FINAL_ANSWER_LLM_MODEL")
            ),
            llm_reasoning_effort=_parse_optional_str(
                os.getenv("AI_DASHBOARD_CHAT_LLM_REASONING_EFFORT")
            ),
            llm_timeout_ms=_parse_optional_int(os.getenv("AI_DASHBOARD_CHAT_LLM_TIMEOUT_MS")),
            llm_max_attempts=int(os.getenv("AI_DASHBOARD_CHAT_LLM_MAX_ATTEMPTS", "1")),
            retrieval_limit=int(os.getenv("AI_DASHBOARD_CHAT_RETRIEVAL_LIMIT", "6")),
            max_query_rows=int(os.getenv("AI_DASHBOARD_CHAT_MAX_QUERY_ROWS", "200")),
            max_distinct_values=int(os.getenv("AI_DASHBOARD_CHAT_MAX_DISTINCT_VALUES", "50")),
            max_schema_tables=int(os.getenv("AI_DASHBOARD_CHAT_MAX_SCHEMA_TABLES", "4")),
        )
