"""Cache helpers for dashboard chat prompt templates."""

DASHBOARD_CHAT_PROMPT_CACHE_TTL_SECONDS = 24 * 60 * 60


def build_dashboard_chat_prompt_cache_key(prompt_key: str) -> str:
    """Return the cache key used for one dashboard chat prompt template."""
    return f"dashboard_chat_prompt:{prompt_key}"
