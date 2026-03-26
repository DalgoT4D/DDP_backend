"""Embedding providers used by dashboard chat retrieval."""

import os
from typing import Any, Protocol

from openai import OpenAI

from ddpui.utils.openai_client import get_shared_openai_client


class DashboardChatEmbeddingProvider(Protocol):
    """Embedding provider interface used by the vector store wrapper."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query."""

    def reset_usage(self) -> None:
        """Reset per-turn embedding usage before a new runtime invocation."""


class OpenAIEmbeddingProvider:
    """OpenAI embeddings adapter for dashboard chat retrieval."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "text-embedding-3-small",
        client: OpenAI | None = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.usage_events: list[dict[str, Any]] = []
        if client is None:
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY must be set for dashboard chat embeddings")
            client = get_shared_openai_client(self.api_key, max_retries=2)
        self.client = client

    def reset_usage(self) -> None:
        """Reset aggregated embedding usage before one new chat turn."""
        self.usage_events = []

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents using OpenAI."""
        if not texts:
            return []
        response = self.client.embeddings.create(model=self.model, input=texts)
        self._record_usage("embed_documents", response, len(texts))
        return [item.embedding for item in response.data]

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query using the document embedding path."""
        return self.embed_documents([text])[0]

    def usage_summary(self) -> dict[str, Any]:
        """Return aggregated embedding usage for the current turn."""
        totals = {
            "prompt_tokens": 0,
            "total_tokens": 0,
        }
        for event in self.usage_events:
            totals["prompt_tokens"] += event.get("prompt_tokens", 0)
            totals["total_tokens"] += event.get("total_tokens", 0)
        return {
            "model": self.model,
            "calls": list(self.usage_events),
            "totals": totals,
        }

    def _record_usage(self, operation: str, response: Any, input_count: int) -> None:
        """Capture embedding usage from one OpenAI embeddings response."""
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        self.usage_events.append(
            {
                "operation": operation,
                "model": self.model,
                "input_count": input_count,
                "prompt_tokens": getattr(usage, "prompt_tokens", 0) or 0,
                "total_tokens": getattr(usage, "total_tokens", 0) or 0,
            }
        )
