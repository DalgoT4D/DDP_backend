"""Generic Chroma result objects shared across Dalgo."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChromaQueryResult:
    """Single query result returned from Chroma."""

    document_id: str
    content: str
    metadata: dict[str, Any]
    distance: float | None = None


@dataclass(frozen=True)
class ChromaStoredDocument:
    """Stored document metadata returned from Chroma collection reads."""

    document_id: str
    metadata: dict[str, Any]
    content: str | None = None
