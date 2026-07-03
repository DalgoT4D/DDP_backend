"""State and runtime-context types for the Chat with Data agent.

RunContext carries everything org- or user-specific into a run. It is resolved
server-side (never from LLM output) and injected into tools via ToolRuntime —
the model never sees or supplies any of these values.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunContext:
    """Per-turn context, built by the transport layer before the agent runs."""

    org_id: int
    org_slug: str
    dialect: str  # "postgres" | "bigquery" (matches OrgWarehouse.wtype)
    allowed_schemas: list[str] = field(default_factory=list)
    max_result_rows: int = 100
    query_timeout_s: int = 30
    # Warehouse client (ddpui.utils.warehouse.client interface). Typed Any to keep
    # this module import-light for tests; only tools touch it.
    warehouse: Any = None
