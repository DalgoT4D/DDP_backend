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
    # Who is chatting — needed by tools that create Dalgo artifacts (charts)
    orguser_id: int | None = None
    # Resolved from RolePermission at context-build time; tools never query RBAC
    can_create_charts: bool = False
    can_create_dashboards: bool = False
    # Org-specific PII detector rules (ChatWithDataOrgConfig.pii_rules) — the
    # transport passes these into build_agent's PII middleware
    pii_rules: list[dict] = field(default_factory=list)
    # Set per turn by the runner (router output) — reflection gate + tool context
    question: str = ""
    complexity: str = "simple"
