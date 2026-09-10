"""Builds the RunContext for an agent turn.

This is the ONLY place in the agent that reads the ORM or resolves warehouse
credentials. Transports call build_run_context() (sync, so wrap with
database_sync_to_async in async consumers), then pass the context into the run —
tools never touch the database or trust an LLM-supplied org identifier.
"""

from ddpui.auth import granted_permission_slugs
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.models.chat_with_data import ChatWithDataOrgConfig, ChatWithDataOrgMemory
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

# Never offered to the agent, regardless of what the warehouse contains
SYSTEM_SCHEMAS = {
    "information_schema",
    "airbyte_internal",
}
# ...and whole families by prefix: pg_* covers pg_catalog/pg_toast/pg_temp_N
# (session temp schemas appear and vanish per connection), _airbyte* covers
# Airbyte's internal and staging scratch schemas.
SYSTEM_SCHEMA_PREFIXES = ("pg_", "_airbyte")

# Discovery scans these first: dbt-convention schemas hold the curated data.
# Matched as substrings of the schema name ("prod" also catches "production").
PRIORITY_SCHEMA_NAMES = ("prod", "intermediate", "staging")

DEFAULT_MAX_RESULT_ROWS = 100
DEFAULT_QUERY_TIMEOUT_S = 30


class ChatWithDataNotReady(Exception):
    """Raised when the org has no warehouse to chat with."""


def priority_sorted_schemas(schemas: list[str]) -> list[str]:
    """Curated-first ordering: prod-ish, then intermediate, then staging, then
    the rest alphabetically. The prompt and list_schemas present schemas in
    this order so the agent scans the curated layers before raw ones."""

    def rank(schema: str):
        low = schema.lower()
        for position, name in enumerate(PRIORITY_SCHEMA_NAMES):
            if name in low:
                return (0, position, low)
        return (1, 0, low)

    return sorted(schemas, key=rank)


def derive_allowed_schemas(warehouse, dialect: str) -> list[str]:
    """Default schema allowlist: every non-system schema in the warehouse.

    Deliberately NOT restricted to the org's dbt output schema (removed
    2026-09-10): real questions often live in staging/intermediate tables the
    dbt schema misses. The agent's prompt steers it to scan prod/intermediate/
    staging first and to ask the user rather than comb everything else; an
    admin can still pin the list via ChatWithDataOrgConfig.allowed_schemas."""
    if dialect == "bigquery":
        sql = "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA"
    else:
        sql = "SELECT schema_name FROM information_schema.schemata"
    existing = {
        name
        for row in warehouse.execute(sql)
        if (name := row["schema_name"]) not in SYSTEM_SCHEMAS
        and not name.startswith(SYSTEM_SCHEMA_PREFIXES)
    }
    return priority_sorted_schemas(list(existing))


def build_run_context(orguser: OrgUser) -> RunContext:
    """Resolve org warehouse + allowlist + limits into a RunContext. Sync (ORM +
    Secrets Manager); call via database_sync_to_async from async code."""
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if org_warehouse is None:
        raise ChatWithDataNotReady("This organization has no warehouse set up yet")

    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    dialect = org_warehouse.wtype

    # per-org knobs; every org works with no config row (all defaults)
    config = ChatWithDataOrgConfig.objects.filter(org=org).first()
    # admin-curated org facts; no row (or empty text) means no prompt section
    memory = ChatWithDataOrgMemory.objects.filter(org=org).only("text").first()

    if config and config.allowed_schemas:
        allowed_schemas = config.allowed_schemas
    else:
        allowed_schemas = derive_allowed_schemas(warehouse, dialect)

    granted = granted_permission_slugs(
        orguser,
        [
            "can_create_charts",
            "can_create_dashboards",
            "can_edit_dashboards",
            "can_create_metrics",
            "can_create_kpis",
        ],
    )

    return RunContext(
        org_id=org.id,
        org_slug=org.slug,
        dialect=dialect,
        allowed_schemas=allowed_schemas,
        max_result_rows=config.max_result_rows if config else DEFAULT_MAX_RESULT_ROWS,
        query_timeout_s=config.query_timeout_s if config else DEFAULT_QUERY_TIMEOUT_S,
        warehouse=warehouse,
        orguser_id=orguser.id,
        can_create_charts="can_create_charts" in granted,
        can_create_dashboards="can_create_dashboards" in granted,
        can_edit_dashboards="can_edit_dashboards" in granted,
        can_create_metrics="can_create_metrics" in granted,
        can_create_kpis="can_create_kpis" in granted,
        pii_rules=(config.pii_rules if config else []) or [],
        org_memory=memory.text if memory else "",
    )
