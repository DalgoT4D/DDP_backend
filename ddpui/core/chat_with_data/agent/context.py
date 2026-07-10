"""Builds the RunContext for an agent turn.

This is the ONLY place in the agent that reads the ORM or resolves warehouse
credentials. Transports call build_run_context() (sync, so wrap with
database_sync_to_async in async consumers), then pass the context into the run —
tools never touch the database or trust an LLM-supplied org identifier.
"""

from ddpui.core.chat_with_data.agent.state import RunContext
from ddpui.core.chat_with_data.scope import ORG_SCOPE, resolve_scope
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import RolePermission
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

# Never offered to the agent, regardless of what the warehouse contains
SYSTEM_SCHEMAS = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "airbyte_internal",
    "_airbyte_internal",
}

DEFAULT_MAX_RESULT_ROWS = 100
DEFAULT_QUERY_TIMEOUT_S = 30


class ChatWithDataNotReady(Exception):
    """Raised when the org has no warehouse to chat with."""


def derive_allowed_schemas(warehouse, dialect: str, dbt_default_schema: str | None) -> list[str]:
    """Default schema allowlist: the org's dbt output schema if it exists in the
    warehouse, else every non-system schema (raw fallback — decision 2)."""
    if dialect == "bigquery":
        sql = "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA"
    else:
        sql = "SELECT schema_name FROM information_schema.schemata"
    existing = {row["schema_name"] for row in warehouse.execute(sql)} - SYSTEM_SCHEMAS

    if dbt_default_schema and dbt_default_schema in existing:
        return [dbt_default_schema]
    return sorted(existing)


def build_run_context(orguser: OrgUser, session: ChatWithDataSession | None = None) -> RunContext:
    """Resolve org warehouse + allowlist + limits into a RunContext. Sync (ORM +
    Secrets Manager); call via database_sync_to_async from async code.

    `session` carries the scope: a dashboard-scoped session narrows the context
    to that dashboard's tables. Re-resolved on every call (i.e. every turn) so
    dashboard edits are picked up and a deleted dashboard raises ScopeUnavailable
    for the transport to relay. None (e.g. the REPL) means org-wide."""
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if org_warehouse is None:
        raise ChatWithDataNotReady("This organization has no warehouse set up yet")

    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    dialect = org_warehouse.wtype

    scope = ORG_SCOPE
    if session is not None:
        scope = resolve_scope(org, session.scope_type, session.scope_id)

    if scope.allowed_tables is not None:
        # scoped: schemas follow the scoped tables — no schemata discovery query
        allowed_schemas = sorted({ref.split(".", 1)[0] for ref in scope.allowed_tables})
    else:
        org_dbt: OrgDbt | None = org.dbt
        dbt_schema = org_dbt.default_schema if org_dbt else None
        allowed_schemas = derive_allowed_schemas(warehouse, dialect, dbt_schema)

    granted = set(
        RolePermission.objects.filter(
            role=orguser.new_role,
            permission__slug__in=["can_create_charts", "can_create_dashboards"],
        ).values_list("permission__slug", flat=True)
    )

    return RunContext(
        org_id=org.id,
        org_slug=org.slug,
        dialect=dialect,
        allowed_schemas=allowed_schemas,
        max_result_rows=DEFAULT_MAX_RESULT_ROWS,
        query_timeout_s=DEFAULT_QUERY_TIMEOUT_S,
        warehouse=warehouse,
        orguser_id=orguser.id,
        can_create_charts="can_create_charts" in granted,
        can_create_dashboards="can_create_dashboards" in granted,
        scope_type=scope.scope_type,
        allowed_tables=scope.allowed_tables,
        scope_context=scope.scope_context,
    )
