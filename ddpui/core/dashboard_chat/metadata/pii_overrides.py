"""PII override helpers for dashboard chat metadata artifacts."""

from __future__ import annotations

from typing import Iterable

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataTable,
)
from ddpui.models.dashboard_chat import DashboardChatPIIColumnOverride
from ddpui.models.org import Org


PIIOverrideKey = tuple[str, str, str]


def metadata_table_identity(table: DashboardChatMetadataTable | dict) -> tuple[str, str, str]:
    """Return schema, bare table, and full table names for one metadata table."""
    if isinstance(table, dict):
        full_table_name = str(table.get("table_name") or "")
        schema_name = str(table.get("schema_name") or "")
    else:
        full_table_name = table.table_name
        schema_name = table.schema_name
    if "." in full_table_name:
        inferred_schema, bare_table_name = full_table_name.split(".", 1)
        schema_name = schema_name or inferred_schema
    else:
        bare_table_name = full_table_name
    return schema_name, bare_table_name, full_table_name


def pii_override_key(schema_name: str, table_name: str, column_name: str) -> PIIOverrideKey:
    """Normalize one override lookup key."""
    return (
        str(schema_name or "").strip().lower(),
        str(table_name or "").strip().lower(),
        str(column_name or "").strip().lower(),
    )


def load_pii_overrides_for_org(org: Org | int) -> dict[PIIOverrideKey, bool]:
    """Load user-reviewed PII overrides keyed by schema/table/column."""
    org_id = org if isinstance(org, int) else org.id
    overrides = DashboardChatPIIColumnOverride.objects.filter(org_id=org_id).values(
        "schema_name",
        "table_name",
        "column_name",
        "pii",
    )
    return {
        pii_override_key(
            str(override["schema_name"] or ""),
            str(override["table_name"] or ""),
            str(override["column_name"] or ""),
        ): bool(override["pii"])
        for override in overrides
    }


def apply_pii_overrides_to_payload(
    payload: DashboardChatMetadataArtifactPayload,
    overrides: dict[PIIOverrideKey, bool],
) -> DashboardChatMetadataArtifactPayload:
    """Apply persisted PII overrides to a metadata artifact copy."""
    if not overrides:
        return payload
    payload_copy = payload.model_copy(deep=True)
    updated_tables = []
    for table in payload_copy.tables:
        schema_name, bare_table_name, _ = metadata_table_identity(table)
        updated_columns = []
        for column in table.columns:
            key = pii_override_key(schema_name, bare_table_name, column.column_name)
            if key in overrides:
                column_copy = column.model_copy(deep=True)
                column_copy.pii = overrides[key]
                updated_columns.append(column_copy)
            else:
                updated_columns.append(column)
        table.columns = updated_columns
        updated_tables.append(table)
    payload_copy.tables = updated_tables
    return payload_copy


def payload_column_keys(
    payloads: Iterable[DashboardChatMetadataArtifactPayload],
) -> set[PIIOverrideKey]:
    """Return every schema/table/column key present in one or more artifacts."""
    keys: set[PIIOverrideKey] = set()
    for payload in payloads:
        for table in payload.tables:
            schema_name, bare_table_name, _ = metadata_table_identity(table)
            for column in table.columns:
                keys.add(pii_override_key(schema_name, bare_table_name, column.column_name))
    return keys
