"""PII override helpers for dashboard chat metadata artifacts."""

from __future__ import annotations

from typing import Iterable

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.metadata.exceptions import DashboardChatPIIColumnNotFoundError
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMetadataArtifact,
    DashboardChatMetadataArtifactStatus,
    DashboardChatPIIColumnOverride,
)
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.schemas.org_preferences_schema import (
    OrgAIDashboardChatPIIColumnsResponse,
    UpdateDashboardChatPIIColumnOverridesSchema,
)


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


def load_dashboard_chat_metadata_payloads(
    org: Org,
) -> list[tuple[Dashboard, DashboardChatMetadataArtifactPayload]]:
    """Load ready metadata artifacts for one organization."""
    artifacts = (
        DashboardChatMetadataArtifact.objects.filter(
            dashboard__org=org,
            dashboard__dashboard_type="native",
            status=DashboardChatMetadataArtifactStatus.READY,
        )
        .select_related("dashboard")
        .order_by("dashboard_id")
    )
    payloads: list[tuple[Dashboard, DashboardChatMetadataArtifactPayload]] = []
    for artifact in artifacts:
        if not artifact.artifact_json:
            continue
        try:
            payload = DashboardChatMetadataArtifactPayload.model_validate(artifact.artifact_json)
        except Exception:
            continue
        payloads.append((artifact.dashboard, payload))
    return payloads


def serialize_dashboard_chat_pii_columns(org: Org) -> OrgAIDashboardChatPIIColumnsResponse:
    """Return metadata columns and reviewed PII state for one organization."""
    payloads = load_dashboard_chat_metadata_payloads(org)
    overrides = load_pii_overrides_for_org(org)
    columns: list[dict] = []
    for dashboard, payload in payloads:
        for table in payload.tables:
            schema_name, bare_table_name, full_table_name = metadata_table_identity(table)
            for column in table.columns:
                key = pii_override_key(schema_name, bare_table_name, column.column_name)
                override_pii = overrides.get(key)
                inferred_pii = bool(column.pii)
                effective_pii = inferred_pii if override_pii is None else override_pii
                columns.append(
                    {
                        "dashboard_id": dashboard.id,
                        "dashboard_title": str(dashboard.title or ""),
                        "schema_name": schema_name,
                        "table_name": bare_table_name,
                        "full_table_name": full_table_name,
                        "model_name": table.model_name,
                        "column_name": column.column_name,
                        "data_type": column.data_type,
                        "description": column.description,
                        "semantic_role": column.semantic_role,
                        "value_semantics": column.value_semantics,
                        "inferred_pii": inferred_pii,
                        "override_pii": override_pii,
                        "effective_pii": effective_pii,
                    }
                )

    columns.sort(
        key=lambda item: (
            item["dashboard_title"].lower(),
            item["full_table_name"].lower(),
            item["column_name"].lower(),
        )
    )
    return OrgAIDashboardChatPIIColumnsResponse(
        columns=columns,
        total_column_count=len(columns),
        pii_column_count=sum(1 for column in columns if column["effective_pii"]),
    )


def update_dashboard_chat_pii_column_overrides(
    *,
    org: Org,
    orguser: OrgUser,
    payload: UpdateDashboardChatPIIColumnOverridesSchema,
) -> OrgAIDashboardChatPIIColumnsResponse:
    """Persist reviewed PII overrides for ready dashboard-chat metadata columns."""
    metadata_payloads = [
        metadata_payload for _, metadata_payload in load_dashboard_chat_metadata_payloads(org)
    ]
    available_keys = payload_column_keys(metadata_payloads)
    for override in payload.overrides:
        schema_name = str(override.schema_name or "").strip()
        table_name = str(override.table_name or "").strip()
        column_name = str(override.column_name or "").strip()
        key = pii_override_key(schema_name, table_name, column_name)
        if key not in available_keys:
            raise DashboardChatPIIColumnNotFoundError(
                f"Column {schema_name}.{table_name}.{column_name} was not found in ready metadata"
            )
        DashboardChatPIIColumnOverride.objects.update_or_create(
            org=org,
            schema_name=key[0],
            table_name=key[1],
            column_name=key[2],
            defaults={
                "pii": bool(override.pii),
                "updated_by": orguser,
            },
        )

    return serialize_dashboard_chat_pii_columns(org)
