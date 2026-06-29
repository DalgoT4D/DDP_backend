"""Load-context node for dashboard chat graph."""

from typing import Any

from ddpui.models.dashboard_chat import (
    DashboardAIContext,
    DashboardChatMetadataArtifact,
    OrgAIContext,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
)
from ddpui.core.dashboard_chat.metadata.builder import (
    build_chart_registry_from_dashboard_export,
)
from ddpui.core.dashboard_chat.metadata.storage import (
    load_dashboard_chat_metadata_artifact,
)
from ddpui.core.dashboard_chat.metadata.pii_overrides import (
    apply_pii_overrides_to_payload,
    load_pii_overrides_for_org,
)
from ddpui.services.dashboard_service import DashboardService

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def load_context_node(state: DashboardChatGraphState) -> dict[str, Any]:
    """Bootstrap or reuse the session-stable checkpointed dashboard context."""
    if (
        state.get("org_context_markdown") is not None
        and state.get("dashboard_context_markdown") is not None
        and state.get("dashboard_export_payload") is not None
        and state.get("chart_registry_payload") is not None
        and state.get("metadata_artifact_status") is not None
        and state.get("allowlist_payload") is not None
    ):
        return {
            "schema_snippet_payloads": dict(state.get("schema_snippet_payloads") or {}),
            "validated_distinct_payloads": dict(state.get("validated_distinct_payloads") or {}),
        }

    org = Org.objects.select_related("dbt").get(id=int(state["org_id"]))
    dashboard_id = int(state["dashboard_id"])
    dashboard = Dashboard.objects.get(id=dashboard_id)
    dashboard_export = DashboardService.export_dashboard_context(
        dashboard_id,
        org,
    )
    metadata_artifact = (
        DashboardChatMetadataArtifact.objects.filter(dashboard_id=dashboard_id)
        .only("status", "schema_version")
        .first()
    )
    metadata_payload = load_dashboard_chat_metadata_artifact(dashboard)
    if metadata_payload is not None:
        allowlist = DashboardChatAllowlistBuilder.build_from_metadata_artifact(metadata_payload)
    else:
        allowlist = DashboardChatAllowlist()
    metadata_status = metadata_artifact.status if metadata_artifact is not None else "missing"
    if (
        metadata_artifact is not None
        and metadata_artifact.status == "ready"
        and metadata_payload is None
    ):
        metadata_status = "stale"
    if metadata_payload is not None:
        metadata_payload = apply_pii_overrides_to_payload(
            metadata_payload,
            load_pii_overrides_for_org(org),
        )
    org_context_markdown = (
        OrgAIContext.objects.filter(org=org).values_list("markdown", flat=True).first() or ""
    )
    dashboard_context_markdown = (
        DashboardAIContext.objects.filter(dashboard_id=dashboard_id)
        .values_list("markdown", flat=True)
        .first()
        or ""
    )
    return {
        "org_context_markdown": org_context_markdown,
        "dashboard_context_markdown": dashboard_context_markdown,
        "dashboard_export_payload": dashboard_export,
        "chart_registry_payload": build_chart_registry_from_dashboard_export(dashboard_export),
        "metadata_artifact_payload": (
            metadata_payload.model_dump(mode="json") if metadata_payload is not None else None
        ),
        "metadata_artifact_status": metadata_status,
        "allowlist_payload": allowlist.model_dump(mode="json"),
        "schema_snippet_payloads": dict(state.get("schema_snippet_payloads") or {}),
        "validated_distinct_payloads": dict(state.get("validated_distinct_payloads") or {}),
    }
