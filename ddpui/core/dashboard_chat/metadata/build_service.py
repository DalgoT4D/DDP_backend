"""Shared services for dashboard-chat metadata artifact builds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.utils import timezone

from ddpui.core.dashboard_chat.metadata.builder import DashboardChatMetadataArtifactBuilder
from ddpui.core.dashboard_chat.metadata.enricher import DashboardChatMetadataEnricher
from ddpui.core.dashboard_chat.metadata.pii_overrides import (
    apply_pii_overrides_to_payload,
    load_pii_overrides_for_org,
)
from ddpui.core.dashboard_chat.metadata.schemas import DASHBOARD_CHAT_METADATA_SCHEMA_VERSION
from ddpui.core.dashboard_chat.metadata.storage import persist_dashboard_chat_metadata_artifact
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardAIContext,
    DashboardChatMetadataArtifact,
    DashboardChatMetadataArtifactStatus,
    DashboardChatMetadataBuildRun,
    OrgAIContext,
)
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


@dataclass
class DashboardChatMetadataBuildResult:
    """Normalized build outcome for one dashboard artifact."""

    dashboard_id: int
    dashboard_title: str
    status: str
    table_count: int = 0
    join_path_count: int = 0
    chart_count: int = 0
    source_fingerprint: str = ""
    built_at: str | None = None
    error_message: str | None = None


class DashboardChatMetadataBuildService:
    """Build one or more dashboard-scoped metadata artifacts."""

    def __init__(
        self,
        *,
        builder: DashboardChatMetadataArtifactBuilder | None = None,
        enricher: DashboardChatMetadataEnricher | None = None,
    ):
        self.builder = builder or DashboardChatMetadataArtifactBuilder()
        self.enricher = enricher or DashboardChatMetadataEnricher()

    def build_dashboards(
        self,
        *,
        org: Org,
        dashboards: list[Dashboard],
        builder_model: str,
        built_by: OrgUser | None = None,
    ) -> list[DashboardChatMetadataBuildResult]:
        """Build dashboard metadata artifacts synchronously."""
        warehouse_tools = DashboardChatWarehouseTools(org=org)
        results: list[DashboardChatMetadataBuildResult] = []

        for dashboard in dashboards:
            build_run = DashboardChatMetadataBuildRun.objects.create(
                dashboard=dashboard,
                status=DashboardChatMetadataArtifactStatus.BUILDING,
                builder_model=builder_model,
                started_at=timezone.now(),
            )
            artifact, _ = DashboardChatMetadataArtifact.objects.get_or_create(dashboard=dashboard)
            artifact.schema_version = DASHBOARD_CHAT_METADATA_SCHEMA_VERSION
            artifact.status = DashboardChatMetadataArtifactStatus.BUILDING
            artifact.builder_model = builder_model
            artifact.error_payload = None
            artifact.save(
                update_fields=[
                    "schema_version",
                    "status",
                    "builder_model",
                    "error_payload",
                    "updated_at",
                ]
            )

            try:
                payload = self.builder.build_for_dashboard(
                    org=org,
                    dashboard=dashboard,
                    warehouse_tools=warehouse_tools,
                )
                org_context_markdown = (
                    OrgAIContext.objects.filter(org=org).values_list("markdown", flat=True).first()
                    or ""
                )
                dashboard_context_markdown = (
                    DashboardAIContext.objects.filter(dashboard=dashboard)
                    .values_list("markdown", flat=True)
                    .first()
                    or ""
                )
                payload = self.enricher.enrich_payload(
                    payload=payload,
                    org_context_markdown=org_context_markdown,
                    dashboard_context_markdown=dashboard_context_markdown,
                    warehouse_tools=warehouse_tools,
                )
                payload = apply_pii_overrides_to_payload(
                    payload,
                    load_pii_overrides_for_org(org),
                )
                payload = self.builder.rebuild_derived_indexes(payload)
                saved_artifact = persist_dashboard_chat_metadata_artifact(
                    dashboard=dashboard,
                    payload=payload,
                    source_fingerprint=payload.source_fingerprint,
                    builder_model=builder_model,
                    built_by=built_by,
                )
                build_run.status = DashboardChatMetadataArtifactStatus.READY
                build_run.log_payload = {
                    "dashboard_id": dashboard.id,
                    "table_count": len(payload.tables),
                    "join_path_count": len(payload.join_paths),
                    "chart_count": len(payload.chart_table_map),
                    "source_fingerprint": payload.source_fingerprint,
                }
                results.append(
                    DashboardChatMetadataBuildResult(
                        dashboard_id=dashboard.id,
                        dashboard_title=str(dashboard.title or ""),
                        status=DashboardChatMetadataArtifactStatus.READY,
                        table_count=len(payload.tables),
                        join_path_count=len(payload.join_paths),
                        chart_count=len(payload.chart_table_map),
                        source_fingerprint=payload.source_fingerprint,
                        built_at=saved_artifact.built_at.isoformat()
                        if saved_artifact.built_at
                        else None,
                    )
                )
            except Exception as error:
                error_message = str(error)
                artifact.status = DashboardChatMetadataArtifactStatus.FAILED
                artifact.error_payload = {"error": error_message}
                artifact.save(update_fields=["status", "error_payload", "updated_at"])
                build_run.status = DashboardChatMetadataArtifactStatus.FAILED
                build_run.error_payload = {"error": error_message}
                results.append(
                    DashboardChatMetadataBuildResult(
                        dashboard_id=dashboard.id,
                        dashboard_title=str(dashboard.title or ""),
                        status=DashboardChatMetadataArtifactStatus.FAILED,
                        error_message=error_message,
                    )
                )
            finally:
                build_run.finished_at = timezone.now()
                build_run.save(
                    update_fields=["status", "finished_at", "log_payload", "error_payload"]
                )

        return results

    def mark_dashboards_building(
        self,
        *,
        dashboards: list[Dashboard],
        builder_model: str,
    ) -> None:
        """Mark dashboard artifacts as building before an async worker picks them up."""
        for dashboard in dashboards:
            artifact, _ = DashboardChatMetadataArtifact.objects.get_or_create(dashboard=dashboard)
            artifact.schema_version = DASHBOARD_CHAT_METADATA_SCHEMA_VERSION
            artifact.status = DashboardChatMetadataArtifactStatus.BUILDING
            artifact.builder_model = builder_model
            artifact.error_payload = None
            artifact.save(
                update_fields=[
                    "schema_version",
                    "status",
                    "builder_model",
                    "error_payload",
                    "updated_at",
                ]
            )


def summarize_dashboard_metadata_status(
    dashboards: list[Dashboard],
) -> dict[str, Any]:
    """Return org-scoped dashboard metadata status for settings pages."""
    artifacts = {
        artifact.dashboard_id: artifact
        for artifact in DashboardChatMetadataArtifact.objects.filter(
            dashboard__in=dashboards
        ).select_related("dashboard")
    }
    dashboard_rows: list[dict[str, Any]] = []
    ready_count = 0
    failed_count = 0
    stale_count = 0
    missing_count = 0
    last_built_at = None

    for dashboard in dashboards:
        artifact = artifacts.get(dashboard.id)
        artifact_payload = artifact.artifact_json if artifact else {}
        table_count = len((artifact_payload or {}).get("tables") or [])
        chart_count = len((artifact_payload or {}).get("chart_table_map") or {})
        error_message = None
        if artifact and artifact.error_payload:
            error_message = artifact.error_payload.get("error")

        status = artifact.status if artifact else "missing"
        if (
            artifact is not None
            and status == DashboardChatMetadataArtifactStatus.READY
            and artifact.schema_version != DASHBOARD_CHAT_METADATA_SCHEMA_VERSION
        ):
            status = DashboardChatMetadataArtifactStatus.STALE
        if status == DashboardChatMetadataArtifactStatus.READY:
            ready_count += 1
        elif status == DashboardChatMetadataArtifactStatus.FAILED:
            failed_count += 1
        elif status == DashboardChatMetadataArtifactStatus.STALE:
            stale_count += 1
        else:
            missing_count += 1

        if (
            artifact
            and artifact.built_at
            and (last_built_at is None or artifact.built_at > last_built_at)
        ):
            last_built_at = artifact.built_at

        dashboard_rows.append(
            {
                "dashboard_id": dashboard.id,
                "dashboard_title": str(dashboard.title or ""),
                "status": status,
                "table_count": table_count,
                "chart_count": chart_count,
                "builder_model": artifact.builder_model if artifact else "",
                "source_fingerprint": artifact.source_fingerprint if artifact else "",
                "built_at": artifact.built_at if artifact else None,
                "error_message": error_message,
            }
        )

    return {
        "dashboards": dashboard_rows,
        "total_dashboard_count": len(dashboards),
        "ready_dashboard_count": ready_count,
        "failed_dashboard_count": failed_count,
        "stale_dashboard_count": stale_count,
        "missing_dashboard_count": missing_count,
        "last_built_at": last_built_at,
    }
