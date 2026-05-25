"""Persistence helpers for dashboard chat metadata artifacts."""

from typing import Any

from django.utils import timezone
from pydantic import ValidationError

from ddpui.core.dashboard_chat.metadata.schemas import (
    DASHBOARD_CHAT_METADATA_SCHEMA_VERSION,
    DashboardChatMetadataArtifactPayload,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMetadataArtifact,
    DashboardChatMetadataArtifactStatus,
)


def load_dashboard_chat_metadata_artifact(
    dashboard: Dashboard,
) -> DashboardChatMetadataArtifactPayload | None:
    """Load and validate the persisted artifact for one dashboard."""
    artifact = (
        DashboardChatMetadataArtifact.objects.filter(
            dashboard=dashboard,
            status=DashboardChatMetadataArtifactStatus.READY,
        )
        .only("artifact_json")
        .first()
    )
    if artifact is None or not artifact.artifact_json:
        return None
    if artifact.schema_version != DASHBOARD_CHAT_METADATA_SCHEMA_VERSION:
        return None
    try:
        payload = DashboardChatMetadataArtifactPayload.model_validate(artifact.artifact_json)
    except ValidationError:
        return None
    if payload.schema_version != DASHBOARD_CHAT_METADATA_SCHEMA_VERSION:
        return None
    return payload


def persist_dashboard_chat_metadata_artifact(
    *,
    dashboard: Dashboard,
    payload: DashboardChatMetadataArtifactPayload,
    source_fingerprint: str,
    builder_model: str,
    built_by=None,
    error_payload: dict[str, Any] | None = None,
) -> DashboardChatMetadataArtifact:
    """Create or update one dashboard metadata artifact row."""
    artifact, _ = DashboardChatMetadataArtifact.objects.get_or_create(dashboard=dashboard)
    artifact.schema_version = payload.schema_version
    artifact.status = DashboardChatMetadataArtifactStatus.READY
    artifact.artifact_json = payload.model_dump(mode="json")
    artifact.source_fingerprint = source_fingerprint
    artifact.builder_model = builder_model
    artifact.built_by = built_by
    artifact.error_payload = error_payload
    artifact.built_at = timezone.now()
    artifact.save(
        update_fields=[
            "schema_version",
            "status",
            "artifact_json",
            "source_fingerprint",
            "builder_model",
            "built_by",
            "error_payload",
            "built_at",
            "updated_at",
        ]
    )
    return artifact
