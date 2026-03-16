"""Helpers for dashboard-chat settings APIs."""

from typing import Any

from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences, default_ai_chat_source_config
from ddpui.models.org_user import OrgUser
from ddpui.utils.feature_flags import is_feature_flag_enabled


class AiDashboardChatSettingsValidationError(Exception):
    """Raised when AI dashboard chat settings payloads are invalid."""


class AiDashboardChatSettingsService:
    """Encapsulates AI dashboard chat settings normalization and persistence."""

    @staticmethod
    def get_or_create_preferences(org: Org) -> OrgPreferences:
        """Return the org preferences row, creating it when absent."""
        preferences = OrgPreferences.objects.filter(org=org).first()
        if preferences is None:
            preferences = OrgPreferences.objects.create(org=org)
        return preferences

    @staticmethod
    def normalize_source_config(source_config: dict[str, Any] | None) -> dict[str, bool]:
        """Merge partial source config updates with the supported defaults."""
        normalized = default_ai_chat_source_config()
        if source_config is None:
            return normalized

        unexpected_keys = set(source_config.keys()) - set(normalized.keys())
        if unexpected_keys:
            raise AiDashboardChatSettingsValidationError(
                f"Unsupported ai_chat_source_config keys: {sorted(unexpected_keys)}"
            )

        for key, value in source_config.items():
            if not isinstance(value, bool):
                raise AiDashboardChatSettingsValidationError(
                    f"ai_chat_source_config.{key} must be a boolean"
                )
            normalized[key] = value

        return normalized

    @staticmethod
    def mark_context_dirty(org: Org) -> None:
        """Reset the vector-ingest freshness marker when context changes."""
        orgdbt = getattr(org, "dbt", None)
        if orgdbt is None or orgdbt.ai_vector_last_ingested_at is None:
            return

        orgdbt.ai_vector_last_ingested_at = None
        orgdbt.save(update_fields=["ai_vector_last_ingested_at", "updated_at"])

    @staticmethod
    def update_org_settings(orguser: OrgUser, payload_data: dict[str, Any]) -> OrgPreferences:
        """Apply org-level settings changes and return the updated preferences row."""
        preferences = AiDashboardChatSettingsService.get_or_create_preferences(orguser.org)
        update_fields: list[str] = []
        now = timezone.now()
        source_config_changed = False
        org_context_changed = False

        if "ai_data_sharing_enabled" in payload_data:
            enabled = payload_data["ai_data_sharing_enabled"]
            if enabled != preferences.ai_data_sharing_enabled:
                preferences.ai_data_sharing_enabled = enabled
                update_fields.append("ai_data_sharing_enabled")
                if enabled:
                    preferences.ai_data_sharing_consented_by = orguser
                    preferences.ai_data_sharing_consented_at = now
                else:
                    preferences.ai_data_sharing_consented_by = None
                    preferences.ai_data_sharing_consented_at = None
                update_fields.extend(
                    ["ai_data_sharing_consented_by", "ai_data_sharing_consented_at"]
                )

        if "ai_org_context_markdown" in payload_data:
            markdown = payload_data["ai_org_context_markdown"] or ""
            if markdown != preferences.ai_org_context_markdown:
                preferences.ai_org_context_markdown = markdown
                preferences.ai_org_context_updated_by = orguser
                preferences.ai_org_context_updated_at = now
                update_fields.extend(
                    [
                        "ai_org_context_markdown",
                        "ai_org_context_updated_by",
                        "ai_org_context_updated_at",
                    ]
                )
                org_context_changed = True

        if "ai_chat_source_config" in payload_data:
            source_config = AiDashboardChatSettingsService.normalize_source_config(
                payload_data["ai_chat_source_config"]
            )
            current_source_config = AiDashboardChatSettingsService.normalize_source_config(
                preferences.ai_chat_source_config
            )
            if source_config != current_source_config:
                preferences.ai_chat_source_config = source_config
                update_fields.append("ai_chat_source_config")
                source_config_changed = True

        if update_fields:
            preferences.save(update_fields=list(dict.fromkeys(update_fields + ["updated_at"])))

        if org_context_changed or source_config_changed:
            AiDashboardChatSettingsService.mark_context_dirty(orguser.org)

        return preferences

    @staticmethod
    def build_settings_response(org: Org, preferences: OrgPreferences) -> dict[str, Any]:
        """Build the org-level settings response payload."""
        source_config = AiDashboardChatSettingsService.normalize_source_config(
            preferences.ai_chat_source_config
        )
        orgdbt = getattr(org, "dbt", None)

        return {
            "feature_flag_enabled": bool(is_feature_flag_enabled("AI_DASHBOARD_CHAT", org=org)),
            "ai_data_sharing_enabled": bool(preferences.ai_data_sharing_enabled),
            "ai_data_sharing_consented_by": (
                preferences.ai_data_sharing_consented_by.user.email
                if preferences.ai_data_sharing_consented_by
                else None
            ),
            "ai_data_sharing_consented_at": preferences.ai_data_sharing_consented_at,
            "ai_org_context_markdown": preferences.ai_org_context_markdown or "",
            "ai_org_context_updated_by": (
                preferences.ai_org_context_updated_by.user.email
                if preferences.ai_org_context_updated_by
                else None
            ),
            "ai_org_context_updated_at": preferences.ai_org_context_updated_at,
            "ai_chat_source_config": source_config,
            "dbt_configured": bool(org.dbt_id),
            "ai_docs_generated_at": orgdbt.ai_docs_generated_at if orgdbt else None,
            "ai_vector_last_ingested_at": orgdbt.ai_vector_last_ingested_at if orgdbt else None,
        }

    @staticmethod
    def build_status_response(org: Org, preferences: OrgPreferences) -> dict[str, Any]:
        """Build the org-level availability response payload."""
        feature_flag_enabled = bool(is_feature_flag_enabled("AI_DASHBOARD_CHAT", org=org))
        orgdbt = getattr(org, "dbt", None)

        return {
            "feature_flag_enabled": feature_flag_enabled,
            "ai_data_sharing_enabled": bool(preferences.ai_data_sharing_enabled),
            "chat_available": feature_flag_enabled and bool(preferences.ai_data_sharing_enabled),
            "dbt_configured": bool(org.dbt_id),
            "ai_docs_generated_at": orgdbt.ai_docs_generated_at if orgdbt else None,
            "ai_vector_last_ingested_at": orgdbt.ai_vector_last_ingested_at if orgdbt else None,
        }
