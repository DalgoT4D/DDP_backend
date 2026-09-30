"""Service for the Copilot settings surface: the org's CHAT_WITH_DATA feature
flag (enable/disable Copilot) and its org memory (admin-curated facts injected
into the agents' system prompts — see org_memory_section in core/ai/prompts.py).
"""

from django.utils import timezone

from ddpui.core.ai.constants import CHAT_WITH_DATA_FLAG, MAX_ORG_MEMORY_CHARS
from ddpui.models.chat_with_data import ChatWithDataOrgConfig
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import CopilotSettingsOut, CopilotSettingsUpdate
from ddpui.utils.feature_flags import (
    disable_feature_flag,
    enable_feature_flag,
    is_feature_flag_enabled,
)


class MemoryTooLong(Exception):
    """Memory text exceeds MAX_ORG_MEMORY_CHARS."""


def get_settings(orguser: OrgUser) -> CopilotSettingsOut:
    """Current Copilot settings for the org. Never 404s: no config row reads
    as empty text, no flag row reads as disabled."""
    org = orguser.org
    config = ChatWithDataOrgConfig.objects.filter(org=org).first()
    return CopilotSettingsOut(
        enabled=bool(is_feature_flag_enabled(CHAT_WITH_DATA_FLAG, org)),
        text=config.memory_text if config else "",
        updated_at=config.memory_updated_at.isoformat()
        if config and config.memory_updated_at
        else None,
        updated_by_email=(
            config.memory_updated_by.user.email if config and config.memory_updated_by else None
        ),
        max_chars=MAX_ORG_MEMORY_CHARS,
    )


def update_settings(orguser: OrgUser, payload: CopilotSettingsUpdate) -> CopilotSettingsOut:
    """Partial update: each field only changes when supplied. `enabled` writes
    the org's CHAT_WITH_DATA flag row (org row beats a global one); `text`
    upserts the config row — empty string is a valid clear."""
    org = orguser.org
    enabled, text = payload.enabled, payload.text

    if enabled is True:
        enable_feature_flag(CHAT_WITH_DATA_FLAG, org)
    elif enabled is False:
        disable_feature_flag(CHAT_WITH_DATA_FLAG, org)

    if text is not None:
        text = text.strip()
        if len(text) > MAX_ORG_MEMORY_CHARS:
            raise MemoryTooLong(f"context must be at most {MAX_ORG_MEMORY_CHARS} characters")
        ChatWithDataOrgConfig.objects.update_or_create(
            org=org,
            defaults={
                "memory_text": text,
                "memory_updated_by": orguser,
                "memory_updated_at": timezone.now(),
            },
        )

    return get_settings(orguser)
