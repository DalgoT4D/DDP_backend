"""Service for the Copilot settings surface: the org's CHAT_WITH_DATA feature
flag (enable/disable Copilot) and its org memory (admin-curated facts injected
into the agents' system prompts — see core/ai/agent/org_memory.py).
"""

from ddpui.core.ai.agent.org_memory import MAX_ORG_MEMORY_CHARS
from ddpui.models.chat_with_data import ChatWithDataOrgMemory
from ddpui.models.org_user import OrgUser
from ddpui.utils.feature_flags import (
    disable_feature_flag,
    enable_feature_flag,
    is_feature_flag_enabled,
)

CHAT_WITH_DATA_FLAG = "CHAT_WITH_DATA"


class MemoryTooLong(Exception):
    """Memory text exceeds MAX_ORG_MEMORY_CHARS."""


def get_settings(orguser: OrgUser) -> dict:
    """Current Copilot settings for the org. Never 404s: no memory row reads
    as empty text, no flag row reads as disabled."""
    org = orguser.org
    memory = ChatWithDataOrgMemory.objects.filter(org=org).first()
    return {
        "enabled": bool(is_feature_flag_enabled(CHAT_WITH_DATA_FLAG, org)),
        "text": memory.text if memory else "",
        "updated_at": memory.updated_at.isoformat() if memory else None,
        "updated_by_email": (
            memory.updated_by.user.email if memory and memory.updated_by else None
        ),
        "max_chars": MAX_ORG_MEMORY_CHARS,
    }


def update_settings(orguser: OrgUser, enabled: bool | None, text: str | None) -> dict:
    """Partial update: each field only changes when supplied. `enabled` writes
    the org's CHAT_WITH_DATA flag row (org row beats a global one); `text`
    upserts the memory row — empty string is a valid clear."""
    org = orguser.org

    if enabled is True:
        enable_feature_flag(CHAT_WITH_DATA_FLAG, org)
    elif enabled is False:
        disable_feature_flag(CHAT_WITH_DATA_FLAG, org)

    if text is not None:
        text = text.strip()
        if len(text) > MAX_ORG_MEMORY_CHARS:
            raise MemoryTooLong(
                f"context must be at most {MAX_ORG_MEMORY_CHARS} characters"
            )
        ChatWithDataOrgMemory.objects.update_or_create(
            org=org, defaults={"text": text, "updated_by": orguser}
        )

    return get_settings(orguser)
